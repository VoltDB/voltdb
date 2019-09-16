/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.export;

import java.io.File;
import java.io.FileDescriptor;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.channels.FileChannel;
import java.util.BitSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Bits;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.utils.PosixAdvise;
import org.voltdb.utils.VoltFile;

import com.google.common.collect.ImmutableSortedMap;

/*
 * This class provides a means for periodically saving smallish pieces of state to disk. All parts of
 * the generated files have checksums to verify that each file is self-consistent. There are always 2
 * files that get written alternately so that if one file is only partially written and then becomes
 * corrupted (due to an hardware failure), the previously written file can be used.
 *
 * The file structure has two sections. The first section contains the individual cursors. The first
 * section is sized as a multiple of a disk page boundary and is normally the only section that is
 * written. This section is divided up into a small header followed by blocks of cursors. Each block
 * contains partition count (specified in the constructor), cursors. Each cursor is assigned a fixed
 * offset in the first section that will not change. That way cursors can be updated in parallel and
 * the checksum is updated atomically.
 *
 * The second section contains the information necessary to reconstruct the cursor state after a
 * recovery. It contains the partitionIds and the offset of each partitionId in each block as well
 * as information about the name and offset (block) of the cursor in the first section. For the first
 * use case the name refers to the stream name that block of cursors is associated with. The second
 * section should only be rewritten when a block is added removed.
 */
public final class DurableCursor {
    private static final VoltLogger LOG = new VoltLogger("HOST");
    private final RateLimitedLogger m_failedUpdateLogger = new RateLimitedLogger(
            TimeUnit.MINUTES.toMillis(30), LOG, Level.INFO);
    private final File m_targetPath;
    private final long m_syncInterval;
    private boolean m_initialized = false;
    private SymmetricLongChecksum m_checksum = new SymmetricLongChecksum();
    private int m_currentSequence = 0;
    private int m_lastWrittenChecksum = 0;
    private ReadWriteLock m_checksumLock = new ReentrantReadWriteLock();
    SlotMapper m_slotMapper;
    private BBContainer m_cursorBuffer;
    private LongBuffer m_lb;
    DurabilityMarkerFile m_fileRef0 = null;
    DurabilityMarkerFile m_fileRef1 = null;

    static final int MIN_SUPPORTED_PAGE_SIZE = 4096;
    // Checksum of all slots to verify file integrity (4 Bytes), File sequence number (2 Bytes)
    // and offset in 4k byte pages to the slot mapper data in the file (2 Bytes)
    private static final int FILE_HEADER_SIZE = 8;
    private static final short FILE_VERSION = 1;
    static final int CURSOR_SIZE = 8;
    // slotMapBuffer needs a Buffer CRC(4), bufferSize including names(4), opaque size(4),
    // version(2), partitionCount(2), maxStreamCount(4), sparseStreamCount(4).
    private static final int FIXED_TRAILER_SPACE = 4 + 4 + 4 + 2 + 2 + 4 + 4;

    static {
        // Make sure cursors are word aligned so that updates to cursors stay on cache line boundaries
        assert(CURSOR_SIZE % 4 == 0);
    }

    static class DurabilityMarkerFile {
        final File m_path;
        final private FileDescriptor m_fd;
        final private FileChannel m_channel;
        final private boolean m_createdFile;

        DurabilityMarkerFile(File marker, boolean create) throws IOException {
            if (create) {
                marker.delete();
            }
            m_path = marker;
            RandomAccessFile ras;
            ras = new RandomAccessFile(m_path, "rw");
            m_channel = ras.getChannel();
            m_fd = ras.getFD();
            m_createdFile = m_channel.size() == 0;
        }

        void closeFile() {
            try {
                m_channel.close();
            }
            catch (IOException e) {
                LOG.warn("Unable to close Durable Cursor file " + m_path.getName());
            }
        }
    }

    static class SymmetricChecksum {
        final AtomicInteger m_checksum = new AtomicInteger(0);

        void initFromFile(int value) {
            m_checksum.set(value);
        }

        int get() {
            return m_checksum.get();
        }
    }

    static class SymmetricLongChecksum extends SymmetricChecksum {
        SymmetricLongChecksum() {
            super();
        }

        void swap(long oldVal, long newVal) {
            if (oldVal == newVal) {
                return;
            }
            int collapsedOld = (int) (oldVal & 0xffffffff);
            collapsedOld += ((oldVal >>> 32) & 0xffffffff);
            int collapsedNew = (int) (newVal & 0xffffffff);
            collapsedNew += ((newVal >>> 32) & 0xffffffff);
            m_checksum.addAndGet(collapsedNew-collapsedOld);
        }

        // Used to reset the slot after the stream is dropped
        void clearVal(long oldVal) {
            int collapsed = (int) (oldVal & 0xffffffff);
            collapsed += ((oldVal >>> 32) & 0xffffffff);
            m_checksum.addAndGet(-collapsed);
        }
    }

    /*
     * This class in effect maintains the relationship between a stream name and it's associated
     * cursor block. When changes to the block layout occur, this class is used to serialize the
     * update into the on disk files.
     */
    class SlotMapper {
        private final int m_slotBlockSize;
        private int m_cursorSpace;
        private int m_basePageCount;
        private final int m_opaqueDataSize;
        private int m_nonNamesTrailerSize;
        private int m_nameTrailerSize;
        int m_maxStreams;
        private boolean m_staleSlotMapper = true;
        private boolean m_staleMapBufferInOtherFile = false;
        private byte[] m_latestOpaque = null;
        private BBContainer m_slotMapBuffer;
        int m_sparseStreamCount;
        TreeMap<String, Integer> m_streamnameToSlotBlock = new TreeMap<>();
        TreeMap<Integer, String> m_slotBlockToStreamname = new TreeMap<>();
        Map<Integer, Integer> m_localPartitionIds;
        // BitSet is a good structure for small numbers of cursor blocks (streams).
        // Revisit this for many streams or managing streams on a per partition basis.
        BitSet m_freeSlots = new BitSet();

        SlotMapper(int reservedStreamCount, TreeSet<Integer> partitionIds, byte[] opaque) {
            m_slotBlockSize = CURSOR_SIZE * partitionIds.size();
            m_sparseStreamCount = 0;
            m_nameTrailerSize = 0;
            int offset = 0;
            ImmutableSortedMap.Builder<Integer, Integer> builder = ImmutableSortedMap.<Integer, Integer>naturalOrder();
            for (Integer pid: partitionIds) {
                builder.put(pid, offset++);
            }
            m_localPartitionIds = builder.build();
            m_latestOpaque = opaque;
            m_opaqueDataSize = opaque.length;
            updatePageTable(reservedStreamCount);
            m_freeSlots = new BitSet(m_maxStreams);
            // At a minimum the slotMapBuffer needs a FIXED_TRAILER_SPACE, opaque(?),
            // partitionList(2 * partitionCount), and name sizes.
            m_nonNamesTrailerSize = FIXED_TRAILER_SPACE + m_opaqueDataSize + (2 * m_localPartitionIds.size());
            // Dummy Buffer to deallocate after first file write
            m_slotMapBuffer = DBBPool.allocateDirect(8);
            updateSlotMapBuffer();
        }

        SlotMapper(BBContainer mapBuff, int cursorBufferSize) {
            ByteBuffer buff = mapBuff.b();
            buff.position(8);
            m_opaqueDataSize = buff.getInt();
            m_latestOpaque = new byte[m_opaqueDataSize];
            buff.get(m_latestOpaque);
            short fileVersion = buff.getShort();
            assert(fileVersion == FILE_VERSION);
            short partitionCount = buff.getShort();
            assert(buff.remaining() >= (partitionCount * 2) + 4);
            m_cursorSpace = cursorBufferSize;
            m_slotBlockSize = CURSOR_SIZE * partitionCount;
            int offset = 0;
            ImmutableSortedMap.Builder<Integer, Integer> builder = ImmutableSortedMap.<Integer, Integer>naturalOrder();
            for (int ii = 0; ii < partitionCount; ii++) {
                builder.put(new Integer(buff.getShort()), offset++);
            }
            m_localPartitionIds = builder.build();
            m_maxStreams = buff.getInt();
            m_freeSlots = new BitSet(m_maxStreams);
            m_sparseStreamCount = buff.getInt();
            assert((m_cursorSpace - FILE_HEADER_SIZE) / m_slotBlockSize == m_maxStreams);
            assert(buff.remaining() >= m_sparseStreamCount * 4);
            Map<Integer, Integer> slotsToLen = new TreeMap<>();
            for (int ii = 0; ii < m_sparseStreamCount; ii++) {
                Integer nameLen = new Integer(buff.getInt());
                if (nameLen == -1) {
                    m_freeSlots.set(ii);
                }
                else {
                    slotsToLen.put(ii, nameLen);
                }
            }
            m_nonNamesTrailerSize = buff.position();
            byte[] name = new byte[1024];
            for (Entry<Integer, Integer> e : slotsToLen.entrySet()) {
                Integer len = e.getValue();
                buff.get(name, 0, len);
                String cursorName = new String(name, 0, len, Constants.UTF8ENCODING);
                m_streamnameToSlotBlock.put(cursorName, e.getKey());
                m_slotBlockToStreamname.put(e.getKey(), cursorName);
            }
            buff.position(0);
            m_slotMapBuffer = mapBuff;
        }

        private void updateSlotMapBuffer() {
            assert(m_latestOpaque.length == m_opaqueDataSize);
            BBContainer cont = DBBPool.allocateDirect(m_nonNamesTrailerSize + m_nameTrailerSize);
            ByteBuffer buff = cont.b();
            buff.putInt(0); // CRC
            buff.putInt(0); // total size
            buff.putInt(m_opaqueDataSize);
            buff.put(m_latestOpaque);
            buff.putShort(FILE_VERSION);
            buff.putShort((short)m_localPartitionIds.size());
            for (int pid : m_localPartitionIds.keySet()) {
                buff.putShort((short)pid);
            }
            buff.putInt(m_maxStreams);
            buff.putInt(m_sparseStreamCount);
            int nextFreeSlot = m_freeSlots.nextSetBit(0);
            for (int ii=0; ii < m_sparseStreamCount; ii++) {
                if(ii == nextFreeSlot) {
                    nextFreeSlot = m_freeSlots.nextSetBit(nextFreeSlot+1);
                    buff.putInt(-1);
                }
                else {
                    buff.putInt(m_slotBlockToStreamname.get(ii).length());
                }
            }
            assert(buff.position() == m_nonNamesTrailerSize);
            for (String streamname : m_slotBlockToStreamname.values()) {
                buff.put(streamname.getBytes(Constants.UTF8ENCODING));
            }
            assert(buff.position() == buff.limit());
            applyCRC(buff);
            BBContainer oldBuff = m_slotMapBuffer;
            m_slotMapBuffer = cont;
            oldBuff.discard();
        }

        private void updatePageTable(int streamCount) {
            int pages = Bits.numPages(streamCount * m_slotBlockSize + FILE_HEADER_SIZE);
            // 8 bytes at beginning of the file is offset to simple checksum (4), SeqNo (2) and pages (2)
            assert(pages * Bits.pageSize() > m_cursorSpace);
            m_cursorSpace = pages * Bits.pageSize();

            // Multiples of 4096
            m_basePageCount = m_cursorSpace / MIN_SUPPORTED_PAGE_SIZE;
            m_maxStreams = (m_cursorSpace - FILE_HEADER_SIZE) / m_slotBlockSize;
        }

        void applyCRC(ByteBuffer buff) {
            CRC32 crc = new CRC32();
            int bufferSize = buff.position();
            buff.limit(bufferSize);
            buff.position(4);
            buff.putInt(4, bufferSize);
            crc.reset();
            crc.update(buff);
            buff.position(0);
            buff.putInt(0, (int)crc.getValue());
        }

        boolean addStream(String streamName, byte[] opaque) {
            boolean expandedCursorBuffer = false;
            Integer block;
            block = m_streamnameToSlotBlock.get(streamName);
            if (block == null) {
                m_nameTrailerSize += streamName.length();

                if (!m_freeSlots.isEmpty()) {
                    block = m_freeSlots.nextSetBit(0);
                    m_freeSlots.clear(block);
                }
                else {
                    block = new Integer(m_sparseStreamCount++);
                    // Account for additional name length field
                    m_nonNamesTrailerSize += 4;
                    if (m_sparseStreamCount > m_maxStreams) {
                        updatePageTable(m_sparseStreamCount);
                        expandedCursorBuffer = true;
                    }
                }
                m_streamnameToSlotBlock.put(streamName, block);
                m_slotBlockToStreamname.put(block, streamName);
                m_staleSlotMapper = true;
            }
            m_latestOpaque = opaque;
            return expandedCursorBuffer;
        }

        void dropStream(String streamName, byte[] opaque) {
            Integer block = m_streamnameToSlotBlock.remove(streamName);
            if (block != null) {
                m_slotBlockToStreamname.remove(block);
                m_nameTrailerSize -= streamName.length();
                m_freeSlots.set(block);
            }
            m_latestOpaque = opaque;
            m_staleSlotMapper = true;
        }

        Integer streamNameToBlockNum(String streamName) {
            return m_streamnameToSlotBlock.get(streamName);
        }
        int blockOffset(int blockNum) {
            return blockNum*m_localPartitionIds.size();
        }

        boolean haveStreamName(String streamName) {
            return m_streamnameToSlotBlock.get(streamName) != null;
        }

        int slotForBlockId(int partitionId, String streamName) {
            Integer partitionOffset = m_localPartitionIds.get(partitionId);
            if (partitionOffset == null) {
                return -1;
            }
            Integer blockNum = streamNameToBlockNum(streamName);
            if (blockNum == null) {
                return -1;
            }

            // There is a 8 byte header at the beginning of the cursor section so add 1
            return blockOffset(blockNum) + partitionOffset + 1;
        }

        void writeSlotBuffer(DurabilityMarkerFile dmf) {
            if (m_staleSlotMapper || m_staleMapBufferInOtherFile) {
                if (m_staleSlotMapper) {
                    updateSlotMapBuffer();
                    m_staleSlotMapper = false;
                    // Make sure the other file's slotMapBuffer gets written too.
                    m_staleMapBufferInOtherFile = true;
                }
                else {
                    m_staleMapBufferInOtherFile = false;
                }
                try {
                    dmf.m_channel.write(m_slotMapBuffer.b(), m_cursorSpace);
                }
                catch (IOException e) {
                    m_failedUpdateLogger.log("Failed to write to Durable Cursor file " + dmf.m_path +
                            ". Cursors will not be remembered after a Cluster Recovery. " +
                            "This message is rate limited to once every five minutes.",
                            System.currentTimeMillis());
                }
                m_slotMapBuffer.b().position(0);
            }
        }

        void shutdown() {
            m_slotMapBuffer.discard();
            m_slotMapBuffer = null;
        }

        private int getStreamCount() {
            return m_slotBlockToStreamname.size();
        }
    }

    private final Object m_writeThreadCondition = new Object();

    void writeNextSequencedFile() throws IOException {
        DurabilityMarkerFile target;
        if ((m_currentSequence & 1) == 1) {
            target = m_fileRef1;
        }
        else {
            target = m_fileRef0;
        }
        writeFile(target);
        try {
            long retval =
                    PosixAdvise.fadvise(target.m_fd,
                                        0,
                                        target.m_channel.size(),
                                        PosixAdvise.POSIX_FADV_DONTNEED);
            if (retval != 0) {
                m_writeCollisionLogger.log("Failed to fadvise DONTNEED completed Export Cursor file, this is harmless: " + retval,
                        System.currentTimeMillis());
            }
        } catch (Throwable t) {
            VoltDB.crashLocalVoltDB("Unexpected exception in Export's Durable Cursor file write",
                    true, t);
        }
    }

    private final Thread m_periodicWriteThread = new Thread(new Runnable() {
        @Override
        public void run() {
            try {
                long lastWriteTime = System.nanoTime();
                do {
                    long currentTime = System.nanoTime();
                    try {
                        synchronized (m_writeThreadCondition) {
                            long timeout = TimeUnit.NANOSECONDS.toMillis(
                                    m_syncInterval + lastWriteTime - currentTime);
                            if (timeout > 0) {
                                m_writeThreadCondition.wait(timeout);
                            }
                        }
                    } catch (InterruptedException e) {
                        return;
                    }
                    if (m_initialized) {
                        writeNextSequencedFile();
                    }
                    lastWriteTime = currentTime;
                } while (true);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unexpected exception in Export's Durable Cursor write thread", true, e);
            }

        }
    }, "Export Cursor Sync Scheduler");

    private static class CursorScanResult {
        final BBContainer m_cursorBuff;
        final BBContainer m_mapBuff;
        final int m_seqNum;
        CursorScanResult(BBContainer cursorBuff, BBContainer mapBuff, int seqNum) {
            m_cursorBuff = cursorBuff;
            m_mapBuff = mapBuff;
            m_seqNum = seqNum;
        }
    }

    // returns the sequence number of the file if valid and -1 if the file is corrupt
    private static CursorScanResult ValidateCursorFile(FileChannel ch) throws IOException {
        if (ch.size() < FILE_HEADER_SIZE) {
            return new CursorScanResult(null, null, -1);
        }
        ByteBuffer buff = ByteBuffer.allocate(FILE_HEADER_SIZE);
        ch.read(buff, 0);
        SymmetricLongChecksum checksum = new SymmetricLongChecksum();
        buff.position(0);
        int cs = buff.getInt();
        int seqNum = buff.getShort() & 0xFFFF;
        int basePageCount = buff.getShort() & 0xFFFF;
        buff.position(0);
        int trailerOffset = basePageCount * MIN_SUPPORTED_PAGE_SIZE;
        // include the header in the checksum
        int field = seqNum;
        field <<= 16;
        field |= basePageCount;
        cs -= field;
        checksum.initFromFile(cs);
        assert(trailerOffset % Bits.pageSize() == 0);
        if (ch.size() < trailerOffset) {
            return new CursorScanResult(null, null, -1);
        }
        BBContainer cursorBuff = DBBPool.allocateDirect(trailerOffset);
        ByteBuffer cur = cursorBuff.b();
        cur.put(buff);
        ch.read(cur, FILE_HEADER_SIZE);
        cur.position(FILE_HEADER_SIZE);
        // We only know how to handle 8 byte cursors right now.
        int onlySupportedCursorSize = 8;
        assert(CURSOR_SIZE == onlySupportedCursorSize);
        while (cur.hasRemaining()) {
            long nextCursor = cur.getLong();
            checksum.clearVal(nextCursor);
        }
        if (checksum.get() != 0) {
            cursorBuff.discard();
            return new CursorScanResult(null, null, -1);
        }

        if ((int)ch.size() - trailerOffset <= FIXED_TRAILER_SPACE) {
            cursorBuff.discard();
            return new CursorScanResult(null, null, -1);
        }
        BBContainer mapBuff = DBBPool.allocateDirect((int)ch.size() - trailerOffset);
        ByteBuffer b = mapBuff.b();
        ch.read(b, trailerOffset);
        b.position(0);
        int trailerCRC = b.getInt();
        CRC32 crc = new CRC32();
        crc.reset();
        crc.update(b);
        if ((int)crc.getValue() != trailerCRC) {
            cursorBuff.discard();
            mapBuff.discard();
            return new CursorScanResult(null, null, -1);
        }
        cursorBuff.b().position(0);
        return new CursorScanResult(cursorBuff, mapBuff, seqNum);
    }

    // Create and Rejoin constructor (create new cursor files)
    public DurableCursor(long syncInterval, File targetPath, String targetPrefix,
            int cursorsPerPartition, TreeSet<Integer> partitionIds, byte[] opaque) throws IOException {
        assert(Bits.pageSize() % MIN_SUPPORTED_PAGE_SIZE == 0);
        m_syncInterval = TimeUnit.MILLISECONDS.toNanos(syncInterval);
        m_targetPath = targetPath;
        m_slotMapper = new SlotMapper(cursorsPerPartition, partitionIds, opaque);
        m_cursorBuffer = DBBPool.allocateDirect(m_slotMapper.m_cursorSpace);
        m_lb = m_cursorBuffer.bD().asLongBuffer();
        try {
            m_fileRef0 = new DurabilityMarkerFile(new VoltFile(m_targetPath, targetPrefix + "_0"), true);
            m_fileRef1 = new DurabilityMarkerFile(new VoltFile(m_targetPath, targetPrefix + "_1"), true);
        }
        catch (IOException e) {
            m_cursorBuffer.discard();
            m_slotMapper.shutdown();
            m_slotMapper = null;
            throw e;
        }
        m_initialized = true;
    }

    private DurableCursor(CursorScanResult buffers, File targetPath) {
        assert(Bits.pageSize() % MIN_SUPPORTED_PAGE_SIZE == 0);
        m_targetPath = targetPath;
        m_syncInterval = -1;
        m_cursorBuffer = buffers.m_cursorBuff;
        m_slotMapper = new SlotMapper(buffers.m_mapBuff, m_cursorBuffer.b().remaining());
        m_cursorBuffer.b().position(0);
        m_lb = m_cursorBuffer.bD().asLongBuffer();
        m_currentSequence = buffers.m_seqNum;
    }

    // Recover path (validate existing cursor files)
    static DurableCursor RecoverBestCursor(File target, String targetPrefix) throws IOException {
        DurabilityMarkerFile dmf0 = new DurabilityMarkerFile(new VoltFile(target, targetPrefix + "_0"), false);
        DurabilityMarkerFile dmf1 = new DurabilityMarkerFile(new VoltFile(target, targetPrefix + "_1"), false);
        CursorScanResult dmf0Result;
        CursorScanResult dmf1Result;
        if (!dmf0.m_createdFile) {
            dmf0Result = ValidateCursorFile(dmf0.m_channel);
        }
        else {
            dmf0Result = new CursorScanResult(null, null, -1);
        }
        if (!dmf1.m_createdFile) {
            dmf1Result = ValidateCursorFile(dmf1.m_channel);
        }
        else {
            dmf1Result = new CursorScanResult(null, null, -1);
        }
        // if dmf0 == -1, any value of dmf1 will be less then 0
        // if dmf0 is positive and dmf1 == -1 or dmf1 < dmf0, dmf0 will be used
        if (dmf0Result.m_seqNum - dmf1Result.m_seqNum > 0) {
            if (dmf1Result.m_seqNum >= 0) {
                dmf1Result.m_cursorBuff.discard();
                dmf1Result.m_mapBuff.discard();
            }
            return new DurableCursor(dmf0Result, target);
        }
        // if dmf1 == -1, any value of dmf0 will be less then 0
        // if dmf1 is positive and dmf0 == -1 or dmf0 < dmf1, dmf1 will be used
        else if (dmf1Result.m_seqNum - dmf0Result.m_seqNum > 0) {
            if (dmf0Result.m_seqNum >= 0) {
                dmf0Result.m_cursorBuff.discard();
                dmf0Result.m_mapBuff.discard();
            }
            return new DurableCursor(dmf1Result, target);
        }
        else {
            assert(dmf0Result.m_seqNum == -1);
            // no good file found
            return null;
        }
    }

    private final RateLimitedLogger m_writeCollisionLogger =  new RateLimitedLogger(TimeUnit.MINUTES.toMillis(5), LOG, Level.INFO);

    // Returns a BlockId for each new stream. The actual slot is specified by calling
    // slotForBlocId() with a PartitionId and BlockId
    public void addCursorBlock(String streamName, byte[] opaque) {
        try {
            m_checksumLock.writeLock().lock();
            boolean expandCursorBuff = m_slotMapper.addStream(streamName, opaque);
            if (expandCursorBuff) {
                // update the cursor page
                BBContainer newCursorBuff = DBBPool.allocateDirect(m_slotMapper.m_cursorSpace);
                ByteBuffer b = m_cursorBuffer.b();
                assert(b.position() == 0);
                newCursorBuff.b().put(b);
                newCursorBuff.b().position(0);
                m_lb = newCursorBuff.bD().asLongBuffer();
                m_cursorBuffer.discard();
                m_cursorBuffer = newCursorBuff;
            }
        }
        finally {
            m_checksumLock.writeLock().unlock();
        }
    }

    public void removeCursorBlock(String streamName, byte[] opaque) {
        try {
            m_checksumLock.writeLock().lock();
            m_slotMapper.dropStream(streamName, opaque);
        }
        finally {
            m_checksumLock.writeLock().unlock();
        }
    }

    public boolean haveStreamName(String streamName) {
        return m_slotMapper.haveStreamName(streamName);
    }

    public int slotForBlockId(int partitionId, String streamName) {
       int offset = m_slotMapper.slotForBlockId(partitionId, streamName);
       assert(offset != -1);
       return offset;
    }

    public void updateCursor(int slot, long cursor) {
        try {
            m_checksumLock.readLock().lock();
            long oldCursor = m_lb.get(slot);
            m_lb.put(slot, cursor);
            m_checksum.swap(oldCursor, cursor);
        }
        finally {
            // We may want to add a catch block that recalculates the checksum from scratch
            m_checksumLock.readLock().unlock();
        }
    }

    public int getStreamCount() {
        return m_slotMapper.getStreamCount();
    }

    public Map<Integer, Long> getCursors(String streamName) {
        // This should only be called to find the old cursors and send out acks after a recover
        assert(m_fileRef0 == null);
        Map<Integer, Long> result = new TreeMap<>();
        Integer blockNum = m_slotMapper.streamNameToBlockNum(streamName);
        if (blockNum != null) {
            // add 1 long for the buffer header of 8 bytes
            int blockOffset = m_slotMapper.blockOffset(blockNum) + 1;
            for(Integer pid : m_slotMapper.m_localPartitionIds.keySet()) {
                result.put(pid, m_lb.get(blockOffset));
                blockOffset++;
            }
        }
        return result;
    }

    void writeFile(DurabilityMarkerFile dmf) throws IOException {
        m_checksumLock.writeLock().lock();
        ByteBuffer cursorBuf = m_cursorBuffer.b();
        cursorBuf.putShort(4, (short)m_currentSequence);
        cursorBuf.putShort(6, (short)m_slotMapper.m_basePageCount);
        int checksum = m_checksum.get();
        m_lastWrittenChecksum = checksum;
        int field = m_currentSequence;
        field <<= 16;
        field |= m_slotMapper.m_basePageCount;
        checksum += field;
        cursorBuf.putInt(0, checksum);
        m_currentSequence = m_currentSequence + 1 & 0xFFFF;
        assert(cursorBuf.position() == 0);
        try {
            dmf.m_channel.write(cursorBuf, 0);
        }
        catch (IOException e) {
            m_failedUpdateLogger.log("Failed to write to Durable Cursor file " + dmf.m_path +
                    ". Cursors will not be remembered after a Cluster Recovery. " +
                    "This message is rate limited to once every five minutes.",
                    System.currentTimeMillis());
        }
        cursorBuf.position(0);
        m_slotMapper.writeSlotBuffer(dmf);
        m_checksumLock.writeLock().unlock();
    }

    public void startupScheduledWriter() {
        m_periodicWriteThread.start();
    }

    public void shutdown() throws InterruptedException {
        if (m_periodicWriteThread.getState() != Thread.State.NEW) {
            m_periodicWriteThread.interrupt();
            m_periodicWriteThread.join();
        }
        if (m_checksum.get() != m_lastWrittenChecksum && m_fileRef0.m_channel.isOpen() && m_fileRef1.m_channel.isOpen()) {
            try {
                writeNextSequencedFile();
            }
            catch (IOException e) {
                VoltDB.crashLocalVoltDB("Unexpected exception in Export's DurableCursor Shutdown", true, e);
            }
        }
        if (m_fileRef0 != null) {
            m_fileRef0.closeFile();
        }
        if (m_fileRef1 != null) {
            m_fileRef1.closeFile();
        }
        //Release io buffer memory
        if (m_slotMapper != null) {
            m_slotMapper.shutdown();
            m_slotMapper = null;
            m_cursorBuffer.discard();
        }
    }

    public int getCurrentSequence() {
        return m_currentSequence;
    }
}
