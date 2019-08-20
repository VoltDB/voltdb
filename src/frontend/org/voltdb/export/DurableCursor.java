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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.zip.CRC32;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.apache.zookeeper_voltpatches.AsyncCallback;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Bits;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DBBPool.RefCountedDirectBBContainer;
import org.voltcore.utils.InstanceId;
import org.voltcore.utils.LatencyWatchdog;
import org.voltcore.utils.Pair;
import org.voltcore.utils.RateLimitedLogger;
import org.voltdb.VoltDB;
import org.voltdb.common.Constants;
import org.voltdb.iv2.TransactionTask;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.utils.PosixAdvise;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Stopwatch;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.ImmutableSortedMap;
import com.google_voltpatches.common.collect.ImmutableSortedSet;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

import vanilla.java.affinity.impl.PosixJNAAffinity;

public final class DurableCursor {
    private static final VoltLogger log = new VoltLogger("HOST");

    private final File m_targetPath;
    private final long m_syncInterval;
    private boolean m_initialized = false;
    private int m_checksum = 0;
    private int m_currentWriter = 0;
    private ReadWriteLock m_checksumLock = new ReentrantReadWriteLock();
    private SlotMapper m_slotManager;
    private DurabilityMarkerFile m0;
    private DurabilityMarkerFile m1;

    private static final int MIN_SUPPORTED_PAGE_SIZE = 1024;
    // Checksum of all slots to verify file integrity (4 Bytes), File sequence number (2 Bytes)
    // and offset in 4k byte pages to the slot mapper data in the file (2 Bytes)
    private static final int FILE_HEADER_SIZE = 8;
    private static final short FILE_VERSION = 1;
    private static final int CURSOR_SIZE = 8;

    private static class DurabilityMarkerFile {
        private File m_durabilityMarker;
        private FileDescriptor m_fd;
        private FileChannel m_channel;
        private boolean m_createdFile;
        private volatile boolean m_staleSlotMapper = true;

        DurabilityMarkerFile(File marker) throws IOException {
            if (!marker.isDirectory()) {
                throw new IOException("Path " + marker + " does not exist");
            }
            if (!marker.canRead()) {
                throw new IOException("Path " + marker + " is not readable");
            }
            if (!marker.canWrite()) {
                throw new IOException("Path " + marker + " is not writable");
            }
            if (!marker.canExecute()) {
                throw new IOException("Path " + marker + " is not executable");
            }
            m_durabilityMarker = marker;
            RandomAccessFile ras;
            ras = new RandomAccessFile(m_durabilityMarker, "rw");
            m_channel = ras.getChannel();
            m_fd = ras.getFD();
            m_createdFile = m_channel.size() == 0;
        }

        void writeSlots() {
            if (m_staleSlotMapper) {

            }
        }

        void markSlotMapperStale() {
            m_staleSlotMapper = true;
        }

        void closeFile() {
            try {
                m_channel.close();
            }
            catch (IOException e) {
            }
        }
    }

    interface SymmetricChecksum {
        final AtomicInteger m_checksum = new AtomicInteger(0);

        default void initFromFile(int value) {
            m_checksum.set(value);
        }

        int get();
    }

    static class SymmetricLongChecksum implements SymmetricChecksum {

        void swap(long oldVal, long newVal) {
            long diff = newVal-oldVal;
            if (diff == 0) {
                return;
            }
            assert(diff > 0 && diff < (1 << 31));
            int collapsed = (int) (diff & 0xffffffff);
            m_checksum.addAndGet(collapsed);
        }

        // Used to reset the slot after the stream is dropped
        void clearVal(long oldVal) {
            int collapsed = (int) (oldVal & 0xffffffff);
            collapsed += ((oldVal >>> 32) & 0xffffffff);
            m_checksum.addAndGet(-collapsed);
        }

        @Override
        public int get() {
            return m_checksum.get();
        }
    }

    private class SlotMapper {
        private final int m_slotBlockSize;
        private int m_cursorSpace;
        private int m_basePageCount;
        private final int m_opaqueDataSize;
        private int m_nonNamesTrailerSize;
        private int m_nameTrailerSize;
        int m_maxStreams;
        volatile RefCountedDirectBBContainer m_slotMapBuffer;
        int m_sparseStreamCount;
        TreeMap<String, Integer> m_streamnameToSlotBlock = new TreeMap<>();
        TreeMap<Integer, String> m_slotBlockToStreamname = new TreeMap<>();
        Set<Integer> m_localPartitionIds;
        TreeSet<Integer> m_freeSlots = new TreeSet<>();

        SlotMapper(int streamCount, Set<Integer> partitionIds, int opaqueDataSize) {
            assert(CURSOR_SIZE % 4 == 0);
            m_slotBlockSize = CURSOR_SIZE * partitionIds.size();
            updatePageTable(streamCount);
            m_sparseStreamCount = 0;
            m_nameTrailerSize = 0;
            m_localPartitionIds = ImmutableSortedSet.copyOf(partitionIds);
            m_opaqueDataSize = opaqueDataSize;

            // At a minimum the slotMapBuffer needs a Buffer CRC(4), bufferSize including names(4), opaque(4 + ?),
            // version(2), partitionCount(2), partitionList(2 * partitionCount), maxStreamCount(4) and name sizes.
            m_nonNamesTrailerSize = 4 + 4 + 4 + opaqueDataSize + 2 + (2 * partitionIds.size()) + 4 + (m_maxStreams * 4);
        }

        SlotMapper(RefCountedDirectBBContainer mapBuff) {
            ByteBuffer buff = mapBuff.b();
            buff.position(8);
            m_opaqueDataSize = buff.getInt();
            buff.position(buff.position() + m_opaqueDataSize);
            short fileVersion = buff.getShort();
            assert(fileVersion == FILE_VERSION);
            short partitionCount = buff.getShort();
            m_slotBlockSize = CURSOR_SIZE * partitionCount;
            ArrayList<Integer> partitions = new ArrayList<>();
            for (int ii = 0; ii < partitionCount; ii++) {
                partitions.add(new Integer(buff.getShort()));
            }
            m_localPartitionIds = ImmutableSortedSet.copyOf(partitions);
            m_maxStreams = buff.getInt();
            Map<Integer, Integer> slotsToLen = new TreeMap<>();
            for (int ii = 0; ii < m_maxStreams; ii++) {
                Integer nameLen = new Integer(buff.getInt());
                if (nameLen == -1) {
                    m_freeSlots.add(ii);
                }
                else {
                    slotsToLen.put(ii, nameLen);
                }
            }
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

        void updateSlotMapBuffer(byte[] opaque) {
            assert(opaque.length == m_opaqueDataSize);
            RefCountedDirectBBContainer cont = DBBPool.allocateRefCountedDirect(m_nonNamesTrailerSize + m_nameTrailerSize);
            ByteBuffer buff = cont.b();
            buff.putInt(0); // CRC
            buff.putInt(0); // total size
            buff.putInt(opaque.length);
            buff.put(opaque);
            buff.putShort(FILE_VERSION);
            buff.putShort((short)m_localPartitionIds.size());
            for (int pid : m_localPartitionIds) {
                buff.putShort((short)pid);
            }
            buff.putInt(m_maxStreams);
            Iterator<Integer> nextFree = m_freeSlots.iterator();
            Integer nextFreeSlot = nextFree.hasNext() ? nextFree.next() : new Integer(-1);
            for (int ii=0; ii < m_maxStreams; ii++) {
                if (ii == nextFreeSlot.intValue()) {
                    nextFreeSlot = nextFree.hasNext() ? nextFree.next() : new Integer(-1);
                    buff.putInt(0);
                }
                else {
                    buff.putInt(m_slotBlockToStreamname.get(ii).length());
                }
            }
            for (String streamname : m_slotBlockToStreamname.values()) {
                buff.put(streamname.getBytes(Constants.UTF8ENCODING));
            }
            assert(buff.position() == buff.limit());
            applyCRC(buff);
            RefCountedDirectBBContainer oldBuff = m_slotMapBuffer;
            m_slotMapBuffer = cont;
            oldBuff.release();
        }

        private void updatePageTable(int streamCount) {
            int pages = Bits.numPages(streamCount * m_slotBlockSize + FILE_HEADER_SIZE);
            // 8 bytes at beginning of the file is offset to simple checksum (4), SeqNo (2) and pages (2)
            m_cursorSpace = pages * Bits.pageSize();
            // Multiples of 4096
            m_basePageCount = m_cursorSpace / 4064;
            m_maxStreams = m_cursorSpace - FILE_HEADER_SIZE / m_slotBlockSize;
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

        void increasePageCount(int streamCount) {
            updatePageTable(streamCount);
        }

        int addStream(String streamName) {
            Integer block;
            m_nameTrailerSize += streamName.length();
            block = m_streamnameToSlotBlock.get(streamName);
            if (block != null) {
                // streamName already added by another partition
                return block;
            }
            if (!m_freeSlots.isEmpty()) {
                block = m_freeSlots.pollFirst();
            }
            else {
                block = new Integer(m_sparseStreamCount++);
                if (m_sparseStreamCount >= m_maxStreams) {
                    increasePageCount(m_sparseStreamCount);
                }
            }
            m_streamnameToSlotBlock.put(streamName, block);
            m_slotBlockToStreamname.put(block, streamName);
            return block;
        }

        void dropStream(String streamName) {
            Integer block = m_streamnameToSlotBlock.remove(streamName);
            assert(block != null);
            m_slotBlockToStreamname.remove(block);
            m_nameTrailerSize -= streamName.length();
            m_freeSlots.add(block);
        }

        void writeSlotBuffer(FileChannel channel) {
            RefCountedDirectBBContainer targetBuff;
            do {
                targetBuff = m_slotMapBuffer;
            } while (!targetBuff.acquire());
            try {
                channel.write(targetBuff.b(), m_cursorSpace);
                targetBuff.release();
            }
            catch (IOException e) {
            }
        }

        void shutdown() {
            m_slotMapBuffer.discard();
            assert(!m_slotMapBuffer.acquire());
            m_slotMapBuffer = null;
        }
    }

    private final Object m_writeThreadCondition = new Object();

    private final Thread m_periodicWriteThread = new Thread(new Runnable() {
        @Override
        public void run() {
            try {
                long lastCurrentTime = System.currentTimeMillis();
                do {
                    long currentTime = System.currentTimeMillis();
                    try {
                        synchronized (m_writeThreadCondition) {
                            m_writeThreadCondition.wait(m_syncInterval + lastCurrentTime - currentTime);
                        }
                    } catch (InterruptedException e) {
                        return;
                    }
                    if (m_initialized) {
                        if (m_currentWriter == 0) {
                            m_currentWriter = 1;
                        }
                        else {
                            m_currentWriter = 0;
                        }
                    }
                    lastCurrentTime = currentTime;
                } while (true);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unexpected exception in log sync thread", true, e);
            }

        }
    }, "Export Cursor Sync Scheduler");

    private static class CursorScanResult {
        final RefCountedDirectBBContainer m_cursorBuff;
        final RefCountedDirectBBContainer m_mapBuff;
        final int m_seqNum;
        CursorScanResult(RefCountedDirectBBContainer cursorBuff, RefCountedDirectBBContainer mapBuff, int seqNum) {
            m_cursorBuff = cursorBuff;
            m_mapBuff = mapBuff;
            m_seqNum = seqNum;
        }
    }

    // returns the sequence number of the file if valid and -1 if the file is corrupt
    private static CursorScanResult ValidCursorFile(FileChannel ch) throws IOException {
        ByteBuffer buff = ByteBuffer.allocate(FILE_HEADER_SIZE);
        ch.read(buff);
        SymmetricLongChecksum checksum = new SymmetricLongChecksum();
        checksum.initFromFile(buff.getInt());
        int seqNum = buff.getShort() & 0xFFFF;
        int trailerOffset = (buff.getShort() & 0xFFFF) * 4086;
        assert(trailerOffset % Bits.pageSize() == 0);
        RefCountedDirectBBContainer cursorBuff = DBBPool.allocateRefCountedDirect(trailerOffset - FILE_HEADER_SIZE);
        ByteBuffer cur = cursorBuff.b();
        ch.read(cur);
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

        assert(ch.position() == trailerOffset);
        RefCountedDirectBBContainer mapBuff = DBBPool.allocateRefCountedDirect((int)ch.size() - trailerOffset);
        ByteBuffer b = mapBuff.b();
        ch.read(b, trailerOffset);
        int trailerCRC = b.getInt();
        CRC32 crc = new CRC32();
        crc.reset();
        crc.update(b);
        if ((int)crc.getValue() != trailerCRC) {
            cursorBuff.discard();
            mapBuff.discard();
            return new CursorScanResult(null, null, -1);
        }
        return new CursorScanResult(cursorBuff, mapBuff, seqNum);
    }

    // Create and Rejoin constructor (create new cursor files)
    public DurableCursor(long syncInterval, String targetPath, String targetPrefix,
            int cursorsPerPartition, Set<Integer> partitionIds, byte[] opaque) {
        m_syncInterval = syncInterval;
        m_targetPath = new VoltFile(targetPath);
        m_slotManager = new SlotMapper(cursorsPerPartition, partitionIds, opaque.length);
        m_slotManager.updateSlotMapBuffer(opaque);
    }

    private DurableCursor(CursorScanResult buffers, File targetPath) {
        m_targetPath = targetPath;
        m_syncInterval = -1;
        m_slotManager = new SlotMapper(buffers.m_mapBuff);
    }

    // Recover path (validate existing cursor files)
    static DurableCursor RecoverBestCursor(String targetPath, String targetPrefix) throws IOException {
        File target = new VoltFile(targetPath);
        DurabilityMarkerFile dmf0 = new DurabilityMarkerFile(new VoltFile(target, targetPrefix + "_0"));
        DurabilityMarkerFile dmf1 = new DurabilityMarkerFile(new VoltFile(target, targetPrefix + "_1"));
        CursorScanResult dmf0Result;
        CursorScanResult dmf1Result;
        if (!dmf0.m_createdFile) {
            dmf0Result = ValidCursorFile(dmf0.m_channel);
        }
        else {
            dmf0Result = new CursorScanResult(null, null, -1);
        }
        if (!dmf1.m_createdFile) {
            dmf1Result = ValidCursorFile(dmf1.m_channel);
        }
        else {
            dmf1Result = new CursorScanResult(null, null, -1);
        }
        if (dmf0Result.m_seqNum > dmf1Result.m_seqNum) {
            return new DurableCursor(dmf0Result, target);
        }
        else if (dmf1Result.m_seqNum > dmf0Result.m_seqNum) {
            return new DurableCursor(dmf1Result, target);
        }
        else {
            assert(dmf0Result.m_seqNum == -1);
            // no good file found
            return null;
        }
    }

    private final RateLimitedLogger m_writeCollisionLogger =  new RateLimitedLogger(TimeUnit.MINUTES.toMillis(5), log, Level.INFO);

    public void shutdown() throws InterruptedException {
        if (m_periodicWriteThread.getState() != Thread.State.NEW) {
            m_periodicWriteThread.interrupt();
            m_periodicWriteThread.join();

            //Discard io buffer memory
            m_slotManager.shutdown();
        }
    }
}
