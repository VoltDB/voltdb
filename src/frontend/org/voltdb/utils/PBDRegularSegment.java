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

package org.voltdb.utils;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.zip.CRC32;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;

import com.google_voltpatches.common.base.Preconditions;

/**
 * Objects placed in the deque are stored in file segments that are up to 64 megabytes.
 * Segments only support appending objects. A segment will throw an IOException if an attempt
 * to insert an object that exceeds the remaining space is made. A segment can be used
 * for reading and writing, but not both at the same time.
 */
class PBDRegularSegment<M> extends PBDSegment<M> {
    private static final String TRUNCATOR_CURSOR = "__truncator__";
    private static final String SCANNER_CURSOR = "__scanner__";
    private static final int VERSION = 2;
    private static final Random RANDOM = new Random();

    private final Map<String, SegmentReader> m_readCursors = new HashMap<>();
    private final Map<String, SegmentReader> m_closedCursors = new HashMap<>();
    private final VoltLogger m_usageSpecificLog;

    private boolean m_closed = true;
    private FileChannelWrapper m_fc;
    // Avoid unnecessary sync with this flag
    private boolean m_syncedSinceLastEdit = true;
    // Reusable crc calculator. Must be reset before each use
    private final CRC32 m_crc;
    // Mirror of the isFinal metadata on the filesystem
    private boolean m_isFinal;
    // Whether or not this is the current active segment being written to
    private boolean m_isActive = false;

    private final BinaryDequeSerializer<M> m_extraHeaderSerializer;
    private M m_extraHeaderCache;

    private int m_numOfEntries = -1;
    private int m_size = -1;
    private boolean m_compress;
    private int m_segmentRandomId;
    private int m_extraHeaderSize = 0;
    // Not guaranteed to be valid unless m_extraHeaderSize > 0
    private int m_extraHeaderCrc = 0;

    private DBBPool.BBContainer m_segmentHeaderBuf = null;
    private DBBPool.BBContainer m_entryHeaderBuf = null;
    Boolean INJECT_PBD_CHECKSUM_ERROR = Boolean.getBoolean("INJECT_PBD_CHECKSUM_ERROR");

    PBDRegularSegment(long index, long id, File file, VoltLogger usageSpecificLog,
            BinaryDequeSerializer<M> extraHeaderSerializer) {
        super(file, index, id);
        m_crc = new CRC32();
        m_isFinal = PBDSegment.isFinal(m_file);
        m_usageSpecificLog = usageSpecificLog;
        m_extraHeaderSerializer = extraHeaderSerializer;
        reset();
    }

    private void reset() {
        m_syncedSinceLastEdit = false;
        if (m_segmentHeaderBuf != null) {
            m_segmentHeaderBuf.discard();
            m_segmentHeaderBuf = null;
        }
        if (m_entryHeaderBuf != null) {
            m_entryHeaderBuf.discard();
            m_entryHeaderBuf = null;
        }
    }

    @Override
    int getNumEntries() throws IOException
    {
        initializeFromHeader();
        return m_numOfEntries;
    }

    @Override
    boolean isBeingPolled()
    {
        return !m_readCursors.isEmpty();
    }

    @Override
    boolean isOpenForReading(String cursorId) {
        return m_readCursors.containsKey(cursorId);
    }

    @Override
    SegmentReader openForRead(String cursorId) throws IOException
    {
        Preconditions.checkNotNull(cursorId, "Reader id must be non-null");
        if (m_readCursors.containsKey(cursorId) || m_closedCursors.containsKey(cursorId)) {
            throw new IOException("Segment is already open for reading for cursor " + cursorId);
        }

        if (m_closed) {
            open(false, false, false);
        }
        SegmentReader reader = new SegmentReader(cursorId);
        m_readCursors.put(cursorId, reader);
        return reader;
    }

    @Override
    PBDSegmentReader<M> getReader(String cursorId) {
        PBDSegmentReader<M> reader = m_closedCursors.get(cursorId);
        return (reader == null) ? m_readCursors.get(cursorId) : reader;
    }

    @Override
    void openForTruncate() throws IOException {
        open(true, false, false);
    }

    @Override
    void openNewSegment(boolean compress) throws IOException {
        open(true, true, compress);
    }

    /**
     * Force a read and validation of the header and any extra header metadata which might exist
     *
     * @throws IOException If there was an error reading or validating the header
     */
    private void validateHeader() throws IOException {
        readHeader(true, true);
    }

    int getExtraHeaderSize() throws IOException {
        initializeFromHeader();
        return m_extraHeaderSize;
    }

    private void open(boolean forWrite, boolean emptyFile, boolean compress) throws IOException {
        if (!m_closed) {
            throw new IOException("Segment is already opened");
        }

        if (!m_file.exists()) {
            if (!forWrite) {
                throw new IOException("File " + m_file + " does not exist");
            }
            m_syncedSinceLastEdit = false;
        }
        assert (m_fc == null);
        m_fc = openFile(m_file, forWrite);
        m_segmentHeaderBuf = DBBPool.allocateDirect(SEGMENT_HEADER_BYTES);
        m_entryHeaderBuf = DBBPool.allocateDirect(ENTRY_HEADER_BYTES);

        // Those asserts ensure the file is opened with correct flag
        if (emptyFile) {
            setFinal(false);
            initNumEntries(0, 0);
            m_compress = compress;
            m_isActive = true;
            m_segmentRandomId = RANDOM.nextInt();
        }
        if (forWrite) {
            m_fc.position(m_fc.size());
        } else {
            m_fc.position(SEGMENT_HEADER_BYTES);
        }

        m_closed = false;
    }

    FileChannelWrapper openFile(File file, boolean forWrite) throws IOException {
        return new FileChannelWrapper(file, forWrite);
    }

    private void initializeFromHeader() throws IOException {
        if (m_numOfEntries != -1) {
            return;
        }
        readHeader(!isFinal(), false);
    }

    private void readHeader(boolean crcCheck, boolean skipInitialization) throws IOException {
        boolean wasClosed = false;
        if (m_closed) {
            wasClosed = true;
            open(false, false, false);
        }
        try {
            if (m_fc.size() >= SEGMENT_HEADER_BYTES) {
                ByteBuffer b = m_segmentHeaderBuf.b();
                b.clear();
                PBDUtils.readBufferFully(m_fc, b, 0);
                int version = b.getInt();
                if (version != VERSION) {
                    String message = "File version incorrect. Detected version " + version + " requires version "
                            + VERSION + " in file " + m_file.getName();
                    m_usageSpecificLog.warn(message);
                    throw new IOException(message);
                }

                int crc = b.getInt();
                int numOfEntries = b.getInt();
                int size = b.getInt();
                int segmentRandomId = b.getInt();
                int extraHeaderSize = b.getInt();
                int extraHeaderCrc = b.getInt();
                if (crcCheck) {
                    if (crc != calculateSegmentHeaderCrc(numOfEntries, size, segmentRandomId, extraHeaderSize,
                            extraHeaderCrc)) {
                        m_usageSpecificLog
                                .warn("File corruption detected in " + m_file.getName() + ": invalid file header. ");
                        throw new IOException(
                                "File corruption detected in " + m_file.getName() + ": invalid file header.");
                    }
                    if (extraHeaderSize > 0) {
                        BBContainer extraHeader = DBBPool.allocateDirect(extraHeaderSize);
                        try {
                            PBDUtils.readBufferFully(m_fc, extraHeader.b(), HEADER_EXTRA_HEADER_OFFSET);
                            if (extraHeaderCrc != calculateExtraHeaderCrc(extraHeader.b())) {
                                String message = "File corruption deteced in " + m_file.getName()
                                        + ": invalid extended file header";
                                m_usageSpecificLog.warn(message);
                                throw new IOException(message);
                            }
                        } finally {
                            extraHeader.discard();
                        }
                    }
                }
                if (skipInitialization) {
                    return;
                }
                m_numOfEntries = numOfEntries;
                m_size = size;
                m_segmentRandomId = segmentRandomId;
                m_extraHeaderSize = extraHeaderSize;
                m_extraHeaderCrc = extraHeaderCrc;
            } else {
                m_numOfEntries = 0;
                m_size = 0;
            }
        } finally {
            if (wasClosed) {
                closeReadersAndFile();
            }
        }
    }

    private void writeOutHeader() throws IOException {
        int crc = calculateSegmentHeaderCrc(m_numOfEntries, m_size, m_segmentRandomId, m_extraHeaderSize,
                m_extraHeaderCrc);

        ByteBuffer b = m_segmentHeaderBuf.b();
        b.clear();
        // the checksum here is really an unsigned int, store integer to save 4 bytes
        b.putInt(VERSION);
        b.putInt(crc);
        b.putInt(m_numOfEntries);
        b.putInt(m_size);
        b.putInt(m_segmentRandomId);
        b.putInt(m_extraHeaderSize);
        b.putInt(m_extraHeaderCrc);
        b.flip();
        PBDUtils.writeBuffer(m_fc, m_segmentHeaderBuf.bDR(), PBDSegment.HEADER_START_OFFSET);
        m_syncedSinceLastEdit = false;
    }

    private int calculateSegmentHeaderCrc(int numOfEntries, int size, int segmentRandomId, int extraHeaderSize,
            int extraHeaderCrc) {
        m_crc.reset();
        m_crc.update(VERSION);
        m_crc.update(numOfEntries);
        m_crc.update(size);
        m_crc.update(segmentRandomId);
        m_crc.update(extraHeaderSize);
        if (extraHeaderSize > 0) {
            m_crc.update(extraHeaderCrc);
        }
        return (int) m_crc.getValue();
    }

    private int calculateExtraHeaderCrc(ByteBuffer extraHeader) {
        m_crc.reset();
        m_crc.update(extraHeader);
        return (int) m_crc.getValue();
    }

    private void initNumEntries(int count, int size) throws IOException {
        m_numOfEntries = count;
        m_size = size;
        writeOutHeader();
    }

    @Override
    int parseAndTruncate(BinaryDeque.BinaryDequeTruncator truncator) throws IOException {
        if (!m_closed) {
            close();
        }
        openForTruncate();
        SegmentReader reader = openForRead(TRUNCATOR_CURSOR);

        // Do stuff
        validateHeader();
        final int initialEntryCount = getNumEntries();
        int entriesTruncated = 0;
        // Zero entry count means the segment is empty or corrupted, in both cases
        // the segment can be deleted.
        if (initialEntryCount == 0) {
            reader.close();
            close();
            return Integer.MAX_VALUE;
        }
        int sizeInBytes = 0;

        DBBPool.BBContainer cont;
        while (true) {
            final long beforePos = reader.readOffset();

            cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, true, !isFinal());
            if (cont == null) {
                break;
            }

            final int compressedLength = (int) (reader.readOffset() - beforePos - ENTRY_HEADER_BYTES);
            final int uncompressedLength = cont.b().limit();

            try {
                // Handoff the object to the truncator and await a decision
                BinaryDeque.TruncatorResponse retval = truncator.parse(cont);
                if (retval == null) {
                    // Nothing to do, leave the object alone and move to the next
                    sizeInBytes += uncompressedLength;
                } else {
                    // If the returned bytebuffer is empty, remove the object and truncate the file
                    if (retval.status == BinaryDeque.TruncatorResponse.Status.FULL_TRUNCATE) {
                        if (reader.readIndex() == 1) {
                            /*
                             * If truncation is occurring at the first object Whammo! Delete the file.
                             */
                            entriesTruncated = Integer.MAX_VALUE;
                        } else {
                            entriesTruncated = initialEntryCount - (reader.readIndex() - 1);

                            // Don't forget to update the number of entries in the file
                            initNumEntries(reader.readIndex() - 1, sizeInBytes);
                            m_fc.truncate(reader.readOffset() - (compressedLength + ENTRY_HEADER_BYTES));
                        }
                    } else {
                        assert retval.status == BinaryDeque.TruncatorResponse.Status.PARTIAL_TRUNCATE;
                        entriesTruncated = initialEntryCount - reader.readIndex();
                        // Partial object truncation
                        reader.rewindReadOffset(compressedLength + ENTRY_HEADER_BYTES);
                        final long partialEntryBeginOffset = reader.readOffset();
                        m_fc.position(partialEntryBeginOffset);

                        final int written = writeTruncatedEntry(retval, reader.readIndex());
                        sizeInBytes += written;
                        initNumEntries(reader.readIndex(), sizeInBytes);
                        m_fc.truncate(partialEntryBeginOffset + written + ENTRY_HEADER_BYTES);
                    }

                    break;
                }
            } finally {
                cont.discard();
            }
        }
        int entriesScanned = reader.readIndex();
        reader.close();

        if (entriesTruncated == 0) {
            int entriesNotScanned = initialEntryCount - entriesScanned;
            // If we checksum the file and it looks good, mark as final
            if (!isFinal() && entriesNotScanned == 0) {
                finalize(true);
            }
            return -entriesNotScanned;
        }

        close();

        return entriesTruncated;
    }

    @Override
    int scan(BinaryDeque.BinaryDequeScanner scanner) throws IOException {
        SegmentReader reader = openForRead(SCANNER_CURSOR);
        try {
            validateHeader();
            DBBPool.BBContainer cont = null;
            int initialEntryCount = getNumEntries();
            if (initialEntryCount == 0) {
                return 0;
            }
            while (true) {
                cont = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY, true, true);
                if (cont == null) {
                    break;
                }
                try {
                    scanner.scan(cont);
                } finally {
                    cont.discard();
                }
            }
            int entriesScanned = reader.readIndex();

            // Scan through entire file, everything looks good
            int entriesTruncated = initialEntryCount - entriesScanned;
            if (!m_isActive && !isFinal() && entriesTruncated == 0) {
                finalize(false);
            }

            return entriesTruncated;
        } finally {
            reader.purge();
        }
    }

    /**
     * Set or clear segment as 'final', i.e. whether segment is complete and logically immutable.
     *
     * NOTES:
     *
     * This is a best-effort feature: On any kind of I/O failure, the exception is swallowed and the operation is a
     * no-op: this will be the case on filesystems that do no support extended file attributes. Also note that the
     * {@code FileStore.supportsFileAttributeView} method does not provide a reliable way to test for the availability
     * of the extended file attributes.
     *
     * Must be called with 'true' by segment owner when it has filled the segment, written all segment metadata, and
     * after it has either closed or sync'd the segment file.
     *
     * Must be called with 'false' whenever opening segment for writing new data.
     *
     * Note that all calls to 'setFinal' are done by the class owning the segment because the segment itself generally
     * lacks context to decide whether it's final or not.
     *
     * @param isFinal true if segment is set to final, false otherwise
     * @throws IOException
     */
    void setFinal(boolean isFinal) throws IOException {
        if (isFinal != m_isFinal) {
            if (PBDSegment.setFinal(m_file, isFinal)) {
                if (!isFinal) {
                    // It is dangerous to leave final on a segment so make sure the metadata is flushed
                    m_fc.force(true);
                }
            } else if (PBDSegment.isFinal(m_file) && !isFinal) {
                throw new IOException("Could not remove the final attribute from " + m_file.getName());
            }
            // It is OK for m_isFinal to be true when isFinal(File) returns false but not the other way
            m_isFinal = isFinal;
        }
    }

    @Override
    void finalize(boolean close) throws IOException {
        m_isActive = false;
        IOException exception = null;
        try {
            if (canBeFinalized()) {
                sync();
                setFinal(true);
            }
        } catch (IOException e) {
            exception = e;
        } finally {
            try {
                if (close) {
                    close();
                } else {
                    setReadOnly();
                }
            } catch (IOException e) {
                if (exception == null) {
                    exception = e;
                } else {
                    exception.addSuppressed(e);
                }
            }

            if (exception != null) {
                throw exception;
            }
        }
    }

    /**
     * Returns whether the file is final
     *
     * @see notes on {@code setFinal}
     * @return true if file is final, false otherwise
     */
    @Override
    boolean isFinal() {
        return m_isFinal;
    }

    private void incrementNumEntries(int size) throws IOException
    {
        m_numOfEntries++;
        m_size += size;
        writeOutHeader();
    }

    /**
     * Bytes of space available for inserting more entries
     * @return
     */
    private int remaining() throws IOException {
        //Subtract 12 for the crc, number of entries and size prefix
        return (int)(PBDSegment.CHUNK_SIZE - m_fc.position()) - SEGMENT_HEADER_BYTES;
    }

    @Override
    void closeAndDelete() throws IOException {
        try {
            close();
        } finally {
            m_file.delete();
        }

        m_numOfEntries = -1;
        m_size = -1;
    }

    @Override
    boolean isClosed()
    {
        return m_closed;
    }

    @Override
    void close() throws IOException {
        if (m_usageSpecificLog.isDebugEnabled()) {
            m_usageSpecificLog.debug("Close PBD Segment " + m_file.getName());
        }
        m_closedCursors.clear();
        closeReadersAndFile();
    }

    private void closeReadersAndFile() throws IOException {
        m_readCursors.clear();
        try {
            if (m_fc != null) {
                m_fc.close();
            }
        } finally {
            m_fc = null;
            m_closed = true;
            reset();
        }
    }

    @Override
    void setReadOnly() throws IOException {
        m_fc.reopen(false);
    }

    @Override
    void sync() throws IOException {
        if (m_closed) {
            throw new IOException("Segment closed");
        }
        if (!m_syncedSinceLastEdit) {
            m_fc.force(true);
        }
        m_syncedSinceLastEdit = true;
    }

    @Override
    boolean hasAllFinishedReading() throws IOException {
        if (m_closed) {
            throw new IOException("Segment closed");
        }

        if (m_readCursors.size() == 0) {
            return false;
        }

        for (SegmentReader reader : m_readCursors.values()) {
            if (reader.m_objectReadIndex < m_numOfEntries) {
                return false;
            }
        }

        return true;
    }

    // Used by Export path
    @Override
    boolean offer(DBBPool.BBContainer cont) throws IOException
    {
        if (m_closed) {
            throw new IOException("Segment closed");
        }
        final ByteBuffer buf = cont.b();
        final int remaining = buf.remaining();
        boolean compress = m_compress && remaining >= 32 && buf.isDirect();

        final int maxCompressedSize = (compress ? CompressionService.maxCompressedLength(remaining) : remaining) + ENTRY_HEADER_BYTES;
        if (remaining() < maxCompressedSize) {
            return false;
        }

        m_syncedSinceLastEdit = false;
        DBBPool.BBContainer destBuf = cont;
        try {
            m_entryHeaderBuf.b().clear();

            if (compress) {
                destBuf = DBBPool.allocateDirectAndPool(maxCompressedSize);
                final int compressedSize = CompressionService.compressBuffer(buf, destBuf.b());
                destBuf.b().limit(compressedSize);
                writeEntryHeader(destBuf.b(), PBDSegment.FLAG_COMPRESSED);
            } else {
                destBuf = cont;
                writeEntryHeader(destBuf.b(), PBDSegment.NO_FLAGS);
            }
            // Write entry header
            m_entryHeaderBuf.b().flip();
            while (m_entryHeaderBuf.b().hasRemaining()) {
                m_fc.write(m_entryHeaderBuf.b());
            }

            // Write entry
            destBuf.b().flip();
            while (destBuf.b().hasRemaining()) {
                m_fc.write(destBuf.b());
            }
            // Update segment header
            incrementNumEntries(remaining);
        } finally {
            destBuf.discard();
            if (compress) {
                cont.discard();
            }
        }

        return true;
    }

    // Used by DR path
    @Override
    int offer(DeferredSerialization ds) throws IOException
    {
        if (m_closed) {
            throw new IOException("closed");
        }
        final int fullSize = ds.getSerializedSize();
        if (remaining() < fullSize) {
            return -1;
        }

        m_syncedSinceLastEdit = false;
        DBBPool.BBContainer destBuf = DBBPool.allocateDirectAndPool(fullSize);

        try {
            m_entryHeaderBuf.b().clear();
            final int written = MiscUtils.writeDeferredSerialization(destBuf.b(), ds);
            destBuf.b().flip();
            // Write entry header
            writeEntryHeader(destBuf.b(), PBDSegment.NO_FLAGS);
            m_entryHeaderBuf.b().flip();
            while (m_entryHeaderBuf.b().hasRemaining()) {
                m_fc.write(m_entryHeaderBuf.b());
            }
            // Write entry
            destBuf.b().flip();
            while (destBuf.b().hasRemaining()) {
                m_fc.write(destBuf.b());
            }
            // Update segment header
            incrementNumEntries(written);
            return written;
        } finally {
            destBuf.discard();
        }
    }

    private void writeEntryHeader(ByteBuffer data, char flags) {
        PBDUtils.writeEntryHeader(m_crc, m_entryHeaderBuf.b(), data, m_segmentRandomId + m_numOfEntries + 1, flags);
    }

    @Override
    int size() {
        return m_size;
    }

    private int writeTruncatedEntry(BinaryDeque.TruncatorResponse entry, int entryNumber) throws IOException
    {
        int written = 0;
        final DBBPool.BBContainer partialCont =
                DBBPool.allocateDirect(ENTRY_HEADER_BYTES + entry.getTruncatedBuffSize());
        try {
            written += entry.writeTruncatedObject(partialCont.b(), m_segmentRandomId + entryNumber);
            partialCont.b().flip();

            while (partialCont.b().hasRemaining()) {
                m_fc.write(partialCont.b());
            }
        } finally {
            partialCont.discard();
        }
        return written;
    }

    @Override
    void writeExtraHeader(M extraHeader) throws IOException {
        if (!(m_numOfEntries == 0 && m_extraHeaderSize == 0)) {
            throw new IllegalStateException("Extra header must be written before any entries");
        }
        int size = m_extraHeaderSerializer.getMaxSize(extraHeader);
        DBBPool.BBContainer destBuf = DBBPool.allocateDirect(size);
        try {
            ByteBuffer b = destBuf.b();
            b.order(ByteOrder.LITTLE_ENDIAN);
            m_extraHeaderSerializer.write(extraHeader, b);
            b.flip();
            do {
                m_fc.write(b);
            } while (b.hasRemaining());
            b.flip();
            m_extraHeaderCrc = calculateExtraHeaderCrc(b);
            m_extraHeaderSize = b.position();
            m_extraHeaderCache = extraHeader;
        } finally {
            destBuf.discard();
        }
        writeOutHeader();
    }

    /**
     * @return {@code true} if this segment is eligible for finalization
     */
    private boolean canBeFinalized() {
        if (m_fc != null) {
            return m_fc.m_stable;
        }
        return false;
    }

    DBBPool.BBContainer readExtraHeader() throws IOException {
        if (m_closed) {
            throw new IOException("Closed");
        }

        if (m_extraHeaderSize == 0) {
            return null;
        }

        DBBPool.BBContainer schemaBuf = null;
        try {
            schemaBuf = DBBPool.allocateDirect(m_extraHeaderSize);
            PBDUtils.readBufferFully(m_fc, schemaBuf.b().order(ByteOrder.LITTLE_ENDIAN), HEADER_EXTRA_HEADER_OFFSET);
            return schemaBuf;
        } catch (Exception e) {
            if (schemaBuf != null) {
                schemaBuf.discard();
            }
            m_usageSpecificLog.error("Error reading extra header of file: " + m_file.getName(), e);
            return null;
        }
    }

    @Override
    M getExtraHeader() throws IOException {
        if (m_closed) {
            throw new IOException("Closed");
        }

        if (m_extraHeaderSize != 0 && m_extraHeaderCache == null) {
            BBContainer container = readExtraHeader();
            if (container != null) {
                try {
                    m_extraHeaderCache = m_extraHeaderSerializer.read(container.b());
                } finally {
                    container.discard();
                }
            }
        }

        return m_extraHeaderCache;
    }

    private class SegmentReader implements PBDSegmentReader<M> {
        private final String m_cursorId;
        private long m_readOffset;
        //Index of the next object to read, not an offset into the file
        private int m_objectReadIndex = 0;
        private int m_bytesRead = 0;
        private int m_discardCount = 0;
        private boolean m_readerClosed = false;
        private CRC32 m_crcReader = new CRC32();

        public SegmentReader(String cursorId) throws IOException {
            assert(cursorId != null);
            m_cursorId = cursorId;
            m_readOffset = SEGMENT_HEADER_BYTES + getExtraHeaderSize();
        }

        @Override
        public boolean hasMoreEntries() {
            return m_objectReadIndex < m_numOfEntries;
        }

        @Override
        public boolean anyReadAndDiscarded() {
            return m_discardCount > 0;
        }

        @Override
        public DBBPool.BBContainer poll(OutputContainerFactory factory) throws IOException {
            return poll(factory, false, false);
        }

        DBBPool.BBContainer poll(OutputContainerFactory factory, boolean canTruncate, boolean checkCrc)
                throws IOException {
            assert !checkCrc || canTruncate : "canTruncate must be true if checkCrc is true";
            if (m_readerClosed) {
                throw new IOException("Reader closed");
            }

            if (!hasMoreEntries()) {
                return null;
            }

            final long writePos = m_fc.position();
            m_fc.position(m_readOffset);

            try {
                //Get the length and size prefix and then read the object
                ByteBuffer b = m_entryHeaderBuf.b();
                b.clear();
                if (!read(b)) {
                    return null;
                }
                b.flip();
                final int entryCRC = b.getInt();
                final int length = b.getInt();
                final int entryId = b.getInt();
                final char flags = b.getChar();
                final boolean compressed = (flags & FLAG_COMPRESSED) != 0;
                final int uncompressedLen;

                if (length < 1 || length > PBDSegment.CHUNK_SIZE - PBDSegment.SEGMENT_HEADER_BYTES) {
                    handleCorruptHeader("File corruption detected in " + m_file.getName() + ": invalid entry length.",
                            canTruncate);
                    return null;
                }

                if (entryId != m_segmentRandomId + m_objectReadIndex + 1) {
                    handleCorruptHeader("File corruption detected in " + m_file.getName() + ": invalid entry id.",
                            canTruncate);
                    return null;
                }

                DBBPool.BBContainer retcont = null;
                try {
                    if (compressed) {
                        final DBBPool.BBContainer compressedBuf = DBBPool.allocateDirectAndPool(length);
                        try {
                            if (!fillBuffer(compressedBuf.b(), entryId, flags, entryCRC, checkCrc)) {
                                return null;
                            }

                            uncompressedLen = CompressionService.uncompressedLength(compressedBuf.bDR());
                            retcont = factory.getContainer(uncompressedLen);
                            retcont.b().limit(uncompressedLen);
                            CompressionService.decompressBuffer(compressedBuf.bDR(), retcont.b());
                        } finally {
                            compressedBuf.discard();
                        }
                    } else {
                        uncompressedLen = length;
                        retcont = factory.getContainer(length);
                        retcont.b().limit(length);

                        if (!fillBuffer(retcont.b(), entryId, flags, entryCRC, checkCrc)) {
                            retcont.discard();
                            return null;
                        }
                    }
                } catch (Throwable t) {
                    if (retcont != null) {
                        retcont.discard();
                    }
                    throw t;
                }

                m_bytesRead += uncompressedLen;
                m_objectReadIndex++;

                return new DBBPool.DBBDelegateContainer(retcont) {
                    private boolean m_discarded = false;

                    @Override
                    public void discard() {
                        checkDoubleFree();
                        if (m_discarded) {
                            m_usageSpecificLog.error("PBD Container discarded more than once");
                            return;
                        }

                        m_discarded = true;
                        super.discard();
                        m_discardCount++;
                    }
                };
            } catch (IOException e) {
                if (canTruncate) {
                    m_usageSpecificLog.warn(
                            "Error reading segment " + m_file.getName() + ". Truncate the file to last safe point.",
                            e);
                    truncateToCurrentReadIndex();
                    return null;
                }
                throw e;
            } finally {
                m_readOffset = m_fc.position();
                m_fc.position(writePos);
            }
        }

        private void handleCorruptHeader(String message, boolean canTruncate) throws IOException {
            if (canTruncate) {
                message += " Truncate the file to last safe point.";
            }
            m_usageSpecificLog.warn(message);
            if (canTruncate) {
                truncateToCurrentReadIndex();
            } else {
                throw new IOException(message);
            }
        }

        private boolean fillBuffer(ByteBuffer entry, int entryId, char flags, int crc, boolean checkCrc)
                throws IOException {
            int origPosition = entry.position();
            if (!read(entry)) {
                return false;
            }

            entry.position(origPosition);

            if (checkCrc) {
                if (crc != PBDUtils.calculateEntryCrc(m_crcReader, entry, entryId, flags)
                        || INJECT_PBD_CHECKSUM_ERROR) {
                    m_usageSpecificLog.warn("File corruption detected in " + m_file.getName() + ": checksum error. "
                            + "Truncate the file to last safe point.");
                    truncateToCurrentReadIndex();
                    return false;
                }
                entry.position(origPosition);
            }

            return true;
        }

        private boolean read(ByteBuffer buffer) throws IOException {
            do {
                try {
                    int read = m_fc.read(buffer);
                    if (read == -1) {
                        throw new EOFException("EOF encountered reading " + m_file + " at position " + m_fc.position()
                                + " expected to be able to read " + buffer.remaining() + " more bytes");
                    }
                } catch (IOException e) {
                    throw new IOException("Error encountered reading: " + m_file, e);
                }
            } while (buffer.hasRemaining());
            return true;
        }

        private void truncateToCurrentReadIndex() throws IOException {
            boolean wasReadOnly = m_fc.reopen(true);
            try {
                setFinal(false);
                initNumEntries(m_objectReadIndex, m_bytesRead);
                m_fc.position(m_readOffset);
            } finally {
                if (wasReadOnly) {
                    setReadOnly();
                }
            }
        }

        @Override
        public DBBPool.BBContainer getExtraHeader() throws IOException {
            if (m_readerClosed) {
                throw new IOException("Reader closed");
            }

            return PBDRegularSegment.this.readExtraHeader();
        }

        @Override
        public int uncompressedBytesToRead() {
            if (m_readerClosed) {
                throw new RuntimeException("Reader closed");
            }

            return m_size - m_bytesRead;
        }

        @Override
        public long readOffset() {
            return m_readOffset;
        }

        @Override
        public int readIndex() {
            return m_objectReadIndex;
        }

        @Override
        public void rewindReadOffset(int byBytes) {
            m_readOffset -= byBytes;
        }

        @Override
        public void close() throws IOException {
            close(true);
        }

        @Override
        public void purge() throws IOException {
            close(false);
        }

        private void close(boolean keep) throws IOException {
            m_readerClosed = true;
            m_readCursors.remove(m_cursorId);
            if (keep) {
                m_closedCursors.put(m_cursorId, this);
            }
            if (m_readCursors.isEmpty() && !m_isActive) {
                closeReadersAndFile();
            }
        }

        @Override
        public boolean isClosed() {
            return m_readerClosed;
        }

        @Override
        public void reopen() throws IOException {
            if (m_readerClosed) {
                open(false, false, false);
                m_readerClosed = false;
            }
            if (m_cursorId != null) {
                m_closedCursors.remove(m_cursorId);
                m_readCursors.put(m_cursorId, this);
            }
        }
    }

    /**
     * A simple delegation wrapper around a {@link FileChannel} which tracks whether or not any exceptions were thrown
     * by the delegate
     */
    static class FileChannelWrapper extends FileChannel {
        private final Path m_path;
        private FileChannel m_delegate;
        boolean m_writable;
        boolean m_stable = true;

        FileChannelWrapper(File file, boolean forWrite) throws IOException {
            m_path = file.toPath();
            open(forWrite);
        }

        boolean reopen(boolean forWrite) throws IOException {
            if (forWrite != m_writable) {
                m_delegate.close();
                open(forWrite);
                return true;
            }
            return false;
        }

        private void open(boolean forWrite) throws IOException {
            m_delegate = FileChannel.open(m_path, forWrite
                    ? EnumSet.of(StandardOpenOption.READ, StandardOpenOption.WRITE, StandardOpenOption.CREATE)
                    : EnumSet.of(StandardOpenOption.READ));
            m_writable = forWrite;
        }

        @Override
        public String toString() {
            return m_delegate.toString();
        }

        @Override
        public int read(ByteBuffer dst) throws IOException {
            try {
                return m_delegate.read(dst);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
            try {
                return m_delegate.read(dsts, offset, length);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public int write(ByteBuffer src) throws IOException {
            try {
                return m_delegate.write(src);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
            try {
                return m_delegate.write(srcs, offset, length);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public long position() throws IOException {
            try {
                return m_delegate.position();
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public FileChannel position(long newPosition) throws IOException {
            try {
                m_delegate.position(newPosition);
                return this;
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public long size() throws IOException {
            try {
                return m_delegate.size();
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public FileChannel truncate(long size) throws IOException {
            try {
                return m_delegate.truncate(size);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public void force(boolean metaData) throws IOException {
            try {
                m_delegate.force(metaData);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public long transferTo(long position, long count, WritableByteChannel target) throws IOException {
            try {
                return m_delegate.transferTo(position, count, target);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public long transferFrom(ReadableByteChannel src, long position, long count) throws IOException {
            try {
                return m_delegate.transferFrom(src, position, count);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public int read(ByteBuffer dst, long position) throws IOException {
            try {
                return m_delegate.read(dst, position);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public int write(ByteBuffer src, long position) throws IOException {
            try {
                return m_delegate.write(src, position);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public MappedByteBuffer map(MapMode mode, long position, long size) throws IOException {
            try {
                return m_delegate.map(mode, position, size);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public FileLock lock(long position, long size, boolean shared) throws IOException {
            try {
                return m_delegate.lock(position, size, shared);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        public FileLock tryLock(long position, long size, boolean shared) throws IOException {
            try {
                return m_delegate.tryLock(position, size, shared);
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }

        @Override
        protected void implCloseChannel() throws IOException {
            try {
                m_delegate.close();
            } catch (Throwable e) {
                m_stable = false;
                throw e;
            }
        }
    }
}
