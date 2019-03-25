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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.CRC32;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;

import com.google_voltpatches.common.base.Preconditions;

/**
 * Objects placed in the deque are stored in file segments that are up to 64 megabytes.
 * Segments only support appending objects. A segment will throw an IOException if an attempt
 * to insert an object that exceeds the remaining space is made. A segment can be used
 * for reading and writing, but not both at the same time.
 */
public class PBDRegularSegment extends PBDSegment {
    private static final VoltLogger LOG = new VoltLogger("HOST");

    private final Map<String, SegmentReader> m_readCursors = new HashMap<>();
    private final Map<String, SegmentReader> m_closedCursors = new HashMap<>();

    // Index of this segment in the in-memory segment map
    private final long m_index;

    // Persistent ID of this segment, based on managing a monotonic counter
    private final long m_id;

    private int m_numOfEntries = -1;
    private int m_size = -1;
    private int m_extraHeaderSize = 0;

    private DBBPool.BBContainer m_segmentHeaderBuf = null;
    private DBBPool.BBContainer m_entryHeaderBuf = null;
    Boolean INJECT_PBD_CHECKSUM_ERROR = Boolean.getBoolean("INJECT_PBD_CHECKSUM_ERROR");

    public PBDRegularSegment(Long index, long id, File file) {
        super(file);
        m_index = index;
        m_id = id;
        reset();
    }

    @Override
    long segmentIndex()
    {
        return m_index;
    }

    @Override
    long segmentId() {
        return m_id;
    }

    @Override
    File file()
    {
        return m_file;
    }

    @Override
    void reset()
    {
        m_syncedSinceLastEdit = false;
        if (m_segmentHeaderBuf != null) {
            m_segmentHeaderBuf.discard();
            m_segmentHeaderBuf = null;
        }
        if (m_entryHeaderBuf != null) {
            m_entryHeaderBuf.discard();
            m_entryHeaderBuf = null;
        }
        m_segmentHeaderCRC.reset();
        m_entryCRC.reset();
    }

    @Override
    int getNumEntries() throws IOException
    {
        initializeFromHeader(crcCheck);
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
    PBDSegmentReader openForRead(String cursorId) throws IOException
    {
        Preconditions.checkNotNull(cursorId, "Reader id must be non-null");
        if (m_readCursors.containsKey(cursorId) || m_closedCursors.containsKey(cursorId)) {
            throw new IOException("Segment is already open for reading for cursor " + cursorId);
        }

        if (m_closed) {
            open(false, false);
        }
        SegmentReader reader = new SegmentReader(cursorId);
        m_readCursors.put(cursorId, reader);
        return reader;
    }

    @Override
    PBDSegmentReader getReader(String cursorId) {
        PBDSegmentReader reader = m_closedCursors.get(cursorId);
        return (reader == null) ? m_readCursors.get(cursorId) : reader;
    }

    @Override
    protected void openForWrite(boolean emptyFile) throws IOException {
        open(true, emptyFile);
    }

    int getExtraHeaderSize() throws IOException {
        initializeFromHeader(false);
        return m_extraHeaderSize;
    }

    private void open(boolean forWrite, boolean emptyFile) throws IOException {
        if (!m_closed) {
            throw new IOException("Segment is already opened");
        }

        if (!m_file.exists()) {
            if (!forWrite) {
                throw new IOException("File " + m_file + " does not exist");
            }
            m_syncedSinceLastEdit = false;
        }
        assert(m_ras == null);
        m_ras = new RandomAccessFile( m_file, forWrite ? "rw" : "r");
        m_fc = m_ras.getChannel();
        m_segmentHeaderBuf = DBBPool.allocateDirect(SEGMENT_HEADER_BYTES);
        m_entryHeaderBuf = DBBPool.allocateDirect(ENTRY_HEADER_BYTES);

        // Those asserts ensure the file is opened with correct flag
        if (emptyFile) {
            initNumEntries(0, 0);
        }
        if (forWrite) {
            m_fc.position(m_fc.size());
        } else {
            m_fc.position(SEGMENT_HEADER_BYTES);
        }

        m_closed = false;
    }

    private void initializeFromHeader(boolean crcCheck) throws IOException {
        if (m_numOfEntries != -1) {
            return;
        }
        boolean wasClosed = false;
        if (m_closed) {
            wasClosed = true;
            open(false, false);
        }
        try {
            if (m_fc.size() >= SEGMENT_HEADER_BYTES) {
                ByteBuffer b = m_segmentHeaderBuf.b();
                b.clear();
                PBDUtils.readBufferFully(m_fc, b, 0);
                int crc = b.getInt();
                int numOfEntries = b.getInt();
                int size = b.getInt();
                int extraHeaderSize = b.getInt();
                if (crcCheck) {
                    m_segmentHeaderCRC.reset();
                    m_segmentHeaderCRC.update(numOfEntries);
                    m_segmentHeaderCRC.update(size);
                    m_segmentHeaderCRC.update(extraHeaderSize);
                    if (crc != (int) m_segmentHeaderCRC.getValue()) {
                        LOG.warn("File corruption detected in" + m_file.getName() + ": invalid file header. ");
                        throw new IOException(
                                "File corruption detected in" + m_file.getName() + ": invalid file header.");
                    }
                }
                m_numOfEntries = numOfEntries;
                m_size = size;
                m_extraHeaderSize = extraHeaderSize;
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
        // Update segment header CRC
        m_segmentHeaderCRC.reset();
        m_segmentHeaderCRC.update(m_numOfEntries);
        m_segmentHeaderCRC.update(m_size);
        m_segmentHeaderCRC.update(m_extraHeaderSize);

        ByteBuffer b = m_segmentHeaderBuf.b();
        b.clear();
        // the checksum here is really an unsigned int, store integer to save 4 bytes
        b.putInt((int) m_segmentHeaderCRC.getValue());
        b.putInt(m_numOfEntries);
        b.putInt(m_size);
        b.putInt(m_extraHeaderSize);
        b.flip();
        PBDUtils.writeBuffer(m_fc, m_segmentHeaderBuf.bDR(), PBDSegment.HEADER_CRC_OFFSET);
        m_syncedSinceLastEdit = false;
    }

    @Override
    protected void initNumEntries(int count, int size) throws IOException {
        m_numOfEntries = count;
        m_size = size;
        writeOutHeader();
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
        close();
        m_file.delete();

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("Close PBD Segment " + m_file.getName());
        }
        m_closedCursors.clear();
        closeReadersAndFile();
    }

    private void closeReadersAndFile() throws IOException {
        m_readCursors.clear();
        try {
            if (m_ras != null) {
                m_ras.close();
            }
        } finally {
            m_ras = null;
            m_fc = null;
            m_closed = true;
            reset();
        }
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
    public boolean offer(DBBPool.BBContainer cont, boolean compress) throws IOException
    {
        if (m_closed) {
            throw new IOException("Segment closed");
        }
        final ByteBuffer buf = cont.b();
        final int remaining = buf.remaining();
        if (remaining < 32 || !buf.isDirect()) {
            compress = false;
        }
        final int maxCompressedSize = (compress ? CompressionService.maxCompressedLength(remaining) : remaining) + ENTRY_HEADER_BYTES;
        if (remaining() < maxCompressedSize) {
            return false;
        }

        m_syncedSinceLastEdit = false;
        DBBPool.BBContainer destBuf = cont;
        try {
            m_entryCRC.reset();
            m_entryHeaderBuf.b().clear();

            if (compress) {
                destBuf = DBBPool.allocateDirectAndPool(maxCompressedSize);
                final int compressedSize = CompressionService.compressBuffer(buf, destBuf.b());
                destBuf.b().limit(compressedSize);
                PBDUtils.writeEntryHeader(m_entryCRC, m_entryHeaderBuf.b(), destBuf.b(),
                        compressedSize, FLAG_COMPRESSED);
            } else {
                destBuf = cont;
                PBDUtils.writeEntryHeader(m_entryCRC, m_entryHeaderBuf.b(), destBuf.b(),
                        remaining, NO_FLAGS);
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
            m_entryCRC.reset();
            m_entryHeaderBuf.b().clear();
            final int written = MiscUtils.writeDeferredSerialization(destBuf.b(), ds);
            destBuf.b().flip();
            // Write entry header
            PBDUtils.writeEntryHeader(m_entryCRC, m_entryHeaderBuf.b(), destBuf.b(),
                    written, PBDSegment.NO_FLAGS);
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

    @Override
    int size() {
        return m_size;
    }

    @Override
    protected int writeTruncatedEntry(BinaryDeque.TruncatorResponse entry) throws IOException
    {
        int written = 0;
        final DBBPool.BBContainer partialCont =
                DBBPool.allocateDirect(ENTRY_HEADER_BYTES + entry.getTruncatedBuffSize());
        try {
            written += entry.writeTruncatedObject(partialCont.b());
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
    void writeExtraHeader(DeferredSerialization ds) throws IOException {
        if (!(m_numOfEntries == 0 && m_extraHeaderSize == 0)) {
            throw new IllegalStateException("Extra header must be written before any entries");
        }
        int size = ds.getSerializedSize();
        DBBPool.BBContainer destBuf = DBBPool.allocateDirect(size);
        try {
            ByteBuffer b = destBuf.b();
            b.order(ByteOrder.LITTLE_ENDIAN);
            ds.serialize(b);
            b.flip();
            while (b.hasRemaining()) {
                m_fc.write(b);
            }
            m_extraHeaderSize = size;
            writeOutHeader();
        } finally {
            destBuf.discard();
        }
    }

    private class SegmentReader implements PBDSegmentReader {
        private final String m_cursorId;
        private long m_readOffset;
        //Index of the next object to read, not an offset into the file
        private int m_objectReadIndex = 0;
        private int m_bytesRead = 0;
        private int m_discardCount = 0;
        private boolean m_readerClosed = false;
        private CRC32 m_crc32 = new CRC32();

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
        public boolean allReadAndDiscarded() {
            return m_discardCount == m_numOfEntries;
        }

        @Override
        public DBBPool.BBContainer poll(OutputContainerFactory factory, boolean checkCRC) throws IOException {

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
                m_entryHeaderBuf.b().clear();
                while (m_entryHeaderBuf.b().hasRemaining()) {
                    int read = m_fc.read(m_entryHeaderBuf.b());
                    if (read == -1) {
                        throw new EOFException();
                    }
                }
                m_entryHeaderBuf.b().flip();
                final int entryCRC = m_entryHeaderBuf.b().getInt();
                final int length = m_entryHeaderBuf.b().getInt();
                final int flags = m_entryHeaderBuf.b().getInt();
                final boolean compressed = (flags & FLAG_COMPRESSED) != 0;
                final int uncompressedLen;

                if (length < 1 || length > PBDSegment.CHUNK_SIZE - PBDSegment.SEGMENT_HEADER_BYTES) {
                    LOG.warn("File corruption detected in " + m_file.getName() + ": invalid entry length. "
                            + "Truncate the file to last safe point.");
                    PBDRegularSegment.this.close();
                    openForWrite(false);
                    initNumEntries(m_objectReadIndex, m_bytesRead);
                    m_fc.truncate(m_readOffset);
                    return null;
                }

                final DBBPool.BBContainer retcont;
                if (compressed) {
                    final DBBPool.BBContainer compressedBuf = DBBPool.allocateDirectAndPool(length);
                    try {
                        while (compressedBuf.b().hasRemaining()) {
                            int read = m_fc.read(compressedBuf.b());
                            if (read == -1) {
                                throw new EOFException();
                            }
                        }
                        compressedBuf.b().flip();
                        if (checkCRC && !checkEntryCrc(length, flags, compressedBuf.b(), entryCRC)) {
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
                    while (retcont.b().hasRemaining()) {
                        int read = m_fc.read(retcont.b());
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    retcont.b().flip();
                    if (checkCRC && !checkEntryCrc(length, flags, retcont.b(), entryCRC)) {
                        retcont.discard();
                        return null;
                    }
                }

                m_bytesRead += uncompressedLen;
                m_objectReadIndex++;

                return new DBBPool.BBContainer(retcont.b()) {
                    private boolean m_discarded = false;

                    @Override
                    public void discard() {
                        checkDoubleFree();
                        if (m_discarded) {
                            LOG.error("PBD Container discarded more than once");
                            return;
                        }

                        m_discarded = true;
                        retcont.discard();
                        m_discardCount++;
                    }
                };
            } finally {
                m_readOffset = m_fc.position();
                m_fc.position(writePos);
            }
        }

        private boolean checkEntryCrc(int length, int flags, ByteBuffer entry, int crc) throws IOException {
            int origPosition = entry.position();
            m_crc32.reset();
            m_crc32.update(length);
            m_crc32.update(flags);
            m_crc32.update(entry);
            entry.position(origPosition);

            if (crc != (int) m_crc32.getValue() || INJECT_PBD_CHECKSUM_ERROR) {
                LOG.warn("File corruption detected in " + m_file.getName() + ": checksum error. "
                        + "Truncate the file to last safe point.");
                PBDRegularSegment.this.close();
                openForWrite(false);
                initNumEntries(m_objectReadIndex, m_bytesRead);
                m_fc.truncate(m_readOffset);
                return false;
            }
            return true;
        }

        @Override
        public DBBPool.BBContainer getExtraHeader() throws IOException {
            if (m_readerClosed) {
                throw new IOException("Reader closed");
            }

            int extraHeaderSize = getExtraHeaderSize();
            if (extraHeaderSize == 0) {
                return null;
            }

            final long writePos = m_fc.position();
            m_fc.position(SEGMENT_HEADER_BYTES);

            DBBPool.BBContainer schemaBuf = null;
            try {
                schemaBuf = DBBPool.allocateDirect(extraHeaderSize);
                ByteBuffer schemaBuffer = schemaBuf.b();
                do {
                    int read = m_fc.read(schemaBuffer);
                    if (read == -1) {
                        throw new EOFException();
                    }
                } while (schemaBuffer.hasRemaining());
                schemaBuffer.order(ByteOrder.LITTLE_ENDIAN).flip();
                return schemaBuf;
            } catch (Exception e) {
                if (schemaBuf != null) {
                    schemaBuf.discard();
                }
                LOG.error(e);
                return null;
            } finally {
                m_fc.position(writePos);
            }
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
            m_readerClosed = true;
            m_readCursors.remove(m_cursorId);
            m_closedCursors.put(m_cursorId, this);
            if (m_readCursors.isEmpty()) {
                closeReadersAndFile();
            }
        }

        @Override
        public boolean isClosed() {
            return m_readerClosed;
        }

        @Override
        public void reopen(boolean forWrite, boolean emptyFile) throws IOException {
            if (m_readerClosed) {
                open(forWrite, emptyFile);
                m_readerClosed = false;
            }
            if (m_cursorId != null) {
                m_closedCursors.remove(m_cursorId);
                m_readCursors.put(m_cursorId, this);
            }
        }
    }
}
