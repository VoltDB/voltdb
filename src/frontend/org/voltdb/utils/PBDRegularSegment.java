/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;

import com.google_voltpatches.common.base.Preconditions;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

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

    //ID of this segment
    private final Long m_index;

    private int m_numOfEntries = -1;
    private int m_size = -1;

    private DBBPool.BBContainer m_tmpHeaderBuf = null;

    public PBDRegularSegment(Long index, File file) {
        super(file);
        m_index = index;
        reset();
    }

    @Override
    public long segmentId()
    {
        return m_index;
    }

    @Override
    public File file()
    {
        return m_file;
    }

    @Override
    public void reset()
    {
        m_syncedSinceLastEdit = false;
        if (m_tmpHeaderBuf != null) {
            m_tmpHeaderBuf.discard();
            m_tmpHeaderBuf = null;
        }
    }

    @Override
    public int getNumEntries() throws IOException
    {
        boolean wasClosed = false;
        if (m_closed) {
            wasClosed = true;
            open(false, false);
        }
        if (m_fc.size() > 0) {
            m_tmpHeaderBuf.b().clear();
            PBDUtils.readBufferFully(m_fc, m_tmpHeaderBuf.b(), COUNT_OFFSET);
            m_numOfEntries = m_tmpHeaderBuf.b().getInt();
            m_size = m_tmpHeaderBuf.b().getInt();
        } else {
            m_numOfEntries = 0;
            m_size = 0;
        }
        if (wasClosed) closeReadersAndFile();
        return m_numOfEntries;
    }

    @Override
    public boolean isBeingPolled()
    {
        return !m_readCursors.isEmpty();
    }

    @Override
    public boolean isOpenForReading(String cursorId) {
        return m_readCursors.containsKey(cursorId);
    }

    @Override
    public PBDSegmentReader openForRead(String cursorId) throws IOException
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
    public PBDSegmentReader getReader(String cursorId) {
        PBDSegmentReader reader = m_closedCursors.get(cursorId);
        return (reader == null) ? m_readCursors.get(cursorId) : reader;
    }

    @Override
    protected void openForWrite(boolean emptyFile) throws IOException {
        open(true, emptyFile);
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
        m_tmpHeaderBuf = DBBPool.allocateDirect(SEGMENT_HEADER_BYTES);

        if (emptyFile) {
            initNumEntries(0, 0);
        }
        m_fc.position(SEGMENT_HEADER_BYTES);

        m_closed = false;
    }


    @Override
    protected void initNumEntries(int count, int size) throws IOException {
        m_numOfEntries = count;
        m_size = size;

        m_tmpHeaderBuf.b().clear();
        m_tmpHeaderBuf.b().putInt(m_numOfEntries);
        m_tmpHeaderBuf.b().putInt(m_size);
        m_tmpHeaderBuf.b().flip();
        PBDUtils.writeBuffer(m_fc, m_tmpHeaderBuf.bDR(), COUNT_OFFSET);
        m_syncedSinceLastEdit = false;
    }

    private void incrementNumEntries(int size) throws IOException
    {
        m_numOfEntries++;
        m_size += size;

        m_tmpHeaderBuf.b().clear();
        m_tmpHeaderBuf.b().putInt(m_numOfEntries);
        m_tmpHeaderBuf.b().putInt(m_size);
        m_tmpHeaderBuf.b().flip();
        PBDUtils.writeBuffer(m_fc, m_tmpHeaderBuf.bDR(), COUNT_OFFSET);
        m_syncedSinceLastEdit = false;
    }

    /**
     * Bytes of space available for inserting more entries
     * @return
     */
    private int remaining() throws IOException {
        //Subtract 8 for the length and size prefix
        return (int)(PBDSegment.CHUNK_SIZE - m_fc.position()) - SEGMENT_HEADER_BYTES;
    }

    @Override
    public void closeAndDelete() throws IOException {
        close();
        m_file.delete();

        m_numOfEntries = -1;
        m_size = -1;
    }

    @Override
    public boolean isClosed()
    {
        return m_closed;
    }

    @Override
    public void close() throws IOException {
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
    public void sync() throws IOException {
        if (m_closed) throw new IOException("Segment closed");
        if (!m_syncedSinceLastEdit) {
            m_fc.force(true);
        }
        m_syncedSinceLastEdit = true;
    }

    @Override
    public boolean hasAllFinishedReading() throws IOException {
        if (m_closed) throw new IOException("Segment closed");

        if (m_readCursors.size() == 0) return false;

        for (SegmentReader reader : m_readCursors.values()) {
            if (reader.m_objectReadIndex < m_numOfEntries) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean offer(DBBPool.BBContainer cont, boolean compress) throws IOException
    {
        if (m_closed) throw new IOException("Segment closed");
        final ByteBuffer buf = cont.b();
        final int remaining = buf.remaining();
        if (remaining < 32 || !buf.isDirect()) compress = false;
        final int maxCompressedSize = (compress ? CompressionService.maxCompressedLength(remaining) : remaining) + OBJECT_HEADER_BYTES;
        if (remaining() < maxCompressedSize) return false;

        m_syncedSinceLastEdit = false;
        DBBPool.BBContainer destBuf = cont;

        try {
            m_tmpHeaderBuf.b().clear();

            if (compress) {
                destBuf = DBBPool.allocateDirectAndPool(maxCompressedSize);
                final int compressedSize = CompressionService.compressBuffer(buf, destBuf.b());
                destBuf.b().limit(compressedSize);

                m_tmpHeaderBuf.b().putInt(compressedSize);
                m_tmpHeaderBuf.b().putInt(FLAG_COMPRESSED);
            } else {
                destBuf = cont;
                m_tmpHeaderBuf.b().putInt(remaining);
                m_tmpHeaderBuf.b().putInt(NO_FLAGS);
            }

            m_tmpHeaderBuf.b().flip();
            while (m_tmpHeaderBuf.b().hasRemaining()) {
                m_fc.write(m_tmpHeaderBuf.b());
            }

            while (destBuf.b().hasRemaining()) {
                m_fc.write(destBuf.b());
            }

            incrementNumEntries(remaining);
        } finally {
            destBuf.discard();
            if (compress) {
                cont.discard();
            }
        }

        return true;
    }

    @Override
    public int offer(DeferredSerialization ds) throws IOException
    {
        if (m_closed) throw new IOException("closed");
        final int fullSize = ds.getSerializedSize() + OBJECT_HEADER_BYTES;
        if (remaining() < fullSize) return -1;

        m_syncedSinceLastEdit = false;
        DBBPool.BBContainer destBuf = DBBPool.allocateDirectAndPool(fullSize);

        try {
            final int written = PBDUtils.writeDeferredSerialization(destBuf.b(), ds);
            destBuf.b().flip();

            while (destBuf.b().hasRemaining()) {
                m_fc.write(destBuf.b());
            }

            incrementNumEntries(written);
            return written;
        } finally {
            destBuf.discard();
        }
    }

    @Override
    public int size() {
        return m_size;
    }

    @Override
    protected int writeTruncatedEntry(BinaryDeque.TruncatorResponse entry) throws IOException
    {
        int written = 0;
        final DBBPool.BBContainer partialCont = DBBPool.allocateDirect(OBJECT_HEADER_BYTES + entry.getTruncatedBuffSize());
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

    private class SegmentReader implements PBDSegmentReader {
        private final String m_cursorId;
        private long m_readOffset = SEGMENT_HEADER_BYTES;
        //Index of the next object to read, not an offset into the file
        private int m_objectReadIndex = 0;
        private int m_bytesRead = 0;
        private int m_discardCount = 0;
        private boolean m_closed = false;

        public SegmentReader(String cursorId) {
            assert(cursorId != null);
            m_cursorId = cursorId;
        }

        private void resetReader() {
            m_objectReadIndex = 0;
            m_bytesRead = 0;
            m_readOffset = SEGMENT_HEADER_BYTES;
            m_discardCount = 0;
        }

        @Override
        public boolean hasMoreEntries() throws IOException {
            return m_objectReadIndex < m_numOfEntries;
        }

        @Override
        public boolean allReadAndDiscarded() throws IOException {
            return m_discardCount == m_numOfEntries;
        }

        @Override
        public BBContainer poll(OutputContainerFactory factory) throws IOException {
            if (m_closed) throw new IOException("Reader closed");

            if (!hasMoreEntries()) {
                return null;
            }

            final long writePos = m_fc.position();
            m_fc.position(m_readOffset);

            try {
                //Get the length and size prefix and then read the object
                m_tmpHeaderBuf.b().clear();
                while (m_tmpHeaderBuf.b().hasRemaining()) {
                    int read = m_fc.read(m_tmpHeaderBuf.b());
                    if (read == -1) {
                        throw new EOFException();
                    }
                }
                m_tmpHeaderBuf.b().flip();
                final int length = m_tmpHeaderBuf.b().getInt();
                final int flags = m_tmpHeaderBuf.b().getInt();
                final boolean compressed = (flags & FLAG_COMPRESSED) != 0;
                final int uncompressedLen;

                if (length < 1) {
                    throw new IOException("Read an invalid length");
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

        @Override
        public int uncompressedBytesToRead() {
            if (m_closed) throw new RuntimeException("Reader closed");

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
            m_closed = true;
            m_readCursors.remove(m_cursorId);
            m_closedCursors.put(m_cursorId, this);
            if (m_readCursors.isEmpty()) {
                closeReadersAndFile();
            }
        }

        @Override
        public boolean isClosed() {
            return m_closed;
        }
    }
}
