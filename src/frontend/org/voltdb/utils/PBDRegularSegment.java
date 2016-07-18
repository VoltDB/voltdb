/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
import org.voltcore.utils.DeferredSerialization;

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

    private Map<String, SegmentReader> m_readCursors = new HashMap<>();
    //Index of the next object to read, not an offset into the file
    //private int m_objectReadIndex = 0;
    //private int m_bytesRead = 0;
    // Maintains the read byte offset
    //private long m_readOffset = SEGMENT_HEADER_BYTES;

    //ID of this segment
    private final Long m_index;

    //private int m_discardCount;
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
        for (SegmentReader reader : m_readCursors.values()) {
            reader.m_objectReadIndex = 0;
            reader.m_bytesRead = 0;
            reader.m_readOffset = SEGMENT_HEADER_BYTES;
            reader.m_discardCount = 0;
        }
        if (m_tmpHeaderBuf != null) {
            m_tmpHeaderBuf.discard();
            m_tmpHeaderBuf = null;
        }
    }

    @Override
    public int getNumEntries() throws IOException
    {
        if (m_closed) {
            open(false, false);
        }
        if (m_fc.size() > 0) {
            m_tmpHeaderBuf.b().clear();
            PBDUtils.readBufferFully(m_fc, m_tmpHeaderBuf.b(), COUNT_OFFSET);
            m_numOfEntries = m_tmpHeaderBuf.b().getInt();
            m_size = m_tmpHeaderBuf.b().getInt();
            return m_numOfEntries;
        } else {
            m_numOfEntries = 0;
            m_size = 0;
            return 0;
        }
    }

    @Override
    public boolean isBeingPolled()
    {
        for (SegmentReader reader : m_readCursors.values()) {
            if (reader.m_objectReadIndex != 0) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int readIndex(String cursorId)
    {
        return m_readCursors.get(cursorId).m_objectReadIndex;
    }

    @Override
    public boolean isOpenForReading(String cursorId) {
        return m_readCursors.containsKey(cursorId);
    }

    @Override
    public void openForRead(String cursorId) throws IOException
    {
        //TODO: start a cursor
        if (m_readCursors.containsKey(cursorId)) {
            throw new IOException("Segment is already open for reading for cursor " + cursorId);
        }

        m_readCursors.put(cursorId, new SegmentReader(cursorId));
        if (m_closed) {
            open(false, false);
        }
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
        m_readCursors.clear();
        try {
            if (m_fc != null) {
                m_fc.close();
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
    public boolean hasMoreEntries(String cursorId) throws IOException {
        if (m_closed) throw new IOException("Segment closed");

        if (m_readCursors.size() == 0) return false;

        return m_readCursors.get(cursorId).m_objectReadIndex < m_numOfEntries;
    }

    @Override
    public boolean hasAllFinishedReading() throws IOException {
        if (m_closed) throw new IOException("Segment closed");

        if (m_readCursors.size() == 0) return false;

        for (String cid : m_readCursors.keySet()) {
            SegmentReader reader = m_readCursors.get(cid);
            if (reader.m_objectReadIndex < m_numOfEntries) {
                return false;
            }
        }

        return true;
    }

    @Override
    public boolean isCursorEmpty(String cursorId) throws IOException {
        if (m_closed) throw new IOException("Segment closed");
        return m_readCursors.get(cursorId).m_discardCount == m_numOfEntries;
    }

    @Override
    public boolean isSegmentEmpty() throws IOException {
        if (m_closed) throw new IOException("Segment closed");

        for (String cid : m_readCursors.keySet()) {
            SegmentReader reader = m_readCursors.get(cid);
            if (reader.m_discardCount < m_numOfEntries) {
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
    public DBBPool.BBContainer poll(String cursorId, BinaryDeque.OutputContainerFactory factory) throws IOException
    {
        if (m_closed) throw new IOException("closed");

        if (!hasMoreEntries(cursorId)) {
            return null;
        }

        final long writePos = m_fc.position();
        SegmentReader reader = m_readCursors.get(cursorId);
        m_fc.position(reader.m_readOffset);
        reader.m_objectReadIndex++;

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

            reader.m_bytesRead += uncompressedLen;

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
                    reader.m_discardCount++;
                }
            };
        } finally {
            reader.m_readOffset = m_fc.position();
            m_fc.position(writePos);
        }
    }

    @Override
    public int uncompressedBytesToRead(String cursorId) {
        if (m_closed) throw new RuntimeException("Segment closed");
        return m_size - m_readCursors.get(cursorId).m_bytesRead;
    }

    @Override
    public int size() {
        return m_size;
    }

    @Override
    protected long readOffset(String cursorId)
    {
        return m_readCursors.get(cursorId).m_readOffset;
    }

    @Override
    protected void rewindReadOffset(String cursorId, int byBytes)
    {
        m_readCursors.get(cursorId).m_readOffset -= byBytes;
    }

    @Override
    protected int writeTruncatedEntry(BinaryDeque.TruncatorResponse entry, int length) throws IOException
    {
        int written = 0;
        final DBBPool.BBContainer partialCont = DBBPool.allocateDirect(length);
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

    private class SegmentReader {
        @SuppressWarnings("unused")
        private final String m_cursorId;
        private long m_readOffset = SEGMENT_HEADER_BYTES;
        //Index of the next object to read, not an offset into the file
        private int m_objectReadIndex = 0;
        private int m_bytesRead = 0;
        private int m_discardCount;

        public SegmentReader(String cursorId) {
            m_cursorId = cursorId;
        }
    }
}
