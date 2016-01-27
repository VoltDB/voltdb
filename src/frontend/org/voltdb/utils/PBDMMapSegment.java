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

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel.MapMode;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Bits;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DBBPool.MBBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;
import org.xerial.snappy.Snappy;

/**
 * Objects placed in the queue are stored in file segments that are up to 64 megabytes.
 * Segments only support appending objects. A segment will throw an IOException if an attempt
 * to insert an object that exceeds the remaining space is made. A segment can be used
 * for reading and writing, but not both at the same time.
 *
 */
class PBDMMapSegment extends PBDSegment {
    private static final VoltLogger LOG = new VoltLogger("HOST");

    private MBBContainer m_buf;
    private ByteBuffer m_readBuf;

    //If this is the first time polling a segment, madvise the entire thing
    //into memory
    private boolean m_haveMAdvised;

    //Index of the next object to read, not an offset into the file
    //The offset is maintained by the ByteBuffer. Used to determine if there is another object
    int m_objectReadIndex;
    private int m_bytesRead;

    //ID of this segment
    private final Long m_index;

    private boolean m_closed = true;

    //How many entries that have been polled have from this file have been discarded.
    //Convenient to let PBQ maintain the counter here
    private int m_discardCount;

    public PBDMMapSegment(Long index, File file) {
        super(file);
        m_index = index;
        reset();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Creating Segment: " + file.getName() + " At Index: " + m_index);
        }
    }

    @Override
    public long segmentId() {
        return m_index;
    }

    @Override
    public File file() {
        return m_file;
    }

    @Override
    public void reset() {
        m_syncedSinceLastEdit = true;
        m_haveMAdvised = false;
        m_objectReadIndex = 0;
        m_bytesRead = 0;
        m_discardCount = 0;
    }

    @Override
    public int getNumEntries() throws IOException {
        if (m_closed) {
            open(false);
        }
        if (m_fc.size() > SEGMENT_HEADER_BYTES) {
            return m_buf.b().getInt(COUNT_OFFSET);
        } else {
            return 0;
        }
    }

    @Override
    public boolean isBeingPolled() {
        return m_objectReadIndex != 0;
    }

    @Override
    public int readIndex() {
        return m_objectReadIndex;
    }

    @Override
    protected void initNumEntries(int count, int size) throws IOException {
        final ByteBuffer buf = m_buf.b();
        buf.putInt(0, count);
        buf.putInt(4, size);
        m_syncedSinceLastEdit = false;
    }

    private void incrementNumEntries(int size) throws IOException {
        final ByteBuffer buf = m_buf.b();
        //First read the existing amount
        buf.putInt(COUNT_OFFSET, buf.getInt(COUNT_OFFSET) + 1);
        buf.putInt(SIZE_OFFSET, buf.getInt(SIZE_OFFSET) + size);
        m_syncedSinceLastEdit = false;
    }

    @Override
    public void open(boolean forWrite) throws IOException {
        open(forWrite, forWrite);
    }

    @Override
    protected void open(boolean forWrite, boolean truncate) throws IOException {
        if (!m_closed) {
            throw new IOException("Segment is already opened");
        }

        if (!m_file.exists()) {
            m_syncedSinceLastEdit = false;
        }
        assert(m_ras == null);
        m_ras = new RandomAccessFile(m_file, "rw");
        m_fc = m_ras.getChannel();

        if (forWrite) {
            //If this is for writing, map the chunk size RW and put the buf positions at the start
            m_buf = DBBPool.wrapMBB(m_fc.map(MapMode.READ_WRITE, 0, CHUNK_SIZE));
            m_buf.b().position(SIZE_OFFSET + 4);
            m_readBuf = m_buf.b().duplicate();
            if (truncate) {
                initNumEntries(0, 0);
            }
        } else {
            //If it isn't for write, map read only to the actual size and put the write buf position at the end
            //so size is reported correctly
            final long size = m_fc.size();
            m_buf = DBBPool.wrapMBB(m_fc.map(MapMode.READ_ONLY, 0, size));
            m_readBuf = m_buf.b().duplicate();
            m_buf.b().position((int) size);
            m_readBuf.position(SIZE_OFFSET + 4);
        }

        m_closed = false;
    }

    @Override
    public void closeAndDelete() throws IOException {
        close();
        if (LOG.isDebugEnabled()) {
            LOG.debug("Deleting segment at Index " + m_index + " File: " + m_file.getAbsolutePath());
        }
        m_file.delete();
    }

    @Override
    public boolean isClosed() {
        return m_closed;
    }

    @Override
    public void close() throws IOException {
        try {
            if (m_fc != null) {
                m_fc.close();
                m_ras = null;
                m_fc = null;
                m_buf.discard();
                m_buf = null;
                m_readBuf = null;
            }
        } finally {
            m_closed = true;
            reset();
        }
    }

    @Override
    public void sync() throws IOException {
        if (m_closed) throw new IOException("closed");
        if (!m_syncedSinceLastEdit) {
            m_buf.b().force();
        }
        m_syncedSinceLastEdit = true;
    }

    @Override
    public boolean hasMoreEntries() throws IOException {
        if (m_closed) throw new IOException("closed");
        return m_objectReadIndex < m_buf.b().getInt(COUNT_OFFSET);
    }

    @Override
    public boolean isEmpty() throws IOException {
        if (m_closed) throw new IOException("closed");
        return m_discardCount == getNumEntries();
    }

    @Override
    public boolean offer(BBContainer cont, boolean compress) throws IOException {
        if (m_closed) throw new IOException("closed");
        final ByteBuffer buf = cont.b();
        final int remaining = buf.remaining();
        if (remaining < 32 || !buf.isDirect()) compress = false;
        final int maxCompressedSize = compress ? Snappy.maxCompressedLength(remaining) : remaining;
        final ByteBuffer mbuf = m_buf.b();
        if (mbuf.remaining() < maxCompressedSize + OBJECT_HEADER_BYTES) return false;


        m_syncedSinceLastEdit = false;
        try {
            //Leave space for length prefix and flags
            final int objSizePosition = mbuf.position();
            mbuf.position(mbuf.position() + OBJECT_HEADER_BYTES);

            int written = maxCompressedSize;
            if (compress) {
                //Calculate destination pointer and compress directly to file
                final long destAddr = m_buf.address() + mbuf.position();
                written = (int)Snappy.rawCompress(cont.address() + buf.position(), remaining, destAddr);
                mbuf.position(mbuf.position() + written);
            } else {
                mbuf.put(buf);
            }

            //Record the size of the compressed object and update buffer positions
            //and whether the object was compressed
            mbuf.putInt(objSizePosition, written);
            mbuf.putInt(objSizePosition + 4, compress ? FLAG_COMPRESSED: NO_FLAGS);
            buf.position(buf.limit());
            incrementNumEntries(remaining);
        } finally {
            cont.discard();
        }

        return true;
    }

    @Override
    public int offer(DeferredSerialization ds) throws IOException {
        if (m_closed) throw new IOException("closed");
        final ByteBuffer mbuf = m_buf.b();
        if (mbuf.remaining() < ds.getSerializedSize() + OBJECT_HEADER_BYTES) return -1;

        m_syncedSinceLastEdit = false;
        int written = PBDUtils.writeDeferredSerialization(mbuf, ds);
        incrementNumEntries(written);
        return written;
    }

    @Override
    public BBContainer poll(OutputContainerFactory factory) throws IOException {
        if (m_closed) throw new IOException("closed");
        final long mBufAddr = m_buf.address();
        if (!m_haveMAdvised) {
            final ByteBuffer mbuf = m_buf.b();
            m_haveMAdvised = true;
            final long retval = PosixAdvise.madvise(
                    m_buf.address(),
                    mbuf.position(),
                    PosixAdvise.POSIX_MADV_WILLNEED);
            if (retval != 0) {
                LOG.warn("madvise will need failed: " + retval);
            }
        }

        //No more entries to read
        if (!hasMoreEntries()) {
            return null;
        }

        m_objectReadIndex++;

        //Get the length prefix and then read the object
        final int nextCompressedLength = m_readBuf.getInt();
        final int nextFlags = m_readBuf.getInt();

        //Check for compression
        final boolean compressed = (nextFlags & FLAG_COMPRESSED) != 0;
        //Determine the length of the object if uncompressed
        final int nextUncompressedLength = compressed ? (int)Snappy.uncompressedLength(mBufAddr + m_readBuf.position(), nextCompressedLength) : nextCompressedLength;
        m_bytesRead += nextUncompressedLength;

        final BBContainer retcont;
        if (compressed) {
            //Get storage for output
            retcont = factory.getContainer(nextUncompressedLength);
            final ByteBuffer retbuf = retcont.b();

            //Limit to appropriate uncompressed size
            retbuf.limit(nextUncompressedLength);

            //Uncompress to output buffer
            final long sourceAddr = mBufAddr + m_readBuf.position();
            final long destAddr = retcont.address();
            Snappy.rawUncompress(sourceAddr, nextCompressedLength, destAddr);
            m_readBuf.position(m_readBuf.position() + nextCompressedLength);
        } else {
            //Return a slice
            final int oldLimit = m_readBuf.limit();
            m_readBuf.limit(m_readBuf.position() + nextUncompressedLength);
            ByteBuffer retbuf = m_readBuf.slice();
            m_readBuf.position(m_readBuf.limit());
            m_readBuf.limit(oldLimit);

            /*
             * For uncompressed data, touch all the pages to make 100% sure
             * they are available since they will be accessed directly.
             *
             * This code mimics MappedByteBuffer.load, but without the expensive
             * madvise call for data we are 99% sure was already madvised.
             *
             * This would only ever be an issue in the unlikely event that the page cache
             * is trashed at the wrong moment or we are very low on memory
             */
            retcont = DBBPool.dummyWrapBB(retbuf);
            Bits.readEveryPage(retcont);
        }

        return new BBContainer(retcont.b()) {
            private boolean m_discarded = false;

            @Override
            public void discard()
            {
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
    }

    /*
     * Don't use size in bytes to determine empty, could potentially
     * diverge from object count on crash or power failure
     * although incredibly unlikely
     */
    @Override
    public int uncompressedBytesToRead() {
        if (m_closed) throw new RuntimeException("closed");
        return Math.max(0, m_buf.b().getInt(SIZE_OFFSET) - m_bytesRead);
    }

    @Override
    protected long readOffset()
    {
        return m_readBuf.position();
    }

    @Override
    protected void rewindReadOffset(int byBytes)
    {
        m_readBuf.position(m_readBuf.position() - byBytes);
    }

    @Override
    protected int writeTruncatedEntry(BinaryDeque.TruncatorResponse entry, int length) throws IOException
    {
        return entry.writeTruncatedObject(m_readBuf);
    }
}
