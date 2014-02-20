/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque.OutputContainerFactory;
import org.xerial.snappy.Snappy;

import sun.nio.ch.DirectBuffer;

/**
 * Objects placed in the queue are stored in file segments that are up to 64 megabytes.
 * Segments only support appending objects. A segment will throw an IOException if an attempt
 * to insert an object that exceeds the remaining space is made. A segment can be used
 * for reading and writing, but not both at the same time.
 *
 */
class PBDSegment {
    private static final VoltLogger LOG = new VoltLogger("HOST");

    public static final int FLAG_COMPRESSED = 1;

    //Avoid unecessary sync with this flag
    private boolean m_syncedSinceLastEdit = true;
    final File m_file;
    private RandomAccessFile m_ras;
    private FileChannel m_fc;
    private MappedByteBuffer m_buf;
    private ByteBuffer m_readBuf;
    private long m_bufAddr;

    //If this is the first time polling a segment, madvise the entire thing
    //into memory
    private boolean m_haveMAdvised = false;

    //Index of the next object to read, not an offset into the file
    //The offset is maintained by the ByteBuffer. Used to determine if there is another object
    private int m_objectReadIndex = 0;

    //ID of this segment
    final Long m_index;
    static final int m_chunkSize = (1024 * 1024) * 64;
    static final int m_objectHeaderBytes = 8;

    private boolean m_closed = false;

    //How many entries that have been polled have from this file have been discarded.
    //Convenient to let PBQ maintain the counter here
    int m_discardCount = 0;

    public PBDSegment(Long index, File file ) {
        m_index = index;
        m_file = file;
    }

    int getNumEntries() throws IOException {
        if (m_fc == null) {
            open(false);
        }
        if (m_fc.size() > 4) {
            final int numEntries = m_buf.getInt(0);
            return numEntries;
        } else {
            return 0;
        }
    }

    private void initNumEntries() throws IOException {
        m_buf.putInt(0, 0);
        m_syncedSinceLastEdit = false;
    }

    private void incrementNumEntries() throws IOException {
        //First read the existing amount
        m_buf.putInt(0, m_buf.getInt(0) + 1);
        m_syncedSinceLastEdit = false;
    }

    void open(boolean forWrite) throws IOException {
        if (!m_file.exists()) {
            m_syncedSinceLastEdit = false;
        }
        if (m_ras != null) {
            throw new IOException(m_file + " was already opened");
        }
        m_ras = new RandomAccessFile( m_file, "rw");
        m_fc = m_ras.getChannel();

        if (forWrite) {
            //If this is for writing, map the chunk size RW and put the buf positions at the start
            m_buf = m_fc.map(MapMode.READ_WRITE, 0, m_chunkSize);
            m_buf.position(4);
            m_readBuf = m_buf.duplicate();
            initNumEntries();
        } else {
            //If it isn't for write, map read only to the actual size and put the write buf position at the end
            //so size is reported correctly
            final long size = m_fc.size();
            m_buf = m_fc.map(MapMode.READ_ONLY, 0, size);
            m_readBuf = m_buf.duplicate();
            m_buf.position((int)size);
            m_readBuf.position(4);
        }
        m_bufAddr = ((DirectBuffer)m_buf).address();
    }

    public void closeAndDelete() throws IOException {
        close();
        m_file.delete();
    }

    public void close() throws IOException {
        if (m_fc != null) {
            m_fc.close();
            m_ras = null;
            m_fc = null;
            DBBPool.cleanByteBuffer(m_buf);
            m_buf = null;
            m_readBuf = null;
        }
        m_closed = true;
    }

    void sync() throws IOException {
        if (!m_syncedSinceLastEdit) {
            m_buf.force();
        }
        m_syncedSinceLastEdit = true;
    }

    boolean hasMoreEntries() throws IOException {
        return m_buf.position() > m_readBuf.position();
    }

    boolean offer(BBContainer cont, boolean compress) throws IOException {
        final ByteBuffer buf = cont.b;
        if (cont.b.remaining() < 32) compress = false;
        final int maxCompressedSize = compress ? Snappy.maxCompressedLength(buf.remaining()) : buf.remaining();
        if (m_buf.remaining() < maxCompressedSize + 8) return false;


        m_syncedSinceLastEdit = false;
        try {
            //Leave space for length prefix and flags
            final int objSizePosition = m_buf.position();
            m_buf.position(m_buf.position() + 8);

            int written = maxCompressedSize;
            if (compress) {
                //Calculate destination pointer and compress directly to file
                final long destAddr = m_bufAddr + m_buf.position();
                written = (int)Snappy.rawCompress(((DirectBuffer)buf).address() + buf.position(), buf.remaining(), destAddr);
                m_buf.position(m_buf.position() + written);
            } else {
                m_buf.put(buf);
            }

            //Record the size of the compressed object and update buffer positions
            //and whether the object was compressed
            m_buf.putInt(objSizePosition, written);
            m_buf.putInt(objSizePosition + 4, compress ? FLAG_COMPRESSED: 0);
            buf.position(buf.limit());
            incrementNumEntries();
        } finally {
            cont.discard();
        }

        return true;
    }

    BBContainer poll(OutputContainerFactory factory) throws IOException {
        if (!m_haveMAdvised) {
            m_haveMAdvised = true;
            final long retval = PosixAdvise.madvise(
                    m_bufAddr,
                    m_buf.capacity(),
                    PosixAdvise.POSIX_MADV_WILLNEED);
            if (retval != 0) {
                LOG.warn("madvise will need failed: " + retval);
            }
        }

        //No more entries to read
        int numEntries = getNumEntries();
        if (m_objectReadIndex == numEntries) {
            return null;
        }

        m_objectReadIndex++;

        //Get the length prefix and then read the object
        final int nextCompressedLength = m_readBuf.getInt();
        final int nextFlags = m_readBuf.getInt();

        //Check for compression
        final boolean compressed = nextFlags == FLAG_COMPRESSED;
        //Determine the length of the object if uncompressed
        final int nextUncompressedLength = compressed ? (int)Snappy.uncompressedLength(m_bufAddr + m_readBuf.position(), nextCompressedLength) : nextCompressedLength;

        if (compressed) {
            //Get storage for output
            final BBContainer retcont = factory.getContainer(nextUncompressedLength);
            final ByteBuffer retbuf = retcont.b;

            //Limit to appropriate uncompressed size
            retbuf.limit(nextUncompressedLength);

            //Uncompress to output buffer
            final long sourceAddr = m_bufAddr + m_readBuf.position();
            final long destAddr = retcont.address();
            Snappy.rawUncompress(sourceAddr, nextCompressedLength, destAddr);
            m_readBuf.position(m_readBuf.position() + nextCompressedLength);
            return retcont;
        } else {
            //Return a slice
            final int oldLimit = m_readBuf.limit();
            m_readBuf.limit(m_readBuf.position() + nextUncompressedLength);
            ByteBuffer retbuf = m_readBuf.slice();
            m_readBuf.position(m_readBuf.limit());
            m_readBuf.limit(oldLimit);
            return DBBPool.wrapBB(retbuf);
        }
    }

    int sizeInBytes() {
        return m_buf.position() - m_readBuf.position();
    }
}
