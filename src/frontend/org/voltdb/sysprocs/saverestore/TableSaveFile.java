/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.sysprocs.saverestore;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.zip.CRC32;

import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;

/**
 * An abstraction around a table's save file for restore.  Deserializes the
 * meta-data that was stored when the table was saved and makes it available
 * to clients.  This follows the structure in
 * src/ee/storage/TableDiskHeader.{h,cpp} and looks like:
 *
 * Header length - 4 octet integer
 * version       - 4 octet integer
 * Host ID       - 4 octet integer (this is the name, *not* the GUID)
 * Cluster name  - VoltDB serialized string (2 octet length followed by chars)
 * Database name - VoltDB serialized string
 * Table name    - VoltDB serialized string
 * isReplicated  - 1 octet, indicates whether the table was replicated
 *   The following fields are conditional on isReplicated == false
 * Partition Ids - Array of 4 octet integer ids for partitions in this file
 * Total Hosts - The number of hosts for this table when it was saved
 */
public class TableSaveFile
{

    private static class Container extends BBContainer {
        @SuppressWarnings("unused")
        private final BBContainer m_origin;
        Container(ByteBuffer b, long pointer, BBContainer origin) {
            super(b, pointer);
            m_origin = origin;
        }

        @Override
        public void discard() {
            m_buffers.add(this);
        }
    }

    /**
     * It is actually possible to make a bigger chunk then this if the table header is
     * big enough...
     */
    private static final int DEFAULT_CHUNKSIZE =
        org.voltdb.ExecutionSite.m_snapshotBufferLength + Short.MAX_VALUE;

    // XXX maybe consider an IOException subclass at some point
    public TableSaveFile(
            FileChannel dataIn,
            int readAheadChunks,
            int relevantPartitionIds[]) throws IOException
    {
        if (relevantPartitionIds == null) {
            m_relevantPartitionIds = null;
        } else {
            m_relevantPartitionIds = new HashSet<Integer>();
            for (Integer i : relevantPartitionIds) {
                m_relevantPartitionIds.add(i);
            }
        }
        m_chunkReads = new Semaphore(readAheadChunks);
        m_saveFile = dataIn;

        final CRC32 crc = new CRC32();

        /*
         * Get the header with the save restore specific information
         */
        final ByteBuffer lengthBuffer = ByteBuffer.allocate(8);
        while (lengthBuffer.hasRemaining()) {
            final int read = m_saveFile.read(lengthBuffer);
            if (read == -1) {
                throw new EOFException();
            }
        }
        lengthBuffer.flip();
        final int originalCRC = lengthBuffer.getInt();
        int length = lengthBuffer.getInt();
        crc.update(lengthBuffer.array(), 4, 4);

        if (length < 0) {
            throw new IOException("Corrupted save file has negative header length");
        }

        if (length > 2097152) {
            throw new IOException("Corrupted save file has unreasonable header length > 2 megs");
        }

        final ByteBuffer saveRestoreHeader = ByteBuffer.allocate(length);
        while (saveRestoreHeader.hasRemaining()) {
            final int read = m_saveFile.read(saveRestoreHeader);
            if (read == -1 || read < length) {
                throw new EOFException();
            }
        }
        saveRestoreHeader.flip();
        crc.update(saveRestoreHeader.array());

        /*
         *  Get the template for the VoltTable serialization header.
         *  It will have an extra length value preceded to it so that
         *  it can be sucked straight into a buffer. This will not
         *  contain a row count since that varies from chunk to chunk
         *  and is supplied by the chunk
         */
        lengthBuffer.clear();
        lengthBuffer.limit(2);
        /*
         * Why this stupidity and no while loop?
         * Because java is broken and complains about a random final
         * elsewhere if you do.
         */
        {
            final int read = m_saveFile.read(lengthBuffer);
            if (read == -1) {
                throw new EOFException();
            }
        }
        crc.update(lengthBuffer.array(), 0, 2);
        lengthBuffer.flip();
        length = lengthBuffer.getShort();

        if (length < 2) {
            throw new IOException("Corrupted save file has negative length or too small length for VoltTable header");
        }

        m_tableHeader = ByteBuffer.allocate(length + 2);
        m_tableHeader.putShort((short)length);
        while (m_tableHeader.hasRemaining()) {
            final int read = m_saveFile.read(m_tableHeader);
            if (read == -1) {
                throw new EOFException();
            }
        }
        crc.update(m_tableHeader.array(), 2, length);

        final int actualCRC = (int)crc.getValue();
        if (originalCRC != actualCRC) {
            throw new IOException("Checksum mismatch");
        }

        FastDeserializer fd = new FastDeserializer(saveRestoreHeader);
        m_completed = fd.readByte() == 1 ? true : false;
        for (int ii = 0; ii < 4; ii++) {
            m_versionNum[ii] = fd.readInt();
        }
        m_createTime = fd.readLong();
        m_hostId = fd.readInt();
        m_clusterName = fd.readString();
        m_databaseName = fd.readString();
        m_tableName = fd.readString();
        m_isReplicated = fd.readBoolean();
        if (!m_isReplicated) {
            m_partitionIds = (int[])fd.readArray(int.class);
            m_totalPartitions = fd.readInt();
        } else {
            m_partitionIds = new int[] {0};
            m_totalPartitions = 1;
        }
    }

    public int[] getVersionNumber()
    {
        return m_versionNum;
    }

    public int getHostId()
    {
        return m_hostId;
    }

    public String getClusterName()
    {
        return m_clusterName;
    }

    public String getDatabaseName()
    {
        return m_databaseName;
    }

    public String getTableName()
    {
        return m_tableName;
    }

    public int[] getPartitionIds() {
        return m_partitionIds;
    }

    public boolean isReplicated()
    {
        return m_isReplicated;
    }

    public int getTotalPartitions() {
        return m_totalPartitions;
    }

    public boolean getCompleted() {
        return m_completed;
    }

    public long getCreateTime() {
        return m_createTime;
    }

    public void close() throws IOException {
        m_saveFile.close();
    }

    public ByteBuffer getTableHeader() {
        return m_tableHeader;
    }

    // Will get the next chunk of the table that is just over the chunk size
    public synchronized BBContainer getNextChunk() throws IOException
    {
        if (m_chunkReader == null) {
            m_chunkReader = new ChunkReader();
            new Thread(m_chunkReader, "ChunkReader").start();
        }

        Container c = null;
        while (c == null && (m_hasMoreChunks || !m_availableChunks.isEmpty())) {
            c = m_availableChunks.poll();
            if (c == null) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (c != null) {
            m_chunkReads.release();
        }
        if (m_chunkReaderException != null) {
            throw m_chunkReaderException;
        }
        return c;
    }

    public synchronized boolean hasMoreChunks()
    {
        return m_hasMoreChunks || !m_availableChunks.isEmpty();
    }
//
//    /**
//     * A wrapper for the in memory storage for a table chunk
//     * that counts the number of times the chunk is discarded
//     * and only returns the memory back to the pool when the
//     * chunk has been read by enough times. This is necessary
//     * for replicated tables so that they only have to
//     *
//     */
//    private class ChunkCounter {
//
//        private ChunkCounter(BBContainer c, int chunkIndex) {
//            m_container = c;
//            m_chunkIndex = chunkIndex;
//        }
//
//        private BBContainer fetch() {
//            m_fetches++;
//            if (m_fetches == m_fetchCount) {
//                return m_container;
//            }
//        }
//
//        private final BBContainer m_container;
//        private int m_chunkIndex;
//        private int m_fetches = 0;
//    }

//    /**
//     * Number of times a chunk must be fetched before its buffer can
//     * be returned to the pool
//     */
//    private final int m_fetchCount;

    private final FileChannel m_saveFile;
    private final ByteBuffer m_tableHeader;
    private final boolean m_completed;
    private final int m_versionNum[] = new int[4];
    private final int m_hostId;
    private final String m_clusterName;
    private final String m_databaseName;
    private final String m_tableName;
    private final boolean m_isReplicated;
    private final int m_partitionIds[];
    private final int m_totalPartitions;
    private final long m_createTime;
    private boolean m_hasMoreChunks = true;
    private static ConcurrentLinkedQueue<Container> m_buffers = new ConcurrentLinkedQueue<Container>();
    private ArrayDeque<Container> m_availableChunks = new ArrayDeque<Container>();
    private final HashSet<Integer> m_relevantPartitionIds;

    /**
     * The thread reading chunks will read at most this number of chunks
     */
    private final Semaphore m_chunkReads;

    private ChunkReader m_chunkReader = null;
    private IOException m_chunkReaderException = null;

    /**
     * Thread to read chunks from the disk
     */
    private class ChunkReader implements Runnable {

        private void readChunks() {
            int chunksRead = 0;
            while (m_hasMoreChunks) {
                try {
                    m_chunkReads.acquire();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                try {

                    /*
                     * Get the length of the next chunk, crc, and partition id
                     */
                    ByteBuffer chunkLengthB = ByteBuffer.allocate(12);
                    while (chunkLengthB.hasRemaining()) {
                        final int read = m_saveFile.read(chunkLengthB);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    chunkLengthB.flip();
                    final int nextChunkLength = chunkLengthB.getInt();
                    final int nextChunkCRC = chunkLengthB.getInt();
                    final int nextChunkPartitionId = chunkLengthB.getInt();

                    if (nextChunkLength < 0) {
                        throw new IOException("Corrupted TableSaveFile chunk has negative chunk length");
                    }

                    if (nextChunkLength > 2097152) {
                        throw new IOException("Corrupted TableSaveFile chunk has unreasonable header length > 2 megs");
                    }

                    /*
                     * Now allocate space to store the chunk using the VoltTable serialization representation.
                     * The chunk will contain an integer row count preceding it so it can
                     * be sucked straight in. There is a little funny business to overwrite the
                     * partition id that is not part of the serialization format
                     */
                    Container c = m_buffers.poll();
                    if (c == null) {
                        final BBContainer originContainer = DBBPool.allocateDirect(DEFAULT_CHUNKSIZE);
                        final ByteBuffer b = originContainer.b;
                        final long pointer = org.voltdb.utils.DBBPool.getBufferAddress(b);
                        c = new Container(b, pointer, originContainer);
                    }

                    c.b.clear();
                    c.b.limit((nextChunkLength - 4)  + m_tableHeader.capacity());
                    m_tableHeader.position(0);
                    c.b.put(m_tableHeader);
                    final int checksumStartPosition = c.b.position();
                    c.b.position(c.b.position() + 4);//Leave space for row count to be moved
                    while (c.b.hasRemaining()) {
                        final int read = m_saveFile.read(c.b);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    c.b.position(c.b.position() - 4);
                    final int rowCount = c.b.getInt();
                    c.b.position(checksumStartPosition);
                    c.b.putInt(nextChunkPartitionId);
                    c.b.position(c.b.position() - 4);
                    final int calculatedCRC = DBBPool.getBufferCRC32(c.b, c.b.position(), c.b.remaining());

                    if (calculatedCRC != nextChunkCRC) {
                        throw new IOException("CRC mismatch in saved table chunk");
                    }

                    /*
                     * Skip irrelevant chunks after CRC is calculated
                     */
                    if (m_relevantPartitionIds != null) {
                        if (!m_relevantPartitionIds.contains(nextChunkPartitionId)) {
                            m_saveFile.position(m_saveFile.position() + (nextChunkLength - 8));
                        }
                    }

                    c.b.limit(c.b.limit() - 4);
                    c.b.position(checksumStartPosition);
                    c.b.putInt(rowCount);
                    c.b.position(0);
                    ++chunksRead;
                    synchronized (TableSaveFile.this) {
                        m_availableChunks.offer(c);
                        TableSaveFile.this.notifyAll();
                    }
                } catch (EOFException eof) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        TableSaveFile.this.notifyAll();
                    }
                } catch (IOException e) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        m_chunkReaderException = e;
                        TableSaveFile.this.notifyAll();
                    }
                }
            }
        }
        @Override
        public void run() {
            try {
                readChunks();
            } finally {
                synchronized (TableSaveFile.this) {
                    m_hasMoreChunks = false;
                    TableSaveFile.this.notifyAll();
                }
            }
        }

    }
}
