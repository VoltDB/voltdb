/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.sysprocs.saverestore;

import java.io.EOFException;
import java.io.FileDescriptor;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Semaphore;
import java.util.zip.Checksum;

import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.apache.hadoop_voltpatches.util.PureJavaCrc32C;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.TransactionIdManager;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Bits;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.EELibraryLoader;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.PosixAdvise;

/**
 * An abstraction around a table's save file for restore.  Deserializes the
 * meta-data that was stored when the table was saved and makes it available
 * to clients.  The meta data is stored as a JSON blob with length prefixing and a CRC
 * as well as a byte to that is set once the file is completely written and synced.
 * A VoltTable header describing the schema is follows the JSON blob.
 */
public class TableSaveFile
{

    public static enum ChecksumType {
        CRC32, CRC32C
    }

    public class Container extends BBContainer {
        public final int partitionId;
        private final BBContainer m_origin;
        private boolean discarded = false;
        Container(ByteBuffer b, BBContainer origin, int partitionId) {
            super(b);
            m_origin = origin;
            this.partitionId = partitionId;
        }

        @Override
        public void discard() {
            checkDoubleFree();
            discarded = true;
            if (m_hasMoreChunks == false) {
                m_origin.discard();
            } else {
                m_buffers.add(m_origin);
            }
        }

    }

    /**
     * It is actually possible to make a bigger chunk then this if the table header is
     * big enough...
     */
    private static final int DEFAULT_CHUNKSIZE =
            org.voltdb.SnapshotSiteProcessor.m_snapshotBufferLength + (1024 * 256);

    public TableSaveFile(
            FileInputStream fis,
            int readAheadChunks,
            Integer[] relevantPartitionIds) throws IOException {
        this(fis, readAheadChunks, relevantPartitionIds, false);
    }

    // XXX maybe consider an IOException subclass at some point
    public TableSaveFile(
            FileInputStream fis,
            int readAheadChunks,
            Integer[] relevantPartitionIds,
            boolean continueOnCorruptedChunk) throws IOException
            {
                m_fd = fis.getFD();
                FileChannel dataIn = fis.getChannel();
        try {
            EELibraryLoader.loadExecutionEngineLibrary(true);
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
            m_continueOnCorruptedChunk = continueOnCorruptedChunk;

            final PureJavaCrc32 crc = new PureJavaCrc32();
            /*
             * If the CRC check fails because the file wasn't completed
             */
            final PureJavaCrc32 secondCRC = new PureJavaCrc32();

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
            secondCRC.update(lengthBuffer.array(), 4, 4);

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
            secondCRC.update(new byte[] { 1 });
            secondCRC.update(saveRestoreHeader.array(), 1, saveRestoreHeader.array().length - 1);

            /*
             *  Get the template for the VoltTable serialization header.
             *  It will have an extra length value preceded to it so that
             *  it can be sucked straight into a buffer. This will not
             *  contain a row count since that varies from chunk to chunk
             *  and is supplied by the chunk
             */
            lengthBuffer.clear();
            lengthBuffer.limit(4);
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
            crc.update(lengthBuffer.array(), 0, 4);
            secondCRC.update(lengthBuffer.array(), 0, 4);
            lengthBuffer.flip();
            length = lengthBuffer.getInt();

            if (length < 4) {
                throw new IOException("Corrupted save file has negative length or too small length for VoltTable header");
            }

            if (length > 2097152) {
                throw new IOException("Corrupted save file has unreasonable VoltTable header length > 2 megs");
            }

            m_tableHeader = ByteBuffer.allocate(length + 4);
            m_tableHeader.putInt(length);
            while (m_tableHeader.hasRemaining()) {
                final int read = m_saveFile.read(m_tableHeader);
                if (read == -1) {
                    throw new EOFException();
                }
            }
            crc.update(m_tableHeader.array(), 4, length);
            secondCRC.update(m_tableHeader.array(), 4, length);

            boolean failedCRCDueToNotCompleted = false;

            final int actualCRC = (int)crc.getValue();
            if (originalCRC != actualCRC) {
                /*
                 * Check if the CRC mismatch is due to the snapshot not being completed
                 */
                final int secondCRCValue = (int)secondCRC.getValue();
                if (secondCRCValue == originalCRC) {
                    failedCRCDueToNotCompleted = true;
                } else {
                    throw new IOException("Checksum mismatch");
                }
            }

            FastDeserializer fd = new FastDeserializer(saveRestoreHeader);
            byte completedByte = fd.readByte();
            m_completed = failedCRCDueToNotCompleted ? false : (completedByte == 1 ? true : false);
            for (int ii = 0; ii < 4; ii++) {
                m_versionNum[ii] = fd.readInt();
            }

            /*
             * Support the original pre 1.3 header format as well as a new JSON format.
             * JSON will make it possible to add info to a snapshot header without
             * breaking backwards compatibility.
             */
            if (m_versionNum[3] == 0) {
                m_txnId = fd.readLong();
                m_timestamp = TransactionIdManager.getTimestampFromTransactionId(m_txnId);
                m_hostId = fd.readInt();
                m_hostname = fd.readString();
                m_clusterName = fd.readString();
                m_databaseName = fd.readString();
                m_tableName = fd.readString();
                m_isReplicated = fd.readBoolean();
                m_isCompressed = false;
                m_checksumType = ChecksumType.CRC32;
                if (!m_isReplicated) {
                    m_partitionIds = (int[])fd.readArray(int.class);
                    if (!m_completed) {
                        for (Integer partitionId : m_partitionIds) {
                            m_corruptedPartitions.add(partitionId);
                        }
                    }
                    m_totalPartitions = fd.readInt();
                } else {
                    m_partitionIds = new int[] {0};
                    m_totalPartitions = 1;
                    if (!m_completed) {
                        m_corruptedPartitions.add(0);
                    }
                }
                m_hasVersion2FormatChunks = false;
            } else {
                assert(m_versionNum[3] == 1 || m_versionNum[3] == 2);
                if (m_versionNum[3] >= 2) {
                    m_hasVersion2FormatChunks = true;
                } else {
                    m_hasVersion2FormatChunks = false;
                }
                int numJSONBytes = fd.readInt();
                byte jsonBytes[] = new byte[numJSONBytes];
                fd.readFully(jsonBytes);
                String jsonString = new String(jsonBytes, "UTF-8");
                JSONObject obj = new JSONObject(jsonString);

                m_txnId = obj.getLong("txnId");
                //Timestamp field added for 3.0, might not be there
                if (obj.has("timestamp")) {
                    m_timestamp = obj.getLong("timestamp");
                } else {
                    //Pre 3.0/IV2 the timestamp was in the transactionid
                    m_timestamp = TransactionIdManager.getTimestampFromTransactionId(m_txnId);
                }
                m_hostId = obj.getInt("hostId");
                m_hostname = obj.getString("hostname");
                m_clusterName = obj.getString("clusterName");
                m_databaseName = obj.getString("databaseName");
                m_tableName = obj.getString("tableName");
                m_isReplicated = obj.getBoolean("isReplicated");
                m_isCompressed = obj.optBoolean("isCompressed", false);
                m_checksumType = ChecksumType.valueOf(obj.optString("checksumType", "CRC32"));
                if (!m_isReplicated) {
                    JSONArray partitionIds = obj.getJSONArray("partitionIds");
                    m_partitionIds = new int[partitionIds.length()];
                    for (int ii = 0; ii < m_partitionIds.length; ii++) {
                        m_partitionIds[ii] = partitionIds.getInt(ii);
                    }

                    if (!m_completed) {
                        for (Integer partitionId : m_partitionIds) {
                            m_corruptedPartitions.add(partitionId);
                        }
                    }
                    m_totalPartitions = obj.getInt("numPartitions");
                } else {
                    m_partitionIds = new int[] {0};
                    m_totalPartitions = 1;
                    if (!m_completed) {
                        m_corruptedPartitions.add(0);
                    }
                }
            }
            /*
             * Several runtime exceptions can be thrown in valid failure cases where
             * a corrupt save file is being detected.
             */
        } catch (BufferUnderflowException e) {
            throw new IOException(e);
        } catch (BufferOverflowException e) {
            throw new IOException(e);
        } catch (IndexOutOfBoundsException e) {
            throw new IOException(e);
        } catch (JSONException e) {
            throw new IOException(e);
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

    public String getHostname()
    {
        return m_hostname;
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

    public boolean isCompressed() {
        return m_isCompressed;
    }

    public int getTotalPartitions() {
        return m_totalPartitions;
    }

    public boolean getCompleted() {
        return m_completed;
    }

    public long getTxnId() {
        return m_txnId;
    }

    public long getTimestamp() {
        return m_timestamp;
    }

    public void close() throws IOException {
        Thread chunkReader;
        synchronized (this) {
            m_hasMoreChunks = false;
            chunkReader = m_chunkReaderThread;
        }

        if (chunkReader != null) {
            chunkReader.interrupt();
            try {
                chunkReader.join();
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        }

        synchronized (this) {
            while (!m_availableChunks.isEmpty()) {
                m_availableChunks.poll().discard();
            }
            notifyAll();
        }

        /*
         * Free buffers used to pull snapshot data in process
         */
        BBContainer cont;
        while ((cont = m_buffers.poll()) != null) {
            cont.discard();
        }
    }

    public Set<Integer> getCorruptedPartitionIds() {
        return m_corruptedPartitions;
    }

    public ByteBuffer getTableHeader() {
        return m_tableHeader;
    }

    // Will get the next chunk of the table that is just over the chunk size
    public synchronized BBContainer getNextChunk() throws IOException
    {
        if (m_chunkReaderException != null) {
            throw m_chunkReaderException;
        }
        if (!m_hasMoreChunks) {
            final Container c = m_availableChunks.poll();
            return c;
        }

        if (m_chunkReader == null) {
            m_chunkReader = new ChunkReader();
            m_chunkReaderThread = new Thread(m_chunkReader, "ChunkReader");
            m_chunkReaderThread.start();
        }

        Container c = null;
        while (c == null && (m_hasMoreChunks || !m_availableChunks.isEmpty())) {
            c = m_availableChunks.poll();
            if (c == null) {
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        }
        if (c != null) {
            m_chunkReads.release();
        } else {
            if (m_chunkReaderException != null) {
                throw m_chunkReaderException;
            }
        }
        return c;
    }

    public synchronized boolean hasMoreChunks() throws IOException
    {
        if (m_chunkReaderException != null) {
            throw m_chunkReaderException;
        }
        return m_hasMoreChunks || !m_availableChunks.isEmpty();
    }

    private final FileChannel m_saveFile;
    private final FileDescriptor m_fd;
    private final ByteBuffer m_tableHeader;
    private final boolean m_completed;
    private final int m_versionNum[] = new int[4];
    private final int m_hostId;
    private final String m_hostname;
    private final String m_clusterName;
    private final String m_databaseName;
    private final String m_tableName;
    private final boolean m_isReplicated;
    private final boolean m_isCompressed;
    private final int m_partitionIds[];
    private final int m_totalPartitions;
    private final long m_txnId;
    private final long m_timestamp;
    private boolean m_hasMoreChunks = true;
    private ConcurrentLinkedQueue<BBContainer> m_buffers = new ConcurrentLinkedQueue<BBContainer>();
    private final ArrayDeque<Container> m_availableChunks = new ArrayDeque<Container>();
    private final HashSet<Integer> m_relevantPartitionIds;
    private final ChecksumType m_checksumType;

    /*
     * In version 2 the layout of chunks was rejiggered to do less work
     * in execution sites. The checksum is done after the compression so the layout
     * of the block is very different.
     */
    private final boolean m_hasVersion2FormatChunks;

    /**
     * Maintain a list of corrupted partitions. It is possible for uncorrupted partitions
     * to be recovered from a save file in the future
     */
    private final HashSet<Integer> m_corruptedPartitions = new HashSet<Integer>();

    /**
     * Ignore corrupted chunks and continue validation of the rest of the chunks.
     */
    private final boolean m_continueOnCorruptedChunk;

    /**
     * The thread reading chunks will read at most this number of chunks
     */
    private final Semaphore m_chunkReads;

    private ChunkReader m_chunkReader = null;
    private Thread m_chunkReaderThread = null;
    private IOException m_chunkReaderException = null;

    /**
     * Thread to read chunks from the disk
     */
    private class ChunkReader implements Runnable {

        /*
         * The old method was out of hand. Going to start a new one with a different format
         * that should be easier to understand and validate.
         */
        private void readChunksV2() {
            //For reading the compressed input.
            final BBContainer fileInputBufferC =
                    DBBPool.allocateDirect(CompressionService.maxCompressedLength(DEFAULT_CHUNKSIZE));
            final ByteBuffer fileInputBuffer = fileInputBufferC.b();
            long sinceLastFAdvise = Long.MAX_VALUE;
            long positionAtLastFAdvise = 0;
            while (m_hasMoreChunks) {
                if (sinceLastFAdvise > 1024 * 1024 * 48) {
                    sinceLastFAdvise = 0;
                    VoltLogger log = new VoltLogger("SNAPSHOT");
                    try {
                        final long position = m_saveFile.position();
                        long retval = PosixAdvise.fadvise(
                                m_fd,
                                position,
                                position + 1024 * 1024 * 64,
                                PosixAdvise.POSIX_FADV_WILLNEED);
                        if (retval != 0) {
                            log.info("Failed to fadvise in TableSaveFile, this is harmless: " + retval);
                        }

                        //Get aligned start and end position
                        final long fadviseStart = positionAtLastFAdvise;
                        //-1 because we don't want to drop the last page because
                        //We will be reading it soon
                        positionAtLastFAdvise = ((position / Bits.pageSize()) - 1) * Bits.pageSize();
                        final long length = positionAtLastFAdvise - fadviseStart;
                        if (length > 0) {
                            retval = PosixAdvise.fadvise(
                                    m_fd,
                                    fadviseStart,
                                    length,
                                    PosixAdvise.POSIX_FADV_DONTNEED);
                        }
                        if (retval != 0) {
                            log.info("Failed to fadvise in TableSaveFile, this is harmless: " + retval);
                        }
                        positionAtLastFAdvise = position;
                    } catch (Throwable t) {
                        log.info("Exception attempting fadvise", t);
                    }
                }

                /*
                 * Limit the number of chunk materialized into memory at one time
                 */
                try {
                    m_chunkReads.acquire();
                } catch (InterruptedException e) {
                    return;
                }
                boolean expectedAnotherChunk = false;
                Container c = null;
                try {

                    /*
                     * Get the length of the next chunk, partition id, crc for partition id, and length prefix,
                     * and then the CRC of the compressed payload
                     */
                    ByteBuffer chunkLengthB = ByteBuffer.allocate(16);
                    while (chunkLengthB.hasRemaining()) {
                        final int read = m_saveFile.read(chunkLengthB);
                        if (read == -1) {
                            throw new EOFException();
                        }
                        sinceLastFAdvise += read;
                    }
                    int nextChunkLength = chunkLengthB.getInt(0);
                    expectedAnotherChunk = true;

                    /*
                     * Get the partition id and its CRC (CRC now covers length prefix) and validate it. Validating the
                     * partition ID for the chunk separately makes it possible to
                     * continue processing chunks from other partitions if only one partition
                     * has corrupt chunks in the file.
                     */
                    assert(m_checksumType == ChecksumType.CRC32C);
                    final Checksum partitionIdCRC = new PureJavaCrc32C();
                    final int nextChunkPartitionId = chunkLengthB.getInt(4);
                    final int nextChunkPartitionIdCRC = chunkLengthB.getInt(8);

                    partitionIdCRC.update(chunkLengthB.array(), 0, 8);
                    int generatedValue = (int)partitionIdCRC.getValue();
                    if (generatedValue != nextChunkPartitionIdCRC) {
                        chunkLengthB.position(0);
                        for (int partitionId : m_partitionIds) {
                            m_corruptedPartitions.add(partitionId);
                        }
                        throw new IOException("Chunk partition ID CRC check failed. " +
                                "This corrupts all partitions in this file");
                    }

                    /*
                     * CRC for the data portion of the chunk
                     */
                    final int nextChunkCRC = chunkLengthB.getInt(12);

                    /*
                     * Sanity check the length value to ensure there isn't
                     * a runtime exception or OOM.
                     */
                    if (nextChunkLength < 0) {
                        throw new IOException("Corrupted TableSaveFile chunk has negative chunk length");
                    }

                    if (nextChunkLength > fileInputBuffer.capacity()) {
                        throw new IOException("Corrupted TableSaveFile chunk has unreasonable length " +
                                "> DEFAULT_CHUNKSIZE bytes");
                    }

                    /*
                     * Go fetch the compressed data so that the uncompressed size is known
                     * and use that to set nextChunkLength to be the uncompressed length,
                     * the code ahead that constructs the volt table is expecting
                     * the uncompressed size/data since it is producing an uncompressed table
                     */
                    fileInputBuffer.clear();
                    fileInputBuffer.limit(nextChunkLength);
                    while (fileInputBuffer.hasRemaining()) {
                        final int read = m_saveFile.read(fileInputBuffer);
                        if (read == -1) {
                            throw new EOFException();
                        }
                        sinceLastFAdvise += read;
                    }
                    fileInputBuffer.flip();
                    nextChunkLength = CompressionService.uncompressedLength(fileInputBuffer);

                    /*
                     * Validate the rest of the chunk. This can fail if the data is corrupted
                     * or the length value was corrupted.
                     */
                    final int calculatedCRC =
                            DBBPool.getBufferCRC32C(fileInputBuffer, 0, fileInputBuffer.remaining());
                    if (calculatedCRC != nextChunkCRC) {
                        m_corruptedPartitions.add(nextChunkPartitionId);
                        if (m_continueOnCorruptedChunk) {
                            m_chunkReads.release();
                            continue;
                        } else {
                            throw new IOException("CRC mismatch in saved table chunk");
                        }
                    }

                    /*
                     * Now allocate space to store the chunk using the VoltTable serialization representation.
                     * The chunk will contain an integer row count preceding it so it can
                     * be sucked straight in. There is a little funny business to overwrite the
                     * partition id that is not part of the serialization format
                     */
                    c = getOutputBuffer(nextChunkPartitionId);

                    /*
                     * If the length value is wrong or not all data made it to disk this read will
                     * not complete correctly. There could be overflow, underflow etc.
                     * so use a try finally block to indicate that all partitions are now corrupt.
                     * The enclosing exception handlers will do the right thing WRT to
                     * propagating the error and closing the file.
                     */
                    boolean completedRead = false;
                    try {
                        final ByteBuffer buf = c.b();
                        /*
                         * Assemble a VoltTable out of the chunk of tuples.
                         * Put in the header that was cached in the constructor,
                         * then copy the tuple data.
                         */
                        buf.clear();
                        buf.limit(nextChunkLength  + m_tableHeader.capacity());
                        m_tableHeader.position(0);
                        buf.put(m_tableHeader);
                        //Doesn't move buffer position, does change the limit
                        CompressionService.decompressBuffer(fileInputBuffer, buf);
                        completedRead = true;
                    } finally {
                        if (!completedRead) {
                            for (int partitionId : m_partitionIds) {
                                m_corruptedPartitions.add(partitionId);
                            }
                            if (m_continueOnCorruptedChunk) {
                                m_chunkReads.release();
                                continue;
                            } else {
                                throw new IOException("Failed decompression of saved table chunk");
                            }
                        }
                    }

                    /*
                     * Skip irrelevant chunks after CRC is calculated. Always calulate the CRC
                     * in case it is the length value that is corrupted
                     */
                    if (m_relevantPartitionIds != null) {
                        if (!m_relevantPartitionIds.contains(nextChunkPartitionId)) {
                            m_chunkReads.release();
                            continue;
                        }
                    }

                    /*
                     * VoltTable wants the buffer at the home position 0
                     */
                    c.b().position(0);

                    synchronized (TableSaveFile.this) {
                        m_availableChunks.offer(c);
                        c = null;
                        TableSaveFile.this.notifyAll();
                    }
                } catch (EOFException eof) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        if (expectedAnotherChunk) {
                            m_chunkReaderException = new IOException(
                                    "Expected to find another chunk but reached end of file instead");
                        }
                        TableSaveFile.this.notifyAll();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        m_chunkReaderException = e;
                        TableSaveFile.this.notifyAll();
                    }
                } catch (BufferUnderflowException e) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        m_chunkReaderException = new IOException(e);
                        TableSaveFile.this.notifyAll();
                    }
                } catch (BufferOverflowException e) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        m_chunkReaderException = new IOException(e);
                        TableSaveFile.this.notifyAll();
                    }
                } catch (IndexOutOfBoundsException e) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        m_chunkReaderException = new IOException(e);
                        TableSaveFile.this.notifyAll();
                    }
                } finally {
                    if (c != null) c.discard();
                }
            }
            fileInputBufferC.discard();
        }

        private void readChunks() {
            //For reading the compressed input.
            BBContainer fileInputBufferC =
                    DBBPool.allocateDirect(CompressionService.maxCompressedLength(DEFAULT_CHUNKSIZE));
            ByteBuffer fileInputBuffer = fileInputBufferC.b();
            while (m_hasMoreChunks) {
                /*
                 * Limit the number of chunk materialized into memory at one time
                 */
                try {
                    m_chunkReads.acquire();
                } catch (InterruptedException e) {
                    return;
                }
                boolean expectedAnotherChunk = false;
                Container c = null;
                try {

                    /*
                     * Get the length of the next chunk, partition id, crc for partition id,
                     */
                    ByteBuffer chunkLengthB = ByteBuffer.allocate(16);
                    while (chunkLengthB.hasRemaining()) {
                        final int read = m_saveFile.read(chunkLengthB);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    chunkLengthB.flip();
                    int nextChunkLength = chunkLengthB.getInt();
                    expectedAnotherChunk = true;

                    /*
                     * Get the partition id and its CRC and validate it. Validating the
                     * partition ID for the chunk separately makes it possible to
                     * continue processing chunks from other partitions if only one partition
                     * has corrupt chunks in the file.
                     */
                    final Checksum partitionIdCRC = m_checksumType == ChecksumType.CRC32C ? new PureJavaCrc32C() : new PureJavaCrc32();
                    chunkLengthB.mark();
                    final int nextChunkPartitionId = chunkLengthB.getInt();
                    final int nextChunkPartitionIdCRC = chunkLengthB.getInt();
                    chunkLengthB.reset();
                    byte partitionIdBytes[] = new byte[4];
                    chunkLengthB.get(partitionIdBytes);
                    partitionIdCRC.update(partitionIdBytes, 0, partitionIdBytes.length);
                    int generatedValue = (int)partitionIdCRC.getValue();
                    if (generatedValue != nextChunkPartitionIdCRC) {
                        chunkLengthB.position(0);
                        for (int partitionId : m_partitionIds) {
                            m_corruptedPartitions.add(partitionId);
                        }
                        throw new IOException("Chunk partition ID CRC check failed. " +
                                "This corrupts all partitions in this file");
                    }

                    /*
                     * CRC for the data portion of the chunk
                     */
                    chunkLengthB.position(chunkLengthB.position() + 4);
                    final int nextChunkCRC = chunkLengthB.getInt();

                    /*
                     * Sanity check the length value to ensure there isn't
                     * a runtime exception or OOM.
                     */
                    if (nextChunkLength < 0) {
                        throw new IOException("Corrupted TableSaveFile chunk has negative chunk length");
                    }

                    if (isCompressed()) {
                        if (nextChunkLength > fileInputBuffer.capacity()) {
                            throw new IOException("Corrupted TableSaveFile chunk has unreasonable length " +
                                    "> DEFAULT_CHUNKSIZE bytes");
                        }
                    } else {
                        if (nextChunkLength > DEFAULT_CHUNKSIZE) {
                            throw new IOException("Corrupted TableSaveFile chunk has unreasonable length " +
                                    "> DEFAULT_CHUNKSIZE bytes");
                        }
                    }

                    /*
                     * Go fetch the compressed data so that the uncompressed size is known
                     * and use that to set nextChunkLength to be the uncompressed length,
                     * the code ahead that constructs the volt table is expecting
                     * the uncompressed size/data since it is producing an uncompressed table
                     */
                    if (isCompressed()) {
                        fileInputBuffer.clear();
                        fileInputBuffer.limit(nextChunkLength);
                        while (fileInputBuffer.hasRemaining()) {
                            final int read = m_saveFile.read(fileInputBuffer);
                            if (read == -1) {
                                throw new EOFException();
                            }
                        }
                        fileInputBuffer.flip();
                        nextChunkLength = CompressionService.uncompressedLength(fileInputBuffer);
                    }

                    /*
                     * Now allocate space to store the chunk using the VoltTable serialization representation.
                     * The chunk will contain an integer row count preceding it so it can
                     * be sucked straight in. There is a little funny business to overwrite the
                     * partition id that is not part of the serialization format
                     */
                    c = getOutputBuffer(nextChunkPartitionId);

                    /*
                     * If the length value is wrong or not all data made it to disk this read will
                     * not complete correctly. There could be overflow, underflow etc.
                     * so use a try finally block to indicate that all partitions are now corrupt.
                     * The enclosing exception handlers will do the right thing WRT to
                     * propagating the error and closing the file.
                     */
                    boolean completedRead = false;
                    int checksumStartPosition = 0;
                    int rowCount = 0;
                    try {
                        /*
                         * Assemble a VoltTable out of the chunk of tuples.
                         * Put in the header that was cached in the constructor,
                         * then copy the tuple data. The row count is at the end
                         * because it isn't known until serialization is complete.
                         * It will have to be moved back to the beginning of the tuple data
                         * after the header once the CRC has been calculated.
                         */
                        c.b().clear();
                        //The length of the chunk already includes space for the 4-byte row count
                        //even though it is at the end, but we need to also leave at the end for the CRC calc
                        if (isCompressed()) {
                            c.b().limit(nextChunkLength  + m_tableHeader.capacity() + 4);
                        } else {
                            //Before compression the chunk length included the stuff added in the EE
                            //like the 2 CRCs and partition id. It is only -8 because we still need the 4-bytes
                            //of padding to move the row count in when constructing the volt table format.
                            c.b().limit((nextChunkLength - 8)  + m_tableHeader.capacity());
                        }
                        m_tableHeader.position(0);
                        c.b().put(m_tableHeader);
                        c.b().position(c.b().position() + 4);//Leave space for row count to be moved into
                        checksumStartPosition = c.b().position();
                        if (isCompressed()) {
                            CompressionService.decompressBuffer(fileInputBuffer, c.b());
                            c.b().position(c.b().limit());
                        } else {
                            while (c.b().hasRemaining()) {
                                final int read = m_saveFile.read(c.b());
                                if (read == -1) {
                                    throw new EOFException();
                                }
                            }
                        }
                        c.b().position(c.b().position() - 4);
                        rowCount = c.b().getInt();
                        c.b().position(checksumStartPosition);
                        completedRead = true;
                    } finally {
                        if (!completedRead) {
                            for (int partitionId : m_partitionIds) {
                                m_corruptedPartitions.add(partitionId);
                            }
                        }
                    }

                    /*
                     * Validate the rest of the chunk. This can fail if the data is corrupted
                     * or the length value was corrupted.
                     */
                    final int calculatedCRC =
                            m_checksumType == ChecksumType.CRC32C  ?
                                    DBBPool.getCRC32C(c.address(), c.b().position(), c.b().remaining()) :
                                        DBBPool.getCRC32(c.address(), c.b().position(), c.b().remaining());
                    if (calculatedCRC != nextChunkCRC) {
                        m_corruptedPartitions.add(nextChunkPartitionId);
                        if (m_continueOnCorruptedChunk) {
                            m_chunkReads.release();
                            continue;
                        } else {
                            throw new IOException("CRC mismatch in saved table chunk");
                        }
                    }

                    /*
                     * Skip irrelevant chunks after CRC is calculated. Always calulate the CRC
                     * in case it is the length value that is corrupted
                     */
                    if (m_relevantPartitionIds != null) {
                        if (!m_relevantPartitionIds.contains(nextChunkPartitionId)) {
                            m_chunkReads.release();
                            continue;
                        }
                    }

                    /*
                     * The row count which was stored on disk at the end (and for the CRC calc)
                     * is now moved to the appropriate place for the table serialization format.
                     * Update the limit to reflect that.
                     *
                     * Surrounded in a try finally just in case there is overflow/underflow. Shouldn't
                     * happen but I could be wrong.
                     */
                    boolean success = false;
                    try {
                        c.b().limit(c.b().limit() - 4);
                        c.b().position(checksumStartPosition - 4);
                        c.b().putInt(rowCount);
                        c.b().position(0);
                        success = true;
                    } finally {
                        if (!success) {
                            for (int partitionId : m_partitionIds) {
                                m_corruptedPartitions.add(partitionId);
                            }
                        }
                    }

                    synchronized (TableSaveFile.this) {
                        m_availableChunks.offer(c);
                        c = null;
                        TableSaveFile.this.notifyAll();
                    }
                } catch (EOFException eof) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        if (expectedAnotherChunk) {
                            m_chunkReaderException = new IOException(
                                    "Expected to find another chunk but reached end of file instead");
                        }
                        TableSaveFile.this.notifyAll();
                    }
                } catch (IOException e) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        m_chunkReaderException = e;
                        TableSaveFile.this.notifyAll();
                    }
                } catch (BufferUnderflowException e) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        m_chunkReaderException = new IOException(e);
                        TableSaveFile.this.notifyAll();
                    }
                } catch (BufferOverflowException e) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        m_chunkReaderException = new IOException(e);
                        TableSaveFile.this.notifyAll();
                    }
                } catch (IndexOutOfBoundsException e) {
                    synchronized (TableSaveFile.this) {
                        m_hasMoreChunks = false;
                        m_chunkReaderException = new IOException(e);
                        TableSaveFile.this.notifyAll();
                    }
                } finally {
                    if (c != null) c.discard();
                }
            }
            fileInputBufferC.discard();
        }
        private Container getOutputBuffer(final int nextChunkPartitionId) {
            BBContainer c = m_buffers.poll();
            if (c == null) {
                final BBContainer originContainer = DBBPool.allocateDirect(DEFAULT_CHUNKSIZE);
                final ByteBuffer b = originContainer.b();
                final Container retcont = new Container(b, originContainer, nextChunkPartitionId);
                return retcont;
            }
            /*
             * Need to reconstruct the container with the partition id of the next
             * chunk so it can be a final public field. The buffer, address, and origin
             * container remain the same.
             */
            final Container retcont = new Container(c.b(), c, nextChunkPartitionId);
            return retcont;
        }

        @Override
        public void run() {
            try {
                if (m_hasVersion2FormatChunks) {
                    readChunksV2();
                } else {
                    readChunks();
                }
            } finally {
                synchronized (TableSaveFile.this) {
                    m_hasMoreChunks = false;
                    TableSaveFile.this.notifyAll();
                    try {
                        m_saveFile.close();
                    } catch (IOException e) {
                    }
                }
            }
        }

    }
}
