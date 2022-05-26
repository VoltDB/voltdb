/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Iterator;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Table;
import org.voltdb.exportclient.ExportRowSchema;
import org.voltdb.exportclient.PersistedMetadata;
import org.voltdb.exportclient.PersistedMetadataSerializer;
import org.voltdb.utils.BinaryDeque;
import org.voltdb.utils.BinaryDeque.BinaryDequeScanner;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltdb.utils.BinaryDeque.BinaryDequeValidator;
import org.voltdb.utils.BinaryDeque.TruncatorResponse;
import org.voltdb.utils.BinaryDequeReader;
import org.voltdb.utils.PersistentBinaryDeque;
import org.voltdb.utils.PersistentBinaryDeque.ByteBufferTruncatorResponse;

/**
 * A customized queue for StreamBlocks that contain export data. The queue is able to
 * overflow to disk when more then two stream blocks are stored
 * as well as persist to disk when sync is invoked. Right now sync doesn't actually do an fsync on
 * the file unless it is specifically requested. It just pushed the two in memory blocks to the persistent
 * portion of the queue.
 *
 * Export PBD buffer layout:
 *    -- Segment Header ---
 *    (defined in PBDSegment.java, see comments for segment header layout)
 *
 *    -- Export Extra Segment Header ---
 *    exportVersion(1) + generationId(8) + schemaLen(4) + tupleSchema(var length) +
 *    tableNameLength(4) + tableName(var length) + colNameLength(4) + colName(var length) +
 *    colType(1) + colLength(4) + ...
 *
 *    --- Common Entry Header   ---
 *   (defined in PBDSegment.java, see comments for entry header layout)
 *
 *    --- Export Entry Header   ---
 *    seqNo(8) + committedSeqNo(8) + tupleCount(4) + uniqueId(8)
 *
 *    --- Row Header      ---
 *    rowLength(4) + partitionColumnIndex(4) + columnCount(4, includes metadata columns) +
 *    nullArrayLength(4) + nullArray(var length)
 *
 *    --- Metadata        ---
 *    TxnId(8) + timestamp(8) + seqNo(8) + partitionId(8) + siteId(8) + exportOperation(1)
 *
 *    --- Row Data        ---
 *    rowData(var length)
 *
 *    repeat row header, meta data and row data...
 */
public class StreamBlockQueue {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

    public static final String EXPORT_DISABLE_COMPRESSION_OPTION = "EXPORT_DISABLE_COMPRESSION";
    private static final boolean DISABLE_COMPRESSION = Boolean.getBoolean(EXPORT_DISABLE_COMPRESSION_OPTION);

    /**
     * Deque containing reference to stream blocks that are in memory. Some of these
     * stream blocks may still be persisted to disk others are stored completely in memory
     */
    private final ArrayDeque<StreamBlock> m_memoryDeque = new ArrayDeque<StreamBlock>();

    /**
     * A deque for persisting data to disk both for persistence and as a means of overflowing storage
     */
    private BinaryDeque<PersistedMetadata> m_persistentDeque;

    private final String m_nonce;
    private final String m_path;
    private final int m_partitionId;
    private final String m_streamName;
    // The initial generation id of the stream that SBQ currently represents.
    private long m_initialGenerationId;
    private BinaryDequeReader<PersistedMetadata> m_reader;

    public StreamBlockQueue(String path, String nonce, String streamName, int partitionId, long genId)
            throws java.io.IOException {
        // Not a creation by default
        this(path, nonce, streamName, partitionId, genId, false);
    }

    public StreamBlockQueue(String path, String nonce, String streamName, int partitionId, long genId, boolean create)
            throws java.io.IOException {
        m_path = path;
        m_nonce = nonce;
        m_streamName = streamName;
        m_partitionId = partitionId;
        m_initialGenerationId = genId;

        // When creating, delete any existing PBD files
        constructPBD(genId, m_initialGenerationId, create);
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(m_nonce + " At SBQ creation, PBD size is " +
                    (m_reader.sizeInBytes() - (8 * m_reader.getNumObjects())) +
                    " initial generation ID is " + m_initialGenerationId);
        }
    }

    boolean isEmpty() throws IOException {
        if (m_memoryDeque.isEmpty() && m_reader.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * Wrapper around the common operation of pulling an element out of the persistent deque.
     * The behavior is complicated (and might change) since the persistent deque can throw an IOException.
     * The poll always removes the element from the persistent queue (although not necessarily removing the
     * file backing, that happens at deleteContents) and will add a reference to the block to the in memory
     * deque unless actuallyPoll is true, in which case the polled block ownership is transferred to the caller.
     *
     * @param actuallyPoll true if this is an actual poll transferring the block to the caller
     * @return the polled block
     */
    private StreamBlock pollPersistentDeque(boolean actuallyPoll) {

        BinaryDequeReader.Entry<PersistedMetadata> entry = null;
        StreamBlock block = null;
        try {
            entry = m_reader.pollEntry(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            if (entry != null) {
                block = StreamBlock.from(entry);

                // Optionally store a reference to the block in the in memory deque
                // Note that any in-memory block must have a schema
                if (!actuallyPoll) {
                    assert(entry.getExtraHeader() != null);
                    m_memoryDeque.offer(block);
                }
            }
        }
        catch (Exception e) {
            exportLog.error("Failed to poll from persistent binary deque", e);
        }
        return block;
    }

    /*
     * Present an iterator that is backed by the blocks
     * that are already loaded as well as blocks that
     * haven't been polled from the persistent deque.
     *
     * The iterator wraps an iterator from the memoryDeque,
     * and regenerates it every time an element is added to the memoryDeque from
     * the persistent deque.
     */
     Iterator<StreamBlock> iterator() {
        return new Iterator<StreamBlock>() {
            private Iterator<StreamBlock> m_memoryIterator = m_memoryDeque.iterator();
            @Override
            public boolean hasNext() {
                if (m_memoryIterator.hasNext()) {
                    return true;
                } else {
                    if (pollPersistentDeque(false) != null) {
                        m_memoryIterator = m_memoryDeque.iterator();
                        for (int ii = 0; ii < m_memoryDeque.size() - 1; ii++) {
                            m_memoryIterator.next();
                        }
                        return true;
                    }
                }
                return false;
            }

            @Override
            public StreamBlock next() {
                if (m_memoryIterator.hasNext()) {
                    return m_memoryIterator.next();
                }

                StreamBlock block = pollPersistentDeque(false);
                if (block == null) {
                    throw new java.util.NoSuchElementException();
                } else {
                    m_memoryIterator = m_memoryDeque.iterator();
                    for (int ii = 0; ii < m_memoryDeque.size(); ii++) {
                        m_memoryIterator.next();
                    }
                    return block;
                }
            }

            @Override
            public void remove() {
                m_memoryIterator.remove();
            }
        };
    }

    StreamBlock peek() {
        if (m_memoryDeque.peek() != null) {
            return m_memoryDeque.peek();
        }
        return pollPersistentDeque(false);
    }

    // For test
    StreamBlock poll() {
        StreamBlock sb = null;
        if (m_memoryDeque.peek() != null) {
            sb = m_memoryDeque.poll();
        } else {
            sb = pollPersistentDeque(true);
        }
        return sb;
    }

    StreamBlock pop() {
        if (m_memoryDeque.isEmpty()) {
            StreamBlock sb = pollPersistentDeque(true);
            if (sb == null) {
                throw new java.util.NoSuchElementException();
            }
            return sb;
        } else {
            return m_memoryDeque.pop();
        }
    }

    void updateSchema(PersistedMetadata metadata) throws IOException {
        m_persistentDeque.updateExtraHeader(metadata);
    }

    /*
     * Only allow two blocks in memory, put the rest in the persistent deque
     */
    void offer(StreamBlock streamBlock) throws IOException {
        m_persistentDeque.offer(streamBlock.asBBContainer());
        long unreleasedSeqNo = streamBlock.unreleasedSequenceNumber();
        if (m_memoryDeque.size() < 2) {
            StreamBlock fromPBD = pollPersistentDeque(false);
            if ((streamBlock.startSequenceNumber() == fromPBD.startSequenceNumber()) &&
                    (unreleasedSeqNo > streamBlock.startSequenceNumber())) {
                fromPBD.releaseTo(unreleasedSeqNo - 1);
            }
        }
    }

    void sync() throws IOException {
        m_persistentDeque.sync();
    }

    // Only used in tests, should be removed.
    long sizeInBytes() throws IOException {
        long memoryBlockUsage = 0;
        for (StreamBlock b : m_memoryDeque) {
            //Use only total size, but throw in the USO
            //to make book keeping consistent when flushed to disk
            //Also dont count persisted blocks.
            memoryBlockUsage += b.totalSize();
        }
        //Subtract USO from on disk size
        return memoryBlockUsage + m_reader.sizeInBytes() - (StreamBlock.HEADER_SIZE * m_reader.getNumObjects());
    }

    void close() throws IOException {
        sync();
        m_persistentDeque.close();
        for (StreamBlock sb : m_memoryDeque) {
            sb.discard();
        }
        m_memoryDeque.clear();
    }

    void closeAndDelete() throws IOException {
        m_persistentDeque.closeAndDelete();
        for (StreamBlock sb : m_memoryDeque) {
            sb.discard();
        }
    }

    // See PDB segment layout at beginning of this file.
    void truncateToSequenceNumber(final long truncationSeqNo) throws IOException {
        assert(m_memoryDeque.isEmpty());
        m_persistentDeque.parseAndTruncate(new BinaryDequeTruncator() {

            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                ByteOrder endianness = b.order();
                b.order(ByteOrder.LITTLE_ENDIAN);
                try {
                    final long startSequenceNumber = b.getLong();
                    // If the truncation is before the first row in the block, the entire block is to be discarded
                    if (startSequenceNumber > truncationSeqNo) {
                        return PersistentBinaryDeque.fullTruncateResponse();
                    }
                    b.getLong(); // committedSequenceNumber
                    final int tupleCountPos = b.position();
                    final int tupleCount = b.getInt();
                    // There is nothing to do with this buffer
                    final long lastSequenceNumber = startSequenceNumber + tupleCount - 1;
                    if (lastSequenceNumber <= truncationSeqNo) {
                        return null;
                    }
                    b.getLong(); // uniqueId

                    // Partial truncation
                    int offset = 0;
                    while (b.hasRemaining()) {
                        if (startSequenceNumber + offset > truncationSeqNo) {
                            // The sequence number of this row is the greater than the truncation sequence number.
                            // Don't want this row, but want to preserve all rows before it.
                            // Move back before the row length prefix, txnId and header
                            // Return everything in the block before the truncation point.
                            // Indicate this is the end of the interesting data.
                            b.limit(b.position());
                            // update tuple count in the header
                            b.putInt(tupleCountPos, offset);
                            b.position(0);
                            return new ByteBufferTruncatorResponse(b);
                        }
                        offset++;
                        // Not the row we are looking to truncate at. Skip past it (row length + row length field).
                        final int rowLength = b.getInt();
                        b.position(b.position() + rowLength);
                    }
                    return null;
                } finally {
                    b.order(endianness);
                }
            }
        });
        // close reopen reader
        m_persistentDeque.close();
        CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
        constructPBD(catalogContext.m_genId, m_initialGenerationId, false);
        // temporary debug stmt
        exportLog.info("After truncate, PBD size is " + (m_reader.sizeInBytes() - (8 * m_reader.getNumObjects())));
    }

    /**
     * Every time the initial generation ID changes, stream's PBD header needs to update as well.
     * Database recover or node rejoin may cause the initial generation ID to change.
     * @param initialGenId
     * @throws IOException
     */
    void setInitialGenerationId(long initialGenId) throws IOException {
        exportLog.info("SBQ updated the initial generation id from " + m_initialGenerationId + " to " + initialGenId);
        m_initialGenerationId = initialGenId;
        CatalogContext catalogContext = VoltDB.instance().getCatalogContext();
        Table streamTable = catalogContext.database.getTables().get(m_streamName);
        PersistedMetadata metadata = new PersistedMetadata(streamTable, m_partitionId, m_initialGenerationId, catalogContext.m_genId);
        m_persistentDeque.updateExtraHeader(metadata);
    }

    ExportSequenceNumberTracker scanForGap() throws IOException {
        ExportSequenceNumberTracker tracker = new ExportSequenceNumberTracker();
        m_persistentDeque.scanEntries(new BinaryDequeScanner() {
            @Override
            public long scan(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                ByteOrder endianness = b.order();
                b.order(ByteOrder.LITTLE_ENDIAN);
                final long startSequenceNumber = b.getLong();
                b.getLong(); // committed sequence number
                final int tupleCount = b.getInt();
                final long endSequenceNumber = startSequenceNumber + tupleCount - 1;
                b.order(endianness);
                tracker.addRange(startSequenceNumber, endSequenceNumber);
                return endSequenceNumber;
            }

        });
        return tracker;
    }

    boolean deleteStaleBlocks(long generationId) throws IOException {
        boolean didCleanup = m_persistentDeque.deletePBDSegment(new BinaryDequeValidator<PersistedMetadata>() {

            @Override
            public boolean isStale(PersistedMetadata metadata) {
                assert (metadata != null);
                ExportRowSchema schema = metadata.getSchema();
                boolean fromOlderGeneration = schema.initialGenerationId < generationId;
                if (fromOlderGeneration) {
                    exportLog.info("Delete PBD segments of " + schema.tableName + "_" + schema.partitionId
                            + " from older generation " + schema.initialGenerationId);
                }
                return fromOlderGeneration;
            }

        });
        return didCleanup;
    }

    long getGenerationIdCreated() {
        return m_initialGenerationId;
    }

    private void constructPBD(long genId, long initialGenId, boolean deleteExisting) throws IOException {
        Table streamTable = VoltDB.instance().getCatalogContext().database.getTables().get(m_streamName);

        PersistedMetadata metadata = new PersistedMetadata(streamTable, m_partitionId, initialGenId, genId);
        PersistedMetadataSerializer serializer = new PersistedMetadataSerializer();

        m_persistentDeque = PersistentBinaryDeque.builder(m_nonce, new File(m_path), exportLog)
                .initialExtraHeader(metadata, serializer)
                .compression(!DISABLE_COMPRESSION)
                .deleteExisting(deleteExisting)
                .build();

        m_reader = m_persistentDeque.openForRead(m_nonce);
    }
}
