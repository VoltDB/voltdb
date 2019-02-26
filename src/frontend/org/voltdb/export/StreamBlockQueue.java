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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Iterator;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.BinaryDeque;
import org.voltdb.utils.BinaryDeque.BinaryDequeReader;
import org.voltdb.utils.BinaryDeque.BinaryDequeScanner;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltdb.utils.BinaryDeque.TruncatorResponse;
import org.voltdb.utils.PersistentBinaryDeque;
import org.voltdb.utils.PersistentBinaryDeque.ByteBufferTruncatorResponse;
import org.voltdb.utils.VoltFile;

/**
 * A customized queue for StreamBlocks that contain export data. The queue is able to
 * overflow to disk when more then two stream blocks are stored
 * as well as persist to disk when sync is invoked. Right now sync doesn't actually do an fsync on
 * the file unless it is specifically requested. It just pushed the two in memory blocks to the persistent
 * portion of the queue.
 *
 * Export PBD buffer layout:
 *    --- Buffer Header   ---
 *    seqNo(8) + tupleCount(4) + uniqueId(8) + exportVersion(1) + generationId(8) + schemaLen(4) + tupleSchema(var length)
 *    {
 *          ---Inside schema---
 *          tableNameLength(4) + tableName(var length) + colNameLength(4) + colName(var length) + colType(1) + colLength(4) + ...
 *    }
 *    --- Row Header      ---
 *    rowLength(4) + partitionColumnIndex(4) + columnCount(4, includes metadata columns) +
 *    nullArrayLength(4) + nullArray(var length)
 *    --- Metadata        ---
 *    TxnId(8) + timestamp(8) + seqNo(8) + partitionId(8) + siteId(8) + exportOperation(1)
 *    --- Row Data        ---
 *    rowData(var length)
 *
 *    repeat row header, meta data and row data...
 */
public class StreamBlockQueue {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");
    public static final int EXPORT_BUFFER_VERSION = 1;

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
    private BinaryDeque m_persistentDeque;

    private final String m_nonce;
    private final String m_path;
    private BinaryDequeReader m_reader;

    public StreamBlockQueue(String path, String nonce) throws java.io.IOException {
        m_persistentDeque = new PersistentBinaryDeque( nonce, new VoltFile(path), exportLog);
        m_path = path;
        m_nonce = nonce;
        m_reader = m_persistentDeque.openForRead(m_nonce);
        if (exportLog.isDebugEnabled()) {
            exportLog.debug(m_nonce + " At SBQ creation, PBD size is " + (m_reader.sizeInBytes() - (8 * m_reader.getNumObjects())));
        }
    }

    public boolean isEmpty() throws IOException {
        if (m_memoryDeque.isEmpty() && m_reader.isEmpty()) {
            return true;
        }
        return false;
    }

    /**
     * Wrapper around the common operation of pulling an element out of the persistent deque.
     * The behavior is complicated (and might change) since the persistent deque can throw an IOException.
     * The poll always removes the element from the persistent queue
     * (although not necessarily removing the file backing, that happens at deleteContents) and will add
     * a reference to the block to the in memory deque unless actuallyPoll is true.
     * @param actuallyPoll
     * @return
     */
    private StreamBlock pollPersistentDeque(boolean actuallyPoll) {
        BBContainer cont = null;
        try {
            cont = m_reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        } catch (IOException e) {
            exportLog.error(e);
        }

        if (cont == null) {
            return null;
        } else {
            cont.b().order(ByteOrder.LITTLE_ENDIAN);
            //If the container is not null, unpack it.
            final BBContainer fcont = cont;
            long seqNo = cont.b().getLong(0);
            long committedSeqNo = cont.b().getLong(8);
            int tupleCount = cont.b().getInt(16);
            long uniqueId = cont.b().getLong(20);
            //Pass the stream block a subset of the bytes, provide
            //a container that discards the original returned by the persistent deque
            StreamBlock block = new StreamBlock( fcont,
                seqNo,
                committedSeqNo,
                tupleCount,
                uniqueId,
                true);

            //Optionally store a reference to the block in the in memory deque
            if (!actuallyPoll) {
                m_memoryDeque.offer(block);
            }
            return block;
        }
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
    public Iterator<StreamBlock> iterator() {
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

    public StreamBlock peek() {
        if (m_memoryDeque.peek() != null) {
            return m_memoryDeque.peek();
        }
        return pollPersistentDeque(false);
    }

    // For test
    public StreamBlock poll() {
        StreamBlock sb = null;
        if (m_memoryDeque.peek() != null) {
            sb = m_memoryDeque.poll();
        } else {
            sb = pollPersistentDeque(true);
        }
        return sb;
    }

    public StreamBlock pop() {
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

    /*
     * Only allow two blocks in memory, put the rest in the persistent deque
     */
    public void offer(StreamBlock streamBlock) throws IOException {
        m_persistentDeque.offer(streamBlock.asBBContainer(), !DISABLE_COMPRESSION);
        long unreleasedSeqNo = streamBlock.unreleasedSequenceNumber();
        if (m_memoryDeque.size() < 2) {
            StreamBlock fromPBD = pollPersistentDeque(false);
            if ((streamBlock.startSequenceNumber() == fromPBD.startSequenceNumber()) &&
                    (unreleasedSeqNo > streamBlock.startSequenceNumber())) {
                fromPBD.releaseTo(unreleasedSeqNo - 1);
            }
        }
    }

    /*
     * This is a no-op now with nofsync=true
     */
    public void sync(boolean nofsync) throws IOException {
        if (!nofsync) {
            m_persistentDeque.sync();
        }
    }

    // Only used in tests, should be removed.
    public long sizeInBytes() throws IOException {
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

    public void close() throws IOException {
        sync(true);
        m_persistentDeque.close();
        for (StreamBlock sb : m_memoryDeque) {
            sb.discard();
        }
        m_memoryDeque.clear();
    }

    public void closeAndDelete() throws IOException {
        m_persistentDeque.closeAndDelete();
        for (StreamBlock sb : m_memoryDeque) {
            sb.discard();
        }
    }

    // See PDB segment layout at beginning of this file.
    public void truncateToSequenceNumber(final long truncationSeqNo) throws IOException {
        assert(m_memoryDeque.isEmpty());
        m_persistentDeque.parseAndTruncate(new BinaryDequeTruncator() {

            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                b.order(ByteOrder.LITTLE_ENDIAN);
                final long startSequenceNumber = b.getLong();
                // If after the truncation point is the first row in the block, the entire block is to be discarded
                if (startSequenceNumber > truncationSeqNo) {
                    return PersistentBinaryDeque.fullTruncateResponse();
                }
                final int tupleCountPos = b.position();
                final int tupleCount = b.getInt();
                // There is nothing to do with this buffer
                final long lastSequenceNumber = startSequenceNumber + tupleCount - 1;
                if (lastSequenceNumber <= truncationSeqNo) {
                    return null;
                }
                b.getLong(); // uniqueId
                byte version = b.get(); // export version
                assert(version == EXPORT_BUFFER_VERSION);
                b.getLong(); // generation id
                int firstRowStart = b.getInt() + b.position();
                b.position(firstRowStart);

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
                        b.putInt(tupleCountPos, offset - 1);
                        b.position(0);
                        return new ByteBufferTruncatorResponse(b);
                    }
                    offset++;
                    // Not the row we are looking to truncate at. Skip past it (row length + row length field).
                    final int rowLength = b.getInt();
                    if (b.position() + rowLength > b.limit()) {
                        System.out.println(rowLength);
                    }
                    b.position(b.position() + rowLength);
                }
                return null;
            }
        });

        // close reopen reader
        m_persistentDeque.close();
        m_persistentDeque = new PersistentBinaryDeque(m_nonce, new VoltFile(m_path), exportLog);
        m_reader = m_persistentDeque.openForRead(m_nonce);
        // temporary debug stmt
        exportLog.info("After truncate, PBD size is " + (m_reader.sizeInBytes() - (8 * m_reader.getNumObjects())));
    }

    public ExportSequenceNumberTracker scanForGap() throws IOException {
        assert(m_memoryDeque.isEmpty());
        return m_persistentDeque.scanForGap(new BinaryDequeScanner() {

            @Override
            public ExportSequenceNumberTracker scan(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                b.order(ByteOrder.LITTLE_ENDIAN);
                final long startSequenceNumber = b.getLong();
                final int tupleCount = b.getInt();
                ExportSequenceNumberTracker gapTracker = new ExportSequenceNumberTracker();
                gapTracker.append(startSequenceNumber, startSequenceNumber + tupleCount - 1);
                return gapTracker;
            }

        });
    }


    @Override
    public void finalize() {
        try {
            int nonEmptyCnt = 0;
            nonEmptyCnt = m_memoryDeque.stream().filter((block) -> (!block.isPersisted())).map((_item) -> 1).reduce(nonEmptyCnt, Integer::sum);
            if (nonEmptyCnt > 0) {
                exportLog.error("Finalized StreamBlockQueue with " + nonEmptyCnt + " items in the memory deque that are not persisted. Path: " + m_path + " Nonce: " + m_nonce);
            }
        } finally {
            try {
                super.finalize();
            } catch (Throwable ex) {
               ;
            }
        }
    }

}