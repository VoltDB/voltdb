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
 *
 * portion of the queue
 *
 */
public class StreamBlockQueue {

    private static final VoltLogger exportLog = new VoltLogger("EXPORT");

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
        // temporary debug stmt
        exportLog.info(m_nonce + " At SBQ creation, PBD size is " + (m_reader.sizeInBytes() - (8 * m_reader.getNumObjects())));
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
            //If the container is not null, unpack it.
            final BBContainer fcont = cont;
            long uso = cont.b().getLong(0);
            //Pass the stream block a subset of the bytes, provide
            //a container that discards the original returned by the persistent deque
            StreamBlock block = new StreamBlock( fcont,
                uso,
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
        m_persistentDeque.offer(streamBlock.asBBContainer());
        long unreleasedUso = streamBlock.unreleasedUso();
        if (m_memoryDeque.size() < 2) {
            StreamBlock fromPBD = pollPersistentDeque(false);
            if ((streamBlock.uso() == fromPBD.uso()) && (unreleasedUso > streamBlock.uso())) {
                fromPBD.releaseUso(unreleasedUso - 1);
                assert(fromPBD.unreleasedUso() < fromPBD.uso() + fromPBD.totalSize() - 1);
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

    public long sizeInBytes() throws IOException {
        long memoryBlockUsage = 0;
        for (StreamBlock b : m_memoryDeque) {
            //Use only unreleased size, but throw in the USO
            //to make book keeping consistent when flushed to disk
            //Also dont count persisted blocks.
            memoryBlockUsage += b.unreleasedSize();
        }
        //Subtract USO from on disk size
        return memoryBlockUsage + m_reader.sizeInBytes() - (8 * m_reader.getNumObjects());
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

    public void truncateToTxnId(final long txnId) throws IOException {
        assert(m_memoryDeque.isEmpty());
        m_persistentDeque.parseAndTruncate(new BinaryDequeTruncator() {

        @Override
        public TruncatorResponse parse(BBContainer bbc) {
            ByteBuffer b = bbc.b();
            b.order(ByteOrder.LITTLE_ENDIAN);
            try {
                final int headerSize = 8 + 4 + 4 + 1; // generation, partition index + column count + byte for schema flag.
                b.position(b.position() + 8);//Don't need the USO
                while (b.hasRemaining()) {
                    int rowLength = b.getInt();
                    //Get Generation
                    b.getLong();
                    //Get partition col index
                    b.getInt();
                    //Get column count includes metadata column count.
                    int columnCount = b.getInt();
                    //Get schema flag.
                    byte hasSchema = b.get();

                    int nullArrayLength = ((columnCount + 7) & -8) >> 3;
                    b.position(b.position() + nullArrayLength);

                    int skiplen = 0;
                    if (hasSchema == 1) {
                        //Table Name + Its length size
                        skiplen += 4;
                        int tlen = b.getInt();
                        byte[] bx = new byte[tlen];
                        b.get(bx);
                        skiplen += tlen;

                        for (int i = 0; i < columnCount; i++) {
                            //Col Name length
                            tlen = b.getInt();
                            skiplen += 4;
                            bx = new byte[tlen];
                            //Col Name
                            b.get(bx);
                            skiplen += tlen;
                            //Type Byte
                            b.get();
                            skiplen++;
                            //Get length of column
                            b.getInt();
                            skiplen += 4;
                        }
                    }

                    long rowTxnId = b.getLong();
                    if (exportLog.isTraceEnabled()) {
                        exportLog.trace("Evaluating row with txnId " + rowTxnId + " for truncation, skiplen=" + skiplen);
                    }
                    if (rowTxnId > txnId) {
                        if (exportLog.isDebugEnabled()) {
                            exportLog.debug(
                                    "Export stream " + m_nonce + " found export data to truncate at txn " + rowTxnId);
                        }
                        //The txnid of this row is the greater then the truncation txnid.
                        //Don't want this row, but want to preserve all rows before it.
                        //Move back before the row length prefix, txnId and header
                        b.position(b.position() - (skiplen + 12 + headerSize + nullArrayLength));

                        //If the truncation point was the first row in the block, the entire block is to be discard
                        //We know it is the first row if the position before the row is after the uso (8 bytes)
                        if (b.position() == 8) {
                            return PersistentBinaryDeque.fullTruncateResponse();
                        } else {
                            //Return everything in the block before the truncation point.
                            //Indicate this is the end of the interesting data.
                            b.limit(b.position());
                            b.position(0);
                            return new ByteBufferTruncatorResponse(b);
                        }
                    } else {
                        //Not the row we are looking to truncate at. Skip past it keeping in mind
                        //we read the first 8 bytes for the txn id, the null array which
                        //is included in the length prefix and the header size
                        b.position(b.position() + (rowLength - (skiplen + 8 + headerSize + nullArrayLength)));
                    }
                }
            } finally {
                b.order(ByteOrder.BIG_ENDIAN);
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
