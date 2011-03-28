/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
package org.voltdb.export;

import java.util.ArrayDeque;
import java.util.Iterator;

import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltdb.utils.PersistentBinaryDeque;
import org.voltdb.utils.BinaryDeque;
import org.voltdb.utils.VoltFile;

import org.voltdb.utils.DBBPool.BBContainer;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
    private final BinaryDeque m_persistentDeque;

    private final String m_nonce;

    public StreamBlockQueue(String path, String nonce) throws java.io.IOException {
        m_persistentDeque = new PersistentBinaryDeque( nonce, new VoltFile(path));
        m_nonce = nonce;
    }

    public boolean isEmpty() throws IOException {
        if (m_memoryDeque.isEmpty() && m_persistentDeque.isEmpty()) {
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
            cont = m_persistentDeque.poll();
        } catch (IOException e) {
            exportLog.error(e);
        }

        if (cont == null) {
            return null;
        } else {
            //If the container is not null, unpack it.
            final BBContainer fcont = cont;
            long uso = cont.b.getLong();
            ByteBuffer buf = cont.b.slice();
            //Pass the stream block a subset of the bytes, provide
            //a container that discards the original returned by the persistent deque
            StreamBlock block = new StreamBlock( new BBContainer(buf, 0L) {
                    @Override
                    public void discard() {
                        fcont.discard();
                    }
                },
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
        //Already have two blocks, put it in the deque
        if (m_memoryDeque.size() > 1) {
            m_persistentDeque.offer(streamBlock.asBufferChain());
        } else {
            //Don't offer into the memory deque if there is anything waiting to be
            //polled out of the persistent deque. Check the persistent deque
            if (pollPersistentDeque(false) != null) {
               m_persistentDeque.offer( streamBlock.asBufferChain());
            } else {
            //Persistent deque is empty put this in memory
               m_memoryDeque.offer(streamBlock);
            }
        }
    }

    /*
     * Push all the buffers that are in memory to disk
     * and then have the persistent deque sync.
     * Skip the fsync for an asynchronous push of the in memory
     * buffers to disk
     */
    public void sync(boolean nofsync) throws IOException {
        if (m_memoryDeque.peek() != null && !m_memoryDeque.peek().isPersisted()) {
            ArrayDeque<BBContainer[]> buffersToPush = new ArrayDeque<BBContainer[]>();
            Iterator<StreamBlock> iter = m_memoryDeque.iterator();
            while (iter.hasNext()) {
                StreamBlock sb = iter.next();
                if (sb.isPersisted()) {
                    exportLog.error("Found a persisted export buffer after a memory buffer." +
                            " This shouldn't happen. Will make a best effort to return all the data " +
                            " and not leak memory");
                    break;
                }

                buffersToPush.offer( sb.asBufferChain() );
                iter.remove();
            }
            m_memoryDeque.clear();
            if (!buffersToPush.isEmpty()) {
                m_persistentDeque.push(buffersToPush.toArray(new BBContainer[0][0]));
            }
        }

        if (!nofsync) {
            m_persistentDeque.sync();
        }
    }

    public long sizeInBytes() {
        long memoryBlockUsage = 0;
        for (StreamBlock b : m_memoryDeque) {
            if (b.isPersisted()) {
                break;
            }
            memoryBlockUsage += b.totalUso();
        }
        return memoryBlockUsage + m_persistentDeque.sizeInBytes();
    }

    public void close() throws IOException {
        sync(true);
        m_memoryDeque.clear();
        m_persistentDeque.close();
    }

    public void closeAndDelete() throws IOException {
        m_persistentDeque.closeAndDelete();
        for (StreamBlock sb : m_memoryDeque) {
            if (!sb.isPersisted()) {
                sb.deleteContent();
            }
        }
    }

    public void truncateToTxnId(final long txnId, final int nullArrayLength) throws IOException {
        assert(m_memoryDeque.isEmpty());
        m_persistentDeque.parseAndTruncate(new BinaryDequeTruncator() {

        @Override
        public ByteBuffer parse(ByteBuffer b) {
            b.order(ByteOrder.LITTLE_ENDIAN);
            try {
                b.position(b.position() + 8);//Don't need the USO
                while (b.hasRemaining()) {
                    int rowLength = b.getInt();
                    b.position(b.position() + nullArrayLength);
                    long rowTxnId = b.getLong();
                    exportLog.trace("Evaluating row with txnId " + rowTxnId + " for truncation");
                    if (rowTxnId > txnId) {
                        exportLog.debug(
                                "Export stream " + m_nonce + " found export data to truncate at txn " + rowTxnId);
                        //The txnid of this row is the greater then the truncation txnid.
                        //Don't want this row, but want to preserve all rows before it.
                        //Move back before the row length prefix and txnId
                        b.position(b.position() - (12 + nullArrayLength));

                        //If the truncation point was the first row in the block, the entire block is to be discard
                        //We know it is the first row if the position before the row is after the uso (8 bytes)
                        if (b.position() == 8) {
                            return ByteBuffer.allocate(0);
                        } else {
                            //Return everything in the block before the truncation point.
                            //Indicate this is the end of the interesting data.
                            b.limit(b.position());
                            b.position(0);
                            return b;
                        }
                    } else {
                        //Not the row we are looking to truncate at. Skip past it keeping in mind
                        //we read the first 8 bytes for the txn id, and the null array which
                        //is included in the length prefix
                        b.position(b.position() + (rowLength - (8 + nullArrayLength)));
                    }
                }
            } finally {
                b.order(ByteOrder.BIG_ENDIAN);
            }
            return null;
        }
        });
    }


    @Override
    public void finalize() {
        if (!m_memoryDeque.isEmpty()) {
            exportLog.error("Finalized StreamBlockQueue with items in the memory deque");
        }
    }
}
