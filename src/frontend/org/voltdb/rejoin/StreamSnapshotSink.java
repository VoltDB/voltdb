/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.rejoin;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.utils.CachedByteBufferAllocator;
import org.voltdb.utils.FixedDBBPool;

/**
 * Takes the decompressed snapshot blocks and pass them to EE. Once constructed,
 * caller should initialize it and then poll(). If poll() returns null and
 * isEOF() is true, that means end of stream has reached, no more snapshot
 * blocks will arrive. It's safe to move on.
 *
 * This class is not thread-safe.
 */
public class StreamSnapshotSink {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    private final Mailbox m_mb;
    private StreamSnapshotDataReceiver m_in = null;
    private Thread m_inThread = null;
    private StreamSnapshotAckSender m_ack = null;
    private Thread m_ackThread = null;
    private final AtomicInteger m_expectedEOFs = new AtomicInteger();
    private boolean m_EOF = false;
    // Schemas of the tables
    private final Map<Integer, byte[]> m_schemas = new HashMap<Integer, byte[]>();
    private long m_bytesReceived = 0;

    public StreamSnapshotSink(Mailbox mb)
    {
        Preconditions.checkArgument(mb != null);
        m_mb = mb;
    }

    public long initialize(int sourceCount, FixedDBBPool bufferPool) {
        // Expect sourceCount number of EOFs at the end
        m_expectedEOFs.set(sourceCount);

        m_in = new StreamSnapshotDataReceiver(m_mb, bufferPool);
        m_inThread = new Thread(m_in, "Snapshot data receiver");
        m_inThread.setDaemon(true);
        m_ack = new StreamSnapshotAckSender(m_mb);
        m_ackThread = new Thread(m_ack, "Snapshot ack sender");
        m_inThread.start();
        m_ackThread.start();

        return m_mb.getHSId();
    }

    public boolean isEOF() {
        return m_EOF;
    }

    public void close() {
        if (m_in != null) {
            m_in.close();
            // Interrupt the thread in case it's blocked on mailbox recv.
            m_inThread.interrupt();
            try {
                m_inThread.join();
            } catch (InterruptedException e) {}
        }

        if (m_ack != null) {
            m_ack.close();
            try {
                m_ackThread.join();
            } catch (InterruptedException e) {}
        }

        m_in = null;
        m_ack = null;
    }

    /**
     * Assemble the chunk so that it can be used to construct the VoltTable that
     * will be passed to EE.
     *
     * @param buf
     * @return
     */
    private ByteBuffer getNextChunk(int tableId, ByteBuffer buf,
                                    CachedByteBufferAllocator resultBufferAllocator) {
        byte[] schemaBytes = m_schemas.get(tableId);
        buf.position(buf.position() + 4);//skip partition id
        int length = schemaBytes.length + buf.remaining();

        ByteBuffer outputBuffer = resultBufferAllocator.allocate(length);
        outputBuffer.put(schemaBytes);
        outputBuffer.put(buf);
        outputBuffer.flip();

        return outputBuffer;
    }

    public Pair<Integer, ByteBuffer> take(CachedByteBufferAllocator resultBufferAllocator)
        throws InterruptedException {
        if (m_in == null || m_ack == null) {
            // terminated already
            return null;
        }

        Pair<Integer, ByteBuffer> result = null;
        while (!m_EOF) {
            Pair<Long, Pair<Long, BBContainer>> msg = m_in.take();
            result = processMessage(msg, resultBufferAllocator);
            if (result != null) {
                break;
            }
        }

        return result;
    }

    public Pair<Integer, ByteBuffer> poll(CachedByteBufferAllocator resultBufferAllocator) {
        if (m_in == null || m_ack == null) {
            // not initialized yet or terminated already
            return null;
        }

        Pair<Long, Pair<Long, BBContainer>> msg = m_in.poll();
        return processMessage(msg, resultBufferAllocator);
    }

    /**
     * Process a message pulled off from the network thread, and discard the
     * container once it's processed.
     *
     * @param msg A pair of <sourceHSId, blockContainer>
     * @return The processed message, or null if there's no data block to return
     *         to the site.
     */
    private Pair<Integer, ByteBuffer> processMessage(Pair<Long, Pair<Long, BBContainer>> msg,
                                                     CachedByteBufferAllocator resultBufferAllocator) {
        if (msg == null) {
            return null;
        }

        long hsId = msg.getFirst();
        long targetId = msg.getSecond().getFirst();
        BBContainer container = msg.getSecond().getSecond();
        try {
            ByteBuffer block = container.b;
            byte typeByte = block.get(StreamSnapshotDataTarget.typeOffset);
            StreamSnapshotMessageType type = StreamSnapshotMessageType.values()[typeByte];
            if (type == StreamSnapshotMessageType.FAILURE) {
                VoltDB.crashLocalVoltDB("Rejoin source sent failure message.", false, null);

                // for test code only
                if (m_expectedEOFs.decrementAndGet() == 0) {
                    m_EOF = true;
                }
                return null;
            }
            if (type == StreamSnapshotMessageType.END) {
                rejoinLog.trace("Got END message");

                // End of stream, no need to ack this buffer
                if (m_expectedEOFs.decrementAndGet() == 0) {
                    m_EOF = true;
                }
                return null;
            }
            else if (type == StreamSnapshotMessageType.SCHEMA) {
                rejoinLog.trace("Got SCHEMA message");

                block.position(block.position() + 1 + 4);
                byte[] schemaBytes = new byte[block.remaining()];
                block.get(schemaBytes);
                m_schemas.put(block.getInt(StreamSnapshotDataTarget.tableIdOffset),
                              schemaBytes);
                return null;
            }

            // It's normal snapshot data afterwards

            final int tableId = block.getInt(StreamSnapshotDataTarget.tableIdOffset);
            final int blockIndex = block.getInt(StreamSnapshotDataTarget.blockIndexOffset);

            if (!m_schemas.containsKey(tableId)) {
                VoltDB.crashLocalVoltDB("No schema for table with ID " + tableId,
                                        false, null);
            }

            // Get the byte buffer ready to be consumed
            block.position(StreamSnapshotDataTarget.contentOffset);
            ByteBuffer nextChunk = getNextChunk(tableId, block, resultBufferAllocator);
            m_bytesReceived += nextChunk.remaining();

            // Queue ack to this block
            m_ack.ack(hsId, targetId, blockIndex);

            return Pair.of(tableId, nextChunk);
        } finally {
            container.discard();
        }
    }

    public long bytesTransferred() {
        return m_bytesReceived;
    }
}
