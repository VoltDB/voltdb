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

package org.voltdb.rejoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.FixedDBBPool;

/**
 * Receives snapshot data from a replica. This is used on a rejoining partition.
 */
public class StreamSnapshotDataReceiver extends StreamSnapshotBase
implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    /*
     * element is a pair of <sourceHSId, blockData>. The hsId should remain the
     * same for the length of the data transfer process for this partition.
     */
    private final LinkedBlockingQueue<StreamSnapshotSink.DecodedContainer> m_queue =
            new LinkedBlockingQueue<StreamSnapshotSink.DecodedContainer>();

    private final Mailbox m_mb;
    private final Queue<BBContainer> m_dataBufferPool;
    private final Queue<BBContainer> m_compressedDataBufferPool;
    private volatile boolean m_closed = false;

    private class TrackedDataBBContainer extends BBContainer {
        private final BBContainer m_delegate;
        TrackedDataBBContainer(BBContainer container) {
            super(container.b());
            m_delegate = container;
        }
        @Override
        public void discard() {
            checkDoubleFree();
            m_dataBufferPool.offer(m_delegate);
        }
    }

    private class TrackedCompressedDataBBContainer extends BBContainer {
        private final BBContainer m_delegate;
        TrackedCompressedDataBBContainer(BBContainer container) {
            super(container.b());
            m_delegate = container;
        }
        @Override
        public void discard() {
            checkDoubleFree();
            m_compressedDataBufferPool.offer(m_delegate);
        }
    }

    public StreamSnapshotDataReceiver(Mailbox mb, Queue<BBContainer> dataPool, Queue<BBContainer> compressedDataPool) {
        super();
        m_mb = mb;
        m_dataBufferPool = dataPool;
        m_compressedDataBufferPool = compressedDataPool;
    }

    public void close() {
        m_closed = true;
    }

    /**
     * Get the next message from queue.
     *
     * @return null if the queue is empty.
     */
    public StreamSnapshotSink.DecodedContainer poll() {
        return m_queue.poll();
    }

    /**
     * Get the next message from the queue. This blocks until one is available.
     *
     * @return
     * @throws InterruptedException
     */
    public StreamSnapshotSink.DecodedContainer take() throws InterruptedException {
        return m_queue.take();
    }

    public int size() {
        return m_queue.size();
    }

    @Override
    public void run() {
        try {
            while (true) {
                BBContainer container = null;
                BBContainer compressionBufferC = null;
                ByteBuffer compressionBuffer = null;
                boolean success = false;

                try {
                    VoltMessage msg = m_mb.recvBlocking();
                    if (msg == null) {
                        // If interrupted, break
                        break;
                    }

                    assert(msg instanceof RejoinDataMessage);
                    RejoinDataMessage dataMsg = (RejoinDataMessage) msg;
                    byte[] data = dataMsg.getData();

                    // Only grab the buffer from the pool after receiving a message from the
                    // mailbox. If the buffer is grabbed before receiving the message,
                    // this thread could hold on to a buffer it may not need and other receivers
                    // will be blocked if the pool has no more buffers left.
                    container = m_dataBufferPool.poll();
                    if (container == null) {
                        container = new TrackedDataBBContainer(DBBPool.allocateDirect(SnapshotSiteProcessor.m_snapshotBufferLength));
                    }
                    ByteBuffer messageBuffer = container.b();
                    messageBuffer.clear();

                    compressionBufferC = m_compressedDataBufferPool.poll();
                    if (compressionBufferC == null) {
                        compressionBufferC = new TrackedCompressedDataBBContainer(DBBPool.allocateDirect(SnapshotSiteProcessor.m_snapshotBufferCompressedLen));
                    }
                    compressionBuffer = compressionBufferC.b();
                    compressionBuffer.clear();
                    compressionBuffer.limit(data.length);
                    compressionBuffer.put(data);
                    compressionBuffer.flip();
                    int uncompressedSize =
                            CompressionService.decompressBuffer(
                                    compressionBuffer,
                                    messageBuffer);
                    messageBuffer.limit(uncompressedSize);
                    m_queue.offer(new StreamSnapshotSink.DecodedContainer(dataMsg.m_sourceHSId, dataMsg.getTargetId(), container));
                    success = true;
                } finally {
                    if (!success && container != null) {
                        container.discard();
                    }
                    if (compressionBuffer != null) {
                        compressionBufferC.discard();
                    }
                }
            }
        } catch (IOException e) {
            /*
             * Wait until the last message is delivered and then wait some more
             * so it can be processed so that closed can be set and the
             * exception suppressed.
             */
            try {
                while (!m_queue.isEmpty()) {
                    Thread.sleep(50);
                }
                Thread.sleep(300);
            } catch (InterruptedException e2) {}
            if (m_closed) {
                return;
            }
            rejoinLog.error("Error reading a message from a recovery stream.", e);
        }
    }
}
