/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.rejoin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.utils.CompressionService;

/**
 * Receives snapshot data from a replica. This is used on a rejoining partition.
 */
public class StreamSnapshotDataReceiver extends StreamSnapshotBase
implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("JOIN");

    /*
     * element is a pair of <sourceHSId, blockData>. The hsId should remain the
     * same for the length of the data transfer process for this partition.
     */
    private final LinkedBlockingQueue<Pair<Long, BBContainer>> m_queue =
            new LinkedBlockingQueue<Pair<Long, BBContainer>>();

    private final Mailbox m_mb;
    private volatile boolean m_closed = false;

    public StreamSnapshotDataReceiver(Mailbox mb) {
        super();
        m_mb = mb;
    }

    public void close() {
        m_closed = true;
    }

    /**
     * Get the next message from queue.
     *
     * @return null if the queue is empty.
     */
    public Pair<Long, BBContainer> poll() {
        return m_queue.poll();
    }

    /**
     * Get the next message from the queue. This blocks until one is available.
     *
     * @return
     * @throws InterruptedException
     */
    public Pair<Long, BBContainer> take() throws InterruptedException {
        return m_queue.take();
    }

    public int size() {
        return m_queue.size();
    }

    @Override
    public void run() {
        try {

            final ByteBuffer compressionBuffer =
                    ByteBuffer.allocateDirect(
                            CompressionService.maxCompressedLength(1024 * 1024 * 2 + (1024 * 256)));
            while (true) {
                BBContainer container = m_buffers.take();
                ByteBuffer messageBuffer = container.b;
                messageBuffer.clear();
                compressionBuffer.clear();
                boolean success = false;

                try {
                    VoltMessage msg = m_mb.recvBlocking();
                    assert(msg instanceof RejoinDataMessage);
                    RejoinDataMessage dataMsg = (RejoinDataMessage) msg;
                    byte[] data = dataMsg.getData();

                    compressionBuffer.limit(data.length);
                    compressionBuffer.put(data);
                    compressionBuffer.flip();
                    int uncompressedSize =
                            CompressionService.decompressBuffer(
                                    compressionBuffer,
                                    messageBuffer);
                    messageBuffer.limit(uncompressedSize);
                    m_queue.offer(Pair.of(dataMsg.m_sourceHSId, container));
                    success = true;
                } finally {
                    if (!success) {
                        container.discard();
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
        } catch (InterruptedException e) {
            return;
        }
    }
}
