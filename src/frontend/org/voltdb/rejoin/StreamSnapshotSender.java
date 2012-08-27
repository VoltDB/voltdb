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
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.CompressionService;

/**
 * A thread that sends snapshot data across the wire.
 */
public class StreamSnapshotSender implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("JOIN");

    private final Mailbox m_mb;
    private final long m_destHSId;
    private final LinkedBlockingQueue<BBContainer> m_queue =
            new LinkedBlockingQueue<BBContainer>();
    private final AtomicLong m_bytesSent = new AtomicLong();

    private volatile Throwable m_lastException = null;

    public StreamSnapshotSender(Mailbox mb, long destHSId) {
        m_mb = mb;
        m_destHSId = destHSId;
    }

    /**
     * Get the bytes sent. Note that this is the compressed bytes sent.
     * @return
     */
    public long getBytesSent() {
        return m_bytesSent.get();
    }

    public Throwable getLastException() {
        return m_lastException;
    }

    /**
     * Queues a new block to be sent.
     * @param chunk
     */
    public void offer(BBContainer chunk) {
        m_queue.offer(chunk);
    }

    @Override
    public void run() {
        try {
            final ByteBuffer compressionBuffer =
                    ByteBuffer.allocateDirect(
                            CompressionService.maxCompressedLength(1024 * 1024 * 2 + (1024 * 256)));
            while (true) {
                BBContainer message = m_queue.take();
                if (message.b == null) {
                    rejoinLog.debug("Got terminator, terminating the snapshot sender");
                    return;
                }

                try {
                    if (message.b.isDirect()) {
                        compressionBuffer.clear();
                        final int compressedSize =
                                CompressionService.compressBuffer(message.b, compressionBuffer);
                        compressionBuffer.limit(compressedSize);
                        compressionBuffer.position(0);

                        byte[] data = new byte[compressedSize];
                        compressionBuffer.get(data);
                        RejoinDataMessage msg = new RejoinDataMessage(data);
                        m_mb.send(m_destHSId, msg);
                        m_bytesSent.addAndGet(compressedSize);
                    } else {
                        byte compressedBytes[] =
                                CompressionService.compressBytes(
                                        message.b.array(), message.b.position(),
                                        message.b.remaining());

                        RejoinDataMessage msg = new RejoinDataMessage(compressedBytes);
                        m_mb.send(m_destHSId, msg);
                        m_bytesSent.addAndGet(compressedBytes.length);
                    }
                } catch (IOException e) {
                    rejoinLog.error("Error writing rejoin snapshot block", e);
                    return;
                } finally {
                    message.discard();
                }

            }
        } catch (InterruptedException e) {}
    }

}
