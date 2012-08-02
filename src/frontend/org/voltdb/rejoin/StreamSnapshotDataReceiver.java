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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.CompressionService;

/**
 * Receives snapshot data from a replica. This is used on a rejoining partition.
 */
public class StreamSnapshotDataReceiver extends StreamSnapshotBase
implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("JOIN");

    private final SocketChannel m_sock;
    private final LinkedBlockingQueue<BBContainer> m_queue =
            new LinkedBlockingQueue<BBContainer>();

    private volatile boolean m_closed = false;

    public StreamSnapshotDataReceiver(SocketChannel sock) {
        super();
        m_sock = sock;
    }

    public void close() {
        m_closed = true;
    }

    /**
     * Get the next message from queue.
     *
     * @return null if the queue is empty.
     */
    public BBContainer poll() {
        return m_queue.poll();
    }

    /**
     * Get the next message from the queue. This blocks until one is available.
     *
     * @return
     * @throws InterruptedException
     */
    public BBContainer take() throws InterruptedException {
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
                ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
                while (lengthBuffer.hasRemaining()) {
                    int read = m_sock.read(lengthBuffer);
                    if (read == -1) {
                        return;
                    }
                }
                lengthBuffer.flip();
                final int length = lengthBuffer.getInt();

                BBContainer container = m_buffers.take();
                boolean success = false;
                try {
                    ByteBuffer messageBuffer = container.b;
                    messageBuffer.clear();
                    compressionBuffer.clear();
                    compressionBuffer.limit(length);
                    while(compressionBuffer.hasRemaining()) {
                        int read = m_sock.read(compressionBuffer);
                        if (read == -1) {
                            throw new EOFException();
                        }
                    }
                    compressionBuffer.flip();
                    int uncompressedSize =
                            CompressionService.decompressBuffer(
                                    compressionBuffer,
                                    messageBuffer);
                    messageBuffer.limit(uncompressedSize);
                    m_queue.offer(container);
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
