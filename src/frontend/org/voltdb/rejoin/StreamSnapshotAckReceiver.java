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

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import org.voltcore.logging.VoltLogger;

/**
 * Reads acks from a rejoining partition
 */
public class StreamSnapshotAckReceiver implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    private final SocketChannel m_sock;
    private final StreamSnapshotAckTracker m_ackTracker;

    private volatile boolean m_closed = false;
    private volatile Throwable m_lastException = null;

    public StreamSnapshotAckReceiver(SocketChannel sock,
                                     StreamSnapshotAckTracker tracker) {
        m_sock = sock;
        m_ackTracker = tracker;
    }

    public Throwable getLastException() {
        return m_lastException;
    }

    public void close() {
        m_closed = true;
    }

    @Override
    public void run() {
        try {
            while (!m_closed) {
                ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
                while (lengthBuffer.hasRemaining()) {
                    int read = m_sock.read(lengthBuffer);
                    if (read == -1) {
                        return;
                    }
                }
                lengthBuffer.flip();

                ByteBuffer messageBuffer = ByteBuffer.allocate(lengthBuffer.getInt());
                while(messageBuffer.hasRemaining()) {
                    int read = m_sock.read(messageBuffer);
                    if (read == -1) {
                        return;
                    }
                }
                messageBuffer.flip();
                messageBuffer.getLong();//drop source site id
                final int blockIndex = messageBuffer.getInt();
                m_ackTracker.ackReceived(blockIndex);
            }
        } catch (Exception e) {
            if (m_closed) {
                return;
            }
            m_lastException = e;
            rejoinLog.error("Error reading a message from a recovery stream", e);
        }
    }

}
