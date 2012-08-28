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

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;

/**
 * Reads acks from a rejoining partition
 */
public class StreamSnapshotAckReceiver implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("JOIN");

    private final Mailbox m_mb;
    private final StreamSnapshotAckTracker m_ackTracker;

    private volatile boolean m_closed = false;
    private volatile Throwable m_lastException = null;

    public StreamSnapshotAckReceiver(Mailbox mb,
                                     StreamSnapshotAckTracker tracker) {
        m_mb = mb;
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
                VoltMessage msg = m_mb.recvBlocking();
                assert(msg instanceof RejoinDataAckMessage);
                RejoinDataAckMessage ackMsg = (RejoinDataAckMessage) msg;
                final int blockIndex = ackMsg.getBlockIndex();
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
