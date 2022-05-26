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

import java.util.concurrent.LinkedBlockingQueue;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.Pair;

/**
 * Sends acks of snapshot blocks to the snapshot sender.
 */
public class StreamSnapshotAckSender implements Runnable {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    private final Mailbox m_mb;
    private final LinkedBlockingQueue<Pair<Long, RejoinDataAckMessage>> m_blockIndices =
        new LinkedBlockingQueue<Pair<Long, RejoinDataAckMessage>>();

    public StreamSnapshotAckSender(Mailbox mb) {
        m_mb = mb;
    }

    public void close() {
        // null message terminates the thread
        m_blockIndices.offer(Pair.of(-1L, (RejoinDataAckMessage) null));
    }

    /**
     * Ack with a positive block index.
     * @param hsId The mailbox to send the ack to
     * @param blockIndex
     */
    public void ack(long hsId, boolean isEOS, long targetId, int blockIndex) {
        rejoinLog.debug("Queue ack for hsId:" + hsId + " isEOS: " +
                isEOS + " targetId:" + targetId + " blockIndex: " + blockIndex);
        m_blockIndices.offer(Pair.of(hsId, new RejoinDataAckMessage(isEOS, targetId, blockIndex)));
    }

    @Override
    public void run() {
        while (true) {
            long hsId;
            RejoinDataAckMessage ackMsg;
            try {
                Pair<Long, RejoinDataAckMessage> work = m_blockIndices.take();
                hsId = work.getFirst();
                ackMsg = work.getSecond();
            } catch (InterruptedException e1) {
                break;
            }

            if (ackMsg == null) {
                rejoinLog.debug(m_blockIndices.size() + " acks remaining, " +
                        "terminating ack sender");
                // special value of -1 terminates the thread
                break;
            }

            m_mb.send(hsId, ackMsg);
        }
    }
}
