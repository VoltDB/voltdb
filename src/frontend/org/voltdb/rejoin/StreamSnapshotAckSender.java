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
    private final LinkedBlockingQueue<Pair<Long, Pair<Long, Integer>>> m_blockIndices =
            new LinkedBlockingQueue<Pair<Long, Pair<Long, Integer>>>();

    public StreamSnapshotAckSender(Mailbox mb) {
        m_mb = mb;
    }

    public void close() {
        // an index of -1 will terminate the thread
        m_blockIndices.offer(Pair.of(-1L, Pair.of(-1L, -1)));
    }

    /**
     * Ack with a positive block index.
     * @param hsId The mailbox to send the ack to
     * @param blockIndex
     */
    public void ack(long hsId, long targetId, int blockIndex) {
        m_blockIndices.offer(Pair.of(hsId, Pair.of(targetId, blockIndex)));
    }

    @Override
    public void run() {
        while (true) {
            long hsId;
            long targetId;
            int blockIndex;
            try {
                Pair<Long, Pair<Long, Integer>> blockToAck = m_blockIndices.take();
                hsId = blockToAck.getFirst();
                targetId = blockToAck.getSecond().getFirst();
                blockIndex = blockToAck.getSecond().getSecond();
            } catch (InterruptedException e1) {
                break;
            }

            if (blockIndex == -1) {
                rejoinLog.debug(m_blockIndices.size() + " acks remaining, " +
                        "terminating ack sender");
                // special value of -1 terminates the thread
                break;
            }

            RejoinDataAckMessage msg = new RejoinDataAckMessage(targetId, blockIndex);
            m_mb.send(hsId, msg);
        }
    }
}
