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

import java.util.HashMap;
import org.voltcore.logging.VoltLogger;

/**
 * Keep track of how many times a block has been acked and how many acks are
 * expected
 */
public class StreamSnapshotAckTracker {
    private static final VoltLogger rejoinLog = new VoltLogger("REJOIN");

    private final HashMap<Integer, Integer> m_acks = new HashMap<Integer, Integer>();
    private final int m_bufferQuota;
    private boolean m_ignoreAcks = false;

    public StreamSnapshotAckTracker(int bufferQuota) {
        m_bufferQuota = bufferQuota;
    }

    public synchronized void waitForAcks(int blockIndex, int acksExpected) {
        assert(!m_acks.containsKey(blockIndex));
        m_acks.put(blockIndex, acksExpected);
    }

    public synchronized boolean ackReceived(int blockIndex) {
        assert(m_acks.containsKey(blockIndex));
        int acksRemaining = m_acks.get(blockIndex);
        acksRemaining--;
        if (acksRemaining == 0) {
            rejoinLog.trace("Ack received for block " + blockIndex);
            m_acks.remove(blockIndex);
            return true;
        }
        rejoinLog.trace("Ack received for block " + blockIndex + " with " +
                        acksRemaining + " remaining");
        m_acks.put(blockIndex, acksRemaining);
        return false;
    }

    /*
     * Don't bother expecting acks to come. Invoked by handle failure
     * when the destination fails.
     */
    public synchronized void ignoreAcks() {
        m_ignoreAcks = true;
    }

    public synchronized boolean hasMoreBufferQuota() {
        return m_acks.size() < m_bufferQuota;
    }

    public synchronized boolean hasOutstanding() {
        if (m_ignoreAcks) {
            return false;
        }
        return !m_acks.isEmpty();
    }
}
