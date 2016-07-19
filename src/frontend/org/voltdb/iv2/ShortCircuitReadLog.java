/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.iv2;

import java.util.ArrayDeque;
import java.util.Deque;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltdb.messaging.InitiateResponseMessage;

public class ShortCircuitReadLog
{
    VoltLogger tmLog = new VoltLogger("TM");

    private static final boolean IS_SP = true;
    private static final boolean IS_MP = false;

    // Initialize to Long MIN_VALUE
    long m_lastSpTruncationHandle = Long.MIN_VALUE;
    long m_lastMpTruncationHandle = Long.MIN_VALUE;

    static class Item
    {
        private final InitiateResponseMessage m_message;

        Item (InitiateResponseMessage message) {
            m_message = message;
        }

        public final InitiateResponseMessage getMessage() {
            return m_message;
        }

        public boolean shouldTruncate(long handle) {
            if (m_message.getSpHandle() <= handle) {
                return true;
            }
            return false;
        }
    }

    final Deque<Item> m_shortCircuitReadSp;
    final Deque<Item> m_shortCircuitReadMp;
    Mailbox m_mailbox;

    ShortCircuitReadLog(Mailbox mailbox)
    {
        m_shortCircuitReadSp = new ArrayDeque<Item>();
        m_shortCircuitReadMp = new ArrayDeque<Item>();

        assert(mailbox != null);
        m_mailbox = mailbox;
    }

    public void setLastSpTruncationHandle(long spHandle) {
        if (spHandle > m_lastSpTruncationHandle) {
//            System.out.println("update the m_lastSpTruncationHandle from " + m_lastSpTruncationHandle + " to " + spHandle);
            m_lastSpTruncationHandle = spHandle;
        }
    }

    public void setLastMpTruncationHandle(long mpHandle) {
        if (mpHandle > m_lastMpTruncationHandle) {
            m_lastMpTruncationHandle = mpHandle;
        }
    }

    // Offer a new message.
    public void offerSp(InitiateResponseMessage msg, boolean isLeader, long handle)
    {
//        System.out.println(String.format("Leader %s, truncation handle %d, replica truncation handle %d",
//                Boolean.toString(isLeader), handle, m_lastSpTruncationHandle));

        long truncationHandle = m_lastSpTruncationHandle;
        if (isLeader) {
            truncationHandle = handle;
        }

        if (msg.getSpHandle() >= truncationHandle || truncationHandle == Long.MIN_VALUE) {
            m_mailbox.send(msg.getInitiatorHSId(), msg);
        } else {
            m_shortCircuitReadSp.add(new Item(msg));
        }
        releaseShortCircuitRead(truncationHandle, IS_SP);
    }

    public void releaseShortCircuitRead(long handle, boolean isSP)
    {
        Deque<ShortCircuitReadLog.Item> deq = null;
        if (isSP) {
            deq = m_shortCircuitReadSp;
        }
        else {
            deq = m_shortCircuitReadMp;
        }

        ShortCircuitReadLog.Item item = null;
        while ((item = deq.peek()) != null) {
            if (item.shouldTruncate(handle)) {
                InitiateResponseMessage msg = item.getMessage();
                m_mailbox.send(msg.getInitiatorHSId(), msg);
                deq.poll();
            } else {
                break;
            }
        }
    }
}
