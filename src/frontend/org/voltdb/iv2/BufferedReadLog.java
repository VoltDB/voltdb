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
import java.util.Iterator;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.InitiateResponseMessage;

public class BufferedReadLog
{
    public static class Item {
        final InitiateResponseMessage m_initiateMsg;
        final FragmentResponseMessage m_fragmentMsg;
        final long m_txnId;

        Item(InitiateResponseMessage msg) {
            m_initiateMsg = msg;
            m_fragmentMsg = null;
            m_txnId = Long.MIN_VALUE;
        }

        Item(FragmentResponseMessage msg, long txnId) {
            m_initiateMsg = null;
            m_fragmentMsg = msg;
            m_txnId = txnId;
        }

        long getTxnId() {
            if (m_initiateMsg != null) {
                return m_initiateMsg.getSpHandle();
            }
            return m_txnId;
        }

        long getResponseHSId() {
            if (m_initiateMsg != null) {
                return m_initiateMsg.getInitiatorHSId();
            }
            return m_fragmentMsg.getDestinationSiteId();
        }

        VoltMessage getMessage() {
            if (m_initiateMsg != null) {
                return m_initiateMsg;
            }
            return m_fragmentMsg;
        }

        @Override
        public String toString() {
            if (m_initiateMsg != null) {
                return "Buffered read msg: " + m_initiateMsg.toString();
            }
            return "Buffered read msg: " + m_fragmentMsg.toString();
        }
    }

    private static final int INIT_BUFFER_CAPACITY = 64;

    final Deque<Item> m_bufferedReads;

    BufferedReadLog()
    {
        m_bufferedReads = new ArrayDeque<Item>(INIT_BUFFER_CAPACITY);
    }

    public void offer(Mailbox mailbox, InitiateResponseMessage msg, long handle)
    {
        offerInternal(mailbox, new Item(msg), handle);
    }

    public void offer(Mailbox mailbox, FragmentResponseMessage msg, long txnId, long handle)
    {
        offerInternal(mailbox, new Item(msg, txnId), handle);
    }

    //  SPI offers a new message.
    private void offerInternal(Mailbox mailbox, Item item, long handle) {
        m_bufferedReads.add(item);
        releaseBufferedReads(mailbox, handle);
    }


    public void releaseBufferedReads(Mailbox mailbox, long spHandle)
    {
        Deque<Item> deq = m_bufferedReads;
        Item item = null;
        while ((item = deq.peek()) != null) {
            if (item.getTxnId() <= spHandle) {
                // when the sp reads' handle is less equal than truncation handle
                // we know any previous write has been confirmed and it's safe to release.
                mailbox.send(item.getResponseHSId(), item.getMessage());
                deq.poll();
            } else {
                break;
            }
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("BufferedReadLog contents:");
        Iterator<Item> itr = m_bufferedReads.iterator();
        for(int i = 0; itr.hasNext(); i++)  {
            sb.append("           ").append(i).append(":").append(itr.next().toString()).append("\n");
        }
        return sb.toString();
    }
}
