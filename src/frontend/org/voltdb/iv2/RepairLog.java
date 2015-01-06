/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.TheHashinator;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;

/**
 * The repair log stores messages received from a PI in case they need to be
 * shared with less informed RIs should the PI shed its mortal coil.  This includes
 * recording and sharing messages starting and completing multipartition transactions
 * so that a new MPI can repair the cluster state on promotion.
 */
public class RepairLog
{
    private static final boolean IS_SP = true;
    private static final boolean IS_MP = false;

    VoltLogger tmLog = new VoltLogger("TM");

    // Initialize to Long MAX_VALUE to prevent feeding a newly joined node
    // transactions it should never have seen
    long m_lastSpHandle = Long.MAX_VALUE;
    long m_lastMpHandle = Long.MAX_VALUE;

    // is this a partition leader?
    boolean m_isLeader = false;

    // The HSID of this initiator, for logging purposes
    long m_HSId = Long.MIN_VALUE;

    // want voltmessage as payload with message-independent metadata.
    static class Item
    {
        final VoltMessage m_msg;
        final long m_handle;
        final long m_txnId;
        final boolean m_type;

        Item(boolean type, VoltMessage msg, long handle, long txnId)
        {
            m_type = type;
            m_msg = msg;
            m_handle = handle;
            m_txnId = txnId;
        }

        long getHandle()
        {
            return m_handle;
        }

        long getTxnId() {
            return m_txnId;
        }

        VoltMessage getMessage()
        {
            return m_msg;
        }

        boolean isSP()
        {
            return m_type == IS_SP;
        }

        boolean isMP()
        {
            return m_type == IS_MP;
        }

        boolean canTruncate(long handle)
        {
            if (isSP() && m_handle <= handle) {
                return true;
            }
            else if (isMP() && m_txnId <= handle) {
                return true;
            }
            return false;
        }
    }

    // log storage.
    final Deque<Item> m_logSP;
    final Deque<Item> m_logMP;

    RepairLog()
    {
        m_logSP = new ArrayDeque<Item>();
        m_logMP = new ArrayDeque<Item>();
    }

    // get the HSID for dump logging
    void setHSId(long HSId)
    {
        m_HSId = HSId;
    }

    // leaders log differently
    void setLeaderState(boolean isLeader)
    {
        m_isLeader = isLeader;
        // The leader doesn't truncate its own SP log; if promoted,
        // wipe out the SP portion of the existing log. This promotion
        // action always happens after repair is completed.
        if (m_isLeader) {
            truncate(Long.MAX_VALUE, IS_SP);
        }
    }

    // Offer a new message to the repair log. This will truncate
    // the repairLog if the message includes a truncation hint.
    public void deliver(VoltMessage msg)
    {
        if (!m_isLeader && msg instanceof Iv2InitiateTaskMessage) {
            final Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)msg;
            // We can't repair read-only SP transactions due to their short-circuited nature.
            // Just don't log them to the repair log.
            if (!m.isReadOnly()) {
                m_lastSpHandle = m.getSpHandle();
                truncate(m.getTruncationHandle(), IS_SP);
                m_logSP.add(new Item(IS_SP, m, m.getSpHandle(), m.getTxnId()));
            }
        } else if (msg instanceof FragmentTaskMessage) {
            final TransactionInfoBaseMessage m = (TransactionInfoBaseMessage)msg;
            if (!m.isReadOnly()) {
                truncate(m.getTruncationHandle(), IS_MP);
                // only log the first fragment of a procedure (and handle 1st case)
                if (m.getTxnId() > m_lastMpHandle || m_lastMpHandle == Long.MAX_VALUE) {
                    m_logMP.add(new Item(IS_MP, m, m.getSpHandle(), m.getTxnId()));
                    m_lastMpHandle = m.getTxnId();
                    m_lastSpHandle = m.getSpHandle();
                }
            }
        }
        else if (msg instanceof CompleteTransactionMessage) {
            // a CompleteTransactionMessage which indicates restart is not the end of the
            // transaction.  We don't want to log it in the repair log.
            CompleteTransactionMessage ctm = (CompleteTransactionMessage)msg;
            if (!ctm.isReadOnly() && !ctm.isRestart()) {
                truncate(ctm.getTruncationHandle(), IS_MP);
                m_logMP.add(new Item(IS_MP, ctm, ctm.getSpHandle(), ctm.getTxnId()));
                //Restore will send a complete transaction message with a lower mp transaction id because
                //the restore transaction precedes the loading of the right mp transaction id from the snapshot
                //Hence Math.max
                m_lastMpHandle = Math.max(m_lastMpHandle, ctm.getTxnId());
                m_lastSpHandle = ctm.getSpHandle();
            }
        }
        else if (msg instanceof DumpMessage) {
            String who = CoreUtils.hsIdToString(m_HSId);
            tmLog.warn("Repair log dump for site: " + who + ", isLeader: " + m_isLeader);
            tmLog.warn("" + who + ": lastSpHandle: " + m_lastSpHandle + ", lastMpHandle: " + m_lastMpHandle);
            for (Iv2RepairLogResponseMessage il : contents(0l, false)) {
               tmLog.warn("" + who + ": msg: " + il);
            }
        }
    }

    // trim unnecessary log messages.
    private void truncate(long handle, boolean isSP)
    {
        // MIN value means no work to do, is a startup condition
        if (handle == Long.MIN_VALUE) {
            return;
        }

        Deque<RepairLog.Item> deq = null;
        if (isSP) {
            deq = m_logSP;
        }
        else {
            deq = m_logMP;
        }

        RepairLog.Item item = null;
        while ((item = deq.peek()) != null) {
            if (item.canTruncate(handle)) {
                deq.poll();
            } else {
                break;
            }
        }
    }

    // return the last seen SP handle
    public long getLastSpHandle()
    {
        return m_lastSpHandle;
    }

    Comparator<Item> m_handleComparator = new Comparator<Item>()
    {
        @Override
        public int compare(Item i1, Item i2)
        {
            if (i1.getHandle() < i2.getHandle()) {
                return -1;
            }
            else if (i1.getHandle() > i2.getHandle()) {
                return 1;
            }
            return 0;
        }
    };

    // produce the contents of the repair log.
    public List<Iv2RepairLogResponseMessage> contents(long requestId, boolean forMPI)
    {
        List<Item> items = new LinkedList<Item>();
        // All cases include the log of MP transactions
        items.addAll(m_logMP);
        // SP repair requests also want the SP transactions
        if (!forMPI) {
            items.addAll(m_logSP);
        }

        // Contents need to be sorted in increasing spHandle order
        Collections.sort(items, m_handleComparator);

        int ofTotal = items.size() + 1;
        tmLog.debug("Responding with " + ofTotal + " repair log parts.");
        List<Iv2RepairLogResponseMessage> responses =
            new LinkedList<Iv2RepairLogResponseMessage>();

        // this constructor sets its sequence no to 0 as ack
        // messages are first in the sequence
        Iv2RepairLogResponseMessage hheader =
                new Iv2RepairLogResponseMessage(
                        requestId,
                        ofTotal,
                        m_lastSpHandle,
                        m_lastMpHandle,
                        TheHashinator.getCurrentVersionedConfigCooked());
        responses.add(hheader);

        int seq = responses.size();

        Iterator<Item> itemator = items.iterator();
        while (itemator.hasNext()) {
            Item item = itemator.next();
            Iv2RepairLogResponseMessage response =
                new Iv2RepairLogResponseMessage(
                        requestId,
                        seq++,
                        ofTotal,
                        item.getHandle(),
                        item.getTxnId(),
                        item.getMessage());
            responses.add(response);
        }
        return responses;
    }
}
