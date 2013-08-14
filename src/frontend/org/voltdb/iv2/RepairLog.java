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

package org.voltdb.iv2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;

/**
 * The repair log stores messages received from a PI in case they need to be
 * shared with less informed RIs should the PI shed its mortal coil.
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
    }

    // log storage.
    final List<Item> m_log;

    RepairLog()
    {
        m_log = new ArrayList<Item>();
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
        // The leader doesn't truncate its own log; if promoted,
        // wipe out the SP portion of the existing log. This promotion
        // action always happens after repair is completed.
        if (m_isLeader) {
            truncate(Long.MIN_VALUE, Long.MAX_VALUE);
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
                truncate(Long.MIN_VALUE, m.getTruncationHandle());
                m_log.add(new Item(IS_SP, m, m.getSpHandle(), m.getTxnId()));
            }
        } else if (msg instanceof FragmentTaskMessage) {
            final TransactionInfoBaseMessage m = (TransactionInfoBaseMessage)msg;
            if (!m.isReadOnly()) {
                truncate(m.getTruncationHandle(), Long.MIN_VALUE);
                // only log the first fragment of a procedure (and handle 1st case)
                if (m.getTxnId() > m_lastMpHandle || m_lastMpHandle == Long.MAX_VALUE) {
                    m_log.add(new Item(IS_MP, m, m.getSpHandle(), m.getTxnId()));
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
                truncate(ctm.getTruncationHandle(), Long.MIN_VALUE);
                m_log.add(new Item(IS_MP, ctm, ctm.getSpHandle(), ctm.getTxnId()));
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
    private void truncate(long mpHandle, long spHandle)
    {
        // MIN signals no truncation work to do.
        if (spHandle == Long.MIN_VALUE && mpHandle == Long.MIN_VALUE) {
            return;
        }

        Iterator<RepairLog.Item> it = m_log.iterator();
        while (it.hasNext()) {
            RepairLog.Item item = it.next();
            if (item.isSP() && item.m_handle <= spHandle) {
                it.remove();
            }
            else if (item.isMP() && item.m_txnId <= mpHandle) {
                it.remove();
            }
        }
    }

    // return the last seen SP handle
    public long getLastSpHandle()
    {
        return m_lastSpHandle;
    }

    // produce the contents of the repair log.
    public List<Iv2RepairLogResponseMessage> contents(long requestId, boolean forMPI)
    {
        List<Item> items = new LinkedList<Item>();
        Iterator<Item> it = m_log.iterator();
        while (it.hasNext()) {
            Item i = it.next();
            if (!forMPI || i.isMP()) {
                items.add(i);
            }
        }

        int ofTotal = items.size() + 1;
        tmLog.debug("Responding with " + ofTotal + " repair log parts.");
        List<Iv2RepairLogResponseMessage> responses =
            new LinkedList<Iv2RepairLogResponseMessage>();

        int seq = 0;
        Iv2RepairLogResponseMessage header =
            new Iv2RepairLogResponseMessage(
                    requestId,
                    seq++,
                    ofTotal,
                    m_lastSpHandle,
                    m_lastMpHandle,
                    null); // no payload. just an ack.
        responses.add(header);

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
