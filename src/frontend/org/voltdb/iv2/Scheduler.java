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

package org.voltdb.iv2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.voltcore.messaging.Mailbox;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.VoltDB;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class Scheduler
{
    final private SiteTaskerQueue m_tasks;
    private LoadedProcedureSet m_loadedProcs;
    private PartitionClerk m_clerk;
    private Mailbox m_mailbox;
    private Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();

    // hacky temp txnid
    AtomicLong m_txnId = new AtomicLong(0);

    Scheduler(PartitionClerk clerk)
    {
        m_tasks = new SiteTaskerQueue();
        m_clerk = clerk;
    }

    void setMailbox(Mailbox mailbox)
    {
        m_mailbox = mailbox;
    }

    void setProcedureSet(LoadedProcedureSet loadedProcs)
    {
        m_loadedProcs = loadedProcs;
    }

    public SiteTaskerQueue getQueue()
    {
        return m_tasks;
    }

    public void handleIv2InitiateTaskMessage(Iv2InitiateTaskMessage message)
    {
        final String procedureName = message.getStoredProcedureName();
        if (message.isSinglePartition()) {
            final SpProcedureTask task =
                new SpProcedureTask(m_mailbox, m_loadedProcs.procs.get(procedureName),
                        m_txnId.incrementAndGet(), message);
            m_tasks.offer(task);
        }
        else {
            // HACK: grab the current sitetracker until we write leader notices.
            m_clerk = VoltDB.instance().getSiteTracker();
            final List<Long> partitionInitiators = m_clerk.getHSIdsForPartitionInitiators();
            System.out.println("partitionInitiators list: " + partitionInitiators.toString());
            final MpProcedureTask task =
                new MpProcedureTask(m_mailbox, m_loadedProcs.procs.get(procedureName),
                        m_txnId.incrementAndGet(), message, partitionInitiators);
            m_outstandingTxns.put(task.m_txn.txnId, task.m_txn);
            System.out.println("CREATED INIT map for TXNID: " + task.m_txn.txnId);
            m_tasks.offer(task);
        }
    }

    public void handleFragmentTaskMessage(FragmentTaskMessage message)
    {
        // IZZY: going to need to keep this around or extract the
        // transaction state from the task for scheduler blocking
        // Actually, we're going to need to create the transaction state
        // here if one does not exist so that we can hand it to future
        // FragmentTasks
        //
        // For now (one-shot reads), just create everything from scratch
        long localTxnId = m_txnId.incrementAndGet();
        final FragmentTask task =
            new FragmentTask(m_mailbox, localTxnId, message);
        m_outstandingTxns.put(message.getTxnId(), task.m_txn);
        System.out.println("CREATED FRAG map for TXNID: " + message.getTxnId());
        m_tasks.offer(task);
    }

    public void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        System.out.println(this.m_mailbox.getHSId() + " LOOKING UP RESP map for TXNID: " + message.getTxnId());
        TransactionState txn = m_outstandingTxns.get(message.getTxnId());
        ((MpTransactionState)txn).offerReceivedFragmentResponse(message);
    }

    public void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        System.out.println(this.m_mailbox.getHSId() + "LOOKING UP COMPLETE map for TXNID: " + message.getTxnId());
        TransactionState txn = m_outstandingTxns.remove(message.getTxnId());
        // XXX IZZY This feels like papering over something...come back and
        // be smarter later
        if (txn != null)
        {
            final CompleteTransactionTask task =
                new CompleteTransactionTask(txn, message);
            m_tasks.offer(task);
        }
    }
}
