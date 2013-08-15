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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.exceptions.TransactionRestartException;

import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;

public class MpTransactionTaskQueue extends TransactionTaskQueue
{
    protected static final VoltLogger tmLog = new VoltLogger("TM");

    private final Map<Long, TransactionTask> m_currentWrites = new HashMap<Long, TransactionTask>();
    private final Map<Long, TransactionTask> m_currentReads = new HashMap<Long, TransactionTask>();
    private Deque<TransactionTask> m_backlog = new ArrayDeque<TransactionTask>();
    private Deque<TransactionTask> m_readBacklog = new ArrayDeque<TransactionTask>();

    private MpRoSitePool m_sitePool = null;

    MpTransactionTaskQueue(SiteTaskerQueue queue)
    {
        super(queue);
    }

    void setMpRoSitePool(MpRoSitePool sitePool)
    {
        m_sitePool = sitePool;
    }

    synchronized void updateCatalog(String diffCmds, CatalogContext context, CatalogSpecificPlanner csp)
    {
        m_sitePool.updateCatalog(diffCmds, context, csp);
    }

    /**
     * If necessary, stick this task in the backlog.
     * Many network threads may be racing to reach here, synchronize to
     * serialize queue order
     * @param task
     * @return true if this task was stored, false if not
     */
    synchronized boolean offer(TransactionTask task)
    {
        Iv2Trace.logTransactionTaskQueueOffer(task);
        if (task.getTransactionState().isReadOnly()) {
            m_readBacklog.addLast(task);
        }
        else {
            m_backlog.addLast(task);
        }
        taskQueueOffer();
        return true;
    }

    // repair is used by MPI repair to inject a repair task into the
    // SiteTaskerQueue.  Before it does this, it unblocks the MP transaction
    // that may be running in the Site thread and causes it to rollback by
    // faking an unsuccessful FragmentResponseMessage.
    synchronized void repair(SiteTasker task, List<Long> masters, Map<Integer, Long> partitionMasters)
    {
        // At the MPI's TransactionTaskQueue, we know that there can only be
        // one transaction in the SiteTaskerQueue at a time, because the
        // TransactionTaskQueue will only release one MP transaction to the
        // SiteTaskerQueue at a time.  So, when we offer this repair task, we
        // know it will be the next thing to run once we poison the current
        // TXN.
        // First, poison all the stuff that is currently running
        Map<Long, TransactionTask> currentSet;
        if (!m_currentReads.isEmpty()) {
            assert(m_currentWrites.isEmpty());
            tmLog.debug("MpTTQ: repairing reads");
            for (Long txnId : m_currentReads.keySet()) {
                m_sitePool.repair(txnId, task);
            }
            currentSet = m_currentReads;
        }
        else {
            tmLog.debug("MpTTQ: repairing writes");
            m_taskQueue.offer(task);
            currentSet = m_currentWrites;
        }
        for (Entry<Long, TransactionTask> e : currentSet.entrySet()) {
            if (e.getValue() instanceof MpProcedureTask) {
                MpProcedureTask next = (MpProcedureTask)e.getValue();
                tmLog.debug("MpTTQ: poisoning task: " + next);
                next.doRestart(masters, partitionMasters);
                // get head
                // Only the MPI's TransactionTaskQueue is ever called in this way, so we know
                // that the TransactionTasks we pull out of it have to be MP transactions, so this
                // cast is safe
                MpTransactionState txn = (MpTransactionState)next.getTransactionState();
                // inject poison pill
                FragmentTaskMessage dummy = new FragmentTaskMessage(0L, 0L, 0L, 0L, false, false, false);
                FragmentResponseMessage poison =
                    new FragmentResponseMessage(dummy, 0L); // Don't care about source HSID here
                // Provide a TransactionRestartException which will be converted
                // into a ClientResponse.RESTART, so that the MpProcedureTask can
                // detect the restart and take the appropriate actions.
                TransactionRestartException restart = new TransactionRestartException(
                        "Transaction being restarted due to fault recovery or shutdown.", next.getTxnId());
                poison.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, restart);
                txn.offerReceivedFragmentResponse(poison);
            }
            else {
                assert(false);
            }
        }
        // Now, iterate through both backlogs and update the partition masters
        // for all MpProcedureTasks not at the head of the TransactionTaskQueue
        Iterator<TransactionTask> iter = m_backlog.iterator();
        while (iter.hasNext()) {
            // EveryPartition work is just going to cross its fingers here.
            TransactionTask tt = iter.next();
            if (task instanceof MpProcedureTask) {
                MpProcedureTask next = (MpProcedureTask)tt;
                next.updateMasters(masters, partitionMasters);
            }
            else {
                assert(false);
            }
        }
        iter = m_readBacklog.iterator();
        while (iter.hasNext()) {
            // EveryPartition work is just going to cross its fingers here.
            TransactionTask tt = iter.next();
            if (task instanceof MpProcedureTask) {
                MpProcedureTask next = (MpProcedureTask)tt;
                next.updateMasters(masters, partitionMasters);
            }
            else {
                assert(false);
            }
        }
    }

    private void taskQueueOffer(TransactionTask task)
    {
        Iv2Trace.logSiteTaskerQueueOffer(task);
        if (task.getTransactionState().isReadOnly()) {
            m_sitePool.doWork(task.getTxnId(), task);
        }
        else {
            m_taskQueue.offer(task);
        }
    }

    private boolean taskQueueOffer()
    {
        // Do we have a write to do?
        //   if so, are there reads or writes outstanding?
        //     if not, pull it from the write backlog, add it to current write set, and queue it
        //     if so, bail for now
        //   if not, do we have a read to do?
        //     if so, are we currently trying to do a write?
        //       if not, is there more capacity in the pool?
        //         if so, pull it from the read backlog, add it to current read set, and queue it
        //         if not, bail for now
        //       if so, bail for now
        //     if not, bail for now

        boolean retval = false;
        if (!m_backlog.isEmpty()) {
            if (m_currentReads.isEmpty() && m_currentWrites.isEmpty()) {
                TransactionTask task = m_backlog.pollFirst();
                assert(!task.getTransactionState().isReadOnly());
                m_currentWrites.put(task.getTxnId(), task);
                taskQueueOffer(task);
                retval = true;
            }
        }
        else if (!m_readBacklog.isEmpty()) {
            if (m_currentWrites.isEmpty()) {
                if (m_sitePool.canAcceptWork()) {
                    TransactionTask task = m_readBacklog.pollFirst();
                    assert(task.getTransactionState().isReadOnly());
                    m_currentReads.put(task.getTxnId(), task);
                    taskQueueOffer(task);
                    retval = true;
                }
            }
        }
        return retval;
    }

    /**
     * Try to offer as many runnable Tasks to the SiteTaskerQueue as possible.
     * @return the number of TransactionTasks queued to the SiteTaskerQueue
     */
    synchronized int flush(long txnId)
    {
        int offered = 0;
        if (m_currentReads.containsKey(txnId)) {
            m_currentReads.remove(txnId);
            m_sitePool.completeWork(txnId);
        }
        else {
            assert(m_currentWrites.containsKey(txnId));
            m_currentWrites.remove(txnId);
            assert(m_currentWrites.isEmpty());
        }
        if (taskQueueOffer()) {
            ++offered;
        }
        return offered;
    }

    /**
     * Restart the current task at the head of the queue.  This will be called
     * instead of flush by the currently blocking MP transaction in the event a
     * restart is necessary.
     */
    synchronized void restart()
    {
        TransactionTask task;
        if (!m_currentReads.isEmpty()) {
            task = m_currentReads.entrySet().iterator().next().getValue();
        }
        else {
            assert(!m_currentWrites.isEmpty());
            task = m_currentWrites.entrySet().iterator().next().getValue();
        }
        taskQueueOffer(task);
    }

    /**
     * How many Tasks are un-runnable?
     * @return
     */
    synchronized int size()
    {
        return m_backlog.size() + m_readBacklog.size();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("MpTransactionTaskQueue:").append("\n");
        sb.append("\tWRITE SIZE: ").append(m_backlog.size()).append("\n");
        if (!m_backlog.isEmpty()) {
            sb.append("\tWRITE HEAD: ").append(m_backlog.getFirst()).append("\n");
        }
        sb.append("\tREAD SIZE:  ").append(m_readBacklog.size()).append("\n");
        if (!m_backlog.isEmpty()) {
            sb.append("\tREAD HEAD: ").append(m_readBacklog.getFirst()).append("\n");
        }
        return sb.toString();
    }
}
