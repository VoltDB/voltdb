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

package org.voltdb.iv2;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.exceptions.TransactionRestartException;
import org.voltdb.iv2.MpTerm.RepairType;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;

/**
 * Provide an implementation of the TransactionTaskQueue specifically for the MPI.
 * This class will manage separating the stream of reads and writes to different
 * Sites and block appropriately so that reads and writes never execute concurrently.
 */
public class MpTransactionTaskQueue extends TransactionTaskQueue
{
    protected static final VoltLogger tmLog = new VoltLogger("TM");
    public static final String TXN_RESTART_MSG = "Transaction being restarted due to fault recovery or shutdown.";
    // Track the current writes and reads in progress.  If writes contains anything, reads must be empty,
    // and vice versa
    private final Map<Long, TransactionTask> m_currentWrites = new HashMap<Long, TransactionTask>();
    private final Map<Long, TransactionTask> m_currentReads = new HashMap<Long, TransactionTask>();
    private Deque<TransactionTask> m_backlog = new ArrayDeque<TransactionTask>();

    private MpRoSitePool m_sitePool = null;

    private long m_repairLogTruncationHandle = Long.MIN_VALUE;

    MpTransactionTaskQueue(SiteTaskerQueue queue)
    {
        super(queue, false);
    }

    void setMpRoSitePool(MpRoSitePool sitePool)
    {
        m_sitePool = sitePool;
    }

    synchronized void updateCatalog(String diffCmds, CatalogContext context)
    {
        m_sitePool.updateCatalog(diffCmds, context);
    }

    synchronized void updateSettings(CatalogContext context)
    {
        m_sitePool.updateSettings(context);
    }

    void shutdown()
    {
        if (m_sitePool != null) {
            m_sitePool.shutdown();
        }
    }

    /**
     * Stick this task in the backlog.
     * Many network threads may be racing to reach here, synchronize to
     * serialize queue order.
     * Always returns true in this case, side effect of extending
     * TransactionTaskQueue.
     */
    @Override
    synchronized void offer(TransactionTask task)
    {
        Iv2Trace.logTransactionTaskQueueOffer(task);
        m_backlog.addLast(task);
        taskQueueOffer();
    }

    // repair is used by MPI repair to inject a repair task into the
    // SiteTaskerQueue.  Before it does this, it unblocks the MP transaction
    // that may be running in the Site thread and causes it to rollback by
    // faking an unsuccessful FragmentResponseMessage.
    synchronized void repair(SiteTasker task, List<Long> masters, Map<Integer, Long> partitionMasters, RepairType repairType)
    {
        // We know that every Site assigned to the MPI (either the main writer or
        // any of the MP read pool) will only have one active transaction at a time,
        // and that we either have active reads or active writes, but never both.
        // Figure out which we're doing, and then poison all of the appropriate sites.
        Map<Long, TransactionTask> currentSet;
        if (!m_currentReads.isEmpty()) {
            assert(m_currentWrites.isEmpty());
            for (Long txnId : m_currentReads.keySet()) {
                m_sitePool.repair(txnId, task);
            }
            currentSet = m_currentReads;
        }
        else {
            m_taskQueue.offer(task);
            currentSet = m_currentWrites;
        }
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("MpTTQ: repairing transactions. Transaction restart:" + repairType.isSkipTxnRestart());
        }

        for (Entry<Long, TransactionTask> e : currentSet.entrySet()) {
            if (e.getValue() instanceof MpProcedureTask) {
                MpProcedureTask next = (MpProcedureTask)e.getValue();
                MpTransactionState txn = (MpTransactionState)next.getTransactionState();

                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("MpTTQ: poisoning task: " + next.toShortString());
                }
                next.doRestart(masters, partitionMasters);
                if ( repairType.isTxnRestart()) {
                    // If there are dependencies on failed host for rerouted transaction, poison the transaction
                    if (txn.checkFailedHostDependancies(masters)) {
                        poisonTransaction(txn, next);
                    }
                } else if (!repairType.isSkipTxnRestart()) {
                    poisonTransaction(txn, next);
                }
            }
        }
        // Now, iterate through the backlog and update the partition masters
        // for all ProcedureTasks
        Iterator<TransactionTask> iter = m_backlog.iterator();
        while (iter.hasNext()) {
            TransactionTask tt = iter.next();
            tt.updateMasters(masters, partitionMasters);
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("Repair updating task: " + tt + " with masters: " + CoreUtils.hsIdCollectionToString(masters));
            }
        }
    }

    private void poisonTransaction(MpTransactionState txn, MpProcedureTask next) {
        // inject poison pill
        FragmentTaskMessage dummy = new FragmentTaskMessage(0L, 0L, txn.txnId, txn.uniqueId,
                false, false, false, txn.isNPartTxn(), txn.getTimetamp());
        FragmentResponseMessage poison =
                new FragmentResponseMessage(dummy, 0L); // Don't care about source HSID here
        // Provide a TransactionRestartException which will be converted
        // into a ClientResponse.RESTART, so that the MpProcedureTask can
        // detect the restart and take the appropriate actions.
        TransactionRestartException restart = new TransactionRestartException(TXN_RESTART_MSG, next.getTxnId());
        restart.setMisrouted(false);
        poison.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, restart);
        txn.offerReceivedFragmentResponse(poison);
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("MpTTQ: restarting:" + next.toShortString());
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
        // Do we have something to do?
        // - If so, is it a write?
        //   - If so, are there reads or writes outstanding?
        //     - if not, pull it from the backlog, add it to current write set, and queue it
        //     - if so, bail for now
        //   - If not, are there writes outstanding?
        //     - if not, while there are reads on the backlog and the pool has capacity:
        //       - pull the read from the backlog, add it to the current read set, and queue it.
        //       - bail when done
        //     - if so, bail for now

        boolean retval = false;
        if (!m_backlog.isEmpty()) {
            // We may not queue the next task, just peek to get the read-only state
            TransactionTask task = m_backlog.peekFirst();
            if (!task.getTransactionState().isReadOnly()) {
                if (m_currentReads.isEmpty() && m_currentWrites.isEmpty()) {
                    task = m_backlog.pollFirst();
                    m_currentWrites.put(task.getTxnId(), task);
                    taskQueueOffer(task);
                    retval = true;
                }
            }
            else if (m_currentWrites.isEmpty()) {
                while (task != null && task.getTransactionState().isReadOnly() &&
                       m_sitePool.canAcceptWork())
                {
                    task = m_backlog.pollFirst();
                    assert(task.getTransactionState().isReadOnly());
                    m_currentReads.put(task.getTxnId(), task);
                    taskQueueOffer(task);
                    retval = true;
                    // Prime the pump with the head task, if any.  If empty,
                    // task will be null
                    task = m_backlog.peekFirst();
                }
            }
        }
        return retval;
    }

    /**
     * Indicate that the transaction associated with txnId is complete.  Perform
     * management of reads/writes in progress then call taskQueueOffer() to
     * submit additional tasks to be done, determined by whatever the current state is.
     * See giant comment at top of taskQueueOffer() for what happens.
     */
    @Override
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
    @Override
    synchronized void restart()
    {
        if (!m_currentReads.isEmpty()) {
            // re-submit all the tasks in the current read set to the pool.
            // the pool will ensure that things submitted with the same
            // txnID will go to the the MpRoSite which is currently running it
            for (TransactionTask task : m_currentReads.values()) {
                taskQueueOffer(task);
            }
        }
        else {
            assert(!m_currentWrites.isEmpty());
            TransactionTask task;
            // There currently should only ever be one current write.  This
            // is the awkward way to get a single value out of a Map
            task = m_currentWrites.entrySet().iterator().next().getValue();
            taskQueueOffer(task);
        }
    }

    /**
     * How many Tasks are un-runnable?
     * @return
     */
    @Override
    synchronized int size()
    {
        return m_backlog.size();
    }

    synchronized public void toString(StringBuilder sb)
    {
        sb.append("MpTransactionTaskQueue:").append("\n");
        sb.append("\tSIZE: ").append(m_backlog.size());
        if (!m_backlog.isEmpty()) {
            // Print deduped list of backlog
            Iterator<TransactionTask> it = m_backlog.iterator();
            Set<String> pendingInvocations = new HashSet<>(m_backlog.size()*2);
            if (it.hasNext()) {
                String procName = getProcName(it.next());
                pendingInvocations.add(procName);
                sb.append("\n\tPENDING: ").append(procName);
            }
            while(it.hasNext()) {
                String procName = getProcName(it.next());
                if (pendingInvocations.add(procName)) {
                    sb.append(", ").append(procName);
                }
            }
        }
    }

    private String getProcName(TransactionTask task) {
        return (task.m_txnState == null) ? "Null txn state" :
                   (task.m_txnState.getInvocation() == null) ?
                   "Null invocation" : task.m_txnState.getInvocation().getProcName();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        toString(sb);
        return sb.toString();
    }

    public long getRepairLogTruncationHandle() {
        return m_repairLogTruncationHandle;
    }

     public void setRepairLogTruncationHandle(long repairLogTruncationHandle) {
        m_repairLogTruncationHandle = repairLogTruncationHandle;
    }
}
