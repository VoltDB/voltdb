/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.util.Collections;
import java.util.Deque;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Map.Entry;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Maps;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.exceptions.TransactionRestartException;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;


/**
 * Provide an implementation of the TransactionTaskQueue specifically for the MPI.
 * This class will manage separating the stream of reads and writes to different
 * Sites and block appropriately so that reads and writes never execute concurrently.
 *
 * Extend the responsibility of the MpTransactionTaskQueue that handles concurrent scheduling non overlappping 2p txn
 */
public class MpTransactionTaskQueue extends TransactionTaskQueue
{
    protected static final VoltLogger tmLog = new VoltLogger("TM");

    // Track the current writes and reads in progress.  If writes contains anything, reads must be empty,
    // and vice versa

    // System allow mp/np txn in following 3 states
    // 1. one mp write txn
    // 2. multiple Np write and np read txns
    // 3. multiple np read and mp read txns
    private Map.Entry<Long, TransactionTask> m_currentMpWrite;
    private final Map<Long, TransactionTask> m_currentNpWrites = new HashMap<>();
    private final Map<Long, TransactionTask> m_currentMpReads = new HashMap<>();
    private final Map<Long, TransactionTask> m_currentNpReads = new HashMap<>();
    private int m_numberOfPartitions;
    private Deque<TransactionTask> m_backlog = new ArrayDeque<>();
    // secondary backlog
    private Map<Long, TransactionTask> m_npBacklog = new HashMap<>();
    // TODO: handle partition count changes case during elastic operation
    private ImmutableList<Deque<Long>> m_ledgerForWrite;
    private ImmutableList<Deque<Long>> m_ledgerForRead;

    private NpSitePool m_sitePool = null;

    private long m_repairLogTruncationHandle = Long.MIN_VALUE;

    MpTransactionTaskQueue(SiteTaskerQueue queue, int numberOfPartitions)
    {
        super(queue, false);
        m_numberOfPartitions = numberOfPartitions;
        ImmutableList.Builder<Deque<Long>> builderForRead = ImmutableList.builder();
        ImmutableList.Builder<Deque<Long>> builderForWrite = ImmutableList.builder();
        for (int i=0; i< m_numberOfPartitions; i++){
            builderForRead.add(new ArrayDeque<>());
            builderForWrite.add(new ArrayDeque<>());
        }
        m_ledgerForWrite = builderForWrite.build();
        m_ledgerForRead = builderForRead.build();
    }

    void setNpSitePool(NpSitePool sitePool)
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
    synchronized void offer(TransactionTask task) {
        Iv2Trace.logTransactionTaskQueueOffer(task);
        m_backlog.addLast(task);
        taskQueueOffer();
    }

    // repair is used by MPI repair to inject a repair task into the
    // SiteTaskerQueue.  Before it does this, it unblocks the MP transaction
    // that may be running in the Site thread and causes it to rollback by
    // faking an unsuccessful FragmentResponseMessage.
    synchronized void repair(SiteTasker task, List<Long> masters, Map<Integer, Long> partitionMasters, boolean balanceSPI)
    {
        // We know that every Site assigned to the MPI (either the main writer or
        // any of the MP read pool) will only have one active transaction at a time,
        // and that we either have active reads or active writes, but never both.
        // Figure out which we're doing, and then poison all of the appropriate sites.
        Map<Long, TransactionTask> currentSet;
        if (m_currentMpWrite != null) {
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("MpTTQ: repairing MP writes. MigratePartitionLeader:" + balanceSPI);
            }
            m_taskQueue.offer(task);
            currentSet = Collections.singletonMap(m_currentMpWrite.getKey(), m_currentMpWrite.getValue());
        } else {
            currentSet = new HashMap<>();
            if (!m_currentNpWrites.isEmpty()) {
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("MpTTQ: repairing writes. MigratePartitionLeader:" + balanceSPI);
                }
                for (Long txnId : m_currentNpWrites.keySet()) {
                    m_sitePool.repair(txnId, task);
                }
                currentSet.putAll(m_currentNpWrites);
            }
            if (!m_currentNpReads.isEmpty()) {
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("MpTTQ: repairing NP reads. MigratePartitionLeader:" + balanceSPI);
                }
                for (Long txnId : m_currentNpReads.keySet()) {
                    m_sitePool.repair(txnId, task);
                }
                currentSet.putAll(m_currentNpReads);
            }
            if (!m_currentMpReads.isEmpty()) {
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("MpTTQ: repairing MP reads. MigratePartitionLeader:" + balanceSPI);
                }
                for (Long txnId : m_currentMpReads.keySet()) {
                    m_sitePool.repair(txnId, task);
                }
                currentSet.putAll(m_currentMpReads);
            }
        }

        for (Entry<Long, TransactionTask> e : currentSet.entrySet()) {
            if (e.getValue() instanceof MpProcedureTask) {
                MpProcedureTask next = (MpProcedureTask)e.getValue();
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("MpTTQ: poisoning task: " + next.toShortString());
                }
                next.doRestart(masters, partitionMasters);

                if (!balanceSPI) {
                    MpTransactionState txn = (MpTransactionState)next.getTransactionState();
                    // inject poison pill
                    FragmentTaskMessage dummy = new FragmentTaskMessage(0L, 0L, txn.txnId, txn.uniqueId,
                            false, false, false, txn.isNPartTxn(), txn.getTimetamp());
                    FragmentResponseMessage poison =
                            new FragmentResponseMessage(dummy, 0L); // Don't care about source HSID here
                    // Provide a TransactionRestartException which will be converted
                    // into a ClientResponse.RESTART, so that the MpProcedureTask can
                    // detect the restart and take the appropriate actions.
                    TransactionRestartException restart = new TransactionRestartException(
                            "Transaction being restarted due to fault recovery or shutdown.", next.getTxnId());
                    restart.setMisrouted(false);
                    poison.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, restart);
                    txn.offerReceivedFragmentResponse(poison);
                    if (tmLog.isDebugEnabled()) {
                        tmLog.debug("MpTTQ: restarting:" + next.toShortString());
                    }
                }
            }
        }
        // Now, iterate through the backlog and update the partition masters
        // for all ProcedureTasks
        Iterator<TransactionTask> iter = m_backlog.iterator();
        while (iter.hasNext()) {
            TransactionTask tt = iter.next();
            if (tt instanceof MpProcedureTask) {
                MpProcedureTask next = (MpProcedureTask)tt;

                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("Repair updating task: " + next.toShortString() + " with masters: " + CoreUtils.hsIdCollectionToString(masters));
                }
                next.updateMasters(masters, partitionMasters);
            }
            else if (tt instanceof EveryPartitionTask) {
                EveryPartitionTask next = (EveryPartitionTask)tt;
                if (tmLog.isDebugEnabled())  {
                    tmLog.debug("Repair updating EPT task: " + next + " with masters: " + CoreUtils.hsIdCollectionToString(masters));
                }
                next.updateMasters(masters);
            }
        }
    }

    private void offerTaskToPoolOrSite(TransactionTask task, boolean toSitePool) {
        Iv2Trace.logSiteTaskerQueueOffer(task);
        if (toSitePool) {
            m_sitePool.doWork(task.getTxnId(), task);
        }
        else {
            m_taskQueue.offer(task);
        }
    }

    private boolean isInvolvedPartitionsAvailable(TransactionTask task) {
        Set<Integer> partitions = task.getTransactionState().getInvolvedPartitions();
        boolean readOnly = task.getTransactionState().isReadOnly();
        boolean retval = true;
        if (readOnly) {
            for (int partition : partitions) {
                if (!m_ledgerForWrite.get(partition).isEmpty()) {
                    retval = false;
                }
                m_ledgerForRead.get(partition).offer(task.getTxnId());
            }
            if (retval) {
                m_currentNpReads.put(task.getTxnId(), task);
            }
        } else {
            retval = m_currentMpReads.isEmpty();
            for (int partition : partitions) {
                if (!m_ledgerForWrite.get(partition).isEmpty() || !m_ledgerForRead.get(partition).isEmpty()) {
                    retval = false;
                }
                m_ledgerForWrite.get(partition).offer(task.getTxnId());
            }
            if (retval) {
                m_currentNpWrites.put(task.getTxnId(), task);
            }
        }

        return retval;
    }

    private boolean taskQueueOffer()
    {
        // Do we have something to do?
        // - If so, is it a MP write?
        //   - If so, are there reads or writes outstanding ?
        //     - if not, pull it from the backlog, add it to the current write set, and queue it
        //     - if so, bail for now

        //   - If not, is it NP Txn?
        //     - if so, while there are NP txn on the backlog and pool has capacity:
        //          - pull from backlog
        //          - if it's write
        //              - are there reads or writes outstanding on involved partitions
        //                  - if not, add to current write set, queue it
        //                  - if so, put to the secondary backlog (per partition based)
        //          - if it's read
        //              - are there writes outstanding on involved partitions
        //                  - if not, add to current read set, queue it
        //                  - if so, put to the secondary backlog (per partition based)

        //     - if not, while there are reads on the backlog and the pool has capacity:
        //       - pull the read from the backlog, add it to the  current read set, and queue it
        //       - bail when done
        //     - if so, bail for now

        boolean retval = false;
        if (m_currentMpWrite == null) {
            // loop through the ledger first
            Map<Long, Integer> count = new HashMap<>();
            for (int i = 0; i < m_numberOfPartitions; i++) {
                if (!m_ledgerForWrite.get(i).isEmpty()) {
                    long txnId = m_ledgerForWrite.get(i).getFirst();
                    count.put(txnId, count.getOrDefault(txnId, 0) + 1);
                } else if (!m_ledgerForRead.get(i).isEmpty()) {
                    long txnId = m_ledgerForRead.get(i).getFirst();
                    count.put(txnId, count.getOrDefault(txnId, 0) + 1);
                }
            }
            for (Map.Entry<Long, Integer> pair: count.entrySet()) {
                TransactionTask task = m_npBacklog.get(pair.getKey());
                assert(task != null);
                if (pair.getValue() == task.getTransactionState().getInvolvedPartitions().size()) {
                    m_npBacklog.remove(pair.getKey());
                    if (task.getTransactionState().isReadOnly()) {
                        m_currentNpReads.put(task.getTxnId(), task);
                    } else {
                        m_currentNpWrites.put(task.getTxnId(), task);
                    }
                    offerTaskToPoolOrSite(task, true);
                    retval = true;
                }
            }
            if (!m_backlog.isEmpty()) {
                // We may not queue the next task, just peek to get the read-only state
                TransactionTask task = m_backlog.peekFirst();
                if (!task.getTransactionState().isNPartTxn() && !task.getTransactionState().isReadOnly()) { // MP write
                    if (m_currentMpReads.isEmpty() && m_currentNpReads.isEmpty() && m_currentNpWrites.isEmpty()) {
                        task = m_backlog.pollFirst();
                        m_currentMpWrite = Maps.immutableEntry(task.getTxnId(), task);
                        offerTaskToPoolOrSite(task, false);
                        retval = true;
                    }
                } else if (task.getTransactionState().isNPartTxn()) { // NP txn
                    while (task != null && task.getTransactionState().isNPartTxn() &&
                            m_sitePool.canAcceptWork()) {
                        task = m_backlog.pollFirst();
                        if (isInvolvedPartitionsAvailable(task)) {
                            offerTaskToPoolOrSite(task, true);
                            retval = true;
                        } else {
                            m_npBacklog.put(task.getTxnId(), task);
                        }
                        // Prime the pump with the head task, if any.  If empty,
                        // task will be null
                        task = m_backlog.peekFirst();
                    }
                } else if (m_currentNpWrites.isEmpty()) { // MP read
                    while (task != null && task.getTransactionState().isReadOnly() &&
                            m_sitePool.canAcceptWork()) {
                        task = m_backlog.pollFirst();
                        assert (task.getTransactionState().isReadOnly());
                        m_currentMpReads.put(task.getTxnId(), task);
                        offerTaskToPoolOrSite(task, true);
                        retval = true;
                        // Prime the pump with the head task, if any.  If empty,
                        // task will be null
                        task = m_backlog.peekFirst();
                    }
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
    // TODO: remove the unused returned offered value
    @Override
    synchronized int flush(long txnId)
    {
        int offered = 0;
        if (m_currentMpReads.containsKey(txnId)) {
            m_currentMpReads.remove(txnId);
            m_sitePool.completeWork(txnId);
        } else if (m_currentNpReads.containsKey(txnId)) {
            TransactionTask task = m_currentNpReads.remove(txnId);
            Set<Integer> partitions = task.getTransactionState().getInvolvedPartitions();
            for (int partition: partitions) {
                m_ledgerForRead.get(partition).remove(txnId);
            }
            m_sitePool.completeWork(txnId);
        } else if (m_currentNpWrites.containsKey(txnId)) {
            TransactionTask task = m_currentNpWrites.remove(txnId);
            Set<Integer> partitions = task.getTransactionState().getInvolvedPartitions();
            for (int partition: partitions) {
                m_ledgerForWrite.get(partition).remove(txnId);
            }
            m_sitePool.completeWork(txnId);
        } else {
            assert(txnId == m_currentMpWrite.getKey());
            m_currentMpWrite = null;
        }

        if (taskQueueOffer()) {
            ++offered; // TODO: for np, this offered is not accurate
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
        if (m_currentMpWrite != null) {
            assert (m_currentNpReads.isEmpty() & m_currentNpWrites.isEmpty() && m_currentMpReads.isEmpty());
            offerTaskToPoolOrSite(m_currentMpWrite.getValue(), false);
        } else {
            // re-submit all the tasks in the current read set to the pool.
            // the pool will ensure that things submitted with the same
            // txnID will go to the the NpSite which is currently running it
            for (TransactionTask task : m_currentNpWrites.values()) {
                offerTaskToPoolOrSite(task, true);
            }
            for (TransactionTask task : m_currentNpReads.values()) {
                offerTaskToPoolOrSite(task, true);
            }
            for (TransactionTask task : m_currentMpReads.values()) {
                offerTaskToPoolOrSite(task, true);
            }
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
        sb.append("\tcurrent mp write: ").append(m_currentMpWrite).append("\n");
        sb.append("\tcurrent mp reads: ").append(m_currentMpReads).append("\n");
        sb.append("\tcurrent np write: ").append(m_currentNpWrites).append("\n");
        sb.append("\tcurrent np read: ").append(m_currentNpReads).append("\n");


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
