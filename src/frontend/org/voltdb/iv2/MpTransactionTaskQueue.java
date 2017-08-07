/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import org.voltdb.exceptions.TransactionRestartException;
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

    // Track the current writes and reads in progress.  If writes contains anything, reads must be empty,
    // and vice versa
    private final Map<Long, TransactionTask> m_currentWrites = new HashMap<Long, TransactionTask>();
    private final Map<Long, TransactionTask> m_currentReads = new HashMap<Long, TransactionTask>();
    private Deque<TransactionTask> m_backlog = new ArrayDeque<TransactionTask>();

    private MpRoSitePool m_sitePool = null;

    // Partition r/w counts, the key is the partition master hsid
    private HashMap<Long, PartitionLock> m_lockedPartitions = new HashMap<>();

    // A temporary implementation for the r/w lock, will be improved to
    // use fixed arrays with primitives only.
    private class PartitionLock {
        private int reads, writes;

        public PartitionLock() {
            reads = 0;
            writes = 0;
        }

        public void updateRead(int count) {
            if (count == 1) {
                assert(writes == 0);
                reads += 1;
            } else {
                assert(count == -1);
                reads -= 1;
                assert(reads >= 0);
            }
        }

        public void updateWrite(int count) {
            if (count == 1) {
                assert(reads == 0 && writes == 0);
                writes += 1;
            } else {
                assert(count == -1);
                writes -= 1;
                assert(writes == 0);
            }
        }
    }

    MpTransactionTaskQueue(SiteTaskerQueue queue)
    {
        super(queue);
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
    synchronized boolean offer(TransactionTask task)
    {
        // if (task.isNP) {
        //     System.err.println(task.getPartitionMasterHsids());
        // }

        Iv2Trace.logTransactionTaskQueueOffer(task);
        m_backlog.addLast(task);
        taskQueueOffer();
        return true;
    }

    // repair is used by MPI repair to inject a repair task into the
    // SiteTaskerQueue.  Before it does this, it unblocks the MP transaction
    // that may be running in the Site thread and causes it to rollback by
    // faking an unsuccessful FragmentResponseMessage.
    // TODO: handle partition locks here as well. Updating the partition masters for NP txn must be carefully handled.
    synchronized void repair(SiteTasker task, List<Long> masters, Map<Integer, Long> partitionMasters)
    {
        // We know that every Site assigned to the MPI (either the main writer or
        // any of the MP read pool) will only have one active transaction at a time,
        // and that we either have active reads or active writes, but never both.
        // Figure out which we're doing, and then poison all of the appropriate sites.
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
                // Don't think that EveryPartitionTasks need to do anything here, since they
                // don't actually run java, they just exist for sequencing.  Any cleanup should be
                // to the duplicate counter in MpScheduler for this transaction.
            }
        }
        // Now, iterate through the backlog and update the partition masters
        // for all ProcedureTasks
        Iterator<TransactionTask> iter = m_backlog.iterator();
        while (iter.hasNext()) {
            TransactionTask tt = iter.next();
            if (tt instanceof MpProcedureTask) {
                MpProcedureTask next = (MpProcedureTask)tt;
                tmLog.debug("Repair updating task: " + next + " with masters: " + masters);
                next.updateMasters(masters, partitionMasters);
            }
            else if (tt instanceof EveryPartitionTask) {
                EveryPartitionTask next = (EveryPartitionTask)tt;
                tmLog.debug("Repair updating EPT task: " + next + " with masters: " + masters);
                next.updateMasters(masters);
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
        // Do we have something to do?
        // Keep do the following until first failure :

        // - If so, is it a write?
        //   - If so, are there reads or writes outstanding on the partitions used ?
        //     - if not, pull it from the backlog, add it to current write set, update the r/w counts, and queue it
        //     - if so, bail for now
        //   - If not, are there writes outstanding on the partitions used ?
        //     - if not, pull the read from the backlog, add it to the current read set, update the r/w counts, and queue it.
        //     - if so, bail for now

        boolean retval = false;
        if (!m_backlog.isEmpty()) {
            // We may not queue the next task, just peek to get the read-only state
            TransactionTask task = m_backlog.peekFirst();

            while (task != null) {
                if (!task.getTransactionState().isReadOnly()) {
                    // write txn
                    if ((task.isNP && checkPartitions(task))
                        ||
                        (!task.isNP && m_currentReads.isEmpty() && m_currentWrites.isEmpty())) {
                        task = m_backlog.pollFirst();
                        m_currentWrites.put(task.getTxnId(), task);
                        taskQueueOffer(task);
                        retval = true;
                        updatePartitionLocks(task, 1);
                    } else {
                        break;
                    }
                }
                else {
                    // read txn
                    if (m_sitePool.canAcceptWork() &&
                           (
                               (!task.isNP && m_currentWrites.isEmpty()) ||
                               (task.isNP && checkPartitions(task))
                           )
                       )
                    {
                        task = m_backlog.pollFirst();
                        assert(task.getTransactionState().isReadOnly());
                        m_currentReads.put(task.getTxnId(), task);
                        taskQueueOffer(task);
                        retval = true;
                        updatePartitionLocks(task, 1);
                    } else {
                        break;
                    }
                }

                task = m_backlog.peekFirst();
            }

            // DEBUG
            // System.err.println();
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
            updatePartitionLocks(m_currentReads.get(txnId), -1);
            m_currentReads.remove(txnId);
            m_sitePool.completeWork(txnId);
        }
        else {
            assert(m_currentWrites.containsKey(txnId));
            updatePartitionLocks(m_currentWrites.get(txnId), -1);
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
    // TODO: handle this for np as well
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

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("MpTransactionTaskQueue:").append("\n");
        sb.append("\tSIZE: ").append(m_backlog.size()).append("\n");
        if (!m_backlog.isEmpty()) {
            sb.append("\tHEAD: ").append(m_backlog.getFirst()).append("\n");
        }
        return sb.toString();
    }

    /**
     * update the r/w locks for this task
     * @param task
     * @param count 1 for incrementing count by 1, -1 for decreasing by 1
     */
    private void updatePartitionLocks(TransactionTask task, int count) {
        assert(count == 1 || count == -1);
        List<Long> hsids = task.getPartitionMasterHsids();
        if (hsids != null) {    // avoid the MPIEndOfLogTask case
            for (Long hsid : hsids) {
                PartitionLock pLock = m_lockedPartitions.get(hsid);
                assert(pLock != null);
                if (task.getTransactionState().isReadOnly())
                    pLock.updateRead(count);
                else
                    pLock.updateWrite(count);
            }
        }
    }

    /*
     * If the partitions can be used, return true, otherwise false
     */
    private boolean checkPartitions(TransactionTask task) {
        if (task.getTransactionState().isReadOnly()) {
            for (Long hsid : task.getPartitionMasterHsids()) {
                PartitionLock pLock = m_lockedPartitions.get(hsid);
                if (pLock.writes > 0) { return false; }
            }
        } else {
            for (Long hsid : task.getPartitionMasterHsids()) {
                PartitionLock pLock = m_lockedPartitions.get(hsid);
                if (pLock.reads > 0 || pLock.writes > 0) { return false; }
            }
        }

        return true;
    }

    public synchronized void updatePartitions(List<Long> masters, Map<Integer, Long> masterHsids) {
        // TODO: need to be carefuly considered during replica update
        for (Long i : masters) {
            System.err.println(i);
        }
        System.err.println("=======");
        for (Integer i : masterHsids.keySet()) {
            System.err.println(i + " -> " + String.format("0x%12X", masterHsids.get(i)));
        }

        // Update the locked partitions
        for (Long i : masters) {
            m_lockedPartitions.putIfAbsent(i, new PartitionLock()); // for now this is enough
        }
    }
}
