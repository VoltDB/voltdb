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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.TreeMap;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;

public class TransactionTaskQueue
{
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    final private SiteTaskerQueue m_taskQueue;

    /*
     * Task for a multi-part transaction that can't be executed because this is a replay
     * transaction and the sentinel has not been received.
     */
    private TransactionTask m_multiPartPendingSentinelReceipt = null;

    /*
     * Multi-part transactions create a backlog of tasks behind them.
     * A queue is created for each multi-part task to maintain the backlog until the next multi-part
     * task.
     */
    TreeMap<Long, Deque<TransactionTask>> m_multipartBacklog = new TreeMap<Long, Deque<TransactionTask>>();

    TransactionTaskQueue(SiteTaskerQueue queue)
    {
        m_taskQueue = queue;
    }

    synchronized void offerMPSentinel(long txnId) {
        if (m_multiPartPendingSentinelReceipt != null) {
            /*
             * The fragment has arrived already, then this MUST be the correct
             * sentinel for this fragment.
             */
            if (txnId != m_multiPartPendingSentinelReceipt.getTxnId()) {
                VoltDB.crashLocalVoltDB("Mismatch between replay sentinel txnid " +
                        txnId + " and next mutli-part fragment id " +
                        m_multiPartPendingSentinelReceipt.getTxnId(), false, null);
            }
            /*
             * Queue this in the back, you know nothing precedes it
             * since the sentinel is part of the single part stream
             */
            TransactionTask ts = m_multiPartPendingSentinelReceipt;
            m_multiPartPendingSentinelReceipt = null;
            Deque<TransactionTask> deque = new ArrayDeque<TransactionTask>();
            deque.addLast(ts);
            m_multipartBacklog.put(txnId, deque);
            taskQueueOffer(ts);
        } else {
            /*
             * The sentinel has arrived, but not the fragment. Stash it away and wait for the fragment.
             * The presence of this handle pairing indicates that execution of the single part stream must
             * block until the multi-part is satisfied
             */
            m_multipartBacklog.put(txnId, new ArrayDeque<TransactionTask>());
        }
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
        boolean retval = false;

        final TransactionState ts = task.getTransactionState();

        // Single partitions never queue if empty
        // Multipartitions always queue
        // Fragments queue if they're not part of the queue head TXN ID
        // offer to SiteTaskerQueue if:
        // the queue was empty
        // the queue wasn't empty but the txn IDs matched
        if (ts.isForReplay() && (task instanceof FragmentTask)) {
            /*
             * If this is a multi-partition transaction for replay then it can't
             * be inserted into the order for this partition until it's position is known
             * via the sentinel value. That value may not be known at the is point.
             */
            if (!m_multipartBacklog.isEmpty() && ts.txnId == m_multipartBacklog.firstKey()) {
                /*
                 * This branch is for fragments that follow the first fragment during replay
                 * or first fragments during replay that follow the sentinenl (hence the key exists)
                 * It is executed immeidately either way, but it may need to be inserted into the backlog
                 * if it is the first fragment
                 */
                Deque<TransactionTask> backlog = m_multipartBacklog.firstEntry().getValue();
                TransactionTask first = backlog.peekFirst();
                if (first != null) {
                    if (first.m_txn.txnId != task.getTxnId()) {
                        if (!first.m_txn.isSinglePartition()) {
                            VoltDB.crashLocalVoltDB(
                                    "If the first backlog task is multi-part, " +
                                    "but has a different transaction id it is a bug", true, null);
                        }
                        backlog.addFirst(task);
                    }
                } else {
                    // The txnids match, don't need to put the second fragment at head of queue
                    //The first task is expected to be in the head of the queue
                    backlog.offer(task);
                }
                taskQueueOffer(task);
            }
            else {
                /*
                 * This is the situation where the first fragment arrived before the sentinel.
                 * Its position in the order is not known.
                 * m_multiPartPendingSentinelReceipt should be null because the MP coordinator should only
                 * run one transaction at a time.
                 * It is not time to block single parts from executing because the order is not know,
                 * the only thing to do is stash it away for when the order is known from the sentinel
                 */
                if (m_multiPartPendingSentinelReceipt != null) {
                    hostLog.fatal("\tBacklog length: " + m_multipartBacklog.size());
                    if (!m_multipartBacklog.isEmpty()) {
                        hostLog.fatal("\tBacklog first item: " + m_multipartBacklog.firstEntry().getValue().peekFirst());
                    }
                    hostLog.fatal("\tHave this one SentinelReceipt: " + m_multiPartPendingSentinelReceipt);
                    hostLog.fatal("\tAnd got this one, too: " + task);
                    VoltDB.crashLocalVoltDB(
                            "There should be only one multipart pending sentinel receipt at a time", true, null);
                }
                m_multiPartPendingSentinelReceipt = task;
                retval = true;
            }
        } else if (!m_multipartBacklog.isEmpty()) {
            /*
             * This branch happens during regular execution when a multi-part is in progress.
             * The first task for the multi-part is the head of the queue, and all the single parts
             * are being queued behind it. The txnid check catches tasks that are part of the multi-part
             * and immediately queues them for execution.
             */
            if (task.getTxnId() != m_multipartBacklog.firstKey())
            {
                if (!ts.isSinglePartition()) {
                    /*
                     * In this case it is a multi-part fragment for the next transaction
                     * make sure it goes into the queue for that transaction
                     */
                    Deque<TransactionTask> d = m_multipartBacklog.get(task.getTxnId());
                    if (d == null) {
                        d = new ArrayDeque<TransactionTask>();
                        m_multipartBacklog.put(task.getTxnId(), d);
                    }
                    d.offerLast(task);
                } else {
                    /*
                     * Note the use of last entry here. Each multi-part sentinel generates a backlog queue
                     * specific to the that multi-part. New single part transactions from the log go
                     * into the queue following the last received multi-part sentinel.
                     *
                     * It's possible to receive several sentinels with single part tasks mixed in
                     * before receiving the first fragment task from the MP coordinator for any of them
                     * so the backlog has to correctly preserve the order.
                     *
                     * During regular execution and not replay there should be at most one element in the
                     * multipart backlog, except for the kooky rollback corner case
                     */
                    m_multipartBacklog.lastEntry().getValue().addLast(task);
                    retval = true;
                }
            }
            else {
                taskQueueOffer(task);
            }
        }
        else {
            /*
             * Base case nothing queued nothing in progress
             * If the task is a multipart then put an entry in the backlog which
             * will act as a barrier for single parts, queuing them for execution after the
             * multipart
             */
            if (!task.getTransactionState().isSinglePartition()) {
                Deque<TransactionTask> d = new ArrayDeque<TransactionTask>();
                d.offer(task);
                m_multipartBacklog.put(task.getTxnId(), d);
                retval = true;
            }
            taskQueueOffer(task);
        }
        return retval;
    }

    // repair is used by MPI repair to inject a repair task into the
    // SiteTaskerQueue.  Before it does this, it unblocks the MP transaction
    // that may be running in the Site thread and causes it to rollback by
    // faking an unsuccessful FragmentResponseMessage.
    synchronized void repair(SiteTasker task)
    {
        m_taskQueue.offer(task);
        if (!m_multipartBacklog.isEmpty()) {
            // get head
            MpTransactionState txn =
                    (MpTransactionState)m_multipartBacklog.firstEntry().getValue().getFirst().getTransactionState();
            // inject poison pill
            FragmentTaskMessage dummy = new FragmentTaskMessage(0L, 0L, 0L, 0L, false, false, false);
            FragmentResponseMessage poison =
                new FragmentResponseMessage(dummy, 0L); // Don't care about source HSID here
            // Provide a serializable exception so that the procedure runner sees
            // this as an Expected (allowed) exception and doesn't take the crash-
            // cluster-path.
            SerializableException forcedTermination = new SerializableException(
                    "Transaction rolled back by fault recovery or shutdown.");
            poison.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, forcedTermination);
            txn.offerReceivedFragmentResponse(poison);
        }
    }

    // Add a local method to offer to the SiteTaskerQueue so we have
    // a single point we can log through.
    private void taskQueueOffer(TransactionTask task)
    {
        Iv2Trace.logSiteTaskerQueueOffer(task);
        m_taskQueue.offer(task);
    }

    /**
     * Try to offer as many runnable Tasks to the SiteTaskerQueue as possible.
     * Currently just blocks on the next uncompleted multipartition transaction
     * @return
     */
    synchronized int flush()
    {
        int offered = 0;
        // check to see if head is done
        // then offer until the next MP or FragTask
        if (!m_multipartBacklog.isEmpty()) {
            Deque<TransactionTask> backlog = m_multipartBacklog.firstEntry().getValue();
            if (backlog.peek().getTransactionState().isDone()) {
                // remove the completed MP txn
                backlog.removeFirst();
                m_multipartBacklog.remove(m_multipartBacklog.firstKey());

                /*
                 * Drain all the single parts in that backlog queue
                 */
                for (TransactionTask task : backlog) {
                    taskQueueOffer(task);
                    ++offered;
                }

                /*
                 * Now check to see if there was another multi-part queued after the one we just finished.
                 *
                 * This is a kooky corner case where a multi-part transaction can actually have multiple outstanding
                 * tasks. At first glance you would think that because the relationship is request response there
                 * can be only one outstanding task for a given multi-part transaction.
                 *
                 * That isn't true because a rollback can cause there to be a fragment task as well as a rollback
                 * task. The rollback is generated asynchronously by another partition.
                 * If we don't capture all the tasks right now then flush won't be called again because it is waiting
                 * for the complete transaction task that is languishing in the queue to do the flush post multi-part.
                 * It can't be called eagerly because that would destructively flush single parts as well.
                 *
                 * Iterate the queue to extract all tasks for the multi-part. The only time it isn't necessary
                 * to do this is when the first transaction in the queue is single part. This happens
                 * during replay when a sentinel creates the queue, but the multi-part task hasn't arrived yet.
                 */
                if (!m_multipartBacklog.isEmpty() &&
                        !m_multipartBacklog.firstEntry().getValue().isEmpty() &&
                        !m_multipartBacklog.firstEntry().getValue().getFirst().getTransactionState().isSinglePartition()) {
                    Deque<TransactionTask> nextBacklog = m_multipartBacklog.firstEntry().getValue();
                    long txnId = m_multipartBacklog.firstKey();
                    Iterator<TransactionTask> iter = nextBacklog.iterator();

                    TransactionTask task = null;
                    boolean firstTask = true;
                    while (iter.hasNext()) {
                        task = iter.next();
                        if (task.getTxnId() == txnId) {
                            /*
                             * The old code always left the first fragment task
                             * in the head of the queue. The new code can probably do without it
                             * since the map contains the txnid, but I will
                             * leave it in to minimize change.
                             */
                            if (firstTask) {
                                firstTask = false;
                            } else {
                                iter.remove();
                            }
                            taskQueueOffer(task);
                            ++offered;
                        }
                    }
                }
            }
        }
        return offered;
    }

    /**
     * How many Tasks are un-runnable?
     * @return
     */
    synchronized int size()
    {
        int size = 0;
        for (Deque<TransactionTask> d : m_multipartBacklog.values()) {
            size += d.size();
        }
        return size;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("TransactionTaskQueue:").append("\n");
        sb.append("\tSIZE: ").append(size());
        sb.append("\tHEAD: ").append(
                m_multipartBacklog.firstEntry() != null ? m_multipartBacklog.firstEntry().getValue().peekFirst() : null);
        return sb.toString();
    }
}
