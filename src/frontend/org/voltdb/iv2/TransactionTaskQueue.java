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
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;

public class TransactionTaskQueue
{
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    final private SiteTaskerQueue m_taskQueue;

    /*
     * Multi-part transactions create a backlog of tasks behind them. A queue is
     * created for each multi-part task to maintain the backlog until the next
     * multi-part task.
     *
     * DR uses m_drMPSentinelBacklog to queue additional sentinels than the one
     * currently in progress because DR uses GENERIC_MP_SENTINEL value for all
     * sentinels. When a DR multipart completes, another sentinel will be polled
     * from m_drMPSentinelBacklog and put in this map. It is impossible to have
     * an empty m_backlog while m_drMPSentinelBacklog has more
     * sentinels queued.
     */
    Deque<TransactionTask> m_backlog = new ArrayDeque<TransactionTask>();

    TransactionTaskQueue(SiteTaskerQueue queue)
    {
        m_taskQueue = queue;
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
        if (!m_backlog.isEmpty()) {
            /*
             * This branch happens during regular execution when a multi-part is in progress.
             * The first task for the multi-part is the head of the queue, and all the single parts
             * are being queued behind it. The txnid check catches tasks that are part of the multi-part
             * and immediately queues them for execution.
             */
            if (task.getTxnId() != m_backlog.getFirst().getTxnId())
            {
                m_backlog.addLast(task);
                retval = true;
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
                m_backlog.addLast(task);
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
    synchronized void repair(SiteTasker task, List<Long> masters)
    {
        m_taskQueue.offer(task);
        Iterator<TransactionTask> iter = m_backlog.iterator();
        if (iter.hasNext()) {
            TransactionTask next = iter.next();
            // get head
            // Only the MPI's TransactionTaskQueue is ever called in this way, so we know
            // that the TransactionTasks we pull out of it have to be MP transactions, so this
            // cast is safe
            MpTransactionState txn = (MpTransactionState)next.getTransactionState();
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
            // Now, iterate through the rest of the data structure and update the partition masters
            // for all MpProcedureTasks not at the head of the TransactionTaskQueue
            while (iter.hasNext())
            {
                next = iter.next();
                ((MpProcedureTask)next).updateMasters(masters);
            }
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
     * @return the number of TransactionTasks queued to the SiteTaskerQueue
     */
    synchronized int flush()
    {
        int offered = 0;
        // If the first entry of the backlog is a completed transaction, clear it so it no longer
        // blocks the backlog then iterate the backlog for more work.
        //
        // Note the kooky corner case where a multi-part transaction can actually have multiple outstanding
        // tasks. At first glance you would think that because the relationship is request response there
        // can be only one outstanding task for a given multi-part transaction.
        //
        // That isn't true.
        //
        // A rollback can cause there to be a fragment task as well as a rollback
        // task. The rollback is generated asynchronously by another partition.
        // If we don't flush all the associated tasks now then flush won't be called again because it is waiting
        // for the complete transaction task that is languishing in the queue to do the flush post multi-part.
        // It can't be called eagerly because that would destructively flush single parts as well.
        if (m_backlog.isEmpty() || !m_backlog.getFirst().getTransactionState().isDone()) {
            return offered;
        }
        m_backlog.removeFirst();
        Iterator<TransactionTask> iter = m_backlog.iterator();
        while (iter.hasNext()) {
            TransactionTask task = iter.next();
            long lastQueuedTxnId = task.getTxnId();
            taskQueueOffer(task);
            ++offered;
            if (task.getTransactionState().isSinglePartition()) {
                // single part can be immediately removed and offered
                iter.remove();
                continue;
            }
            else {
                // leave the mp fragment at the head of the backlog but
                // iterate and take care of the kooky case explained above.
                while (iter.hasNext()) {
                    task = iter.next();
                    if (task.getTxnId() == lastQueuedTxnId) {
                        iter.remove();
                        taskQueueOffer(task);
                        ++offered;
                    }
                }
                break;
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
        return m_backlog.size();
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("TransactionTaskQueue:").append("\n");
        sb.append("\tSIZE: ").append(size());
        sb.append("\tHEAD: ").append(m_backlog.getFirst());
        return sb.toString();
    }
}
