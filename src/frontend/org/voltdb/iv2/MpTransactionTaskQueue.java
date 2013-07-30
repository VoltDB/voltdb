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

import java.util.Iterator;
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltdb.exceptions.TransactionRestartException;

import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;

public class MpTransactionTaskQueue extends TransactionTaskQueue
{
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    MpTransactionTaskQueue(SiteTaskerQueue queue)
    {
        super(queue);
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
        // At the MPI's TransactionTaskQueue, we know that there can only be
        // one transaction in the SiteTaskerQueue at a time, because the
        // TransactionTaskQueue will only release one MP transaction to the
        // SiteTaskerQueue at a time.  So, when we offer this repair task, we
        // know it will be the next thing to run once we poison the current
        // TXN.
        m_taskQueue.offer(task);
        Iterator<TransactionTask> iter = m_backlog.iterator();
        if (iter.hasNext()) {
            MpProcedureTask next = (MpProcedureTask)iter.next();
            next.doRestart(masters);
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
            // Now, iterate through the rest of the data structure and update the partition masters
            // for all MpProcedureTasks not at the head of the TransactionTaskQueue
            while (iter.hasNext())
            {
                next = (MpProcedureTask)iter.next();
                next.updateMasters(masters);
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
     * Restart the current task at the head of the queue.  This will be called
     * instead of flush by the currently blocking MP transaction in the event a
     * restart is necessary.
     */
    synchronized void restart()
    {
        taskQueueOffer(m_backlog.getFirst());
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
        sb.append("MpTransactionTaskQueue:").append("\n");
        sb.append("\tSIZE: ").append(size());
        if (!m_backlog.isEmpty()) {
            sb.append("\tHEAD: ").append(m_backlog.getFirst());
        }
        return sb.toString();
    }
}
