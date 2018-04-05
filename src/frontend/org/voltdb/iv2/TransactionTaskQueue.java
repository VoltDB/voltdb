/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.Iterator;

import org.cliffc_voltpatches.high_scale_lib.NonBlockingHashMap;
import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.dtxn.TransactionState;

public class TransactionTaskQueue
{
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    final protected SiteTaskerQueue m_taskQueue;

    // The purpose of having it is to synchronize all MP write messages (FragmentTask, CompleteTransactionTask)
    // across sites on the same node. Why? First reason is synchronized MP writes prevent
    // partial commit that we've observed in some rare cases. Second reason is prevent shared-replicated
    // table writes from blocking sites partially, e.g. some sites wait on countdown latch for all
    // sites within a node to receive a fragment, but due to node failure the failed remote leader may never forward the
    // fragment to the rest of sites on the given node, causes deadlock and can't be recovered from repair.
    static NonBlockingHashMap<SiteTaskerQueue, TransactionTask> stashedMpWrites = new NonBlockingHashMap<>();

    final private int m_siteCount;

    /*
     * Multi-part transactions create a backlog of tasks behind them. A queue is
     * created for each multi-part task to maintain the backlog until the next
     * multi-part task.
     */
    private Deque<TransactionTask> m_backlog = new ArrayDeque<TransactionTask>();

    TransactionTaskQueue(SiteTaskerQueue queue)
    {
        m_taskQueue = queue;
        m_siteCount = VoltDB.instance().getCatalogContext().getNodeSettings().getLocalSitesCount();
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
        TransactionState txnState = task.getTransactionState();
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
            /*
             * This branch is safe because
             */
            else if (task instanceof FragmentTask ||
                       task instanceof SysprocFragmentTask ||
                       task instanceof CompleteTransactionTask) {
                stashedMpWrites.put(m_taskQueue, task);
                if (stashedMpWrites.size() == m_siteCount) {
                    releaseStashedMpWrites();
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
            if (!txnState.isSinglePartition()) {
                m_backlog.addLast(task);
                retval = true;
            }
            /*
             * For MP writes, the last site reach here enqueues task for every body.
             */
            if (task instanceof FragmentTask ||
                task instanceof SysprocFragmentTask ||
                task instanceof CompleteTransactionTask) {
                stashedMpWrites.put(m_taskQueue, task);
                if (stashedMpWrites.size() == m_siteCount) {
                    releaseStashedMpWrites();
                }
            } else {
                taskQueueOffer(task);
            }
        }
        return retval;
    }

    // Add a local method to offer to the SiteTaskerQueue so we have
    // a single point we can log through.
    private void taskQueueOffer(TransactionTask task)
    {
        Iv2Trace.logSiteTaskerQueueOffer(task);
        m_taskQueue.offer(task);
    }

    // All sites receives Mp write messages, time to fire the task.
    static private void releaseStashedMpWrites()
    {
        stashedMpWrites.forEach((queue, task) -> {
            Iv2Trace.logSiteTaskerQueueOffer(task);
            queue.offer(task);
        });
        stashedMpWrites.clear();
    }

    /**
     * Try to offer as many runnable Tasks to the SiteTaskerQueue as possible.
     * @param txnId The transaction ID of the TransactionTask which is completing and causing the flush
     * @return the number of TransactionTasks queued to the SiteTaskerQueue
     */
    synchronized int flush(long txnId)
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
        sb.append("TransactionTaskQueue:").append("\n");
        sb.append("\tSIZE: ").append(size());
        if (!m_backlog.isEmpty()) {
            sb.append("\tHEAD: ").append(m_backlog.getFirst());
        }
        return sb.toString();
    }
}
