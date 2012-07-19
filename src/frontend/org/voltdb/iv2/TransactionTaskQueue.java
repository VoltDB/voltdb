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

import org.voltcore.logging.VoltLogger;

import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;

public class TransactionTaskQueue
{
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    final private Deque<TransactionTask> m_backlog =
        new ArrayDeque<TransactionTask>();
    final private SiteTaskerQueue m_taskQueue;

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
        // Single partitions never queue if empty
        // Multipartitions always queue
        // Fragments queue if they're not part of the queue head TXN ID
        // offer to SiteTaskerQueue if:
        // the queue was empty
        // the queue wasn't empty but the txn IDs matched
        if (!m_backlog.isEmpty()) {
            if (task.getMpTxnId() != m_backlog.getFirst().getMpTxnId())
            {
                m_backlog.addLast(task);
                retval = true;
            }
            else {
                taskQueueOffer(task);
            }
        }
        else {
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
    synchronized void repair(SiteTasker task)
    {
        m_taskQueue.offer(task);
        if (!m_backlog.isEmpty()) {
            // get head
            MpTransactionState txn = (MpTransactionState)m_backlog.getFirst().getTransactionState();
            // inject poison pill
            FragmentTaskMessage dummy = new FragmentTaskMessage(0L, 0L, 0L, false, false);
            FragmentResponseMessage poison =
                new FragmentResponseMessage(dummy, 0L); // Don't care about source HSID here
            poison.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, null);
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
        if (!m_backlog.isEmpty()) {
            if (m_backlog.getFirst().getTransactionState().isDone()) {
                // remove the completed MP txn
                m_backlog.removeFirst();
                while (!m_backlog.isEmpty()) {
                    TransactionTask next = m_backlog.getFirst();
                    taskQueueOffer(next);
                    ++offered;
                    if (next.getTransactionState().isSinglePartition()) {
                        m_backlog.removeFirst();
                    }
                    else {
                        // Walk the queue and find any other task with the same
                        // txn ID as the MP transaction we just offered and also
                        // offer them.
                        Iterator<TransactionTask> iter = m_backlog.iterator();
                        // need to skip the head because that's the fragment we're looking
                        // to match; don't want to add it twice and pull it out of the backlog
                        iter.next();
                        while (iter.hasNext()) {
                            TransactionTask task = iter.next();
                            if (task.getMpTxnId() == next.getMpTxnId())
                            {
                                iter.remove();
                                taskQueueOffer(task);
                                ++offered;
                            }
                        }
                        break;
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
