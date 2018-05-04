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

import org.voltdb.messaging.CompleteTransactionMessage;

/*
 * The purpose of having it is to synchronize all MP write messages (FragmentTask, CompleteTransactionTask)
 * across sites on the same node. Why? First reason is synchronized MP writes prevent
 * partial commit that we've observed in some rare cases. Second reason is prevent shared-replicated
 * table writes from blocking sites partially, e.g. some sites wait on countdown latch for all
 * sites within a node to receive a fragment, but due to node failure the failed remote leader may never forward the
 * fragment to the rest of sites on the given node, causes deadlock and can't be recovered from repair.
 * We need to track 2 completions because during restart repairlog may resend a completion for
 * a transaction that has been processed as well as an abort for the actual transaction in progress.
 */
public class Scoreboard {
    private Deque<CompleteTransactionTask> m_compTasks = new ArrayDeque<>(2);
    private FragmentTaskBase m_fragTask;

    //return true if CompleteTransactionTask is stashed
    public boolean addCompletedTransactionTask(CompleteTransactionTask task, Boolean missingTxn) {
        // special case, scoreboard is empty
        if (m_compTasks.peekFirst() == null) {
            m_compTasks.addFirst(task);
            return true;
        }

        // This is an extremely rare case were a MPI restart completion arrives before the dead MPI's completion
        // Ignore this message because the restart completion is more recent and should step on the initial completion
        if (task.getTimestamp() == CompleteTransactionMessage.INITIAL_TIMESTAMP &&
                ( hasRestartCompletion(task) || missingTxn)) {
            return false;
        }

        // Restart completion steps on any pending prior fragment of the same transaction
        if (task.getTimestamp() != CompleteTransactionMessage.INITIAL_TIMESTAMP &&
                MpRestartSequenceGenerator.isForRestart(task.getTimestamp()) &&
                m_fragTask != null && m_fragTask.getTxnId() == task.getMsgTxnId()) {
            m_fragTask = null;
        }
        // scoreboard has one completion
        if (m_compTasks.size() == 1) {
            CompleteTransactionTask head = m_compTasks.peekFirst();
            if (head.getMsgTxnId() < task.getMsgTxnId()) {
                // Completion with higher txnId adds to tail
                m_compTasks.addLast(task);
            } else if (head.getMsgTxnId() > task.getMsgTxnId()) {
                // Completion with lower txnId goes to head
                m_compTasks.removeFirst();
                m_compTasks.addFirst(task);
                m_compTasks.addLast(head);
            } else {
                // Only keep the completion with latest timestamp if txnId is same
                if (head.getTimestamp() < task.getTimestamp() && isComparable(head, task)) {
                    m_compTasks.removeFirst();
                    m_compTasks.addFirst(task);
                } else {
                    // Ignore stale completion
                    return false;
                }
            }
        } else {
            // scoreboard has two completions
            CompleteTransactionTask head = m_compTasks.peekFirst();
            CompleteTransactionTask tail = m_compTasks.peekLast();
            // scorebaord can take completions from two transactions at most
            assert (task.getMsgTxnId() != head.getMsgTxnId() && task.getMsgTxnId() != tail.getMsgTxnId());

            // Keep newer completion, discard the older one
            if ( task.getTimestamp() > head.getTimestamp() && isComparable(head, task)) {
                m_compTasks.removeFirst();
                m_compTasks.addFirst(task);
            } else if ( task.getTimestamp() > tail.getTimestamp() && isComparable(tail, task)) {
                m_compTasks.removeLast();
                m_compTasks.addLast(task);
            } else {
                // Ignore stale completion
                return false;
            }
        }
        return true;
    }

    public void addFragmentTask(FragmentTaskBase task) {
        m_fragTask = task;
    }

    public Deque<CompleteTransactionTask> getCompletionTasks() {
        return m_compTasks;
    }

    public FragmentTaskBase getFragmentTask() {
        return m_fragTask;
    }

    public void clearFragment() {
        m_fragTask = null;
    }

    // Overwrite criteria:
    //   1) from same transaction,
    //   2) repair completion can overwrite initial completion or older repair completion
    //   3) restart completion can overwrite initial completion or older restart completion
    //   4) restart completion and repair completion can't overwrite each other
    private static boolean isComparable(CompleteTransactionTask c1, CompleteTransactionTask c2) {
        return c1.getMsgTxnId() == c2.getMsgTxnId() &&
                MpRestartSequenceGenerator.isForRestart(c1.getTimestamp()) ==
                MpRestartSequenceGenerator.isForRestart(c2.getTimestamp());
    }

    private boolean hasRestartCompletion(CompleteTransactionTask task) {
        if (m_compTasks.peekFirst() != null &&
                MpRestartSequenceGenerator.isForRestart(m_compTasks.peekFirst().getTimestamp()) &&
                task.getMsgTxnId() < m_compTasks.peekFirst().getMsgTxnId()) {
            return true;
        } else if (m_compTasks.size() == 2 &&
                MpRestartSequenceGenerator.isForRestart(m_compTasks.peekLast().getTimestamp()) &&
                task.getMsgTxnId() < m_compTasks.peekLast().getMsgTxnId()) {
            return true;
        }
        return false;
    }

    public boolean matchFragmentTask(long txnId, long timestamp) {
        return m_fragTask != null && m_fragTask.getTxnId() == txnId && m_fragTask.getTimestamp() == timestamp;
    }

    // Only match CompleteTransactionTask at head of the queue
    public boolean matchCompleteTransactionTask(long txnId, long timestamp) {
        return !m_compTasks.isEmpty() &&
                m_compTasks.peekFirst().getMsgTxnId() == txnId &&
                m_compTasks.peekFirst().getTimestamp() == timestamp;
    }

    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (!m_compTasks.isEmpty()){
            builder.append("CompleteTransactionTasks: " + m_compTasks.peekFirst() +
                    (m_compTasks.size() == 2 ? "\n" + m_compTasks.peekLast() : ""));
            builder.append("\n");
        }
        if (m_fragTask != null) {
            builder.append("FragmentTask: " + m_fragTask);
        }
        return builder.toString();
    }
}
