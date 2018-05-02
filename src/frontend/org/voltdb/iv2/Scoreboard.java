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

import org.voltcore.utils.Pair;
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
    private Deque<Pair<CompleteTransactionTask, Boolean>> m_compTasks = new ArrayDeque<>(2);
    private FragmentTaskBase m_fragTask;

    public void addCompletedTransactionTask(CompleteTransactionTask task, Boolean missingTxn) {
        if (task.getTimestamp() == CompleteTransactionMessage.INITIAL_TIMESTAMP &&
                (m_compTasks.peekFirst() != null || missingTxn)) {
            // This is an extremely rare case were a MPI repair arrives before the dead MPI's completion
            // Ignore this message because the repair completion is more recent and should step on the initial completion
            if (!missingTxn) {
                assert(MpRestartSequenceGenerator.isForRestart(m_compTasks.peekFirst().getFirst().getTimestamp()));
            }
        }
        else
        if (task.getTimestamp() == CompleteTransactionMessage.INITIAL_TIMESTAMP ||
                (m_compTasks.peekFirst() != null &&
                !MpRestartSequenceGenerator.isForRestart(task.getTimestamp()) &&
                m_compTasks.peekFirst().getFirst().getMsgTxnId() == task.getMsgTxnId())) {
            // This is a submission of a completion. In case this is a resubmission of a completion that not
            // all sites received clear the whole queue. The Completion may or may not be for a transaction
            // that has already been completed (if it was completed missingTxn will be true)
            m_compTasks.clear();
            m_compTasks.addLast(Pair.of(task, missingTxn));
        }
        else {
            // This is an abort completion that will be followed with a resubmitted fragment,
            // so step on any fragment that is pending
            Pair<CompleteTransactionTask, Boolean> lastTaskPair = m_compTasks.peekLast();
            if (lastTaskPair != null && lastTaskPair.getFirst().getTimestamp() != CompleteTransactionMessage.INITIAL_TIMESTAMP &&
                    lastTaskPair.getFirst().getMsgTxnId() == task.getMsgTxnId()) {
                assert(lastTaskPair.getFirst().getMsgTxnId() == task.getMsgTxnId());
                m_compTasks.removeLast();
            }
            else {
                assert(m_compTasks.size() <= 1);
            }
            m_compTasks.addLast(Pair.of(task, missingTxn));
            m_fragTask = null;
        }
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

    public void addFragmentTask(FragmentTaskBase task) {
        m_fragTask = task;
    }

    public Deque<Pair<CompleteTransactionTask, Boolean>> getCompletionTasks() {
        return m_compTasks;
    }

    public FragmentTaskBase getFragmentTask() {
        return m_fragTask;
    }

    public void clearFragment() {
        m_fragTask = null;
    }

    //There could be two transactions in the queue.
    public boolean matchCompleteTransactionTask(long matchingCompletionTime) {
        if (m_compTasks.isEmpty()) {
            return false;
        }
        if (matchingCompletionTime == m_compTasks.peekFirst().getFirst().getTimestamp()) {
            return true;
        }

        if (m_compTasks.size() == 2) {
            return (matchingCompletionTime == m_compTasks.peekLast().getFirst().getTimestamp());
        }
        return false;
    }

    //Find the CompleteTransactionTask to be released. The task could be in header or tail
    //If the released one has the latest time stamp, then remove txn before the released one.
    public CompleteTransactionTask releaseCompleteTransactionTaskAndRemoveStaleTxn(long txnId) {
        Pair<CompleteTransactionTask, Boolean> header = m_compTasks.pollFirst();
        if (m_compTasks.isEmpty()) {
            return header.getFirst();
        }

        Pair<CompleteTransactionTask, Boolean> tail = m_compTasks.pollFirst();
        //match in the header
        if (header.getFirst().getMsgTxnId() == txnId) {
            if (txnId < tail.getFirst().getMsgTxnId()) {
                m_compTasks.addLast(tail);
            }
            return header.getFirst();
        } else { //match in the tail
            if (header.getFirst().getMsgTxnId() > txnId) {
                m_compTasks.addLast(header);
            }
            return tail.getFirst();
        }
    }
}
