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

import java.util.Map;
import java.util.TreeMap;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.iv2.TransactionTaskQueue.CompletionCounter;
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
    private TreeMap<Long, Pair<CompleteTransactionTask, Boolean>> m_compTasks = new TreeMap<>();
    private FragmentTaskBase m_fragTask;
    protected static final VoltLogger tmLog = new VoltLogger("TM");

    public void addCompletedTransactionTask(CompleteTransactionTask task, Boolean missingTxn) {
        // This is an extremely rare case were a MPI restart completion arrives before the dead MPI's completion
        // Ignore this message because the restart completion is more recent and should step on the initial completion
        if (task.getTimestamp() == CompleteTransactionMessage.INITIAL_TIMESTAMP &&
                (hasRestartCompletion(task) || missingTxn)) {
            return;
        }

        // Restart completion steps on any pending prior fragment of the same transaction
        if (task.getTimestamp() != CompleteTransactionMessage.INITIAL_TIMESTAMP &&
                m_fragTask != null && m_fragTask.getTxnId() == task.getMsgTxnId()) {
            m_fragTask = null;
        }

        Pair<CompleteTransactionTask, Boolean> pair = m_compTasks.get(task.getMsgTxnId());
        if ( pair == null) {
            m_compTasks.put(task.getMsgTxnId(), Pair.of(task, missingTxn));
        } else {
            if ( task.getTimestamp() > pair.getFirst().getTimestamp() && isComparable(pair.getFirst(), task)) {
                m_compTasks.put(task.getMsgTxnId(), Pair.of(task, missingTxn));
            }
        }
    }

    public void addFragmentTask(FragmentTaskBase task) {
        m_fragTask = task;
    }

    /**
     * Remove the CompleteTransactionTask from the head and count the next CompleteTransactionTask
     * @param nextTaskCounter CompletionCounter
     * @return the removed CompleteTransactionTask
     */
    public Pair<CompleteTransactionTask, Boolean> pollFirstCompletionTask(CompletionCounter nextTaskCounter) {

        // remove from the head
        Pair<CompleteTransactionTask, Boolean> pair = m_compTasks.pollFirstEntry().getValue();
        if (m_compTasks.isEmpty()) {
            return pair;
        }
        // check next task for completion to ensure that the heads on all the site
        // have the same transaction and timestamp
        Pair<CompleteTransactionTask, Boolean> next = peekFirst();
        if (nextTaskCounter.txnId == 0L) {
            nextTaskCounter.txnId = next.getFirst().getMsgTxnId();
            nextTaskCounter.completionCount++;
            nextTaskCounter.timestamp = next.getFirst().getTimestamp();
            nextTaskCounter.missingTxn |= next.getSecond() || next.getFirst().m_txnState.isDone();
        } else if (nextTaskCounter.txnId == next.getFirst().getMsgTxnId() &&
                nextTaskCounter.timestamp == next.getFirst().getTimestamp()) {
            nextTaskCounter.missingTxn |= next.getSecond();
            nextTaskCounter.completionCount++;
        }
        return pair;
    }

    public Pair<CompleteTransactionTask, Boolean> peekFirst() {
        if (!m_compTasks.isEmpty()) {
            return m_compTasks.firstEntry().getValue();
        }
        return null;
    }

    public Pair<CompleteTransactionTask, Boolean> peekLast() {
        if (!m_compTasks.isEmpty()) {
            return m_compTasks.lastEntry().getValue();
        }
        return null;
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
        Pair<CompleteTransactionTask, Boolean> pair = peekFirst();
        if (pair != null) {
            return (MpRestartSequenceGenerator.isForRestart(pair.getFirst().getTimestamp()) &&
                    task.getMsgTxnId() < pair.getFirst().getMsgTxnId());
        }

        return false;
    }

    public boolean matchFragmentTask(long txnId, long timestamp) {
        return m_fragTask != null && m_fragTask.getTxnId() == txnId && m_fragTask.getTimestamp() == timestamp;
    }

    // Only match CompleteTransactionTask at head of the queue
    public boolean matchCompleteTransactionTask(long txnId, long timestamp) {
        Map.Entry<Long, Pair<CompleteTransactionTask, Boolean>> entry = m_compTasks.firstEntry();
        return entry != null && entry.getKey() == txnId && entry.getValue().getFirst().getTimestamp() == timestamp;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        if (!m_compTasks.isEmpty()){
            builder.append("CompleteTransactionTasks: ");
            for (Pair<CompleteTransactionTask, Boolean> pair : m_compTasks.values()) {
                builder.append("\n" + pair);
            }
        }
        if (m_fragTask != null) {
            builder.append("\nFragmentTask: " + m_fragTask);
        }
        return builder.toString();
    }
}
