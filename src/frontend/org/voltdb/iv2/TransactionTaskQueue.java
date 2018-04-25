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
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.Pair;
import org.voltdb.VoltDB;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;

public class TransactionTaskQueue
{
    protected static final VoltLogger hostLog = new VoltLogger("HOST");
    protected static final VoltLogger tmLog = new VoltLogger("TM");

    final protected SiteTaskerQueue m_taskQueue;

    final private int m_siteCount;
    final private ScoreboardTasks m_scoreboard;

    /*
     * Multi-part transactions create a backlog of tasks behind them. A queue is
     * created for each multi-part task to maintain the backlog until the next
     * multi-part task.
     */
    private Deque<TransactionTask> m_backlog = new ArrayDeque<TransactionTask>();

    final static ArrayList<Pair<SiteTaskerQueue, ScoreboardTasks>> s_stashedMpWrites = new ArrayList<>(0);
    static Object s_lock = new Object();

    // The purpose of having it is to synchronize all MP write messages (FragmentTask, CompleteTransactionTask)
    // across sites on the same node. Why? First reason is synchronized MP writes prevent
    // partial commit that we've observed in some rare cases. Second reason is prevent shared-replicated
    // table writes from blocking sites partially, e.g. some sites wait on countdown latch for all
    // sites within a node to receive a fragment, but due to node failure the failed remote leader may never forward the
    // fragment to the rest of sites on the given node, causes deadlock and can't be recovered from repair.
    private static class ScoreboardTasks {
        // We need to track 2 completions because during restart repairlog may resend a completion for
        // a transaction that has been processed as well as an abort for the actual transaction in progress.
        public Deque<Pair<CompleteTransactionTask, Boolean>> m_lastCompleteTxnTasks = new ArrayDeque<>(2);
        public TransactionTask m_lastFragTask;

        public void addCompletedTransactionTask(CompleteTransactionTask task, Boolean missingTxn) {
            if (task.getTimestamp() == CompleteTransactionMessage.INITIAL_TIMESTAMP &&
                    (m_lastCompleteTxnTasks.peekFirst() != null || missingTxn)) {
                // This is an extremely rare case were a MPI repair arrives before the dead MPI's completion
                // Ignore this message because the repair completion is more recent and should step on the initial completion
                if (!missingTxn) {
                    assert(MpRestartSequenceGenerator.isForRestart(m_lastCompleteTxnTasks.peekFirst().getFirst().getTimestamp()));
                }
            }
            else
            if (task.getTimestamp() == CompleteTransactionMessage.INITIAL_TIMESTAMP ||
                    (m_lastCompleteTxnTasks.peekFirst() != null &&
                    !MpRestartSequenceGenerator.isForRestart(task.getTimestamp()) &&
                    m_lastCompleteTxnTasks.peekFirst().getFirst().getMsgTxnId() == task.getMsgTxnId())) {
                // This is a submission of a completion. In case this is a resubmission of a completion that not
                // all sites received clear the whole queue. The Completion may or may not be for a transaction
                // that has already been completed (if it was completed missingTxn will be true)
                m_lastCompleteTxnTasks.clear();
                m_lastCompleteTxnTasks.addLast(Pair.of(task, missingTxn));
            }
            else {
                // This is an abort completion that will be followed with a resubmitted fragment,
                // so step on any fragment that is pending
                Pair<CompleteTransactionTask, Boolean> lastTaskPair = m_lastCompleteTxnTasks.peekLast();
                if (lastTaskPair != null && lastTaskPair.getFirst().getTimestamp() != CompleteTransactionMessage.INITIAL_TIMESTAMP) {
                    assert(lastTaskPair.getFirst().getMsgTxnId() == task.getMsgTxnId());
                    m_lastCompleteTxnTasks.removeLast();
                }
                else {
                    assert(m_lastCompleteTxnTasks.size() <= 1);
                }
                m_lastCompleteTxnTasks.addLast(Pair.of(task, missingTxn));
                m_lastFragTask = null;
            }
        }

        public void addFragmentTask(TransactionTask task) {
            m_lastFragTask = task;
        }

        public String toString() {
            StringBuilder builder = new StringBuilder();
            if (!m_lastCompleteTxnTasks.isEmpty()){
                builder.append("CompleteTransactionTasks: " + m_lastCompleteTxnTasks.peekFirst() +
                        (m_lastCompleteTxnTasks.size() == 2 ? "\n" + m_lastCompleteTxnTasks.peekLast() : ""));
            }
            if (m_lastFragTask != null) {
                builder.append("\nFragmentTask: " + m_lastFragTask);
            }
            return builder.toString();
        }
    }

    TransactionTaskQueue(SiteTaskerQueue queue)
    {
        m_taskQueue = queue;
        m_siteCount = VoltDB.instance().getCatalogContext().getNodeSettings().getLocalSitesCount();
        if (queue.getPartitionId() == MpInitiator.MP_INIT_PID) {
            m_scoreboard = null;
        }
        else {
            m_scoreboard = new ScoreboardTasks();
        }
    }

    public static void resetScoreboards() {
        synchronized (s_lock) {
            s_stashedMpWrites.clear();
        }
    }

    void initializeScoreboard(int siteId) {
        synchronized (s_lock) {
            if (m_taskQueue.getPartitionId() != MpInitiator.MP_INIT_PID) {
                if (s_stashedMpWrites.isEmpty()) {
                    s_stashedMpWrites.ensureCapacity(m_siteCount);
                }
                s_stashedMpWrites.add(siteId, Pair.of(m_taskQueue, m_scoreboard));
                assert(s_stashedMpWrites.size() <= m_siteCount);
            }
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
             * This branch coordinates FragmentTask or CompletedTransactionTask,
             * holds the tasks until all the sites on the node receive the task.
             * Task with newer spHandle will
             */
            else if (task.needCoordination()) {
                coordinatedTaskQueueOffer(task);
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
             * This branch coordinates FragmentTask or CompletedTransactionTask,
             * holds the tasks until all the sites on the node receive the task.
             * Task with newer spHandle will
             */
            if (task.needCoordination()) {
                coordinatedTaskQueueOffer(task);
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

    // All sites receives FragmentTask messages, time to fire the task.
    static private void releaseStashedFragments(long txnId)
    {
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("release stashed fragment messages:" + TxnEgo.txnIdToString(txnId));
        }
        long lastTxnId = 0;
        for (Pair<SiteTaskerQueue, ScoreboardTasks> p : s_stashedMpWrites) {
            TransactionTask task = p.getSecond().m_lastFragTask;
            assert(lastTxnId == 0 || lastTxnId == task.getTxnId());
            lastTxnId = task.getTxnId();
            Iv2Trace.logSiteTaskerQueueOffer(task);
            p.getFirst().offer(task);
            p.getSecond().m_lastFragTask = null;
        }
    }

    // All sites receives CompletedTransactionTask messages, time to fire the task.
    static private void releaseStashedComleteTxns(boolean missingTxn, long txnId)
    {
        if (hostLog.isDebugEnabled()) {
            if (missingTxn) {
                hostLog.debug("skipped incomplete rollback transaction message:" + TxnEgo.txnIdToString(txnId));
            }
            else {
                hostLog.debug("release stashed complete transaction message:" + TxnEgo.txnIdToString(txnId));
            }
        }
        long lastTxnId = 0;
        for (Pair<SiteTaskerQueue, ScoreboardTasks> p : s_stashedMpWrites) {
            CompleteTransactionTask completion = p.getSecond().m_lastCompleteTxnTasks.poll().getFirst();
            assert(lastTxnId == 0 || lastTxnId == completion.getMsgTxnId());
            lastTxnId = completion.getMsgTxnId();
            if (!missingTxn) {
                Iv2Trace.logSiteTaskerQueueOffer(completion);
                p.getFirst().offer(completion);
            }
        }
    }

    private void coordinatedTaskQueueOffer(TransactionTask task) {
        synchronized (s_lock) {
            long matchingCompletionTime = -1;
            if (task instanceof CompleteTransactionTask) {
                matchingCompletionTime = ((CompleteTransactionTask)task).getTimestamp();
                m_scoreboard.addCompletedTransactionTask((CompleteTransactionTask)task, false);

            } else if (task instanceof FragmentTask ||
                       task instanceof SysprocFragmentTask) {
                m_scoreboard.addFragmentTask(task);
            }

            int receivedFrags = 0;
            int receivedCompleteTxns = 0;
            boolean missingTxn = false;
            for (Pair<SiteTaskerQueue, ScoreboardTasks> p : s_stashedMpWrites) {
                if (!p.getSecond().m_lastCompleteTxnTasks.isEmpty()) {
                    if (matchingCompletionTime != p.getSecond().m_lastCompleteTxnTasks.peekFirst().getFirst().getTimestamp()) {
                        continue;
                    }
                    missingTxn |= p.getSecond().m_lastCompleteTxnTasks.peekFirst().getSecond();
                    // At repair time MPI may send many rounds of CompleteTxnMessage due to the fact that
                    // many SPI leaders are promoted, each round of CompleteTxnMessages share the same
                    // timestamp, so at TransactionTaskQueue level it only counts messages from the same round.
                    receivedCompleteTxns++;
                }
                if (p.getSecond().m_lastFragTask != null) {
                    receivedFrags++;
                }
            }

            if (hostLog.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder("MP Write Scoreboard Received " + task + "\nFrags: " + receivedFrags + "/" + m_siteCount +
                        " Comps: " + receivedCompleteTxns + "/" + m_siteCount + ".\n");
                dumpStashedMpWrites(sb);
                hostLog.debug(sb.toString());
            }
            if (receivedCompleteTxns == m_siteCount) {
                releaseStashedComleteTxns(missingTxn, task.getTxnId());
            }
            else
            if (receivedFrags == m_siteCount && receivedCompleteTxns == 0) {
                releaseStashedFragments(task.getTxnId());
            }
        }
    }

    public void handleCompletionForMissingTxn(CompleteTransactionTask missingTxnCompletion) {
        synchronized (s_lock) {
            long matchingCompletionTime = missingTxnCompletion.getTimestamp();
            m_scoreboard.addCompletedTransactionTask(missingTxnCompletion, true);
            int receivedCompleteTxns = 0;
            for (Pair<SiteTaskerQueue, ScoreboardTasks> p : s_stashedMpWrites) {
                if (!p.getSecond().m_lastCompleteTxnTasks.isEmpty()) {
                    if (matchingCompletionTime != p.getSecond().m_lastCompleteTxnTasks.peekFirst().getFirst().getTimestamp()) {
                        continue;
                    }
                    // At repair time MPI may send many rounds of CompleteTxnMessage due to the fact that
                    // many SPI leaders are promoted, each round of CompleteTxnMessages share the same
                    // timestamp, so at TransactionTaskQueue level it only counts messages from the same round.
                    receivedCompleteTxns++;
                }
            }

            if (hostLog.isDebugEnabled()) {
                StringBuilder sb = new StringBuilder("MP Write Scoreboard Received unmatched " + missingTxnCompletion +
                        "\nComps: " + receivedCompleteTxns + "/" + m_siteCount);
                dumpStashedMpWrites(sb);
                hostLog.debug(sb.toString());
            }
            if (receivedCompleteTxns == m_siteCount) {
                releaseStashedComleteTxns(true, missingTxnCompletion.getMsgTxnId());
            }
        }
    }

    // should only be used for debugging purpose
    private void dumpStashedMpWrites(StringBuilder builder) {
        for (Pair<SiteTaskerQueue, ScoreboardTasks> p : s_stashedMpWrites) {
            builder.append("\nQueue " + p.getFirst().getPartitionId() + ":" + p.getSecond());
        }
    }

    /**
     * Try to offer as many runnable Tasks to the SiteTaskerQueue as possible.
     * @param txnId The transaction ID of the TransactionTask which is completing and causing the flush
     * @return the number of TransactionTasks queued to the SiteTaskerQueue
     */
    synchronized int flush(long txnId)
    {
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("Flush backlog with txnId:" + TxnEgo.txnIdToString(txnId) +
                    ", backlog head txnId is:" + (m_backlog.isEmpty()? "empty" : TxnEgo.txnIdToString(m_backlog.getFirst().getTxnId()))
                    );
        }
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
            if (task.needCoordination()) {
                coordinatedTaskQueueOffer(task);
            } else {
                taskQueueOffer(task);
            }
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
                        if (task.needCoordination()) {
                            coordinatedTaskQueueOffer(task);
                        } else {
                            taskQueueOffer(task);
                        }
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
        TransactionTask task = m_backlog.getFirst();
        if (task.needCoordination()) {
            coordinatedTaskQueueOffer(task);
        } else {
            taskQueueOffer(task);
        }
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
        sb.append("\tScoreboard:").append("\n");
        synchronized (s_lock) {
            sb.append("\t").append(m_scoreboard.toString());
        }
        return sb.toString();
    }

    // Called from streaming snapshot execution
    public synchronized List<TransactionTask> getBacklogTasks() {
        List<TransactionTask> pendingTasks = new ArrayList<>();
        Iterator<TransactionTask> iter = m_backlog.iterator();
        // skip the first fragments which is streaming snapshot
        TransactionTask mpTask = iter.next();
        assert (!mpTask.getTransactionState().isSinglePartition());
        while (iter.hasNext()) {
            TransactionTask task = iter.next();
            // Skip all fragments of current transaction
            if (task.getTxnId() == mpTask.getTxnId()) {
                continue;
            }
            assert (task.getTransactionState().isSinglePartition());
            pendingTasks.add(task);
        }
        return pendingTasks;
    }
}
