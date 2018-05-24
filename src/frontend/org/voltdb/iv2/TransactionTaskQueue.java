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
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.Pair;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionResponseMessage;

public class TransactionTaskQueue
{
    protected static final VoltLogger hostLog = new VoltLogger("HOST");
    protected static final VoltLogger tmLog = new VoltLogger("TM");

    final protected SiteTaskerQueue m_taskQueue;

    final private Scoreboard m_scoreboard;
    private boolean m_scoreboardEnabled;

    private static class RelativeSiteOffset {
        private SiteTaskerQueue[] m_stashedMpQueues;
        private Scoreboard[] m_stashedMpScoreboards;
        private TransactionTaskQueue[] m_txnTaskQueues;
        private int m_lowestSiteId = Integer.MIN_VALUE;
        private int m_siteCount = 0;
        private Mailbox[] m_mailBoxes;
        void resetScoreboards(int firstSiteId, int siteCount) {
            m_stashedMpQueues = null;
            m_stashedMpScoreboards = null;
            m_lowestSiteId = firstSiteId;
            m_siteCount = siteCount;
            m_txnTaskQueues = null;
        }

        void initializeScoreboard(int siteId, SiteTaskerQueue queue, Scoreboard scoreboard, Mailbox mailBox, TransactionTaskQueue taskQueue) {
            assert(m_lowestSiteId != Integer.MIN_VALUE);
            assert(siteId >= m_lowestSiteId && siteId-m_lowestSiteId < m_siteCount);
            if (m_stashedMpQueues == null) {
                m_stashedMpQueues = new SiteTaskerQueue[m_siteCount];
                m_stashedMpScoreboards = new Scoreboard[m_siteCount];
                m_mailBoxes = new Mailbox[m_siteCount];
                m_txnTaskQueues = new TransactionTaskQueue[m_siteCount];
            }
            m_stashedMpQueues[siteId-m_lowestSiteId] = queue;
            m_stashedMpScoreboards[siteId-m_lowestSiteId] = scoreboard;
            m_mailBoxes[siteId-m_lowestSiteId] = mailBox;
            m_txnTaskQueues[siteId-m_lowestSiteId] = taskQueue;
        }

        // All sites receives FragmentTask messages, time to fire the task.
        void releaseStashedFragments(long txnId) {
            if (hostLog.isDebugEnabled()) {
                hostLog.debug("release stashed fragment messages:" + TxnEgo.txnIdToString(txnId));
            }
            long lastTxnId = 0;
            for (int ii = m_siteCount-1; ii >= 0; ii--) {
                TransactionTask task = m_stashedMpScoreboards[ii].getFragmentTask();
                assert(lastTxnId == 0 || lastTxnId == task.getTxnId());
                lastTxnId = task.getTxnId();
                Iv2Trace.logSiteTaskerQueueOffer(task);
                m_stashedMpQueues[ii].offer(task);
                m_stashedMpScoreboards[ii].clearFragment();
            }

        }

        // All sites receives CompletedTransactionTask messages, time to fire the task.
        // If there are enough completeTransactionTask messages to fire another round of release, return false, otherwise true.
        boolean releaseStashedCompleteTxns(boolean missingTxn, long txnId)
        {
            boolean missingTask = missingTxn ? true : hasMissingTxn(txnId);
            if (hostLog.isDebugEnabled()) {
                if (missingTask) {
                    hostLog.debug("skipped incomplete rollback transaction message:" + TxnEgo.txnIdToString(txnId));
                } else {
                    hostLog.debug("release stashed complete transaction message:" + TxnEgo.txnIdToString(txnId));
                }
            }
            int tasksAtTail = 0;
            for (int ii = m_siteCount-1; ii >= 0; ii--) {
                // only release completions at head of queue
                CompleteTransactionTask completion = m_stashedMpScoreboards[ii].getCompletionTasks().pollFirst().getFirst();
                if (missingTask) {
                    //flush the backlog to avoid no task is pushed to site queue
                    if (completion.isAbortDuringRepair()) {
                        if (hostLog.isDebugEnabled()) {
                            hostLog.debug("releaseStashedComleteTxns: flush non-restartable logs at " + TxnEgo.txnIdToString(txnId));
                        }
                        // Mark the transaction state as DONE
                        // Transaction state could be null when a CompleteTransactionTask is added to scorecboard.
                        if (completion.m_txnState != null) {
                            completion.m_txnState.setDone();
                        }
                        // Flush us out of the head of the TransactionTaskQueue.
                        m_txnTaskQueues[ii].flush(txnId);
                    }
                    //Some sites may have processed CompleteTransactionResponseMessage, re-deliver this message to all sites and clear
                    //up the site outstanding transaction queue and duplicate counter
                    final CompleteTransactionResponseMessage resp = new CompleteTransactionResponseMessage(completion.getCompleteMessage());
                    resp.m_sourceHSId = m_mailBoxes[ii].getHSId();
                    m_mailBoxes[ii].deliver(resp);
                } else {
                    Iv2Trace.logSiteTaskerQueueOffer(completion);
                    m_stashedMpQueues[ii].offer(completion);
                }
                Pair<CompleteTransactionTask, Boolean> tail = m_stashedMpScoreboards[ii].getCompletionTasks().pollLast();
                if (tail != null) {
                    m_stashedMpScoreboards[ii].getCompletionTasks().addFirst(tail);
                    tasksAtTail++;
                }
            }
            return tasksAtTail != m_siteCount;
        }

        Scoreboard[] getScoreboards() {
            return m_stashedMpScoreboards;
        }

        int getSiteCount() {
            return m_siteCount;
        }

        // should only be used for debugging purpose
        private void dumpStashedMpWrites(StringBuilder builder) {
            for (int ii = 0; ii < m_siteCount; ii++) {
                builder.append("\nQueue " + m_stashedMpQueues[ii].getPartitionId() + ":" + m_stashedMpScoreboards[ii]);
            }
        }

        boolean hasMissingTxn(long txnId) {
            for (int ii = m_siteCount-1; ii >= 0; ii--) {
                if (m_stashedMpScoreboards[ii].isTransactionMissing(txnId)) {
                    return true;
                }
            }
            return false;
        }
    }

    /*
     * Multi-part transactions create a backlog of tasks behind them. A queue is
     * created for each multi-part task to maintain the backlog until the next
     * multi-part task.
     */
    private Deque<TransactionTask> m_backlog = new ArrayDeque<TransactionTask>();

    final private static RelativeSiteOffset s_stashedMpWrites = new RelativeSiteOffset();
    private static Object s_lock = new Object();
    private static CyclicBarrier s_barrier;

    TransactionTaskQueue(SiteTaskerQueue queue, boolean scoreboardEnabled)
    {
        m_taskQueue = queue;
        if (queue.getPartitionId() == MpInitiator.MP_INIT_PID) {
            m_scoreboard = null;
        }
        else {
            m_scoreboard = new Scoreboard();
        }
        m_scoreboardEnabled = scoreboardEnabled;
    }

    public static void initBarrier(int siteCount) {
        s_barrier = new CyclicBarrier(siteCount);
    }


    // We start joining nodes with scoreboard disabled
    // After all sites has been fully initilized and ready for snapshot, we should enable the scoreboard.
    boolean enableScoreboard() {
        assert (s_barrier != null);
        try {
            s_barrier.await(3L, TimeUnit.MINUTES);
        } catch (InterruptedException | BrokenBarrierException |TimeoutException e) {
            hostLog.error("Cannot re-enable the scoreboard.");
            s_barrier.reset();
            return false;
        }

        m_scoreboardEnabled = true;
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("Scoreboard has been enabled.");
        }
        return true;
    }

    public boolean scoreboardEnabled() {
        return m_scoreboardEnabled;
    }

    public static void resetScoreboards(int firstSiteId, int siteCount) {
        synchronized (s_lock) {
            s_stashedMpWrites.resetScoreboards(firstSiteId, siteCount);
        }
    }

    void initializeScoreboard(int siteId, Mailbox mailBox) {
        synchronized (s_lock) {
            if (m_taskQueue.getPartitionId() != MpInitiator.MP_INIT_PID) {
                s_stashedMpWrites.initializeScoreboard(siteId, m_taskQueue, m_scoreboard, mailBox, this);
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
             * and immediately queues them for execution. If any multi-part txn with smaller txnId shows up,
             * it must from repair process, just let it through.
             */
            if (txnState.isSinglePartition() || TxnEgo.getSequence(task.getTxnId()) > TxnEgo.getSequence(m_backlog.getFirst().getTxnId())) {
                m_backlog.addLast(task);
                retval = true;
            }
            /*
             * This branch coordinates FragmentTask or CompletedTransactionTask,
             * holds the tasks until all the sites on the node receive the task.
             * Task with newer spHandle will
             */
            else if (task.needCoordination() && m_scoreboardEnabled) {
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
            if (task.needCoordination() && m_scoreboardEnabled) {
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

    private void coordinatedTaskQueueOffer(TransactionTask task) {
        synchronized (s_lock) {
            long taskTimestamp = -1;
            long taskTxnId = -1;
            boolean isFragTask = false;
            boolean done = false;
            if (task instanceof CompleteTransactionTask) {
                taskTimestamp = ((CompleteTransactionTask)task).getTimestamp();
                taskTxnId = ((CompleteTransactionTask)task).getMsgTxnId();
                m_scoreboard.addCompletedTransactionTask((CompleteTransactionTask)task, false);

            } else if (task instanceof FragmentTaskBase) {
                FragmentTaskBase ft = (FragmentTaskBase)task;
                taskTimestamp = ft.getTimestamp();
                taskTxnId = ft.getTxnId();
                m_scoreboard.addFragmentTask(ft);
                isFragTask = true;
            }
            while (!done) {
                int fragmentScore = 0;
                int completionScore = 0;
                boolean missingTxn = false;
                for (Scoreboard sb : s_stashedMpWrites.getScoreboards()) {
                    if (isFragTask) {
                        if (!sb.matchFragmentTask(taskTxnId, taskTimestamp)) {
                            break;
                        }
                        fragmentScore++;
                    } else {
                        if (!sb.matchCompleteTransactionTask(taskTxnId, taskTimestamp)) {
                            break;
                        }
                        missingTxn |= sb.getCompletionTasks().peekFirst().getSecond();
                        // At repair time MPI may send many rounds of CompleteTxnMessage due to the fact that
                        // many SPI leaders are promoted, each round of CompleteTxnMessages share the same
                        // timestamp, so at TransactionTaskQueue level it only counts messages from the same round.
                        completionScore++;
                    }
                }

                if (hostLog.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder("MP Write Scoreboard Received " + task +
                            "\nFrags: " + fragmentScore + "/" + s_stashedMpWrites.getSiteCount() +
                            " Comps: " + completionScore + "/" + s_stashedMpWrites.getSiteCount() + ".\n");
                    s_stashedMpWrites.dumpStashedMpWrites(sb);
                    hostLog.debug(sb.toString());
                }
                if (completionScore == s_stashedMpWrites.getSiteCount()) {
                    done = s_stashedMpWrites.releaseStashedCompleteTxns(missingTxn, task.getTxnId());
                }
                else if (fragmentScore == s_stashedMpWrites.getSiteCount() && completionScore == 0) {
                    s_stashedMpWrites.releaseStashedFragments(task.getTxnId());
                    done = true;
                } else {
                    done = true;
                }
            }
        }
    }

    public void handleCompletionForMissingTxn(CompleteTransactionTask missingTxnCompletion) {
        if (!m_scoreboardEnabled) return;
        synchronized (s_lock) {
            long taskTxnId = missingTxnCompletion.getMsgTxnId();
            long taskTimestamp = missingTxnCompletion.getTimestamp();
            m_scoreboard.addCompletedTransactionTask(missingTxnCompletion, true);
            boolean done = false;

            while (!done) {
                int completionScore = 0;
                for (Scoreboard sb : s_stashedMpWrites.getScoreboards()) {
                    if (!sb.getCompletionTasks().isEmpty()) {
                        if (!sb.matchCompleteTransactionTask(taskTxnId, taskTimestamp)) {
                            break;
                        }                    // At repair time MPI may send many rounds of CompleteTxnMessage due to the fact that
                        // many SPI leaders are promoted, each round of CompleteTxnMessages share the same
                        // timestamp, so at TransactionTaskQueue level it only counts messages from the same round.
                        completionScore++;
                    }
                }

                if (hostLog.isDebugEnabled()) {
                    StringBuilder sb = new StringBuilder("MP Write Scoreboard Received unmatched " + missingTxnCompletion +
                            "\nComps: " + completionScore + "/" + s_stashedMpWrites.getSiteCount());
                    s_stashedMpWrites.dumpStashedMpWrites(sb);
                    hostLog.debug(sb.toString());
                }
                if (completionScore == s_stashedMpWrites.getSiteCount()) {
                    done = s_stashedMpWrites.releaseStashedCompleteTxns(true, missingTxnCompletion.getMsgTxnId());
                } else {
                    done = true;
                }
            }
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

        // Add a guard to protect the scenario that backlog been flushed multiple times for same txnId
        if (m_backlog.getFirst().getTxnId() != txnId) {
            return offered;
        }

        m_backlog.removeFirst();
        Iterator<TransactionTask> iter = m_backlog.iterator();
        while (iter.hasNext()) {
            TransactionTask task = iter.next();
            long lastQueuedTxnId = task.getTxnId();
            if (task.needCoordination() && m_scoreboardEnabled) {
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
                        if (task.needCoordination() && m_scoreboardEnabled) {
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
        if (task.needCoordination() && m_scoreboardEnabled) {
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
        sb.append("\n\tScoreboard:").append("\n");
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

    //flush mp readonly transactions out of backlog
    public synchronized void removeMPReadTransactions() {
        TransactionTask  task = m_backlog.peekFirst();
        while (task != null && task.getTransactionState().isReadOnly()) {
            task.getTransactionState().setDone();
            flush(task.getTxnId());
            task = m_backlog.peekFirst();
        }
    }
}
