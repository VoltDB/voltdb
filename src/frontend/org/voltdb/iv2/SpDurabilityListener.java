/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import org.voltdb.CommandLog.DurabilityListener;
import org.voltdb.iv2.SiteTasker.SiteTaskerRunnable;
import org.voltdb.iv2.SpScheduler.DurableMpUniqueIdListener;
import org.voltdb.iv2.SpScheduler.DurableSpUniqueIdListener;

class SpDurabilityListener implements DurabilityListener {

    // AsyncSpCompletionChecks
    class CompletionChecks {
        long m_lastSpUniqueId;

        CompletionChecks(long lastSpUniqueId) {
            m_lastSpUniqueId = lastSpUniqueId;
        }

        CompletionChecks startNewCheckList(int startSize) {
            return new CompletionChecks(m_lastSpUniqueId);
        }

        void addTask(TransactionTask task) {
            m_lastSpUniqueId = task.m_txnState.uniqueId;
        }

        int getTaskListSize() {
            return 0;
        }

        void processChecks() {
            for (DurableSpUniqueIdListener mpListener : m_spUniqueIdListeners) {
                mpListener.lastSpUniqueIdMadeDurable(m_lastSpUniqueId);
            }
        }
    };

    class AsyncMpCompletionChecks extends CompletionChecks {
        long m_lastMpUniqueId;

        AsyncMpCompletionChecks(long lastSpUniqueId, long lastMpUniqueId) {
            super(lastSpUniqueId);
            m_lastMpUniqueId = lastMpUniqueId;
        }

        @Override
        CompletionChecks startNewCheckList(int startSize) {
            return new AsyncMpCompletionChecks(m_lastSpUniqueId, m_lastMpUniqueId);
        }

        @Override
        void addTask(TransactionTask task) {
            m_lastSpUniqueId = task.m_txnState.uniqueId;
            if (!task.m_txnState.isSinglePartition()) {
                m_lastMpUniqueId = m_lastSpUniqueId;
            }
        }

        @Override
        void processChecks() {
            // Notify the SP UniqueId listeners
            super.processChecks();
            for (DurableMpUniqueIdListener mpListener : m_mpUniqueIdListeners) {
                mpListener.lastMpUniqueIdMadeDurable(m_lastMpUniqueId);
            }
        }
    };

    class SyncSpCompletionChecks extends CompletionChecks {
        ArrayList<TransactionTask> m_pendingTransactions;

        public SyncSpCompletionChecks(long lastSpUniqueId, int startSize) {
            super(lastSpUniqueId);
            m_pendingTransactions = new ArrayList<TransactionTask>(startSize);
        }

        @Override
        CompletionChecks startNewCheckList(int startSize) {
            return new SyncSpCompletionChecks(m_lastSpUniqueId, startSize);
        }

        @Override
        void addTask(TransactionTask task) {
            m_pendingTransactions.add(task);
            m_lastSpUniqueId = task.m_txnState.uniqueId;
        }

        @Override
        public int getTaskListSize() {
            return m_pendingTransactions.size();
        }

        @Override
        void processChecks() {
            for (TransactionTask o : m_pendingTransactions) {
                m_pendingTasks.offer(o);
                // Make sure all queued tasks for this MP txn are released
                if (!o.getTransactionState().isSinglePartition()) {
                    m_spScheduler.offerPendingMPTasks(o.getTxnId());
                }
            }
            // Notify the SP UniqueId listeners
            super.processChecks();
        }
    }

    class SyncMpCompletionChecks extends SyncSpCompletionChecks {
        long m_lastMpUniqueId;

        public SyncMpCompletionChecks(long lastSpUniqueId, long lastMpUniqueId, int startSize) {
            super(lastSpUniqueId, startSize);
            m_lastMpUniqueId = lastMpUniqueId;
        }

        @Override
        CompletionChecks startNewCheckList(int startSize) {
            return new SyncMpCompletionChecks(m_lastSpUniqueId, m_lastMpUniqueId, startSize);
        }

        @Override
        void addTask(TransactionTask task) {
            m_pendingTransactions.add(task);
            m_lastSpUniqueId = task.m_txnState.uniqueId;
            if (!task.m_txnState.isSinglePartition()) {
                m_lastMpUniqueId = m_lastSpUniqueId;
            }
        }

        @Override
        public int getTaskListSize() {
            return m_pendingTransactions.size();
        }

        @Override
        void processChecks() {
            // Notify all sync transactions and the SP UniqueId listeners
            super.processChecks();
            for (DurableMpUniqueIdListener mpListener : m_mpUniqueIdListeners) {
                mpListener.lastMpUniqueIdMadeDurable(m_lastMpUniqueId);
            }
        }
    }

    final ArrayDeque<CompletionChecks> m_writingTransactionLists =
            new ArrayDeque<CompletionChecks>(4);

    private CompletionChecks m_currentCompletionChecks = null;

    private final SpScheduler m_spScheduler;
    private final TransactionTaskQueue m_pendingTasks;
    private final SiteTaskerQueue m_tasks;
    private Object m_lock;

    private final ArrayList<DurableSpUniqueIdListener> m_spUniqueIdListeners = new ArrayList<DurableSpUniqueIdListener>(2);
    private final ArrayList<DurableMpUniqueIdListener> m_mpUniqueIdListeners = new ArrayList<DurableMpUniqueIdListener>(2);

    public SpDurabilityListener(SpScheduler spScheduler, TransactionTaskQueue pendingTasks, SiteTaskerQueue taskQueue) {
        m_spScheduler = spScheduler;
        m_pendingTasks = pendingTasks;
        m_tasks = taskQueue;
    }

    @Override
    public void setLock(Object o) {
        m_lock = o;
    }

    @Override
    public void setSpUniqueIdListener(DurableSpUniqueIdListener listener) {
        m_spUniqueIdListeners.add(listener);
    }

    @Override
    public void setMpUniqueIdListener(DurableMpUniqueIdListener listener) {
        m_mpUniqueIdListeners.add(listener);
    }

    @Override
    public void onDurability() {
        final CompletionChecks m_currentChecks = m_writingTransactionLists.poll();
        final SiteTaskerRunnable r = new SiteTasker.SiteTaskerRunnable() {
            @Override
            void run() {
                assert(m_currentChecks != null);
                synchronized (m_lock) {
                    m_currentChecks.processChecks();
                }
            }
        };
        if (InitiatorMailbox.SCHEDULE_IN_SITE_THREAD) {
            m_tasks.offer(r);
        } else {
            r.run();
        }
    }

    @Override
    public void addTransaction(TransactionTask pendingTask) {
        m_currentCompletionChecks.addTask(pendingTask);
    }

    @Override
    public void startNewTaskList(int nextStartTaskListSize) {
        m_writingTransactionLists.offer(m_currentCompletionChecks);
        m_currentCompletionChecks = m_currentCompletionChecks.startNewCheckList(nextStartTaskListSize);
    }

    @Override
    public int getNumberOfTasks() {
        return m_currentCompletionChecks.getTaskListSize();
    }

    @Override
    public void createFirstCompletionCheck(boolean isSyncLogging, boolean haveMpGateway) {
        if (isSyncLogging) {
            if (haveMpGateway) {
                m_currentCompletionChecks = new SyncMpCompletionChecks(Long.MIN_VALUE, Long.MIN_VALUE, 16);
            }
            else {
                m_currentCompletionChecks = new SyncSpCompletionChecks(Long.MIN_VALUE, 16);
            }
        }
        else {
            if (haveMpGateway) {
                m_currentCompletionChecks = new AsyncMpCompletionChecks(Long.MIN_VALUE, Long.MIN_VALUE);
            }
            else {
                m_currentCompletionChecks = new CompletionChecks(Long.MIN_VALUE);
            }
        }
    }

    @Override
    public boolean completionCheckInitialized(boolean supportsMp) {
        if (supportsMp) {
            return (m_currentCompletionChecks != null &&
                    (m_currentCompletionChecks instanceof AsyncMpCompletionChecks ||
                    m_currentCompletionChecks instanceof SyncMpCompletionChecks));
        }
        else {
            return (m_currentCompletionChecks != null);
        }
    }
}