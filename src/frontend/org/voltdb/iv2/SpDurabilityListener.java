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

import java.util.ArrayList;

import org.voltdb.CommandLog;
import org.voltdb.CommandLog.DurabilityListener;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;

/**
 * This class is not thread-safe. Most of its usage is on the Site thread.
 */
public class SpDurabilityListener implements DurabilityListener {

    // No command logging
    class NoCompletionChecks implements CommandLog.CompletionChecks {
        NoCompletionChecks() {}

        public CommandLog.CompletionChecks startNewCheckList(int startSize) {
            return this;
        }

        @Override
        public void addTask(TransactionTask task) {}

        @Override
        public int getTaskListSize() {
            return 0;
        }

        @Override
        public void processChecks() {}
    };

    class AsyncCompletionChecks implements CommandLog.CompletionChecks {
        protected long m_lastSpUniqueId;
        protected long m_lastMpUniqueId;

        AsyncCompletionChecks(long lastSpUniqueId, long lastMpUniqueId) {
            m_lastSpUniqueId = lastSpUniqueId;
            m_lastMpUniqueId = lastMpUniqueId;
        }

        @Override
        public CommandLog.CompletionChecks startNewCheckList(int startSize) {
            return new AsyncCompletionChecks(m_lastSpUniqueId, m_lastMpUniqueId);
        }

        @Override
        public void addTask(TransactionTask task) {
            if (UniqueIdGenerator.getPartitionIdFromUniqueId(task.m_txnState.uniqueId) == MpInitiator.MP_INIT_PID) {
                m_lastMpUniqueId = task.m_txnState.uniqueId;
            }
            else {
                m_lastSpUniqueId = task.m_txnState.uniqueId;
            }
        }

        @Override
        public int getTaskListSize() {
            return 0;
        }

        @Override
        public void processChecks() {
            // Notify the SP UniqueId listeners
            for (DurableUniqueIdListener listener : m_uniqueIdListeners) {
                listener.lastUniqueIdsMadeDurable(m_lastSpUniqueId, m_lastMpUniqueId);
            }
        }
    };

    class SyncCompletionChecks extends AsyncCompletionChecks {
        ArrayList<TransactionTask> m_pendingTransactions;

        public SyncCompletionChecks(long lastSpUniqueId, long lastMpUniqueId, int startSize) {
            super(lastSpUniqueId, lastMpUniqueId);
            m_pendingTransactions = new ArrayList<TransactionTask>(startSize);
        }

        @Override
        public CommandLog.CompletionChecks startNewCheckList(int startSize) {
            return new SyncCompletionChecks(m_lastSpUniqueId, m_lastMpUniqueId, startSize);
        }

        @Override
        public void addTask(TransactionTask task) {
            m_pendingTransactions.add(task);
            super.addTask(task);
        }

        @Override
        public int getTaskListSize() {
            return m_pendingTransactions.size();
        }

        @Override
        public void processChecks() {
            // Notify all sync transactions and the SP UniqueId listeners
            for (TransactionTask o : m_pendingTransactions) {
                m_pendingTasks.offer(o);
                // Make sure all queued tasks for this MP txn are released
                if (!o.getTransactionState().isSinglePartition()) {
                    m_spScheduler.offerPendingMPTasks(o.getTxnId());
                }
            }
            super.processChecks();
        }
    }

    private CommandLog.CompletionChecks m_currentCompletionChecks = null;

    private final SpScheduler m_spScheduler;
    private final TransactionTaskQueue m_pendingTasks;
    private boolean m_commandLoggingEnabled;

    private final ArrayList<DurableUniqueIdListener> m_uniqueIdListeners = new ArrayList<DurableUniqueIdListener>(2);

    public SpDurabilityListener(SpScheduler spScheduler, TransactionTaskQueue pendingTasks) {
        m_spScheduler = spScheduler;
        m_pendingTasks = pendingTasks;
    }

    @Override
    public void setUniqueIdListener(DurableUniqueIdListener listener) {
        m_uniqueIdListeners.add(listener);
        if (m_currentCompletionChecks != null && !m_commandLoggingEnabled) {
            // Since command logging is disabled set the durable uniqueId to maxLong
            listener.lastUniqueIdsMadeDurable(Long.MAX_VALUE, Long.MAX_VALUE);
        }
    }

    @Override
    public void addTransaction(TransactionTask pendingTask) {
        m_currentCompletionChecks.addTask(pendingTask);
    }

    @Override
    public CommandLog.CompletionChecks startNewTaskList(int nextStartTaskListSize) {
        CommandLog.CompletionChecks lastChecks = m_currentCompletionChecks;
        m_currentCompletionChecks = m_currentCompletionChecks.startNewCheckList(nextStartTaskListSize);
        return lastChecks;
    }

    @Override
    public int getNumberOfTasks() {
        return m_currentCompletionChecks.getTaskListSize();
    }

    @Override
    public void createFirstCompletionCheck(boolean isSyncLogging, boolean commandLoggingEnabled) {
        m_commandLoggingEnabled = commandLoggingEnabled;
        if (!commandLoggingEnabled) {
            m_currentCompletionChecks = new NoCompletionChecks();
            // Since command logging is disabled set the durable uniqueId to maxLong
            for (DurableUniqueIdListener listener : m_uniqueIdListeners) {
                listener.lastUniqueIdsMadeDurable(Long.MAX_VALUE, Long.MAX_VALUE);
            }
        }
        else
        if (isSyncLogging) {
            m_currentCompletionChecks = new SyncCompletionChecks(Long.MIN_VALUE, Long.MIN_VALUE, 16);
        }
        else {
            m_currentCompletionChecks = new AsyncCompletionChecks(Long.MIN_VALUE, Long.MIN_VALUE);
        }
    }

    @Override
    public boolean completionCheckInitialized() {
        return (m_currentCompletionChecks != null);
    }

    @Override
    public void processDurabilityChecks(CommandLog.CompletionChecks completionChecks) {
        m_spScheduler.processDurabilityChecks(completionChecks);
    }
}
