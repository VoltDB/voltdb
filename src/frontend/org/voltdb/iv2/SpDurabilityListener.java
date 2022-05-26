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

import java.util.ArrayList;

import org.voltcore.logging.VoltLogger;
import org.voltdb.CommandLog;
import org.voltdb.CommandLog.DurabilityListener;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTrace;

/**
 * This class is not thread-safe. Most of its usage is on the Site thread.
 */
public class SpDurabilityListener implements DurabilityListener {
    private static final VoltLogger log = new VoltLogger("LOGGING");

    // No command logging
    class NoCompletionChecks implements CommandLog.CompletionChecks {
        NoCompletionChecks() {}

        public CommandLog.CompletionChecks startNewCheckList(int startSize) {
            return this;
        }

        @Override
        public void addTask(TransactionTask task) {}

        @Override
        public void setLastDurableUniqueId(long uniqueId) {}

        @Override
        public boolean isChanged() {
            return false;
        }

        @Override
        public int getTaskListSize() {
            return 0;
        }

        @Override
        public void processChecks() {}
    }

    class AsyncCompletionChecks implements CommandLog.CompletionChecks {
        protected long m_lastSpUniqueId;
        protected long m_lastMpUniqueId;
        protected boolean m_changed = false;

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
            if (!task.m_txnState.isReadOnly()) {
                setLastDurableUniqueId(task.m_txnState.uniqueId);
            }
        }

        @Override
        public void setLastDurableUniqueId(long uniqueId) {
            if (UniqueIdGenerator.getPartitionIdFromUniqueId(uniqueId) == MpInitiator.MP_INIT_PID) {
                assert m_lastMpUniqueId <= uniqueId;
                m_lastMpUniqueId = uniqueId;
            }
            else {
                assert m_lastSpUniqueId <= uniqueId;
                m_lastSpUniqueId = uniqueId;
            }
            m_changed = true;
        }

        @Override
        public boolean isChanged() {
            return m_changed;
        }

        @Override
        public int getTaskListSize() {
            return 0;
        }

        @Override
        public void processChecks() {
            if (m_changed) {
                if (log.isTraceEnabled()) {
                    log.trace("Notifying of last made durable: SP " + UniqueIdGenerator.toShortString(m_lastSpUniqueId) +
                              ", MP " + UniqueIdGenerator.toShortString(m_lastMpUniqueId));
                }
                // Notify the SP UniqueId listeners
                for (DurableUniqueIdListener listener : m_uniqueIdListeners) {
                    listener.lastUniqueIdsMadeDurable(m_lastSpUniqueId, m_lastMpUniqueId);
                }
            }
        }
    }

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
        public boolean isChanged() {
            return !m_pendingTransactions.isEmpty();
        }

        @Override
        public int getTaskListSize() {
            return m_pendingTransactions.size();
        }

        private void queuePendingTasks() {
            // Notify all sync transactions and the SP UniqueId listeners
            for (TransactionTask o : m_pendingTransactions) {
                final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
                if (traceLog != null) {
                    traceLog.add(() -> VoltTrace.endAsync("durability",
                                                          MiscUtils.hsIdTxnIdToString(m_spScheduler.m_mailbox.getHSId(),
                                                                                      o.getSpHandle())));
                }

                m_pendingTasks.offer(o);
                // Make sure all queued tasks for this MP txn are released
                if (!o.getTransactionState().isSinglePartition()) {
                    m_spScheduler.offerPendingMPTasks(o.getTxnId());
                }
            }
        }

        @Override
        public void processChecks() {
            queuePendingTasks();
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
    public void configureUniqueIdListener(DurableUniqueIdListener listener, boolean install) {
        if (install) {
            m_uniqueIdListeners.add(listener);
            if (m_currentCompletionChecks != null && !m_commandLoggingEnabled) {
                // Since command logging is disabled set the durable uniqueId to maxLong
                listener.lastUniqueIdsMadeDurable(Long.MAX_VALUE, Long.MAX_VALUE);
            }
        }
        else {
            m_uniqueIdListeners.remove(listener);
        }
    }

    @Override
    public void addTransaction(TransactionTask pendingTask) {
        m_currentCompletionChecks.addTask(pendingTask);
    }

    @Override
    public void initializeLastDurableUniqueId(long uniqueId) {
        m_currentCompletionChecks.setLastDurableUniqueId(uniqueId);
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
        if (completionChecks.isChanged()) {
            m_spScheduler.processDurabilityChecks(completionChecks);
        }
    }

}
