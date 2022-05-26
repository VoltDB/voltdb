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
package org.voltdb;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.iv2.TransactionTask;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

public interface CommandLog {

    /**
     *
     * @param logSize log size for splitting in segments.
     * @param txnId The txnId of the truncation snapshot at the end of restore, or
     * @param coreBinding PosixJNAAffinity bindings.
     * @param perPartitionTxnId per partition transaction ids
     * @param partitionCount partition count
     */
    public abstract void init(int logSize,
                                 long txnId,
                                 String coreBinding,
                                 Map<Integer, Long> perPartitionTxnId);

    /**
     *
     * @param logSize log size for splitting in segments.
     * @param txnId The txnId of the truncation snapshot at the end of restore, or Long.MIN if there was none.
     * @param partitionCount Partition count
     * @param isRejoin Is Rejoin
     * @param coreBinding PosixJNAAffinity bindings.
     * @param perPartitionTxnId per partition transaction ids
     */
    public abstract void initForRejoin(int logSize,
                                          long txnId,
                                          boolean isRejoin,
                                          String coreBinding, Map<Integer, Long> perPartitionTxnId);

    public abstract boolean needsInitialization();

    /*
     *
     * The listener is will be provided with the handle once the message is durable.
     *
     * Returns a listenable future. If the returned future is null, then synchronous command logging
     * is in use and durability will be indicated via the durability listener. If the returned future
     * is not null then async command logging is in use. If the command log isn't falling behind the future
     * will already be completed, but if the command log is falling behind the future will be completed
     * when the log successfully writes out enough data to the file (although it won't call fsync since async)
     */
    public abstract ListenableFuture<Object> log(
            Iv2InitiateTaskMessage message,
            long spHandle,
            int[] involvedPartitions,
            DurabilityListener listener,
            TransactionTask durabilityHandle);

    public abstract void shutdown() throws InterruptedException;

    /**
     * IV2-only method. Write this Iv2FaultLogEntry to the fault log portion of the command log.
     * @return a settable future that is set true after the entry has been written to disk.
     */
    public abstract SettableFuture<Boolean> logIv2Fault(long writerHSId, Set<Long> survivorHSId,
            int partitionId, long spHandle);

    /**
     * IV2-only method. Write this Iv2FaultLogEntry to the fault log portion of the command log.
     * @return a settable future that is set true after the entry has been written to disk.
     */
    public abstract SettableFuture<Boolean> logIv2Fault(long writerHSId, Set<Long> survivorHSId,
            int partitionId, long spHandle, LogEntryType entryType);

    /**
     * Called on the very first message a rejoined SpScheduler receives to initialize the last durable value.
     * Thread it through here because the durability listener is owned by the command log thread.
     * @param uniqueId    The last durable unique ID passed from the master.
     */
    void initializeLastDurableUniqueId(DurabilityListener listener, long uniqueId);

    interface CompletionChecks {
        /**
         * Use the current CompletionChecks object to create a new CompletionChecks object
         * @param startSize - pre-allocated size of the next empty transaction list
         * @return the newly created CompletionChecks object
         */
        CompletionChecks startNewCheckList(int startSize);

        /**
         * Add a new transaction to the per-scheduler durable transaction tracker
         * @param task
         */
        void addTask(TransactionTask task);

        /**
         * Called on the very first message a rejoined SpScheduler receives to initialize the last durable value.
         * @param uniqueId    The last durable unique ID passed from the master.
         */
        void setLastDurableUniqueId(long uniqueId);

        /**
         * Returns <tt>true</tt> if this instance contains update of durable unique ID.
         *
         * @return <tt>true</tt> if this instance contains update of durable unique ID.
         */
        boolean isChanged();

        /**
         * Get the number of TransactionTasks tracked by this instance of CompletionChecks
         * @return
         */
        int getTaskListSize();

        /**
         * Perform all class-specific processing for this batch of transactions including
         * Durability Listener notifications
         */
        public void processChecks();
    }

    public interface DurabilityListener {
        /**
         * Assign or remove the listener that we will send SP and MP UniqueId durability notifications to
         */
        public void configureUniqueIdListener(DurableUniqueIdListener listener, boolean install);

        /**
         * Called from Scheduler to set up how all future completion checks will be handled
         */
        public void createFirstCompletionCheck(boolean isSyncLogging, boolean commandLoggingEnabled);

        /**
         * Determines if a completionCheck has already been allocated.
         */
        public boolean completionCheckInitialized();

        /**
         * Called from CommandLog to assign a new task to be tracked by the DurabilityListener
         */
        public void addTransaction(TransactionTask pendingTask);

        /**
         * Called on the very first message a rejoined SpScheduler receives to initialize the last durable value.
         * @param uniqueId    The last durable unique ID passed from the master.
         */
        void initializeLastDurableUniqueId(long uniqueId);

        /**
         * Used by CommandLog to calculate the next task list size
         */
        public int getNumberOfTasks();

        /**
         * Used by CommandLog to crate a new CompletionCheck so the last CompletionCheck can be
         * triggered when the sync completes
         */
        public CompletionChecks startNewTaskList(int nextMaxRowCnt);

        /**
         * Process checks on the correct scheduler thread
         * @param completionChecks
         */
        void processDurabilityChecks(CompletionChecks completionChecks);
    }

    /**
     * Is Command logging enabled?
     */
    public abstract boolean isEnabled();

    /**
     * Attempt to start a truncation snapshot
     * If a truncation snapshot is pending, passing false means don't start another one
     */
    public void requestTruncationSnapshot(final boolean queueIfPending);

    /**
     * Report when a truncation snapshot has started so CommandLogging does not submit
     * a new request until after the truncation work is complete
     */
    public default void notifyTruncationSnapshotStarted() {
        // Ignore snapshot start notification when Command Logging is disabled
    }

    /**
     * Statistics-related interface
     * Implementation should populate the stats based on column name to index mapping
     */
    public void populateCommandLogStats(int offset, Object[] rowValues);

    /**
     * Statistics-related interface, used by ActivityStats.
     * Implementaton should return outstanding byte count
     * in out[0], outstanding txn count in out[1].
     * If counts are not available, ok to do nothing.
     */
    public void getCommandLogOutstanding(long[] out);

    /**
     * Does this logger do synchronous logging
     */
    public abstract boolean isSynchronous();

    /**
     * Can the SpScheduler offer the task for execution.
     * @return true if it can, false if it has to wait until the task is made durable.
     */
    boolean canOfferTask();

    /**
     * Assign DurabilityListener from each SpScheduler to commmand log
     */
    public abstract void registerDurabilityListener(DurabilityListener durabilityListener);

    /**
     * @param partitions teh dcommissioned partitions on this host
     */
    public void notifyDecommissionPartitions(List<Integer> partitions);

    /**
     * @return return a list of decommissioned replicas on this host
     */
    public ImmutableSet<Integer> getDecommissionedPartitions();
}
