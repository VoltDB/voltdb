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

package org.voltdb;

import java.util.Map;

import org.voltcore.utils.InstanceId;
import org.voltdb.dtxn.TransactionCreator;

public interface CommandLogReinitiator {
    public interface Callback {
        public void onReplayCompletion();
    }

    /**
     * Set the snapshot transaction ID that got restored
     * @param txnId
     */
    public void setSnapshotTxnId(RestoreAgent.SnapshotInfo info);

    public void setCallback(Callback callback);

    public void setInitiator(TransactionCreator initiator);

    public void initPartitionTracking();

    /**
     * Generate the local replay plan. Call this before starting replay.
     * @param newPartitionCount
     * @param true if this node contains the MPI
     */
    public void generateReplayPlan(int newPartitionCount, boolean isMPINode);

    /**
     * Start replaying the log. Two threads will be started, one for reading the
     * log and transforming them into task messages, the other one for reading
     * them off the queue and reinitiating them.
     *
     * Note: the replay plan has to be generated prior to calling this method.
     * Call {@link #generateReplayPlan(int)} to generate the replay plan.
     */
    public void replay();

    /**
     * Whether or not there were log segments replayed in the cluster. This will
     * return true even if there were segments replayed by other nodes. It
     * doesn't necessarily mean that there were SPIs replayed. The segments
     * could be empty.
     *
     * @return true if there were at least one segment replayed
     */
    public boolean hasReplayedSegments();

    /**
     * Whether or not there were SPIs replayed in the cluster. Call this after
     * the replay. Call it before replay will not give you the correct result.
     *
     * @return true if the logs are empty.
     */
    public boolean hasReplayedTxns();

    /**
     * Get the maximum transaction ID among the last seen transactions across
     * all initiators in the previous segment.
     *
     * @return null if the log is empty
     */
    public Long getMaxLastSeenTxn();

    /**
     * IV2 ONLY:
     * Get the map of the max TXN ID seen for each partition in the command l0g
     */
    public Map<Integer, Long> getMaxLastSeenTxnByPartition();

    /**
     * Get the cluster InstanceId of the cluster that wrote the command log.
     */
    public InstanceId getInstanceId();

    /**
     * Returns all command log segments to the pool and closes the reader. This
     * discards the command log.
     */
    public void returnAllSegments();

    /**
     * Call @BalancePartitions until the partitions are balanced if necessary. Does nothing if the partitions are
     * already balanced or if the legacy hashinator is used.
     * @return true if the partitions are balanced successfully.
     */
    public boolean checkAndBalancePartitions();
}
