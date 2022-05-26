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
import java.util.concurrent.ExecutionException;

import org.voltcore.utils.Pair;
import org.voltdb.ProducerDRGateway.MeshMemberInfo;

// Interface through which the outside world can interact with the consumer side
// of DR. Currently, there's not much to do here, since the subsystem is
// largely self-contained
public interface ConsumerDRGateway extends Promotable {

    /**
     * Notify the consumer of catalog updates.
     * @param catalog             The new catalog.
     * @param newConnectionSource The new connection source if changed, or null if not.
     * @param snapshotSource The cluster from which this joiner cluster should request snapshot.
     *        Use -1 if there is no preferred snapshot source. If this joiner cluster has already
     *        received snapshot, this change will have no effect.
     */
    void updateCatalog(CatalogContext catalog, String newConnectionSource, byte snapshotSource);

    void swapTables(final Set<Pair<String, Long>> swappedTables);

    Map<Byte, DRRoleStats.State> getStates();

    /**
     * If this cluster was a joiner (at some point) dataSourceCluster will not be -1. If this is the case
     * the trackers for this clusterId must exist. If they don't exist, this cluster did not finish loading
     * the snapshot as a joiner.
     * @param dataSourceCluster
     * @param expectedClusterMembers
     */
    void setInitialConversationMembership(byte dataSourceCluster, List<MeshMemberInfo> expectedClusterMembers);

    void initialize(StartAction startAction, boolean doActualRecover);

    void shutdown(final boolean isRestart, final boolean blocking) throws InterruptedException, ExecutionException;

    void restart(final boolean blocking) throws InterruptedException, ExecutionException;

    DRConsumerMpCoordinator getDRConsumerMpCoordinator();

    void clusterUnrecoverable(byte clusterId, Throwable t);

    void queueStartCursors(final MeshMemberInfo newMeshMember);

    void producerTopologyUpdated(final MeshMemberInfo existingCluster);

    void startConsumerDispatcher(final MeshMemberInfo member);

    void deactivateConsumerDispatcher(byte clusterId);

    void addLocallyLedPartition(int partitionId);

    boolean isSafeForReset(byte clusterId);

    void pauseConsumerDispatcher(byte clusterId);

    void resumeConsumerDispatcher(byte clusterId);

    void resetDrAppliedTracker(byte clusterId);

    void populateEmptyTrackersIfNeeded(byte producerClusterId, int producerPartitionCount);

    void dropLocal();

    boolean isSafeForDropLocal();

    void handleProducerClusterElasticChange(byte producerClusterId, int newProducerPartitionCount);

    /**
     * Log the current set of clusters that this consumer is receiving data from.
     */
    public void logActiveConversations();
}
