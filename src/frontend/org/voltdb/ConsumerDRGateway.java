/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.concurrent.ExecutionException;

import org.voltdb.ProducerDRGateway.MeshMemberInfo;

// Interface through which the outside world can interact with the consumer side
// of DR. Currently, there's not much to do here, since the subsystem is
// largely self-contained
public interface ConsumerDRGateway extends Promotable {

    /**
     * Notify the consumer of catalog updates.
     * @param catalog             The new catalog.
     * @param newConnectionSource The new connection source if changed, or null if not.
     */
    void updateCatalog(CatalogContext catalog, String newConnectionSource);

    Map<Byte, DRRoleStats.State> getStates();

    void initialize(boolean resumeReplication, final byte dataSourceCluster, List<MeshMemberInfo> expectedClusterMembers);

    void shutdown(final boolean blocking) throws InterruptedException, ExecutionException;

    void restart() throws InterruptedException, ExecutionException;

    DRConsumerMpCoordinator getDRConsumerMpCoordinator();

    void clusterUnrecoverable(byte clusterId, Throwable t);

    void queueStartCursors(final MeshMemberInfo newMeshMember);

    void producerTopologyUpdated(final MeshMemberInfo existingCluster);

    void startConsumerDispatcher(final MeshMemberInfo member);

    void addLocallyLedPartition(int partitionId);
}
