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

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.iv2.InitiatorMailbox;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.sysprocs.saverestore.HiddenColumnFilter;

public interface SystemProcedureExecutionContext {
    public Database getDatabase();

    public Cluster getCluster();

    public ClusterSettings getClusterSettings();

    public NodeSettings getPaths();

    public long getSpHandleForSnapshotDigest();

    public long getSiteId();

    public int getLocalSitesCount();

    public int getLocalActiveSitesCount();

    // does this site have "lowest site id" responsibilities.
    public boolean isLowestSiteId();

    public void setLowestSiteId();

    public int getClusterId();

    public int getHostId();

    public int getPartitionId();

    public long getCatalogCRC();

    public int getCatalogVersion();

    public long getGenerationId();

    public byte[] getCatalogHash();

    public byte[] getDeploymentHash();

    // Separate SiteTracker accessor for IV2 use.
    // Snapshot services that need this can get a SiteTracker in IV2, but
    // all other calls to the regular getSiteTracker() are going to throw.
    public SiteTracker getSiteTrackerForSnapshot();

    public int getNumberOfPartitions();

    public void setNumberOfPartitions(int partitionCount);

    public SiteProcedureConnection getSiteProcedureConnection();

    public SiteSnapshotConnection getSiteSnapshotConnection();

    public void updateBackendLogLevels();

    public boolean updateCatalog(String catalogDiffCommands, CatalogContext context,
            boolean requiresSnapshotIsolation, long txnId, long uniqueId, long spHandle, boolean isReplay,
            boolean requireCatalogDiffCmdsApplyToEE, boolean requiresNewExportGeneration,
            Map<Byte, String[]> replicableTables);

    public boolean updateSettings(CatalogContext context);

    public TheHashinator getCurrentHashinator();

    public Procedure ensureDefaultProcLoaded(String procName);

    /**
     * Update the EE hashinator with the given configuration.
     */
    public void updateHashinator(TheHashinator hashinator);

    boolean activateTableStream(int tableId, TableStreamType type, HiddenColumnFilter hiddenColumnFilter, boolean undo,
            byte[] predicates);

    public void forceAllDRNodeBuffersToDisk(final boolean nofsync);

    public DRIdempotencyResult isExpectedApplyBinaryLog(int producerClusterId, int producerPartitionId,
                                                        long logId);

    public void appendApplyBinaryLogTxns(int producerClusterId, int producerPartitionId,
                                         long localUniqueId, DRConsumerDrIdTracker tracker);

    public void recoverDrState(int clusterId, Map<Integer, Map<Integer, DRSiteDrIdTracker>> trackers,
            Map<Byte, String[]> replicableTables);

    public void resetAllDrAppliedTracker();

    public void resetDrAppliedTracker(int clusterId);

    public boolean hasRealDrAppliedTracker(byte clusterId);

    public void initDRAppliedTracker(Map<Byte, Integer> clusterIdToPartitionCountMap);

    public Map<Integer, Map<Integer, DRSiteDrIdTracker>> getDrAppliedTrackers();

    public Pair<Long, Long> getDrLastAppliedUniqueIds();

    Pair<Long, int[]> tableStreamSerializeMore(int tableId, TableStreamType type,
                                               List<DBBPool.BBContainer> outputBuffers);

    public InitiatorMailbox getInitiatorMailbox();

    void decommissionSite(boolean remove, boolean promote, int newSitePerHost);

    TopicsSystemTableConnection getTopicsSystemTableConnection();
}
