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

package org.voltdb.export;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltcore.zk.SynchronizedStatesManager;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.Promotable;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.StatsSelector;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.deploymentfile.FeatureType;
import org.voltdb.compiler.deploymentfile.FeaturesType;
import org.voltdb.export.ExportDataSource.StreamStartAction;

/**
 * @author rdykiel
 *
 * Generic Export Manager Interface, also exposes singleton ExportManager instance.
 */
public interface ExportManagerInterface extends Promotable {

    public static final String EXPORT_FEATURE = "export";

    public static enum ExportMode {
        BASIC("org.voltdb.export.ExportManager"),
        ADVANCED("org.voltdb.e3.E3ExportManager");

        private final String m_mgrClassName;

        private ExportMode(String mgrClassName) {
            m_mgrClassName = mgrClassName;
        }
    }

    static ExportMode getExportFeatureMode(FeaturesType features) throws SetupException {
        boolean isEnterprise = VoltDB.instance().getConfig().m_isEnterprise;
        String modeName = getExportFeatureConfigured(features);

        if (modeName == null) {
            return isEnterprise ? ExportMode.ADVANCED : ExportMode.BASIC;
        }

        for (ExportMode modeEnum : ExportMode.values()) {
            if (modeName.equalsIgnoreCase(modeEnum.name())) {
                if (!isEnterprise && modeEnum == ExportMode.ADVANCED) {
                    throw new SetupException("Cannot use ADVANCED export mode in community edition");
                }
                return modeEnum;
            }
        }
        throw new SetupException("Unknown export feature mode: " + modeName);
    }

    static String getExportFeatureConfigured(FeaturesType features) {
        if (features == null) {
            return null;
        }
        for (FeatureType feature : features.getFeature()) {
            if (feature.getName().equalsIgnoreCase(EXPORT_FEATURE)) {
                return feature.getOption();
            }
        }
        return null;
    }


    /**
     * Construct ExportManager using catalog.
     * @param myHostId
     * @param catalogContext
     * @throws ExportManager.SetupException
     * @throws ReflectiveOperationException
     */
    public static ExportManagerInterface initialize(
            FeaturesType deploymentFeatures,
            int myHostId,
            VoltDB.Configuration configuration,
            CatalogContext catalogContext,
            boolean isRejoin,
            boolean forceCreate,
            HostMessenger messenger,
            Map<Integer, Integer> partitions)
            throws ExportManagerInterface.SetupException, ReflectiveOperationException
    {
        ExportMode mode = getExportFeatureMode(deploymentFeatures);
        Class<?> exportMgrClass = Class.forName(mode.m_mgrClassName);
        Constructor<?> constructor = exportMgrClass.getConstructor(int.class, VoltDB.Configuration.class,
                CatalogContext.class, HostMessenger.class);
        ExportManagerInterface em = (ExportManagerInterface) constructor.newInstance(myHostId, configuration,
                catalogContext, messenger);
        VoltDB.setExportManagerInstance(em);
        if (forceCreate) {
            em.clearOverflowData();
        }
        em.initialize(catalogContext, partitions, isRejoin);

        VoltDB.instance().getStatsAgent().registerStatsSource(StatsSelector.EXPORT,
                myHostId, // m_siteId,
                em.getExportStats());
        return em;
    }

    /**
     * Thrown if the initial setup of the loader fails
     */
    public static class SetupException extends Exception {
        private static final long serialVersionUID = 1L;

        public SetupException(final String msg) {
            super(msg);
        }

        SetupException(final Throwable cause) {
            super(cause);
        }
    }

    public void clearOverflowData() throws ExportManagerInterface.SetupException;

    public int getConnCount();

    public Generation getGeneration();

    public ExportStats getExportStats();

    public int getExportTablesCount();

    public List<ExportStatsRow> getStats(final boolean interval);

    /**
     * Used for export activity checks by 'operator' code
     */
    default long getTotalPendingCount() {
        long total = 0;
        for (ExportStatsRow st : getStats(false)) {
            total += st.m_tuplesPending;
        }
        return total;
    }

    default int getMastershipCount() {
        int count = 0;
        for (ExportStatsRow st : getStats(false)) {
            if (Boolean.parseBoolean(st.m_exportingRole)) {
                count++;
            }
        }
        return count;
    }

    /**
     * Initialize on startup; any exceptions thrown from here will crash VoltDB
     */
    public void initialize(CatalogContext catalogContext, Map<Integer, Integer> localPartitionsToSites,
            boolean isRejoin);

    public void startListeners(ClientInterface cif);

    public void shutdown();

    /**
     * Start polling.
     *
     * Called exactly once from RealVoltDB after rejoin or replay completion,
     * after the PBDs have been safely truncated.
     */
    public void startPolling(CatalogContext catalogContext);

    public void updateCatalog(CatalogContext catalogContext, boolean requireCatalogDiffCmdsApplyToEE,
            boolean requiresNewExportGeneration, Map<Integer, Integer> localPartitionsToSites);

    public void updateInitialExportStateToSeqNo(int partitionId, String streamName,
            StreamStartAction action,
            Map<Integer, ExportSnapshotTuple> sequenceNumberPerPartition);

    public void updateDanglingExportStates(StreamStartAction action,
            Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers);

    public void processExportControl(String exportSource, List<String> exportTargets, StreamControlOperation operation,
            VoltTable results);

    default public void processTopicControl(String topic, StreamControlOperation operation, VoltTable results) {
        throw new UnsupportedOperationException("Topics are not supported in this version");
    }

    /**
     * Push a stream buffer to either an export target or a topic
     *
     * @param partitionId
     * @param tableName
     * @param startSequenceNumber
     * @param committedSequenceNumber
     * @param tupleCount
     * @param uniqueId
     * @param committedSpHandle
     * @param buffer
     */
    public void pushBuffer(
            int partitionId,
            String tableName,
            long startSequenceNumber,
            long committedSequenceNumber,
            long tupleCount,
            long uniqueId,
            long committedSpHandle,
            BBContainer buffer);

    /**
     * Push a buffer for an opaque topic
     *
     * <p>The export manager manages the starting sequence number in order to allow
     * correctly reporting the offsets to the producer application</p>
     *
     * @param partitionId
     * @param tableName
     * @param tupleCount
     * @param uniqueId
     * @param buffer
     * @return a {@link Pair} containing the data source's starting offset, and the offset of the
     * first record inserted
     */
    default public Pair<Long, Long> pushOpaqueTopicBuffer(
            int partitionId,
            String topicName,
            long tupleCount,
            long uniqueId,
            BBContainer buffer) {
        throw new UnsupportedOperationException("Opaque topics are not supported in this version");
    }

    /**
     * Return the offset of the next record that would be added to the opaque topic
     *
     * @param partitionId
     * @param topicName
     * @return
     */
    default public long getNextOpaqueTopicOffset(int partitionId, String topicName) {
        throw new UnsupportedOperationException("Opaque topics are not supported in this version");
    }

    /**
     * Return the {@link ExportSnapshotTuple} of the opaque topic
     *
     * @param partitionId
     * @param topicName
     * @return
     */
    default ExportSnapshotTuple getOpaqueTopicSnapshotTuple(int partitionId, String topicName) {
        throw new UnsupportedOperationException("Opaque topics are not supported in this version");
    }

    public void sync();

    public void invokeMigrateRowsDelete(int partition, String tableName, long deletableTxnId,  ProcedureCallback cb);

    public ExportMode getExportMode();

    /**
     * If data sources are still closing, wait until a fixed timeout, and proceed.
     * <p>
     * When a data source is dropped, the shutdown of the export coordinators proceed in the
     * background, as it involves shutting down multi-node synchronized state machines (SSM).
     * It is necessary to wait for this process to be completed before running the next catalog
     * update, in case the latter update re-creates data sources that were dropped, and attempts
     * to initialize SSMs using the same Zookeeper nodes as the previous SSM instances.
     *
     * @see {@link ExportCoordinator}, {@link E3ExportCoordinator}, and {@link SynchronizedStatesManager}.
     */
    public void waitOnClosingSources();

    /**
     * Notification that a data source was drained
     *
     * @param tableName
     * @param partition
     */
    public void onDrainedSource(String tableName, int partition);

    /**
     * Notification that a data source is closing (or being shut down)
     *
     * @param tableName
     * @param partition
     */
    public void onClosingSource(String tableName, int partition);

    /**
     * Notification that a data source has been closed (or shut down)
     *
     * @param tableName
     * @param partition
     */
    public void onClosedSource(String tableName, int partition);

    /**
     * The local partitions on the host has been removed after hash mismatch. Release the associated
     * resources.
     * @param removedPartitions  The de-commissioned local partitions
     */
    public void releaseResources(List<Integer> removedPartitions);

    @Override
    default void acceptPromotion() throws InterruptedException, java.util.concurrent.ExecutionException,
            org.apache.zookeeper_voltpatches.KeeperException {};
}
