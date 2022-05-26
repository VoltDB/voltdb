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
import java.util.concurrent.Future;

import javax.annotation_voltpatches.Nullable;

import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.dtxn.UndoAction;
import org.voltdb.exceptions.EEException;
import org.voltdb.iv2.DeterminismHash;
import org.voltdb.iv2.JoinProducerBase;
import org.voltdb.jni.ExecutionEngine.LoadTableCaller;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.sysprocs.LowImpactDeleteNT.ComparisonOperation;

/**
 * VoltProcedures invoke SiteProcedureConnection methods to
 * manipulate or request services from an ExecutionSite.
 */
public interface SiteProcedureConnection {

    public long getLatestUndoToken();

    /**
     * Get the non-VoltDB backend, if any, such as an HSQL or PostgreSQL
     * backend used for comparison testing. Returns null if we're not
     * configured to use it.
     */
    public NonVoltDBBackend getNonVoltDBBackendIfExists();

    /**
     * Get the catalog site id for the corresponding SiteProcedureConnection
     */
    public long getCorrespondingSiteId();

    /**
     * Get the partition id for the corresponding SiteProcedureConnection
     */
    public int getCorrespondingPartitionId();

    /**
     * Get the catalog host id for the corresponding SiteProcedureConnection
     */
    public int getCorrespondingHostId();

    /**
     * Get the catalog cluster id for the corresponding SiteProcedureConnection
     */
    public int getCorrespondingClusterId();

    /**
     * Get the DR gateway
     */
    public PartitionDRGateway getDRGateway();

    /**
     * Log settings changed. Signal EE to update log level.
     */
    public void updateBackendLogLevels();

    /**
     * loadTable method used by user-facing voltLoadTable() call in ProcedureRunner
     */
    public byte[] loadTable(
            TransactionState transactionState,
            String tableName,
            VoltTable data,
            LoadTableCaller caller)
    throws VoltAbortException;

    /**
     * loadTable method used internally by ExecutionSite/Site clients
     */
    public byte[] loadTable(
            long txnId,
            long spHandle,
            long uniqueId,
            int tableId,
            VoltTable data,
            LoadTableCaller caller);

    /**
     * Execute a set of plan fragments.
     * Note: it's ok to pass null for inputDepIds if the fragments
     * have no dependencies.
     */
    public FastDeserializer executePlanFragments(
            int numFragmentIds,
            long[] planFragmentIds,
            long[] inputDepIds,
            Object[] parameterSets,
            DeterminismHash determinismHash,
            String[] sqlTexts,
            boolean[] isWriteFrags,
            int[] sqlCRCs,
            long txnId,
            long spHandle,
            long uniqueId,
            boolean readOnly,
            boolean traceOn) throws EEException;

    /**
     * Allows caller to determine that a 50MB temporary (deallocated on next call) buffer
     * was used to generate the EE result table.
     */
    public boolean usingFallbackBuffer();

    /**
     * Let the EE know which batch of sql is running so it can include this
     * information in any slow query progress log messages.
     */
    public void setBatch(int batchIndex);

    /**
     * Let the EE know what the name of the currently executing procedure is so it can include this information in any
     * slow query progress log messages.
     *
     * @param procedureName Name of the procedure which is about to be run.
     */
    public void setupProcedure(@Nullable String procedureName);

    /**
     * Let the EE know that the currently executing procedure has completed. Should be called by all callers of
     * {@link #setupProcedure(String)}
     */
    public void completeProcedure();

    public void setBatchTimeout(int batchTimeout);
    public int getBatchTimeout();

    /**
     * Legacy recursable execution interface for MP transaction states.
     */
    public Map<Integer, List<VoltTable>> recursableRun(TransactionState currentTxnState);

    /**
     * Set the spHandle that's used by snapshot digest as the per-partition txnId. This gets updated during rejoin so
     * that the snapshot right after rejoin can have the correct value. It is also updated when transaction commits.
     */
    public void setSpHandleForSnapshotDigest(long spHandle);

    /**
     * IV2 commit / rollback interface to the EE and java level roll back if needed
     */
    public void truncateUndoLog(boolean rollback, boolean isEmptyDRTxn, long token, long spHandle, List<UndoAction> undoActions);

    /**
     * IV2: send dependencies to the EE
     */
    public void stashWorkUnitDependencies(final Map<Integer, List<VoltTable>> dependencies);

    /**
     * IV2: run a system procedure plan fragment
     */
    public DependencyPair executeSysProcPlanFragment(
            TransactionState txnState,
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params);

    /**
     * IV2: get the procedure runner for the named procedure
     */
    public ProcedureRunner getProcedureRunner(String procedureName);

    public ProcedureRunner getNibbleDeleteProcRunner(String procedureName,
                                                     Table table,
                                                     Column column,
                                                     ComparisonOperation op);

    public ProcedureRunner getMigrateProcRunner(String procName,
                                                     Table catTable,
                                                     Column column,
                                                     ComparisonOperation op);
    /**
     * @return SystemProcedureExecutionContext
     */
    public SystemProcedureExecutionContext getSystemProcedureExecutionContext();

    /*
     * This isn't just a simple setter, it has behavior side effects
     * as well because it causes the Site to start replaying log data
     * if configured to do so and it will also set export sequence numbers
     */
    public void setRejoinComplete(
            JoinProducerBase.JoinCompletionAction action,
            Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers,
            Map<Integer, Long> drSequenceNumbers,
            Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> allConsumerSiteTrackers,
            Map<Byte, byte[]> drCatalogCommands,
            Map<Byte, String[]> replicableTables,
            boolean requireExistingSequenceNumbers,
            long clusterCreateTime);

    public long[] getUSOForExportTable(String streamName);

    public TupleStreamStateInfo getDRTupleStreamStateInfo();

    public void setDRSequenceNumbers(Long partitionSequenceNumber, Long mpSequenceNumber);

    public void toggleProfiler(int toggle);

    public void tick();

    public void quiesce();

    public void setExportStreamPositions(ExportSnapshotTuple sequences,
                                         Integer partitionId,
                                         String tableSignature);

    public boolean deleteMigratedRows(long txnid, long spHandle, long uniqueId,
            String tableName, long deletableTxnId);

    public VoltTable[] getStats(StatsSelector selector, int[] locators,
                                boolean interval, Long now);

    // Snapshot services provided by the site
    public Future<?> doSnapshotWork();
    public void setPerPartitionTxnIds(long[] perPartitionTxnIds, boolean skipMultiPart);

    public TheHashinator getCurrentHashinator();
    public void updateHashinator(TheHashinator hashinator);
    public void setViewsEnabled(String viewNames, boolean enabled);
    public long[] validatePartitioning(long tableIds[], byte hashinatorConfig[]);
    public void notifyOfSnapshotNonce(String nonce, long snapshotSpHandle);

    public long applyBinaryLog(long txnId, long spHandle, long uniqueId, int remoteClusterId, byte logData[]);

    public long applyMpBinaryLog(long txnId, long spHandle, long uniqueId, int remoteClusterId, long remoteTxnUniqueId, byte logsData[]);
    public void setDRProtocolVersion(int drVersion);
    /*
     * Starting in DR version 7.0, we also generate a special event indicating the beginning of
     * binary log stream when we set protocol version.
     */
    public void setDRProtocolVersion(int drVersion, long txnId, long spHandle, long uniqueId);

    public void setDRStreamEnd(long txnId, long spHandle, long uniqueId);

    public void generateElasticChangeEvents(int oldPartitionCnt, int newPartitionCnt, long txnId, long spHandle, long uniqueId);

    public void generateElasticRebalanceEvents(int srcPartition, int destPartition, long txnId, long spHandle, long uniqueId);

    /**
     * Use this to disable all external streams (DR, export) from this site.
     * This is used by Elastic Shrink after all data from this site has been migrated.
     * The site continues to participate in MP txns until the partition is fully removed from
     * the cluster, but this will disable all external writes as well so that in effect the sites
     * are not participating.
     * <p> By default this is enabled in all sites.
     */
    public void disableExternalStreams();

    /**
     * Returns value showing whether external streams (DR and export) are enabled for this Site.
     *
     * @return true if external streams are enabled for this site, false otherwise.
     */
    public boolean externalStreamsEnabled();

    /**
     * @return the max size for all MP responses
     */
    public long getMaxTotalMpResponseSize();

    /**
     * Set the list of tables which can be replicated to from {@code clusterId}
     *
     * @param clusterId of producer cluster
     * @param tables    which match in both schemas and can be the target of replication
     */
    public void setReplicableTables(int clusterId, String[] tables, boolean clear);
}
