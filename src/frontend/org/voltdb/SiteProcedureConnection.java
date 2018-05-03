/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import org.voltcore.utils.Pair;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.dtxn.UndoAction;
import org.voltdb.exceptions.EEException;
import org.voltdb.iv2.DeterminismHash;
import org.voltdb.iv2.JoinProducerBase;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.sysprocs.LowImpactDelete.ComparisonOperation;

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
            long txnId,
            long spHandle,
            long uniqueId,
            String clusterName,
            String databaseName,
            String tableName,
            VoltTable data,
            boolean returnUniqueViolations,
            boolean shouldDRStream,
            boolean undo)
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
            boolean returnUniqueViolations,
            boolean shouldDRStream,
            boolean undo);

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
     * Let the EE know what the name of the currently executing procedure is
     * so it can include this information in any slow query progress log messages.
     */
    public void setProcedureName(String procedureName);

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
    public void truncateUndoLog(boolean rollback, long token, long spHandle, List<UndoAction> undoActions);

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
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            Map<Integer, Long> drSequenceNumbers,
            Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> allConsumerSiteTrackers,
            boolean requireExistingSequenceNumbers,
            long clusterCreateTime);

    public long[] getUSOForExportTable(String signature);

    public TupleStreamStateInfo getDRTupleStreamStateInfo();

    public void setDRSequenceNumbers(Long partitionSequenceNumber, Long mpSequenceNumber);

    public void toggleProfiler(int toggle);

    public void tick();

    public void quiesce();

    public void exportAction(boolean syncAction,
                             long uso,
                             Long sequenceNumber,
                             Integer partitionId,
                             String tableSignature);

    public VoltTable[] getStats(StatsSelector selector, int[] locators,
                                boolean interval, Long now);

    // Snapshot services provided by the site
    public Future<?> doSnapshotWork();
    public void setPerPartitionTxnIds(long[] perPartitionTxnIds, boolean skipMultiPart);

    public TheHashinator getCurrentHashinator();
    public void updateHashinator(TheHashinator hashinator);
    public long[] validatePartitioning(long tableIds[], byte hashinatorConfig[]);
    public long applyBinaryLog(long txnId, long spHandle, long uniqueId, int remoteClusterId, int remotePartitionId, byte logData[]);
    public void setDRProtocolVersion(int drVersion);
    /*
     * Starting in DR version 7.0, we also generate a special event indicating the beginning of
     * binary log stream when we set protocol version.
     */
    public void setDRProtocolVersion(int drVersion, long txnId, long spHandle, long uniqueId);

    public void setDRStreamEnd(long txnId, long spHandle, long uniqueId);

    public void generateElasticChangeEvents(int oldPartitionCnt, int newPartitionCnt, long txnId, long spHandle, long uniqueId);

    public void generateElasticRebalanceEvents(int srcPartition, int destPartition, long txnId, long spHandle, long uniqueId);
}
