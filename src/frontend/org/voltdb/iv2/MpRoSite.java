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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.DRConsumerDrIdTracker;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.DRIdempotencyResult;
import org.voltdb.DependencyPair;
import org.voltdb.HsqlBackend;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.NonVoltDBBackend;
import org.voltdb.ParameterSet;
import org.voltdb.PartitionDRGateway;
import org.voltdb.PostGISBackend;
import org.voltdb.PostgreSQLBackend;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SiteSnapshotConnection;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.StatsSelector;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator;
import org.voltdb.TupleStreamStateInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.dtxn.UndoAction;
import org.voltdb.exceptions.EEException;
import org.voltdb.jni.ExecutionEngine.LoadTableCaller;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.sysprocs.LowImpactDeleteNT.ComparisonOperation;
import org.voltdb.sysprocs.saverestore.HiddenColumnFilter;

/**
 * An implementation of Site which provides only the functionality
 * necessary to run read-only multi-partition transactions.  A pool
 * of these will be used to run multiple read-only transactions
 * concurrently.
 */
public class MpRoSite implements Runnable, SiteProcedureConnection
{
    @SuppressWarnings("unused")
    private static final VoltLogger tmLog = new VoltLogger("TM");

    // Set to false trigger shutdown.
    volatile boolean m_shouldContinue = true;

    // HSId of this site's initiator.
    final long m_siteId;

    // What type of EE is controlled
    final BackendTarget m_backend;

    // Manages pending tasks.
    final SiteTaskerQueue m_scheduler;

    // Still need m_non_voltdb_backend (formerly m_hsql) here
    NonVoltDBBackend m_non_voltdb_backend;

    // Current catalog
    volatile CatalogContext m_context;

    // Currently available procedure
    volatile LoadedProcedureSet m_loadedProcedures;

    // Current topology
    int m_partitionId;

    //a place holder for current running transaction on this site
    //the transaction will be terminated upon node shutdown.
    private TransactionState m_txnState = null;

    @Override
    public long getLatestUndoToken()
    {
        throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
    }

    SiteProcedureConnection getSiteProcedureConnection()
    {
        return this;
    }

    /**
     * SystemProcedures are "friends" with ExecutionSites and granted
     * access to internal state via m_systemProcedureContext.
     *
     * The only sysproc which should run on the RO MP Site is Adhoc.  Everything
     * else will yell at you.
     */
    SystemProcedureExecutionContext m_sysprocContext = new SystemProcedureExecutionContext() {
        @Override
        public ClusterSettings getClusterSettings() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public NodeSettings getPaths() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public Database getDatabase() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public Cluster getCluster() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public long getSpHandleForSnapshotDigest() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public long getSiteId() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public int getLocalSitesCount() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public int getLocalActiveSitesCount() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public boolean isLowestSiteId()
        {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public void setLowestSiteId()
        {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public int getClusterId()
        {
            return getCorrespondingClusterId();
        }

        @Override
        public int getHostId() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public int getPartitionId() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public long getCatalogCRC() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public byte[] getCatalogHash() {
            // AdHoc invocations need to be able to check the hash of the current catalog
            // against the hash of the catalog they were planned against.
            return m_context.getCatalogHash();
        }

        @Override
        public byte[] getDeploymentHash() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        // Needed for Adhoc queries
        @Override
        public int getCatalogVersion() {
            return m_context.catalogVersion;
        }

        @Override
        public long getGenerationId() {
            return m_context.m_genId;
        }

        @Override
        public SiteTracker getSiteTrackerForSnapshot() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public int getNumberOfPartitions() {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public void setNumberOfPartitions(int partitionCount) {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public SiteProcedureConnection getSiteProcedureConnection()
        {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public SiteSnapshotConnection getSiteSnapshotConnection()
        {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public void updateBackendLogLevels() {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public boolean updateCatalog(String diffCmds, CatalogContext context,
                boolean requiresSnapshotIsolation, long txnId, long uniqueId, long spHandle, boolean isReplay,
                boolean requireCatalogDiffCmdsApplyToEE, boolean requiresNewExportGeneration,
                Map<Byte, String[]> replicableTables)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public boolean updateSettings(CatalogContext context)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public TheHashinator getCurrentHashinator()
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public void updateHashinator(TheHashinator hashinator)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public boolean activateTableStream(int tableId, TableStreamType type, HiddenColumnFilter hiddenColumnFilter,
                boolean undo, byte[] predicates)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public void forceAllDRNodeBuffersToDisk(final boolean nofsync)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public DRIdempotencyResult isExpectedApplyBinaryLog(int producerClusterId, int producerPartitionId,
                                                            long logId)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public void appendApplyBinaryLogTxns(int producerClusterId, int producerPartitionId,
                                             long localUniqueId, DRConsumerDrIdTracker tracker)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public void recoverDrState(int clusterId, Map<Integer, Map<Integer, DRSiteDrIdTracker>> trackers,
                Map<Byte, String[]> replicableTables)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public void resetAllDrAppliedTracker() {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public void resetDrAppliedTracker(int clusterId) {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public boolean hasRealDrAppliedTracker(byte clusterId) {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public void initDRAppliedTracker(Map<Byte, Integer> clusterIdToPartitionCountMap) {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public Map<Integer, Map<Integer, DRSiteDrIdTracker>> getDrAppliedTrackers()
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public Pair<Long, Long> getDrLastAppliedUniqueIds()
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public Pair<Long, int[]> tableStreamSerializeMore(int tableId, TableStreamType type,
                List<DBBPool.BBContainer> outputBuffers)
        {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public Procedure ensureDefaultProcLoaded(String procName) {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public InitiatorMailbox getInitiatorMailbox() {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public void decommissionSite(boolean remove, boolean promote, int newSitePerHost) {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        }

        @Override
        public org.voltdb.TopicsSystemTableConnection getTopicsSystemTableConnection() {
            throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
        };
    };

    /** Create a new RO MP execution site */
    public MpRoSite(
            SiteTaskerQueue scheduler,
            long siteId,
            BackendTarget backend,
            CatalogContext context,
            int partitionId)
    {
        m_siteId = siteId;
        m_context = context;
        m_partitionId = partitionId;
        m_scheduler = scheduler;
        m_backend = backend;
    }

    /** Update the loaded procedures. */
    void setLoadedProcedures(LoadedProcedureSet loadedProcedure)
    {
        m_loadedProcedures = loadedProcedure;
    }

    /** Thread specific initialization */
    void initialize()
    {
        if (m_backend == BackendTarget.HSQLDB_BACKEND) {
            m_non_voltdb_backend = HsqlBackend.initializeHSQLBackend(m_siteId,
                                                       m_context);
        }
        else if (m_backend == BackendTarget.POSTGRESQL_BACKEND) {
            m_non_voltdb_backend = PostgreSQLBackend.initializePostgreSQLBackend(m_context);
        }
        else if (m_backend == BackendTarget.POSTGIS_BACKEND) {
            m_non_voltdb_backend = PostGISBackend.initializePostGISBackend(m_context);
        }
        else {
            m_non_voltdb_backend = null;
        }
    }

    @Override
    public void run()
    {
        initialize();

        try {
            while (m_shouldContinue) {
                // Normal operation blocks the site thread on the sitetasker queue.
                m_txnState = null;
                SiteTasker task = m_scheduler.take();
                task.run(getSiteProcedureConnection());
            }
        }
        catch (OutOfMemoryError e)
        {
            // Even though OOM should be caught by the Throwable section below,
            // it sadly needs to be handled seperately. The goal here is to make
            // sure VoltDB crashes.
            String errmsg = "Site: " + org.voltcore.utils.CoreUtils.hsIdToString(m_siteId) +
                " ran out of Java memory. " + "This node will shut down.";
            VoltDB.crashLocalVoltDB(errmsg, true, e);
        }
        catch (Throwable t)
        {
            String errmsg = "Site: " + org.voltcore.utils.CoreUtils.hsIdToString(m_siteId) +
                " encountered an " + "unexpected error and will die, taking this VoltDB node down.";
            VoltDB.crashLocalVoltDB(errmsg, true, t);
        }
    }

    /**
     * Commence the shutdown of the Site.  This will allow the run loop to escape, but
     * it will be blocked on taking from the SiteTaskerQueue.  To make shutdown actually
     * happen, the site needs to be unblocked by offering a null SiteTasker to the queue, see
     * Scheduler.m_nullTask for an example (or just use it).
     */
    public void startShutdown()
    {
        m_shouldContinue = false;
        //abort the current transaction
        if (m_txnState != null) {
            m_txnState.terminateTransaction();
        }
    }

    //
    // Legacy SiteProcedureConnection needed by ProcedureRunner
    //
    @Override
    public long getCorrespondingSiteId()
    {
        return m_siteId;
    }

    @Override
    public int getCorrespondingPartitionId()
    {
        return m_partitionId;
    }

    @Override
    public int getCorrespondingHostId()
    {
        return CoreUtils.getHostIdFromHSId(m_siteId);
    }

    @Override
    public int getCorrespondingClusterId()
    {
        return m_context.cluster.getDrclusterid();
    }

    @Override
    public PartitionDRGateway getDRGateway()
    {
        throw new UnsupportedOperationException("RO MP Site doesn't have DR gateway");
    }

    @Override
    public byte[] loadTable(TransactionState state, String tableName, VoltTable data, LoadTableCaller caller)
            throws VoltAbortException
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public byte[] loadTable(long txnId, long spHandle, long uniqueId, int tableId, VoltTable data,
            LoadTableCaller caller)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void updateBackendLogLevels()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public Map<Integer, List<VoltTable>> recursableRun(
            TransactionState currentTxnState)
    {
        m_txnState = currentTxnState;
        Map<Integer, List<VoltTable>> results = currentTxnState.recursableRun(this);
        m_txnState = null;
        return results;
    }

    @Override
    public void setSpHandleForSnapshotDigest(long spHandle)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void truncateUndoLog(boolean rollback, boolean isEmptyDRTxn, long beginUndoToken,
            long spHandle, List<UndoAction> undoActions)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void stashWorkUnitDependencies(Map<Integer, List<VoltTable>> dependencies)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public DependencyPair executeSysProcPlanFragment(
            TransactionState txnState,
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public NonVoltDBBackend getNonVoltDBBackendIfExists()
    {
        return m_non_voltdb_backend;
    }

    @Override
    public long[] getUSOForExportTable(String streamName)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public TupleStreamStateInfo getDRTupleStreamStateInfo()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setDRSequenceNumbers(Long partitionSequenceNumber, Long mpSequenceNumber)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void toggleProfiler(int toggle)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void tick()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void quiesce()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setExportStreamPositions(ExportSnapshotTuple sequences, Integer partitionId, String tableSignature)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public boolean deleteMigratedRows(long txnid,
                                      long spHandle,
                                      long uniqueId,
                                      String tableName,
                                      long deletableTxnId)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public VoltTable[] getStats(StatsSelector selector, int[] locators,
                                boolean interval, Long now)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public Future<?> doSnapshotWork()
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setRejoinComplete(
            JoinProducerBase.JoinCompletionAction replayComplete,
            Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers,
            Map<Integer, Long> drSequenceNumbers,
            Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> allConsumerSiteTrackers,
            Map<Byte, byte[]> drCatalogCommands,
            Map<Byte, String[]> replicableTables,
            boolean requireExistingSequenceNumbers,
            long clusterCreateTime)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
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
            boolean traceOn)
            throws EEException
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public boolean usingFallbackBuffer() {
        return false;
    }

    @Override
    public ProcedureRunner getProcedureRunner(String procedureName) {
        return m_loadedProcedures.getProcByName(procedureName);
    }

    @Override
    public ProcedureRunner getNibbleDeleteProcRunner(String procedureName,
                                                     Table catTable,
                                                     Column column,
                                                     ComparisonOperation op)
    {
        return m_loadedProcedures.getNibbleDeleteProc(
                    procedureName, catTable, column, op);
    }

    /**
     * Update the catalog.  If we're the MPI, don't bother with the EE.
     */
    public boolean updateCatalog(String diffCmds, CatalogContext context,
            boolean requiresSnapshotIsolationboolean, boolean isMPI)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setPerPartitionTxnIds(long[] perPartitionTxnIds, boolean skipMultipart) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public TheHashinator getCurrentHashinator()
    {
        return null;
    }

    @Override
    public void updateHashinator(TheHashinator hashinator) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setViewsEnabled(String viewNames, boolean enabled) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    /**
     * For the specified list of table ids, return the number of mispartitioned rows using
     * the provided hashinator config
     */
    @Override
    public long[] validatePartitioning(long[] tableIds, byte[] hashinatorConfig) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setBatch(int batchIndex) {
        // don't need to do anything here
    }

    @Override
    public void setupProcedure(String procedureName) {
        // don't need to do anything here I think?
    }

    @Override
    public void completeProcedure() {
        // don't need to do anything here
    }

    @Override
    public long applyBinaryLog(long txnId, long spHandle, long uniqueId, int remoteClusterId, byte log[]) {
        throw new UnsupportedOperationException("RO MP Site doesn't do this, shouldn't be here");
    }

    @Override
    public long applyMpBinaryLog(long txnId, long spHandle, long uniqueId, int remoteClusterId, long remoteUniqueId, byte[] logsData) {
        throw new UnsupportedOperationException("RO MP Site doesn't do this, shouldn't be here");
    }

    @Override
    public void setBatchTimeout(int batchTimeout) {
        throw new UnsupportedOperationException("RO MP Site doesn't do this, shouldn't be here");
    }

    @Override
    public int getBatchTimeout() {
        throw new UnsupportedOperationException("RO MP Site doesn't do this, shouldn't be here");
    }

    @Override
    public void setDRProtocolVersion(int drVersion) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setDRProtocolVersion(int drVersion, long txnId, long spHandle, long uniqueId)
    {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void generateElasticChangeEvents(int oldPartitionCnt, int newPartitionCnt, long txnId, long spHandle, long uniqueId) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void generateElasticRebalanceEvents(int srcPartition, int destPartition, long txnId, long spHandle, long uniqueId) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public void setDRStreamEnd(long spHandle, long txnId, long uniqueId) {
        throw new RuntimeException("RO MP Site doesn't do this, shouldn't be here.");
    }

    @Override
    public SystemProcedureExecutionContext getSystemProcedureExecutionContext() {
        return m_sysprocContext;
    }

    @Override
    public void notifyOfSnapshotNonce(String nonce, long snapshotSpHandle) {
        // TODO Auto-generated method stub
    }

    @Override
    public ProcedureRunner getMigrateProcRunner(String procName, Table catTable, Column column,
            ComparisonOperation op) {
        return null;
    }

    @Override
    public void disableExternalStreams() {
        throw new RuntimeException("disableExternalStreams should not be called on MpRoSite");
    }

    @Override
    public boolean externalStreamsEnabled() {
        throw new RuntimeException("externalStreamsEnabled should not be called on MpRoSite");
    }

    @Override
    public long getMaxTotalMpResponseSize() {
        return MpTransactionState.MP_MAX_TOTAL_RESP_SIZE / MpRoSitePool.MAX_POOL_SIZE;
    }

    @Override
    public void setReplicableTables(int clusterId, String[] tables, boolean clear) {
        throw new UnsupportedOperationException();
    }
}
