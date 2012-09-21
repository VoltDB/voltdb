/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.iv2;

import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.DependencyPair;
import org.voltdb.HsqlBackend;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SiteSnapshotConnection;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;
import org.voltdb.SysProcSelector;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngineIPC;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.utils.LogKeys;

import com.google.common.collect.ImmutableMap;

public class Site implements Runnable, SiteProcedureConnection, SiteSnapshotConnection
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    // Set to false trigger shutdown.
    volatile boolean m_shouldContinue = true;

    // HSId of this site's initiator.
    final long m_siteId;

    final int m_snapshotPriority;

    // Partition count is important for some reason.
    final int m_numberOfPartitions;

    // What type of EE is controlled
    final BackendTarget m_backend;

    // Is the site in a rejoining mode.
    boolean m_isRejoining;

    // Enumerate execution sites by host.
    private static final AtomicInteger siteIndexCounter = new AtomicInteger(0);
    private final int m_siteIndex = siteIndexCounter.getAndIncrement();

    // Manages pending tasks.
    final SiteTaskerQueue m_scheduler;

    /*
     * There is really no legit reason to touch the initiator mailbox from the site,
     * but it turns out to be necessary at startup when restoring a snapshot. The snapshot
     * has the transaction id for the partition that it must continue from and it has to be
     * set at all replicas of the partition.
     */
    final InitiatorMailbox m_initiatorMailbox;

    // Almighty execution engine and its HSQL sidekick
    ExecutionEngine m_ee;
    HsqlBackend m_hsql;

    // Each execution site manages snapshot using a SnapshotSiteProcessor
    private SnapshotSiteProcessor m_snapshotter;

    // Current catalog
    volatile CatalogContext m_context;

    // Currently available procedure
    volatile LoadedProcedureSet m_loadedProcedures;

    // Current topology
    int m_partitionId;

    // Need temporary access to some startup parameters in order to
    // initialize EEs in the right thread.
    private static class StartupConfig
    {
        final String m_serializedCatalog;
        final long m_txnId;
        StartupConfig(final String serCatalog, final long txnId)
        {
            m_serializedCatalog = serCatalog;
            m_txnId = txnId;
        }
    }
    private StartupConfig m_startupConfig = null;


    // Undo token state for the corresponding EE.
    public final static long kInvalidUndoToken = -1L;
    long latestUndoToken = 0L;

    @Override
    public long getNextUndoToken()
    {
        return ++latestUndoToken;
    }

    @Override
    public long getLatestUndoToken()
    {
        return latestUndoToken;
    }

    // Advanced in complete transaction.
    long m_lastCommittedTxnId = 0;
    long m_lastCommittedSpHandle = 0;
    long m_currentTxnId = Long.MIN_VALUE;
    long m_lastTxnTime = System.currentTimeMillis();

    SiteProcedureConnection getSiteProcedureConnection()
    {
        return this;
    }


    /**
     * SystemProcedures are "friends" with ExecutionSites and granted
     * access to internal state via m_systemProcedureContext.
     */
    SystemProcedureExecutionContext m_sysprocContext = new SystemProcedureExecutionContext() {
        @Override
        public Database getDatabase() {
            return m_context.database;
        }

        @Override
        public Cluster getCluster() {
            return m_context.cluster;
        }

        @Override
        public long getLastCommittedSpHandle() {
            return m_lastCommittedSpHandle;
        }

        @Override
        public long getCurrentTxnId() {
            return m_currentTxnId;
        }

        @Override
        public long getNextUndo() {
            return getNextUndoToken();
        }

        @Override
        public ImmutableMap<String, ProcedureRunner> getProcedures() {
            throw new RuntimeException("Not implemented in iv2");
            // return m_loadedProcedures.procs;
        }

        @Override
        public long getSiteId() {
            return m_siteId;
        }

        @Override
        public boolean isLowestSiteId()
        {
            // FUTURE: should pass this status in at construction.
            long lowestSiteId = VoltDB.instance().getSiteTrackerForSnapshot().getLowestSiteForHost(getHostId());
            return m_siteId == lowestSiteId;
        }


        @Override
        public int getHostId() {
            return CoreUtils.getHostIdFromHSId(m_siteId);
        }

        @Override
        public int getPartitionId() {
            return m_partitionId;
        }

        @Override
        public long getCatalogCRC() {
            return m_context.getCatalogCRC();
        }

        @Override
        public int getCatalogVersion() {
            return m_context.catalogVersion;
        }

        @Override
        public SiteTracker getSiteTracker() {
            throw new RuntimeException("Not implemented in iv2");
        }

        @Override
        public SiteTracker getSiteTrackerForSnapshot() {
            return VoltDB.instance().getSiteTrackerForSnapshot();
        }

        @Override
        public int getNumberOfPartitions() {
            return m_numberOfPartitions;
        }

        @Override
        public SiteProcedureConnection getSiteProcedureConnection()
        {
            return Site.this;
        }

        @Override
        public SiteSnapshotConnection getSiteSnapshotConnection()
        {
            return Site.this;
        }

        @Override
        public void updateBackendLogLevels() {
            Site.this.updateBackendLogLevels();
        }

        @Override
        public boolean updateCatalog(String diffCmds, CatalogContext context, CatalogSpecificPlanner csp) {
            return Site.this.updateCatalog(diffCmds, context, csp, false);
        }
    };

    /** Create a new execution site and the corresponding EE */
    public Site(
            SiteTaskerQueue scheduler,
            long siteId,
            BackendTarget backend,
            CatalogContext context,
            String serializedCatalog,
            long txnId,
            int partitionId,
            int numPartitions,
            boolean createForRejoin,
            int snapshotPriority,
            InitiatorMailbox initiatorMailbox)
    {
        m_siteId = siteId;
        m_context = context;
        m_partitionId = partitionId;
        m_numberOfPartitions = numPartitions;
        m_scheduler = scheduler;
        m_backend = backend;
        m_isRejoining = createForRejoin;
        m_snapshotPriority = snapshotPriority;
        // need this later when running in the final thread.
        m_startupConfig = new StartupConfig(serializedCatalog, txnId);
        m_lastCommittedTxnId = TxnEgo.makeZero(partitionId).getTxnId();
        m_lastCommittedSpHandle = TxnEgo.makeZero(partitionId).getTxnId();
        m_currentTxnId = Long.MIN_VALUE;
        m_initiatorMailbox = initiatorMailbox;
    }

    /** Update the loaded procedures. */
    void setLoadedProcedures(LoadedProcedureSet loadedProcedure)
    {
        m_loadedProcedures = loadedProcedure;
    }

    /** Thread specific initialization */
    void initialize(String serializedCatalog, long txnId)
    {
        if (m_backend == BackendTarget.NONE) {
            m_hsql = null;
            m_ee = new MockExecutionEngine();
        }
        else if (m_backend == BackendTarget.HSQLDB_BACKEND) {
            m_hsql = HsqlBackend.initializeHSQLBackend(m_siteId,
                                                       m_context);
            m_ee = new MockExecutionEngine();
        }
        else {
            m_hsql = null;
            m_ee = initializeEE(serializedCatalog, txnId);
        }

        m_snapshotter = new SnapshotSiteProcessor(new Runnable() {
            @Override
            public void run() {
                m_scheduler.offer(new SnapshotTask());
            }
        },
        m_snapshotPriority,
        new SnapshotSiteProcessor.IdlePredicate() {

            @Override
            public boolean idle(long now) {
                return (now - 5) > m_lastTxnTime;
            }
        });
    }

    /** Create a native VoltDB execution engine */
    ExecutionEngine initializeEE(String serializedCatalog, final long txnId)
    {
        String hostname = CoreUtils.getHostnameOrAddress();
        ExecutionEngine eeTemp = null;
        try {
            if (m_backend == BackendTarget.NATIVE_EE_JNI) {
                eeTemp =
                    new ExecutionEngineJNI(
                        m_context.cluster.getRelativeIndex(),
                        m_siteId,
                        m_partitionId,
                        CoreUtils.getHostIdFromHSId(m_siteId),
                        hostname,
                        m_context.cluster.getDeployment().get("deployment").
                        getSystemsettings().get("systemsettings").getMaxtemptablesize(),
                        m_numberOfPartitions);
                eeTemp.loadCatalog( txnId, serializedCatalog);
                // TODO: export integration will require a tick.
                // lastTickTime = EstTime.currentTimeMillis();
                // eeTemp.tick(lastTickTime, txnId);
            }
            else {
                // set up the EE over IPC
                eeTemp =
                    new ExecutionEngineIPC(
                            m_context.cluster.getRelativeIndex(),
                            m_siteId,
                            m_partitionId,
                            CoreUtils.getHostIdFromHSId(m_siteId),
                            hostname,
                            m_context.cluster.getDeployment().get("deployment").
                            getSystemsettings().get("systemsettings").getMaxtemptablesize(),
                            m_backend,
                            VoltDB.instance().getConfig().m_ipcPorts.remove(0),
                            m_numberOfPartitions);
                eeTemp.loadCatalog( 0, serializedCatalog);
                // TODO: export integration will require a tick.
                // lastTickTime = EstTime.currentTimeMillis();
                // eeTemp.tick( lastTickTime, 0);
            }
        }
        // just print error info an bail if we run into an error here
        catch (final Exception ex) {
            hostLog.l7dlog( Level.FATAL, LogKeys.host_ExecutionSite_FailedConstruction.name(),
                            new Object[] { m_siteId, m_siteIndex }, ex);
            VoltDB.crashLocalVoltDB(ex.getMessage(), true, ex);
        }
        return eeTemp;
    }


    @Override
    public void run()
    {
        Thread.currentThread().setName("Iv2ExecutionSite: " + CoreUtils.hsIdToString(m_siteId));
        initialize(m_startupConfig.m_serializedCatalog, m_startupConfig.m_txnId);
        m_startupConfig = null; // release the serializedCatalog bytes.

        try {
            while (m_shouldContinue) {
                runLoop();
            }
        }
        catch (final InterruptedException e) {
            // acceptable - this is how site blocked on an empty scheduler terminates.
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
        shutdown();
    }

    void runLoop() throws InterruptedException
    {
        SiteTasker task = m_scheduler.poll();
        if (task != null) {
            if (task instanceof TransactionTask) {
                m_currentTxnId = ((TransactionTask)task).getTxnId();
                m_lastTxnTime = EstTime.currentTimeMillis();
            }
            if (m_isRejoining) {
                task.runForRejoin(getSiteProcedureConnection());
            }
            else {
                task.run(getSiteProcedureConnection());
            }
        }
    }

    public void startShutdown()
    {
        m_shouldContinue = false;
    }

    void shutdown()
    {
        try {
            if (m_hsql != null) {
                HsqlBackend.shutdownInstance();
            }
            if (m_ee != null) {
                m_ee.release();
            }
            if (m_snapshotter != null) {
                m_snapshotter.shutdown();
            }
        } catch (InterruptedException e) {
            hostLog.warn("Interrupted shutdown execution site.", e);
        }
    }

    //
    // SiteSnapshotConnection interface
    //
    @Override
    public void initiateSnapshots(Deque<SnapshotTableTask> tasks, long txnId, int numLiveHosts) {
        m_snapshotter.initiateSnapshots(m_ee, tasks, txnId, numLiveHosts);
    }

    /*
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the syncing and closing of snapshot data targets has completed.
     */
    @Override
    public HashSet<Exception> completeSnapshotWork() throws InterruptedException {
        return m_snapshotter.completeSnapshotWork(m_ee);
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
    public void loadTable(long txnId, String clusterName, String databaseName,
            String tableName, VoltTable data) throws VoltAbortException
    {
        Cluster cluster = m_context.cluster;
        if (cluster == null) {
            throw new VoltAbortException("cluster '" + clusterName + "' does not exist");
        }
        Database db = cluster.getDatabases().get(databaseName);
        if (db == null) {
            throw new VoltAbortException("database '" + databaseName + "' does not exist in cluster " + clusterName);
        }
        Table table = db.getTables().getIgnoreCase(tableName);
        if (table == null) {
            throw new VoltAbortException("table '" + tableName + "' does not exist in database " + clusterName + "." + databaseName);
        }

        loadTable(txnId, table.getRelativeIndex(), data);
    }

    @Override
    public void loadTable(long txnId, int tableId, VoltTable data)
    {
        m_ee.loadTable(tableId, data,
                txnId,
                m_lastCommittedTxnId);
    }

    @Override
    public void updateBackendLogLevels()
    {
        m_ee.setLogLevels(org.voltdb.jni.EELoggers.getLogLevels());
    }

    @Override
    public long getReplicatedDMLDivisor()
    {
        return m_numberOfPartitions;
    }

    @Override
    public void simulateExecutePlanFragments(long txnId, boolean readOnly)
    {
        throw new RuntimeException("Not supported in IV2.");
    }

    @Override
    public Map<Integer, List<VoltTable>> recursableRun(
            TransactionState currentTxnState)
    {
        return currentTxnState.recursableRun(this);
    }

    @Override
    public void truncateUndoLog(boolean rollback, long beginUndoToken, long txnId, long spHandle)
    {
        if (rollback) {
            m_ee.undoUndoToken(beginUndoToken);
        }
        else {
            assert(latestUndoToken != Site.kInvalidUndoToken);
            assert(latestUndoToken >= beginUndoToken);
            if (latestUndoToken > beginUndoToken) {
                m_ee.releaseUndoToken(latestUndoToken);
            }
            m_lastCommittedTxnId = txnId;
            if (TxnEgo.getPartitionId(m_lastCommittedSpHandle) != TxnEgo.getPartitionId(spHandle)) {
                VoltDB.crashLocalVoltDB("Mismatch SpHandle partitiond id " +
                        TxnEgo.getPartitionId(m_lastCommittedSpHandle) + ", " +
                        TxnEgo.getPartitionId(spHandle), true, null);
            }
            m_lastCommittedSpHandle = spHandle;
        }
    }

    @Override
    public void stashWorkUnitDependencies(Map<Integer, List<VoltTable>> dependencies)
    {
        m_ee.stashWorkUnitDependencies(dependencies);
    }

    @Override
    public DependencyPair executeSysProcPlanFragment(
            TransactionState txnState,
            Map<Integer, List<VoltTable>> dependencies, long fragmentId,
            ParameterSet params)
    {
        ProcedureRunner runner = m_loadedProcedures.getSysproc(fragmentId);
        return runner.executePlanFragment(txnState, dependencies, fragmentId, params);
    }

    @Override
    public HsqlBackend getHsqlBackendIfExists()
    {
        return m_hsql;
    }

    @Override
    public long[] getUSOForExportTable(String signature)
    {
        return m_ee.getUSOForExportTable(signature);
    }

    @Override
    public void toggleProfiler(int toggle)
    {
        m_ee.toggleProfiler(toggle);
    }

    @Override
    public void quiesce()
    {
        m_ee.quiesce(m_lastCommittedTxnId);
    }

    @Override
    public void exportAction(boolean syncAction,
                             int ackOffset,
                             Long sequenceNumber,
                             Integer partitionId, String tableSignature)
    {
        m_ee.exportAction(syncAction, ackOffset, sequenceNumber,
                          partitionId, tableSignature);
    }

    @Override
    public VoltTable[] getStats(SysProcSelector selector, int[] locators,
                                boolean interval, Long now)
    {
        return m_ee.getStats(selector, locators, interval, now);
    }

    @Override
    public Future<?> doSnapshotWork(boolean ignoreQuietPeriod)
    {
        return m_snapshotter.doSnapshotWork(m_ee, ignoreQuietPeriod);
    }

    @Override
    public void setRejoinComplete() {
        m_isRejoining = false;
    }

    @Override
    public long loadPlanFragment(byte[] plan) throws EEException {
        return m_ee.loadPlanFragment(plan);
    }

    @Override
    public VoltTable[] executePlanFragments(int numFragmentIds,
            long[] planFragmentIds, long[] inputDepIds,
            ParameterSet[] parameterSets, long txnId, boolean readOnly)
            throws EEException {
        return m_ee.executePlanFragments(
                numFragmentIds,
                planFragmentIds,
                inputDepIds,
                parameterSets,
                txnId,
                m_lastCommittedTxnId,
                readOnly ? Long.MAX_VALUE : getNextUndoToken());
    }

    @Override
    public ProcedureRunner getProcedureRunner(String procedureName) {
        return m_loadedProcedures.getProcByName(procedureName);
    }

    /**
     * Update the catalog.  If we're the MPI, don't bother with the EE.
     */
    public boolean updateCatalog(String diffCmds, CatalogContext context, CatalogSpecificPlanner csp,
            boolean isMPI)
    {
        m_context = context;
        m_loadedProcedures.loadProcedures(m_context, m_backend, csp);

        if (!isMPI) {
            //Necessary to quiesce before updating the catalog
            //so export data for the old generation is pushed to Java.
            m_ee.quiesce(m_lastCommittedTxnId);
            m_ee.updateCatalog(m_context.m_transactionId, diffCmds);
        }

        return true;
    }

    @Override
    public void setPerPartitionTxnIds(long[] perPartitionTxnIds) {
        boolean foundMultipartTxnId = false;
        boolean foundSinglepartTxnId = false;
        for (long txnId : perPartitionTxnIds) {
            if (TxnEgo.getPartitionId(txnId) == m_partitionId) {
                if (foundSinglepartTxnId) {
                    VoltDB.crashLocalVoltDB(
                            "Found multiple transactions ids during restore for a partition", false, null);
                }
                foundSinglepartTxnId = true;
                m_initiatorMailbox.setMaxLastSeenTxnId(txnId);
            }
            if (TxnEgo.getPartitionId(txnId) == MpInitiator.MP_INIT_PID) {
                if (foundMultipartTxnId) {
                    VoltDB.crashLocalVoltDB(
                            "Found multiple transactions ids during restore for a multipart txnid", false, null);
                }
                foundMultipartTxnId = true;
                m_initiatorMailbox.setMaxLastSeenMultipartTxnId(txnId);
            }
        }
        if (!foundMultipartTxnId) {
            VoltDB.crashLocalVoltDB("Didn't find a multipart txnid on restore", false, null);
        }
    }
}
