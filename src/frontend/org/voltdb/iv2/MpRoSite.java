/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.DependencyPair;
import org.voltdb.HsqlBackend;
import org.voltdb.IndexStats;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.MemoryStats;
import org.voltdb.ParameterSet;
import org.voltdb.PartitionDRGateway;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SiteSnapshotConnection;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.StartAction;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TableStats;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.EEException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngine.TaskType;
import org.voltdb.jni.ExecutionEngineIPC;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.jni.Sha1Wrapper;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.utils.LogKeys;

import vanilla.java.affinity.impl.PosixJNAAffinity;

import com.google.common.collect.ImmutableMap;

public class MpRoSite implements Runnable, SiteProcedureConnection
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    // Set to false trigger shutdown.
    volatile boolean m_shouldContinue = true;

    // HSId of this site's initiator.
    final long m_siteId;

    final int m_snapshotPriority;

    // Partition count is important for some reason.
    int m_numberOfPartitions;

    // What type of EE is controlled
    final BackendTarget m_backend;

    // Is the site in a rejoining mode.
    private final static int kStateRunning = 0;
    private final static int kStateRejoining = 1;
    private final static int kStateReplayingRejoin = 2;
    private int m_rejoinState;
    private final TaskLog m_rejoinTaskLog;
    private JoinProducerBase.JoinCompletionAction m_replayCompletionAction;

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

    // Stats
    final TableStats m_tableStats;
    final IndexStats m_indexStats;
    final MemoryStats m_memStats;

    // Each execution site manages snapshot using a SnapshotSiteProcessor
    private SnapshotSiteProcessor m_snapshotter;

    // Current catalog
    volatile CatalogContext m_context;

    // Currently available procedure
    volatile LoadedProcedureSet m_loadedProcedures;

    // Cache the DR gateway here so that we can pass it to tasks as they are reconstructed from
    // the task log
    private final PartitionDRGateway m_drGateway;

    // Current topology
    int m_partitionId;

    private final String m_coreBindIds;

    // Need temporary access to some startup parameters in order to
    // initialize EEs in the right thread.
    private static class StartupConfig
    {
        final String m_serializedCatalog;
        final long m_timestamp;
        StartupConfig(final String serCatalog, final long timestamp)
        {
            m_serializedCatalog = serCatalog;
            m_timestamp = timestamp;
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

        /*
         * Expensive to compute, memoize it
         */
        private Boolean m_isLowestSiteId = null;
        @Override
        public boolean isLowestSiteId()
        {
            if (m_isLowestSiteId != null) {
                return m_isLowestSiteId;
            } else {
                // FUTURE: should pass this status in at construction.
                long lowestSiteId = VoltDB.instance().getSiteTrackerForSnapshot().getLowestSiteForHost(getHostId());
                m_isLowestSiteId = m_siteId == lowestSiteId;
                return m_isLowestSiteId;
            }
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
        public void setNumberOfPartitions(int partitionCount) {
            MpRoSite.this.setNumberOfPartitions(partitionCount);
        }

        @Override
        public SiteProcedureConnection getSiteProcedureConnection()
        {
            return MpRoSite.this;
        }

        @Override
        public SiteSnapshotConnection getSiteSnapshotConnection()
        {
            throw new RuntimeException("Not needed for RO MP Site, shouldn't be here.");
        }

        @Override
        public void updateBackendLogLevels() {
             MpRoSite.this.updateBackendLogLevels();
        }

        @Override
        public boolean updateCatalog(String diffCmds, CatalogContext context,
                CatalogSpecificPlanner csp, boolean requiresSnapshotIsolation)
        {
            return MpRoSite.this.updateCatalog(diffCmds, context, csp, requiresSnapshotIsolation, false);
        }

        @Override
        public void updateHashinator(Pair<TheHashinator.HashinatorType, byte[]> config)
        {
             MpRoSite.this.updateHashinator(config);
        }
    };

    /** Create a new execution site and the corresponding EE */
    public MpRoSite(
            SiteTaskerQueue scheduler,
            long siteId,
            BackendTarget backend,
            CatalogContext context,
            String serializedCatalog,
            long txnId,
            int partitionId,
            int numPartitions,
            StartAction startAction,
            int snapshotPriority,
            InitiatorMailbox initiatorMailbox,
            StatsAgent agent,
            MemoryStats memStats,
            String coreBindIds,
            TaskLog rejoinTaskLog,
            PartitionDRGateway drGateway)
    {
        m_siteId = siteId;
        m_context = context;
        m_partitionId = partitionId;
        m_numberOfPartitions = numPartitions;
        m_scheduler = scheduler;
        m_backend = backend;
        m_rejoinState = VoltDB.createForRejoin(startAction) || startAction == StartAction
                .JOIN ? kStateRejoining :
                kStateRunning;
        m_snapshotPriority = snapshotPriority;
        // need this later when running in the final thread.
        m_startupConfig = new StartupConfig(serializedCatalog, context.m_uniqueId);
        m_lastCommittedTxnId = TxnEgo.makeZero(partitionId).getTxnId();
        m_lastCommittedSpHandle = TxnEgo.makeZero(partitionId).getTxnId();
        m_currentTxnId = Long.MIN_VALUE;
        m_initiatorMailbox = initiatorMailbox;
        m_coreBindIds = coreBindIds;
        m_rejoinTaskLog = rejoinTaskLog;
        m_drGateway = drGateway;

        if (agent != null) {
            m_tableStats = new TableStats(m_siteId);
            agent.registerStatsSource(StatsSelector.TABLE,
                                      m_siteId,
                                      m_tableStats);
            m_indexStats = new IndexStats(m_siteId);
            agent.registerStatsSource(StatsSelector.INDEX,
                                      m_siteId,
                                      m_indexStats);
            m_memStats = memStats;
        } else {
            // MPI doesn't need to track these stats
            m_tableStats = null;
            m_indexStats = null;
            m_memStats = null;
        }
    }

    /** Update the loaded procedures. */
    void setLoadedProcedures(LoadedProcedureSet loadedProcedure)
    {
        m_loadedProcedures = loadedProcedure;
    }

    /** Thread specific initialization */
    void initialize(String serializedCatalog, long timestamp)
    {
        if (m_backend == BackendTarget.NONE) {
            m_hsql = null;
            m_ee = new MockExecutionEngine(this);
        }
        else if (m_backend == BackendTarget.HSQLDB_BACKEND) {
            m_hsql = HsqlBackend.initializeHSQLBackend(m_siteId,
                                                       m_context);
            m_ee = new MockExecutionEngine(this);
        }
        else {
            m_hsql = null;
            m_ee = initializeEE(serializedCatalog, timestamp);
        }

        m_snapshotter = new SnapshotSiteProcessor(m_scheduler,
        m_snapshotPriority,
        new SnapshotSiteProcessor.IdlePredicate() {
            @Override
            public boolean idle(long now) {
                return (now - 5) > m_lastTxnTime;
            }
        });
    }

    /** Create a native VoltDB execution engine */
    ExecutionEngine initializeEE(String serializedCatalog, final long timestamp)
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
                        getSystemsettings().get("systemsettings").getMaxtemptablesize(),
                        TheHashinator.getConfiguredHashinatorType(),
                        this);
                eeTemp.loadCatalog( timestamp, serializedCatalog);
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
                            TheHashinator.getConfiguredHashinatorType(),
                            TheHashinator.getConfigureBytes(m_numberOfPartitions),
                            this);
                eeTemp.loadCatalog( timestamp, serializedCatalog);
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
        if (m_coreBindIds != null) {
            PosixJNAAffinity.INSTANCE.setAffinity(m_coreBindIds);
        }
        initialize(m_startupConfig.m_serializedCatalog, m_startupConfig.m_timestamp);
        m_startupConfig = null; // release the serializedCatalog bytes.

        try {
            while (m_shouldContinue) {
                if (m_rejoinState == kStateRunning) {
                    // Normal operation blocks the site thread on the sitetasker queue.
                    SiteTasker task = m_scheduler.take();
                    if (task instanceof TransactionTask) {
                        m_currentTxnId = ((TransactionTask)task).getTxnId();
                        m_lastTxnTime = EstTime.currentTimeMillis();
                    }
                    task.run(getSiteProcedureConnection());
                }
                else {
                    // Rejoin operation poll and try to do some catchup work. Tasks
                    // are responsible for logging any rejoin work they might have.
                    SiteTasker task = m_scheduler.poll();
                    if (task != null) {
                        task.runForRejoin(getSiteProcedureConnection(), m_rejoinTaskLog);
                    }
                    replayFromTaskLog();
                }
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
        shutdown();
    }

    ParticipantTransactionState global_replay_mpTxn = null;
    void replayFromTaskLog() throws IOException
    {
        // not yet time to catch-up.
        if (m_rejoinState != kStateReplayingRejoin) {
            return;
        }

        // replay 10:1 in favor of replay
        for (int i=0; i < 10; ++i) {
            if (m_rejoinTaskLog.isEmpty()) {
                break;
            }

            TransactionInfoBaseMessage tibm = m_rejoinTaskLog.getNextMessage();
            if (tibm == null) {
                break;
            }

            if (tibm instanceof Iv2InitiateTaskMessage) {
                Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)tibm;
                SpProcedureTask t = new SpProcedureTask(
                        m_initiatorMailbox, m.getStoredProcedureName(),
                        null, m, m_drGateway);
                if (!filter(tibm)) {
                    t.runFromTaskLog(this);
                }
            }
            else if (tibm instanceof FragmentTaskMessage) {
                FragmentTaskMessage m = (FragmentTaskMessage)tibm;
                if (global_replay_mpTxn == null) {
                    global_replay_mpTxn = new ParticipantTransactionState(m.getTxnId(), m);
                }
                else if (global_replay_mpTxn.txnId != m.getTxnId()) {
                    VoltDB.crashLocalVoltDB("Started a MP transaction during replay before completing " +
                            " open transaction.", false, null);
                }
                FragmentTask t = new FragmentTask(m_initiatorMailbox, m, global_replay_mpTxn);
                if (!filter(tibm)) {
                    t.runFromTaskLog(this);
                }
            }
            else if (tibm instanceof CompleteTransactionMessage) {
                // Needs improvement: completes for sysprocs aren't filterable as sysprocs.
                // Only complete transactions that are open...
                if (global_replay_mpTxn != null) {
                    CompleteTransactionMessage m = (CompleteTransactionMessage)tibm;
                    CompleteTransactionTask t = new CompleteTransactionTask(global_replay_mpTxn,
                            null, m, m_drGateway);
                    if (!m.isRestart()) {
                        global_replay_mpTxn = null;
                    }
                    if (!filter(tibm)) {
                        t.runFromTaskLog(this);
                    }
                }
            }
            else {
                VoltDB.crashLocalVoltDB("Can not replay message type " +
                        tibm + " during live rejoin. Unexpected error.",
                        false, null);
            }
        }

        // exit replay being careful not to exit in the middle of a multi-partititon
        // transaction. The SPScheduler doesn't have a valid transaction state for a
        // partially replayed MP txn and in case of rollback the scheduler's undo token
        // is wrong. Run MP txns fully kStateRejoining or fully kStateRunning.
        if (m_rejoinTaskLog.isEmpty() && global_replay_mpTxn == null) {
            setReplayRejoinComplete();
        }
    }

    private boolean filter(TransactionInfoBaseMessage tibm)
    {
        // don't log sysproc fragments or iv2 intiiate task messages.
        // this is all jealously; should be refactored to ask tibm
        // if it wants to be filtered for rejoin and eliminate this
        // horrible introspection. This implementation mimics the
        // original live rejoin code for ExecutionSite...
        if (tibm instanceof FragmentTaskMessage && ((FragmentTaskMessage)tibm).isSysProcTask()) {
            return true;
        }
        else if (tibm instanceof Iv2InitiateTaskMessage) {
            Iv2InitiateTaskMessage itm = (Iv2InitiateTaskMessage)tibm;
            if ((itm.getStoredProcedureName().startsWith("@") == false) ||
                (itm.getStoredProcedureName().startsWith("@AdHoc") == true)) {
                return false;
            }
            else {
                return true;
            }
        }
        return false;
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
                try {
                    m_snapshotter.shutdown();
                } catch (InterruptedException e) {
                    hostLog.warn("Interrupted during shutdown", e);
                }
            }
        } catch (InterruptedException e) {
            hostLog.warn("Interrupted shutdown execution site.", e);
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
    public byte[] loadTable(long txnId, String clusterName, String databaseName,
            String tableName, VoltTable data, boolean returnUniqueViolations,
            long undoToken) throws VoltAbortException
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

        return loadTable(txnId, table.getRelativeIndex(), data, returnUniqueViolations, undoToken);
    }

    @Override
    public byte[] loadTable(long spHandle, int tableId, VoltTable data, boolean returnUniqueViolations,
            long undoToken)
    {
        return m_ee.loadTable(tableId, data,
                spHandle,
                m_lastCommittedSpHandle, returnUniqueViolations, undoToken);
    }

    @Override
    public void updateBackendLogLevels()
    {
        m_ee.setLogLevels(org.voltdb.jni.EELoggers.getLogLevels());
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
            assert(latestUndoToken != MpRoSite.kInvalidUndoToken);
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
        return runner.executeSysProcPlanFragment(txnState, dependencies, fragmentId, params);
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
    public void tick()
    {
        long time = System.currentTimeMillis();

        m_ee.tick(time, m_lastCommittedSpHandle);
        statsTick(time);
    }

    /**
     * Cache the current statistics.
     *
     * @param time
     */
    private void statsTick(long time)
    {
        /*
         * grab the table statistics from ee and put it into the statistics
         * agent.
         */
        if (m_tableStats != null) {
            CatalogMap<Table> tables = m_context.database.getTables();
            int[] tableIds = new int[tables.size()];
            int i = 0;
            for (Table table : tables) {
                tableIds[i++] = table.getRelativeIndex();
            }

            // data to aggregate
            long tupleCount = 0;
            int tupleDataMem = 0;
            int tupleAllocatedMem = 0;
            int indexMem = 0;
            int stringMem = 0;

            // update table stats
            final VoltTable[] s1 =
                m_ee.getStats(StatsSelector.TABLE, tableIds, false, time);
            if ((s1 != null) && (s1.length > 0)) {
                VoltTable stats = s1[0];
                assert(stats != null);

                // rollup the table memory stats for this site
                while (stats.advanceRow()) {
                    //Assert column index matches name for ENG-4092
                    assert(stats.getColumnName(7).equals("TUPLE_COUNT"));
                    tupleCount += stats.getLong(7);
                    assert(stats.getColumnName(8).equals("TUPLE_ALLOCATED_MEMORY"));
                    tupleAllocatedMem += (int) stats.getLong(8);
                    assert(stats.getColumnName(9).equals("TUPLE_DATA_MEMORY"));
                    tupleDataMem += (int) stats.getLong(9);
                    assert(stats.getColumnName(10).equals("STRING_DATA_MEMORY"));
                    stringMem += (int) stats.getLong(10);
                }
                stats.resetRowPosition();

                m_tableStats.setStatsTable(stats);
            }

            // update index stats
            final VoltTable[] s2 =
                m_ee.getStats(StatsSelector.INDEX, tableIds, false, time);
            if ((s2 != null) && (s2.length > 0)) {
                VoltTable stats = s2[0];
                assert(stats != null);

                // rollup the index memory stats for this site
                while (stats.advanceRow()) {
                    //Assert column index matches name for ENG-4092
                    assert(stats.getColumnName(11).equals("MEMORY_ESTIMATE"));
                    indexMem += stats.getLong(11);
                }
                stats.resetRowPosition();

                m_indexStats.setStatsTable(stats);
            }

            // update the rolled up memory statistics
            if (m_memStats != null) {
                m_memStats.eeUpdateMemStats(m_siteId,
                                            tupleCount,
                                            tupleDataMem,
                                            tupleAllocatedMem,
                                            indexMem,
                                            stringMem,
                                            m_ee.getThreadLocalPoolAllocations());
            }
        }
    }

    @Override
    public void quiesce()
    {
        m_ee.quiesce(m_lastCommittedSpHandle);
    }

    @Override
    public void exportAction(boolean syncAction,
                             long ackOffset,
                             Long sequenceNumber,
                             Integer partitionId, String tableSignature)
    {
        m_ee.exportAction(syncAction, ackOffset, sequenceNumber,
                          partitionId, tableSignature);
    }

    @Override
    public VoltTable[] getStats(StatsSelector selector, int[] locators,
                                boolean interval, Long now)
    {
        return m_ee.getStats(selector, locators, interval, now);
    }

    @Override
    public Future<?> doSnapshotWork()
    {
        return m_snapshotter.doSnapshotWork(m_sysprocContext, m_ee);
    }

    @Override
    public void setRejoinComplete(
            JoinProducerBase.JoinCompletionAction replayComplete,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers) {
        // transition from kStateRejoining to live rejoin replay.
        // pass through this transition in all cases; if not doing
        // live rejoin, will transfer to kStateRunning as usual
        // as the rejoin task log will be empty.
        assert(m_rejoinState == kStateRejoining);

        if (replayComplete == null) {
            throw new RuntimeException("Null Replay Complete Action.");
        }

        for (Map.Entry<String, Map<Integer, Pair<Long,Long>>> tableEntry : exportSequenceNumbers.entrySet()) {
            final Table catalogTable = m_context.tables.get(tableEntry.getKey());
            if (catalogTable == null) {
                VoltDB.crashLocalVoltDB(
                        "Unable to find catalog entry for table named " + tableEntry.getKey(),
                        true,
                        null);
            }
            Pair<Long,Long> sequenceNumbers = tableEntry.getValue().get(m_partitionId);
            if (sequenceNumbers == null) {
                VoltDB.crashLocalVoltDB(
                        "Could not find export sequence numbers for partition " +
                                m_partitionId + " table " +
                                tableEntry.getKey() + " have " + exportSequenceNumbers, false, null);
            }
            exportAction(
                    true,
                    sequenceNumbers.getFirst().longValue(),
                    sequenceNumbers.getSecond(),
                    m_partitionId,
                    catalogTable.getSignature());
        }

        m_rejoinState = kStateReplayingRejoin;
        m_replayCompletionAction = replayComplete;
        if (m_rejoinTaskLog != null) {
            m_rejoinTaskLog.setEarliestTxnId(
                    m_replayCompletionAction.getSnapshotTxnId());
        }
    }

    private void setReplayRejoinComplete() {
        // transition out of rejoin replay to normal running state.
        assert(m_rejoinState == kStateReplayingRejoin);
        m_replayCompletionAction.run();
        m_rejoinState = kStateRunning;
    }

    @Override
    public VoltTable[] executePlanFragments(int numFragmentIds,
            long[] planFragmentIds, long[] inputDepIds,
            Object[] parameterSets, long spHandle, long uniqueId, boolean readOnly)
            throws EEException {
        return m_ee.executePlanFragments(
                numFragmentIds,
                planFragmentIds,
                inputDepIds,
                parameterSets,
                spHandle,
                m_lastCommittedSpHandle,
                uniqueId,
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
            boolean requiresSnapshotIsolationboolean, boolean isMPI)
    {
        m_context = context;
        m_loadedProcedures.loadProcedures(m_context, m_backend, csp);

        if (isMPI) {
            // the rest of the work applies to sites with real EEs
            return true;
        }

        // if a snapshot is in process, wait for it to finish
        // don't bother if this isn't a schema change
        //
        if (requiresSnapshotIsolationboolean && m_snapshotter.isEESnapshotting()) {
            hostLog.info(String.format("Site %d performing schema change operation must block until snapshot is locally complete.",
                    CoreUtils.getSiteIdFromHSId(m_siteId)));
            try {
                m_snapshotter.completeSnapshotWork(m_sysprocContext, m_ee);
                hostLog.info(String.format("Site %d locally finished snapshot. Will update catalog now.",
                        CoreUtils.getSiteIdFromHSId(m_siteId)));
            }
            catch (InterruptedException e) {
                VoltDB.crashLocalVoltDB("Unexpected Interrupted Exception while finishing a snapshot for a catalog update.", true, e);
            }
        }

        //Necessary to quiesce before updating the catalog
        //so export data for the old generation is pushed to Java.
        m_ee.quiesce(m_lastCommittedTxnId);
        m_ee.updateCatalog(m_context.m_uniqueId, diffCmds);

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

    public void setNumberOfPartitions(int partitionCount)
    {
        m_numberOfPartitions = partitionCount;
    }

    private void updateHashinator(Pair<TheHashinator.HashinatorType, byte[]> config)
    {
        m_ee.updateHashinator(config.getFirst(), config.getSecond());
    }

    /**
     * For the specified list of table ids, return the number of mispartitioned rows using
     * the provided hashinator and hashinator config
     */
    @Override
    public long[] validatePartitioning(long[] tableIds, int hashinatorType, byte[] hashinatorConfig) {
        ByteBuffer paramBuffer = ByteBuffer.allocate(4 + (8 * tableIds.length) + 4 + 4 + hashinatorConfig.length);
        paramBuffer.putInt(tableIds.length);
        for (long tableId : tableIds) {
            paramBuffer.putLong(tableId);
        }
        paramBuffer.putInt(hashinatorType);
        paramBuffer.put(hashinatorConfig);

        ByteBuffer resultBuffer = ByteBuffer.wrap(m_ee.executeTask( TaskType.VALIDATE_PARTITIONING, paramBuffer.array()));
        long mispartitionedRows[] = new long[tableIds.length];
        for (int ii = 0; ii < tableIds.length; ii++) {
            mispartitionedRows[ii] = resultBuffer.getLong();
        }
        return mispartitionedRows;
    }
}
