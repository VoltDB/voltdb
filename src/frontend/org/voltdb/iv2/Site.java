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

import java.io.File;
import java.io.IOException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.EstTime;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.DependencyPair;
import org.voltdb.HsqlBackend;
import org.voltdb.IndexStats;
import org.voltdb.LoadedProcedureSet;

import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.MemoryStats;
import org.voltdb.ParameterSet;
import org.voltdb.ProcedureRunner;

import org.voltdb.rejoin.TaskLog;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SiteSnapshotConnection;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;
import org.voltdb.StatsAgent;
import org.voltdb.SysProcSelector;
import org.voltdb.SystemProcedureExecutionContext;

import org.voltdb.utils.MiscUtils;
import org.voltdb.TableStats;
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
    private final static int kStateRunning = 0;
    private final static int kStateRejoining = 1;
    private final static int kStateReplayingRejoin = 2;
    private int m_rejoinState;
    private TaskLog m_rejoinTaskLog;
    private RejoinProducer.ReplayCompletionAction m_replayCompletionAction;
    private final VoltDB.START_ACTION m_startAction;

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

    // Current topology
    int m_partitionId;

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
            VoltDB.START_ACTION startAction,
            int snapshotPriority,
            InitiatorMailbox initiatorMailbox,
            StatsAgent agent,
            MemoryStats memStats)
    {
        m_siteId = siteId;
        m_context = context;
        m_partitionId = partitionId;
        m_numberOfPartitions = numPartitions;
        m_scheduler = scheduler;
        m_backend = backend;
        m_startAction = startAction;
        m_rejoinState = VoltDB.createForRejoin(startAction) ? kStateRejoining : kStateRunning;
        m_snapshotPriority = snapshotPriority;
        // need this later when running in the final thread.
        m_startupConfig = new StartupConfig(serializedCatalog, context.m_timestamp);
        m_lastCommittedTxnId = TxnEgo.makeZero(partitionId).getTxnId();
        m_lastCommittedSpHandle = TxnEgo.makeZero(partitionId).getTxnId();
        m_currentTxnId = Long.MIN_VALUE;
        m_initiatorMailbox = initiatorMailbox;

        if (agent != null) {
            m_tableStats = new TableStats(m_siteId);
            agent.registerStatsSource(SysProcSelector.TABLE,
                                      m_siteId,
                                      m_tableStats);
            m_indexStats = new IndexStats(m_siteId);
            agent.registerStatsSource(SysProcSelector.INDEX,
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
            m_ee = new MockExecutionEngine();
        }
        else if (m_backend == BackendTarget.HSQLDB_BACKEND) {
            m_hsql = HsqlBackend.initializeHSQLBackend(m_siteId,
                                                       m_context);
            m_ee = new MockExecutionEngine();
        }
        else {
            m_hsql = null;
            m_ee = initializeEE(serializedCatalog, timestamp);
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

        if (m_startAction == VoltDB.START_ACTION.LIVE_REJOIN) {
            initializeForLiveRejoin();
        }

        if (m_rejoinTaskLog == null) {
            m_rejoinTaskLog = new TaskLog() {
                @Override
                public void logTask(TransactionInfoBaseMessage message)
                        throws IOException {
                }

                @Override
                public TransactionInfoBaseMessage getNextMessage()
                        throws IOException {
                    return null;
                }

                @Override
                public void setEarliestTxnId(long txnId) {
                }

                @Override
                public boolean isEmpty() throws IOException {
                    return true;
                }

                @Override
                public void close() throws IOException {
                }
            };
        }
    }


    void initializeForLiveRejoin()
    {
        // Construct task log and start logging task messages
        File overflowDir =
            new File(m_context.cluster.getVoltroot(), "rejoin_overflow");
        Class<?> taskLogKlass =
            MiscUtils.loadProClass("org.voltdb.rejoin.TaskLogImpl", "Rejoin", false);
        if (taskLogKlass != null) {
            Constructor<?> taskLogConstructor;
            try {
                taskLogConstructor = taskLogKlass.getConstructor(int.class, File.class, boolean.class);
                m_rejoinTaskLog = (TaskLog) taskLogConstructor.newInstance(m_partitionId, overflowDir, true);
            } catch (InvocationTargetException e) {
                VoltDB.crashLocalVoltDB("Unable to construct rejoin task log", true, e.getCause());
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to construct rejoin task log", true, e);
            }
        }
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
                        m_context.cluster.getDeployment().get("deployment").
                        getSystemsettings().get("systemsettings").getMaxtemptablesize(),
                        m_numberOfPartitions);
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
                            m_numberOfPartitions);
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

            // Apply the readonly / sysproc filter. With Iv2 read optimizations,
            // reads should not reach here; the cost of post-filtering shouldn't
            // be particularly high (vs pre-filtering).
            if (filter(tibm)) {
                continue;
            }

            if (tibm instanceof Iv2InitiateTaskMessage) {
                Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)tibm;
                SpProcedureTask t = new SpProcedureTask(
                        m_initiatorMailbox, m.getStoredProcedureName(),
                        null, m, null);
                t.runFromTaskLog(this);
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
                t.runFromTaskLog(this);
            }
            else if (tibm instanceof CompleteTransactionMessage) {
                // Needs improvement: completes for sysprocs aren't filterable as sysprocs.
                // Only complete transactions that are open...
                if (global_replay_mpTxn != null) {
                    CompleteTransactionMessage m = (CompleteTransactionMessage)tibm;
                    CompleteTransactionTask t = new CompleteTransactionTask(global_replay_mpTxn, null, m, null);
                    if (!m.isRestart()) {
                        global_replay_mpTxn = null;
                    }
                    t.runFromTaskLog(this);
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
    public void tick()
    {
        long time = System.currentTimeMillis();

        m_ee.tick(time, m_lastCommittedTxnId);
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
                m_ee.getStats(SysProcSelector.TABLE, tableIds, false, time);
            if (s1 != null) {
                VoltTable stats = s1[0];
                assert(stats != null);

                // rollup the table memory stats for this site
                while (stats.advanceRow()) {
                    tupleCount += stats.getLong(7);
                    tupleAllocatedMem += (int) stats.getLong(8);
                    tupleDataMem += (int) stats.getLong(9);
                    stringMem += (int) stats.getLong(10);
                }
                stats.resetRowPosition();

                m_tableStats.setStatsTable(stats);
            }

            // update index stats
            final VoltTable[] s2 =
                m_ee.getStats(SysProcSelector.INDEX, tableIds, false, time);
            if ((s2 != null) && (s2.length > 0)) {
                VoltTable stats = s2[0];
                assert(stats != null);

                // rollup the index memory stats for this site
                while (stats.advanceRow()) {
                    indexMem += stats.getLong(10);
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
    public void setRejoinComplete(RejoinProducer.ReplayCompletionAction replayComplete) {
        // transition from kStateRejoining to live rejoin replay.
        // pass through this transition in all cases; if not doing
        // live rejoin, will transfer to kStateRunning as usual
        // as the rejoin task log will be empty.
        assert(m_rejoinState == kStateRejoining);

        if (replayComplete == null) {
            throw new RuntimeException("Null Replay Complete Action.");
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
            m_ee.updateCatalog(m_context.m_timestamp, diffCmds);
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
