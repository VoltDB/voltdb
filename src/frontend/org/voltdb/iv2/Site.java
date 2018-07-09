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

package org.voltdb.iv2;

import java.io.IOException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.DRConsumerDrIdTracker;
import org.voltdb.DRConsumerDrIdTracker.DRSiteDrIdTracker;
import org.voltdb.DRIdempotencyResult;
import org.voltdb.DRLogSegmentId;
import org.voltdb.DependencyPair;
import org.voltdb.ExtensibleSnapshotDigestData;
import org.voltdb.HsqlBackend;
import org.voltdb.IndexStats;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.MemoryStats;
import org.voltdb.NonVoltDBBackend;
import org.voltdb.ParameterSet;
import org.voltdb.PartitionDRGateway;
import org.voltdb.PostGISBackend;
import org.voltdb.PostgreSQLBackend;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.SiteSnapshotConnection;
import org.voltdb.SnapshotDataTarget;
import org.voltdb.SnapshotFormat;
import org.voltdb.SnapshotSiteProcessor;
import org.voltdb.SnapshotTableTask;
import org.voltdb.StartAction;
import org.voltdb.StatsAgent;
import org.voltdb.StatsSelector;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.SystemProcedureExecutionContext;
import org.voltdb.TableStats;
import org.voltdb.TableStreamType;
import org.voltdb.TheHashinator;
import org.voltdb.TheHashinator.HashinatorConfig;
import org.voltdb.TupleStreamStateInfo;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure.VoltAbortException;
import org.voltdb.VoltTable;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.DRCatalogCommands;
import org.voltdb.catalog.DRCatalogDiffEngine;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.dtxn.UndoAction;
import org.voltdb.exceptions.EEException;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.jni.ExecutionEngine.EventType;
import org.voltdb.jni.ExecutionEngine.TaskType;
import org.voltdb.jni.ExecutionEngineIPC;
import org.voltdb.jni.ExecutionEngineJNI;
import org.voltdb.jni.MockExecutionEngine;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.rejoin.TaskLog;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.sysprocs.LowImpactDelete.ComparisonOperation;
import org.voltdb.sysprocs.SysProcFragmentId;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MinimumRatioMaintainer;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Preconditions;

import vanilla.java.affinity.impl.PosixJNAAffinity;

public class Site implements Runnable, SiteProcedureConnection, SiteSnapshotConnection
{
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger drLog = new VoltLogger("DRAGENT");

    private static final double m_taskLogReplayRatio =
            Double.valueOf(System.getProperty("TASKLOG_REPLAY_RATIO", "0.6"));

    // Set to false trigger shutdown.
    volatile boolean m_shouldContinue = true;

    // HSId of this site's initiator.
    final long m_siteId;

    final int m_snapshotPriority;

    // Partition count is important on SPIs, MPI doesn't use it.
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
     * There is really no legitimate reason to touch the initiator mailbox from the site,
     * but it turns out to be necessary at startup when restoring a snapshot. The snapshot
     * has the transaction id for the partition that it must continue from and it has to be
     * set at all replicas of the partition.
     */
    final InitiatorMailbox m_initiatorMailbox;

    // Almighty execution engine and its (HSQL or PostgreSQL) backend sidekick
    ExecutionEngine m_ee;
    NonVoltDBBackend m_non_voltdb_backend;

    // Stats
    final TableStats m_tableStats;
    final IndexStats m_indexStats;
    final MemoryStats m_memStats;

    // Each execution site manages snapshot using a SnapshotSiteProcessor
    private SnapshotSiteProcessor m_snapshotter;

    // Current catalog
    volatile CatalogContext m_context;

    // Currently available procedures
    volatile LoadedProcedureSet m_loadedProcedures;

    // Cache the DR gateway here so that we can pass it to tasks as they are reconstructed from
    // the task log
    private PartitionDRGateway m_drGateway;
    private PartitionDRGateway m_mpDrGateway;
    private final boolean m_isLowestSiteId; // true if this site has the MP gateway

    /*
     * Track the last producer-cluster unique IDs and drIds associated with an
     *  @ApplyBinaryLogSP and @ApplyBinaryLogMP invocation so it can be provided to the
     *  ReplicaDRGateway on repair
     */
    private Map<Integer, Map<Integer, DRSiteDrIdTracker>> m_maxSeenDrLogsBySrcPartition =
            new HashMap<>();
    private long m_lastLocalSpUniqueId = -1L;   // Only populated by the Site for ApplyBinaryLog Txns
    private long m_lastLocalMpUniqueId = -1L;   // Only populated by the Site for ApplyBinaryLog Txns

    // Current topology
    int m_partitionId;

    private final String m_coreBindIds;

    // Need temporary access to some startup parameters in order to
    // initialize EEs in the right thread.
    private static class StartupConfig
    {
        final String m_serializedCatalog;
        final long m_timestamp;
        StartupConfig(final String catalog, final long timestamp)
        {
            m_serializedCatalog = catalog;
            m_timestamp = timestamp;
        }
    }
    private StartupConfig m_startupConfig = null;


    // Undo token state for the corresponding EE.
    public final static long kInvalidUndoToken = -1L;
    private long m_latestUndoToken = 0L;
    private long m_latestUndoTxnId = Long.MIN_VALUE;

    private long getNextUndoToken(long txnId)
    {
        if (txnId != m_latestUndoTxnId) {
            m_latestUndoTxnId = txnId;
            return ++m_latestUndoToken;
        } else {
            return m_latestUndoToken;
        }
    }

    /*
     * Increment the undo token blindly to work around
     * issues using a single token per transaction
     * See ENG-5242
     */
    private long getNextUndoTokenBroken() {
        m_latestUndoTxnId = m_currentTxnId;
        return ++m_latestUndoToken;
    }

    @Override
    public long getLatestUndoToken()
    {
        return m_latestUndoToken;
    }

    // Advanced in complete transaction.
    long m_lastCommittedSpHandle = 0;
    long m_spHandleForSnapshotDigest = 0;
    long m_currentTxnId = Long.MIN_VALUE;
    long m_lastTxnTime = System.currentTimeMillis();

    /*
     * The version of the hashinator currently in use at the site will be consistent
     * across the node because balance partitions runs everywhere and all sites update.
     *
     * There is a corner case with live rejoin where sites replay their log and some sites
     * can pull ahead and update the global hashinator to ones further ahead causing transactions
     * to not be applied correctly during replay at the other sites. To avoid this each site
     * maintains a reference to it's own hashinator (which will be shared if possible).
     *
     * When two partition transactions come online they will diverge for pretty much the entire rebalance,
     * but will converge at the end when the final hash function update is issued everywhere
     */
    TheHashinator m_hashinator;

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
        public ClusterSettings getClusterSettings() {
            return m_context.getClusterSettings();
        }

        @Override
        public NodeSettings getPaths() {
            return m_context.getNodeSettings();
        }

        @Override
        public Database getDatabase() {
            return m_context.database;
        }

        @Override
        public Cluster getCluster() {
            return m_context.cluster;
        }

        @Override
        public long getSpHandleForSnapshotDigest() {
            return m_spHandleForSnapshotDigest;
        }

        @Override
        public long getSiteId() {
            return m_siteId;
        }

        @Override
        public int getLocalSitesCount() {
            return m_context.getNodeSettings().getLocalSitesCount();
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
        public int getClusterId() {
            return getCorrespondingClusterId();
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
        public byte[] getCatalogHash() {
            return m_context.getCatalogHash();
        }

        @Override
        public byte[] getDeploymentHash() {
            return m_context.getDeploymentHash();
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
            Site.this.setNumberOfPartitions(partitionCount);
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
        public boolean updateCatalog(String diffCmds, CatalogContext context,
                boolean requiresSnapshotIsolation,
                long txnId, long uniqueId, long spHandle,
                boolean isReplay,
                boolean requireCatalogDiffCmdsApplyToEE,
                boolean requiresNewExportGeneration)
        {
            return Site.this.updateCatalog(diffCmds, context, requiresSnapshotIsolation,
                    false, txnId, uniqueId, spHandle, isReplay,
                    requireCatalogDiffCmdsApplyToEE, requiresNewExportGeneration);
        }

        @Override
        public boolean updateSettings(CatalogContext context)
        {
            return Site.this.updateSettings(context);
        }

        @Override
        public TheHashinator getCurrentHashinator()
        {
            return m_hashinator;
        }

        @Override
        public void updateHashinator(TheHashinator hashinator)
        {
            Site.this.updateHashinator(hashinator);
        }

        @Override
        public boolean activateTableStream(final int tableId, TableStreamType type, boolean undo, byte[] predicates)
        {
            return m_ee.activateTableStream(tableId, type, undo ? getNextUndoToken(m_currentTxnId) : Long.MAX_VALUE, predicates);
        }

        @Override
        public Pair<Long, int[]> tableStreamSerializeMore(int tableId, TableStreamType type,
                                                          List<DBBPool.BBContainer> outputBuffers)
        {
            return m_ee.tableStreamSerializeMore(tableId, type, outputBuffers);
        }

        @Override
        public void forceAllDRNodeBuffersToDisk(final boolean nofsync)
        {
            if (m_drGateway != null) {
                m_drGateway.forceAllDRNodeBuffersToDisk(nofsync);
            }
            if (m_mpDrGateway != null) {
                m_mpDrGateway.forceAllDRNodeBuffersToDisk(nofsync);
            }
        }

        /**
         * Check to see if binary log is expected (start DR id adjacent to last received DR id)
         */
        @Override
        public DRIdempotencyResult isExpectedApplyBinaryLog(int producerClusterId, int producerPartitionId,
                                                            long logId)
        {
            Map<Integer, DRSiteDrIdTracker> clusterSources = m_maxSeenDrLogsBySrcPartition.get(producerClusterId);
            if (clusterSources == null) {
                drLog.warn(String.format("P%d binary log site idempotency check failed. " +
                                "Site doesn't have tracker for this cluster while processing logId %d",
                        producerPartitionId, logId));
            }
            else {
                DRSiteDrIdTracker targetTracker = clusterSources.get(producerPartitionId);
                if (targetTracker == null) {
                    drLog.warn(String.format("P%d binary log site idempotency check failed. " +
                                    "Site's tracker is null while processing logId %d",
                            producerPartitionId, logId));
                }
                else {
                    assert (targetTracker.size() > 0);

                    final long lastReceivedLogId = targetTracker.getLastReceivedLogId();
                    if (lastReceivedLogId+1 == logId) {
                        // This is what we expected
                        return DRIdempotencyResult.SUCCESS;
                    }
                    if (lastReceivedLogId >= logId) {
                        // This is a duplicate
                        return DRIdempotencyResult.DUPLICATE;
                    }

                    if (drLog.isTraceEnabled()) {
                        drLog.trace(String.format("P%d binary log site idempotency check failed. " +
                                                  "Site's tracker lastReceivedLogId %d while the logId %d",
                                                  producerPartitionId, lastReceivedLogId, logId));
                    }
                }
            }
            return DRIdempotencyResult.GAP;
        }

        @Override
        public void appendApplyBinaryLogTxns(int producerClusterId, int producerPartitionId,
                                             long localUniqueId, DRConsumerDrIdTracker tracker)
        {
            assert(tracker.size() > 0);
            if (UniqueIdGenerator.getPartitionIdFromUniqueId(localUniqueId) == MpInitiator.MP_INIT_PID) {
                m_lastLocalMpUniqueId = localUniqueId;
            }
            else {
                m_lastLocalSpUniqueId = localUniqueId;
            }
            Map<Integer, DRSiteDrIdTracker> clusterSources = m_maxSeenDrLogsBySrcPartition.get(producerClusterId);
            assert(clusterSources != null);
            DRSiteDrIdTracker targetTracker = clusterSources.get(producerPartitionId);
            assert(targetTracker != null);
            targetTracker.incLastReceivedLogId();
            targetTracker.mergeTracker(tracker);
        }

        @Override
        public void recoverWithDrAppliedTrackers(Map<Integer, Map<Integer, DRSiteDrIdTracker>> trackers)
        {
            m_maxSeenDrLogsBySrcPartition = trackers;
        }

        @Override
        public void resetDrAppliedTracker() {
            m_maxSeenDrLogsBySrcPartition.clear();
            if (drLog.isDebugEnabled()) {
                drLog.debug("Cleared DR Applied tracker");
            }
            m_lastLocalSpUniqueId = -1L;
            m_lastLocalMpUniqueId = -1L;
        }

        @Override
        public void resetDrAppliedTracker(byte clusterId) {
            m_maxSeenDrLogsBySrcPartition.remove((int) clusterId);
            if (drLog.isDebugEnabled()) {
                drLog.debug("Reset DR Applied tracker for " + clusterId);
            }
            if (m_maxSeenDrLogsBySrcPartition.isEmpty()) {
                m_lastLocalSpUniqueId = -1L;
                m_lastLocalMpUniqueId = -1L;
            }
        }

        @Override
        public boolean hasRealDrAppliedTracker(byte clusterId) {
            boolean has = false;
            if (m_maxSeenDrLogsBySrcPartition.containsKey((int) clusterId)) {
                for (DRConsumerDrIdTracker tracker: m_maxSeenDrLogsBySrcPartition.get((int) clusterId).values()) {
                    if (tracker.isRealTracker()) {
                        has = true;
                        break;
                    }
                }
            }
            return has;
        }

        @Override
        public void initDRAppliedTracker(Map<Byte, Integer> clusterIdToPartitionCountMap, boolean hasReplicatedStream) {
            for (Map.Entry<Byte, Integer> entry : clusterIdToPartitionCountMap.entrySet()) {
                int producerClusterId = entry.getKey();
                Map<Integer, DRSiteDrIdTracker> clusterSources =
                        m_maxSeenDrLogsBySrcPartition.getOrDefault(producerClusterId, new HashMap<>());
                if (hasReplicatedStream && !clusterSources.containsKey(MpInitiator.MP_INIT_PID)) {
                    DRSiteDrIdTracker tracker =
                            DRConsumerDrIdTracker.createSiteTracker(0,
                                    DRLogSegmentId.makeEmptyDRId(producerClusterId),
                                    Long.MIN_VALUE, Long.MIN_VALUE, MpInitiator.MP_INIT_PID);
                    clusterSources.put(MpInitiator.MP_INIT_PID, tracker);
                }
                int oldProducerPartitionCount = clusterSources.size() - (clusterSources.containsKey(MpInitiator.MP_INIT_PID) ? 1 : 0);
                int newProducerPartitionCount = entry.getValue();
                assert(oldProducerPartitionCount >= 0);
                assert(newProducerPartitionCount != -1);

                for (int i = oldProducerPartitionCount; i < newProducerPartitionCount; i++) {
                    DRSiteDrIdTracker tracker =
                            DRConsumerDrIdTracker.createSiteTracker(0,
                                    DRLogSegmentId.makeEmptyDRId(producerClusterId),
                                    Long.MIN_VALUE, Long.MIN_VALUE, i);
                    clusterSources.put(i, tracker);
                }

                m_maxSeenDrLogsBySrcPartition.put(producerClusterId, clusterSources);
            }
        }

        @Override
        public Map<Integer, Map<Integer, DRSiteDrIdTracker>> getDrAppliedTrackers()
        {
            DRConsumerDrIdTracker.debugTraceTracker(drLog, m_maxSeenDrLogsBySrcPartition);
            return m_maxSeenDrLogsBySrcPartition;
        }

        @Override
        public Pair<Long, Long> getDrLastAppliedUniqueIds()
        {
            return Pair.of(m_lastLocalSpUniqueId, m_lastLocalMpUniqueId);
        }

        @Override
        public Procedure ensureDefaultProcLoaded(String procName) {
            ProcedureRunner runner = Site.this.m_loadedProcedures.getProcByName(procName);
            return runner.getCatalogProcedure();
        }

        @Override
        public InitiatorMailbox getInitiatorMailbox() {
            return m_initiatorMailbox;
        }
    };

    /** Create a new execution site and the corresponding EE */
    public Site(
            SiteTaskerQueue scheduler,
            long siteId,
            BackendTarget backend,
            CatalogContext context,
            String serializedCatalog,
            int partitionId,
            int numPartitions,
            StartAction startAction,
            int snapshotPriority,
            InitiatorMailbox initiatorMailbox,
            StatsAgent agent,
            MemoryStats memStats,
            String coreBindIds,
            TaskLog rejoinTaskLog,
            boolean isLowestSiteId)
    {
        m_siteId = siteId;
        m_context = context;
        m_partitionId = partitionId;
        m_numberOfPartitions = numPartitions;
        m_scheduler = scheduler;
        m_backend = backend;
        m_rejoinState = startAction.doesJoin() ? kStateRejoining : kStateRunning;
        m_snapshotPriority = snapshotPriority;
        // need this later when running in the final thread.
        m_startupConfig = new StartupConfig(serializedCatalog, context.m_genId);
        m_lastCommittedSpHandle = TxnEgo.makeZero(partitionId).getTxnId();
        m_spHandleForSnapshotDigest = m_lastCommittedSpHandle;
        m_currentTxnId = Long.MIN_VALUE;
        m_initiatorMailbox = initiatorMailbox;
        m_coreBindIds = coreBindIds;
        m_rejoinTaskLog = rejoinTaskLog;
        m_isLowestSiteId = isLowestSiteId;
        m_hashinator = TheHashinator.getCurrentHashinator();

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

    public void setDRGateway(PartitionDRGateway drGateway,
                             PartitionDRGateway mpDrGateway)
    {
        m_drGateway = drGateway;
        m_mpDrGateway = mpDrGateway;
        if (m_isLowestSiteId && m_mpDrGateway == null) {
            throw new IllegalArgumentException("This site should contain the MP DR gateway but was not given");
        } else if (!m_isLowestSiteId && m_mpDrGateway != null) {
            throw new IllegalArgumentException("This site should not contain the MP DR gateway but was given");
        }
    }

    /** Update the loaded procedures. */
    void setLoadedProcedures(LoadedProcedureSet loadedProcedure)
    {
        m_loadedProcedures = loadedProcedure;
    }

    /** Thread specific initialization */
    void initialize()
    {
        if (m_backend == BackendTarget.NONE) {
            m_non_voltdb_backend = null;
            m_ee = new MockExecutionEngine();
        }
        else if (m_backend == BackendTarget.HSQLDB_BACKEND) {
            m_non_voltdb_backend = HsqlBackend.initializeHSQLBackend(m_siteId, m_context);
            m_ee = new MockExecutionEngine();
        }
        else if (m_backend == BackendTarget.POSTGRESQL_BACKEND) {
            m_non_voltdb_backend = PostgreSQLBackend.initializePostgreSQLBackend(m_context);
            m_ee = new MockExecutionEngine();
        }
        else if (m_backend == BackendTarget.POSTGIS_BACKEND) {
            m_non_voltdb_backend = PostGISBackend.initializePostGISBackend(m_context);
            m_ee = new MockExecutionEngine();
        }
        else {
            m_non_voltdb_backend = null;
            m_ee = initializeEE();
        }
        m_ee.loadFunctions(m_context);

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
    ExecutionEngine initializeEE()
    {
        String hostname = CoreUtils.getHostnameOrAddress();
        HashinatorConfig hashinatorConfig = TheHashinator.getCurrentConfig();
        ExecutionEngine eeTemp = null;
        Deployment deploy = m_context.cluster.getDeployment().get("deployment");
        final int defaultDrBufferSize = Integer.getInteger("DR_DEFAULT_BUFFER_SIZE", 512 * 1024); // 512KB
        int tempTableMaxSize = deploy.getSystemsettings().get("systemsettings").getTemptablemaxsize();
        if (System.getProperty("TEMP_TABLE_MAX_SIZE") != null) {
            // Allow a system property to override the deployment setting
            // for testing purposes.
            tempTableMaxSize = Integer.getInteger("TEMP_TABLE_MAX_SIZE");
        }

        try {
            if (m_backend == BackendTarget.NATIVE_EE_JNI) {
                eeTemp =
                    new ExecutionEngineJNI(
                        m_context.cluster.getRelativeIndex(),
                        m_siteId,
                        m_partitionId,
                        m_context.getNodeSettings().getLocalSitesCount(),
                        CoreUtils.getHostIdFromHSId(m_siteId),
                        hostname,
                        m_context.cluster.getDrclusterid(),
                        defaultDrBufferSize,
                        tempTableMaxSize,
                        hashinatorConfig,
                        m_isLowestSiteId);
            }
            else if (m_backend == BackendTarget.NATIVE_EE_SPY_JNI){
                Class<?> spyClass = Class.forName("org.mockito.Mockito");
                Method spyMethod = spyClass.getDeclaredMethod("spy", Object.class);
                ExecutionEngine internalEE = new ExecutionEngineJNI(
                        m_context.cluster.getRelativeIndex(),
                        m_siteId,
                        m_partitionId,
                        m_context.getNodeSettings().getLocalSitesCount(),
                        CoreUtils.getHostIdFromHSId(m_siteId),
                        hostname,
                        m_context.cluster.getDrclusterid(),
                        defaultDrBufferSize,
                        tempTableMaxSize,
                        hashinatorConfig,
                        m_isLowestSiteId);
                eeTemp = (ExecutionEngine) spyMethod.invoke(null, internalEE);
            }
            else {
                // set up the EE over IPC
                eeTemp =
                    new ExecutionEngineIPC(
                            m_context.cluster.getRelativeIndex(),
                            m_siteId,
                            m_partitionId,
                            m_context.getNodeSettings().getLocalSitesCount(),
                            CoreUtils.getHostIdFromHSId(m_siteId),
                            hostname,
                            m_context.cluster.getDrclusterid(),
                            defaultDrBufferSize,
                            tempTableMaxSize,
                            m_backend,
                            VoltDB.instance().getConfig().m_ipcPort,
                            hashinatorConfig,
                            m_isLowestSiteId);
            }
            eeTemp.loadCatalog(m_startupConfig.m_timestamp, m_startupConfig.m_serializedCatalog);
            eeTemp.setBatchTimeout(m_context.cluster.getDeployment().get("deployment").
                            getSystemsettings().get("systemsettings").getQuerytimeout());
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
        if (m_partitionId == MpInitiator.MP_INIT_PID) {
            Thread.currentThread().setName("MP Site - " + CoreUtils.hsIdToString(m_siteId));
        }
        else {
            Thread.currentThread().setName("SP " + m_partitionId + " Site - " + CoreUtils.hsIdToString(m_siteId));
        }
        if (m_coreBindIds != null) {
            PosixJNAAffinity.INSTANCE.setAffinity(m_coreBindIds);
        }
        initialize();
        m_startupConfig = null; // release the serializableCatalog.
        //Maintain a minimum ratio of task log (unrestricted) to live (restricted) transactions
        final MinimumRatioMaintainer mrm = new MinimumRatioMaintainer(m_taskLogReplayRatio);
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
                } else if (m_rejoinState == kStateReplayingRejoin) {
                    // Rejoin operation poll and try to do some catchup work. Tasks
                    // are responsible for logging any rejoin work they might have.
                    SiteTasker task = m_scheduler.peek();
                    boolean didWork = false;
                    if (task != null) {
                        didWork = true;
                        //If the task log is empty, free to execute the task
                        //If the mrm says we can do a restricted task, go do it
                        //Otherwise spin doing unrestricted tasks until we can bail out
                        //and do the restricted task that was polled
                        while (!m_rejoinTaskLog.isEmpty() && !mrm.canDoRestricted()) {
                            replayFromTaskLog(mrm);
                        }
                        mrm.didRestricted();
                        // If m_rejoinState didn't change to kStateRunning because of replayFromTaskLog(),
                        // remove the task from the scheduler and give it to task log.
                        // Otherwise, keep the task in the scheduler and let the next loop take and handle it
                        if (m_rejoinState != kStateRunning) {
                            m_scheduler.poll();
                            task.runForRejoin(getSiteProcedureConnection(), m_rejoinTaskLog);
                        }
                    } else {
                        //If there are no tasks, do task log work
                        didWork = replayFromTaskLog(mrm);
                    }
                    if (!didWork) Thread.yield();
                } else {
                    SiteTasker task = m_scheduler.take();
                    task.runForRejoin(getSiteProcedureConnection(), m_rejoinTaskLog);
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
        catch (Throwable t) {

            //do not emit message while the node is being shutdown.
            if (m_shouldContinue) {
                String errmsg = "Site: " + org.voltcore.utils.CoreUtils.hsIdToString(m_siteId) +
                        " encountered an " + "unexpected error and will die, taking this VoltDB node down.";
                hostLog.error(errmsg);

                for (StackTraceElement ste: t.getStackTrace()) {
                    hostLog.error(ste.toString());
                }

                VoltDB.crashLocalVoltDB(errmsg, true, t);
            }
        }

        try {
            shutdown();
        } finally {
            CompressionService.releaseThreadLocal();
        }
    }

    ParticipantTransactionState global_replay_mpTxn = null;
    boolean replayFromTaskLog(MinimumRatioMaintainer mrm) throws IOException
    {
        // not yet time to catch-up.
        if (m_rejoinState != kStateReplayingRejoin) {
            return false;
        }

        TransactionInfoBaseMessage tibm = m_rejoinTaskLog.getNextMessage();
        if (tibm != null) {
            mrm.didUnrestricted();
            if (tibm instanceof Iv2InitiateTaskMessage) {
                Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)tibm;
                SpProcedureTask t = new SpProcedureTask(
                        m_initiatorMailbox, m.getStoredProcedureName(),
                        null, m);
                if (!filter(tibm)) {
                    m_currentTxnId = t.getTxnId();
                    m_lastTxnTime = EstTime.currentTimeMillis();
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

                TransactionTask t;
                if (m.isSysProcTask()) {
                    t = new SysprocFragmentTask(m_initiatorMailbox, m, global_replay_mpTxn);
                } else {
                    t = new FragmentTask(m_initiatorMailbox, m, global_replay_mpTxn);
                }

                if (!filter(tibm)) {
                    m_currentTxnId = t.getTxnId();
                    m_lastTxnTime = EstTime.currentTimeMillis();
                    t.runFromTaskLog(this);
                }
            }
            else if (tibm instanceof CompleteTransactionMessage) {
                // Needs improvement: completes for sysprocs aren't filterable as sysprocs.
                // Only complete transactions that are open...
                if (global_replay_mpTxn != null) {
                    CompleteTransactionMessage m = (CompleteTransactionMessage)tibm;
                    CompleteTransactionTask t = new CompleteTransactionTask(m_initiatorMailbox, global_replay_mpTxn,
                            null, m);
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
        return tibm != null;
    }

    static boolean filter(TransactionInfoBaseMessage tibm)
    {
        // don't log sysproc fragments or iv2 initiate task messages.
        // this is all jealously; should be refactored to ask tibm
        // if it wants to be filtered for rejoin and eliminate this
        // horrible introspection. This implementation mimics the
        // original live rejoin code for ExecutionSite...
        // Multi part AdHoc Does not need to be chacked because its an alias and runs procedure as planned.
        if (tibm instanceof FragmentTaskMessage && ((FragmentTaskMessage)tibm).isSysProcTask()) {
            if (!SysProcFragmentId.isDurableFragment(((FragmentTaskMessage) tibm).getPlanHash(0))) {
                return true;
            }
        }
        else if (tibm instanceof Iv2InitiateTaskMessage) {
            Iv2InitiateTaskMessage itm = (Iv2InitiateTaskMessage) tibm;
            final SystemProcedureCatalog.Config sysproc = SystemProcedureCatalog.listing.get(itm.getStoredProcedureName());
            // All durable sysprocs and non-sysprocs should not get filtered.
            return sysproc != null && !sysproc.isDurable();
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
            if (m_non_voltdb_backend != null) {
                m_non_voltdb_backend.shutdownInstance();
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
            if (m_rejoinTaskLog != null) {
                try {
                    m_rejoinTaskLog.close();
                } catch (IOException e) {
                    hostLog.error("Exception closing rejoin task log", e);
                }
            }
        } catch (InterruptedException e) {
            hostLog.warn("Interrupted shutdown execution site.", e);
        }
    }

    //
    // SiteSnapshotConnection interface
    //
    @Override
    public void initiateSnapshots(
            SnapshotFormat format,
            Deque<SnapshotTableTask> tasks,
            long txnId,
            boolean isTruncation,
            ExtensibleSnapshotDigestData extraSnapshotData) {
        m_snapshotter.initiateSnapshots(m_sysprocContext, format, tasks, txnId, isTruncation, extraSnapshotData);
    }

    /*
     * Do snapshot work exclusively until there is no more. Also blocks
     * until the syncing and closing of snapshot data targets has completed.
     */
    @Override
    public HashSet<Exception> completeSnapshotWork() throws InterruptedException {
        return m_snapshotter.completeSnapshotWork(m_sysprocContext);
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
        return m_drGateway;
    }

    @Override
    public byte[] loadTable(long txnId, long spHandle, long uniqueId, String clusterName, String databaseName,
            String tableName, VoltTable data,
            boolean returnUniqueViolations, boolean shouldDRStream, boolean undo) throws VoltAbortException
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

        return loadTable(txnId, spHandle, uniqueId, table.getRelativeIndex(), data, returnUniqueViolations, shouldDRStream, undo);
    }

    @Override
    public byte[] loadTable(long txnId, long spHandle, long uniqueId, int tableId,
            VoltTable data, boolean returnUniqueViolations, boolean shouldDRStream,
            boolean undo)
    {
        // Long.MAX_VALUE is a no-op don't track undo token
        return m_ee.loadTable(tableId, data, txnId,
                spHandle,
                m_lastCommittedSpHandle,
                uniqueId,
                returnUniqueViolations,
                shouldDRStream,
                undo ? getNextUndoToken(m_currentTxnId) : Long.MAX_VALUE);
    }

    @Override
    public void updateBackendLogLevels()
    {
        m_ee.setLogLevels(org.voltdb.jni.EELoggers.getLogLevels());
    }

    @Override
    public void setViewsEnabled(String viewNames, boolean enabled) {
        m_ee.setViewsEnabled(viewNames, enabled);
    }

    @Override
    public Map<Integer, List<VoltTable>> recursableRun(
            TransactionState currentTxnState)
    {
        return currentTxnState.recursableRun(this);
    }

    @Override
    public void setSpHandleForSnapshotDigest(long spHandle)
    {
        // During rejoin, the spHandle is updated even though the site is not executing the tasks. If it's a live
        // rejoin, all logged tasks will be replayed. So the spHandle may go backward and forward again. It should
        // stop at the same point after replay.
        m_spHandleForSnapshotDigest = Math.max(m_spHandleForSnapshotDigest, spHandle);
    }

    /**
     * Java level related stuffs that are also needed to roll back
     * @param undoLog
     * @param undo
     */
    private static void handleUndoLog(List<UndoAction> undoLog, boolean undo) {
        if (undoLog == null) return;

        for (final ListIterator<UndoAction> iterator = undoLog.listIterator(undoLog.size()); iterator.hasPrevious();) {
            final UndoAction action = iterator.previous();
            if (undo)
                action.undo();
            else
                action.release();
        }
        if (undo)
            undoLog.clear();
    }

    private void setLastCommittedSpHandle(long spHandle)
    {
        if (TxnEgo.getPartitionId(m_lastCommittedSpHandle) != m_partitionId) {
            VoltDB.crashLocalVoltDB("Mismatch SpHandle partitiond id " +
                                    TxnEgo.getPartitionId(m_lastCommittedSpHandle) + ", " +
                                    TxnEgo.getPartitionId(spHandle), true, null);
        }
        m_lastCommittedSpHandle = spHandle;
        setSpHandleForSnapshotDigest(m_lastCommittedSpHandle);
    }

    @Override
    public void truncateUndoLog(boolean rollback, long beginUndoToken, long spHandle, List<UndoAction> undoLog)
    {
        // Set the last committed txnId even if there is nothing to undo, as long as the txn is not rolling back.
        if (!rollback) {
            setLastCommittedSpHandle(spHandle);
        }

        //Any new txnid will create a new undo quantum, including the same txnid again
        m_latestUndoTxnId = Long.MIN_VALUE;
        //If the begin undo token is not set the txn never did any work so there is nothing to undo/release
        if (beginUndoToken == Site.kInvalidUndoToken) return;
        if (rollback) {
            m_ee.undoUndoToken(beginUndoToken);
        }
        else {
            assert(m_latestUndoToken != Site.kInvalidUndoToken);
            assert(m_latestUndoToken >= beginUndoToken);
            if (m_latestUndoToken > beginUndoToken) {
                m_ee.releaseUndoToken(m_latestUndoToken);
            }
        }

        // java level roll back
        handleUndoLog(undoLog, rollback);
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
    public NonVoltDBBackend getNonVoltDBBackendIfExists()
    {
        return m_non_voltdb_backend;
    }

    @Override
    public long[] getUSOForExportTable(String signature)
    {
        return m_ee.getUSOForExportTable(signature);
    }

    @Override
    public TupleStreamStateInfo getDRTupleStreamStateInfo()
    {
        // Set the psetBuffer buffer capacity and clear the buffer
        m_ee.getParamBufferForExecuteTask(0);
        ByteBuffer resultBuffer = ByteBuffer.wrap(m_ee.executeTask(TaskType.GET_DR_TUPLESTREAM_STATE, ByteBuffer.allocate(0)));
        long partitionSequenceNumber = resultBuffer.getLong();
        long partitionSpUniqueId = resultBuffer.getLong();
        long partitionMpUniqueId = resultBuffer.getLong();
        int drVersion = resultBuffer.getInt();
        DRLogSegmentId partitionInfo = new DRLogSegmentId(partitionSequenceNumber, partitionSpUniqueId, partitionMpUniqueId);
        byte hasReplicatedStateInfo = resultBuffer.get();
        TupleStreamStateInfo info = null;
        if (hasReplicatedStateInfo != 0) {
            long replicatedSequenceNumber = resultBuffer.getLong();
            long replicatedSpUniqueId = resultBuffer.getLong();
            long replicatedMpUniqueId = resultBuffer.getLong();
            DRLogSegmentId replicatedInfo = new DRLogSegmentId(replicatedSequenceNumber, replicatedSpUniqueId, replicatedMpUniqueId);
            info = new TupleStreamStateInfo(partitionInfo, replicatedInfo, drVersion);
        } else {
            info = new TupleStreamStateInfo(partitionInfo, drVersion);
        }
        return info;
    }

    @Override
    public void setDRSequenceNumbers(Long partitionSequenceNumber, Long mpSequenceNumber) {
        if (partitionSequenceNumber == null && mpSequenceNumber == null) return;
        ByteBuffer paramBuffer = m_ee.getParamBufferForExecuteTask(16);
        paramBuffer.putLong(partitionSequenceNumber != null ? partitionSequenceNumber : Long.MIN_VALUE);
        paramBuffer.putLong(mpSequenceNumber != null ? mpSequenceNumber : Long.MIN_VALUE);
        m_ee.executeTask(TaskType.SET_DR_SEQUENCE_NUMBERS, paramBuffer);
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
            long tupleDataMem = 0;
            long tupleAllocatedMem = 0;
            long indexMem = 0;
            long stringMem = 0;

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
                    assert(stats.getColumnName(6).equals("TABLE_TYPE"));
                    assert(stats.getColumnName(5).equals("TABLE_NAME"));
                    boolean isReplicated = tables.getIgnoreCase(stats.getString(5)).getIsreplicated();
                    boolean trackMemory = (!isReplicated) || m_isLowestSiteId;
                    if ("PersistentTable".equals(stats.getString(6)) && trackMemory){
                        tupleCount += stats.getLong(7);
                    }
                    assert(stats.getColumnName(8).equals("TUPLE_ALLOCATED_MEMORY"));
                    if (trackMemory) {
                        tupleAllocatedMem += stats.getLong(8);
                    }
                    assert(stats.getColumnName(9).equals("TUPLE_DATA_MEMORY"));
                    if (trackMemory) {
                        tupleDataMem += stats.getLong(9);
                    }
                    assert(stats.getColumnName(10).equals("STRING_DATA_MEMORY"));
                    if (trackMemory) {
                        stringMem += stats.getLong(10);
                    }
                }
                stats.resetRowPosition();

                m_tableStats.setStatsTable(stats);
            }
            else {
                // the EE returned no table stats, which means there are no tables.
                // Need to ensure the cached stats are cleared to reflect that
                m_tableStats.resetStatsTable();
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
                    assert(stats.getColumnName(6).equals("TABLE_NAME"));
                    boolean isReplicated = tables.getIgnoreCase(stats.getString(6)).getIsreplicated();
                    boolean trackMemory = (!isReplicated) || m_isLowestSiteId;
                    assert(stats.getColumnName(11).equals("MEMORY_ESTIMATE"));
                    if (trackMemory) {
                        indexMem += stats.getLong(11);
                    }
                }
                stats.resetRowPosition();

                m_indexStats.setStatsTable(stats);
            }
            else {
                // the EE returned no index stats, which means there are no indexes.
                // Need to ensure the cached stats are cleared to reflect that
                m_indexStats.resetStatsTable();
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
                             long uso,
                             Long sequenceNumber,
                             Integer partitionId, String tableSignature)
    {
        m_ee.exportAction(syncAction, uso, sequenceNumber,
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
        return m_snapshotter.doSnapshotWork(m_sysprocContext, false);
    }

    @Override
    public void startSnapshotWithTargets(Collection<SnapshotDataTarget> targets)
    {
        m_snapshotter.startSnapshotWithTargets(targets, System.currentTimeMillis());
    }

    @Override
    public void setRejoinComplete(
            JoinProducerBase.JoinCompletionAction replayComplete,
            Map<String, Map<Integer, Pair<Long, Long>>> exportSequenceNumbers,
            Map<Integer, Long> drSequenceNumbers,
            Map<Integer, Map<Integer, Map<Integer, DRSiteDrIdTracker>>> allConsumerSiteTrackers,
            boolean requireExistingSequenceNumbers,
            long clusterCreateTime) {
        // transition from kStateRejoining to live rejoin replay.
        // pass through this transition in all cases; if not doing
        // live rejoin, will transfer to kStateRunning as usual
        // as the rejoin task log will be empty.
        assert(m_rejoinState == kStateRejoining);

        if (replayComplete == null) {
            throw new RuntimeException("Null Replay Complete Action.");
        }

        if (clusterCreateTime != -1) {
            VoltDB.instance().setClusterCreateTime(clusterCreateTime);
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
                if (requireExistingSequenceNumbers) {
                    VoltDB.crashLocalVoltDB(
                            "Could not find export sequence numbers for partition " +
                                    m_partitionId + " table " +
                                    tableEntry.getKey() + " have " + exportSequenceNumbers, false, null);
                } else {
                    sequenceNumbers = Pair.of(0L,0L);
                }
            }

            exportAction(
                    true,
                    sequenceNumbers.getFirst().longValue(),
                    sequenceNumbers.getSecond(),
                    m_partitionId,
                    catalogTable.getSignature());
        }

        if (drSequenceNumbers != null) {
            Long partitionDRSequenceNumber = drSequenceNumbers.get(m_partitionId);
            Long mpDRSequenceNumber = drSequenceNumbers.get(MpInitiator.MP_INIT_PID);
            hostLog.info("Setting drIds " + partitionDRSequenceNumber + " and " + mpDRSequenceNumber);
            setDRSequenceNumbers(partitionDRSequenceNumber, mpDRSequenceNumber);
            if (VoltDB.instance().getNodeDRGateway() != null && m_sysprocContext.isLowestSiteId()) {
                VoltDB.instance().getNodeDRGateway().cacheRejoinStartDRSNs(drSequenceNumbers);
            }
        } else if (requireExistingSequenceNumbers) {
            VoltDB.crashLocalVoltDB("Could not find DR sequence number for partition " + m_partitionId);
        }

        if (allConsumerSiteTrackers != null) {
            Map<Integer, Map<Integer, DRSiteDrIdTracker>> thisConsumerSiteTrackers =
                    allConsumerSiteTrackers.get(m_partitionId);
            if (thisConsumerSiteTrackers != null) {
                m_maxSeenDrLogsBySrcPartition = thisConsumerSiteTrackers;
            }
        }
        m_rejoinState = kStateReplayingRejoin;
        m_replayCompletionAction = replayComplete;
    }

    private void setReplayRejoinComplete() {
        // transition out of rejoin replay to normal running state.
        assert(m_rejoinState == kStateReplayingRejoin);
        m_replayCompletionAction.run();
        m_rejoinState = kStateRunning;
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
        return m_ee.executePlanFragments(
                numFragmentIds,
                planFragmentIds,
                inputDepIds,
                parameterSets,
                determinismHash,
                sqlTexts,
                isWriteFrags,
                sqlCRCs,
                txnId,
                spHandle,
                m_lastCommittedSpHandle,
                uniqueId,
                readOnly ? Long.MAX_VALUE : getNextUndoTokenBroken(),
                traceOn);
    }

    @Override
    public boolean usingFallbackBuffer() {
        return m_ee.usingFallbackBuffer();
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
    public boolean updateCatalog(String diffCmds,
                                 CatalogContext context,
                                 boolean requiresSnapshotIsolationboolean,
                                 boolean isMPI,
                                 long txnId,
                                 long uniqueId,
                                 long spHandle,
                                 boolean isReplay,
                                 boolean requireCatalogDiffCmdsApplyToEE,
                                 boolean requiresNewExportGeneration)
    {
        CatalogContext oldContext = m_context;
        m_context = context;
        m_ee.setBatchTimeout(m_context.cluster.getDeployment().get("deployment").
                getSystemsettings().get("systemsettings").getQuerytimeout());
        m_loadedProcedures.loadProcedures(m_context, isReplay);
        m_ee.loadFunctions(m_context);

        if (isMPI) {
            // the rest of the work applies to sites with real EEs
            return true;
        }

        if (requireCatalogDiffCmdsApplyToEE == false) {
            // empty diff cmds for the EE to apply, so skip the JNI call
            hostLog.debug("Skipped applying diff commands on EE.");
            return true;
        }

        CatalogMap<Table> tables = m_context.catalog.getClusters().get("cluster").getDatabases().get("database").getTables();

        boolean DRCatalogChange = false;
        for (Table t : tables) {
            if (t.getIsdred()) {
                DRCatalogChange |= diffCmds.contains("tables#" + t.getTypeName());
                if (DRCatalogChange) {
                    break;
                }
            }
        }

        if (!DRCatalogChange) { // Check against old catalog for deletions
            CatalogMap<Table> oldTables = oldContext.catalog.getClusters().get("cluster").getDatabases().get("database").getTables();
            for (Table t : oldTables) {
                if (t.getIsdred()) {
                    DRCatalogChange |= diffCmds.contains(CatalogDiffEngine.getDeleteDiffStatement(t, "tables"));
                    if (DRCatalogChange) {
                        break;
                    }
                }
            }
        }

        // if a snapshot is in process, wait for it to finish
        // don't bother if this isn't a schema change
        //
        if (requiresSnapshotIsolationboolean && m_snapshotter.isEESnapshotting()) {
            hostLog.info(String.format("Site %d performing schema change operation must block until snapshot is locally complete.",
                    CoreUtils.getSiteIdFromHSId(m_siteId)));
            try {
                m_snapshotter.completeSnapshotWork(m_sysprocContext);
                hostLog.info(String.format("Site %d locally finished snapshot. Will update catalog now.",
                        CoreUtils.getSiteIdFromHSId(m_siteId)));
            }
            catch (InterruptedException e) {
                VoltDB.crashLocalVoltDB("Unexpected Interrupted Exception while finishing a snapshot for a catalog update.", true, e);
            }
        }

        //Necessary to quiesce before updating the catalog
        //so export data for the old generation is pushed to Java.
        //No need to quiesce as there is no rolling of generation OLD datasources will be polled and pushed until there is no more data.
        //m_ee.quiesce(m_lastCommittedSpHandle);
        m_ee.updateCatalog(m_context.m_genId, requiresNewExportGeneration, diffCmds);
        if (DRCatalogChange) {
            final DRCatalogCommands catalogCommands = DRCatalogDiffEngine.serializeCatalogCommandsForDr(m_context.catalog, -1);
            generateDREvent(EventType.CATALOG_UPDATE, txnId, uniqueId, m_lastCommittedSpHandle,
                    spHandle, catalogCommands.commands.getBytes(Charsets.UTF_8));
        }

        return true;
    }

    /**
     * Update the system settings
     * @param context catalog context
     * @return true if it succeeds
     */
    public boolean updateSettings(CatalogContext context) {
        m_context = context;
        // here you could bring the timeout settings
        m_loadedProcedures.loadProcedures(m_context);
        m_ee.loadFunctions(m_context);
        return true;
    }

    @Override
    public void setPerPartitionTxnIds(long[] perPartitionTxnIds, boolean skipMultiPart) {
        long foundMultipartTxnId = -1;
        long foundSinglepartTxnId = -1;
        for (long txnId : perPartitionTxnIds) {
            if (TxnEgo.getPartitionId(txnId) == m_partitionId) {
                if (foundSinglepartTxnId != -1) {
                    VoltDB.crashLocalVoltDB(
                            "Found multiple transactions ids (" + TxnEgo.txnIdToString(txnId) + " and " +
                            TxnEgo.txnIdToString(foundSinglepartTxnId) + ")during restore for a partition",
                            false, null);
                }
                foundSinglepartTxnId = txnId;
                m_initiatorMailbox.setMaxLastSeenTxnId(txnId);
                setSpHandleForSnapshotDigest(txnId);
            }
            if (!skipMultiPart && TxnEgo.getPartitionId(txnId) == MpInitiator.MP_INIT_PID) {
                if (foundMultipartTxnId != -1) {
                    VoltDB.crashLocalVoltDB(
                            "Found multiple transactions ids (" + TxnEgo.txnIdToString(txnId) + " and " +
                            TxnEgo.txnIdToString(foundMultipartTxnId) + ") during restore for a multipart txnid",
                            false, null);
                }
                foundMultipartTxnId = txnId;
                m_initiatorMailbox.setMaxLastSeenMultipartTxnId(txnId);
            }
        }
        if (!skipMultiPart && foundMultipartTxnId == -1) {
            VoltDB.crashLocalVoltDB("Didn't find a multipart txnid on restore", false, null);
        }
    }

    public void setNumberOfPartitions(int partitionCount)
    {
        m_numberOfPartitions = partitionCount;
    }

    @Override
    public TheHashinator getCurrentHashinator() {
        return m_hashinator;
    }

    @Override
    public void updateHashinator(TheHashinator hashinator)
    {
        Preconditions.checkNotNull(hashinator);
        m_hashinator = hashinator;
        m_ee.updateHashinator(hashinator.pGetCurrentConfig());
    }

    /**
     * For the specified list of table ids, return the number of mispartitioned rows using
     * the provided hashinator config
     */
    @Override
    public long[] validatePartitioning(long[] tableIds, byte[] hashinatorConfig) {
        ByteBuffer paramBuffer = m_ee.getParamBufferForExecuteTask(4 + (8 * tableIds.length) + 4 + hashinatorConfig.length);
        paramBuffer.putInt(tableIds.length);
        for (long tableId : tableIds) {
            paramBuffer.putLong(tableId);
        }
        paramBuffer.put(hashinatorConfig);

        ByteBuffer resultBuffer = ByteBuffer.wrap(m_ee.executeTask( TaskType.VALIDATE_PARTITIONING, paramBuffer));
        long mispartitionedRows[] = new long[tableIds.length];
        for (int ii = 0; ii < tableIds.length; ii++) {
            mispartitionedRows[ii] = resultBuffer.getLong();
        }
        return mispartitionedRows;
    }

    @Override
    public void setBatch(int batchIndex) {
        m_ee.setBatch(batchIndex);
    }

    @Override
    public void setProcedureName(String procedureName) {
        m_ee.setProcedureName(procedureName);
    }

    @Override
    public void notifyOfSnapshotNonce(String nonce, long snapshotSpHandle) {
        m_initiatorMailbox.notifyOfSnapshotNonce(nonce, snapshotSpHandle);
    }

    @Override
    public long applyBinaryLog(long txnId, long spHandle,
                               long uniqueId, int remoteClusterId, int remotePartitionId,
                               byte log[]) throws EEException {
        ByteBuffer paramBuffer = m_ee.getParamBufferForExecuteTask(4 + log.length);
        paramBuffer.putInt(log.length);
        paramBuffer.put(log);
        return m_ee.applyBinaryLog(paramBuffer, txnId, spHandle, m_lastCommittedSpHandle, uniqueId,
                            remoteClusterId, remotePartitionId, getNextUndoToken(m_currentTxnId));
    }

    @Override
    public void setBatchTimeout(int batchTimeout) {
        m_ee.setBatchTimeout(batchTimeout);
    }

    @Override
    public int getBatchTimeout() {
        return m_ee.getBatchTimeout();
    }

    @Override
    public void setDRProtocolVersion(int drVersion) {
        ByteBuffer paramBuffer = m_ee.getParamBufferForExecuteTask(4);
        paramBuffer.putInt(drVersion);
        m_ee.executeTask(TaskType.SET_DR_PROTOCOL_VERSION, paramBuffer);
        hostLog.info("DR protocol version has been set to " + drVersion);
    }

    @Override
    public void setDRProtocolVersion(int drVersion, long txnId, long spHandle, long uniqueId) {
        setDRProtocolVersion(drVersion);
        generateDREvent(
                EventType.DR_STREAM_START, txnId, uniqueId, m_lastCommittedSpHandle, spHandle, new byte[0]);
    }

    @Override
    public void generateElasticChangeEvents(int oldPartitionCnt, int newPartitionCnt, long txnId, long spHandle, long uniqueId) {
//        Enable this code and fix up generateDREvent in DRTuplestream once the DR ReplicatedTable Stream has been removed
//        if (m_partitionId >= oldPartitionCnt) {
//            generateDREvent(
//                    EventType.DR_STREAM_START, uniqueId, m_lastCommittedSpHandle, spHandle, new byte[0]);
//        }
//        else {
            ByteBuffer paramBuffer = ByteBuffer.allocate(8);
            paramBuffer.putInt(oldPartitionCnt);
            paramBuffer.putInt(newPartitionCnt);
            generateDREvent(
                    EventType.DR_ELASTIC_CHANGE, txnId, uniqueId, m_lastCommittedSpHandle, spHandle, paramBuffer.array());
//        }
    }

    @Override
    public void generateElasticRebalanceEvents(int srcPartition, int destPartition, long txnId, long spHandle, long uniqueId) {
        ByteBuffer paramBuffer = ByteBuffer.allocate(8 + 8);
        paramBuffer.putInt(srcPartition);
        paramBuffer.putInt(destPartition);
        paramBuffer.putLong(uniqueId);
        generateDREvent(
                EventType.DR_ELASTIC_REBALANCE, txnId, uniqueId, m_lastCommittedSpHandle, spHandle, paramBuffer.array());
    }

    @Override
    public void setDRStreamEnd(long txnId, long spHandle, long uniqueId) {
        generateDREvent(
                EventType.DR_STREAM_END, txnId, uniqueId, m_lastCommittedSpHandle, spHandle, new byte[0]);
    }

    /**
     * Generate a in-stream DR event which pushes an event buffer to topend
     */
    public void generateDREvent(EventType type, long txnId, long uniqueId, long lastCommittedSpHandle,
            long spHandle, byte[] payloads) {
        m_ee.quiesce(lastCommittedSpHandle);
        ByteBuffer paramBuffer = m_ee.getParamBufferForExecuteTask(32 + 16 + payloads.length);
        paramBuffer.putInt(type.ordinal());
        paramBuffer.putLong(uniqueId);
        paramBuffer.putLong(lastCommittedSpHandle);
        paramBuffer.putLong(spHandle);
        // adding txnId and undoToken to make generateDREvent undoable
        paramBuffer.putLong(txnId);
        paramBuffer.putLong(getNextUndoToken(m_currentTxnId));
        paramBuffer.putInt(payloads.length);
        paramBuffer.put(payloads);
        m_ee.executeTask(TaskType.GENERATE_DR_EVENT, paramBuffer);
    }

    @Override
    public SystemProcedureExecutionContext getSystemProcedureExecutionContext() {
        return m_sysprocContext;
    }

    public ExecutionEngine getExecutionEngine() {
        return m_ee;
    }
}
