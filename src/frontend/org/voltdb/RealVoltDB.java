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

import static java.util.Objects.requireNonNull;
import static org.voltdb.VoltDB.exitAfterMessage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.time.Clock;
import java.time.Instant;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.aeonbits.owner.ConfigFactory;
import org.apache.cassandra_voltpatches.GCInspector;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.KeeperException.Code;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLog4jLogger;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.HostMessenger.HostInfo;
import org.voltcore.messaging.SiteFailureForwardMessage;
import org.voltcore.messaging.SiteMailbox;
import org.voltcore.messaging.SocketJoiner;
import org.voltcore.network.CipherExecutor;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.OnDemandBinaryLogger;
import org.voltcore.utils.Pair;
import org.voltcore.utils.ShutdownHooks;
import org.voltcore.utils.VersionChecker;
import org.voltcore.zk.CoreZK;
import org.voltcore.zk.ZKCountdownLatch;
import org.voltcore.zk.ZKUtil;
import org.voltcore.zk.ZKUtil.ZKCatalogStatus;
import org.voltdb.CatalogContext.CatalogInfo;
import org.voltdb.CatalogContext.CatalogJarWriteMode;
import org.voltdb.ProducerDRGateway.MeshMemberInfo;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltDB.UpdatableSiteCoordinationBarrier;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.common.NodeState;
import org.voltdb.compiler.AdHocCompilerCache;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.CommandLogType;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.compiler.deploymentfile.DrType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.SecurityType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.dr2.DRConsumerStatsBase;
import org.voltdb.dr2.conflicts.DRConflictsStats;
import org.voltdb.dr2.conflicts.DRConflictsTracker;
import org.voltdb.dtxn.InitiatorStats;
import org.voltdb.dtxn.LatencyHistogramStats;
import org.voltdb.dtxn.LatencyStats;
import org.voltdb.dtxn.LatencyUncompressedHistogramStats;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.elastic.BalancePartitionsStatistics;
import org.voltdb.elastic.ElasticService;
import org.voltdb.importer.ImportManager;
import org.voltdb.iv2.BaseInitiator;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.Initiator;
import org.voltdb.iv2.KSafetyStats;
import org.voltdb.iv2.LeaderAppointer;
import org.voltdb.iv2.MigratePartitionLeaderInfo;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.MpTerm;
import org.voltdb.iv2.RejoinProducer;
import org.voltdb.iv2.SpInitiator;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.iv2.TransactionTaskQueue;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.largequery.LargeBlockManager;
import org.voltdb.licensing.CommunityLicensing;
import org.voltdb.licensing.Licensing;
import org.voltdb.messaging.MigratePartitionLeaderMessage;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.modular.ModuleManager;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.probe.MeshProber;
import org.voltdb.processtools.ShellTools;
import org.voltdb.rejoin.Iv2RejoinCoordinator;
import org.voltdb.rejoin.JoinCoordinator;
import org.voltdb.serdes.AvroSerde;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.ClusterSettingsRef;
import org.voltdb.settings.DbSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.settings.Settings;
import org.voltdb.settings.SettingsException;
import org.voltdb.snmp.DummySnmpTrapSender;
import org.voltdb.snmp.FaultFacility;
import org.voltdb.snmp.FaultLevel;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.stats.GcStats;
import org.voltdb.stats.LimitsStats;
import org.voltdb.stats.LiveClientsStats;
import org.voltdb.sysprocs.AdHocNTBase;
import org.voltdb.sysprocs.VerifyCatalogAndWriteJar;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SnapshotUtil.Snapshot;
import org.voltdb.task.TaskManager;
import org.voltdb.task.TaskScope;
import org.voltdb.tasks.clockskew.ClockSkewCollectorScheduler;
import org.voltdb.tasks.clockskew.ClockSkewStats;
import org.voltdb.utils.CLibrary;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndDeployment;
import org.voltdb.utils.CatalogUtil.SegmentedCatalog;
import org.voltdb.utils.CustomProperties;
import org.voltdb.utils.FailedLoginCounter;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.InMemoryJarfile.JarLoader;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.ProClass;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.TopologyZKUtils;
import org.voltdb.utils.VoltSampler;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Suppliers;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.Lists;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Ordering;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.hash.Hashing;
import com.google_voltpatches.common.net.HostAndPort;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

import io.netty.handler.ssl.OpenSsl;

/**
 * RealVoltDB initializes global server components, like the messaging
 * layer, ExecutionSite(s), and ClientInterface. It provides accessors
 * or references to those global objects. It is basically the global
 * namespace. A lot of the global namespace is described by VoltDBInterface
 * to allow test mocking.
 */
public class RealVoltDB implements VoltDBInterface, RestoreAgent.Callback, HostMessenger.HostWatcher {

    private static final boolean DISABLE_JMX = Boolean.parseBoolean(System.getProperty("DISABLE_JMX", "true"));

    /** Default deployment file contents if path to deployment is null */
    private static final String[] defaultDeploymentXML = {
        "<?xml version=\"1.0\"?>",
        "<!-- This file is an auto-generated default deployment configuration. -->",
        "<deployment>",
        "    <cluster hostcount=\"1\" />",
        "    <httpd enabled=\"true\">",
        "        <jsonapi enabled=\"true\" />",
        "    </httpd>",
        "</deployment>"
    };

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");
    private VoltDB.Configuration m_config = new VoltDB.Configuration();
    private final Clock m_clock = Clock.systemUTC();
    int m_configuredNumberOfPartitions;
    int m_configuredReplicationFactor;
    // CatalogContext is immutable, just make sure that accessors see a consistent version
    volatile CatalogContext m_catalogContext;
    // Managed voltdb directories settings
    volatile NodeSettings m_nodeSettings;
    // Cluster settings reference and supplier
    final ClusterSettingsRef m_clusterSettings = new ClusterSettingsRef();
    private String m_buildString;
    static final String m_defaultVersionString = "11.4.0.beta1";
    // by default set the version to only be compatible with itself
    static final String m_defaultHotfixableRegexPattern = "^\\Q11.4.0.beta1\\E\\z";
    // these next two are non-static because they can be overrriden on the CLI for test
    private String m_versionString = m_defaultVersionString;
    private String m_hotfixableRegexPattern = m_defaultHotfixableRegexPattern;
    HostMessenger m_messenger = null;
    private ClientInterface m_clientInterface = null;
    HTTPAdminListener m_adminListener;
    private OpsRegistrar m_opsRegistrar = new OpsRegistrar();
    private final AtomicReference<MeshProber> m_meshProbe = new AtomicReference<>();

    private PartitionCountStats m_partitionCountStats = null;
    private IOStats m_ioStats = null;
    private MemoryStats m_memoryStats = null;
    private CpuStats m_cpuStats = null;
    private GcStats m_gcStats = null;
    private CommandLogStats m_commandLogStats = null;
    private DRRoleStats m_drRoleStats = null;
    private ClockSkewStats skewStats = null;
    private StatsManager m_statsManager = null;
    private SnapshotCompletionMonitor m_snapshotCompletionMonitor;
    // These are unused locally, but they need to be registered with the StatsAgent so they're
    // globally available
    @SuppressWarnings("unused")
    private InitiatorStats m_initiatorStats;
    private LiveClientsStats m_liveClientsStats = null;
    int m_myHostId = -1; // not valid until we have a mesh
    String m_httpPortExtraLogMessage = null;
    boolean m_jsonEnabled;

    // IV2 things
    TreeMap<Integer, Initiator> m_iv2Initiators = new TreeMap<>();
    Cartographer m_cartographer = null;
    Supplier<Boolean> m_partitionZeroLeader = null;
    LeaderAppointer m_leaderAppointer = null;
    GlobalServiceElector m_globalServiceElector = null;
    MpInitiator m_MPI = null;
    Map<Integer, Long> m_iv2InitiatorStartingTxnIds = new HashMap<>();
    private ScheduledFuture<?> resMonitorWork;
    private HealthMonitor m_healthMonitor;

    private final FailedLoginCounter m_flc = new FailedLoginCounter();

    // State tracking
    private NodeStateTracker m_statusTracker;
    private int m_voltPid;

    // Should the execution sites be started in recovery mode
    // (used for joining a node to an existing cluster)
    // If CL is enabled this will be set to true
    // by the CL when the truncation snapshot completes
    // and this node is viable for replay
    volatile boolean m_rejoining = false;
    // Need to separate the concepts of rejoin data transfer and rejoin
    // completion.  This boolean tracks whether or not the data transfer
    // process is done.  CL truncation snapshots will not flip the all-complete
    // boolean until no mode data is pending.
    // Yes, this is fragile having two booleans.  We could aggregate them into
    // some rejoining state enum at some point.
    volatile boolean m_rejoinDataPending = false;
    // Since m_rejoinDataPending is set asynchronously, sites could have inconsistent
    // view of what the value is during the execution of a sysproc. Use this and
    // m_safeMpTxnId to prevent the race. The m_safeMpTxnId is updated once in the
    // lifetime of the node to reflect the first MP txn that witnessed the flip of
    // m_rejoinDataPending.

    CountDownLatch m_meshDeterminationLatch = new CountDownLatch(1);
    private final Object m_safeMpTxnIdLock = new Object();
    private long m_lastSeenMpTxnId = Long.MIN_VALUE;
    private long m_safeMpTxnId = Long.MAX_VALUE;
    String m_rejoinTruncationReqId = null;

    // Are we adding the node to the cluster instead of rejoining?
    volatile boolean m_joining = false;
    private boolean m_preparingShuttingdown = false;

    long m_clusterCreateTime;
    private Instant m_hostStartTime;
    AtomicBoolean m_replicationActive = new AtomicBoolean(false);
    private ProducerDRGateway m_producerDRGateway = null;
    private ConsumerDRGateway m_consumerDRGateway = null;
    // Separate class to manage dr catalog commands, which are needed during recovery
    private final DrProducerCatalogCommands m_drCatalogCommands = new DrProducerCatalogCommands();
    private final DRConflictsTracker m_drConflictsTracker = new DRConflictsTracker(m_clock);

    //Only restrict recovery completion during test
    static Semaphore m_testBlockRecoveryCompletion = new Semaphore(Integer.MAX_VALUE);
    private long m_executionSiteRecoveryFinish;
    private long m_executionSiteRecoveryTransferred;

    // Rejoin coordinator
    private JoinCoordinator m_joinCoordinator = null;
    private ElasticService m_elasticService = null;

    // Scheduler manager
    private TaskManager m_taskManager = null;

    // Snapshot IO agent
    private SnapshotIOAgent m_snapshotIOAgent = null;

    // id of the leader, or the host restore planner says has the catalog
    int m_hostIdWithStartupCatalog;
    String m_pathToStartupCatalog;

    // Synchronize initialize and shutdown
    private final Object m_startAndStopLock = new Object();

    // add a random number to the sampler output to make it likely to be unique for this process.
    private final VoltSampler m_sampler = new VoltSampler(10, "sample" + String.valueOf(new Random().nextInt() % 10000) + ".txt");
    private final AtomicBoolean m_hasStartedSampler = new AtomicBoolean(false);

    RestoreAgent m_restoreAgent = null;

    private final ListeningExecutorService m_es = CoreUtils.getCachedSingleThreadExecutor("StartAction ZK Watcher", 15000);
    private final ListeningExecutorService m_failedHostExecutorService = CoreUtils.getCachedSingleThreadExecutor("Failed Host monitor", 15000);
    private volatile boolean m_isRunning = false;
    private boolean m_isRunningWithOldVerb = true;
    private boolean m_isBare = false;
    /** Last transaction ID at which the logging config updated.
     * Also, use the intrinsic lock to safeguard access from multiple
     * execution site threads */
    private Long m_lastLogUpdateTxnId = 0L;
    private final CopyOnWriteArrayList<CatalogValidator> m_catalogValidators = new CopyOnWriteArrayList<>();

    /**
     * Startup snapshot nonce taken on shutdown --save
     */
    String m_terminusNonce = null;

    // m_durable means commandlogging is enabled.
    boolean m_durable = false;

    private int m_maxThreadsCount;

    // Using Boolean so if this is accessed before assignment the caller will get an NPE
    private Boolean m_eligibleAsLeader = null;

    // Single avro serde configured by deployment file
    private final AvroSerde m_avroSerde = new AvroSerde();

    @Override
    public boolean isRunningWithOldVerbs() {
        return m_isRunningWithOldVerb;
     }

    @Override
    public boolean isPreparingShuttingdown() {
        return m_preparingShuttingdown;
    }
    @Override
    public void setShuttingdown(boolean preparingShuttingdown) {
        consoleLog.info("Preparing to shut down: " + preparingShuttingdown);
        m_preparingShuttingdown = preparingShuttingdown;
    }

    @Override
    public boolean rejoining() {
        return m_rejoining;
    }

    @Override
    public boolean rejoinDataPending() {
        return m_rejoinDataPending;
    }

    @Override
    public boolean isMpSysprocSafeToExecute(long txnId)
    {
        synchronized (m_safeMpTxnIdLock) {
            if (txnId >= m_safeMpTxnId) {
                return true;
            }

            if (txnId > m_lastSeenMpTxnId) {
                m_lastSeenMpTxnId = txnId;
                if (!rejoinDataPending() && m_safeMpTxnId == Long.MAX_VALUE) {
                    m_safeMpTxnId = txnId;
                }
            }

            return txnId >= m_safeMpTxnId;
        }
    }

    @Override
    public StartAction getStartAction() {
        return m_config.m_startAction;
    }

    private long m_recoveryStartTime;

    CommandLog m_commandLog;
    SnmpTrapSender m_snmp;

    private volatile OperationMode m_mode = OperationMode.INITIALIZING;

    private volatile boolean m_isMasterOnly = false;

    private volatile OperationMode m_startMode = OperationMode.RUNNING;

    volatile String m_localMetadata = "";

    private ListeningExecutorService m_computationService;

    private Thread m_configLogger;

    // Hooks for kubernetes operator support functions in Enterprise edition
    private OperatorSupport m_operatorSupport = new OperatorSupport();

    private void connectOperatorSupport() {
        OperatorSupport opSupp = ProClass.newInstanceOf("org.voltdb.operator.OperatorSupportImpl",
                                                        "operator", ProClass.HANDLER_IGNORE);
        if (opSupp != null) {
            m_operatorSupport = opSupp;
        }
    }

    // methods accessed via the singleton
    @Override
    public void startSampler() {
        if (m_hasStartedSampler.compareAndSet(false, true)) {
            m_sampler.start();
        }
    }

    private ScheduledThreadPoolExecutor m_periodicWorkThread;
    private ScheduledThreadPoolExecutor m_periodicPriorityWorkThread;

    // Interface to all things license-related
    private final Licensing m_licensing = new CommunityLicensing();

    private void connectLicensing() {
        ProClass.newInstanceOf("org.voltdb.licensing.Initializer",
                               "licensing", ProClass.HANDLER_IGNORE);
    }

    private LatencyStats m_latencyStats;
    private LatencyHistogramStats m_latencyCompressedStats;
    private LatencyUncompressedHistogramStats m_latencyHistogramStats;

    private File getConfigDirectory() {
        return getConfigDirectory(m_config);
    }

    private File getConfigDirectory(Configuration config) {
        return getConfigDirectory(config.m_voltdbRoot);
    }

    private File getConfigDirectory(File voltdbroot) {
        return new File(voltdbroot, Constants.CONFIG_DIR);
    }

    private File getConfigLogDeployment() {
        return getConfigLogDeployment(m_config);
    }

    private File getConfigLogDeployment(Configuration config) {
        return new File(getConfigDirectory(config), "deployment.xml");
    }

    @Override
    public Licensing getLicensing() {
        return m_licensing;
    }

    @Override
    public String getVoltDBRootPath(PathsType.Voltdbroot path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return getVoltDBRootPath();
    }

    @Override
    public String getCommandLogPath(PathsType.Commandlog path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getCommandLog()).getPath();
    }

    @Override
    public String getCommandLogSnapshotPath(PathsType.Commandlogsnapshot path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getCommandLogSnapshot()).getPath();
    }

    @Override
    public String getSnapshotPath(PathsType.Snapshots path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getSnapshot()).getPath();
    }

    @Override
    public String getExportOverflowPath(PathsType.Exportoverflow path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getExportOverflow()).getPath();
    }

    @Override
    public String getDROverflowPath(PathsType.Droverflow path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getDROverflow()).getPath();
    }

    @Override
    public String getLargeQuerySwapPath(PathsType.Largequeryswap path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getLargeQuerySwap()).getPath();
    }

    @Override
    public String getExportCursorPath(PathsType.Exportcursor path) {
        if (isRunningWithOldVerbs()) {
            return path.getPath();
        }
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getExportCursor()).getPath();
    }

    @Override
    public String getVoltDBRootPath() {
        File root = null;
        try {
            root = m_nodeSettings.getVoltDBRoot();
            return root.getCanonicalPath();
        } catch (IOException e) {
            throw new SettingsException(String.format("Failed to canonicalize: %s. Reason: %s",
                                                      root, e.getMessage()));
        }
    }

    @Override
    public String getCommandLogPath() {
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getCommandLog()).getPath();
    }

    @Override
    public String getCommandLogSnapshotPath() {
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getCommandLogSnapshot()).getPath();
    }

    @Override
    public String getSnapshotPath() {
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getSnapshot()).getPath();
    }

    @Override
    public File getExportOverflowPath() {
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getExportOverflow());
    }

    @Override
    public String getDROverflowPath() {
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getDROverflow()).getPath();
    }

    @Override
    public String getLargeQuerySwapPath() {
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getLargeQuerySwap()).getPath();
    }

    @Override
    public String getExportCursorPath() {
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getExportCursor()).getPath();
    }

    @Override
    public File getTopicsDataPath() {
        return m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getTopicsData());
    }

    public static String getStagedCatalogPath(String voltDbRoot) {
        return voltDbRoot + File.separator + CatalogUtil.STAGED_CATALOG_PATH;
    }

    private static String[] ignoredFilenames = { "lost+found" };

    private String managedPathEmptyCheck(String voltDbRoot, String path) {
        File managedPath;
        if (new File(path).isAbsolute()) {
            managedPath = new File(path);
        } else {
            managedPath = new File(voltDbRoot, path);
        }
        if (!managedPath.exists()) {
            return null; // if it does not exist, there's nothing in it
        }
        String absPath = managedPath.getAbsolutePath();
        if (!managedPath.canRead()) { // can't read? assume empty but note in log
            hostLog.warnFmt("Cannot read directory '%s'", absPath);
            return null;
        }
        Collection<String> ignorable = Arrays.asList(ignoredFilenames); // HashSet<> is overkill
        String[] content = managedPath.list((dir,name) -> !ignorable.contains(name));
        if (content.length == 0) { // nothing we care about
            return null;
        }
        // Directory contains files we should not ignore
        StringBuilder sb = new StringBuilder(256);
        sb.append("Directory '").append(absPath).append("' contains:");
        for (int i=0; i<10 && i<content.length; i++) {
            sb.append(' ').append(content[i]);
        }
        if (content.length > 10) {
            sb.append(" ...");
        }
        hostLog.warn(sb.toString());
        return absPath;
    }

    private void managedPathsEmptyCheck(Configuration config) {
        List<String> nonEmptyPaths = managedPathsWithFiles(config, m_catalogContext.getDeployment());
        if (!nonEmptyPaths.isEmpty()) {
            StringBuilder crashMessage =
                    new StringBuilder("Files from a previous database session exist in the managed directories:");
            for (String nonEmptyPath : nonEmptyPaths) {
                crashMessage.append("\n  - " + nonEmptyPath);
            }
            if (config.m_startAction.isLegacy()) {
                crashMessage.append("\nUse the recover command to restore the previous database or use create --force" +
                    " to start a new database session overwriting existing files.");
            } else {
                crashMessage.append("\nUse start to restore the previous database or use init --force" +
                    " to start a new database session overwriting existing files.");
            }
            VoltDB.crashLocalVoltDB(crashMessage.toString());
        }
    }

    private List<String> managedPathsWithFiles(Configuration config, DeploymentType deployment) {
        ImmutableList.Builder<String> nonEmptyPaths = ImmutableList.builder();
        PathsType paths = deployment.getPaths();
        String voltDbRoot = getVoltDBRootPath(paths.getVoltdbroot());
        String path;

        if (!config.m_isEnterprise) {
            return nonEmptyPaths.build();
        }
        if ((path = managedPathEmptyCheck(voltDbRoot, getSnapshotPath(paths.getSnapshots()))) != null) {
            nonEmptyPaths.add(path);
        }
        if ((path = managedPathEmptyCheck(voltDbRoot, getExportOverflowPath(paths.getExportoverflow()))) != null) {
            nonEmptyPaths.add(path);
        }
        if ((path = managedPathEmptyCheck(voltDbRoot, getDROverflowPath(paths.getDroverflow()))) != null) {
            nonEmptyPaths.add(path);
        }
        if ((path = managedPathEmptyCheck(voltDbRoot, getCommandLogPath(paths.getCommandlog()))) != null) {
            nonEmptyPaths.add(path);
        }
        if ((path = managedPathEmptyCheck(voltDbRoot, getCommandLogSnapshotPath(paths.getCommandlogsnapshot()))) != null) {
            nonEmptyPaths.add(path);
        }
        if ((path = managedPathEmptyCheck(voltDbRoot, getTopicsDataPath().getPath())) != null) {
            nonEmptyPaths.add(path);
        }
        return nonEmptyPaths.build();
    }

    private List<String> pathsWithRecoverableArtifacts(DeploymentType deployment) {
        ImmutableList.Builder<String> nonEmptyPaths = ImmutableList.builder();
        PathsType paths = deployment.getPaths();
        String voltDbRoot = getVoltDBRootPath(paths.getVoltdbroot());
        String path;
        if ((path = managedPathEmptyCheck(voltDbRoot, getSnapshotPath(paths.getSnapshots()))) != null) {
            nonEmptyPaths.add(path);
        }
        if ((path = managedPathEmptyCheck(voltDbRoot, getCommandLogPath(paths.getCommandlog()))) != null) {
            nonEmptyPaths.add(path);
        }
        if ((path = managedPathEmptyCheck(voltDbRoot, getCommandLogSnapshotPath(paths.getCommandlogsnapshot()))) != null) {
            nonEmptyPaths.add(path);
        }
        return nonEmptyPaths.build();
    }

    private boolean checkExistence(Configuration config, String artifact) {
        if ((new File(config.m_getOutput)).exists() && !config.m_forceGetCreate) {
            consoleLog.fatal("Failed to save " + artifact + ", file already exists: " + config.m_getOutput);
            return true;
        }
        return false;
    }

    private int outputDeployment(Configuration config) {
        try {
            File configInfoDir = new File(config.m_voltdbRoot, Constants.CONFIG_DIR);
            File depFH = new File(configInfoDir, "deployment.xml");
            if (!depFH.isFile() || !depFH.canRead()) {
                consoleLog.fatal("Failed to get configuration or deployment configuration is invalid. "
                        + depFH.getAbsolutePath());
                return -1;
            }
            config.m_pathToDeployment = depFH.getCanonicalPath();
        } catch (IOException e) {
            consoleLog.fatal("Failed to read deployment: " + e.getMessage());
            return -1;
        }

        ReadDeploymentResults readDepl = readPrimedDeployment(config);
        try {
            DeploymentType dt = CatalogUtil.updateRuntimeDeploymentPaths(readDepl.deployment);
            // We don't have catalog context so host count is not there.
            String out;
            if ((out = CatalogUtil.getDeployment(dt, true)) != null) {
                if (config.m_getOutput.equals("-")) {
                    System.out.println(out);
                } else {
                    if (checkExistence(config, "deployment")) {
                        return -1;
                    }
                    try (FileOutputStream fos = new FileOutputStream(config.m_getOutput)){
                        fos.write(out.getBytes());
                    } catch (IOException e) {
                        consoleLog.fatal("Failed to write deployment to " + config.m_getOutput
                                + " : " + e.getMessage());
                        return -1;
                    }
                    consoleLog.info("Deployment configuration saved in " + config.m_getOutput);
                }
            } else {
                consoleLog.fatal("Failed to get configuration or deployment configuration is invalid.");
                return -1;
            }
        } catch (Exception e) {
            consoleLog.fatal("Failed to get configuration or deployment configuration is invalid. "
                    + "Please make sure voltdbroot is a valid directory. " + e.getMessage());
            return -1;
        }
        return 0;
    }

    private int outputSchema(Configuration config) {
        try {
            InMemoryJarfile catalogJar = CatalogUtil.loadInMemoryJarFile(MiscUtils.fileToBytes(new File (config.m_pathToCatalog)));
            String ddl = CatalogUtil.getAutoGenDDLFromJar(catalogJar);
            if (config.m_getOutput.equals("-")) {
                System.out.println(ddl);
            } else {
                if (checkExistence(config, "schema")) {
                    return -1;
                }
                try (FileOutputStream fos = new FileOutputStream(config.m_getOutput)){
                    fos.write(ddl.getBytes());
                } catch (IOException e) {
                    consoleLog.fatal("Failed to write schema to " + config.m_getOutput + " : " + e.getMessage());
                    return -1;
                }
                consoleLog.info("Schema saved in " + config.m_getOutput);
            }
        } catch (IOException e) {
            consoleLog.fatal("Failed to load the catalog jar from " + config.m_pathToCatalog
                    + " : " + e.getMessage());
            return -1;
        }
        return 0;
    }

    private int outputProcedures(Configuration config) {
        try {
            InMemoryJarfile catalogJar = CatalogUtil.loadInMemoryJarFile(MiscUtils.fileToBytes(new File (config.m_pathToCatalog)));
            InMemoryJarfile filteredJar = CatalogUtil.getCatalogJarWithoutDefaultArtifacts(catalogJar);
            if (config.m_getOutput.equals("-")) {
                filteredJar.writeToStdout();
            } else {
                if (checkExistence(config, "classes")) {
                    return -1;
                }
                File outputFile = new File(config.m_getOutput);
                filteredJar.writeToFile(outputFile);
                consoleLog.info("Classes saved in " + outputFile.getPath());
            }
        } catch (IOException e) {
            consoleLog.fatal("Failed to read classes " + config.m_pathToCatalog
                    + " : " + e.getMessage());
            return -1;
        }
        return 0;
    }


    @Override
    public void cli(Configuration config) {
        if (config.m_startAction != StartAction.GET) {
            System.err.println("This can only be called for GET action.");
            VoltDB.exit(-1);
        }

        if (!config.m_voltdbRoot.exists() || !config.m_voltdbRoot.canRead() || !config.m_voltdbRoot.canExecute() || !config.m_voltdbRoot.isDirectory()) {
            try {
                System.err.println("FATAL: Invalid Voltdbroot directory: " + config.m_voltdbRoot.getCanonicalPath());
            } catch (IOException ex) {
                //Ignore;
            }
            VoltDB.exit(-1);
        }

        // Handle multiple invocations of server thread in the same JVM.
        // by clearing static variables/properties which ModuleManager,
        // and Settings depend on
        ConfigFactory.clearProperty(Settings.CONFIG_DIR);
        int returnStatus = -1;
        switch (config.m_getOption) {
            case DEPLOYMENT:
                returnStatus = outputDeployment(config);
                break;
            case SCHEMA:
                returnStatus = outputSchema(config);
                break;
            case CLASSES:
                returnStatus = outputProcedures(config);
                break;
            case LICENSE:
                connectLicensing();
                returnStatus = m_licensing.outputLicense(config) ? 0 : -1;
                break;
        }
        VoltDB.exit(returnStatus);
    }

    /**
     * VoltDB initialization (not to be confused with the 'voltdb init' command).
     *
     * Handles global initialization of the process, and is used for the 'init'
     * and 'start' commands, which are INITIALIZE and PROBE in the StartAction
     * enumeration.
     *
     * You can't get here with any other start action using the supported
     * CLI ('voltdb' program). However, legacy actions can for the moment
     * still be set by starting VoltDB directly, as is done in many tests.
     * As a temporary sop to testing, we allow CREATE and RECOVER start
     * actions. Other legacy actions are rejected, there's no known test
     * using them.
     *
     * Note that this routine exits the process on successful completion of
     * INITIALIZE without PROBE, and may exit on certain errors.
     * TODO: clean up that mess and get some consistency.
     *
     * @param config VoltDB configuration
     */
    @Override
    public void initialize(Configuration config) {
        if (!System.getProperty("java.vm.name").contains("64")) {
            hostLog.fatal("You are running on an unsupported (probably 32 bit) JVM. Exiting.");
            VoltDB.exit(-1);
        }

        if (config.m_startAction == null) {
            hostLog.fatal("RealVoltDB: start action not set");
            VoltDB.exit(-1);
        }

        m_isRunningWithOldVerb = config.m_startAction.isLegacy();
        if (m_isRunningWithOldVerb) {
            String testing = CoreUtils.isJunitTest() ? "unit test" : "not unit test";
            switch (config.m_startAction) {
            case CREATE:
            case RECOVER:
            case SAFE_RECOVER:
                hostLog.warnFmt("RealvoltDB: running with legacy start action %s, %s", config.m_startAction, testing);
                break;
            default:
                hostLog.fatalFmt("RealVoltDB: unsupported legacy start action %s, %s", config.m_startAction, testing);
                VoltDB.exit(-1);
            }
        }

        int myPid = CLibrary.getpid();
        ShutdownHooks.enableServerStopLogging();

        synchronized(m_startAndStopLock) {
            exitAfterMessage = false;

            // Handle multiple invocations of server thread in the same JVM.
            // by clearing static variables/properties which ModuleManager,
            // and Settings depend on
            ConfigFactory.clearProperty(Settings.CONFIG_DIR);
            ModuleManager.resetCacheRoot();
            CipherExecutor.SERVER.shutdown();
            CipherExecutor.CLIENT.shutdown();

            // Node state is INITIALIZING
            m_statusTracker = new NodeStateTracker();
            m_voltPid = myPid;

            // Print the startup banner, but only on actual start
            if (config.m_startAction != StartAction.INITIALIZE) {
                CustomProperties props = new CustomProperties();
                consoleLog.info(props.get("host_VoltDB_StartupString", "VOLT ACTIVE DATA"));
                hostLog.info("Initializing VoltDB ...");
                hostLog.infoFmt("PID of this Volt process is %d", myPid);
            }

            // Read and print build info on the console
            readBuildInfo();

            // Check license availability
            connectLicensing();
            m_licensing.readLicenseFile(config);

            // By default, omit the wall of text in unit tests
            if (CoreUtils.isJunitTest() && System.getenv("FULL_CMDLINE_LOGGING") == null) {
                hostLog.info("Declining to log command line arguments in unit test");
            }

            else {
                // Log some facts about the user account: account name,
                // home dir, working dir where Java was started
                hostLog.infoFmt("User properties: user.name '%s' user.home '%s' user.dir '%s'",
                                System.getProperty("user.name"),
                                System.getProperty("user.home"),
                                System.getProperty("user.dir"));

                // Warn if user is named "root"
                if (System.getProperty("user.name").equals("root")) {
                    hostLog.warn("VoltDB is running as root. " +
                                 "Running the VoltDB server software from the system root account is not recommended.");
                }

                // Replay command line args that we can see
                StringBuilder sb = new StringBuilder(2048).append("Command line arguments: ");
                sb.append(System.getProperty("sun.java.command", "[not available]"));
                hostLog.info(sb.toString());

                List<String> iargs = ManagementFactory.getRuntimeMXBean().getInputArguments();
                sb.delete(0, sb.length());
                sb.append("Command line JVM arguments:");
                for (String iarg : iargs) {
                    sb.append(" ").append(iarg);
                }
                if (iargs.size() > 0) {
                    hostLog.info(sb.toString());
                } else {
                    hostLog.info("No JVM command line args known.");
                }

                // And the classpath
                sb.delete(0, sb.length());
                sb.append("Command line JVM classpath: ");
                sb.append(System.getProperty("java.class.path", "[not available]"));
                hostLog.info(sb.toString());
            }

            if (config.m_startAction == StartAction.INITIALIZE) {
                if (config.m_forceVoltdbCreate) {
                    deleteInitializationMarkers(config);
                }
            }

            // Start the listener that responds to "status" requests. This needs
            // to be started "early" to be available during initialization.
            else {
                connectOperatorSupport();
                if (config.m_statusPort != VoltDB.DISABLED_PORT) {
                    m_operatorSupport.startStatusListener(config);
                }
            }

            // If there's no deployment provide a default and put it under voltdbroot.
            if (config.m_pathToDeployment == null) {
                try {
                    config.m_pathToDeployment = setupDefaultDeployment(hostLog, config.m_voltdbRoot);
                    config.m_deploymentDefault = true;
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Failed to write default deployment.");
                    return;
                }
            }

            ReadDeploymentResults readDepl = readPrimedDeployment(config);

            // stage deployment, license, schema, and hidden initialization marker file
            // under voltdbroot
            if (config.m_startAction == StartAction.INITIALIZE) {
                if (config.m_forceVoltdbCreate) {
                    SnapshotArchiver.archiveSnapshotDirectory(getSnapshotPath(), config.m_snapArchiveRetainCount);
                    m_nodeSettings.clean();
                }
                DrType dr = readDepl.deployment.getDr();
                if (dr != null && DrRoleType.XDCR.equals(dr.getRole())) {
                    // add default export configuration to DR conflict table
                    String retention = CatalogUtil.checkDrRetention(dr);
                    CatalogUtil.addExportConfigToDRConflictsTable(readDepl.deployment.getExport(), retention);
                }
                stageDeploymentFileForInitialize(config, readDepl.deployment);
                m_licensing.stageLicenseFile();
                stageSchemaFiles(config,
                        readDepl.deployment.getDr() != null &&
                                DrRoleType.XDCR.equals(readDepl.deployment.getDr().getRole()));
                stageInitializedMarker(config);
                hostLog.info("Initialized VoltDB root directory " + config.m_voltdbRoot.getPath());
                consoleLog.info("Initialized VoltDB root directory " + config.m_voltdbRoot.getPath());
                VoltDB.exit(0);
            }

            // Command is not INITIALIZE
            m_licensing.stageLicenseFile();

            final File stagedCatalogLocation = new File(
                    RealVoltDB.getStagedCatalogPath(config.m_voltdbRoot.getAbsolutePath()));

            if (config.m_startAction.isLegacy()) {
                File rootFH = CatalogUtil.getVoltDbRoot(readDepl.deployment.getPaths());
                File inzFH = new File(rootFH, VoltDB.INITIALIZED_MARKER);
                if (inzFH.exists()) {
                    VoltDB.crashLocalVoltDB("Cannot use legacy start action "
                            + config.m_startAction + " on voltdbroot "
                            + rootFH + " that was initialized with the init command");
                    return;
                }
                //Case where you give primed deployment with -d look in ../../ for initialized marker.
                //Also check if parents are config and voltdbroot
                File cfile = (new File(config.m_pathToDeployment)).getParentFile();
                if (cfile != null) {
                    rootFH = cfile.getParentFile();
                    if ("config".equals(cfile.getName()) && VoltDB.DBROOT.equals(rootFH.getName())) {
                        inzFH = new File(rootFH, VoltDB.INITIALIZED_MARKER);
                        if (inzFH.exists()) {
                            VoltDB.crashLocalVoltDB("Can not use legacy start action "
                                    + config.m_startAction + " on voltdbroot "
                                    + rootFH + " that was initialized with the init command");
                            return;
                        }
                    }
                }
                if (stagedCatalogLocation.isFile()) {
                    hostLog.warn("Initialized schema is present, but is being ignored and may be removed.");
                }
            } else {
                assert (config.m_startAction == StartAction.PROBE);
                if (stagedCatalogLocation.isFile()) {
                    assert (config.m_pathToCatalog == null) : config.m_pathToCatalog;
                    config.m_pathToCatalog = stagedCatalogLocation.getAbsolutePath();
                }
            }

            List<String> failed = m_nodeSettings.ensureDirectoriesExist();
            if (!failed.isEmpty()) {
                String msg = "Unable to access or create the following directories:\n  - " +
                        Joiner.on("\n  - ").join(failed);
                VoltDB.crashLocalVoltDB(msg);
                return;
            }

            if (config.m_hostCount == VoltDB.UNDEFINED) {
                config.m_hostCount = readDepl.deployment.getCluster().getHostcount();
            }

            // set the mode first thing
            m_mode = OperationMode.INITIALIZING;
            m_config = config;
            m_startMode = OperationMode.RUNNING;

            // set a bunch of things to null/empty/new for tests
            // which reuse the process
            m_safeMpTxnId = Long.MAX_VALUE;
            m_lastSeenMpTxnId = Long.MIN_VALUE;
            m_clientInterface = null;
            m_adminListener = null;
            m_commandLog = new DummyCommandLog();
            m_snmp = new DummySnmpTrapSender();
            m_messenger = null;
            m_opsRegistrar = new OpsRegistrar();
            m_snapshotCompletionMonitor = null;
            m_catalogContext = null;
            m_partitionCountStats = null;
            m_ioStats = null;
            m_memoryStats = null;
            m_commandLogStats = null;
            m_statsManager = null;
            m_restoreAgent = null;
            m_recoveryStartTime = System.currentTimeMillis();
            m_hostIdWithStartupCatalog = 0;
            m_pathToStartupCatalog = m_config.m_pathToCatalog;
            m_replicationActive = new AtomicBoolean(false);
            m_configLogger = null;
            m_hostStartTime = m_clock.instant();
            ActivePlanRepository.clear();
            updateMaxThreadsLimit();

            // set up site structure
            final int computationThreads = Math.max(2, CoreUtils.availableProcessors() / 4);
            m_computationService =
                    CoreUtils.getListeningExecutorService(
                            "Computation service thread",
                            computationThreads, m_config.m_computationCoreBindings);

            // Set std-out/err to use the UTF-8 encoding and fail if UTF-8 isn't supported
            try {
                System.setOut(new PrintStream(System.out, true, "UTF-8"));
                System.setErr(new PrintStream(System.err, true, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                hostLog.fatal("Support for the UTF-8 encoding is required for VoltDB. This means you are likely running an unsupported JVM. Exiting.");
                VoltDB.exit(-1);
            }

            m_snapshotCompletionMonitor = new SnapshotCompletionMonitor();

            // use CLI overrides for testing hotfix version compatibility
            if (m_config.m_versionStringOverrideForTest != null) {
                m_versionString = m_config.m_versionStringOverrideForTest;
            }
            if (m_config.m_versionCompatibilityRegexOverrideForTest != null) {
                m_hotfixableRegexPattern = m_config.m_versionCompatibilityRegexOverrideForTest;
            }
            if (m_config.m_buildStringOverrideForTest != null) {
                m_buildString = m_config.m_buildStringOverrideForTest;
            }
            // Prime cluster settings from configuration parameters
            // evaluate properties with the following sources in terms of priority
            // 1) properties from command line options
            // 2) properties from the cluster.properties files
            // 3) properties from the deployment file

            // this reads the file config/cluster.properties
            ClusterSettings fromPropertyFile = ClusterSettings.create();
            // handle case we recover clusters that were elastically expanded
            if (m_config.m_startAction.doesRecover()) {
                m_config.m_hostCount = fromPropertyFile.hostcount();
            }
            if (m_config.m_restorePlacement && !StringUtils.isEmpty(fromPropertyFile.partitionIds())) {
                m_config.m_recoveredPartitions = fromPropertyFile.partitionIds();
            }

            Map<String, String> fromCommandLine = m_config.asClusterSettingsMap();
            Map<String, String> fromDeploymentFile = CatalogUtil.
                    asClusterSettingsMap(readDepl.deployment);

            ClusterSettings clusterSettings = ClusterSettings.create(
                    fromCommandLine, fromPropertyFile.asMap(), fromDeploymentFile);

            clusterSettings.store();
            m_clusterSettings.set(clusterSettings, 1);

            // Before building the mesh, clean up any lingering snapshots left by elastic operations
            cleanElasticSnapshots(readDepl);

            // Potential wait here as mesh is built.
            MeshProber.Determination determination = buildClusterMesh(readDepl);
            if (m_config.m_startAction == StartAction.PROBE) {
                String action = "Starting a new database cluster";
                if (determination.startAction.doesRejoin()) {
                    action = "Rejoining a running cluster";
                } else if (determination.startAction == StartAction.JOIN) {
                    action = "Adding this node to a running cluster";
                } else if (determination.startAction.doesRecover()) {
                    action = "Restarting the database cluster from the command logs";
                }
                hostLog.info(action);
                consoleLog.info(action);
            }

            m_config.m_startAction = determination.startAction;
            m_config.m_hostCount = determination.hostCount;
            m_rejoining = m_config.m_startAction.doesRejoin();
            if (!m_rejoining) {
                assert (determination.paused || !m_config.m_isPaused);
                m_config.m_isPaused = determination.paused;
            } else if (m_config.m_isPaused) {
                m_config.m_isPaused = false;
                m_messenger.unpause();
                hostLog.info("Rejoining a running cluster, ignore paused mode");
            }
            m_terminusNonce = determination.terminusNonce;

            // determine if this is a rejoining node
            // (used for license check and later the actual rejoin)
            m_rejoinDataPending = m_config.m_startAction.doesJoin();
            m_meshDeterminationLatch.countDown();
            m_joining = m_config.m_startAction == StartAction.JOIN;

            if (m_rejoining || m_joining) {
                m_statusTracker.set(NodeState.REJOINING);
            }
            //Register dummy agents immediately
            m_opsRegistrar.registerMailboxes(m_messenger);

            //Start validating the build string in the background
            final Future<?> buildStringValidation = validateBuildString(getBuildString(), m_messenger.getZK());

            // race to create start action nodes and then verify theirs compatibility.
            m_messenger.getZK().create(VoltZK.start_action, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new ZKUtil.StringCallback(), null);
            VoltZK.createStartActionNode(m_messenger.getZK(), m_messenger.getHostId(), m_config.m_startAction);
            validateStartAction();

            // Race to write license to ZK, compare local copy with ZK's.
            m_licensing.checkLicenseConsistency(m_messenger.getZK());

            if (m_rejoining) {
                //grab rejoining lock before catalog read
                Iv2RejoinCoordinator.acquireLock(m_messenger);
            }

            m_durable = readDeploymentAndCreateStarterCatalogContext();

            if (config.m_isEnterprise && m_config.m_startAction.doesRequireEmptyDirectories()
                    && !config.m_forceVoltdbCreate && m_durable) {
                managedPathsEmptyCheck(config);
            }

            // wait to make sure every host actually *see* each other's ZK node state.
            final int numberOfNodes = m_messenger.getLiveHostIds().size();
            Map<Integer, HostInfo> hostInfos = m_messenger.waitForGroupJoin(numberOfNodes);
            if (m_messenger.isPaused() || m_config.m_isPaused) {
                setStartMode(OperationMode.PAUSED);
            }

            // Create the thread pool here.
            m_periodicWorkThread =
                    CoreUtils.getScheduledThreadPoolExecutor("Periodic Work", 1, CoreUtils.SMALL_STACK_SIZE);
            m_periodicPriorityWorkThread =
                    CoreUtils.getScheduledThreadPoolExecutor("Periodic Priority Work", 1, CoreUtils.SMALL_STACK_SIZE);

            m_snapshotIOAgent = new SnapshotIOAgentImpl(m_messenger,
                    m_messenger.getHSIdForLocalSite(HostMessenger.SNAPSHOT_IO_AGENT_ID));
            m_messenger.createMailbox(m_snapshotIOAgent.getHSId(), m_snapshotIOAgent);

            // Create the GlobalServiceElector.  Do this here so we can register the MPI with it
            // when we construct it below
            m_globalServiceElector = new GlobalServiceElector(m_messenger.getZK(), m_messenger.getHostId());

            // Always create a mailbox for elastic service data transfer
            if (m_config.m_isEnterprise) {
                long elasticHSId = m_messenger.getHSIdForLocalSite(HostMessenger.REBALANCE_SITE_ID);
                m_messenger.createMailbox(elasticHSId, new SiteMailbox(m_messenger, elasticHSId));
            }

            if (m_joining) {
                try {
                    int kfactor = m_catalogContext.getDeployment().getCluster().getKfactor();
                    m_joinCoordinator = ProClass.newInstanceOf("org.voltdb.elastic.ElasticJoinNodeCoordinator", "Elastic",
                            ProClass.HANDLER_LOG, m_messenger, VoltDB.instance().getVoltDBRootPath(), kfactor);
                    m_licensing.validateLicense();
                    if (determination.startAction == StartAction.JOIN && m_joinCoordinator.getHostsJoining() > 1) {
                        String waitMessage = "The join process will begin after a total of "
                                + m_joinCoordinator.getHostsJoining() + " nodes are added, waiting...";
                        consoleLog.info(waitMessage);
                    }
                    m_messenger.registerMailbox(m_joinCoordinator);
                    m_joinCoordinator.initialize();
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Failed to instantiate join coordinator", true, e);
                }
            }

            /*
             * Construct all the mailboxes for things that need to be globally addressable so they can be published
             * in one atomic shot.
             *
             * The starting state for partition assignments are statically derived from the host id generated
             * by host messenger and the k-factor/host count/sites per host. This starting state
             * is published to ZK as the topology metadata node.
             *
             * On join and rejoin the node has to inspect the topology meta node to find out what is missing
             * and then update the topology listing itself as the replica for those partitions.
             * Then it does a compare and set of the topology.
             *
             * Ning: topology may not reflect the true partitions in the cluster during join. So if another node
             * is trying to rejoin, it should rely on the cartographer's view to pick the partitions to replace.
             */
            AbstractTopology topo = getTopology(config.m_startAction, hostInfos, m_joinCoordinator);
            try {
                // IV2 mailbox stuff
                m_cartographer = new Cartographer(m_messenger, m_configuredReplicationFactor,
                        m_catalogContext.cluster.getNetworkpartition());
                m_partitionZeroLeader = () -> m_cartographer.isPartitionZeroLeader();
                List<Integer> partitions = null;
                final Set<Integer> partitionGroupPeers;
                if (m_rejoining) {
                    m_configuredNumberOfPartitions = m_cartographer.getPartitionCount();
                    Set<Integer> recoverPartitions = null;
                    if (m_config.m_restorePlacement) {
                        recoverPartitions = hostInfos.get(m_messenger.getHostId()).getRecoveredPartitions();
                    }
                    AbstractTopology recoveredTopo = recoverPartitions(topo, hostInfos.get(m_messenger.getHostId()).m_group, recoverPartitions);
                    if (recoveredTopo != null) {
                        topo = recoveredTopo;
                        partitions = getPartitionsForLocalHost(topo);
                    }
                    if (partitions == null) {
                        partitions = m_cartographer.getIv2PartitionsToReplace(m_configuredReplicationFactor,
                                m_catalogContext.getNodeSettings().getLocalSitesCount(), m_messenger.getHostId(),
                                Maps.transformValues(hostInfos, h -> h.m_group));
                    }
                    partitionGroupPeers = m_cartographer.findPartitionGroupPeers(partitions);
                    if (partitions.size() == 0) {
                        VoltDB.crashLocalVoltDB("The VoltDB cluster already has enough nodes to satisfy " +
                                "the requested k-safety factor of " +
                                m_configuredReplicationFactor + ".\n" +
                                "No more nodes can join.");
                    }
                } else {
                    m_configuredNumberOfPartitions = topo.getPartitionCount();
                    partitions = getPartitionsForLocalHost(topo);
                    partitionGroupPeers = topo.getPartitionGroupPeers(m_messenger.getHostId());
                }

                m_eligibleAsLeader = determineIfEligibleAsLeader(partitions, partitionGroupPeers, topo);

                m_messenger.setPartitionGroupPeers(partitionGroupPeers, getHostCount());

                // The partition id list must be in sorted order
                assert(Ordering.natural().isOrdered(partitions));

                // persist the merged settings
                m_config.m_recoveredPartitions = Joiner.on(",").join(partitions);
                clusterSettings = ClusterSettings.create(
                        m_config.asClusterSettingsMap(), fromPropertyFile.asMap(), fromDeploymentFile);
                clusterSettings.store();
                hostLog.info("Partitions on this host:" + m_config.m_recoveredPartitions);
                for (Integer partition : partitions) {
                    m_iv2InitiatorStartingTxnIds.put(partition, TxnEgo.makeZero(partition).getTxnId());
                }
                m_iv2Initiators = createIv2Initiators(
                        partitions,
                        m_config.m_startAction);
                m_iv2InitiatorStartingTxnIds.put(MpInitiator.MP_INIT_PID,
                        TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId());

                if (m_eligibleAsLeader) {
                    // Start the GlobalServiceElector. Not sure where this will actually belong.
                    try {
                        m_globalServiceElector.start();
                    } catch (Exception e) {
                        VoltDB.crashLocalVoltDB("Unable to start GlobalServiceElector", true, e);
                    }

                    // Pass the local HSIds to the MPI so it can farm out buddy sites
                    // to the RO MP site pool
                    List<Long> localHSIds = new ArrayList<>();
                    for (Initiator ii : m_iv2Initiators.values()) {
                        localHSIds.add(ii.getInitiatorHSId());
                    }

                    m_MPI = new MpInitiator(m_messenger, localHSIds, getStatsAgent(),
                            m_globalServiceElector.getLeaderId());
                    m_iv2Initiators.put(MpInitiator.MP_INIT_PID, m_MPI);
                }

                // Make a list of HDIds to join
                Map<Integer, Long> partsToHSIdsToRejoin = new HashMap<>();
                for (Initiator init : m_iv2Initiators.values()) {
                    if (init.isRejoinable()) {
                        partsToHSIdsToRejoin.put(init.getPartitionId(), init.getInitiatorHSId());
                    }
                }
                OnDemandBinaryLogger.path = VoltDB.instance().getVoltDBRootPath();
                if (m_rejoining) {
                    SnapshotSaveAPI.recoveringSiteCount.set(partsToHSIdsToRejoin.size());
                    hostLog.info("Set recovering site count to " + partsToHSIdsToRejoin.size());

                    m_joinCoordinator = new Iv2RejoinCoordinator(m_messenger,
                            partsToHSIdsToRejoin.values(),
                            VoltDB.instance().getVoltDBRootPath(),
                            m_config.m_startAction == StartAction.LIVE_REJOIN);
                    m_joinCoordinator.initialize();
                    m_messenger.registerMailbox(m_joinCoordinator);
                    if (m_config.m_startAction == StartAction.LIVE_REJOIN) {
                        hostLog.info("Using live rejoin.");
                    }
                    else {
                        hostLog.info("Using blocking rejoin.");
                    }
                } else if (m_joining) {
                    m_joinCoordinator.setPartitionsToHSIds(partsToHSIdsToRejoin);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            // Initialization may require an intialized avro
            m_avroSerde.updateConfig(m_catalogContext);

            // do the many init tasks in the Inits class
            Inits inits = new Inits(m_statusTracker, this, 1, m_durable);
            inits.doInitializationWork();

            int kfactor = m_catalogContext.getDeployment().getCluster().getKfactor();
            if(determination.startAction == StartAction.JOIN && kfactor > 0) {
                int kfactorPlusOne = kfactor + 1;
                String waitMessage = "" + kfactorPlusOne + " nodes added, joining new nodes to the cluster...";
                consoleLog.info(waitMessage);
            }

            // Need the catalog so that we know how many tables so we can guess at the necessary heap size
            // This is done under Inits.doInitializationWork(), so need to wait until we get here.
            // Current calculation needs pro/community knowledge, number of tables, and the sites/host,
            // which is the number of initiators (minus the possibly idle MPI initiator)
            checkHeapSanity(m_catalogContext.tables.size(),
                    (getLocalPartitionCount()), m_configuredReplicationFactor);

            if (m_joining) {
                if (hostLog.isDebugEnabled()) {
                    hostLog.debug("Elastic join happened on a cluster of DR Role:" + getReplicationRole());
                }
            }

            collectLocalNetworkMetadata();

            // Initialize stats
            StatsAgent statsAgent = getStatsAgent();

            m_ioStats = new IOStats();
            statsAgent.registerStatsSource(StatsSelector.IOSTATS, 0, m_ioStats);
            m_memoryStats = new MemoryStats();
            statsAgent.registerStatsSource(StatsSelector.MEMORY,  0, m_memoryStats);
            statsAgent.registerStatsSource(StatsSelector.TOPO, 0, m_cartographer);
            m_partitionCountStats = new PartitionCountStats(m_cartographer);
            statsAgent.registerStatsSource(StatsSelector.PARTITIONCOUNT, 0, m_partitionCountStats);
            m_initiatorStats = new InitiatorStats(m_myHostId);
            m_liveClientsStats = new LiveClientsStats();
            statsAgent.registerStatsSource(StatsSelector.LIVECLIENTS_CONNECTIONS, 0, m_liveClientsStats);

            m_latencyStats = new LatencyStats();
            statsAgent.registerStatsSource(StatsSelector.LATENCY, 0, m_latencyStats);
            m_latencyCompressedStats = new LatencyHistogramStats(m_myHostId);
            statsAgent.registerStatsSource(StatsSelector.LATENCY_COMPRESSED, 0, m_latencyCompressedStats);
            m_latencyHistogramStats = new LatencyUncompressedHistogramStats(m_myHostId);
            statsAgent.registerStatsSource(StatsSelector.LATENCY_HISTOGRAM, 0, m_latencyHistogramStats);

            BalancePartitionsStatistics rebalanceStats = new BalancePartitionsStatistics();
            statsAgent.registerStatsSource(StatsSelector.REBALANCE, 0, rebalanceStats);

            KSafetyStats kSafetyStats = new KSafetyStats();
            statsAgent.registerStatsSource(StatsSelector.KSAFETY, 0, kSafetyStats);
            m_cpuStats = new CpuStats();
            statsAgent.registerStatsSource(StatsSelector.CPU, 0, m_cpuStats);
            m_gcStats = new GcStats();
            statsAgent.registerStatsSource(StatsSelector.GC, 0, m_gcStats);

            m_commandLogStats = new CommandLogStats(m_commandLog);
            statsAgent.registerStatsSource(StatsSelector.COMMANDLOG, 0, m_commandLogStats);

            skewStats = new ClockSkewStats(m_clock, VoltDB.instance(), hostLog);
            statsAgent.registerStatsSource(StatsSelector.CLOCKSKEW, 0, skewStats);

            CompoundProcCallStats.initStats(statsAgent);

            // Dummy DRCONSUMER stats
            replaceDRConsumerStatsWithDummy();

            statsAgent.registerStatsSource(StatsSelector.DRCONFLICTS, 0, new DRConflictsStats(m_drConflictsTracker, m_catalogContext.cluster.getDrclusterid()));

            // Operator function helpers
            m_operatorSupport.registerStatistics(statsAgent);

            /*
             * Initialize the command log on rejoin and join before configuring the IV2
             * initiators.  This will prevent them from receiving transactions
             * which need logging before the internal file writers are
             * initialized.  Root cause of ENG-4136.
             *
             * If sync command log is on, not initializing the command log before the initiators
             * are up would cause deadlock.
             */
            if (m_commandLog != null && m_commandLog.needsInitialization()) {
                consoleLog.info("Initializing the database and command logs. This may take a moment...");
            }
            else {
                consoleLog.info("Initializing the database. This may take a moment...");
            }
            if (m_commandLog != null && (m_rejoining || m_joining)) {
                //On rejoin the starting IDs are all 0 so technically it will load any snapshot
                //but the newest snapshot will always be the truncation snapshot taken after rejoin
                //completes at which point the node will mark itself as actually recovered.
                //
                // Use the partition count from the cluster config instead of the cartographer
                // here. Since the initiators are not started yet, the cartographer still doesn't
                // know about the new partitions at this point.
                m_commandLog.initForRejoin(
                        m_catalogContext.cluster.getLogconfig().get("log").getLogsize(),
                        Long.MIN_VALUE,
                        true,
                        m_config.m_commandLogBinding, m_iv2InitiatorStartingTxnIds);
            }

            // Create the client interface
            try {
                InetAddress clientIntf = null;
                InetAddress adminIntf = null;
                if (!m_config.m_externalInterface.trim().equals("")) {
                    clientIntf = InetAddress.getByName(m_config.m_externalInterface);
                    //client and admin interfaces are same by default.
                    adminIntf = clientIntf;
                }
                //If user has specified on command line host:port override client and admin interfaces.
                if (m_config.m_clientInterface != null && m_config.m_clientInterface.trim().length() > 0) {
                    clientIntf = InetAddress.getByName(m_config.m_clientInterface);
                }
                if (m_config.m_adminInterface != null && m_config.m_adminInterface.trim().length() > 0) {
                    adminIntf = InetAddress.getByName(m_config.m_adminInterface);
                }
                m_clientInterface = ClientInterface.create(m_messenger, m_catalogContext, getReplicationRole(),
                        m_cartographer,
                        clientIntf,
                        config.m_port,
                        adminIntf,
                        config.m_adminPort,
                        m_config.m_sslExternal ? m_config.m_sslServerContext : null);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            LimitsStats limitsStats = new LimitsStats(
                    m_clientInterface.getFileDescriptorTracker(),
                    m_clientInterface.getClientConnectionsTracker()
            );
            statsAgent.registerStatsSource(StatsSelector.LIMITS, 0, limitsStats);

            VoltDB.getExportManager().startListeners(m_clientInterface);
            m_taskManager = new TaskManager(m_clientInterface, statsAgent, m_myHostId,
                    m_config.m_startAction == StartAction.JOIN,
                    // Task manager is read only if db is paused or this is a replica
                    () -> m_mode == OperationMode.PAUSED || getReplicationRole() == ReplicationRole.REPLICA);
            m_globalServiceElector.registerService(() -> m_taskManager.promoteToLeader(m_catalogContext));

            // DR overflow directory
            if (m_licensing.isFeatureAllowed("DR")) {
                m_producerDRGateway = ProClass.newInstanceOf("org.voltdb.dr2.DRProducer", "DR Producer",
                        ProClass.HANDLER_CRASH,
                        new File(VoltDB.instance().getDROverflowPath()),
                        new File(VoltDB.instance().getSnapshotPath()),
                        willDoActualRecover() ? ProducerDRGateway.Mode.RECOVER
                                : m_config.m_startAction.doesRejoin() ? ProducerDRGateway.Mode.REJOIN
                                        : m_config.m_startAction == StartAction.JOIN ? ProducerDRGateway.Mode.JOIN
                                                : ProducerDRGateway.Mode.NEW,
                        m_replicationActive.get(), m_configuredNumberOfPartitions,
                        (m_catalogContext.getClusterSettings().hostcount() - m_config.m_missingHostCount));

                DRConflictReporter.init(m_drConflictsTracker);
            }
            else {
                // set up empty stats for the DR Producer
                statsAgent.registerStatsSource(StatsSelector.DRPRODUCERNODE, 0,
                        new DRProducerStatsBase.DRProducerNodeStatsBase());
                statsAgent.registerStatsSource(StatsSelector.DRPRODUCERPARTITION, 0,
                        new DRProducerStatsBase.DRProducerPartitionStatsBase());
                statsAgent.registerStatsSource(StatsSelector.DRPRODUCERCLUSTER, 0,
                        new DRProducerStatsBase.DRProducerClusterStatsBase());
            }
            m_drRoleStats = new DRRoleStats(this);
            statsAgent.registerStatsSource(StatsSelector.DRROLE, 0, m_drRoleStats);

            /*
             * Configure and start all the IV2 sites
             */
            try {
                final String serializedCatalog = m_catalogContext.catalog.serialize();
                for (Initiator iv2init : m_iv2Initiators.values()) {
                    iv2init.configure(
                            getBackendTargetType(),
                            m_catalogContext,
                            serializedCatalog,
                            m_configuredNumberOfPartitions,
                            m_config.m_startAction,
                            statsAgent,
                            m_memoryStats,
                            m_commandLog,
                            m_config.m_executionCoreBindings.poll(),
                            isLowestSiteId(iv2init));
                }

                // LeaderAppointer startup blocks if the initiators are not initialized.
                // So create the LeaderAppointer after the initiators.
                boolean expectSyncSnapshot = getReplicationRole() == ReplicationRole.REPLICA && config.m_startAction == StartAction.CREATE;
                m_leaderAppointer = new LeaderAppointer(
                        m_messenger,
                        m_configuredNumberOfPartitions,
                        m_catalogContext.getDeployment().getCluster().getKfactor(),
                        topo,
                        m_MPI,
                        kSafetyStats,
                        expectSyncSnapshot
                );
                m_globalServiceElector.registerService(m_leaderAppointer);
            } catch (Exception e) {
                Throwable toLog = e;
                if (e instanceof ExecutionException) {
                    toLog = ((ExecutionException)e).getCause();
                }
                VoltDB.crashLocalVoltDB("Error configuring IV2 initiator.", true, toLog);
            }

            // Create the statistics manager and register it to JMX registry
            m_statsManager = null;
            try {
                if (!DISABLE_JMX) {
                    m_statsManager = ProClass.newInstanceOf("org.voltdb.management.JMXStatsManager", "JMX",
                            ProClass.HANDLER_IGNORE);
                    if (m_statsManager != null) {
                        m_statsManager.initialize();
                    }
                }
            } catch (Exception e) {
                //JMXStatsManager will log and we continue.
            }

            try {
                m_snapshotCompletionMonitor.init(m_messenger.getZK());
            } catch (Exception e) {
                hostLog.fatal("Error initializing snapshot completion monitor", e);
                VoltDB.crashLocalVoltDB("Error initializing snapshot completion monitor", true, e);
            }

            /*
             * Make sure the build string successfully validated
             * before continuing to do operations
             * that might return wrongs answers or lose data.
             */
            try {
                buildStringValidation.get();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to validate cluster build string", false, e);
            }

            //elastic join, make sure all the joining nodes are ready
            //so that the secondary connections can be created.
            if (m_joining) {
                int expectedHosts = m_configuredReplicationFactor + 1;
                m_messenger.waitForJoiningHostsToBeReady(expectedHosts, this.m_myHostId);
            } else if (!m_rejoining) {
                // initial start or recover
                int expectedHosts = m_catalogContext.getClusterSettings().hostcount() - m_config.m_missingHostCount;
                m_messenger.waitForAllHostsToBeReady(expectedHosts);
            }

            // @hostFailed() may not be triggered if there are host failures during mesh determination
            // Fail this rejoining node here if it sees site failure
            if (m_messenger.getFailedSiteCount() > 0) {
                stopRejoiningHost();
            }

            //The connections between peers in partition groups could be slow
            //The problem will be addressed.
            if (!(m_config.m_sslInternal)) {
                // Create secondary connections within partition group
                createSecondaryConnections(m_rejoining);
            }

            if (!m_joining && (m_cartographer.getPartitionCount()) != m_configuredNumberOfPartitions) {
                for (Map.Entry<Integer, ImmutableList<Long>> entry :
                    getSiteTrackerForSnapshot().m_partitionsToSitesImmutable.entrySet()) {
                    hostLog.info(entry.getKey() + " -- "
                            + CoreUtils.hsIdCollectionToString(entry.getValue()));
                }
                VoltDB.crashGlobalVoltDB("Mismatch between configured number of partitions (" +
                        m_configuredNumberOfPartitions + ") and actual (" +
                        m_cartographer.getPartitionCount() + ")",
                        true, null);
            }

            // Write a bunch of useful system info to the host log
            DailyLogging.logInfo();

            // And schedule repeating tasks (including daily logging)
            schedulePeriodicWorks();
            m_clientInterface.schedulePeriodicWorks();

            // warn the user on the console if k=0 or if no command logging
            if (m_configuredReplicationFactor == 0) {
                consoleLog.warn("This is not a highly available cluster. K-Safety is set to 0.");
            }
            boolean usingCommandLog = m_config.m_isEnterprise
                    && (m_catalogContext.cluster.getLogconfig() != null)
                    && (m_catalogContext.cluster.getLogconfig().get("log") != null)
                    && m_catalogContext.cluster.getLogconfig().get("log").getEnabled();
            if (!usingCommandLog) {
                // figure out if using a snapshot schedule
                boolean usingPeridoicSnapshots = false;
                for (SnapshotSchedule ss : m_catalogContext.database.getSnapshotschedule()) {
                    if (ss.getEnabled()) {
                        usingPeridoicSnapshots = true;
                    }
                }
                // print the right warning depending on durability settings
                if (usingPeridoicSnapshots) {
                    consoleLog.warn("Durability is limited to periodic snapshots. Command logging is off.");
                }
                else {
                    consoleLog.warn("Durability is turned off. Command logging is off.");
                }
            }

            // warn if cluster is partitionable, but partition detection is off
            if (! m_catalogContext.cluster.getNetworkpartition() && m_configuredReplicationFactor > 0) {
                hostLog.warn("Running a redundant (k-safe) cluster with network " +
                        "partition detection disabled is not recommended for production use.");
                // we decided not to include the stronger language below for the 3.0 version (ENG-4215)
                //hostLog.warn("With partition detection disabled, data may be lost or " +
                //      "corrupted by certain classes of network failures.");
            }

            assert (m_clientInterface != null);
            m_clientInterface.initializeSnapshotDaemon(m_messenger, m_globalServiceElector);
            statsAgent.registerStatsSource(StatsSelector.TTL, 0, VoltDB.getTTLManager());
            // Start elastic services
            try {
                if (m_config.m_isEnterprise) {
                    m_elasticService = ProClass.newInstanceOf("org.voltdb.elastic.ElasticCoordinator",
                            "Elastic Operations", ProClass.HANDLER_CRASH, m_messenger, m_clientInterface,
                            m_cartographer, rebalanceStats, VoltDB.instance().getCommandLogSnapshotPath(),
                            m_catalogContext.getDeployment().getCluster().getKfactor(), m_clusterSettings,
                            m_eligibleAsLeader);
                    m_elasticService.updateConfig(m_catalogContext);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to instantiate elastic services", false, e);
            }

            // set additional restore agent stuff
            if (m_restoreAgent != null) {
                m_restoreAgent.setInitiator(new Iv2TransactionCreator(m_clientInterface));
            }

            // Start the stats agent at the end, after everything has been constructed
            m_opsRegistrar.setDummyMode(false);

            m_configLogger = new Thread(new ConfigLogging());
            m_configLogger.start();

            scheduleDailyLoggingWorkInNextCheckTime();

            // Write a message to the log file if LARGE_MODE_RATIO system property is set to anything other than 0.
            // This way it will be possible to verify that various frameworks are exercising large mode.

            // If -Dlarge_mode_ratio=xx is specified via ant, the value will show up in the environment variables and
            // take higher priority. Otherwise, the value specified via VOLTDB_OPTS will take effect.
            // If the test is started by ant and -Dlarge_mode_ratio is not set, it will take a default value "-1" which
            // we should ignore.
            final double largeModeRatio = Double.parseDouble(
                    System.getenv("LARGE_MODE_RATIO") == null || System.getenv("LARGE_MODE_RATIO").equals("-1") ?
                            System.getProperty("LARGE_MODE_RATIO", "0") :
                            System.getenv("LARGE_MODE_RATIO"));
            if (largeModeRatio > 0) {
                hostLog.infoFmt("The large_mode_ratio property is set as %.2f", largeModeRatio);
            }
            if (AdHocNTBase.USING_CALCITE) {
                hostLog.warn("Using Calcite as parser/planner. This is an experimental feature.");
            }
        }
    }

    /**
     * Check if actual recover is needed
     * Return false if we need to start new,
     * or command log is disabled,
     * or there is no complete snapshot
     */
   private boolean willDoActualRecover()
   {
       return (m_config.m_startAction.doesRecover() &&
              (m_durable || getTerminusNonce() != null));
   }
    /**
     * recover the partition assignment from one of lost hosts in the same placement group for rejoin
     * Use the placement group of the recovering host to find a matched host from the lost nodes in the topology
     * If the partition count from the lost node is the same as the site count of the recovering host,
     * The partitions on the lost node will be placed on the recovering host. Partition group layout will be maintained.
     * Topology will be updated on ZK if successful
     * @param topology The topology from ZK, which contains the partition assignments for live or lost hosts
     * @param haGroup The placement group of the recovering host
     * @param recoverPartitions the partition placement to be recovered on this host
     * @return A list of partitions if recover effort is a success.
     */
    private AbstractTopology recoverPartitions(AbstractTopology topology, String haGroup, Set<Integer> recoverPartitions) {

        long version = topology.version;
        if (!recoverPartitions.isEmpty()) {
            // In rejoin case, partition list from the rejoining node could be out of range if the rejoining
            // host is a previously elastic removed node or some other used nodes, if out of range, do not restore
            if (Collections.max(recoverPartitions) > Collections.max(m_cartographer.getPartitions())) {
                recoverPartitions.clear();
            }
        }
        AbstractTopology recoveredTopo = AbstractTopology.mutateRecoverTopology(topology,
                m_messenger.getLiveHostIds(),
                m_messenger.getHostId(),
                haGroup,
                recoverPartitions);
        if (recoveredTopo == null) {
            return null;
        }
        List<Integer> partitions = getPartitionsForLocalHost(recoveredTopo);
        if (partitions != null && partitions.size() == m_catalogContext.getNodeSettings().getLocalSitesCount()) {
            TopologyZKUtils.updateTopologyToZK(m_messenger.getZK(), recoveredTopo);
        }
        if (version < recoveredTopo.version && !recoverPartitions.isEmpty()) {
            consoleLog.info("Partition placement layout has been restored for rejoining.");
        }
        return recoveredTopo;
    }

    private List<Integer> getPartitionsForLocalHost(AbstractTopology topo) {
        int hostId = m_messenger.getHostId();
        List<Integer> pars = topo.getPartitionIdList(hostId);
        if (pars == null) { // check, else newArrayList throws an even less understandable error
            String err = String.format("Unexpected error: no partition list for local host id %d (rejoining: %s)",
                                       hostId, m_rejoining);
            hostLog.error(err);
            throw new IllegalStateException(err);
        }
        return Lists.newArrayList(pars);
    }

    @Override
    public void hostsFailed(Set<Integer> failedHosts) {
        m_failedHostExecutorService.submit(new Runnable() {
            @Override
            public void run()
            {
                // stop this node if rejoining.
                if (stopRejoiningHost()) {
                    return;
                }
                if (failedHosts.isEmpty()) {
                    return;
                }

                //create a blocker for repair if this is a MP leader and partition leaders change
                if (m_leaderAppointer.isLeader()) {
                    if (!m_cartographer.hasPartitionMastersOnHosts(failedHosts)) {
                        // When the last partition leader on a host is migrated away and there is an MP transaction which depends on
                        // the partition leader, the transaction can be deadlocked if the host is shutdown. Since the host does not have
                        // any partition leaders, its shutdown won't trigger transaction repair process to beak up the dependency.
                        // Add a new ZK node to trigger transaction repair.
                        MpTerm.createTxnRestartTrigger(m_messenger.getZK());
                    }
                }

                // First check to make sure that the cluster still is viable before
                // before allowing the fault log to be updated by the notifications
                // generated below.
                if (!m_leaderAppointer.isClusterKSafe(failedHosts)) {
                    VoltDB.crashLocalVoltDB("Some partitions have no replicas.  Cluster has become unviable.");
                    return;
                }

                handleHostsFailedForMigratePartitionLeader(failedHosts);

                // Send KSafety trap - BTW the side effect of
                // calling m_leaderAppointer.isClusterKSafe(..) is that leader appointer
                // creates the ksafety stats set
                if (m_cartographer.isPartitionZeroLeader() || isFirstZeroPartitionReplica(failedHosts)) {
                    // Send hostDown traps
                    for (int hostId : failedHosts) {
                        m_snmp.hostDown(FaultLevel.ERROR, hostId, "Host left cluster mesh due to connection loss");
                    }
                    final int missing = m_leaderAppointer.getKSafetyStatsSet().stream()
                            .max((s1,s2) -> s1.getMissingCount() - s2.getMissingCount())
                            .map(s->s.getMissingCount()).orElse(failedHosts.size());
                    final int expected = getHostCount();
                    m_snmp.statistics(FaultFacility.CLUSTER,
                            "Node lost. Cluster is down to " + (expected - missing)
                            + " members out of original "+ expected + ".");
                }
                // Cleanup the rejoin blocker in case the rejoining node failed.
                // This has to run on a separate thread because the callback is
                // invoked on the ZooKeeper server thread.
                //
                // I'm trying to be defensive to have this cleanup code run on
                // all live nodes. One of them will succeed in cleaning up the
                // rejoin ZK nodes. The others will just do nothing if the ZK
                // nodes are already gone. If this node is still initializing
                // when a rejoining node fails, there must be a live node that
                // can clean things up. It's okay to skip this if the executor
                // services are not set up yet.
                //
                // Also be defensive to cleanup stop node indicator on all live
                // hosts.
                for (int hostId : failedHosts) {
                    CoreZK.removeRejoinNodeIndicatorForHost(m_messenger.getZK(), hostId);
                    VoltZK.removeStopNodeIndicator(m_messenger.getZK(),
                            ZKUtil.joinZKPath(VoltZK.host_ids_be_stopped, Integer.toString(hostId)),
                            hostLog);
                    m_messenger.removeStopNodeNotice(hostId);
                }

                // let the client interface know host(s) have failed to clean up any outstanding work
                // especially non-transactional work
                m_clientInterface.handleFailedHosts(failedHosts);
                if (m_elasticService != null) {
                    m_elasticService.hostsFailed(failedHosts);
                }
            }
        });
    }

    // If the current node hasn't finished rejoin when another node fails, fail this node to prevent locking up.
    private boolean stopRejoiningHost() {

        // The host failure notification could come before mesh determination, wait for the determination
        try {
            m_meshDeterminationLatch.await();
        } catch (InterruptedException e) {
        }

        if (m_rejoining) {
            VoltDB.crashLocalVoltDB("Another node failed before this node could finish rejoining. " +
                    "As a result, the rejoin operation has been canceled. Please try again.");
            return true;
        }
        return false;
    }

    private void handleHostsFailedForMigratePartitionLeader(Set<Integer> failedHosts) {

        final boolean disableSpiTask = "true".equalsIgnoreCase(System.getProperty("DISABLE_MIGRATE_PARTITION_LEADER", "false"));
        if (disableSpiTask) {
            return;
        }

        VoltZK.removeActionBlocker(m_messenger.getZK(), VoltZK.migratePartitionLeaderBlocker, hostLog);
        MigratePartitionLeaderInfo migratePartitionLeaderInfo = VoltZK.getMigratePartitionLeaderInfo(m_messenger.getZK());
        if (migratePartitionLeaderInfo == null) {
            return;
        }

        final int oldHostId = migratePartitionLeaderInfo.getOldLeaderHostId();
        final int newHostId = migratePartitionLeaderInfo.getNewLeaderHostId();

        //The host which initiates MigratePartitionLeader is down before it gets chance to notify new leader that
        //all sp transactions are drained.
        //Then reset the MigratePartitionLeader status on the new leader to allow it process transactions as leader
        if (failedHosts.contains(oldHostId) && newHostId == m_messenger.getHostId()) {
            Initiator initiator = m_iv2Initiators.get(migratePartitionLeaderInfo.getPartitionId());
            hostLog.info("The host that initiated @MigratePartitionLeader possibly went down before migration completed. Reset MigratePartitionLeader status on "
                          + CoreUtils.hsIdToString(initiator.getInitiatorHSId()));
            ((SpInitiator)initiator).setMigratePartitionLeaderStatus(oldHostId);
            VoltZK.removeMigratePartitionLeaderInfo(m_messenger.getZK());
        } else if (failedHosts.contains(newHostId) && oldHostId == m_messenger.getHostId()) {
            //The new leader is down, on old leader host:
            VoltZK.removeMigratePartitionLeaderInfo(m_messenger.getZK());
        }
    }

    private boolean isFirstZeroPartitionReplica(Set<Integer> failedHosts) {
        int partitionZeroMaster = CoreUtils.getHostIdFromHSId(m_cartographer.getHSIdForMaster(0));
        if (!failedHosts.contains(partitionZeroMaster)) {
            return false;
        }
        int firstReplica = m_cartographer
                .getReplicasForPartition(0)
                .stream()
                .map(l->CoreUtils.getHostIdFromHSId(l))
                .filter(i-> !failedHosts.contains(i))
                .min((i1,i2) -> i1 - i2)
                .orElse(m_messenger.getHostId() + 1);
        return firstReplica == m_messenger.getHostId();
    }

    class DailyLogTask implements Runnable {
        @Override
        public void run() {
            m_myHostId = m_messenger.getHostId(); // TODO : why are we resetting this?
            DailyLogging.logInfo();
            scheduleDailyLoggingWorkInNextCheckTime();

            // daily maintenance
            EnterpriseMaintenance em = EnterpriseMaintenance.get();
            if (em != null) { em.dailyMaintenanceTask(); }
        }
    }

    /**
     * Schedule the next execution of the DailyLogTask. This is
     * timed for 30 secs after estimated log rolloever. If for
     * some reason we cannot determine the rolloever time, we'll
     * arbitrarily schedule the task for 12 hours from now.
     * (We don't want to run the task frequently, too much
     *  spam in the logs).
     */
    void scheduleDailyLoggingWorkInNextCheckTime() {
        long delta; TimeUnit unit;
        long nextCheck = VoltLog4jLogger.getNextCheckTime();
        if (nextCheck >= 0) {
            delta = Math.max(nextCheck - System.currentTimeMillis(), 0) + 30_000;
            unit = TimeUnit.MILLISECONDS;
        }
        else {
            hostLog.warn("Failed to determine log rollover time");
            delta = 12;
            unit = TimeUnit.HOURS;
        }
        scheduleWork(new DailyLogTask(), delta, 0, unit);
    }

    class StartActionWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (m_mode == OperationMode.SHUTTINGDOWN) {
                return;
            }
            m_es.submit(new Runnable() {
                @Override
                public void run() {
                    validateStartAction();
                }
            });
        }
    }

    private void validateStartAction() {
        ZooKeeper zk = m_messenger.getZK();
        boolean initCompleted = false;
        List<String> children = null;

        try {
            initCompleted = zk.exists(VoltZK.init_completed, false) != null;
            children = zk.getChildren(VoltZK.start_action, new StartActionWatcher(), null);
        } catch (KeeperException e) {
            hostLog.error("Failed to validate the start actions", e);
            return;
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Interrupted during start action validation:" + e.getMessage(), true, e);
        }

        if (children != null && !children.isEmpty()) {
            for (String child : children) {
                byte[] data = null;
                try {
                    data = zk.getData(VoltZK.start_action + "/" + child, false, null);
                } catch (KeeperException excp) {
                    if (excp.code() == Code.NONODE) {
                            hostLog.debug("Failed to validate the start action as node "
                                    + VoltZK.start_action + "/" + child + " got disconnected", excp);
                    } else {
                        hostLog.error("Failed to validate the start actions ", excp);
                    }
                    return;
                } catch (InterruptedException e) {
                    VoltDB.crashLocalVoltDB("Interrupted during start action validation:" + e.getMessage(), true, e);
                }

                if (data == null) {
                    VoltDB.crashLocalVoltDB("Couldn't find " + VoltZK.start_action + "/" + child);
                }
                String startAction = new String(data);
                if ((startAction.equals(StartAction.JOIN.toString()) ||
                        startAction.equals(StartAction.REJOIN.toString()) ||
                        startAction.equals(StartAction.LIVE_REJOIN.toString())) &&
                        !initCompleted) {
                    int nodeId = VoltZK.getHostIDFromChildName(child);
                    if (nodeId == m_messenger.getHostId()) {
                        VoltDB.crashLocalVoltDB("This node was started with start action " + startAction + " during cluster creation. "
                                + "All nodes should be started with matching create or recover actions when bring up a cluster. "
                                + "Join and rejoin are for adding nodes to an already running cluster.");
                    } else {
                        hostLog.warn("Node " + nodeId + " tried to " + startAction + " cluster but it is not allowed during cluster creation. "
                                + "All nodes should be started with matching create or recover actions when bring up a cluster. "
                                + "Join and rejoin are for adding nodes to an already running cluster.");
                    }
                }
            }
        }
    }

    private class ConfigLogging implements Runnable {

        private void logConfigInfo() {
            hostLog.info("Logging config info");

            File configInfoDir = getConfigDirectory();
            configInfoDir.mkdirs();

            File configInfo = new File(configInfoDir, "config.json");

            byte jsonBytes[] = null;
            try {
                JSONStringer stringer = new JSONStringer();
                stringer.object();

                stringer.keySymbolValuePair("workingDir", System.getProperty("user.dir"));
                stringer.keySymbolValuePair("pid", CLibrary.getpid());

                stringer.key("log4jDst").array();
                Enumeration<?> appenders = Logger.getRootLogger().getAllAppenders();
                while (appenders.hasMoreElements()) {
                    Appender appender = (Appender) appenders.nextElement();
                    if (appender instanceof FileAppender){
                        stringer.object();
                        stringer.keySymbolValuePair("path", new File(((FileAppender) appender).getFile()).getCanonicalPath());
                        if (appender instanceof DailyRollingFileAppender) {
                            stringer.keySymbolValuePair("format", ((DailyRollingFileAppender)appender).getDatePattern());
                        }
                        stringer.endObject();
                    }
                }

                Enumeration<?> loggers = Logger.getRootLogger().getLoggerRepository().getCurrentLoggers();
                while (loggers.hasMoreElements()) {
                    Logger logger = (Logger) loggers.nextElement();
                    appenders = logger.getAllAppenders();
                    while (appenders.hasMoreElements()) {
                        Appender appender = (Appender) appenders.nextElement();
                        if (appender instanceof FileAppender){
                            stringer.object();
                            stringer.keySymbolValuePair("path", new File(((FileAppender) appender).getFile()).getCanonicalPath());
                            if (appender instanceof DailyRollingFileAppender) {
                                stringer.keySymbolValuePair("format", ((DailyRollingFileAppender)appender).getDatePattern());
                            }
                            stringer.endObject();
                        }
                    }
                }
                stringer.endArray();

                stringer.endObject();
                JSONObject jsObj = new JSONObject(stringer.toString());
                jsonBytes = jsObj.toString(4).getBytes(Charsets.UTF_8);
            } catch (JSONException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                FileOutputStream fos = new FileOutputStream(configInfo);
                fos.write(jsonBytes);
                fos.getFD().sync();
                fos.close();
            } catch (IOException e) {
                hostLog.error("Failed to log config info: " + e.getMessage());
                e.printStackTrace();
            }
        }

        private void logCatalogAndDeployment(CatalogJarWriteMode mode) {
            File configInfoDir = getConfigDirectory();
            configInfoDir.mkdirs();

            try {
                m_catalogContext.writeCatalogJarToFile(configInfoDir.getPath(), "catalog.jar", mode);
            } catch (IOException e) {
                hostLog.error("Failed to writing catalog jar to disk: " + e.getMessage(), e);
                e.printStackTrace();
                VoltDB.crashLocalVoltDB("Fatal error when writing the catalog jar to disk.", true, e);
            }
            logDeployment();
        }

        private void logDeployment() {
            File configInfoDir = getConfigDirectory();
            configInfoDir.mkdirs();

            try {
                File deploymentFile = getConfigLogDeployment();
                if (deploymentFile.exists()) {
                    deploymentFile.delete();
                }
                FileOutputStream fileOutputStream = new FileOutputStream(deploymentFile);
                fileOutputStream.write(m_catalogContext.getDeploymentBytes());
                fileOutputStream.close();
            } catch (Exception e) {
                hostLog.error("Failed to log deployment file: " + e.getMessage(), e);
                e.printStackTrace();
            }
        }

        @Override
        public void run() {
            logConfigInfo();
            logCatalogAndDeployment(CatalogJarWriteMode.START_OR_RESTART);
        }
    }

    // Get topology information.  If rejoining, get it directly from
    // ZK.  Otherwise, try to do the write/read race to ZK on startup.
    private AbstractTopology getTopology(StartAction startAction, Map<Integer, HostInfo> hostInfos,
            JoinCoordinator joinCoordinator)
    {
        AbstractTopology topology = null;
        if (startAction == StartAction.JOIN) {
            assert(joinCoordinator != null);
            topology = joinCoordinator.getTopology();
        } else if (startAction.doesRejoin()) {
            topology = TopologyZKUtils.readTopologyFromZK(m_messenger.getZK());
        } else {
            try {
                return TopologyZKUtils.readTopologyFromZK(m_messenger.getZK(), null);
            } catch (KeeperException.NoNodeException e) {
                hostLog.debug("Topology doesn't exist yet try to create it");
            } catch (KeeperException | InterruptedException | JSONException e) {
                VoltDB.crashLocalVoltDB("Unable to read topology from ZK, dying", true, e);
            }

            // initial start or recover
            int hostcount = getHostCount();
            if (hostInfos.size() != (hostcount - m_config.m_missingHostCount)) {
                VoltDB.crashLocalVoltDB("The total number of live and missing hosts must be the same as the cluster host count");
            }
            int kfactor = getKFactor();
            if (kfactor == 0 && m_config.m_missingHostCount > 0) {
                VoltDB.crashLocalVoltDB("A cluster with 0 kfactor can not be started with missing nodes ");
            }
            if (hostcount <= kfactor) {
                VoltDB.crashLocalVoltDB("Not enough nodes to ensure K-Safety.");
            }
            // Missing hosts can't be more than number of partition groups times k-factor
            int partitionGroupCount = getHostCount() / (kfactor + 1);
            if (m_config.m_missingHostCount > (partitionGroupCount * kfactor)) {
                VoltDB.crashLocalVoltDB("Too many nodes are missing at startup. This cluster only allow up to "
                        + (partitionGroupCount * kfactor) + " missing hosts.");
            }

            //startup or recover a cluster with missing nodes. make up the missing hosts to fool the topology
            //The topology will contain hosts which are marked as missing.The missing hosts will not host any master partitions.
            //At least one partition replica must be on the live hosts (not missing). Otherwise, the cluster will not be started up.
            //LeaderAppointer will ignore these hosts during startup.
            int sph = hostInfos.values().iterator().next().m_localSitesCount;
            int missingHostId = Integer.MAX_VALUE;
            Set<Integer> missingHosts = Sets.newHashSet();
            for (int i = 0; i < m_config.m_missingHostCount; i++) {
                hostInfos.put(missingHostId, new HostInfo("", AbstractTopology.PLACEMENT_GROUP_DEFAULT, sph, ""));
                missingHosts.add(missingHostId--);
            }
            int totalSites = sph * hostcount;
            if (totalSites % (kfactor + 1) != 0) {
                VoltDB.crashLocalVoltDB("Total number of sites is not divisible by the number of partitions.");
            }
            topology = AbstractTopology.getTopology(hostInfos, missingHosts, kfactor,
                    (m_config.m_restorePlacement && m_config.m_startAction.doesRecover()));
            String err;
            if ((err = topology.validateLayout(m_messenger.getLiveHostIds())) != null) {
                hostLog.warn("Unable to find optimal placement layout. " + err);
                hostLog.warn("When using placement groups, follow two rules to get better cluster availability:\n" +
                             "   1. Each placement group must have the same number of nodes, and\n" +
                             "   2. The number of partition replicas (kfactor + 1) must be a multiple of the number of placement groups.");
            }
            if (topology.hasMissingPartitions()) {
                VoltDB.crashLocalVoltDB("Some partitions are missing in the topology");
            }
            if (m_config.m_restorePlacement && m_config.m_startAction.doesRecover() && topology.version > 1) {
                consoleLog.info("Partition placement has been restored.");
            }
            topology = TopologyZKUtils.registerTopologyToZK(m_messenger.getZK(), topology);
        }
        return topology;
    }

    private TreeMap<Integer, Initiator> createIv2Initiators(Collection<Integer> partitions,
            StartAction startAction)
    {
        TreeMap<Integer, Initiator> initiators = new TreeMap<>();
        // Needed when static is reused by ServerThread
        TransactionTaskQueue.resetScoreboards(m_messenger.getNextSiteId(), m_nodeSettings.getLocalSitesCount());
        for (Integer partition : partitions)
        {
            Initiator initiator = new SpInitiator(m_messenger, partition, getStatsAgent(),
                    m_snapshotCompletionMonitor, startAction);
            initiators.put(partition, initiator);
        }
        if (StartAction.JOIN.equals(startAction)) {
            TransactionTaskQueue.initBarrier(m_nodeSettings.getLocalSitesCount());
        }
        else if (startAction.doesRejoin()) {
            RejoinProducer.initBarrier(m_nodeSettings.getLocalSitesCount());
        }
        return initiators;
    }

    private void createSecondaryConnections(boolean isRejoin) {
        int partitionGroupCount = getHostCount() / (m_configuredReplicationFactor + 1);
        if (m_configuredReplicationFactor > 0 && partitionGroupCount > 1) {
            m_messenger.createAuxiliaryConnections(isRejoin);
        }
    }

    private final List<ScheduledFuture<?>> m_periodicWorks = new ArrayList<>();

    /**
     * Schedule all the periodic works
     */
    private void schedulePeriodicWorks() {
        // JMX stats broadcast
        m_periodicWorks.add(scheduleWork(new Runnable() {
            @Override
            public void run() {
                // A null here was causing a steady stream of annoying but apparently inconsequential
                // NPEs during a debug session of an unrelated unit test.
                if (m_statsManager != null) {
                    m_statsManager.sendNotification();
                }
            }
        }, 0, StatsManager.POLL_INTERVAL, TimeUnit.MILLISECONDS));

        // clear login count
        m_periodicWorks.add(scheduleWork(new Runnable() {
            @Override
            public void run() {
                ScheduledExecutorService es = VoltDB.instance().getSES(false);
                if (es != null && !es.isShutdown()) {
                    es.submit(new Runnable() {
                        @Override
                        public void run()
                        {
                            long timestamp = System.currentTimeMillis();
                            m_flc.checkCounter(timestamp);
                        }
                    });
                }
            }
        }, 0, 10, TimeUnit.SECONDS));

        // small stats samples
        m_periodicWorks.add(scheduleWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(false, false);
            }
        }, 0, 5, TimeUnit.SECONDS));

        // medium stats samples
        m_periodicWorks.add(scheduleWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(true, false);
            }
        }, 0, 1, TimeUnit.MINUTES));

        // large stats samples
        m_periodicWorks.add(scheduleWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(true, true);
            }
        }, 0, 6, TimeUnit.MINUTES));

        // other enterprise setup
        EnterpriseMaintenance em = EnterpriseMaintenance.get();
        if (em != null) { em.setupMaintenanceTasks(); }

        GCInspector.instance.start(m_periodicPriorityWorkThread, m_gcStats);
    }

    /**
     * This host can be a leader if partition 0 is on it.
     * This is because the partition group with partition 0 can never be removed by elastic remove.
     * In the master only mode, this also guarantee the MPI host will always have viable site to do borrow task.
     *
     * @param partitions          {@link List} of partitions on this host
     * @param partitionGroupPeers {@link List} of hostIds which are in the same partition group as this host
     * @param topology            {@link AbstractTopology} for the cluster
     * @return {@code true} if this host is eligible as a leader for non partition services
     */
    private boolean determineIfEligibleAsLeader(Collection<Integer> partitions, Set<Integer> partitionGroupPeers,
            AbstractTopology topology) {
        return partitions.contains(0);
    }

    /**
     * @return The number of local partition initiators not including MPI
     */
    private int getLocalPartitionCount() {
        return m_iv2Initiators.size() - (m_eligibleAsLeader ? 1 : 0);
    }

    @Override
    public boolean isClusterComplete() {
        return (getHostCount() == m_messenger.getLiveHostIds().size());
    }

    private void startMigratePartitionLeaderTask() {
        final boolean disableSpiTask = "true".equals(System.getProperty("DISABLE_MIGRATE_PARTITION_LEADER", "false"));
        if (disableSpiTask) {
            hostLog.info("MigratePartitionLeader is not scheduled.");
            return;
        }

        //MigratePartitionLeader service will be started up only after the last rejoining has finished
        if(!isClusterComplete() || getHostCount() == 1 || m_configuredReplicationFactor == 0) {
            return;
        }

        //So remove any blocker or persisted data on ZK.
        VoltZK.removeMigratePartitionLeaderInfo(m_messenger.getZK());
        VoltZK.removeActionBlocker(m_messenger.getZK(), VoltZK.migratePartitionLeaderBlocker, hostLog);

        MigratePartitionLeaderMessage msg = new MigratePartitionLeaderMessage();
        msg.setStartTask();
        final int minimalNumberOfLeaders = (m_cartographer.getPartitionCount() / getHostCount());
        Set<Integer> hosts = m_messenger.getLiveHostIds();
        for (int hostId : hosts) {
            final int currentMasters = m_cartographer.getMasterCount(hostId);
            if (currentMasters > minimalNumberOfLeaders) {
                if (hostLog.isDebugEnabled()) {
                    hostLog.debug("Host " + hostId + " has more than " + minimalNumberOfLeaders +
                            ". Sending migrate partition message");
                }
                m_messenger.send(CoreUtils.getHSIdFromHostAndSite(hostId,
                        HostMessenger.CLIENT_INTERFACE_SITE_ID), msg);
            }
        }
    }

    private void startHealthMonitor() {
        if (resMonitorWork != null) {
            m_globalServiceElector.unregisterService(m_healthMonitor);
            resMonitorWork.cancel(false);
            try {
                resMonitorWork.get();
            } catch(Exception e) { } // Ignore exceptions because we don't really care about the result here.
            m_periodicWorks.remove(resMonitorWork);
        }
        m_healthMonitor  = new HealthMonitor(m_catalogContext.getDeployment().getSystemsettings(), getSnmpTrapSender());
        m_healthMonitor.logResourceLimitConfigurationInfo();
        if (m_healthMonitor.hasResourceLimitsConfigured()) {
            m_globalServiceElector.registerService(m_healthMonitor);
            resMonitorWork = scheduleWork(m_healthMonitor, m_healthMonitor.getResourceCheckInterval(), m_healthMonitor.getResourceCheckInterval(), TimeUnit.SECONDS);
            m_periodicWorks.add(resMonitorWork);
        }
    }

    /**
     * Takes the deployment file given at initialization and the voltdb root given as
     * a command line options, and it performs the following tasks:
     * <p><ul>
     * <li>creates if necessary the voltdbroot directory
     * <li>fail if voltdbroot is already configured and populated with database artifacts
     * <li>creates command log, DR, snapshot, and export directories
     * <li>creates the config directory under voltdbroot
     * <li>moves the deployment file under the config directory
     * </ul>
     * @param config
     * @param dt a {@link DeploymentType}
     */
    private void stageDeploymentFileForInitialize(Configuration config, DeploymentType dt) {

        String deprootFN = dt.getPaths().getVoltdbroot().getPath();
        File   deprootFH = new File(deprootFN);
        File   cnfrootFH = config.m_voltdbRoot;

        if (!cnfrootFH.exists() && !cnfrootFH.mkdirs()) {
            VoltDB.crashLocalVoltDB("Unable to create the voltdbroot directory in " + cnfrootFH);
        }
        try {
            File depcanoFH = null;
            try {
                depcanoFH = deprootFH.getCanonicalFile();
            } catch (IOException e) {
                depcanoFH = deprootFH;
            }
            File cnfcanoFH = cnfrootFH.getCanonicalFile();
            if (!cnfcanoFH.equals(depcanoFH)) {
                dt.getPaths().getVoltdbroot().setPath(cnfrootFH.getPath());
            }
            // root in deployment conflicts with command line voltdbroot
            if (!VoltDB.DBROOT.equals(deprootFN)) {
                consoleLog.info("Ignoring voltdbroot \"" + deprootFN + "\" specified in the deployment file");
                hostLog.info("Ignoring voltdbroot \"" + deprootFN + "\" specified in the deployment file");
            }
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB(
                    "Unable to resolve voltdbroot location: " + config.m_voltdbRoot,
                    false, e);
            return;
        }

        // check for already existing artifacts
        List<String> nonEmptyPaths = managedPathsWithFiles(config, dt);
        if (!nonEmptyPaths.isEmpty()) {
            StringBuilder crashMessage =
                    new StringBuilder("Files from a previous database session exist in the managed directories:");
            for (String nonEmptyPath : nonEmptyPaths) {
                crashMessage.append("\n  - " + nonEmptyPath);
            }
            crashMessage.append("\nUse the start command to start the initialized database or use init --force" +
                " to initialize a new database session overwriting existing files.");
            VoltDB.crashLocalVoltDB(crashMessage.toString());
            return;
        }
        // create the config subdirectory
        File confDH = getConfigDirectory(config);
        if (!confDH.exists() && !confDH.mkdirs()) {
            VoltDB.crashLocalVoltDB("Unable to create the config directory " + confDH);
            return;
        }
        // create the large query swap subdirectory
        File largeQuerySwapDH = new File(getLargeQuerySwapPath());
        if (! largeQuerySwapDH.exists() && !largeQuerySwapDH.mkdirs()) {
            VoltDB.crashLocalVoltDB("Unable to create the large query swap directory " + confDH);
            return;
        }
        // create the remaining paths
        if (config.m_isEnterprise) {
            List<String> failed = m_nodeSettings.ensureDirectoriesExist();
            if (!failed.isEmpty()) {
                String msg = "Unable to access or create the following directories:\n    "
                        + Joiner.on("\n    ").join(failed);
                VoltDB.crashLocalVoltDB(msg);
                return;
            }
        }

        //Now its safe to Save .paths
        m_nodeSettings.store();

         //Now that we are done with deployment configuration set all path null.
         dt.setPaths(null);

        // log message unconditionally indicating that the provided host-count and admin-mode settings in
        // deployment, if any, will be ignored
        consoleLog.info("When using the INIT command, some deployment file settings (hostcount and voltdbroot path) "
                + "are ignored");
        hostLog.info("When using the INIT command, some deployment file settings (hostcount and voltdbroot path) are "
                + "ignored");

        File depFH = getConfigLogDeployment(config);
        try (FileWriter fw = new FileWriter(depFH)) {
            fw.write(CatalogUtil.getDeployment(dt, true /* pretty print indent */));
        } catch (IOException|RuntimeException e) {
            VoltDB.crashLocalVoltDB("Unable to marshal deployment configuration to " + depFH, false, e);
        }

        // Save cluster settings properties derived from the deployment file
        ClusterSettings.create(CatalogUtil.asClusterSettingsMap(dt)).store();
    }

    private void stageSchemaFiles(Configuration config, boolean isXCDR) {
        if (config.m_userSchemas == null && config.m_stagedClassesPaths == null) {
            return; // nothing to do
        }
        File stagedCatalogFH = new File(getStagedCatalogPath(getVoltDBRootPath()));

        if (!config.m_forceVoltdbCreate && stagedCatalogFH.exists()) {
            VoltDB.crashLocalVoltDB("A previous database was initialized with a schema. You must init with --force to overwrite the schema.");
        }
        final boolean standalone = false;
        VoltCompiler compiler = new VoltCompiler(standalone, isXCDR);

        compiler.setInitializeDDLWithFiltering(true);
        if (!compiler.compileFromSchemaAndClasses(config.m_userSchemas, config.m_stagedClassesPaths, stagedCatalogFH)) {
            VoltDB.crashLocalVoltDB("Could not compile specified schema " + config.m_userSchemas);
        }
    }

    private void stageInitializedMarker(Configuration config) {
        File depFH = new File(config.m_voltdbRoot, VoltDB.INITIALIZED_MARKER);
        try (PrintWriter pw = new PrintWriter(new FileWriter(depFH), true)) {
            pw.println(config.m_clusterName);
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("Unable to stage cluster name destination", false, e);
        }
    }

    private void deleteInitializationMarkers(Configuration configuration) {
        for (File c: configuration.getInitMarkers()) {
            MiscUtils.deleteRecursively(c);
        }
    }

    public static final String SECURITY_OFF_WARNING = "User authentication is not enabled."
            + " The database is accessible and could be modified or shut down by anyone on the network.";

    private byte[] resolveDeploymentWithZK(byte[] localDeploymentBytes) throws KeeperException, InterruptedException {
        // get from zk
        ZooKeeper zk = m_messenger.getZK();
        CatalogAndDeployment catalogStuff;
        do {
            catalogStuff = CatalogUtil.getCatalogFromZK(zk);
        } while (catalogStuff == null);

        // compare local with remote deployment file
        byte[] deploymentBytesTemp = catalogStuff.deploymentBytes;
        if (deploymentBytesTemp != null) {
            //Check hash if its a supplied deployment on command line.
            //We will ignore the supplied or default deployment anyways.
            if (localDeploymentBytes != null && !m_config.m_deploymentDefault) {
                byte[] deploymentHashHere =
                        CatalogUtil.makeHash(localDeploymentBytes);
                byte[] deploymentHash =
                        CatalogUtil.makeHash(deploymentBytesTemp);
                if (!(Arrays.equals(deploymentHashHere, deploymentHash))) {
                    hostLog.warn("The locally provided deployment configuration did not " +
                            "match the configuration information found in the cluster.");
                } else {
                    hostLog.info("Deployment configuration pulled from other cluster node.");
                }
            }
            //Use remote deployment obtained.
            return deploymentBytesTemp;
        } else if(localDeploymentBytes != null){
            hostLog.warn("Could not loaded remote deployement file. Use local deployment: " + m_config.m_pathToDeployment);
            return localDeploymentBytes;
        }
        hostLog.error("Deployment file could not be loaded locally or remotely, "
                + "local supplied path: " + m_config.m_pathToDeployment);
        return null;
    }

    boolean readDeploymentAndCreateStarterCatalogContext() {
        /*
         * Debate with the cluster what the deployment file should be
         */
        try {
            ZooKeeper zk = m_messenger.getZK();
            byte[] deploymentBytes = null;

            try {
                deploymentBytes = org.voltcore.utils.CoreUtils.urlToBytes(m_config.m_pathToDeployment);
            } catch (Exception ex) {
                //Let us get bytes from ZK
            }

            try {
                // Didn't find local deployment file or join/rejoin case
                if (deploymentBytes == null || m_rejoining || m_joining) {
                    deploymentBytes = resolveDeploymentWithZK(deploymentBytes);
                } else {
                    CatalogUtil.writeDeploymentToZK(zk,
                            0L,
                            SegmentedCatalog.create(new byte[0], new byte[]{0}, deploymentBytes),
                            ZKCatalogStatus.COMPLETE,
                            -1);//dummy txnId
                    hostLog.info("URL of deployment: " + m_config.m_pathToDeployment);
                }
            } catch (KeeperException.NodeExistsException e) {
                deploymentBytes = resolveDeploymentWithZK(deploymentBytes);
            } catch(KeeperException.NoNodeException e) {
                // no deploymentBytes case is handled below. So just log this error.
                if (hostLog.isDebugEnabled()) {
                    hostLog.debug("Error trying to get deployment bytes from cluster", e);
                }
            }
            if (deploymentBytes == null) {
                hostLog.error("Deployment information could not be obtained from cluster node or locally");
                VoltDB.crashLocalVoltDB("No such deployment file: " + m_config.m_pathToDeployment);
            }

            DeploymentType deployment = CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));

            // wasn't a valid xml deployment file
            if (deployment == null) {
                hostLog.error("Not a valid XML deployment file at URL: " + m_config.m_pathToDeployment);
                VoltDB.crashLocalVoltDB("Not a valid XML deployment file at URL: " + m_config.m_pathToDeployment);
            }

            checkForEnterpriseFeatures(deployment, true);

            // note the heart beats are specified in seconds in xml, but ms internally
            HeartbeatType hbt = deployment.getHeartbeat();
            if (hbt != null) {
                m_config.m_deadHostTimeoutMS = hbt.getTimeout() * 1000;
                m_messenger.setDeadHostTimeout(m_config.m_deadHostTimeoutMS);
            } else {
                hostLog.info("Dead host timeout set to " + m_config.m_deadHostTimeoutMS + " milliseconds");
            }

            PartitionDetectionType pt = deployment.getPartitionDetection();
            if (pt != null) {
                m_config.m_partitionDetectionEnabled = pt.isEnabled();
                m_messenger.setPartitionDetectionEnabled(m_config.m_partitionDetectionEnabled);
            }

            // log system setting information
            SystemSettingsType sysType = deployment.getSystemsettings();
            if (sysType != null) {
                if (sysType.getElastic() != null) {
                    hostLog.info("Elastic duration set to " + sysType.getElastic().getDuration() + " milliseconds");
                    hostLog.info("Elastic throughput set to " + sysType.getElastic().getThroughput() + " mb/s");
                }
                if (sysType.getTemptables() != null) {
                    hostLog.info("Max temptable size set to " + sysType.getTemptables().getMaxsize() + " mb");
                }
                if (sysType.getSnapshot() != null) {
                    hostLog.info("Snapshot priority set to " + sysType.getSnapshot().getPriority() + " (if > 0, delays snapshot tasks)");
                }
                if (sysType.getQuery() != null) {
                    if (sysType.getQuery().getTimeout() > 0) {
                        hostLog.info("Query timeout set to " + sysType.getQuery().getTimeout() + " milliseconds");
                        m_config.m_queryTimeout = sysType.getQuery().getTimeout();
                    }
                    else if (sysType.getQuery().getTimeout() == 0) {
                        hostLog.info("Query timeout set to unlimited");
                        m_config.m_queryTimeout = 0;
                    }
                }
            }

            // log a warning on console log if security setting is turned off, like durability warning.
            SecurityType securityType = deployment.getSecurity();
            if (securityType == null || !securityType.isEnabled()) {
                consoleLog.warn(SECURITY_OFF_WARNING);
            }

            // create a dummy catalog to load deployment info into
            Catalog catalog = new Catalog();
            // Need these in the dummy catalog
            Cluster cluster = catalog.getClusters().add("cluster");
            cluster.getDatabases().add("database");

            String result = CatalogUtil.compileDeployment(catalog, deployment, true);
            if (result != null) {
                // Any other non-enterprise deployment errors will be caught and handled here
                // (such as <= 0 host count)
                VoltDB.crashLocalVoltDB(result);
            }

            m_catalogContext = new CatalogContext(catalog,
                                                  new DbSettings(m_clusterSettings, m_nodeSettings),
                                                  0, //timestamp
                                                  0,
                                                  new byte[] {},
                                                  null,
                                                  deploymentBytes,
                                                  m_messenger);

            m_configuredReplicationFactor = getCatalogContext().getDeployment().getCluster().getKfactor();
            return ((deployment.getCommandlog() != null) && (deployment.getCommandlog().isEnabled()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void loadLegacyPathProperties(DeploymentType deployment) throws IOException {
        //Load deployment paths now if Legacy so that we access through the interface all the time.
        if (isRunningWithOldVerbs() && m_nodeSettings == null) {
            m_nodeSettings = NodeSettings.create(CatalogUtil.asNodeSettingsMap(deployment));
            List<String> failed = m_nodeSettings.ensureDirectoriesExist();
            if (!failed.isEmpty()) {
                String msg = "Unable to validate path settings:\n  " +
                        Joiner.on("\n  ").join(failed);
                hostLog.fatal(msg);
                throw new IOException(msg);
            }
        }
    }

    static class ReadDeploymentResults {
        final byte [] deploymentBytes;
        final DeploymentType deployment;

        ReadDeploymentResults(byte [] deploymentBytes, DeploymentType deployment) {
            this.deploymentBytes = deploymentBytes;
            this.deployment = deployment;
        }
    }

    ReadDeploymentResults readPrimedDeployment(Configuration config) {
        /*
         * Debate with the cluster what the deployment file should be
         */
        try {
            byte deploymentBytes[] = null;

            try {
                deploymentBytes = org.voltcore.utils.CoreUtils.urlToBytes(config.m_pathToDeployment);
            } catch (Exception ex) {
                //Let us get bytes from ZK
            }

            if (deploymentBytes == null) {
                hostLog.error("Deployment information could not be obtained from cluster node or locally");
                VoltDB.crashLocalVoltDB("No such deployment file: " + config.m_pathToDeployment);
            }
            DeploymentType deployment =
                CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));
            // wasn't a valid xml deployment file
            if (deployment == null) {
                hostLog.error("Not a valid XML deployment file at URL: " + config.m_pathToDeployment);
                VoltDB.crashLocalVoltDB("Not a valid XML deployment file at URL: " + config.m_pathToDeployment);
                return new ReadDeploymentResults(deploymentBytes, deployment);
            }
            // Set local sites count
            config.m_sitesperhost = deployment.getCluster().getSitesperhost();
            NodeSettings nodeSettings = null;
            // adjust deployment host count when the cluster members are given by mesh configuration
            // providers
            switch(config.m_startAction) {
            case GET:
                // once a voltdbroot is inited, the path properties contain the true path values
                Settings.initialize(config.m_voltdbRoot);
                // only override the local sites count
                nodeSettings = NodeSettings.create(config.asNodeSettingsMap(),
                        config.asRelativePathSettingsMap());
                break;
            case PROBE:
                // once a voltdbroot is inited, the path properties contain the true path values
                Settings.initialize(config.m_voltdbRoot);
                // only override the local sites count
                nodeSettings = NodeSettings.create(config.asNodeSettingsMap(),
                        config.asRelativePathSettingsMap());
                File nodeSettingsFH = new File(getConfigDirectory(config), "path.properties");
                consoleLog.info("Loaded node-specific settings from " + nodeSettingsFH.getPath());
                hostLog.info("Loaded node-specific settings from " + nodeSettingsFH.getPath());
                break;
            case INITIALIZE:
                Settings.initialize(config.m_voltdbRoot);
                // voltdbroot value from config overrides voltdbroot value in the deployment
                // file
                nodeSettings = NodeSettings.create(
                        config.asNodeSettingsMap(),
                        config.asPathSettingsMap(),
                        CatalogUtil.asNodeSettingsMap(deployment));
                break;
            default:
                nodeSettings = NodeSettings.create(
                        config.asNodeSettingsMap(),
                        CatalogUtil.asNodeSettingsMap(deployment));
                Settings.initialize(nodeSettings.getVoltDBRoot());
                config.m_voltdbRoot = nodeSettings.getVoltDBRoot();
                break;
            }
            m_nodeSettings = nodeSettings;
            //Now its safe to save node settings
            if (config.m_startAction != StartAction.GET) {
                m_nodeSettings.store();
            }

            if (config.m_startAction == StartAction.PROBE) {
                // once initialized the path properties contain the true path values
                if (config.m_hostCount == VoltDB.UNDEFINED) {
                    config.m_hostCount = 1;
                }
            } else {
                config.m_hostCount = deployment.getCluster().getHostcount();
            }

            checkForEnterpriseFeatures(deployment, false);
            return new ReadDeploymentResults(deploymentBytes, deployment);
        } catch (Exception e) {
            /*
             * When a settings exception is caught (e.g. reading a broken properties file),
             * we probably just want to crash the DB anyway
             */
            consoleLog.fatal(e.getMessage());
            VoltDB.crashLocalVoltDB(e.getMessage());
            return null;
        }
    }

    /**
     * Check for invalid deployment file settings (enterprise-only) in the community edition. Trick here is to print out
     * all applicable problems and then stop, rather than stopping after the first one is found.
     */
    private void checkForEnterpriseFeatures(DeploymentType deployment, boolean checkForDr) {
        if (!m_config.m_isEnterprise) {
            boolean shutdownDeployment = false;
            boolean shutdownAction = false;

            // check license features for community version
            if ((deployment.getCommandlog() != null) && (deployment.getCommandlog().isEnabled())) {
                consoleLog.error("Command logging is not supported in the community edition of VoltDB.");
                shutdownDeployment = true;
            }
            if (deployment.getTopics() != null && deployment.getTopics().isEnabled()) {
                consoleLog.error("Topics feature is not supported in the community edition of VoltDB.");
                shutdownDeployment = true;
            }
            if (checkForDr && deployment.getDr() != null && deployment.getDr().getRole() != DrRoleType.NONE) {
                consoleLog.warn("Database Replication is not supported in the community edition of VoltDB.");
            }
            // check the start action for the community edition
            if (m_config.m_startAction == StartAction.JOIN) {
                consoleLog.error("VoltDB Community Edition does not support adding nodes to a running cluster. "
                        + "You must stop, reconfigure, and restart the cluster to increase its size.");
                shutdownAction = true;
            }

            // if the process needs to stop, try to be helpful
            if (shutdownAction || shutdownDeployment) {
                String msg = "This process will exit. Please run VoltDB with ";
                if (shutdownDeployment) {
                    msg += "a deployment file compatible with the community edition";
                }
                if (shutdownDeployment && shutdownAction) {
                    msg += " and ";
                }

                if (shutdownAction && !shutdownDeployment) {
                    msg += "the CREATE start action";
                }
                msg += ".";

                VoltDB.crashLocalVoltDB(msg);
            }
        }
    }

    void collectLocalNetworkMetadata() {
        boolean threw = false;
        boolean isDebug = hostLog.isDebugEnabled();
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            stringer.key("interfaces").array();

            /*
             * If no interface was specified, do a ton of work
             * to identify all ipv4 or ipv6 interfaces and
             * marshal them into JSON. Always put the ipv4 address first
             * so that the export client will use it
             */
            if (m_config.m_externalInterface.equals("")) {
                hostLog.info("Enumerating interfaces");
                LinkedList<NetworkInterface> interfaces = new LinkedList<>();
                try {
                    Enumeration<NetworkInterface> intfEnum = NetworkInterface.getNetworkInterfaces();
                    while (intfEnum.hasMoreElements()) {
                        NetworkInterface intf = intfEnum.nextElement();
                        if (isDebug) {
                            hostLog.debug("Found interface " + intf);
                        }
                        if (intf.isLoopback()) {
                            hostLog.debug("Skipping loopback interface");
                            continue;
                        }
                        if (!intf.isUp()) {
                            hostLog.debug("Interface is not up");
                            continue;
                        }
                        interfaces.offer(intf);
                    }
                } catch (SocketException e) {
                    hostLog.error("Exception from NetworkInterface.getNetworkInterfaces()", e);
                    throw new RuntimeException(e);
                }

                if (interfaces.isEmpty()) {
                    hostLog.warn("Found no interfaces, so using 'localhost'");
                    stringer.value("localhost");
                } else {
                    boolean skippedLocal = false;
                    boolean addedIp = false;
                    while (!interfaces.isEmpty()) {
                        NetworkInterface intf = interfaces.poll();
                        Enumeration<InetAddress> inetAddrs = intf.getInetAddresses();
                        Inet6Address inet6addr = null;
                        Inet4Address inet4addr = null;
                        while (inetAddrs.hasMoreElements()) {
                            InetAddress addr = inetAddrs.nextElement();
                            if (addr instanceof Inet6Address) {
                                Inet6Address temp = (Inet6Address)addr;
                                if (temp.isLinkLocalAddress()) {
                                    hostLog.info("Ignoring link-local address " + temp);
                                    skippedLocal = true;
                                } else {
                                    inet6addr = temp;
                                    if (isDebug) {
                                        hostLog.debug("Found IPv6 address " + inet6addr);
                                    }
                                }
                            } else if (addr instanceof Inet4Address) {
                                Inet4Address temp = (Inet4Address)addr;
                                if (temp.isLinkLocalAddress()) {
                                    hostLog.info("Ignoring link-local address " + temp);
                                    skippedLocal = true;
                                } else {
                                    inet4addr = temp;
                                    if (isDebug) {
                                        hostLog.debug("Found IPv4 address " + inet4addr);
                                    }
                                }
                            }
                        }
                        if (inet4addr != null) {
                            stringer.value(inet4addr.getHostAddress());
                            addedIp = true;
                        }
                        if (inet6addr != null) {
                            stringer.value(inet6addr.getHostAddress());
                            addedIp = true;
                        }
                    }
                    if (!addedIp) {
                        hostLog.warn("Did not find any usable address on any active interface, so using 'localhost'");
                        if (skippedLocal) {
                            hostLog.warn("A link-local address was found but was ignored");
                        }
                        stringer.value("localhost");
                    }
                }
            } else {
                hostLog.info("Using external interface " + m_config.m_externalInterface);
                stringer.value(m_config.m_externalInterface);
            }
        } catch (Exception e) {
            threw = true;
            hostLog.warn("Error while collecting data about local network interfaces", e);
        }
        try {
            if (threw) {
                hostLog.warn("Previous error encountered: using 'localhost'");
                stringer = new JSONStringer();
                stringer.object();
                stringer.key("interfaces").array();
                stringer.value("localhost");
                stringer.endArray();
            } else {
                stringer.endArray();
            }
            stringer.keySymbolValuePair("clientPort", m_config.m_port);
            stringer.keySymbolValuePair("clientInterface", m_config.m_clientInterface);
            stringer.keySymbolValuePair("adminPort", m_config.m_adminPort);
            stringer.keySymbolValuePair("adminInterface", m_config.m_adminInterface);
            stringer.keySymbolValuePair("httpPort", m_config.m_httpPort);
            stringer.keySymbolValuePair("httpInterface", m_config.m_httpPortInterface);
            stringer.keySymbolValuePair("internalPort", m_config.m_internalPort);
            stringer.keySymbolValuePair("internalInterface", m_config.m_internalInterface);
            stringer.keySymbolValuePair("zkPort", m_config.m_zkPort);
            stringer.keySymbolValuePair("zkInterface", m_config.m_zkInterface);
            stringer.keySymbolValuePair("drPort", VoltDB.getReplicationPort(m_catalogContext.cluster.getDrproducerport()));
            stringer.keySymbolValuePair("drInterface", VoltDB.getDefaultReplicationInterface());
            stringer.keySymbolValuePair(VoltZK.drPublicHostProp, VoltDB.getPublicReplicationInterface());
            stringer.keySymbolValuePair(VoltZK.drPublicPortProp, VoltDB.getPublicReplicationPort());
            stringer.keySymbolValuePair("publicInterface", m_config.m_publicInterface);
            stringer.keySymbolValuePair("topicsPublicHost", VoltDB.getPublicTopicsInterface());
            stringer.keySymbolValuePair("topicsPublicPort", VoltDB.getPublicTopicsPort());
            stringer.keySymbolValuePair("topicsport", VoltDB.getTopicsPort(VoltDB.DEFAULT_TOPICS_PORT));
            stringer.endObject();
            JSONObject obj = new JSONObject(stringer.toString());
            // possibly atomic swap from null to realz
            m_localMetadata = obj.toString(4);
            hostLog.debug("System Metadata is: " + m_localMetadata);
        } catch (Exception e) {
            hostLog.warn("Failed to collect data about lcoal network interfaces", e);
        }
    }

    @Override
    public boolean isBare() {
        return m_isBare;
    }
    void setBare(boolean flag) {
        m_isBare = flag;
    }

    private void cleanElasticSnapshots(ReadDeploymentResults readDepl) {
        if (!m_config.m_isEnterprise) {
            return;
        }
        DeploymentType deployment = readDepl.deployment;
        CommandLogType cl = deployment.getCommandlog();

        // No cleanup if my deployment has command logs enabled
        if (cl != null && cl.isEnabled()) {
            return;
        }

        PathsType paths = deployment.getPaths();
        String clSnapshotpath = getCommandLogSnapshotPath(paths.getCommandlogsnapshot());
        String voltDbRoot = getVoltDBRootPath(paths.getVoltdbroot());

        File managedPath = new File(clSnapshotpath);
        if (!managedPath.isAbsolute()) {
            managedPath = new File(voltDbRoot, clSnapshotpath);
        }
        if (!managedPath.canRead()) {
            return; // this is logged elsewhere...
        }
        String absPath = managedPath.getAbsolutePath();

        List<String> snapshotPrefixes = ElasticService.getSnapshotPrefixes();
        String[] content = managedPath.list((dir,name) -> snapshotPrefixes.stream().anyMatch(p -> name.startsWith(p)));
        if (content.length == 0) { // nothing we care about
            return;
        }

        List<String> deletion = Arrays.asList(content);
        hostLog.infoFmt("Deleting elastic snapshot files: %s", deletion);
        try {
            for (String delete : deletion) {
                File file = new File(absPath, delete);
                if (!file.delete()) {
                    VoltDB.crashLocalVoltDB("Failed to delete " + file.getAbsolutePath());
                }
            }
        }
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to delete elastic snapshot file", true, e);
        }
        hostLog.infoFmt("Deleted %d elastic snapshot files", deletion.size());
    }

    /**
     * Start the voltcore HostMessenger. This joins the node
     * to the existing cluster. In the non rejoin case, this
     * function will return when the mesh is complete. If
     * rejoining, it will return when the node and agreement
     * site are synched to the existing cluster.
     */
    MeshProber.Determination buildClusterMesh(ReadDeploymentResults readDepl) {
        final boolean bareAtStartup  = m_config.m_forceVoltdbCreate
                || pathsWithRecoverableArtifacts(readDepl.deployment).isEmpty();
        setBare(bareAtStartup);

        final Supplier<Integer> hostCountSupplier = new Supplier<Integer>() {
            @Override
            public Integer get() {
                return getHostCount();
            }
        };

        ClusterType clusterType = readDepl.deployment.getCluster();

        MeshProber criteria = MeshProber.builder()
                .coordinators(m_config.m_coordinators)
                .versionChecker(m_versionChecker)
                .enterprise(m_config.m_isEnterprise)
                .startAction(m_config.m_startAction)
                .bare(bareAtStartup)
                .configHash(CatalogUtil.makeDeploymentHashForConfig(readDepl.deploymentBytes))
                .hostCountSupplier(hostCountSupplier)
                .kfactor(clusterType.getKfactor())
                .paused(m_config.m_isPaused)
                .nodeStateSupplier(m_statusTracker.getSupplier())
                .addAllowed(m_config.m_enableAdd)
                .safeMode(m_config.m_safeMode)
                .terminusNonce(getTerminusNonce())
                .licenseHash(m_licensing.getLicenseHash())
                .missingHostCount(m_config.m_missingHostCount)
                .build();

        m_meshProbe.set(criteria);
        HostAndPort hostAndPort = criteria.getLeader();
        String hostname = hostAndPort.getHost();
        int port = hostAndPort.getPort();

        org.voltcore.messaging.HostMessenger.Config hmconfig;

        hmconfig = new org.voltcore.messaging.HostMessenger.Config(hostname, port, m_config.m_isPaused);
        if (m_config.m_placementGroup != null) {
            hmconfig.group = m_config.m_placementGroup;
        }
        hmconfig.internalPort = m_config.m_internalPort;
        hmconfig.internalInterface = m_config.m_internalInterface;
        hmconfig.zkPort = m_config.m_zkPort;
        hmconfig.zkInterface = m_config.m_zkInterface;
        hmconfig.deadHostTimeout = m_config.m_deadHostTimeoutMS;
        hmconfig.factory = new VoltDbMessageFactory();
        hmconfig.coreBindIds = m_config.m_networkCoreBindings;
        hmconfig.acceptor = criteria;
        hmconfig.localSitesCount = m_config.m_sitesperhost;
        // OpsAgents can handle unknown site ID response so register those sites for that response
        hmconfig.respondUnknownSite = Stream.of(OpsSelector.values()).map(OpsSelector::getSiteId)
                .collect(Collectors.toSet());
        if (!StringUtils.isEmpty(m_config.m_recoveredPartitions)) {
            hmconfig.recoveredPartitions = m_config.m_recoveredPartitions;
        }
        //if SSL needs to be enabled for internal communication, SSL context has to be setup before starting HostMessenger
        SslSetup.setupSSL(m_config, readDepl.deployment);
        if (m_config.m_sslInternal) {
            m_messenger = new org.voltcore.messaging.HostMessenger(
                    hmconfig,
                    this,
                    m_config.m_sslServerContext,
                    m_config.m_sslClientContext,
                    CoreUtils.getHostnameOrAddress()
            );
        } else {
            m_messenger = new org.voltcore.messaging.HostMessenger(
                    hmconfig,
                    this,
                    CoreUtils.getHostnameOrAddress()
            );
        }

        hostLog.infoFmt("Beginning inter-node communication on port %d.", m_config.m_internalPort);

        try {
            m_messenger.start();
        } catch (Exception e) {
            boolean printStackTrace =  true;
            // do not log fatal exception message in these cases
            if (e.getMessage() != null) {
                if (e.getMessage().indexOf(SocketJoiner.FAIL_ESTABLISH_MESH_MSG) > -1 ||
                        e.getMessage().indexOf(MeshProber.MESH_ONE_REJOIN_MSG )> -1) {
                    printStackTrace = false;
                }
            }
            VoltDB.crashLocalVoltDB(e.getMessage(), printStackTrace, e);
        }

        VoltZK.createPersistentZKNodes(m_messenger.getZK());

        // Use the host messenger's hostId.
        m_myHostId = m_messenger.getHostId();
        hostLog.infoFmt("Host id of this node is: %d", m_myHostId);
        consoleLog.infoFmt("Host id of this node is: %d", m_myHostId);

        // This is where we wait
        MeshProber.Determination determination = criteria.waitForDetermination();
        m_meshProbe.set(null);
        if (determination.startAction == null) {
            VoltDB.crashLocalVoltDB("Shutdown invoked before Cluster Mesh was established.");
        }

        // paused is determined in the mesh formation exchanged
        if (determination.paused) {
            m_messenger.pause();
        } else {
            m_messenger.unpause();
        }

        // Semi-hacky check to see if we're attempting to rejoin to ourselves.
        // The leader node gets assigned host ID 0, always, so if we're the
        // leader and we're rejoining, this is clearly bad.
        if (m_myHostId == 0 && determination.startAction.doesJoin()) {
            VoltDB.crashLocalVoltDB("Unable to rejoin a node to itself.  " +
                    "Please check your command line and start action and try again.");
        }
        // load or store settings form/to zookeeper
        if (determination.startAction.doesJoin()) {
            m_clusterSettings.load(m_messenger.getZK());
            m_clusterSettings.get().store();
        } else if (m_myHostId == 0) {
            if (hostLog.isDebugEnabled()) {
                hostLog.debug("Writing initial hostcount " +
                               m_clusterSettings.get().getProperty(ClusterSettings.HOST_COUNT) +
                               " to ZK");
            }
            m_clusterSettings.store(m_messenger.getZK());
        }
        m_clusterCreateTime = m_messenger.getInstanceId().getTimestamp();
        return determination;
    }

    public static String[] extractBuildInfo(VoltLogger logger) {
        StringBuilder sb = new StringBuilder(64);
        try {
            InputStream buildstringStream =
                ClassLoader.getSystemResourceAsStream("buildstring.txt");
            if (buildstringStream != null) {
                byte b;
                while ((b = (byte) buildstringStream.read()) != -1) {
                    sb.append((char)b);
                }
                String parts[] = sb.toString().split(" ", 2);
                if (parts.length == 2) {
                    parts[0] = parts[0].trim();
                    parts[1] = parts[0] + "_" + parts[1].trim();
                    return parts;
                }
            }
        } catch (Exception ignored) {
        }
        try {
            InputStream versionstringStream = new FileInputStream("version.txt");
            try {
                byte b;
                while ((b = (byte) versionstringStream.read()) != -1) {
                    sb.append((char)b);
                }
                return new String[] { sb.toString().trim(), "VoltDB" };
            } finally {
                versionstringStream.close();
            }
        }
        catch (Exception ignored2) {
            if (logger != null) {
                logger.error("Exception while retrieving build string");
            }
            return new String[] { m_defaultVersionString, "VoltDB" };
        }
    }

    @Override
    public void readBuildInfo() {
        String buildInfo[] = extractBuildInfo(hostLog);
        m_versionString = buildInfo[0];
        m_buildString = buildInfo[1];
        String buildString = m_buildString;
        if (m_buildString.contains("_")) {
            buildString = m_buildString.split("_", 2)[1];
        }
        // the software 'edition' is not the same as the license type
        String edition = MiscUtils.isPro() ? "Enterprise Edition" : "Community Edition";
        consoleLog.infoFmt("Build: %s %s %s", m_versionString, buildString, edition);
    }

    /**
     * Start all the site's event loops. That's it.
     */
    @Override
    public void run() {
        if (m_restoreAgent != null) {
            // start restore process
            m_restoreAgent.restore();
        }
        else {
            onSnapshotRestoreCompletion();
            onReplayCompletion(Long.MIN_VALUE, m_iv2InitiatorStartingTxnIds);
        }

        // Start the rejoin coordinator
        if (m_joinCoordinator != null) {
            try {
                m_statusTracker.set(NodeState.REJOINING);
                if (!m_joinCoordinator.startJoin(m_catalogContext.database)) {
                    VoltDB.crashLocalVoltDB("Failed to join the cluster", true);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to join the cluster", true, e);
            }
        }

        m_isRunning = true;
    }

    /**
     * Try to shut everything down so the system is ready to call
     * initialize again.
     *
     * There is protection to ensure that only one thread will execute
     * the shutdown sequence. The return value is true for that thread,
     * false for others. A thread getting a false return should simply
     * wait for the shutdown to complete.
     *
     * The protection works like this:
     *
     * 1. Generally, the first thread through immediately sets the mode
     *    to SHUTTINGDOWN, which prevents others from doing anything.
     *    SHUTTINGDOWN persists until JVM exit. The flag m_isRunning
     *    will go false at the end of shutdown.
     *
     * 2. But in the case that the mode is not SHUTTINGDOWN and the
     *    flag m_isRunning is false, this means that we were never
     *    really up. We don't set SHUTTINGDOWN in this case, and that
     *    means we can't know if we are the first thread. It is assumed
     *    that this does not matter and only occurs in test cases.
     *    (In case this was a pre-existing condition)
     *
     * @param unused thread
     * @return true iff this thread executed the shutdown
     */
    @Override
    public boolean shutdown(Thread unused) throws InterruptedException {
        MeshProber criteria = m_meshProbe.get();
        if (criteria != null) {
            criteria.abortDetermination();
        }
        synchronized (m_startAndStopLock) {
            boolean did_it = false;
            if (m_mode != OperationMode.SHUTTINGDOWN) {
                if (!m_isRunning) { // initialize() was never called
                    return true;
                }
                did_it = true;
                m_mode = OperationMode.SHUTTINGDOWN;
                m_statusTracker.set(NodeState.SHUTTINGDOWN);

                if (m_catalogContext != null && m_catalogContext.m_ptool.getAdHocLargeFallbackCount() > 0) {
                    hostLog.infoFmt("%d queries planned through @AdHocLarge were converted to normal @AdHoc plans.",
                                    m_catalogContext.m_ptool.getAdHocLargeFallbackCount());
                }
                /*
                 * Various scheduled tasks get crashy in unit tests if they happen to run
                 * while other stuff is being shut down. Double catch of throwable is only for the sake of tests.
                 */
                try {
                    for (ScheduledFuture<?> sc : m_periodicWorks) {
                        sc.cancel(false);
                        try {
                            sc.get();
                        } catch (Throwable t) { }
                    }
                } catch (Throwable t) { }

                m_avroSerde.shutdown();
                m_taskManager.shutdown();

                //Shutdown import processors.
                VoltDB.getImportManager().shutdown();
                VoltDB.getTTLManager().shutDown();
                // clear resMonitorWork
                resMonitorWork = null;

                m_periodicWorks.clear();
                m_snapshotCompletionMonitor.shutdown();
                m_periodicWorkThread.shutdown();
                m_periodicWorkThread.awaitTermination(356, TimeUnit.DAYS);
                m_periodicPriorityWorkThread.shutdown();
                m_periodicPriorityWorkThread.awaitTermination(356, TimeUnit.DAYS);

                if (m_elasticService != null) {
                    m_elasticService.shutdown();
                }

                if (m_leaderAppointer != null) {
                    m_leaderAppointer.shutdown();
                }
                m_globalServiceElector.shutdown();

                if (m_hasStartedSampler.get()) {
                    m_sampler.setShouldStop();
                    m_sampler.join();
                }

                // shutdown the web monitoring / json
                if (m_adminListener != null) {
                    m_adminListener.stop();
                }

                // send hostDown trap as client interface is
                // no longer available
                m_snmp.hostDown(FaultLevel.INFO, m_messenger.getHostId(), "Host is shutting down");

                shutdownInitiators();

                try {
                    LargeBlockManager.shutdown();
                }
                catch (Exception e) {
                    hostLog.warn(e);
                }

                if (m_cartographer != null) {
                    m_cartographer.shutdown();
                }

                if (m_configLogger != null) {
                    m_configLogger.join();
                }

                // shut down Export and its connectors.
                VoltDB.getExportManager().shutdown();

                // After sites are terminated, shutdown the DRProducer.
                // The DRProducer is shared by all sites; don't kill it while any site is active.
                if (m_producerDRGateway != null) {
                    try {
                        m_producerDRGateway.shutdown();
                    } catch (InterruptedException e) {
                        hostLog.warn("Interrupted shutting down invocation buffer server", e);
                    }
                    finally {
                        m_producerDRGateway = null;
                    }
                }

                shutdownReplicationConsumerRole();

                // shut down the client interface
                if (m_clientInterface != null) {
                    m_clientInterface.shutdown();
                    m_clientInterface = null;
                }

                if (m_snapshotIOAgent != null) {
                    m_snapshotIOAgent.shutdown();
                }

                // shut down the network/messaging stuff
                // Close the host messenger first, which should close down all of
                // the ForeignHost sockets cleanly
                if (m_messenger != null)
                {
                    m_messenger.shutdown();
                }
                m_messenger = null;

                // shutdown the cipher service
                CipherExecutor.SERVER.shutdown();
                CipherExecutor.CLIENT.shutdown();

                //Also for test code that expects a fresh stats agent
                if (m_opsRegistrar != null) {
                    try {
                        m_opsRegistrar.shutdown();
                    }
                    finally {
                        m_opsRegistrar = null;
                    }
                }

                m_computationService.shutdown();
                m_computationService.awaitTermination(1, TimeUnit.DAYS);
                m_computationService = null;
                m_catalogContext = null;
                m_initiatorStats = null;
                m_latencyStats = null;
                m_latencyCompressedStats = null;
                m_latencyHistogramStats = null;

                AdHocCompilerCache.clearHashCache();
                org.voltdb.iv2.InitiatorMailbox.m_allInitiatorMailboxes.clear();

                PartitionDRGateway.m_partitionDRGateways = ImmutableMap.of();

                // We left the status API up as long as possible...
                m_operatorSupport.stopStatusListener();

                DBBPool.cleanup();
                m_isRunning = false;
            }
            return did_it;
        }
    }

    @Override
    synchronized public void logUpdate(String xmlConfig, long currentTxnId, File voltroot)
    {
        // another site already did this work.
        if (currentTxnId == m_lastLogUpdateTxnId) {
            return;
        }
        else if (currentTxnId < m_lastLogUpdateTxnId) {
            throw new RuntimeException(
                    "Trying to update logging config at transaction " + m_lastLogUpdateTxnId
                    + " with an older transaction: " + currentTxnId);
        }
        hostLog.info("Updating RealVoltDB logging config from txnid: " +
                m_lastLogUpdateTxnId + " to " + currentTxnId);
        m_lastLogUpdateTxnId = currentTxnId;
        VoltLogger.configure(xmlConfig, voltroot);
    }

    /*
     * Write the catalog jar to a temporary jar file, this function
     * is supposed to be called in an NT proc
     */
    @Override
    public void writeCatalogJar(byte[] catalogBytes) throws IOException
    {
        File configInfoDir = getConfigDirectory();
        configInfoDir.mkdirs();

        InMemoryJarfile.writeToFile(catalogBytes,
                                    new File(configInfoDir.getPath(),
                                                 InMemoryJarfile.TMP_CATALOG_JAR_FILENAME));
    }

    // Verify the integrity of the newly updated catalog stored on the ZooKeeper
    @Override
    public String verifyJarAndPrepareProcRunners(byte[] catalogBytes, String diffCommands,
            byte[] catalogBytesHash, byte[] deploymentBytes) {
        ImmutableMap.Builder<String, Class<?>> classesMap = ImmutableMap.<String, Class<?>>builder();
        InMemoryJarfile newCatalogJar;
        JarLoader jarLoader;
        String errorMsg;
        try {
            newCatalogJar = new InMemoryJarfile(catalogBytes);
            jarLoader = newCatalogJar.getLoader();
            for (String classname : jarLoader.getClassNames()) {
                try {
                    Class<?> procCls = CatalogContext.classForProcedureOrUDF(classname, jarLoader);
                    classesMap.put(classname, procCls);
                }
                // LinkageError catches most of the various class loading errors we'd
                // care about here.
                catch (UnsupportedClassVersionError e) {
                    errorMsg = "Cannot load classes compiled with a higher version of Java than currently" +
                                 " in use. Class " + classname + " was compiled with ";

                    Integer major = 0;
                    // update the matcher pattern for various jdk
                    Pattern pattern = Pattern.compile("version\\s(\\d+).(\\d+)");
                    Matcher matcher = pattern.matcher(e.getMessage());
                    if (matcher.find()) {
                        major = Integer.parseInt(matcher.group(1));
                    } else {
                        hostLog.info("Unable to parse compile version number from UnsupportedClassVersionError.");
                    }

                    if (VerifyCatalogAndWriteJar.SupportedJavaVersionMap.containsKey(major)) {
                        errorMsg = errorMsg.concat(VerifyCatalogAndWriteJar.SupportedJavaVersionMap.get(major) + ", current runtime version is " +
                                         System.getProperty("java.version") + ".");
                    } else {
                        errorMsg = errorMsg.concat("an incompatible Java version.");
                    }
                    hostLog.info(errorMsg);
                    return errorMsg;
                }
                catch (LinkageError | ClassNotFoundException e) {
                    String cause = e.getMessage();
                    if (cause == null && e.getCause() != null) {
                        cause = e.getCause().getMessage();
                    }
                    errorMsg = "Error loading class \'" + classname + "\': " +
                        e.getClass().getCanonicalName() + " for " + cause;
                    hostLog.info(errorMsg);
                    return errorMsg;
                }
            }
        } catch (Exception e) {
            // catch all exceptions, anything may fail now can be safely rolled back
            return e.getMessage();
        }

        CatalogContext ctx = VoltDB.instance().getCatalogContext();
        Catalog newCatalog = ctx.getNewCatalog(diffCommands);

        Database db = newCatalog.getClusters().get("cluster").getDatabases().get("database");
        CatalogMap<Procedure> catalogProcedures = db.getProcedures();

        int siteCount = m_nodeSettings.getLocalSitesCount() + 1; // + MPI site

        ctx.m_preparedCatalogInfo = new CatalogContext.CatalogInfo(catalogBytes, catalogBytesHash, deploymentBytes);
        ctx.m_preparedCatalogInfo.m_catalog = newCatalog;
        ctx.m_preparedCatalogInfo.m_preparedProcRunners = new ConcurrentLinkedQueue<>();

        for (long i = 0; i < siteCount; i++) {
            try {
                ImmutableMap<String, ProcedureRunner> userProcRunner =
                    LoadedProcedureSet.loadUserProcedureRunners(catalogProcedures, null,
                                                                classesMap.build(), null);

                ctx.m_preparedCatalogInfo.m_preparedProcRunners.offer(userProcRunner);
            } catch (Exception e) {
                String msg = "error setting up user procedure runners using NT-procedure pattern: "
                            + e.getMessage();
                hostLog.info(msg);
                return msg;
            }
        }

        return null;
    }

    // Clean up the temporary jar file
    @Override
    public void cleanUpTempCatalogJar() {
        File configInfoDir = getConfigDirectory();
        if (!configInfoDir.exists()) {
            return;
        }

        File tempJar = new File(configInfoDir.getPath(),
                                    InMemoryJarfile.TMP_CATALOG_JAR_FILENAME);
        if(tempJar.exists()) {
            tempJar.delete();
        }
    }

    @Override
    public void buildCatalogValidators(boolean isPro) {
        List <String> implementations = CatalogValidator.getImplementations(isPro);
        if (!implementations.isEmpty() && !m_catalogValidators.isEmpty()) {
            VoltDB.crashLocalVoltDB("Catalog validators already initialized");
        }

        Set<String> created = new HashSet<>();
        try {
            for (String implementation : implementations) {
                if (!created.add(implementation)) {
                    throw new RuntimeException("Catalog validator " + implementation + " defined multiple times");
                }
                Class<?> validatorClass = Class.forName(implementation);
                CatalogValidator validator = (CatalogValidator) validatorClass.newInstance();
                m_catalogValidators.add(validator);
            }
        }
        catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to create catalog validators", true, e);
        }
    }

    @Override
    public boolean validateDeployment(Catalog catalog, DeploymentType newDep, DeploymentType curDep, CatalogChangeResult ccr) {
        for (CatalogValidator validator : m_catalogValidators) {
            if (!validator.validateDeployment(catalog, newDep, curDep, ccr)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Create a procedure mapper from a catalog database
     *
     * @param database a {@link Database} from a {@link CatalogContext}
     * @return a {@link Function} returning {@link Procedure} from procedure name or {@code null}
     */
    public static Function<String, Procedure> createProcedureMapper(Database database) {
        if (database == null) {
            throw new IllegalArgumentException("Missing Catalog Database");
        }
        DefaultProcedureManager defaultProcedureManager = new DefaultProcedureManager(database);
        CatalogMap<Procedure> procedures = database.getProcedures();
        return p -> InvocationDispatcher.getProcedureFromName(p, procedures, defaultProcedureManager);
    }

    @Override
    public boolean validateConfiguration(Catalog catalog, DeploymentType deployment,
            InMemoryJarfile catalogJar, Catalog curCatalog, CatalogChangeResult ccr) {
        Function<String, Procedure> procedureMapper = createProcedureMapper(CatalogUtil.getDatabase(catalog));
        for (CatalogValidator validator : m_catalogValidators) {
            if (!validator.validateConfiguration(catalog, procedureMapper, deployment, catalogJar, curCatalog, ccr)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public CatalogContext catalogUpdate(
            String diffCommands,
            int expectedCatalogVersion,
            int nextCatalogVersion,
            long genId,
            boolean isForReplay,
            boolean requireCatalogDiffCmdsApplyToEE,
            boolean hasSchemaChange,
            boolean requiresNewExportGeneration,
            boolean hasSecurityUserChange,
            Consumer<Map<Byte, String[]>> replicableTablesConsumer)
    {

        try {
            /*
             * Synchronize updates of catalog contexts across the multiple sites on this host. Ensure that catalogUpdate() is
             * only performed after all sites reach catalogUpdate(). Once all sites have reached this point the first site to
             * execute will perform the actual update while the others wait.
             */
            final UpdatableSiteCoordinationBarrier sysProcBarrier = VoltDB.getSiteCountBarrier();
            sysProcBarrier.await();
            synchronized (sysProcBarrier) {

                if (m_catalogContext.catalogVersion != expectedCatalogVersion) {
                    if (m_catalogContext.catalogVersion < expectedCatalogVersion) {
                        throw new RuntimeException("Trying to update main catalog context with diff " +
                                "commands generated for an out-of date catalog. Expected catalog version: " +
                                expectedCatalogVersion + " does not match actual version: " + m_catalogContext.catalogVersion);
                    }
                    assert(m_catalogContext.catalogVersion == nextCatalogVersion);
                }

                else {
                    final NodeState prevNodeState = m_statusTracker.set(NodeState.UPDATING);
                    try {
                        doCatalogUpdate(diffCommands, nextCatalogVersion, genId, isForReplay,
                                        requireCatalogDiffCmdsApplyToEE, hasSchemaChange,
                                        requiresNewExportGeneration, hasSecurityUserChange,
                                        replicableTablesConsumer);
                    }
                    finally {
                        m_statusTracker.set(prevNodeState);
                    }
                }
            }
        } catch (InterruptedException | BrokenBarrierException e) {
            VoltDB.crashLocalVoltDB("Error waiting for barrier", true, e);
        }

        return m_catalogContext;
    }

    private void doCatalogUpdate(
            String diffCommands,
            int nextCatalogVersion,
            long genId,
            boolean isForReplay,
            boolean requireCatalogDiffCmdsApplyToEE,
            boolean hasSchemaChange,
            boolean requiresNewExportGeneration,
            boolean hasSecurityUserChange,
            Consumer<Map<Byte, String[]>> replicableTablesConsumer) {

        final ReplicationRole oldRole = getReplicationRole();

        //Security credentials may be part of the new catalog update.
        //Notify HTTPClientInterface not to store AuthenticationResult in sessions
        //before CatalogContext swap.
        if (m_adminListener != null && hasSecurityUserChange) {
            m_adminListener.dontStoreAuthenticationResultInHttpSession();
        }

        CatalogInfo catalogInfo = null;
        Catalog newCatalog = null;
        CatalogContext ctx = VoltDB.instance().getCatalogContext();
        if (isForReplay) {
            try {
                CatalogAndDeployment catalogStuff =
                    CatalogUtil.getCatalogFromZK(VoltDB.instance().getHostMessenger().getZK());
                byte[] depbytes = catalogStuff.deploymentBytes;
                if (depbytes == null) {
                    depbytes = ctx.m_catalogInfo.m_deploymentBytes;
                }
                catalogInfo = new CatalogInfo(catalogStuff.catalogBytes, catalogStuff.catalogHash, depbytes);
                newCatalog = ctx.getNewCatalog(diffCommands);
            } catch (Exception e) {
                // impossible to hit, log for debug purpose
                hostLog.error("Error reading catalog from zookeeper for node: " + VoltZK.catalogbytes + ":" + e);
                throw new RuntimeException("Error reading catalog from zookeeper");
            }
        } else {
            if (ctx.m_preparedCatalogInfo == null) {
                // impossible to hit, log for debug purpose
                throw new RuntimeException("Unexpected: @UpdateCore's prepared catalog is null during non-replay case.");
            }
            // using the prepared catalog information if prepared
            catalogInfo = ctx.m_preparedCatalogInfo;
            newCatalog = catalogInfo.m_catalog;
        }

        byte[] oldDeployHash = m_catalogContext.getDeploymentHash();
        final String oldDRConnectionSource = m_catalogContext.cluster.getDrmasterhost();

        // 0. A new catalog! Update the global context and the context tracker
        m_catalogContext = m_catalogContext.update(isForReplay,
                                                   newCatalog,
                                                   nextCatalogVersion,
                                                   genId,
                                                   catalogInfo,
                                                   m_messenger,
                                                   hasSchemaChange);

        // 1. update the export manager.
        VoltDB.getExportManager().updateCatalog(m_catalogContext, requireCatalogDiffCmdsApplyToEE,
                                                requiresNewExportGeneration, getPartitionToSiteMap());

        // 1.1 Update the elastic service throughput settings
        if (m_elasticService != null) {
            m_elasticService.updateConfig(m_catalogContext);
        }

        // 1.5 update the dead host timeout
        if (m_catalogContext.cluster.getHeartbeattimeout() * 1000 != m_config.m_deadHostTimeoutMS) {
            m_config.m_deadHostTimeoutMS = m_catalogContext.cluster.getHeartbeattimeout() * 1000;
            m_messenger.setDeadHostTimeout(m_config.m_deadHostTimeoutMS);
        }

        // 2. update client interface (asynchronously)
        //    CI in turn updates the planner thread.
        if (m_clientInterface != null) {
            m_clientInterface.notifyOfCatalogUpdate();
        }

        // 3. update HTTPClientInterface (asynchronously)
        // This purges cached connection state so that access with
        // stale auth info is prevented.
        if (m_adminListener != null && hasSecurityUserChange) {
            m_adminListener.notifyOfCatalogUpdate();
        }

        m_clientInterface.getDispatcher().notifyNTProcedureServiceOfPreCatalogUpdate();

        // 4. Flush StatisticsAgent old user PROCEDURE statistics.
        // The stats agent will hold all other stats in memory.
        getStatsAgent().notifyOfCatalogUpdate();

        // 4.5. (added)
        // Update the NT procedure service AFTER stats are cleared in the previous step
        m_clientInterface.getDispatcher().notifyNTProcedureServiceOfCatalogUpdate();

        // 5. MPIs don't run fragments. Update them here. Do
        // this after flushing the stats -- this will re-register
        // the MPI statistics.
        if (m_MPI != null) {
            m_MPI.updateCatalog(diffCommands, m_catalogContext, isForReplay,
                                requireCatalogDiffCmdsApplyToEE, requiresNewExportGeneration);
        }

        // Update catalog for import processor this should be just/stop start and update partitions.
        VoltDB.getImportManager().updateCatalog(m_catalogContext, m_messenger);

        // 6. Perform updates required by the DR subsystem

        // 6.1. Perform any actions that would have been taken during the ordinary initialization path
        if (m_consumerDRGateway != null) {
            // 6.2. If we are a DR replica and the consumer was created
            // before the catalog update, we may care about a deployment
            // update. If it was created above, no need to notify
            // because the consumer already has the latest catalog.
            final String newDRConnectionSource = m_catalogContext.cluster.getDrmasterhost();
            m_consumerDRGateway.updateCatalog(m_catalogContext,
                                              (newDRConnectionSource != null && !newDRConnectionSource.equals(oldDRConnectionSource)
                                               ? newDRConnectionSource
                                               : null),
                                              (byte) m_catalogContext.cluster.getPreferredsource());
        }

        /*
         * Calculate the set of replicable tables. Technically this could be skipped if no dr tables were modified. This
         * is performed here and not in the NT procedure because between the execution of the NT procedure and now a
         * remote catalog update could have been applied so this must be done as part of a transaction
         */
        replicableTablesConsumer.accept(m_drCatalogCommands.calculateReplicableTables(m_catalogContext.catalog));

        // Check if this is promotion
        if (oldRole == ReplicationRole.REPLICA &&
            m_catalogContext.cluster.getDrrole().equals("master")) {
            // Promote replica to master
            promoteToMaster();
        }

        // 6.3. If we are a DR master, update the DR table signature hash
        if (m_producerDRGateway != null) {
            m_producerDRGateway.updateCatalog(m_catalogContext,
                                              VoltDB.getReplicationPort(m_catalogContext.cluster.getDrproducerport()));
        }

        // 7 Update tasks (asynchronously) after replica change if it occurred
        m_taskManager.processUpdate(m_catalogContext, !hasSchemaChange);
        m_avroSerde.updateConfig(m_catalogContext);

        new ConfigLogging().logCatalogAndDeployment(CatalogJarWriteMode.CATALOG_UPDATE);

        // log system setting information if the deployment config has changed
        if (!Arrays.equals(oldDeployHash, m_catalogContext.getDeploymentHash())) {
            DailyLogging.logSystemSettingFromCatalogContext();
        }
        //Before starting resource monitor update any Snmp configuration changes.
        if (m_snmp != null) {
            m_snmp.notifyOfCatalogUpdate(m_catalogContext.getDeployment().getSnmp());
        }

        //TTL control works on the host with MPI
        if (m_myHostId == CoreUtils.getHostIdFromHSId(m_cartographer.getHSIdForMultiPartitionInitiator())) {
            VoltDB.getTTLManager().scheduleTTLTasks();
        }
        // restart resource usage monitoring task
        startHealthMonitor();

        checkHeapSanity(m_catalogContext.tables.size(),
                        (getLocalPartitionCount()), m_configuredReplicationFactor);

        checkThreadsSanity();

        registerClockSkewTask();
    }

    Map<Integer, Integer> getPartitionToSiteMap() {
        Map<Integer, Integer> partitions = new HashMap<>();
        for (Initiator initiator : m_iv2Initiators.values()) {
            int partition = initiator.getPartitionId();
            if (partition != MpInitiator.MP_INIT_PID) {
                partitions.put(partition, CoreUtils.getSiteIdFromHSId(initiator.getInitiatorHSId()));
            }
        }
        return partitions;
    }

    @Override
    public CatalogContext settingsUpdate(
            ClusterSettings settings, final int expectedVersionId)
    {
        synchronized (m_startAndStopLock) {
            int stamp [] = new int[]{0};
            ClusterSettings expect = m_clusterSettings.get(stamp);
            if (   stamp[0] == expectedVersionId
                && m_clusterSettings.compareAndSet(expect, settings, stamp[0], expectedVersionId+1)
            ) {
                try {
                    settings.store();
                } catch (SettingsException e) {
                    hostLog.error(e);
                    throw e;
                }
            } else if (stamp[0] != expectedVersionId+1) {
                String msg = "Failed to update cluster setting to version " + (expectedVersionId + 1)
                        + ", from current version " + stamp[0] + ". Reloading from Zookeeper";
                hostLog.warn(msg);
                m_clusterSettings.load(m_messenger.getZK());
            }
            if (m_MPI != null) {
                m_MPI.updateSettings(m_catalogContext);
            }
            // good place to set deadhost timeout once we make it a config
        }
        return m_catalogContext;
    }

    @Override
    public VoltDB.Configuration getConfig() {
        return m_config;
    }

    @Override
    public String getBuildString() {
        return m_buildString == null ? "VoltDB" : m_buildString;
    }

    @Override
    public String getVersionString() {
        return m_versionString;
    }

    public final VersionChecker m_versionChecker = new VersionChecker() {
        @Override
        public boolean isCompatibleVersionString(String other) {
            return RealVoltDB.this.isCompatibleVersionString(other);
        }

        @Override
        public String getVersionString() {
            return RealVoltDB.this.getVersionString();
        }

        @Override
        public String getBuildString() {
            return RealVoltDB.this.getBuildString();
        }
    };

    /**
     * Used for testing when you don't have an instance. Should do roughly what
     * {@link #isCompatibleVersionString(String)} does.
     */
    public static boolean staticIsCompatibleVersionString(String versionString) {
        return versionString.matches(m_defaultHotfixableRegexPattern);
    }

    @Override
    public boolean isCompatibleVersionString(String versionString) {
        return versionString.matches(m_hotfixableRegexPattern);
    }

    @Override
    public String getEELibraryVersionString() {
        return m_defaultVersionString;
    }

    @Override
    public HostMessenger getHostMessenger() {
        return m_messenger;
    }

    @Override
    public ClientInterface getClientInterface() {
        return m_clientInterface;
    }

    @Override
    public OpsAgent getOpsAgent(OpsSelector selector) {
        return m_opsRegistrar.getAgent(selector);
    }

    @Override
    public StatsAgent getStatsAgent() {
        OpsAgent statsAgent = m_opsRegistrar.getAgent(OpsSelector.STATISTICS);
        assert(statsAgent instanceof StatsAgent);
        return (StatsAgent)statsAgent;
    }

    @Override
    public MemoryStats getMemoryStatsSource() {
        return m_memoryStats;
    }

    @Override
    public CatalogContext getCatalogContext() {
        return m_catalogContext;
    }

    /**
     * Tells if the VoltDB is running. m_isRunning needs to be set to true
     * when the run() method is called, and set to false when shutting down.
     *
     * @return true if the VoltDB is running.
     */
    @Override
    public boolean isRunning() {
        return m_isRunning;
    }

    @Override
    public void halt() {
        SnmpTrapSender snmp = getSnmpTrapSender();
        if (snmp != null) {
            snmp.hostDown(FaultLevel.INFO, m_messenger.getHostId(), "Host is shutting down because of @StopNode");
            snmp.shutdown();
        }

        Thread shutdownThread = new Thread() {
            @Override
            public void run() {
                notifyOfShutdown();
                hostLog.warn("VoltDB node shutting down as requested by @StopNode command.");
                shutdownInitiators();
                m_isRunning = false;
                m_statusTracker.set(NodeState.STOPPED); // not that anyone is going to see this.
                hostLog.warn("VoltDB node has been shutdown By @StopNode");
                m_operatorSupport.stopStatusListener();
                System.exit(0);
            }
        };

        //if the resources can not be released in 5 seconds, shutdown the node
        Thread watchThread = new Thread() {
            @Override
            public void run() {
                final long now = System.nanoTime();
                while (m_isRunning) {
                    final long delta = System.nanoTime() - now;
                    if (delta > TimeUnit.SECONDS.toNanos(5)) {
                        hostLog.warn("VoltDB node has been shutdown.");
                        System.exit(0);
                    }
                    try {
                        Thread.sleep(5); // 5 mSec
                    } catch (Exception e) {}
                }
            }
        };
        shutdownThread.start();
        watchThread.start();
    }

    // tell the iv2 sites to stop their runloop
    // The reason to halt MP sites first is that it may wait for some fragment dependencies
    // to be done on SP sites, kill SP sites first may risk MP site to wait forever.
    private void shutdownInitiators() {
        if (m_iv2Initiators == null) {
            return;
        }
        m_iv2Initiators.descendingMap().values().stream().forEach(p->p.shutdown());
    }

    /**
     * Debugging function - creates a record of the current state of the system.
     * @param out PrintStream to write report to.
     */
    public void createRuntimeReport(PrintStream out) {
        // This function may be running in its own thread.

        out.print("MIME-Version: 1.0\n");
        out.print("Content-type: multipart/mixed; boundary=\"reportsection\"");

        out.print("\n\n--reportsection\nContent-Type: text/plain\n\nClientInterface Report\n");
        if (m_clientInterface != null) {
            out.print(m_clientInterface.toString() + "\n");
        }
    }

    @Override
    public BackendTarget getBackendTargetType() {
        return m_config.m_backend;
    }

    @Override
    public synchronized void onExecutionSiteRejoinCompletion(long transferred) {
        m_executionSiteRecoveryFinish = System.currentTimeMillis();
        m_executionSiteRecoveryTransferred = transferred;
        onRejoinCompletion();
    }

    private void onRejoinCompletion() {
        // null out the rejoin coordinator
        if (m_joinCoordinator != null) {
            m_joinCoordinator.close();
        }
        m_joinCoordinator = null;
        // Mark the data transfer as done so CL can make the right decision when a truncation snapshot completes
        m_rejoinDataPending = false;

        try {
            m_testBlockRecoveryCompletion.acquire();
        } catch (InterruptedException e) {}
        final long delta = ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000);
        final long megabytes = m_executionSiteRecoveryTransferred / (1024 * 1024);
        final double megabytesPerSecond = megabytes / ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000.0);

        deleteStagedCatalogIfNeeded();

        if (m_clientInterface != null) {
            m_clientInterface.mayActivateSnapshotDaemon();
            try {
                m_clientInterface.startAcceptingConnections();
            } catch (IOException e) {
                hostLog.fatal("There was an error trying to start a ClientInterface connecting acceptions", e);
                VoltDB.crashLocalVoltDB("Error starting client interface.", true, e);
            }
            // send hostUp trap
            m_snmp.hostUp("Host is now a cluster member");

            if (m_producerDRGateway != null && !m_producerDRGateway.isStarted()) {
                // Initialize DR producer and consumer start listening on the DR ports
                initializeDRProducer();
                createDRConsumerIfNeeded();
                prepareReplication();
            }
        }
        startHealthMonitor();

        try {
            if (m_adminListener != null) {
                m_adminListener.start();
            }
        } catch (Exception e) {
            hostLog.fatal("There was an error trying to start a HTTP interface", e);
            VoltDB.crashLocalVoltDB("HTTP service unable to bind to port.", true, e);
        }
        // Allow export datasources to start consuming their binary deques safely
        // as at this juncture the initial truncation snapshot is already complete
        VoltDB.getExportManager().startPolling(m_catalogContext);

        //Tell import processors that they can start ingesting data.
        VoltDB.getImportManager().readyForData();

        if (m_config.m_startAction == StartAction.REJOIN) {
            consoleLog.info(
                    "Node data recovery completed after " + delta + " seconds with " + megabytes +
                    " megabytes transferred at a rate of " +
                    megabytesPerSecond + " megabytes/sec");
        }

        boolean allDone = false;
        try {
            final ZooKeeper zk = m_messenger.getZK();
            boolean logRecoveryCompleted = false;
            if (getCommandLog().getClass().getName().equals("org.voltdb.CommandLogImpl")) {
                String requestNode = zk.create(VoltZK.request_truncation_snapshot_node, null,
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                if (m_rejoinTruncationReqId == null) {
                    m_rejoinTruncationReqId = requestNode;
                }
            } else {
                logRecoveryCompleted = true;
            }

            // Join creates a truncation snapshot as part of the join process,
            // so there is no need to wait for the truncation snapshot requested
            // above to finish.
            if (logRecoveryCompleted || m_joining) {
                if (m_rejoining) {
                    CoreZK.removeRejoinNodeIndicatorForHost(m_messenger.getZK(), m_myHostId);
                    m_rejoining = false;
                }

                if (m_joining) {
                    CoreZK.removeJoinNodeIndicatorForHost(m_messenger.getZK(), m_myHostId);
                }

                String actionName = m_joining ? "join" : "rejoin";
                m_joining = false;
                consoleLog.infoFmt("Node %s completed", actionName); // onRejoinCompletion
                allDone = true;
            }

            //start MigratePartitionLeader task
            startMigratePartitionLeaderTask();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to log host rejoin completion to ZK", true, e);
        }

        hostLog.info("Logging host rejoin completion to ZK");
        initializationIsComplete(allDone); // onRejoinCompletion
    }

    @Override
    public CommandLog getCommandLog() {
        return m_commandLog;
    }

    @Override
    public OperationMode getMode()
    {
        return m_mode;
    }

    @Override
    public void setMode(OperationMode mode)
    {
        if (m_mode != mode)
        {
            if (mode == OperationMode.PAUSED)
            {
                m_config.m_isPaused = true;
                m_statusTracker.set(NodeState.PAUSED);
                hostLog.info("Server is entering admin mode and pausing.");
            }
            else if (m_mode == OperationMode.PAUSED)
            {
                m_config.m_isPaused = false;
                m_statusTracker.set(NodeState.UP);
                hostLog.info("Server is exiting admin mode and resuming operation.");
            }
            m_taskManager.evaluateReadOnlyMode();
        }
        m_mode = mode;
    }

    @Override
    public void setStartMode(OperationMode mode) {
        m_startMode = mode;
    }

    @Override
    public OperationMode getStartMode()
    {
        return m_startMode;
    }

    @Override
    public NodeState getNodeState()
    {
        return m_statusTracker.get();
    }

    @Override
    public boolean getNodeStartupComplete()
    {
        return m_statusTracker.getStartupComplete();
    }

    @Override
    public int[] getNodeStartupProgress()
    {
        return m_statusTracker.getProgress();
    }

    @Override
    public void reportNodeStartupProgress(int completed, int total)
    {
        if (m_statusTracker != null) {
            m_statusTracker.reportProgress(completed, total);
        }
    }

    @Override
    public int getMyHostId()
    {
        return m_myHostId;
    }

    @Override
    public int getVoltPid()
    {
        return m_voltPid;
    }

    @Override
    public void promoteToMaster()
    {
        consoleLog.info("Promoting replication role from replica to master.");
        hostLog.info("Promoting replication role from replica to master.");
        shutdownReplicationConsumerRole();
        if (m_clientInterface != null) {
            m_clientInterface.setReplicationRole(getReplicationRole());
        }
    }

    private void replaceDRConsumerStatsWithDummy()
    {
        StatsAgent statsAgent = getStatsAgent();
        statsAgent.deregisterStatsSourcesFor(StatsSelector.DRCONSUMERCLUSTER, 0);
        statsAgent.deregisterStatsSourcesFor(StatsSelector.DRCONSUMERNODE, 0);
        statsAgent.deregisterStatsSourcesFor(StatsSelector.DRCONSUMERPARTITION, 0);
        statsAgent.registerStatsSource(StatsSelector.DRCONSUMERCLUSTER, 0,
                new DRConsumerStatsBase.DRConsumerClusterStatsBase());
        statsAgent.registerStatsSource(StatsSelector.DRCONSUMERNODE, 0,
                new DRConsumerStatsBase.DRConsumerNodeStatsBase());
        statsAgent.registerStatsSource(StatsSelector.DRCONSUMERPARTITION, 0,
                new DRConsumerStatsBase.DRConsumerPartitionStatsBase());
    }

    private void shutdownReplicationConsumerRole() {
        if (m_consumerDRGateway != null) {
            try {
                m_consumerDRGateway.shutdown(false, true);
            } catch (InterruptedException|ExecutionException e) {
                hostLog.warn("Interrupted shutting down dr replication", e);
            }
            finally {
                m_globalServiceElector.unregisterService(m_consumerDRGateway);
                m_consumerDRGateway = null;
            }
        }
    }

    @Override
    public ReplicationRole getReplicationRole()
    {
        final String role = m_catalogContext.cluster.getDrrole();
        if (role.equals(DrRoleType.REPLICA.value())) {
            return ReplicationRole.REPLICA;
        } else {
            return ReplicationRole.NONE;
        }
    }

    /**
     * Metadata is a JSON object
     */
    @Override
    public String getLocalMetadata() {
        return m_localMetadata;
    }

    @Override
    public void onSnapshotRestoreCompletion() {
        if (!m_rejoining && !m_joining) {
            initializeDRProducer();
        }
    }

    @Override
    public void onReplayCompletion(long txnId, Map<Integer, Long> perPartitionTxnIds) {
        /*
         * Remove the terminus file if it is there, which is written on shutdown --save
         */
        new File(m_nodeSettings.getVoltDBRoot(), VoltDB.TERMINUS_MARKER).delete();

        /*
         * Command log is already initialized if this is a rejoin or a join
         */
        if ((m_commandLog != null) && (m_commandLog.needsInitialization())) {
            // Initialize command logger
            m_commandLog.init(m_catalogContext.cluster.getLogconfig().get("log").getLogsize(),
                              txnId,
                              m_config.m_commandLogBinding,
                              perPartitionTxnIds);
            try {
                ZKCountdownLatch latch =
                        new ZKCountdownLatch(m_messenger.getZK(),
                                VoltZK.commandlog_init_barrier, m_messenger.getLiveHostIds().size());
                latch.countDown(true);
                latch.await();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to init and wait on command log init barrier", true, e);
            }
        }

        /*
         * IV2: After the command log is initialized, force the writing of the initial
         * viable replay set.  Turns into a no-op with no command log, on the non-leader sites, and on the MPI.
         */
        for (Initiator initiator : m_iv2Initiators.values()) {
            initiator.enableWritingIv2FaultLog();
        }

        /*
         * IV2: From this point on, not all node failures should crash global VoltDB.
         */
        if (m_leaderAppointer != null) {
            m_leaderAppointer.onReplayCompletion();
        }

        deleteStagedCatalogIfNeeded();

        // start mode can be either PAUSED or RUNNING, if server starts as paused
        // set m_mode before allow transaction to come in. If server starts as normal
        // set m_mode later because many unit tests assume RUNNING mode means they
        // can connect to the server.
        if (m_startMode == OperationMode.PAUSED) {
            m_mode = m_startMode;
        }

        if (!m_rejoining && !m_joining) {
            if (m_clientInterface != null) {
                try {
                    m_clientInterface.startAcceptingConnections();
                } catch (IOException e) {
                    hostLog.fatal("There was an error trying to start a ClientInterface connecting acceptions", e);
                    VoltDB.crashLocalVoltDB("Error starting client interface.", true, e);
                }
                // send hostUp trap
                m_snmp.hostUp("host is now a cluster member");
            }

            // Start listening on the DR ports
            createDRConsumerIfNeeded();
            prepareReplication();
            startHealthMonitor();

            // Allow export datasources to start consuming their binary deques safely
            // as at this juncture the initial truncation snapshot is already complete
            VoltDB.getExportManager().startPolling(m_catalogContext);

            //Tell import processors that they can start ingesting data.
            VoltDB.getImportManager().readyForData();

            try {
                if (m_adminListener != null) {
                    m_adminListener.start();
                }
            } catch (Exception e) {
                hostLog.fatal("There was an error trying to start a HTTP interface", e);
                VoltDB.crashLocalVoltDB("HTTP service unable to bind to port.", true, e);
            }

            // Set m_mode to RUNNING, and initialization complete
            databaseIsRunning();
            initializationIsComplete(true); // onReplayCompletion

        } else {
            // Set m_mode to RUNNING
            databaseIsRunning();
        }

        // Create a zk node to indicate initialization is completed
        m_messenger.getZK().create(VoltZK.init_completed, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new ZKUtil.StringCallback(), null);

        m_taskManager.start(m_catalogContext);

        if (m_elasticService != null) {
            try {
                m_elasticService.start();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to start elastic services", false, e);
            }
        }
    }

    /*
     * Sets various indicators to show that initialization is complete,
     * including logging that event to the console.
     */
    private void initializationIsComplete(boolean allDone) {
        consoleLog.infoFmt("Server Operational State is: %s", m_mode == OperationMode.PAUSED ? "PAUSED" : "NORMAL");
        consoleLog.info("Server completed initialization.");
        m_statusTracker.set(m_mode == OperationMode.PAUSED ? NodeState.PAUSED : NodeState.UP);
        if (allDone) {
            m_statusTracker.setStartupComplete();
        }
        registerClockSkewTask();
    }

    private void registerClockSkewTask() {
        int minutes = m_catalogContext.getDeployment().getSystemsettings()
                               .getClockskew()
                               .getInterval();
        Duration interval = Duration.ofMinutes(minutes);
        String taskName = "ClockSkewCollector";
        TaskManager taskManager = requireNonNull(getTaskManager(), "task manager is null");
        taskManager.removeSystemTask(taskName);
        taskManager.addSystemTask(taskName, TaskScope.HOSTS,
                        helper -> new ClockSkewCollectorScheduler(this, helper, skewStats, interval));
    }

    private void databaseIsRunning() {
        if (m_startMode != OperationMode.PAUSED) {
            assert(m_startMode == OperationMode.RUNNING);
            m_mode = OperationMode.RUNNING;
        }
    }

    private void deleteStagedCatalogIfNeeded() {
        if (((m_commandLog != null) && m_commandLog.isEnabled()) || (m_terminusNonce != null)) {
            File stagedCatalog = new File(RealVoltDB.getStagedCatalogPath(getVoltDBRootPath()));
            if (stagedCatalog.exists()) {
                if (stagedCatalog.delete()) {
                    hostLog.info("Saved copy of the initialized schema deleted because command logs and/or snapshots are in use.");
                } else {
                    hostLog.warn("Failed to delete the saved copy of the initialized schema.");
                }
            }
        }
    }

    @Override
    public SnapshotCompletionMonitor getSnapshotCompletionMonitor() {
        return m_snapshotCompletionMonitor;
    }

    @Override
    public synchronized void recoveryComplete(String requestId) {
        assert(m_rejoinDataPending == false);

        if (m_rejoining) {
            if (m_rejoinTruncationReqId.compareTo(requestId) <= 0) {
                String actionName = m_joining ? "join" : "rejoin";
                // remove the rejoin blocker
                CoreZK.removeRejoinNodeIndicatorForHost(m_messenger.getZK(), m_myHostId);
                consoleLog.infoFmt("Node %s completed", actionName); // onRecoveryComplete
                m_statusTracker.setStartupComplete();
                m_rejoinTruncationReqId = null;
                m_rejoining = false;
            }
            else {
                // If we saw some other truncation request ID, then try the same one again.  As long as we
                // don't flip the m_rejoining state, all truncation snapshot completions will call back to here.
                try {
                    final ZooKeeper zk = m_messenger.getZK();
                    String requestNode = zk.create(VoltZK.request_truncation_snapshot_node, null,
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
                    if (m_rejoinTruncationReqId == null) {
                        m_rejoinTruncationReqId = requestNode;
                    }
                }
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Unable to retry post-rejoin truncation snapshot request.", true, e);
                }
            }
        }
    }

    @Override
    public ScheduledExecutorService getSES(boolean priority) {
        return priority ? m_periodicPriorityWorkThread : m_periodicWorkThread;
    }

    /**
     * See comment on {@link VoltDBInterface#scheduleWork(Runnable, long, long, TimeUnit)} vs
     * {@link VoltDBInterface#schedulePriorityWork(Runnable, long, long, TimeUnit)}
     */
    @Override
    public ScheduledFuture<?> scheduleWork(Runnable work,
            long initialDelay,
            long delay,
            TimeUnit unit) {
        if (delay > 0) {
            return m_periodicWorkThread.scheduleWithFixedDelay(work,
                    initialDelay, delay,
                    unit);
        } else {
            return m_periodicWorkThread.schedule(work, initialDelay, unit);
        }
    }

    @Override
    public ListeningExecutorService getComputationService() {
        return m_computationService;
    }

    /**
     * Initialize the DR producer so that any binary log generated on recover
     * will be queued. This does NOT open the DR port. That will happen after
     * command log replay finishes.
     */
    private void initializeDRProducer() {
        try {
            if (m_producerDRGateway != null) {
                m_producerDRGateway.startAndWaitForGlobalAgreement();

                for (Initiator iv2init : m_iv2Initiators.values()) {
                    iv2init.initDRGateway(m_config.m_startAction,
                                          m_producerDRGateway,
                                          isLowestSiteId(iv2init));
                }

                m_producerDRGateway.completeInitialization();
            }
        } catch (Exception ex) {
            MiscUtils.printPortsInUse(hostLog);
            VoltDB.crashLocalVoltDB("Failed to initialize DR producer", false, ex);
        }
    }

    private void prepareReplication() {
        // Warning: This is called on the site thread if this host is rejoining
        try {
            if (m_consumerDRGateway != null) {
                if (m_config.m_startAction != StartAction.CREATE) {
                    Pair<Byte, List<MeshMemberInfo>> expectedClusterMembers = m_producerDRGateway.getInitialConversations();
                    m_consumerDRGateway.setInitialConversationMembership(expectedClusterMembers.getFirst(),
                            expectedClusterMembers.getSecond());
                }
                m_consumerDRGateway.initialize(m_config.m_startAction, willDoActualRecover());
            }
            if (m_producerDRGateway != null) {
                m_producerDRGateway.startListening(m_catalogContext.cluster.getDrproducerenabled(),
                                                   VoltDB.getReplicationPort(m_catalogContext.cluster.getDrproducerport()),
                                                   VoltDB.getDefaultReplicationInterface());
            }
        } catch (Exception ex) {
            MiscUtils.printPortsInUse(hostLog);
            VoltDB.crashLocalVoltDB("Failed to initialize DR", false, ex);
        }
    }

    private boolean isLowestSiteId(Initiator initiator) {
        // The initiator map is sorted, the initiator that has the lowest local
        // partition ID gets to create the MP DR gateway
        return initiator.getPartitionId() == m_iv2Initiators.firstKey();
    }

    private boolean createDRConsumerIfNeeded() {
        if (!m_config.m_isEnterprise || (m_consumerDRGateway != null)) {
            return false;
        }
        final String drRole = m_catalogContext.getCluster().getDrrole();
        DrType drType = m_catalogContext.getDeployment().getDr();
        if (DrRoleType.REPLICA.value().equals(drRole) || DrRoleType.XDCR.value().equals(drRole)) {
            byte drConsumerClusterId = (byte)m_catalogContext.cluster.getDrclusterid();
            final Pair<String, Integer> drIfAndPort = VoltZK.getDRPublicInterfaceAndPortFromMetadata(m_localMetadata);
            m_consumerDRGateway = ProClass.newInstanceOf("org.voltdb.dr2.ConsumerDRGatewayImpl", "DR Consumer",
                    ProClass.HANDLER_CRASH,
                    m_clientInterface, m_cartographer, m_messenger, drConsumerClusterId,
                    (byte) m_catalogContext.cluster.getPreferredsource(), drIfAndPort.getFirst(),
                    drIfAndPort.getSecond(), drType, m_producerDRGateway);
            m_globalServiceElector.registerService(m_consumerDRGateway);
            return true;
        }
        return false;
    }

    // Thread safe
    @Override
    public void setReplicationActive(boolean active)
    {
        if (m_replicationActive.compareAndSet(!active, active)) {

            try {
                JSONStringer js = new JSONStringer();
                js.object();
                js.keySymbolValuePair("active", m_replicationActive.get());
                js.endObject();

                getHostMessenger().getZK().setData(VoltZK.replicationconfig,
                                                   js.toString().getBytes("UTF-8"),
                                                   -1);
            } catch (Exception e) {
                e.printStackTrace();
                hostLog.error("Failed to write replication active state to ZK: " +
                              e.getMessage());
            }

            if (m_producerDRGateway != null) {
                m_producerDRGateway.setActive(active);
            }
        }
    }

    @Override
    public boolean getReplicationActive()
    {
        return m_replicationActive.get();
    }

    @Override
    public ProducerDRGateway getNodeDRGateway()
    {
        return m_producerDRGateway;
    }

    @Override
    public ConsumerDRGateway getConsumerDRGateway() {
        return m_consumerDRGateway;
    }

    @Override
    public void onSyncSnapshotCompletion() {
        m_leaderAppointer.onSyncSnapshotCompletion();
    }

    @Override
    public void configureDurabilityUniqueIdListener(Integer partition, DurableUniqueIdListener listener, boolean install) {
        if (partition == MpInitiator.MP_INIT_PID) {
            m_iv2Initiators.get(m_iv2Initiators.firstKey()).configureDurableUniqueIdListener(listener, install);
        }
        else {
            Initiator init = m_iv2Initiators.get(partition);
            assert init != null;
            init.configureDurableUniqueIdListener(listener, install);
        }
    }

    public ExecutionEngine debugGetSpiedEE(int partitionId) {
        if (m_config.m_backend == BackendTarget.NATIVE_EE_SPY_JNI) {
            Initiator init = m_iv2Initiators.get(partitionId);
            return ((BaseInitiator<?>)init).debugGetSpiedEE();
        }
        else {
            return null;
        }
    }

    @Override
    public SiteTracker getSiteTrackerForSnapshot()
    {
        return new SiteTracker(m_messenger.getHostId(), m_cartographer.getSiteTrackerMailboxMap(), 0);
    }

    /**
     * Create default deployment.xml file in voltdbroot if the deployment path is null.
     *
     * @return path to default deployment file
     * @throws IOException
     */
    static String setupDefaultDeployment(VoltLogger logger) throws IOException {
        return setupDefaultDeployment(logger, CatalogUtil.getVoltDbRoot(null));
    }

    /**
     * Create default deployment.xml file in voltdbroot if the deployment path is null.
     *
     * @return pathto default deployment file
     * @throws IOException
     */
   static String setupDefaultDeployment(VoltLogger logger, File voltdbroot) throws IOException {
        File configInfoDir = new File(voltdbroot, Constants.CONFIG_DIR);
        configInfoDir.mkdirs();

        File depFH = new File(configInfoDir, "deployment.xml");
        if (!depFH.exists()) {
            logger.info("Generating default deployment file \"" + depFH.getAbsolutePath() + "\"");

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(depFH))) {
                for (String line : defaultDeploymentXML) {
                    bw.write(line);
                    bw.newLine();
                }
            } finally {
            }
        }

        return depFH.getAbsolutePath();
    }

    /*
     * Validate the build string with the rest of the cluster
     * by racing to publish it to ZK and then comparing the one this process
     * has to the one in ZK. They should all match. The method returns a future
     * so that init can continue while the ZK call is pending since it ZK is pretty
     * slow.
     */
    private Future<?> validateBuildString(final String buildString, ZooKeeper zk) {
        final SettableFuture<Object> retval = SettableFuture.create();
        byte buildStringBytes[] = null;
        try {
            buildStringBytes = buildString.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
        final byte buildStringBytesFinal[] = buildStringBytes;

        //Can use a void callback because ZK will execute the create and then the get in order
        //It's a race so it doesn't have to succeed
        zk.create(
                VoltZK.buildstring,
                buildStringBytes,
                Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                new ZKUtil.StringCallback(),
                null);

        zk.getData(VoltZK.buildstring, false, new org.apache.zookeeper_voltpatches.AsyncCallback.DataCallback() {

            @Override
            public void processResult(int rc, String path, Object ctx,
                    byte[] data, Stat stat) {
                KeeperException.Code code = KeeperException.Code.get(rc);
                if (code == KeeperException.Code.OK) {
                    if (Arrays.equals(buildStringBytesFinal, data)) {
                        retval.set(null);
                    } else {
                        try {
                            hostLog.info("Different but compatible software versions on the cluster " +
                                         "and the rejoining node. Cluster version is {" + (new String(data, "UTF-8")).split("_")[0] +
                                         "}. Rejoining node version is {" + m_defaultVersionString + "}.");
                            retval.set(null);
                        } catch (UnsupportedEncodingException e) {
                            retval.setException(new AssertionError(e));
                        }
                    }
                } else {
                    retval.setException(KeeperException.create(code));
                }
            }

        }, null);

        return retval;
    }

    /**
     * See comment on {@link VoltDBInterface#schedulePriorityWork(Runnable, long, long, TimeUnit)} vs
     * {@link VoltDBInterface#scheduleWork(Runnable, long, long, TimeUnit)}
     */
    @Override
    public ScheduledFuture<?> schedulePriorityWork(Runnable work,
            long initialDelay,
            long delay,
            TimeUnit unit) {
        if (delay > 0) {
            return m_periodicPriorityWorkThread.scheduleWithFixedDelay(work,
                    initialDelay, delay,
                    unit);
        } else {
            return m_periodicPriorityWorkThread.schedule(work, initialDelay, unit);
        }
    }

    private void checkHeapSanity(int tableCount, int sitesPerHost, int kfactor)
    {
        long megabytes = 1024 * 1024;
        long maxMemory = Runtime.getRuntime().maxMemory() / megabytes;
        // DRv2 now is off heap
        long crazyThresh = computeMinimumHeapRqt(tableCount, sitesPerHost, kfactor);

        if (maxMemory < crazyThresh) {
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("The configuration of %d tables, %d sites-per-host, and k-factor of %d requires at least %d MB of Java heap memory. ", tableCount, sitesPerHost, kfactor, crazyThresh));
            builder.append(String.format("The maximum amount of heap memory available to the JVM is %d MB. ", maxMemory));
            builder.append("Please increase the maximum heap size using the VOLTDB_HEAPMAX environment variable and then restart VoltDB.");
            consoleLog.warn(builder.toString());
        }

    }

    // Compute the minimum required heap to run this configuration.
    long computeMinimumHeapRqt() {
        return computeMinimumHeapRqt(m_catalogContext.tables.size(),
                                     getLocalPartitionCount(),
                                     m_configuredReplicationFactor);
    }

    // Compute the minimum required heap to run this configuration.  This comes from the documentation,
    // http://voltdb.com/docs/PlanningGuide/MemSizeServers.php#MemSizeHeapGuidelines
    // Any changes there should get reflected here and vice versa.
    static public long computeMinimumHeapRqt(int tableCount, int sitesPerHost, int kfactor)
    {
        long baseRqt = 384;
        long tableRqt = 10 * tableCount;
        // K-safety Heap consumption drop to 8 MB (per node)
        // Snapshot cost 32 MB (per node)
        // Theoretically, 40 MB (per node) should be enough
        long rejoinRqt = (kfactor > 0) ? 128 * sitesPerHost : 0;
        return baseRqt + tableRqt + rejoinRqt;
    }

    private void checkThreadsSanity() {
        int tableCount = m_catalogContext.tables.size();
        int partitions = getLocalPartitionCount();
        int replicates = m_configuredReplicationFactor;
        int importPartitions = ImportManager.getPartitionsCount();
        int exportTableCount = VoltDB.getExportManager().getExportTablesCount();
        int exportNonceCount = VoltDB.getExportManager().getConnCount();

        int expThreadsCount = computeThreadsCount(tableCount, partitions, replicates, importPartitions, exportTableCount, exportNonceCount);

        // if the expected number of threads exceeds the limit, update the limit.
        if (m_maxThreadsCount < expThreadsCount) {
            updateMaxThreadsLimit();
        }

        // do insane check again.
        if (m_maxThreadsCount < expThreadsCount) {
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("The configuration of %d tables, %d partitions, %d replicates, ", tableCount, partitions, replicates));
            builder.append(String.format("with importer configuration of %d importer partitions, ", importPartitions));
            builder.append(String.format("with exporter configuration of %d export tables %d partitions %d replicates, ", exportTableCount, partitions, replicates));
            builder.append(String.format("approximately requires %d threads.", expThreadsCount));
            builder.append(String.format("The maximum number of threads to the system is %d. \n", m_maxThreadsCount));
            builder.append("Please increase the maximum system threads number or reduce the number of threads in your program, and then restart VoltDB. \n");
            consoleLog.warn(builder.toString());
        }
    }

    private void updateMaxThreadsLimit() {
        String[] command = {"bash", "-c" ,"ulimit -u"};
        String cmd_rst = ShellTools.local_cmd(command);
        try {
            m_maxThreadsCount = Integer.parseInt(cmd_rst.substring(0, cmd_rst.length() - 1));
        } catch(Exception e) {
            m_maxThreadsCount = Integer.MAX_VALUE;
        }
    }

    private int computeThreadsCount(int tableCount, int partitionCount, int replicateCount, int importerPartitionCount, int exportTableCount, int exportNonceCount) {
        final int clusterBaseCount = 5;
        final int hostBaseCount = 56;
        return clusterBaseCount + (hostBaseCount + partitionCount)
                + computeImporterThreads(importerPartitionCount)
                + computeExporterThreads(exportTableCount, partitionCount, replicateCount, exportNonceCount);
    }

    private int computeImporterThreads(int importerPartitionCount) {
        if (importerPartitionCount == 0) {
            return 0;
        }
        int importerBaseCount = 6;
        return importerBaseCount + importerPartitionCount;
    }

    private int computeExporterThreads(int exportTableCount, int partitionCount, int replicateCount, int exportNonceCount) {
        if (exportTableCount == 0) {
            return 0;
        }
        int exporterBaseCount = 1;
        return exporterBaseCount + partitionCount * exportTableCount + exportNonceCount;
    }

    @Override
    public <T> ListenableFuture<T> submitSnapshotIOWork(Callable<T> work)
    {
        assert m_snapshotIOAgent != null;
        return m_snapshotIOAgent.submit(work);
    }

    @Override
    public long getClusterUptime()
    {
        return System.currentTimeMillis() - getHostMessenger().getInstanceId().getTimestamp();
    }

    @Override
    public long getClusterCreateTime()
    {
        return m_clusterCreateTime;
    }

    @Override
    public void setClusterCreateTime(long clusterCreateTime) {
        m_clusterCreateTime = clusterCreateTime;
        if (m_catalogContext.cluster.getDrconsumerenabled() || m_catalogContext.cluster.getDrproducerenabled()) {
            hostLog.info("Restoring DR with Cluster Id " +  m_catalogContext.cluster.getDrclusterid() +
                    ". The DR cluster was first started at " + new Date(m_clusterCreateTime).toString() + ".");
        }
    }

    @Override
    public Instant getHostStartTime() {
        return m_hostStartTime;
    }

    @Override
    public SnmpTrapSender getSnmpTrapSender() {
        return m_snmp;
    }

    private final Supplier<String> terminusNonceSupplier = Suppliers.memoize(new Supplier<String>() {
        @Override
        public String get() {
            File markerFH = new File(m_nodeSettings.getVoltDBRoot(), VoltDB.TERMINUS_MARKER);
            // file needs to be both writable and readable as it will be deleted onRestoreComplete
            if (!markerFH.exists() || !markerFH.isFile() || !markerFH.canRead() || !markerFH.canWrite()) {
                return null;
            }
            String nonce = null;
            try (BufferedReader rdr = new BufferedReader(new FileReader(markerFH))){
                nonce = rdr.readLine();
            } catch (IOException e) {
                throw new RuntimeException(e); // highly unlikely
            }
            // make sure that there is a snapshot associated with the terminus nonce
            HashMap<String, Snapshot> snapshots = new HashMap<>();
            FileFilter filter = new SnapshotUtil.SnapshotFilter();

            SnapshotUtil.retrieveSnapshotFiles(
                    m_nodeSettings.resolveToAbsolutePath(m_nodeSettings.getSnapshot()),
                    snapshots, filter, false, SnapshotPathType.SNAP_AUTO, hostLog);

            return snapshots.containsKey(nonce) ? nonce : null;
        }
    });

    /**
     * Reads the file containing the startup snapshot nonce
     * @return null if the file is not accessible, or the startup snapshot nonce
     */
    private String getTerminusNonce() {
        return terminusNonceSupplier.get();
    }

    @Override
    public Cartographer getCartographer() {
        return m_cartographer;
    }

    @Override
    public void swapTables(String oneTable, String otherTable) {
        if (m_consumerDRGateway != null) {
            Table tableA = m_catalogContext.tables.get(oneTable);
            Table tableB = m_catalogContext.tables.get(otherTable);
            assert (tableA != null && tableB != null);
            if (tableA.getIsdred() && tableB.getIsdred()) {
                long signatureHashA = Hashing.sha1().hashString(tableA.getSignature(), Charsets.UTF_8).asLong();
                long signatureHashB = Hashing.sha1().hashString(tableB.getSignature(), Charsets.UTF_8).asLong();
                Set<Pair<String, Long>> swappedTables = new HashSet<>();
                swappedTables.add(Pair.of(oneTable.toUpperCase(), signatureHashA));
                swappedTables.add(Pair.of(otherTable.toUpperCase(), signatureHashB));
                m_consumerDRGateway.swapTables(swappedTables);
            }
        }
    }

    public static void printDiagnosticInformation(CatalogContext context, String procName, LoadedProcedureSet procSet) {
        StringBuilder sb = new StringBuilder();
        final CatalogMap<Procedure> catalogProcedures = context.database.getProcedures();
        sb.append("Statements within ").append(procName).append(": ").append("\n");
        Procedure proc = catalogProcedures.get(procName);
        if (proc != null) {
            sb.append(CatalogUtil.printUserProcedureDetail(proc));
        } else {
            sb.append("Unknown procedure: ").append(procName);
        }
        hostLog.error(sb.toString());
    }

    public void logMessageToFLC(long timestampMilis, String user, String ip) {
        m_flc.logMessage(timestampMilis, user, ip);
    }

    public void setClusterSettingsForTest(ClusterSettings settings) {
        m_clusterSettings.set(settings, 1);
    }

    public int getHostCount() {
        return m_clusterSettings.get().hostcount();
    }

    @Override
    public HTTPAdminListener getHttpAdminListener() {
        return m_adminListener;
    }

    @Override
    public long getLowestSiteId() {
        return m_iv2Initiators.firstEntry().getValue().getInitiatorHSId();
    }

    @Override
    public int getLowestPartitionId() {
        return m_iv2Initiators.firstKey();
    }

    public void updateReplicaForJoin(long siteId, TransactionState transactionState) {
        m_iv2Initiators.values().stream().filter(p->p.getInitiatorHSId() == siteId)
                .forEach(s -> ((SpInitiator) s).updateReplicasForJoin(transactionState));
    }

    @Override
    public int getKFactor() {
        return m_configuredReplicationFactor;
    }

    @Override
    public boolean isJoining() {
        return m_joining;
    }

    public Initiator getInitiator(int partition) {
        return m_iv2Initiators.get(partition);
    }

    @Override
    public ElasticService getElasticService() {
        return m_elasticService;
    }

    @Override
    public TaskManager getTaskManager() {
        return m_taskManager;
    }

    @Override
    public AvroSerde getAvroSerde() {
        return m_avroSerde;
    }

    @Override
    public void notifyOfShutdown() {
        if (m_messenger != null) {
            Set<Integer> liveHosts = m_messenger.getLiveHostIds();
            liveHosts.remove(m_messenger.getHostId());
            SiteFailureForwardMessage msg = new SiteFailureForwardMessage();
            msg.m_reportingHSId = CoreUtils.getHSIdFromHostAndSite(m_messenger.getHostId(), HostMessenger.CLIENT_INTERFACE_SITE_ID);
            for (int hostId : liveHosts) {
                m_messenger.send(CoreUtils.getHSIdFromHostAndSite(hostId, HostMessenger.CLIENT_INTERFACE_SITE_ID), msg);
            }
        }
    }

    @Override
    public boolean isMasterOnly() {
        return m_isMasterOnly;
    }

    @Override
    public void setMasterOnly() {
        m_isMasterOnly = true;
    }

    public void cleanupBackLogsOnDecommisionedReplicas(int executorPartition) {
        // execute on the lowest master site only
        if (executorPartition == getLowestLeaderPartitionId()) {
            m_iv2Initiators.values().stream().filter(p -> p.getPartitionId() != MpInitiator.MP_INIT_PID &&
                    ((SpInitiator)p).getServiceState().isRemoved())
            .forEach(s -> ((SpInitiator)s).getScheduler().cleanupTransactionBacklogs());
        }
    }

    private int getLowestLeaderPartitionId(){
        List<Integer> leaderPartitions = getLeaderPartitionIds();
        return leaderPartitions.isEmpty() ? -1 : leaderPartitions.iterator().next();
    }

    public List<Integer> getLeaderPartitionIds(){
        return m_iv2Initiators.values().stream().filter(p -> p.getPartitionId() != MpInitiator.MP_INIT_PID && ((SpInitiator) p).isLeader())
                .map(Initiator::getPartitionId).collect(Collectors.toList());
    }

    public List<Integer> getNonLeaderPartitionIds(){
        return m_iv2Initiators.values().stream().filter(p -> p.getPartitionId() != MpInitiator.MP_INIT_PID && !((SpInitiator) p).isLeader())
                .map(Initiator::getPartitionId).collect(Collectors.toList());
    }

    public List<Long> getLeaderSites() {
        return  m_iv2Initiators.values().stream().filter(p -> p.getPartitionId() != MpInitiator.MP_INIT_PID && ((SpInitiator) p).isLeader())
                .map(Initiator::getInitiatorHSId).collect(Collectors.toList());
    }

    public void processReplicaDecommission(int leaderCount) {
        synchronized(m_startAndStopLock) {
            setMasterOnly();
            if (leaderCount != m_nodeSettings.getLocalActiveSitesCount()) {
                NavigableMap<String, String> settings = m_nodeSettings.asMap();
                ImmutableMap<String, String> newSettings = new ImmutableMap.Builder<String, String>()
                        .putAll(new HashMap<String, String>() {
                            private static final long serialVersionUID = 1L; {
                                putAll(settings);
                                put(NodeSettings.LOCAL_ACTIVE_SITES_COUNT_KEY, Integer.toString(leaderCount));
                            }}).build();
                // update active site count
                m_nodeSettings = NodeSettings.create(newSettings);
                m_nodeSettings.store();
                m_catalogContext.getDbSettings().setNodeSettings(m_nodeSettings);
                hostLog.info("Update local active site count to :" + leaderCount);

                // Update the catalog update and log update barrier to expect the new partition count
                VoltDB.getSiteCountBarrier().setPartyCount(leaderCount);

                // release export resources
                VoltDB.getExportManager().releaseResources(getNonLeaderPartitionIds());
                if (m_commandLog != null) {
                    m_commandLog.notifyDecommissionPartitions(getNonLeaderPartitionIds());
                }
            }
        }
    }

    public boolean isPartitionDecommissioned(int partitionId) {
        if (partitionId != MpInitiator.MP_INIT_PID) {
            SpInitiator init = (SpInitiator)m_iv2Initiators.get(partitionId);
            return (init != null && !(init.getServiceState().isNormal()));
        }
        return false;
    }

    @Override
    public DrProducerCatalogCommands getDrCatalogCommands() {
        return m_drCatalogCommands;
    }

    /**
     * @return true if cluster is recovered with missing nodes and dr enabled
     */
    @Override
    public boolean doRecoverCheck() {
        if (m_durable && m_config.m_missingHostCount > 0 && m_config.m_startAction == StartAction.RECOVER) {
            return !DrRoleType.NONE.value().equals(m_catalogContext.getCluster().getDrrole());
        }
        return false;
    }
}
