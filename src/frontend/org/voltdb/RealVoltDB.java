/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra_voltpatches.GCInspector;
import org.apache.log4j.Appender;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.SiteMailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.OnDemandBinaryLogger;
import org.voltcore.utils.Pair;
import org.voltcore.utils.ShutdownHooks;
import org.voltcore.utils.VersionChecker;
import org.voltcore.zk.CoreZK;
import org.voltcore.zk.ZKCountdownLatch;
import org.voltcore.zk.ZKUtil;
import org.voltdb.TheHashinator.HashinatorType;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Deployment;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.catalog.Systemsettings;
import org.voltdb.common.NodeState;
import org.voltdb.compiler.AdHocCompilerCache;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.deploymentfile.ClusterType;
import org.voltdb.compiler.deploymentfile.ConsistencyType;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.PartitionDetectionType;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.compiler.deploymentfile.SystemSettingsType;
import org.voltdb.dtxn.InitiatorStats;
import org.voltdb.dtxn.LatencyHistogramStats;
import org.voltdb.dtxn.LatencyStats;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.export.ExportManager;
import org.voltdb.importer.ImportManager;
import org.voltdb.iv2.BaseInitiator;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.Initiator;
import org.voltdb.iv2.KSafetyStats;
import org.voltdb.iv2.LeaderAppointer;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.SpInitiator;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.join.BalancePartitionsStatistics;
import org.voltdb.join.ElasticJoinService;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.planner.ActivePlanRepository;
import org.voltdb.probe.MeshProber;
import org.voltdb.processtools.ShellTools;
import org.voltdb.rejoin.Iv2RejoinCoordinator;
import org.voltdb.rejoin.JoinCoordinator;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.ClusterSettingsRef;
import org.voltdb.settings.PathSettings;
import org.voltdb.settings.Settings;
import org.voltdb.settings.SettingsException;
import org.voltdb.utils.CLibrary;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndIds;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.VoltFile;
import org.voltdb.utils.VoltSampler;

import com.google_voltpatches.common.base.Charsets;
import com.google_voltpatches.common.base.Joiner;
import com.google_voltpatches.common.base.Preconditions;
import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.net.HostAndPort;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

/**
 * RealVoltDB initializes global server components, like the messaging
 * layer, ExecutionSite(s), and ClientInterface. It provides accessors
 * or references to those global objects. It is basically the global
 * namespace. A lot of the global namespace is described by VoltDBInterface
 * to allow test mocking.
 */
public class RealVoltDB implements VoltDBInterface, RestoreAgent.Callback, HostMessenger.HostWatcher {
    private static final boolean DISABLE_JMX = Boolean.valueOf(System.getProperty("DISABLE_JMX", "true"));

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

    private final VoltLogger hostLog = new VoltLogger("HOST");
    private final VoltLogger consoleLog = new VoltLogger("CONSOLE");

    private VoltDB.Configuration m_config = new VoltDB.Configuration();
    int m_configuredNumberOfPartitions;
    int m_configuredReplicationFactor;
    // CatalogContext is immutable, just make sure that accessors see a consistent version
    volatile CatalogContext m_catalogContext;
    // Managed voltdb directories settings
    volatile private PathSettings m_paths;
    // Cluster settings reference and supplier
    final ClusterSettingsRef m_clusterSettings = new ClusterSettingsRef();
    private String m_buildString;
    static final String m_defaultVersionString = "6.7";
    // by default set the version to only be compatible with itself
    static final String m_defaultHotfixableRegexPattern = "^\\Q6.7\\E\\z";
    // these next two are non-static because they can be overrriden on the CLI for test
    private String m_versionString = m_defaultVersionString;
    private String m_hotfixableRegexPattern = m_defaultHotfixableRegexPattern;
    HostMessenger m_messenger = null;
    private ClientInterface m_clientInterface = null;
    HTTPAdminListener m_adminListener;
    private OpsRegistrar m_opsRegistrar = new OpsRegistrar();

    private AsyncCompilerAgent m_asyncCompilerAgent = null;
    public AsyncCompilerAgent getAsyncCompilerAgent() { return m_asyncCompilerAgent; }
    private PartitionCountStats m_partitionCountStats = null;
    private IOStats m_ioStats = null;
    private MemoryStats m_memoryStats = null;
    private CpuStats m_cpuStats = null;
    private CommandLogStats m_commandLogStats = null;
    private StatsManager m_statsManager = null;
    private SnapshotCompletionMonitor m_snapshotCompletionMonitor;
    // These are unused locally, but they need to be registered with the StatsAgent so they're
    // globally available
    @SuppressWarnings("unused")
    private InitiatorStats m_initiatorStats;
    private LiveClientsStats m_liveClientsStats = null;
    int m_myHostId;
    String m_httpPortExtraLogMessage = null;
    boolean m_jsonEnabled;

    // IV2 things
    TreeMap<Integer, Initiator> m_iv2Initiators = new TreeMap<>();
    Cartographer m_cartographer = null;
    LeaderAppointer m_leaderAppointer = null;
    GlobalServiceElector m_globalServiceElector = null;
    MpInitiator m_MPI = null;
    Map<Integer, Long> m_iv2InitiatorStartingTxnIds = new HashMap<>();
    private ScheduledFuture<?> resMonitorWork;


    private NodeStateTracker m_statusTracker;
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
    private final Object m_safeMpTxnIdLock = new Object();
    private long m_lastSeenMpTxnId = Long.MIN_VALUE;
    private long m_safeMpTxnId = Long.MAX_VALUE;
    String m_rejoinTruncationReqId = null;

    // Are we adding the node to the cluster instead of rejoining?
    volatile boolean m_joining = false;
    private boolean m_shuttingdown = false;

    long m_clusterCreateTime;
    AtomicBoolean m_replicationActive = new AtomicBoolean(false);
    private ProducerDRGateway m_producerDRGateway = null;
    private ConsumerDRGateway m_consumerDRGateway = null;

    //Only restrict recovery completion during test
    static Semaphore m_testBlockRecoveryCompletion = new Semaphore(Integer.MAX_VALUE);
    private long m_executionSiteRecoveryFinish;
    private long m_executionSiteRecoveryTransferred;

    // Rejoin coordinator
    private JoinCoordinator m_joinCoordinator = null;
    private ElasticJoinService m_elasticJoinService = null;

    // Snapshot IO agent
    private SnapshotIOAgent m_snapshotIOAgent = null;

    // id of the leader, or the host restore planner says has the catalog
    int m_hostIdWithStartupCatalog;
    String m_pathToStartupCatalog;

    // Synchronize initialize and shutdown
    private final Object m_startAndStopLock = new Object();

    // Synchronize updates of catalog contexts across the multiple sites on this host.
    // Ensure that the first site to reach catalogUpdate() does all the work and that no
    // others enter until that's finished.  CatalogContext is immutable and volatile, accessors
    // should be able to always get a valid context without needing this lock.
    private final Object m_catalogUpdateLock = new Object();

    // add a random number to the sampler output to make it likely to be unique for this process.
    private final VoltSampler m_sampler = new VoltSampler(10, "sample" + String.valueOf(new Random().nextInt() % 10000) + ".txt");
    private final AtomicBoolean m_hasStartedSampler = new AtomicBoolean(false);

    List<Integer> m_partitionsToSitesAtStartupForExportInit;

    RestoreAgent m_restoreAgent = null;

    private final ListeningExecutorService m_es = CoreUtils.getCachedSingleThreadExecutor("StartAction ZK Watcher", 15000);

    private volatile boolean m_isRunning = false;
    private boolean m_isRunningWithOldVerb = true;
    private boolean m_isBare = false;

    private int m_maxThreadsCount;

    @Override
    public boolean isRunningWithOldVerbs() {
        return m_isRunningWithOldVerb;
     };

    @Override
    public boolean isShuttingdown() {
        return m_shuttingdown;
    }
    @Override
    public void setShuttingdown(boolean shuttingdown) {
        m_shuttingdown = shuttingdown;
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

    private long m_recoveryStartTime;

    CommandLog m_commandLog;

    private volatile OperationMode m_mode = OperationMode.INITIALIZING;
    private OperationMode m_startMode = null;

    volatile String m_localMetadata = "";

    private ListeningExecutorService m_computationService;

    private Thread m_configLogger;

    // methods accessed via the singleton
    @Override
    public void startSampler() {
        if (m_hasStartedSampler.compareAndSet(false, true)) {
            m_sampler.start();
        }
    }

    private ScheduledThreadPoolExecutor m_periodicWorkThread;
    private ScheduledThreadPoolExecutor m_periodicPriorityWorkThread;

    // The configured license api: use to decide enterprise/community edition feature enablement
    LicenseApi m_licenseApi;
    String m_licenseInformation = "";
    private LatencyStats m_latencyStats;

    private LatencyHistogramStats m_latencyHistogramStats;

    private File getConfigDirectory() {
        return getConfigDirectory(m_config);
    }

    private File getConfigDirectory(Configuration config) {
        return getConfigDirectory(config.m_voltdbRoot);
    }

    private File getConfigDirectory(File voltdbroot) {
        return new VoltFile(voltdbroot, VoltDB.CONFIG_DIR);
    }

    private File getConfigLogDeployment() {
        return getConfigLogDeployment(m_config);
    }

    private File getConfigLogDeployment(Configuration config) {
        return new VoltFile(getConfigDirectory(config), "deployment.xml");
    }

    @Override
    public LicenseApi getLicenseApi() {
        return m_licenseApi;
    }

    @Override
    public String getLicenseInformation() {
        return m_licenseInformation;
    }

    @Override
    public String getVoltDBRootPath(PathsType.Voltdbroot path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_paths.getVoltDBRoot().getPath();
    }

    @Override
    public String getCommandLogPath(PathsType.Commandlog path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_paths.resolve(m_paths.getCommandLog()).getPath();
    }

    @Override
    public String getCommandLogSnapshotPath(PathsType.Commandlogsnapshot path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_paths.resolve(m_paths.getCommandLogSnapshot()).getPath();
    }

    @Override
    public String getSnapshotPath(PathsType.Snapshots path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_paths.resolve(m_paths.getSnapshoth()).getPath();
    }

    @Override
    public String getExportOverflowPath(PathsType.Exportoverflow path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_paths.resolve(m_paths.getExportOverflow()).getPath();
    }

    @Override
    public String getDROverflowPath(PathsType.Droverflow path) {
        if (isRunningWithOldVerbs()) {
           return path.getPath();
        }
        return m_paths.resolve(m_paths.getDROverflow()).getPath();
    }

    @Override
    public String getVoltDBRootPath() {
        return m_paths.getVoltDBRoot().getPath();
    }

    @Override
    public String getCommandLogPath() {
        return m_paths.resolve(m_paths.getCommandLog()).getPath();
    }

    @Override
    public String getCommandLogSnapshotPath() {
        return m_paths.resolve(m_paths.getCommandLogSnapshot()).getPath();
    }

    @Override
    public String getSnapshotPath() {
        return m_paths.resolve(m_paths.getSnapshoth()).getPath();
    }

    @Override
    public String getExportOverflowPath() {
        return m_paths.resolve(m_paths.getExportOverflow()).getPath();
    }

    @Override
    public String getDROverflowPath() {
        return m_paths.resolve(m_paths.getDROverflow()).getPath();
    }

    private String managedPathEmptyCheck(String voltDbRoot, String path) {
        VoltFile managedPath;
        if (new File(path).isAbsolute())
            managedPath = new VoltFile(path);
        else
            managedPath = new VoltFile(voltDbRoot, path);
        if (managedPath.exists() && managedPath.list().length > 0)
            return managedPath.getAbsolutePath();
        return null;
    }

    private void managedPathsEmptyCheck(Configuration config) {
        List<String> nonEmptyPaths = managedPathsWithFiles(config, m_catalogContext.getDeployment());
        if (!nonEmptyPaths.isEmpty()) {
            StringBuilder crashMessage =
                    new StringBuilder("Files from a previous database session exist in the managed directories:");
            for (String nonEmptyPath : nonEmptyPaths) {
                crashMessage.append("\n  - " + nonEmptyPath);
            }
            crashMessage.append("\nUse the recover command to restore the previous database or use create --force" +
                " to start a new database session overwriting existing files.");
            VoltDB.crashLocalVoltDB(crashMessage.toString());
        }
    }

    private List<String> managedPathsWithFiles(Configuration config, DeploymentType deployment) {
        ImmutableList.Builder<String> nonEmptyPaths = ImmutableList.builder();
        if (!config.m_isEnterprise) {
            return nonEmptyPaths.build();
        }
        PathsType paths = deployment.getPaths();
        String voltDbRoot = getVoltDBRootPath(paths.getVoltdbroot());
        String path;
        if ((path = managedPathEmptyCheck(voltDbRoot, getSnapshotPath(paths.getSnapshots()))) != null)
            nonEmptyPaths.add(path);
        if ((path = managedPathEmptyCheck(voltDbRoot, getExportOverflowPath(paths.getExportoverflow()))) != null)
            nonEmptyPaths.add(path);
        if ((path = managedPathEmptyCheck(voltDbRoot, getDROverflowPath(paths.getDroverflow()))) != null)
            nonEmptyPaths.add(path);
        if ((path = managedPathEmptyCheck(voltDbRoot, getCommandLogPath(paths.getCommandlog()))) != null)
            nonEmptyPaths.add(path);
        if ((path = managedPathEmptyCheck(voltDbRoot, getCommandLogSnapshotPath(paths.getCommandlogsnapshot()))) != null)
            nonEmptyPaths.add(path);
        return nonEmptyPaths.build();
    }

    private final List<String> pathsWithRecoverableArtifacts(DeploymentType deployment) {
        ImmutableList.Builder<String> nonEmptyPaths = ImmutableList.builder();
        if (!MiscUtils.isPro()) {
            return nonEmptyPaths.build();
        }
        PathsType paths = deployment.getPaths();
        String voltDbRoot = getVoltDBRootPath(paths.getVoltdbroot());
        String path;
        if ((path = managedPathEmptyCheck(voltDbRoot, getSnapshotPath(paths.getSnapshots()))) != null)
            nonEmptyPaths.add(path);
        if ((path = managedPathEmptyCheck(voltDbRoot, getCommandLogPath(paths.getCommandlog()))) != null)
            nonEmptyPaths.add(path);
        if ((path = managedPathEmptyCheck(voltDbRoot, getCommandLogSnapshotPath(paths.getCommandlogsnapshot()))) != null)
            nonEmptyPaths.add(path);
        return nonEmptyPaths.build();
    }

    /**
     * Initialize all the global components, then initialize all the m_sites.
     * @param config configuration that gets passed in from commandline.
     */
    @Override
    public void initialize(Configuration config) {
        ShutdownHooks.enableServerStopLogging();
        synchronized(m_startAndStopLock) {
            m_isRunningWithOldVerb = config.m_startAction.isLegacy();

            // check that this is a 64 bit VM
            if (System.getProperty("java.vm.name").contains("64") == false) {
                hostLog.fatal("You are running on an unsupported (probably 32 bit) JVM. Exiting.");
                System.exit(-1);
            }

            m_isRunningWithOldVerb = config.m_startAction.isLegacy();
            readBuildInfo(config.m_isEnterprise ? "Enterprise Edition" : "Community Edition");

            // Replay command line args that we can see
            StringBuilder sb = new StringBuilder(2048).append("Command line arguments: ");
            sb.append(System.getProperty("sun.java.command", "[not available]"));
            hostLog.info(sb.toString());

            List<String> iargs = ManagementFactory.getRuntimeMXBean().getInputArguments();
            sb.delete(0, sb.length()).append("Command line JVM arguments:");
            for (String iarg : iargs)
                sb.append(" ").append(iarg);
            if (iargs.size() > 0) hostLog.info(sb.toString());
            else hostLog.info("No JVM command line args known.");

            sb.delete(0, sb.length()).append("Command line JVM classpath: ");
            sb.append(System.getProperty("java.class.path", "[not available]"));
            hostLog.info(sb.toString());

            // config UUID is part of the status tracker that is slated to be an
            // Information source for an http admun endpoint
            m_statusTracker = new NodeStateTracker();

            consoleLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartupString.name(), null);

            if (config.m_startAction == StartAction.INITIALIZE) {
                if (config.m_forceVoltdbCreate) {
                    deleteInitializationMarkers(config);
                }
            }

            // If there's no deployment provide a default and put it under voltdbroot.
            if (config.m_pathToDeployment == null) {
                try {
                    config.m_pathToDeployment = setupDefaultDeployment(hostLog, config.m_voltdbRoot);
                    config.m_deploymentDefault = true;
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Failed to write default deployment.", false, null);
                    return;
                }
            }

            ReadDeploymentResults readDepl = readPrimedDeployment(config);

            if (config.m_startAction == StartAction.INITIALIZE) {
                if (config.m_forceVoltdbCreate && m_paths.clean()) {
                    String msg = "Archived previous snapshot directory to " + m_paths.getSnapshoth() + ".1";
                    consoleLog.info(msg);
                    hostLog.info(msg);
                }
                stageDeploymemtFileForInitialize(config, readDepl.deployment);
                stageInitializedMarker(config);
                hostLog.info("Initialized VoltDB root directory " + config.m_voltdbRoot.getPath());
                consoleLog.info("Initialized VoltDB root directory " + config.m_voltdbRoot.getPath());
                VoltDB.exit(0);
            }

            if (config.m_startAction.isLegacy()) {
                File rootFH = CatalogUtil.getVoltDbRoot(readDepl.deployment.getPaths());
                File inzFH = new VoltFile(rootFH, VoltDB.INITIALIZED_MARKER);
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
                        inzFH = new VoltFile(rootFH, VoltDB.INITIALIZED_MARKER);
                        if (inzFH.exists()) {
                            VoltDB.crashLocalVoltDB("Can not use legacy start action "
                                    + config.m_startAction + " on voltdbroot "
                                    + rootFH + " that was initialized with the init command");
                            return;
                        }
                    }
                }
            }

            List<String> failed = m_paths.ensureDirectoriesExist();
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
            m_startMode = null;

            // set a bunch of things to null/empty/new for tests
            // which reusue the process
            m_safeMpTxnId = Long.MAX_VALUE;
            m_lastSeenMpTxnId = Long.MIN_VALUE;
            m_clientInterface = null;
            m_adminListener = null;
            m_commandLog = new DummyCommandLog();
            m_messenger = null;
            m_opsRegistrar = new OpsRegistrar();
            m_asyncCompilerAgent = null;
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
            Map<String, String> fromCommandLine = m_config.asClusterSettingsMap();
            Map<String, String> fromDeploymentFile = CatalogUtil.
                    asClusterSettingsMap(readDepl.deployment);

            ClusterSettings clusterSettings = ClusterSettings.create(
                    fromCommandLine, fromPropertyFile.asMap(), fromDeploymentFile);

            // persist the merged settings
            clusterSettings.store();

            m_clusterSettings.set(clusterSettings, 1);

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

            // determine if this is a rejoining node
            // (used for license check and later the actual rejoin)
            boolean isRejoin = m_config.m_startAction.doesRejoin();
            m_rejoining = isRejoin;
            m_rejoinDataPending = m_config.m_startAction.doesJoin();

            m_joining = m_config.m_startAction == StartAction.JOIN;

            if (isRejoin || m_joining) {
                m_statusTracker.setNodeState(NodeState.REJOINING);
            }
            //Register dummy agents immediately
            m_opsRegistrar.registerMailboxes(m_messenger);


            //Start validating the build string in the background
            final Future<?> buildStringValidation = validateBuildString(getBuildString(), m_messenger.getZK());

            // race to create start action nodes and then verify theirs compatibility.
            m_messenger.getZK().create(VoltZK.start_action, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new ZKUtil.StringCallback(), null);
            VoltZK.createStartActionNode(m_messenger.getZK(), m_messenger.getHostId(), m_config.m_startAction);
            validateStartAction();

            Map<Integer, String> hostGroups = null;

            final int numberOfNodes = readDeploymentAndCreateStarterCatalogContext(config);
            if (config.m_isEnterprise && m_config.m_startAction.doesRequireEmptyDirectories()
                    && !config.m_forceVoltdbCreate) {
                    managedPathsEmptyCheck(config);
            }

            if (!isRejoin && !m_joining) {
                hostGroups = m_messenger.waitForGroupJoin(numberOfNodes);
            }
            if (m_messenger.isPaused() || m_config.m_isPaused) {
                setStartMode(OperationMode.PAUSED);
                setMode(OperationMode.PAUSED);
            }

            // Create the thread pool here. It's needed by buildClusterMesh()
            m_periodicWorkThread =
                    CoreUtils.getScheduledThreadPoolExecutor("Periodic Work", 1, CoreUtils.SMALL_STACK_SIZE);
            m_periodicPriorityWorkThread =
                    CoreUtils.getScheduledThreadPoolExecutor("Periodic Priority Work", 1, CoreUtils.SMALL_STACK_SIZE);

            Class<?> snapshotIOAgentClass = MiscUtils.loadProClass("org.voltdb.SnapshotIOAgentImpl", "Snapshot", true);
            if (snapshotIOAgentClass != null) {
                try {
                    m_snapshotIOAgent = (SnapshotIOAgent) snapshotIOAgentClass.getConstructor(HostMessenger.class, long.class)
                            .newInstance(m_messenger, m_messenger.getHSIdForLocalSite(HostMessenger.SNAPSHOT_IO_AGENT_ID));
                    m_messenger.createMailbox(m_snapshotIOAgent.getHSId(), m_snapshotIOAgent);
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Failed to instantiate snapshot IO agent", true, e);
                }
            }

            if (m_config.m_pathToLicense == null) {
                m_licenseApi = MiscUtils.licenseApiFactory();
                if (m_licenseApi == null) {
                    hostLog.fatal("Unable to open license file in default directories");
                }
            }
            else {
                m_licenseApi = MiscUtils.licenseApiFactory(m_config.m_pathToLicense);
                if (m_licenseApi == null) {
                    hostLog.fatal("Unable to open license file in provided path: " + m_config.m_pathToLicense);
                }

            }

            if (m_licenseApi == null) {
                hostLog.fatal("Please contact sales@voltdb.com to request a license.");
                VoltDB.crashLocalVoltDB("Failed to initialize license verifier. " +
                        "See previous log message for details.", false, null);
            }
            m_asyncCompilerAgent = new AsyncCompilerAgent(m_licenseApi);

            try {
                SimpleDateFormat sdf = new SimpleDateFormat("EEE MMM d, yyyy");
                JSONObject jo = new JSONObject();
                jo.put("trial",m_licenseApi.isTrial());
                jo.put("hostcount",m_licenseApi.maxHostcount());
                jo.put("commandlogging", m_licenseApi.isCommandLoggingAllowed());
                jo.put("wanreplication", m_licenseApi.isDrReplicationAllowed());
                jo.put("expiration", sdf.format(m_licenseApi.expires().getTime()));
                m_licenseInformation = jo.toString();
            } catch (JSONException ex) {
                //Ignore
            }

            // Create the GlobalServiceElector.  Do this here so we can register the MPI with it
            // when we construct it below
            m_globalServiceElector = new GlobalServiceElector(m_messenger.getZK(), m_messenger.getHostId());
            // Start the GlobalServiceElector.  Not sure where this will actually belong.
            try {
                m_globalServiceElector.start();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to start GlobalServiceElector", true, e);
            }

            // Always create a mailbox for elastic join data transfer
            if (m_config.m_isEnterprise) {
                long elasticHSId = m_messenger.getHSIdForLocalSite(HostMessenger.REBALANCE_SITE_ID);
                m_messenger.createMailbox(elasticHSId, new SiteMailbox(m_messenger, elasticHSId));
            }

            if (m_joining) {
                Class<?> elasticJoinCoordClass =
                        MiscUtils.loadProClass("org.voltdb.join.ElasticJoinNodeCoordinator", "Elastic", false);
                try {
                    Constructor<?> constructor = elasticJoinCoordClass.getConstructor(HostMessenger.class, String.class);
                    m_joinCoordinator = (JoinCoordinator) constructor.newInstance(m_messenger, VoltDB.instance().getVoltDBRootPath());
                    m_messenger.registerMailbox(m_joinCoordinator);
                    m_joinCoordinator.initialize(m_catalogContext.getDeployment().getCluster().getKfactor());
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
            JSONObject topo = getTopology(config.m_startAction, hostGroups, m_joinCoordinator);
            m_partitionsToSitesAtStartupForExportInit = new ArrayList<>();
            try {
                // IV2 mailbox stuff
                ClusterConfig clusterConfig = new ClusterConfig(topo);
                m_configuredReplicationFactor = clusterConfig.getReplicationFactor();
                m_cartographer = new Cartographer(m_messenger, m_configuredReplicationFactor,
                        m_catalogContext.cluster.getNetworkpartition());
                List<Integer> partitions = null;
                if (isRejoin) {
                    m_configuredNumberOfPartitions = m_cartographer.getPartitionCount();
                    partitions = m_cartographer.getIv2PartitionsToReplace(m_configuredReplicationFactor,
                                                                          clusterConfig.getSitesPerHost());
                    if (partitions.size() == 0) {
                        VoltDB.crashLocalVoltDB("The VoltDB cluster already has enough nodes to satisfy " +
                                "the requested k-safety factor of " +
                                m_configuredReplicationFactor + ".\n" +
                                "No more nodes can join.", false, null);
                    }
                }
                else {
                    m_configuredNumberOfPartitions = clusterConfig.getPartitionCount();
                    partitions = ClusterConfig.partitionsForHost(topo, m_messenger.getHostId());
                }
                for (int ii = 0; ii < partitions.size(); ii++) {
                    Integer partition = partitions.get(ii);
                    m_iv2InitiatorStartingTxnIds.put( partition, TxnEgo.makeZero(partition).getTxnId());
                }
                m_iv2Initiators = createIv2Initiators(
                        partitions,
                        m_config.m_startAction,
                        m_partitionsToSitesAtStartupForExportInit);
                m_iv2InitiatorStartingTxnIds.put(
                        MpInitiator.MP_INIT_PID,
                        TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId());
                // Pass the local HSIds to the MPI so it can farm out buddy sites
                // to the RO MP site pool
                List<Long> localHSIds = new ArrayList<>();
                for (Initiator ii : m_iv2Initiators.values()) {
                    localHSIds.add(ii.getInitiatorHSId());
                }
                m_MPI = new MpInitiator(m_messenger, localHSIds, getStatsAgent());
                m_iv2Initiators.put(MpInitiator.MP_INIT_PID, m_MPI);

                // Make a list of HDIds to join
                Map<Integer, Long> partsToHSIdsToRejoin = new HashMap<>();
                for (Initiator init : m_iv2Initiators.values()) {
                    if (init.isRejoinable()) {
                        partsToHSIdsToRejoin.put(init.getPartitionId(), init.getInitiatorHSId());
                    }
                }
                OnDemandBinaryLogger.path = VoltDB.instance().getVoltDBRootPath();
                if (isRejoin) {
                    SnapshotSaveAPI.recoveringSiteCount.set(partsToHSIdsToRejoin.size());
                    hostLog.info("Set recovering site count to " + partsToHSIdsToRejoin.size());

                    m_joinCoordinator = new Iv2RejoinCoordinator(m_messenger,
                            partsToHSIdsToRejoin.values(),
                            VoltDB.instance().getVoltDBRootPath(),
                            m_config.m_startAction == StartAction.LIVE_REJOIN);
                    m_joinCoordinator.initialize(m_catalogContext.getDeployment().getCluster().getKfactor());
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

            // do the many init tasks in the Inits class
            Inits inits = new Inits(m_statusTracker, this, 1);
            inits.doInitializationWork();

            // Need the catalog so that we know how many tables so we can guess at the necessary heap size
            // This is done under Inits.doInitializationWork(), so need to wait until we get here.
            // Current calculation needs pro/community knowledge, number of tables, and the sites/host,
            // which is the number of initiators (minus the possibly idle MPI initiator)
            checkHeapSanity(MiscUtils.isPro(), m_catalogContext.tables.size(),
                    (m_iv2Initiators.size() - 1), m_configuredReplicationFactor);

            if (m_joining && m_config.m_replicationRole == ReplicationRole.REPLICA) {
                VoltDB.crashLocalVoltDB("Elastic join is prohibited on a replica cluster.", false, null);
            }

            collectLocalNetworkMetadata();

            /*
             * Construct an adhoc planner for the initial catalog
             */
            final CatalogSpecificPlanner csp = new CatalogSpecificPlanner(m_asyncCompilerAgent, m_catalogContext);

            // Initialize stats
            m_ioStats = new IOStats();
            getStatsAgent().registerStatsSource(StatsSelector.IOSTATS,
                    0, m_ioStats);
            m_memoryStats = new MemoryStats();
            getStatsAgent().registerStatsSource(StatsSelector.MEMORY,
                    0, m_memoryStats);
            getStatsAgent().registerStatsSource(StatsSelector.TOPO, 0, m_cartographer);
            m_partitionCountStats = new PartitionCountStats(m_cartographer);
            getStatsAgent().registerStatsSource(StatsSelector.PARTITIONCOUNT,
                    0, m_partitionCountStats);
            m_initiatorStats = new InitiatorStats(m_myHostId);
            m_liveClientsStats = new LiveClientsStats();
            getStatsAgent().registerStatsSource(StatsSelector.LIVECLIENTS, 0, m_liveClientsStats);
            m_latencyStats = new LatencyStats(m_myHostId);
            getStatsAgent().registerStatsSource(StatsSelector.LATENCY, 0, m_latencyStats);
            m_latencyHistogramStats = new LatencyHistogramStats(m_myHostId);
            getStatsAgent().registerStatsSource(StatsSelector.LATENCY_HISTOGRAM,
                    0, m_latencyHistogramStats);


            BalancePartitionsStatistics rebalanceStats = new BalancePartitionsStatistics();
            getStatsAgent().registerStatsSource(StatsSelector.REBALANCE, 0, rebalanceStats);

            KSafetyStats kSafetyStats = new KSafetyStats();
            getStatsAgent().registerStatsSource(StatsSelector.KSAFETY, 0, kSafetyStats);
            m_cpuStats = new CpuStats();
            getStatsAgent().registerStatsSource(StatsSelector.CPU,
                    0, m_cpuStats);

            // ENG-6321
            m_commandLogStats = new CommandLogStats(m_commandLog);
            getStatsAgent().registerStatsSource(StatsSelector.COMMANDLOG, 0, m_commandLogStats);

            /*
             * Initialize the command log on rejoin and join before configuring the IV2
             * initiators.  This will prevent them from receiving transactions
             * which need logging before the internal file writers are
             * initialized.  Root cause of ENG-4136.
             *
             * If sync command log is on, not initializing the command log before the initiators
             * are up would cause deadlock.
             */
            if ((m_commandLog != null) && (m_commandLog.needsInitialization())) {
                consoleLog.l7dlog(Level.INFO, LogKeys.host_VoltDB_StayTunedForLogging.name(), null);
            }
            else {
                consoleLog.l7dlog(Level.INFO, LogKeys.host_VoltDB_StayTunedForNoLogging.name(), null);
            }
            if (m_commandLog != null && (isRejoin || m_joining)) {
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
                        m_configuredNumberOfPartitions,
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
                m_clientInterface = ClientInterface.create(m_messenger, m_catalogContext, m_config.m_replicationRole,
                        m_cartographer,
                        m_configuredNumberOfPartitions,
                        clientIntf,
                        config.m_port,
                        adminIntf,
                        config.m_adminPort,
                        m_config.m_timestampTestingSalt);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            // DR overflow directory
            if (VoltDB.instance().getLicenseApi().isDrReplicationAllowed()) {
                try {
                    Class<?> ndrgwClass = null;
                    ndrgwClass = Class.forName("org.voltdb.dr2.DRProducer");
                    Constructor<?> ndrgwConstructor = ndrgwClass.getConstructor(File.class, File.class, boolean.class, int.class, int.class);
                    m_producerDRGateway =
                            (ProducerDRGateway) ndrgwConstructor.newInstance(
                                    new VoltFile(VoltDB.instance().getDROverflowPath()),
                                    new VoltFile(VoltDB.instance().getSnapshotPath()),
                                    m_replicationActive.get(),
                                    m_configuredNumberOfPartitions,m_catalogContext.getClusterSettings().hostcount());
                    m_producerDRGateway.start();
                    m_producerDRGateway.blockOnDRStateConvergence();
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Unable to load DR system", true, e);
                }
            }
            else {
                // set up empty stats for the DR Producer
                getStatsAgent().registerStatsSource(StatsSelector.DRPRODUCERNODE, 0,
                        new DRProducerStatsBase.DRProducerNodeStatsBase());
                getStatsAgent().registerStatsSource(StatsSelector.DRPRODUCERPARTITION, 0,
                        new DRProducerStatsBase.DRProducerPartitionStatsBase());
            }
            createDRConsumerIfNeeded();

            /*
             * Configure and start all the IV2 sites
             */
            try {
                final String serializedCatalog = m_catalogContext.catalog.serialize();
                boolean createMpDRGateway = true;
                for (Initiator iv2init : m_iv2Initiators.values()) {
                    iv2init.configure(
                            getBackendTargetType(),
                            m_catalogContext,
                            serializedCatalog,
                            m_catalogContext.getDeployment().getCluster().getKfactor(),
                            csp,
                            m_configuredNumberOfPartitions,
                            m_config.m_startAction,
                            getStatsAgent(),
                            m_memoryStats,
                            m_commandLog,
                            m_producerDRGateway,
                            iv2init != m_MPI && createMpDRGateway, // first SPI gets it
                            m_config.m_executionCoreBindings.poll());

                    if (iv2init != m_MPI) {
                        createMpDRGateway = false;
                    }
                }

                // LeaderAppointer startup blocks if the initiators are not initialized.
                // So create the LeaderAppointer after the initiators.
                boolean expectSyncSnapshot = m_config.m_replicationRole == ReplicationRole.REPLICA && config.m_startAction == StartAction.CREATE;
                m_leaderAppointer = new LeaderAppointer(
                        m_messenger,
                        m_configuredNumberOfPartitions,
                        m_catalogContext.getDeployment().getCluster().getKfactor(),
                        m_catalogContext.cluster.getFaultsnapshots().get("CLUSTER_PARTITION"),
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
                final Class<?> statsManagerClass =
                        MiscUtils.loadProClass("org.voltdb.management.JMXStatsManager", "JMX", true);
                if (statsManagerClass != null && !DISABLE_JMX) {
                    m_statsManager = (StatsManager)statsManagerClass.newInstance();
                    m_statsManager.initialize();
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

            if (!isRejoin && !m_joining) {
                try {
                    m_messenger.waitForAllHostsToBeReady(m_catalogContext.getClusterSettings().hostcount());
                } catch (Exception e) {
                    hostLog.fatal("Failed to announce ready state.");
                    VoltDB.crashLocalVoltDB("Failed to announce ready state.", false, null);
                }
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

            schedulePeriodicWorks();
            m_clientInterface.schedulePeriodicWorks();

            // print out a bunch of useful system info
            logDebuggingInfo(m_config.m_adminPort, m_config.m_httpPort, m_httpPortExtraLogMessage, m_jsonEnabled);


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
            if ((m_catalogContext.cluster.getNetworkpartition() == false) &&
                    (m_configuredReplicationFactor > 0)) {
                hostLog.warn("Running a redundant (k-safe) cluster with network " +
                        "partition detection disabled is not recommended for production use.");
                // we decided not to include the stronger language below for the 3.0 version (ENG-4215)
                //hostLog.warn("With partition detection disabled, data may be lost or " +
                //      "corrupted by certain classes of network failures.");
            }

            assert (m_clientInterface != null);
            m_clientInterface.initializeSnapshotDaemon(m_messenger, m_globalServiceElector);

            // Start elastic join service
            try {
                if (m_config.m_isEnterprise && TheHashinator.getCurrentConfig().type == HashinatorType.ELASTIC) {
                    Class<?> elasticServiceClass = MiscUtils.loadProClass("org.voltdb.join.ElasticJoinCoordinator",
                                                                          "Elastic join", false);

                    if (elasticServiceClass == null) {
                        VoltDB.crashLocalVoltDB("Missing the ElasticJoinCoordinator class file in the enterprise " +
                                                "edition", false, null);
                    }

                    Constructor<?> constructor =
                        elasticServiceClass.getConstructor(HostMessenger.class,
                                                           ClientInterface.class,
                                                           Cartographer.class,
                                                           BalancePartitionsStatistics.class,
                                                           String.class,
                                                           int.class,
                                                           Supplier.class);
                    m_elasticJoinService =
                        (ElasticJoinService) constructor.newInstance(
                                m_messenger,
                                m_clientInterface,
                                m_cartographer,
                                rebalanceStats,
                                VoltDB.instance().getCommandLogSnapshotPath(),
                                m_catalogContext.getDeployment().getCluster().getKfactor(),
                                m_clusterSettings);
                    m_elasticJoinService.updateConfig(m_catalogContext);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to instantiate elastic join service", false, e);
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
        }
    }

    @Override
    public void hostsFailed(Set<Integer> failedHosts)
    {
        final ScheduledExecutorService es = getSES(true);
        if (es != null && !es.isShutdown()) {
            es.submit(new Runnable() {
                @Override
                public void run()
                {
                    // First check to make sure that the cluster still is viable before
                    // before allowing the fault log to be updated by the notifications
                    // generated below.
                    Set<Integer> hostsOnRing = new HashSet<>();
                    if (!m_leaderAppointer.isClusterKSafe(hostsOnRing)) {
                        VoltDB.crashLocalVoltDB("Some partitions have no replicas.  Cluster has become unviable.",
                                false, null);
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
                    for (int hostId : failedHosts) {
                        CoreZK.removeRejoinNodeIndicatorForHost(m_messenger.getZK(), hostId);
                    }
                }
            });
        }
    }

    class DailyLogTask implements Runnable {
        @Override
        public void run() {
            m_myHostId = m_messenger.getHostId();
            hostLog.info(String.format("Host id of this node is: %d", m_myHostId));
            hostLog.info("URL of deployment info: " + m_config.m_pathToDeployment);
            hostLog.info("Cluster uptime: " + MiscUtils.formatUptime(getClusterUptime()));
            logDebuggingInfo(m_config.m_adminPort, m_config.m_httpPort, m_httpPortExtraLogMessage, m_jsonEnabled);
            // log system setting information
            logSystemSettingFromCatalogContext();

            scheduleDailyLoggingWorkInNextCheckTime();
        }
    }

    /**
     * Get the next check time for a private member in log4j library, which is not a reliable idea.
     * It adds 30 seconds for the initial delay and uses a periodical thread to schedule the daily logging work
     * with this delay.
     * @return
     */
    void scheduleDailyLoggingWorkInNextCheckTime() {
        DailyRollingFileAppender dailyAppender = null;
        Enumeration<?> appenders = Logger.getRootLogger().getAllAppenders();
        while (appenders.hasMoreElements()) {
            Appender appender = (Appender) appenders.nextElement();
            if (appender instanceof DailyRollingFileAppender){
                dailyAppender = (DailyRollingFileAppender) appender;
            }
        }
        final DailyRollingFileAppender dailyRollingFileAppender = dailyAppender;

        Field field = null;
        if (dailyRollingFileAppender != null) {
            try {
                field = dailyRollingFileAppender.getClass().getDeclaredField("nextCheck");
                field.setAccessible(true);
            } catch (NoSuchFieldException e) {
                hostLog.error("Failed to set daily system info logging: " + e.getMessage());
            }
        }
        final Field nextCheckField = field;
        long nextCheck = System.currentTimeMillis();
        // the next part may throw exception, current time is the default value
        if (dailyRollingFileAppender != null && nextCheckField != null) {
            try {
                nextCheck = nextCheckField.getLong(dailyRollingFileAppender);
                scheduleWork(new DailyLogTask(),
                        nextCheck - System.currentTimeMillis() + 30 * 1000, 0, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                hostLog.error("Failed to set daily system info logging: " + e.getMessage());
            }
        }
    }

    class StartActionWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            if (m_mode == OperationMode.SHUTTINGDOWN) return;
            m_es.submit(new Runnable() {
                @Override
                public void run() {
                    validateStartAction();
                }
            });
        }
    }

    private void validateStartAction() {
        try {
            ZooKeeper zk = m_messenger.getZK();
            boolean initCompleted = zk.exists(VoltZK.init_completed, false) != null;
            List<String> children = zk.getChildren(VoltZK.start_action, new StartActionWatcher(), null);
            if (!children.isEmpty()) {
                for (String child : children) {
                    byte[] data = zk.getData(VoltZK.start_action + "/" + child, false, null);
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
        } catch (KeeperException e) {
            hostLog.error("Failed to validate the start actions", e);
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Interrupted during start action validation:" + e.getMessage(), true, e);
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

                stringer.key("workingDir").value(System.getProperty("user.dir"));
                stringer.key("pid").value(CLibrary.getpid());

                stringer.key("log4jDst").array();
                Enumeration<?> appenders = Logger.getRootLogger().getAllAppenders();
                while (appenders.hasMoreElements()) {
                    Appender appender = (Appender) appenders.nextElement();
                    if (appender instanceof FileAppender){
                        stringer.object();
                        stringer.key("path").value(new File(((FileAppender) appender).getFile()).getCanonicalPath());
                        if (appender instanceof DailyRollingFileAppender) {
                            stringer.key("format").value(((DailyRollingFileAppender)appender).getDatePattern());
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
                            stringer.key("path").value(new File(((FileAppender) appender).getFile()).getCanonicalPath());
                            if (appender instanceof DailyRollingFileAppender) {
                                stringer.key("format").value(((DailyRollingFileAppender)appender).getDatePattern());
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
                Throwables.propagate(e);
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

        private void logCatalogAndDeployment() {

            File configInfoDir = getConfigDirectory();
            configInfoDir.mkdirs();

            try {
                m_catalogContext.writeCatalogJarToFile(configInfoDir.getPath(), "catalog.jar");
            } catch (IOException e) {
                hostLog.error("Failed to log catalog: " + e.getMessage(), e);
                e.printStackTrace();
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
            logCatalogAndDeployment();
        }
    }

    // Get topology information.  If rejoining, get it directly from
    // ZK.  Otherwise, try to do the write/read race to ZK on startup.
    private JSONObject getTopology(StartAction startAction, Map<Integer, String> hostGroups,
                                   JoinCoordinator joinCoordinator)
    {
        JSONObject topo = null;
        if (startAction == StartAction.JOIN) {
            assert(joinCoordinator != null);
            topo = joinCoordinator.getTopology();
        }
        else if (!startAction.doesRejoin()) {
            int sitesperhost = m_catalogContext.getDeployment().getCluster().getSitesperhost();
            int hostcount = m_clusterSettings.get().hostcount();
            int kfactor = m_catalogContext.getDeployment().getCluster().getKfactor();
            ClusterConfig clusterConfig = new ClusterConfig(hostcount, sitesperhost, kfactor);
            if (!clusterConfig.validate()) {
                VoltDB.crashLocalVoltDB(clusterConfig.getErrorMsg(), false, null);
            }
            topo = registerClusterConfig(clusterConfig, hostGroups);
        }
        else {
            Stat stat = new Stat();
            try {
                topo =
                    new JSONObject(new String(m_messenger.getZK().getData(VoltZK.topology, false, stat), "UTF-8"));
            }
            catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to get topology from ZK", true, e);
            }
        }
        return topo;
    }

    private TreeMap<Integer, Initiator> createIv2Initiators(Collection<Integer> partitions,
                                                StartAction startAction,
                                                List<Integer> m_partitionsToSitesAtStartupForExportInit)
    {
        TreeMap<Integer, Initiator> initiators = new TreeMap<>();
        for (Integer partition : partitions)
        {
            Initiator initiator = new SpInitiator(m_messenger, partition, getStatsAgent(),
                    m_snapshotCompletionMonitor, startAction);
            initiators.put(partition, initiator);
            m_partitionsToSitesAtStartupForExportInit.add(partition);
        }
        return initiators;
    }

    private JSONObject registerClusterConfig(ClusterConfig config, Map<Integer, String> hostGroups)
    {
        // First, race to write the topology to ZK using Highlander rules
        // (In the end, there can be only one)
        JSONObject topo = null;
        try
        {
            final Set<Integer> liveHostIds = m_messenger.getLiveHostIds();
            Preconditions.checkArgument(hostGroups.keySet().equals(liveHostIds));
            topo = config.getTopology(hostGroups);
            byte[] payload = topo.toString(4).getBytes("UTF-8");
            m_messenger.getZK().create(VoltZK.topology, payload,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
        }
        catch (KeeperException.NodeExistsException nee)
        {
            // It's fine if we didn't win, we'll pick up the topology below
        }
        catch (Exception e)
        {
            VoltDB.crashLocalVoltDB("Unable to write topology to ZK, dying",
                    true, e);
        }

        // Then, have everyone read the topology data back from ZK
        try
        {
            byte[] data = m_messenger.getZK().getData(VoltZK.topology, false, null);
            topo = new JSONObject(new String(data, "UTF-8"));
        }
        catch (Exception e)
        {
            VoltDB.crashLocalVoltDB("Unable to read topology from ZK, dying",
                    true, e);
        }
        return topo;
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

        GCInspector.instance.start(m_periodicPriorityWorkThread);
    }

    private void startResourceUsageMonitor() {
        if (resMonitorWork != null) {
            resMonitorWork.cancel(false);
            try {
                resMonitorWork.get();
            } catch(Exception e) { } // Ignore exceptions because we don't really care about the result here.
            m_periodicWorks.remove(resMonitorWork);
        }
        ResourceUsageMonitor resMonitor  = new ResourceUsageMonitor(m_catalogContext.getDeployment().getSystemsettings());
        resMonitor.logResourceLimitConfigurationInfo();
        if (resMonitor.hasResourceLimitsConfigured()) {
            resMonitorWork = scheduleWork(resMonitor, resMonitor.getResourceCheckInterval(), resMonitor.getResourceCheckInterval(), TimeUnit.SECONDS);
            m_periodicWorks.add(resMonitorWork);
        }
    }

    /**
     * Takes the deployment file given at initialization and the voltdb root given as
     * a command line options, and it performs the following tasks:
     * <p><ul>
     * <li>creates if necessary the voltdbroot directory
     * <li>fail if voltdbroot is already configured and populated with database artifacts
     * <li>creates command log, dr, snaphot, and export directories
     * <li>creates the config directory under voltdbroot
     * <li>moves the deployment file under the config directory
     * </ul>
     * @param config
     * @param dt a {@link DeploymentTypel}
     */
    private void stageDeploymemtFileForInitialize(Configuration config, DeploymentType dt) {

        String deprootFN = dt.getPaths().getVoltdbroot().getPath();
        File   deprootFH = new VoltFile(deprootFN);
        File   cnfrootFH = config.m_voltdbRoot;

        if (!cnfrootFH.exists() && !cnfrootFH.mkdirs()) {
            VoltDB.crashLocalVoltDB("Unable to create the voltdbroot directory in " + cnfrootFH, false, null);
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
        // create the remaining paths
        if (config.m_isEnterprise) {
            List<String> failed = m_paths.ensureDirectoriesExist();
            if (!failed.isEmpty()) {
                String msg = "Unable to access or create the following directories:\n    "
                        + Joiner.on("\n    ").join(failed);
                VoltDB.crashLocalVoltDB(msg);
                return;
            }
        }

        //In init/start mode we save adminmode to false always.
        dt.getAdminMode().setAdminstartup(false);
        //Now its safe to Save .paths
        m_paths.store();

         //Now that we are done with deployment configuration set all path null.
         dt.setPaths(null);

        // log message unconditionally indicating that the provided host-count and admin-mode settings in
        // deployment, if any, will be ignored
        consoleLog.info("When using the INIT command, some deployment file settings (hostcount, voltdbroot path, "
                + "and admin-mode) are ignored");
        hostLog.info("When using the INIT command, some deployment file settings (hostcount, voltdbroot path, "
                + "and admin-mode) are ignored");

        File depFH = getConfigLogDeployment(config);
        try (FileWriter fw = new FileWriter(depFH)) {
            fw.write(CatalogUtil.getDeployment(dt, true /* pretty print indent */));
        } catch (IOException|RuntimeException e) {
            VoltDB.crashLocalVoltDB("Unable to marshal deployment configuration to " + depFH, false, e);
        }

        // Save cluster settings properties derived from the deployment file
        ClusterSettings.create(CatalogUtil.asClusterSettingsMap(dt)).store();
    }

    private void stageInitializedMarker(Configuration config) {
        File depFH = new VoltFile(config.m_voltdbRoot, VoltDB.INITIALIZED_MARKER);
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

    int readDeploymentAndCreateStarterCatalogContext(VoltDB.Configuration config) {
        /*
         * Debate with the cluster what the deployment file should be
         */
        try {
            ZooKeeper zk = m_messenger.getZK();
            byte deploymentBytes[] = null;

            try {
                deploymentBytes = org.voltcore.utils.CoreUtils.urlToBytes(m_config.m_pathToDeployment);
            } catch (Exception ex) {
                //Let us get bytes from ZK
            }
            DeploymentType deployment = null;
            try {
                if (deploymentBytes != null) {
                    CatalogUtil.writeCatalogToZK(zk,
                            // Fill in innocuous values for non-deployment stuff
                            0,
                            0L,
                            0L,
                            new byte[] {},  // spin loop in Inits.LoadCatalog.run() needs
                                            // this to be of zero length until we have a real catalog.
                            null,
                            deploymentBytes);
                    hostLog.info("URL of deployment: " + m_config.m_pathToDeployment);
                } else {
                    CatalogAndIds catalogStuff = CatalogUtil.getCatalogFromZK(zk);
                    deploymentBytes = catalogStuff.deploymentBytes;
                }
            } catch (KeeperException.NodeExistsException e) {
                CatalogAndIds catalogStuff = CatalogUtil.getCatalogFromZK(zk);
                byte[] deploymentBytesTemp = catalogStuff.deploymentBytes;
                if (deploymentBytesTemp != null) {
                    //Check hash if its a supplied deployment on command line.
                    //We will ignore the supplied or default deployment anyways.
                    if (deploymentBytes != null && !m_config.m_deploymentDefault) {
                        byte[] deploymentHashHere =
                            CatalogUtil.makeDeploymentHash(deploymentBytes);
                        if (!(Arrays.equals(deploymentHashHere, catalogStuff.getDeploymentHash())))
                        {
                            hostLog.warn("The locally provided deployment configuration did not " +
                                    " match the configuration information found in the cluster.");
                        } else {
                            hostLog.info("Deployment configuration pulled from other cluster node.");
                        }
                    }
                    //Use remote deployment obtained.
                    deploymentBytes = deploymentBytesTemp;
                } else {
                    hostLog.error("Deployment file could not be loaded locally or remotely, "
                            + "local supplied path: " + m_config.m_pathToDeployment);
                    deploymentBytes = null;
                }
            } catch(KeeperException.NoNodeException e) {
                // no deploymentBytes case is handled below. So just log this error.
                if (hostLog.isDebugEnabled()) {
                    hostLog.debug("Error trying to get deployment bytes from cluster", e);
                }
            }
            if (deploymentBytes == null) {
                hostLog.error("Deployment information could not be obtained from cluster node or locally");
                VoltDB.crashLocalVoltDB("No such deployment file: "
                        + m_config.m_pathToDeployment, false, null);
            }

            if (deployment == null) {
                deployment = CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));
            }

            // wasn't a valid xml deployment file
            if (deployment == null) {
                hostLog.error("Not a valid XML deployment file at URL: " + m_config.m_pathToDeployment);
                VoltDB.crashLocalVoltDB("Not a valid XML deployment file at URL: "
                        + m_config.m_pathToDeployment, false, null);
            }
            /*
             * Check for invalid deployment file settings (enterprise-only) in the community edition.
             * Trick here is to print out all applicable problems and then stop, rather than stopping
             * after the first one is found.
             */
            if (!m_config.m_isEnterprise) {
                boolean shutdownDeployment = false;
                boolean shutdownAction = false;

                // check license features for community version
                if ((deployment.getCluster() != null) && (deployment.getCluster().getKfactor() > 0)) {
                    consoleLog.error("K-Safety is not supported " +
                            "in the community edition of VoltDB.");
                    shutdownDeployment = true;
                }
                if ((deployment.getSnapshot() != null) && (deployment.getSnapshot().isEnabled())) {
                    consoleLog.error("Snapshots are not supported " +
                            "in the community edition of VoltDB.");
                    shutdownDeployment = true;
                }
                if ((deployment.getCommandlog() != null) && (deployment.getCommandlog().isEnabled())) {
                    consoleLog.error("Command logging is not supported " +
                            "in the community edition of VoltDB.");
                    shutdownDeployment = true;
                }
                if ((deployment.getExport() != null) && Boolean.TRUE.equals(deployment.getExport().isEnabled())) {
                    consoleLog.error("Export is not supported " +
                            "in the community edition of VoltDB.");
                    shutdownDeployment = true;
                }
                // check the start action for the community edition
                if (m_config.m_startAction != StartAction.CREATE) {
                    consoleLog.error("Start action \"" + m_config.m_startAction.getClass().getSimpleName() +
                            "\" is not supported in the community edition of VoltDB.");
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

                    VoltDB.crashLocalVoltDB(msg, false, null);
                }
            }

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

                // check for user using deprecated settings
                PartitionDetectionType.Snapshot snapshot = pt.getSnapshot();
                if (snapshot != null) {
                    String prefix = snapshot.getPrefix();
                    if ((prefix != null) && ("partition_detection".equalsIgnoreCase(prefix) == false)) {
                        hostLog.warn(String.format("Partition Detection snapshots are "
                                + "no longer supported. Prefix value \"%s\" will be ignored.", prefix));
                    }
                }
            }

            // get any consistency settings into config
            ConsistencyType consistencyType = deployment.getConsistency();
            if (consistencyType != null) {
                m_config.m_consistencyReadLevel = Consistency.ReadLevel.fromReadLevelType(consistencyType.getReadlevel());
            }

            final String elasticSetting = deployment.getCluster().getElastic().trim().toUpperCase();
            if (elasticSetting.equals("ENABLED")) {
                TheHashinator.setConfiguredHashinatorType(HashinatorType.ELASTIC);
            } else if (!elasticSetting.equals("DISABLED")) {
                VoltDB.crashLocalVoltDB("Error in deployment file,  elastic attribute of " +
                                        "cluster element must be " +
                                        "'enabled' or 'disabled' but was '" + elasticSetting + "'", false, null);
            }
            else {
                TheHashinator.setConfiguredHashinatorType(HashinatorType.LEGACY);
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
                    hostLog.info("Snapshot priority set to " + sysType.getSnapshot().getPriority() + " [0 - 10]");
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

            // create a dummy catalog to load deployment info into
            Catalog catalog = new Catalog();
            // Need these in the dummy catalog
            Cluster cluster = catalog.getClusters().add("cluster");
            @SuppressWarnings("unused")
            Database db = cluster.getDatabases().add("database");

            String result = CatalogUtil.compileDeployment(catalog, deployment, true);
            if (result != null) {
                // Any other non-enterprise deployment errors will be caught and handled here
                // (such as <= 0 host count)
                VoltDB.crashLocalVoltDB(result);
            }

            m_catalogContext = new CatalogContext(
                            TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId(), //txnid
                            0, //timestamp
                            catalog,
                            m_clusterSettings,
                            new byte[] {},
                            null,
                            deploymentBytes,
                            0);

            return m_clusterSettings.get().hostcount();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public void loadLegacyPathProperties(DeploymentType deployment) throws IOException {
        //Load deployment paths now if Legacy so that we access through the interface all the time.
        if (isRunningWithOldVerbs() && m_paths == null) {
            m_paths = PathSettings.create(CatalogUtil.asPathSettingsMap(deployment));
            List<String> failed = m_paths.ensureDirectoriesExist();
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
                VoltDB.crashLocalVoltDB("No such deployment file: "
                        + config.m_pathToDeployment, false, null);
            }
            DeploymentType deployment =
                CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));
            // wasn't a valid xml deployment file
            if (deployment == null) {
                hostLog.error("Not a valid XML deployment file at URL: " + config.m_pathToDeployment);
                VoltDB.crashLocalVoltDB("Not a valid XML deployment file at URL: "
                        + config.m_pathToDeployment, false, null);
                return new ReadDeploymentResults(deploymentBytes, deployment);
            }
            PathSettings pathSettings = null;
            // adjust deployment host count when the cluster members are given by mesh configuration
            // providers
            switch(config.m_startAction) {
            case PROBE:
                // once a voltdbroot is inited, the path properties contain the true path values
                Settings.initialize(config.m_voltdbRoot);
                pathSettings = PathSettings.create();
                File pathSettingsFH = new File(getConfigDirectory(config), "path.properties");
                consoleLog.info("Loaded path settings from " + pathSettingsFH.getPath());
                hostLog.info("Loaded path settings from " + pathSettingsFH.getPath());
                break;
            case INITIALIZE:
                Settings.initialize(config.m_voltdbRoot);
                // voltdbroot value from config overrides voltdbroot value in the deployment
                // file
                pathSettings = PathSettings.create(
                        config.asPathSettingsMap(),
                        CatalogUtil.asPathSettingsMap(deployment));
                break;
            default:
                pathSettings = PathSettings.create(CatalogUtil.asPathSettingsMap(deployment));
                Settings.initialize(pathSettings.getVoltDBRoot());
                config.m_voltdbRoot = pathSettings.getVoltDBRoot();
                break;
            }
            m_paths = pathSettings;

            if (config.m_startAction == StartAction.PROBE) {
                // once initialized the path properties contain the true path values
                if (config.m_hostCount == VoltDB.UNDEFINED) {
                    config.m_hostCount = 1;
                }
            } else {
                config.m_hostCount = deployment.getCluster().getHostcount();
            }
            /*
             * Check for invalid deployment file settings (enterprise-only) in the community edition.
             * Trick here is to print out all applicable problems and then stop, rather than stopping
             * after the first one is found.
             */
            if (!config.m_isEnterprise) {
                boolean shutdownDeployment = false;
                boolean shutdownAction = false;

                // check license features for community version
                if ((deployment.getCluster() != null) && (deployment.getCluster().getKfactor() > 0)) {
                    consoleLog.error("K-Safety is not supported " +
                            "in the community edition of VoltDB.");
                    shutdownDeployment = true;
                }
                if ((deployment.getSnapshot() != null) && (deployment.getSnapshot().isEnabled())) {
                    consoleLog.error("Snapshots are not supported " +
                            "in the community edition of VoltDB.");
                    shutdownDeployment = true;
                }
                if ((deployment.getCommandlog() != null) && (deployment.getCommandlog().isEnabled())) {
                    consoleLog.error("Command logging is not supported " +
                            "in the community edition of VoltDB.");
                    shutdownDeployment = true;
                }
                if ((deployment.getExport() != null) && Boolean.TRUE.equals(deployment.getExport().isEnabled())) {
                    consoleLog.error("Export is not supported " +
                            "in the community edition of VoltDB.");
                    shutdownDeployment = true;
                }
                // check the start action for the community edition
                if (m_config.m_startAction != StartAction.CREATE) {
                    consoleLog.error("Start action \"" + m_config.m_startAction.getClass().getSimpleName() +
                            "\" is not supported in the community edition of VoltDB.");
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

                    VoltDB.crashLocalVoltDB(msg, false, null);
                }
            }
            return new ReadDeploymentResults(deploymentBytes, deployment);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    void collectLocalNetworkMetadata() {
        boolean threw = false;
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
                LinkedList<NetworkInterface> interfaces = new LinkedList<>();
                try {
                    Enumeration<NetworkInterface> intfEnum = NetworkInterface.getNetworkInterfaces();
                    while (intfEnum.hasMoreElements()) {
                        NetworkInterface intf = intfEnum.nextElement();
                        if (intf.isLoopback() || !intf.isUp()) {
                            continue;
                        }
                        interfaces.offer(intf);
                    }
                } catch (SocketException e) {
                    throw new RuntimeException(e);
                }

                if (interfaces.isEmpty()) {
                    stringer.value("localhost");
                } else {

                    boolean addedIp = false;
                    while (!interfaces.isEmpty()) {
                        NetworkInterface intf = interfaces.poll();
                        Enumeration<InetAddress> inetAddrs = intf.getInetAddresses();
                        Inet6Address inet6addr = null;
                        Inet4Address inet4addr = null;
                        while (inetAddrs.hasMoreElements()) {
                            InetAddress addr = inetAddrs.nextElement();
                            if (addr instanceof Inet6Address) {
                                inet6addr = (Inet6Address)addr;
                                if (inet6addr.isLinkLocalAddress()) {
                                    inet6addr = null;
                                }
                            } else if (addr instanceof Inet4Address) {
                                inet4addr = (Inet4Address)addr;
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
                        stringer.value("localhost");
                    }
                }
            } else {
                stringer.value(m_config.m_externalInterface);
            }
        } catch (Exception e) {
            threw = true;
            hostLog.warn("Error while collecting data about local network interfaces", e);
        }
        try {
            if (threw) {
                stringer = new JSONStringer();
                stringer.object();
                stringer.key("interfaces").array();
                stringer.value("localhost");
                stringer.endArray();
            } else {
                stringer.endArray();
            }
            stringer.key("clientPort").value(m_config.m_port);
            stringer.key("clientInterface").value(m_config.m_clientInterface);
            stringer.key("adminPort").value(m_config.m_adminPort);
            stringer.key("adminInterface").value(m_config.m_adminInterface);
            stringer.key("httpPort").value(m_config.m_httpPort);
            stringer.key("httpInterface").value(m_config.m_httpPortInterface);
            stringer.key("internalPort").value(m_config.m_internalPort);
            stringer.key("internalInterface").value(m_config.m_internalInterface);
            String[] zkInterface = m_config.m_zkInterface.split(":");
            stringer.key("zkPort").value(zkInterface[1]);
            stringer.key("zkInterface").value(zkInterface[0]);
            stringer.key("drPort").value(VoltDB.getReplicationPort(m_catalogContext.cluster.getDrproducerport()));
            stringer.key("drInterface").value(VoltDB.getDefaultReplicationInterface());
            stringer.key("publicInterface").value(m_config.m_publicInterface);
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
                return m_clusterSettings.get().hostcount();
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
                .nodeStateSupplier(m_statusTracker.getNodeStateSupplier())
                .addAllowed(m_config.m_enableAdd)
                .safeMode(m_config.m_safeMode)
                .build();

        HostAndPort hostAndPort = criteria.getLeader();
        String hostname = hostAndPort.getHostText();
        int port = hostAndPort.getPort();

        org.voltcore.messaging.HostMessenger.Config hmconfig;

        hmconfig = new org.voltcore.messaging.HostMessenger.Config(hostname, port);
        if (m_config.m_placementGroup != null) {
            hmconfig.group = m_config.m_placementGroup;
        }
        hmconfig.internalPort = m_config.m_internalPort;
        hmconfig.internalInterface = m_config.m_internalInterface;
        hmconfig.zkInterface = m_config.m_zkInterface;
        hmconfig.deadHostTimeout = m_config.m_deadHostTimeoutMS;
        hmconfig.factory = new VoltDbMessageFactory();
        hmconfig.coreBindIds = m_config.m_networkCoreBindings;
        hmconfig.acceptor = criteria;

        m_messenger = new org.voltcore.messaging.HostMessenger(hmconfig, this);

        hostLog.info(String.format("Beginning inter-node communication on port %d.", m_config.m_internalPort));

        try {
            m_messenger.start();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }

        VoltZK.createPersistentZKNodes(m_messenger.getZK());

        // Use the host messenger's hostId.
        m_myHostId = m_messenger.getHostId();
        hostLog.info(String.format("Host id of this node is: %d", m_myHostId));
        consoleLog.info(String.format("Host id of this node is: %d", m_myHostId));

        MeshProber.Determination determination = criteria.waitForDetermination();

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
                    "Please check your command line and start action and try again.", false, null);
        }
        // load or store settings form/to zookeeper
        if (determination.startAction.doesJoin()) {
            m_clusterSettings.load(m_messenger.getZK());
            m_clusterSettings.get().store();
        } else if (m_myHostId == 0) {
            m_clusterSettings.store(m_messenger.getZK());
        }
        ClusterConfig config = new ClusterConfig(
                m_clusterSettings.get().hostcount(),
                clusterType.getSitesperhost(),
                clusterType.getKfactor()
                );

        if (!config.validate()) {
            VoltDB.crashLocalVoltDB("Cluster parameters failed validation: " + config.getErrorMsg());;
        }
        m_clusterCreateTime = m_messenger.getInstanceId().getTimestamp();
        return determination;
    }

    void logDebuggingInfo(int adminPort, int httpPort, String httpPortExtraLogMessage, boolean jsonEnabled) {
        String startAction = m_config.m_startAction.toString();
        String startActionLog = "Database start action is " + (startAction.substring(0, 1).toUpperCase() +
                startAction.substring(1).toLowerCase()) + ".";
        if (!m_rejoining) {
            hostLog.info(startActionLog);
        }
        hostLog.info("PID of this Volt process is " + CLibrary.getpid());

        // print out awesome network stuff
        hostLog.info(String.format("Listening for native wire protocol clients on port %d.", m_config.m_port));
        hostLog.info(String.format("Listening for admin wire protocol clients on port %d.", adminPort));

        if (m_startMode == OperationMode.PAUSED) {
            hostLog.info(String.format("Started in admin mode. Clients on port %d will be rejected in admin mode.", m_config.m_port));
        }

        if (m_config.m_replicationRole == ReplicationRole.REPLICA) {
            consoleLog.info("Started as " + m_config.m_replicationRole.toString().toLowerCase() + " cluster. " +
                             "Clients can only call read-only procedures.");
        }
        if (httpPortExtraLogMessage != null) {
            hostLog.info(httpPortExtraLogMessage);
        }
        if (httpPort != -1) {
            hostLog.info(String.format("Local machine HTTP monitoring is listening on port %d.", httpPort));
        }
        else {
            hostLog.info(String.format("Local machine HTTP monitoring is disabled."));
        }
        if (jsonEnabled) {
            hostLog.info(String.format("Json API over HTTP enabled at path /api/1.0/, listening on port %d.", httpPort));
        }
        else {
            hostLog.info("Json API disabled.");
        }

        // java heap size
        long javamaxheapmem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        javamaxheapmem /= (1024 * 1024);
        hostLog.info(String.format("Maximum usable Java heap set to %d mb.", javamaxheapmem));

        // Computed minimum heap requirement
        long minRqt = computeMinimumHeapRqt(MiscUtils.isPro(), m_catalogContext.tables.size(),
                (m_iv2Initiators.size() - 1), m_configuredReplicationFactor);
        hostLog.info("Minimum required Java heap for catalog and server config is " + minRqt + " MB.");

        SortedMap<String, String> dbgMap = m_catalogContext.getDebuggingInfoFromCatalog();
        for (String line : dbgMap.values()) {
            hostLog.info(line);
        }

        // print out a bunch of useful system info
        PlatformProperties pp = PlatformProperties.getPlatformProperties();
        String[] lines = pp.toLogLines(getVersionString()).split("\n");
        for (String line : lines) {
            hostLog.info(line.trim());
        }
        hostLog.info("The internal DR cluster timestamp is " +
                    new Date(m_clusterCreateTime).toString() + ".");

        final ZooKeeper zk = m_messenger.getZK();
        ZKUtil.ByteArrayCallback operationModeFuture = new ZKUtil.ByteArrayCallback();
        /*
         * Publish our cluster metadata, and then retrieve the metadata
         * for the rest of the cluster
         */
        try {
            zk.create(
                    VoltZK.cluster_metadata + "/" + m_messenger.getHostId(),
                    getLocalMetadata().getBytes("UTF-8"),
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL,
                    new ZKUtil.StringCallback(),
                    null);
            zk.getData(VoltZK.operationMode, false, operationModeFuture, null);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error creating \"/cluster_metadata\" node in ZK", true, e);
        }

        Map<Integer, String> clusterMetadata = new HashMap<>(0);
        /*
         * Spin and attempt to retrieve cluster metadata for all nodes in the cluster.
         */
        Set<Integer> metadataToRetrieve = new HashSet<>(m_messenger.getLiveHostIds());
        metadataToRetrieve.remove(m_messenger.getHostId());
        while (!metadataToRetrieve.isEmpty()) {
            Map<Integer, ZKUtil.ByteArrayCallback> callbacks = new HashMap<>();
            for (Integer hostId : metadataToRetrieve) {
                ZKUtil.ByteArrayCallback cb = new ZKUtil.ByteArrayCallback();
                zk.getData(VoltZK.cluster_metadata + "/" + hostId, false, cb, null);
                callbacks.put(hostId, cb);
            }

            for (Map.Entry<Integer, ZKUtil.ByteArrayCallback> entry : callbacks.entrySet()) {
                try {
                    ZKUtil.ByteArrayCallback cb = entry.getValue();
                    Integer hostId = entry.getKey();
                    clusterMetadata.put(hostId, new String(cb.getData(), "UTF-8"));
                    metadataToRetrieve.remove(hostId);
                } catch (KeeperException.NoNodeException e) {}
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Error retrieving cluster metadata", true, e);
                }
            }

        }

        // print out cluster membership
        hostLog.info("About to list cluster interfaces for all nodes with format [ip1 ip2 ... ipN] client-port,admin-port,http-port");
        for (int hostId : m_messenger.getLiveHostIds()) {
            if (hostId == m_messenger.getHostId()) {
                hostLog.info(
                        String.format(
                                "  Host id: %d with interfaces: %s [SELF]",
                                hostId,
                                MiscUtils.formatHostMetadataFromJSON(getLocalMetadata())));
            }
            else {
                String hostMeta = clusterMetadata.get(hostId);
                hostLog.info(
                        String.format(
                                "  Host id: %d with interfaces: %s [PEER]",
                                hostId,
                                MiscUtils.formatHostMetadataFromJSON(hostMeta)));
            }
        }

        try {
            if (operationModeFuture.getData() != null) {
                String operationModeStr = new String(operationModeFuture.getData(), "UTF-8");
                m_startMode = OperationMode.valueOf(operationModeStr);
            }
        } catch (KeeperException.NoNodeException e) {}
        catch (Exception e) {
            throw new RuntimeException(e);
        }
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
                logger.l7dlog(Level.ERROR, LogKeys.org_voltdb_VoltDB_FailedToRetrieveBuildString.name(), null);
            }
            return new String[] { m_defaultVersionString, "VoltDB" };
        }
    }

    @Override
    public void readBuildInfo(String editionTag) {
        String buildInfo[] = extractBuildInfo(hostLog);
        m_versionString = buildInfo[0];
        m_buildString = buildInfo[1];
        String buildString = m_buildString;
        if (m_buildString.contains("_"))
            buildString = m_buildString.split("_", 2)[1];
        consoleLog.info(String.format("Build: %s %s %s", m_versionString, buildString, editionTag));
    }

    void logSystemSettingFromCatalogContext() {
        if (m_catalogContext == null) {
            return;
        }
        Deployment deploy = m_catalogContext.cluster.getDeployment().get("deployment");
        Systemsettings sysSettings = deploy.getSystemsettings().get("systemsettings");

        if (sysSettings == null) {
            return;
        }

        hostLog.info("Elastic duration set to " + sysSettings.getElasticduration() + " milliseconds");
        hostLog.info("Elastic throughput set to " + sysSettings.getElasticthroughput() + " mb/s");
        hostLog.info("Max temptable size set to " + sysSettings.getTemptablemaxsize() + " mb");
        hostLog.info("Snapshot priority set to " + sysSettings.getSnapshotpriority() + " [0 - 10]");

        if (sysSettings.getQuerytimeout() > 0) {
            hostLog.info("Query timeout set to " + sysSettings.getQuerytimeout() + " milliseconds");
            m_config.m_queryTimeout = sysSettings.getQuerytimeout();
        }
        else if (sysSettings.getQuerytimeout() == 0) {
            hostLog.info("Query timeout set to unlimited");
            m_config.m_queryTimeout = 0;
        }

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
            onRestoreCompletion(Long.MIN_VALUE, m_iv2InitiatorStartingTxnIds);
        }

        // Start the rejoin coordinator
        if (m_joinCoordinator != null) {
            try {
                m_statusTracker.setNodeState(NodeState.REJOINING);
                if (!m_joinCoordinator.startJoin(m_catalogContext.database)) {
                    VoltDB.crashLocalVoltDB("Failed to join the cluster", true, null);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to join the cluster", true, e);
            }
        }

        m_isRunning = true;
    }

    /**
     * Try to shut everything down so they system is ready to call
     * initialize again.
     * @param mainSiteThread The thread that m_inititalized the VoltDB or
     * null if called from that thread.
     */
    @Override
    public boolean shutdown(Thread mainSiteThread) throws InterruptedException {
        synchronized(m_startAndStopLock) {
            boolean did_it = false;
            if (m_mode != OperationMode.SHUTTINGDOWN) {
                did_it = true;
                m_mode = OperationMode.SHUTTINGDOWN;

                /*
                 * Various scheduled tasks get crashy in unit tests if they happen to run
                 * while other stuff is being shut down
                 */
                for (ScheduledFuture<?> sc : m_periodicWorks) {
                    sc.cancel(false);
                    try {
                        sc.get();
                    } catch (Throwable t) {}
                }

                //Shutdown import processors.
                ImportManager.instance().shutdown();

                m_periodicWorks.clear();
                m_snapshotCompletionMonitor.shutdown();
                m_periodicWorkThread.shutdown();
                m_periodicWorkThread.awaitTermination(356, TimeUnit.DAYS);
                m_periodicPriorityWorkThread.shutdown();
                m_periodicPriorityWorkThread.awaitTermination(356, TimeUnit.DAYS);

                if (m_elasticJoinService != null) {
                    m_elasticJoinService.shutdown();
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
                if (m_adminListener != null)
                    m_adminListener.stop();

                // shut down the client interface
                if (m_clientInterface != null) {
                    m_clientInterface.shutdown();
                    m_clientInterface = null;
                }

                // tell the iv2 sites to stop their runloop
                if (m_iv2Initiators != null) {
                    for (Initiator init : m_iv2Initiators.values())
                        init.shutdown();
                }

                if (m_cartographer != null) {
                    m_cartographer.shutdown();
                }

                if (m_configLogger != null) {
                    m_configLogger.join();
                }

                // shut down Export and its connectors.
                ExportManager.instance().shutdown();

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

                //Also for test code that expects a fresh stats agent
                if (m_opsRegistrar != null) {
                    try {
                        m_opsRegistrar.shutdown();
                    }
                    finally {
                        m_opsRegistrar = null;
                    }
                }

                if (m_asyncCompilerAgent != null) {
                    m_asyncCompilerAgent.shutdown();
                    m_asyncCompilerAgent = null;
                }

                ExportManager.instance().shutdown();
                m_computationService.shutdown();
                m_computationService.awaitTermination(1, TimeUnit.DAYS);
                m_computationService = null;
                m_catalogContext = null;
                m_initiatorStats = null;
                m_latencyStats = null;
                m_latencyHistogramStats = null;

                AdHocCompilerCache.clearHashCache();
                org.voltdb.iv2.InitiatorMailbox.m_allInitiatorMailboxes.clear();

                PartitionDRGateway.m_partitionDRGateways = ImmutableMap.of();

                // probably unnecessary, but for tests it's nice because it
                // will do the memory checking and run finalizers
                System.gc();
                System.runFinalization();

                m_isRunning = false;
            }
            return did_it;
        }
    }

    /** Last transaction ID at which the logging config updated.
     * Also, use the intrinsic lock to safeguard access from multiple
     * execution site threads */
    private static Long lastLogUpdate_txnId = 0L;
    @Override
    synchronized public void logUpdate(String xmlConfig, long currentTxnId)
    {
        // another site already did this work.
        if (currentTxnId == lastLogUpdate_txnId) {
            return;
        }
        else if (currentTxnId < lastLogUpdate_txnId) {
            throw new RuntimeException(
                    "Trying to update logging config at transaction " + lastLogUpdate_txnId
                    + " with an older transaction: " + currentTxnId);
        }
        hostLog.info("Updating RealVoltDB logging config from txnid: " +
                lastLogUpdate_txnId + " to " + currentTxnId);
        lastLogUpdate_txnId = currentTxnId;
        VoltLogger.configure(xmlConfig);
    }

    /** Struct to associate a context with a counter of served sites */
    private static class ContextTracker {
        ContextTracker(CatalogContext context, CatalogSpecificPlanner csp) {
            m_dispensedSites = 1;
            m_context = context;
            m_csp = csp;
        }
        long m_dispensedSites;
        final CatalogContext m_context;
        final CatalogSpecificPlanner m_csp;
    }

    /** Associate transaction ids to contexts */
    private final HashMap<Long, ContextTracker>m_txnIdToContextTracker =
        new HashMap<>();

    @Override
    public Pair<CatalogContext, CatalogSpecificPlanner> catalogUpdate(
            String diffCommands,
            byte[] newCatalogBytes,
            byte[] catalogBytesHash,
            int expectedCatalogVersion,
            long currentTxnId,
            long currentTxnUniqueId,
            byte[] deploymentBytes,
            byte[] deploymentHash)
    {
        try {
            synchronized(m_catalogUpdateLock) {
                m_statusTracker.setNodeState(NodeState.UPDATING);
                // A site is catching up with catalog updates
                if (currentTxnId <= m_catalogContext.m_transactionId && !m_txnIdToContextTracker.isEmpty()) {
                    ContextTracker contextTracker = m_txnIdToContextTracker.get(currentTxnId);
                    // This 'dispensed' concept is a little crazy fragile. Maybe it would be better
                    // to keep a rolling N catalogs? Or perhaps to keep catalogs for N minutes? Open
                    // to opinions here.
                    contextTracker.m_dispensedSites++;
                    int ttlsites = VoltDB.instance().getSiteTrackerForSnapshot().getSitesForHost(m_messenger.getHostId()).size();
                    if (contextTracker.m_dispensedSites == ttlsites) {
                        m_txnIdToContextTracker.remove(currentTxnId);
                    }
                    return Pair.of( contextTracker.m_context, contextTracker.m_csp);
                }
                else if (m_catalogContext.catalogVersion != expectedCatalogVersion) {
                    hostLog.fatal("Failed catalog update." +
                            " expectedCatalogVersion: " + expectedCatalogVersion +
                            " currentTxnId: " + currentTxnId +
                            " currentTxnUniqueId: " + currentTxnUniqueId +
                            " m_catalogContext.catalogVersion " + m_catalogContext.catalogVersion);

                    throw new RuntimeException("Trying to update main catalog context with diff " +
                            "commands generated for an out-of date catalog. Expected catalog version: " +
                            expectedCatalogVersion + " does not match actual version: " + m_catalogContext.catalogVersion);
                }

                hostLog.info(String.format("Globally updating the current application catalog and deployment " +
                            "(new hashes %s, %s).",
                        Encoder.hexEncode(catalogBytesHash).substring(0, 10),
                        Encoder.hexEncode(deploymentHash).substring(0, 10)));

                // get old debugging info
                SortedMap<String, String> oldDbgMap = m_catalogContext.getDebuggingInfoFromCatalog();
                byte[] oldDeployHash = m_catalogContext.deploymentHash;

                // 0. A new catalog! Update the global context and the context tracker
                m_catalogContext =
                    m_catalogContext.update(
                            currentTxnId,
                            currentTxnUniqueId,
                            newCatalogBytes,
                            catalogBytesHash,
                            diffCommands,
                            true,
                            deploymentBytes);
                final CatalogSpecificPlanner csp = new CatalogSpecificPlanner( m_asyncCompilerAgent, m_catalogContext);
                m_txnIdToContextTracker.put(currentTxnId,
                        new ContextTracker(
                                m_catalogContext,
                                csp));

                // log the stuff that's changed in this new catalog update
                SortedMap<String, String> newDbgMap = m_catalogContext.getDebuggingInfoFromCatalog();
                for (Entry<String, String> e : newDbgMap.entrySet()) {
                    // skip log lines that are unchanged
                    if (oldDbgMap.containsKey(e.getKey()) && oldDbgMap.get(e.getKey()).equals(e.getValue())) {
                        continue;
                    }
                    hostLog.info(e.getValue());
                }

                //Construct the list of partitions and sites because it simply doesn't exist anymore
                SiteTracker siteTracker = VoltDB.instance().getSiteTrackerForSnapshot();
                List<Long> sites = siteTracker.getSitesForHost(m_messenger.getHostId());

                List<Integer> partitions = new ArrayList<>();
                for (Long site : sites) {
                    Integer partition = siteTracker.getPartitionForSite(site);
                    partitions.add(partition);
                }


                // 1. update the export manager.
                ExportManager.instance().updateCatalog(m_catalogContext, partitions);

                // 1.1 Update the elastic join throughput settings
                if (m_elasticJoinService != null) m_elasticJoinService.updateConfig(m_catalogContext);

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
                if (m_adminListener != null)
                {
                    m_adminListener.notifyOfCatalogUpdate();
                }

                // 4. Flush StatisticsAgent old catalog statistics.
                // Otherwise, the stats agent will hold all old catalogs
                // in memory.
                getStatsAgent().notifyOfCatalogUpdate();

                // 5. MPIs don't run fragments. Update them here. Do
                // this after flushing the stats -- this will re-register
                // the MPI statistics.
                if (m_MPI != null) {
                    m_MPI.updateCatalog(diffCommands, m_catalogContext, csp);
                }

                // Update catalog for import processor this should be just/stop start and updat partitions.
                ImportManager.instance().updateCatalog(m_catalogContext, m_messenger);

                // 6. Perform updates required by the DR subsystem

                // 6.1. Create the DR consumer if we've just enabled active-active.
                // Perform any actions that would have been taken during the ordinary
                // initialization path
                if (createDRConsumerIfNeeded()) {
                    for (int pid : m_cartographer.getPartitions()) {
                        // Notify the consumer of leaders because it was disabled before
                        ClientInterfaceRepairCallback callback = (ClientInterfaceRepairCallback) m_consumerDRGateway;
                        callback.repairCompleted(pid, m_cartographer.getHSIdForMaster(pid));
                    }
                    m_consumerDRGateway.initialize(false);
                }
                // 6.2. If we are a DR replica, we may care about a
                // deployment update
                if (m_consumerDRGateway != null) {
                    m_consumerDRGateway.updateCatalog(m_catalogContext);
                }
                // 6.3. If we are a DR master, update the DR table signature hash
                if (m_producerDRGateway != null) {
                    m_producerDRGateway.updateCatalog(m_catalogContext,
                            VoltDB.getReplicationPort(m_catalogContext.cluster.getDrproducerport()));
                }

                new ConfigLogging().logCatalogAndDeployment();

                // log system setting information if the deployment config has changed
                if (!Arrays.equals(oldDeployHash, m_catalogContext.deploymentHash)) {
                    logSystemSettingFromCatalogContext();
                }

                // restart resource usage monitoring task
                startResourceUsageMonitor();

                checkHeapSanity(MiscUtils.isPro(), m_catalogContext.tables.size(),
                        (m_iv2Initiators.size() - 1), m_configuredReplicationFactor);

                checkThreadsSanity();

                return Pair.of(m_catalogContext, csp);
            }
        } finally {
            //Set state back to UP
            m_statusTracker.setNodeState(NodeState.UP);
        }
    }

    @Override
    public Pair<CatalogContext, CatalogSpecificPlanner> settingsUpdate(
            ClusterSettings settings, final int expectedVersionId)
    {
        CatalogSpecificPlanner csp = new CatalogSpecificPlanner(m_asyncCompilerAgent, m_catalogContext);
        synchronized(m_catalogUpdateLock) {
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
                m_MPI.updateSettings(m_catalogContext, csp);
            }
            // good place to set deadhost timeout once we make it a config
        }
        return Pair.of(m_catalogContext, csp);
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
        Thread shutdownThread = new Thread() {
            @Override
            public void run() {
                hostLog.warn("VoltDB node shutting down as requested by @StopNode command.");
                System.exit(0);
            }
        };
        shutdownThread.start();
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
        if (m_clientInterface != null) {
            m_clientInterface.mayActivateSnapshotDaemon();
            try {
                m_clientInterface.startAcceptingConnections();
            } catch (IOException e) {
                hostLog.l7dlog(Level.FATAL,
                        LogKeys.host_VoltDB_ErrorStartAcceptingConnections.name(),
                        e);
                VoltDB.crashLocalVoltDB("Error starting client interface.", true, e);
            }
            if (m_producerDRGateway != null && !m_producerDRGateway.isStarted()) {
                // Start listening on the DR ports
                prepareReplication();
            }
        }
        startResourceUsageMonitor();

        try {
            if (m_adminListener != null) {
                m_adminListener.start();
            }
        } catch (Exception e) {
            hostLog.l7dlog(Level.FATAL, LogKeys.host_VoltDB_ErrorStartHTTPListener.name(), e);
            VoltDB.crashLocalVoltDB("HTTP service unable to bind to port.", true, e);
        }
        // Allow export datasources to start consuming their binary deques safely
        // as at this juncture the initial truncation snapshot is already complete
        ExportManager.instance().startPolling(m_catalogContext);

        //Tell import processors that they can start ingesting data.
        ImportManager.instance().readyForData(m_catalogContext, m_messenger);

        if (m_config.m_startAction == StartAction.REJOIN) {
            consoleLog.info(
                    "Node data recovery completed after " + delta + " seconds with " + megabytes +
                    " megabytes transferred at a rate of " +
                    megabytesPerSecond + " megabytes/sec");
        }

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

                String actionName = m_joining ? "join" : "rejoin";
                m_joining = false;
                consoleLog.info(String.format("Node %s completed", actionName));
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to log host rejoin completion to ZK", true, e);
        }
        hostLog.info("Logging host rejoin completion to ZK");
        if (!m_joining) {
            m_statusTracker.setNodeState(NodeState.UP);
            Object args[] = { (VoltDB.instance().getMode() == OperationMode.PAUSED) ? "PAUSED" : "NORMAL"};
            consoleLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerOpMode.name(), args, null);
            consoleLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerCompletedInitialization.name(), null, null);
        }
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
                m_statusTracker.setNodeState(NodeState.PAUSED);
                hostLog.info("Server is entering admin mode and pausing.");
            }
            else if (m_mode == OperationMode.PAUSED)
            {
                m_config.m_isPaused = false;
                m_statusTracker.setNodeState(NodeState.UP);
                hostLog.info("Server is exiting admin mode and resuming operation.");
            }
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
    public void setReplicationRole(ReplicationRole role)
    {
        if (role == ReplicationRole.NONE && m_config.m_replicationRole == ReplicationRole.REPLICA) {
            consoleLog.info("Promoting replication role from replica to master.");
            hostLog.info("Promoting replication role from replica to master.");
            shutdownReplicationConsumerRole();
            getStatsAgent().deregisterStatsSourcesFor(StatsSelector.DRCONSUMERNODE, 0);
            getStatsAgent().deregisterStatsSourcesFor(StatsSelector.DRCONSUMERPARTITION, 0);
            getStatsAgent().registerStatsSource(StatsSelector.DRCONSUMERNODE, 0,
                    new DRConsumerStatsBase.DRConsumerNodeStatsBase());
            getStatsAgent().registerStatsSource(StatsSelector.DRCONSUMERPARTITION, 0,
                    new DRConsumerStatsBase.DRConsumerPartitionStatsBase());
        }
        m_config.m_replicationRole = role;
        if (m_clientInterface != null) {
            m_clientInterface.setReplicationRole(m_config.m_replicationRole);
        }
    }

    private void shutdownReplicationConsumerRole() {
        if (m_consumerDRGateway != null) {
            try {
                m_consumerDRGateway.shutdown(true);
            } catch (InterruptedException e) {
                hostLog.warn("Interrupted shutting down dr replication", e);
            }
            finally {
                m_consumerDRGateway = null;
            }
        }
    }

    @Override
    public ReplicationRole getReplicationRole()
    {
        return m_config.m_replicationRole;
    }

    /**
     * Metadata is a JSON object
     */
    @Override
    public String getLocalMetadata() {
        return m_localMetadata;
    }

    @Override
    public void onRestoreCompletion(long txnId, Map<Integer, Long> perPartitionTxnIds) {

        /*
         * Command log is already initialized if this is a rejoin or a join
         */
        if ((m_commandLog != null) && (m_commandLog.needsInitialization())) {
            // Initialize command logger
            m_commandLog.init(m_catalogContext.cluster.getLogconfig().get("log").getLogsize(),
                              txnId, m_cartographer.getPartitionCount(),
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

        if (!m_rejoining && !m_joining) {
            if (m_clientInterface != null) {
                try {
                    m_clientInterface.startAcceptingConnections();
                } catch (IOException e) {
                    hostLog.l7dlog(Level.FATAL,
                                   LogKeys.host_VoltDB_ErrorStartAcceptingConnections.name(),
                                   e);
                    VoltDB.crashLocalVoltDB("Error starting client interface.", true, e);
                }
            }

            // Start listening on the DR ports
            prepareReplication();
            startResourceUsageMonitor();

            // Allow export datasources to start consuming their binary deques safely
            // as at this juncture the initial truncation snapshot is already complete
            ExportManager.instance().startPolling(m_catalogContext);

            //Tell import processors that they can start ingesting data.
            ImportManager.instance().readyForData(m_catalogContext, m_messenger);
        }

        try {
            if (m_adminListener != null) {
                m_adminListener.start();
            }
        } catch (Exception e) {
            hostLog.l7dlog(Level.FATAL, LogKeys.host_VoltDB_ErrorStartHTTPListener.name(), e);
            VoltDB.crashLocalVoltDB("HTTP service unable to bind to port.", true, e);
        }

        if (m_startMode != null) {
            m_mode = m_startMode;
        } else {
            // Shouldn't be here, but to be safe
            m_mode = OperationMode.RUNNING;
        }
        if (!m_rejoining && !m_joining) {
            Object args[] = { (m_mode == OperationMode.PAUSED) ? "PAUSED" : "NORMAL"};
            consoleLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerOpMode.name(), args, null);
            consoleLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerCompletedInitialization.name(), null, null);
        }

        // Create a zk node to indicate initialization is completed
        m_messenger.getZK().create(VoltZK.init_completed, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new ZKUtil.StringCallback(), null);
        m_statusTracker.setNodeState(NodeState.UP);
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
                consoleLog.info(String.format("Node %s completed", actionName));
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

    private void prepareReplication() {
        try {
            if (m_producerDRGateway != null) {
                m_producerDRGateway.initialize(m_catalogContext.cluster.getDrproducerenabled(),
                        VoltDB.getReplicationPort(m_catalogContext.cluster.getDrproducerport()),
                        VoltDB.getDefaultReplicationInterface());
            }
            if (m_consumerDRGateway != null) {
                m_consumerDRGateway.initialize(m_config.m_startAction != StartAction.CREATE);
            }
        } catch (Exception ex) {
            CoreUtils.printPortsInUse(hostLog);
            VoltDB.crashLocalVoltDB("Failed to initialize DR", false, ex);
        }
    }

    private boolean createDRConsumerIfNeeded() {
        if (!m_config.m_isEnterprise
                || (m_consumerDRGateway != null)
                || !m_catalogContext.cluster.getDrconsumerenabled()) {
            if (!m_config.m_isEnterprise || !m_catalogContext.cluster.getDrconsumerenabled()) {
                // This is called multiple times but the new value will be ignored if a StatSource has been assigned
                getStatsAgent().registerStatsSource(StatsSelector.DRCONSUMERNODE, 0,
                        new DRConsumerStatsBase.DRConsumerNodeStatsBase());
                getStatsAgent().registerStatsSource(StatsSelector.DRCONSUMERPARTITION, 0,
                        new DRConsumerStatsBase.DRConsumerPartitionStatsBase());
            }
            return false;
        }
        if (m_config.m_replicationRole == ReplicationRole.REPLICA ||
                 m_catalogContext.database.getIsactiveactivedred()) {
            String drProducerHost = m_catalogContext.cluster.getDrmasterhost();
            byte drConsumerClusterId = (byte)m_catalogContext.cluster.getDrclusterid();
            if (m_catalogContext.cluster.getDrconsumerenabled() &&
                    (drProducerHost == null || drProducerHost.isEmpty())) {
                VoltDB.crashLocalVoltDB("Cannot start as DR consumer without an enabled DR data connection.");
            }
            try {
                getStatsAgent().deregisterStatsSourcesFor(StatsSelector.DRCONSUMERNODE, 0);
                getStatsAgent().deregisterStatsSourcesFor(StatsSelector.DRCONSUMERPARTITION, 0);
                Class<?> rdrgwClass = Class.forName("org.voltdb.dr2.ConsumerDRGatewayImpl");
                Constructor<?> rdrgwConstructor = rdrgwClass.getConstructor(
                        int.class,
                        String.class,
                        ClientInterface.class,
                        byte.class);
                m_consumerDRGateway = (ConsumerDRGateway) rdrgwConstructor.newInstance(
                        m_messenger.getHostId(),
                        drProducerHost,
                        m_clientInterface,
                        drConsumerClusterId);
                m_globalServiceElector.registerService(m_consumerDRGateway);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to load DR system", true, e);
            }
            return true;
        }
        else {
            getStatsAgent().registerStatsSource(StatsSelector.DRCONSUMERNODE, 0,
                    new DRConsumerStatsBase.DRConsumerNodeStatsBase());
            getStatsAgent().registerStatsSource(StatsSelector.DRCONSUMERPARTITION, 0,
                    new DRConsumerStatsBase.DRConsumerPartitionStatsBase());
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
                // Replication role should the be same across the cluster
                js.key("role").value(getReplicationRole().ordinal());
                js.key("active").value(m_replicationActive.get());
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
    public void setDurabilityUniqueIdListener(Integer partition, DurableUniqueIdListener listener) {
        if (partition == MpInitiator.MP_INIT_PID) {
            m_iv2Initiators.get(m_iv2Initiators.firstKey()).setDurableUniqueIdListener(listener);
        }
        else {
            Initiator init = m_iv2Initiators.get(partition);
            assert init != null;
            init.setDurableUniqueIdListener(listener);
        }
    }

    public ExecutionEngine debugGetSpiedEE(int partitionId) {
        if (m_config.m_backend == BackendTarget.NATIVE_EE_SPY_JNI) {
            BaseInitiator init = (BaseInitiator)m_iv2Initiators.get(partitionId);
            return init.debugGetSpiedEE();
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
        File configInfoDir = new VoltFile(voltdbroot, VoltDB.CONFIG_DIR);
        configInfoDir.mkdirs();

        File depFH = new VoltFile(configInfoDir, "deployment.xml");
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

    private void checkHeapSanity(boolean isPro, int tableCount, int sitesPerHost, int kfactor)
    {
        long megabytes = 1024 * 1024;
        long maxMemory = Runtime.getRuntime().maxMemory() / megabytes;
        // DRv2 now is off heap
        long crazyThresh = computeMinimumHeapRqt(isPro, tableCount, sitesPerHost, kfactor);

        if (maxMemory < crazyThresh) {
            StringBuilder builder = new StringBuilder();
            builder.append(String.format("The configuration of %d tables, %d sites-per-host, and k-factor of %d requires at least %d MB of Java heap memory. ", tableCount, sitesPerHost, kfactor, crazyThresh));
            builder.append(String.format("The maximum amount of heap memory available to the JVM is %d MB. ", maxMemory));
            builder.append("Please increase the maximum heap size using the VOLTDB_HEAPMAX environment variable and then restart VoltDB.");
            consoleLog.warn(builder.toString());
        }

    }

    // Compute the minimum required heap to run this configuration.  This comes from the documentation,
    // http://voltdb.com/docs/PlanningGuide/MemSizeServers.php#MemSizeHeapGuidelines
    // Any changes there should get reflected here and vice versa.
    static public long computeMinimumHeapRqt(boolean isPro, int tableCount, int sitesPerHost, int kfactor)
    {
        long baseRqt = 384;
        long tableRqt = 10 * tableCount;
        // K-safety Heap consumption drop to 8 MB (per node)
        // Snapshot cost 32 MB (per node)
        // Theoretically, 40 MB (per node) should be enough
        long rejoinRqt = (isPro && kfactor > 0) ? 128 * sitesPerHost : 0;
        return baseRqt + tableRqt + rejoinRqt;
    }

    private void checkThreadsSanity() {
        int tableCount = m_catalogContext.tables.size();
        int partitions = m_iv2Initiators.size() - 1;
        int replicates = m_configuredReplicationFactor;
        int importPartitions = ImportManager.getPartitionsCount();
        int exportTableCount = ExportManager.instance().getExportTablesCount();
        int exportNonceCount = ExportManager.instance().getConnCount();

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
        hostLog.info("The internal DR cluster timestamp being restored from a snapshot is " +
                new Date(m_clusterCreateTime).toString() + ".");
    }
}
