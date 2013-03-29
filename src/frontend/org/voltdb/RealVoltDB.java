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

package org.voltdb;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Constructor;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra_voltpatches.GCInspector;
import org.apache.hadoop_voltpatches.util.PureJavaCrc32;
import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.COWMap;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.AdHocCompilerCache;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.SecurityType;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.dtxn.InitiatorStats;
import org.voltdb.dtxn.LatencyStats;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.export.ExportManager;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.fault.FaultDistributorInterface;
import org.voltdb.fault.SiteFailureFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.Initiator;
import org.voltdb.iv2.LeaderAppointer;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.SpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.rejoin.Iv2RejoinCoordinator;
import org.voltdb.rejoin.RejoinCoordinator;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.VoltSampler;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.SettableFuture;

/**
 * RealVoltDB initializes global server components, like the messaging
 * layer, ExecutionSite(s), and ClientInterface. It provides accessors
 * or references to those global objects. It is basically the global
 * namespace. A lot of the global namespace is described by VoltDBInterface
 * to allow test mocking.
 */
public class RealVoltDB implements VoltDBInterface, RestoreAgent.Callback
{
    private static final VoltLogger log = new VoltLogger(VoltDB.class.getName());
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");

    /** Default deployment file contents if path to deployment is null */
    private static final String[] defaultDeploymentXML = {
        "<?xml version=\"1.0\"?>",
        "<!-- IMPORTANT: This file is an auto-generated default deployment configuration.",
        "                Changes to this file will be overwritten. Copy it elsewhere if you",
        "                want to use it as a starting point for a custom configuration. -->",
        "<deployment>",
        "   <cluster hostcount=\"1\" sitesperhost=\"2\" />",
        "   <httpd enabled=\"true\">",
        "      <jsonapi enabled=\"true\" />",
        "   </httpd>",
        "</deployment>"
    };

    public VoltDB.Configuration m_config = new VoltDB.Configuration();
    int m_configuredNumberOfPartitions;
    CatalogContext m_catalogContext;
    private String m_buildString;
    private static final String m_defaultVersionString = "3.2";
    private String m_versionString = m_defaultVersionString;
    HostMessenger m_messenger = null;
    final ArrayList<ClientInterface> m_clientInterfaces = new ArrayList<ClientInterface>();
    private Map<Long, ExecutionSite> m_localSites;
    HTTPAdminListener m_adminListener;
    private Map<Long, Thread> m_siteThreads;
    private ArrayList<ExecutionSiteRunner> m_runners;
    private ExecutionSite m_currentThreadSite;
    private StatsAgent m_statsAgent = new StatsAgent();
    private AsyncCompilerAgent m_asyncCompilerAgent = new AsyncCompilerAgent();
    public AsyncCompilerAgent getAsyncCompilerAgent() { return m_asyncCompilerAgent; }
    FaultDistributor m_faultManager;
    private PartitionCountStats m_partitionCountStats = null;
    private IOStats m_ioStats = null;
    private MemoryStats m_memoryStats = null;
    private StatsManager m_statsManager = null;
    private SnapshotCompletionMonitor m_snapshotCompletionMonitor;
    private InitiatorStats m_initiatorStats;
    int m_myHostId;
    long m_depCRC = -1;
    String m_serializedCatalog;
    String m_httpPortExtraLogMessage = null;
    boolean m_jsonEnabled;
    DeploymentType m_deployment;

    // IV2 things
    List<Initiator> m_iv2Initiators = new ArrayList<Initiator>();
    Cartographer m_cartographer = null;
    LeaderAppointer m_leaderAppointer = null;
    GlobalServiceElector m_globalServiceElector = null;
    MpInitiator m_MPI = null;
    Map<Integer, Long> m_iv2InitiatorStartingTxnIds = new HashMap<Integer, Long>();


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
    String m_rejoinTruncationReqId = null;

    // Are we adding the node to the cluster instead of rejoining?
    volatile boolean m_joining = false;

    boolean m_replicationActive = false;
    private NodeDRGateway m_nodeDRGateway = null;

    //Only restrict recovery completion during test
    static Semaphore m_testBlockRecoveryCompletion = new Semaphore(Integer.MAX_VALUE);
    private long m_executionSiteRecoveryFinish;
    private long m_executionSiteRecoveryTransferred;

    // Rejoin coordinator
    private RejoinCoordinator m_rejoinCoordinator = null;

    // id of the leader, or the host restore planner says has the catalog
    int m_hostIdWithStartupCatalog;
    String m_pathToStartupCatalog;

    // Synchronize initialize and shutdown.
    private final Object m_startAndStopLock = new Object();

    // Synchronize updates of catalog contexts with context accessors.
    private final Object m_catalogUpdateLock = new Object();

    // add a random number to the sampler output to make it likely to be unique for this process.
    private final VoltSampler m_sampler = new VoltSampler(10, "sample" + String.valueOf(new Random().nextInt() % 10000) + ".txt");
    private final AtomicBoolean m_hasStartedSampler = new AtomicBoolean(false);

    final VoltDBSiteFailureFaultHandler m_faultHandler = new VoltDBSiteFailureFaultHandler(this);

    List<Pair<Integer, Long>> m_partitionsToSitesAtStartupForExportInit;

    RestoreAgent m_restoreAgent = null;

    private volatile boolean m_isRunning = false;

    @Override
    public boolean rejoining() { return m_rejoining; }

    @Override
    public boolean rejoinDataPending() { return m_rejoinDataPending; }

    private long m_recoveryStartTime;

    CommandLog m_commandLog;

    private volatile OperationMode m_mode = OperationMode.INITIALIZING;
    private OperationMode m_startMode = null;

    volatile String m_localMetadata = "";

    private ListeningExecutorService m_computationService;

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
    private LatencyStats m_latencyStats;

    @Override
    public LicenseApi getLicenseApi() {
        return m_licenseApi;
    }

    @Override
    public boolean isIV2Enabled() {
        return m_config.m_enableIV2;
    }

    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    @Override
    public void initialize(VoltDB.Configuration config) {
        synchronized(m_startAndStopLock) {
            // check that this is a 64 bit VM
            if (System.getProperty("java.vm.name").contains("64") == false) {
                hostLog.fatal("You are running on an unsupported (probably 32 bit) JVM. Exiting.");
                System.exit(-1);
            }
            consoleLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartupString.name(), null);
            if (!config.m_enableIV2) {
                consoleLog.warn("Running 3.0 preview (legacy mode).  THIS MODE IS DEPRECATED.");
            }

            // If there's no deployment provide a default and put it under voltdbroot.
            if (config.m_pathToDeployment == null) {
                try {
                    config.m_pathToDeployment = setupDefaultDeployment();
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Failed to write default deployment.", false, null);
                }
            }

            // set the mode first thing
            m_mode = OperationMode.INITIALIZING;
            m_config = config;
            m_startMode = null;

            // set a bunch of things to null/empty/new for tests
            // which reusue the process
            m_clientInterfaces.clear();
            m_adminListener = null;
            m_commandLog = new DummyCommandLog();
            m_deployment = null;
            m_messenger = null;
            m_startMode = null;
            m_statsAgent = new StatsAgent();
            m_asyncCompilerAgent = new AsyncCompilerAgent();
            m_faultManager = null;
            m_snapshotCompletionMonitor = null;
            m_catalogContext = null;
            m_partitionCountStats = null;
            m_ioStats = null;
            m_memoryStats = null;
            m_statsManager = null;
            m_restoreAgent = null;
            m_recoveryStartTime = System.currentTimeMillis();
            m_hostIdWithStartupCatalog = 0;
            m_pathToStartupCatalog = m_config.m_pathToCatalog;
            m_replicationActive = false;

            // set up site structure
            m_localSites = new COWMap<Long, ExecutionSite>();
            m_siteThreads = new HashMap<Long, Thread>();
            m_runners = new ArrayList<ExecutionSiteRunner>();
            final int computationThreads = Math.max(2, CoreUtils.availableProcessors() / 4);
            m_computationService =
                    CoreUtils.getListeningExecutorService(
                            "Computation service thread",
                            computationThreads, m_config.m_computationCoreBindings);

            // determine if this is a rejoining node
            // (used for license check and later the actual rejoin)
            boolean isRejoin = false;
            if (config.m_startAction == START_ACTION.REJOIN ||
                    config.m_startAction == START_ACTION.LIVE_REJOIN) {
                isRejoin = true;
            }
            m_rejoining = isRejoin;
            m_rejoinDataPending = isRejoin;

            m_joining = config.m_startAction == START_ACTION.JOIN;

            // Set std-out/err to use the UTF-8 encoding and fail if UTF-8 isn't supported
            try {
                System.setOut(new PrintStream(System.out, true, "UTF-8"));
                System.setErr(new PrintStream(System.err, true, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                hostLog.fatal("Support for the UTF-8 encoding is required for VoltDB. This means you are likely running an unsupported JVM. Exiting.");
                System.exit(-1);
            }

            m_snapshotCompletionMonitor = new SnapshotCompletionMonitor();

            readBuildInfo(config.m_isEnterprise ? "Enterprise Edition" : "Community Edition");

            buildClusterMesh(isRejoin || m_joining);

            //Start validating the build string in the background
            final Future<?> buildStringValidation = validateBuildString(getBuildString(), m_messenger.getZK());

            final int numberOfNodes = readDeploymentAndCreateStarterCatalogContext();
            if (!isRejoin && !m_joining) {
                m_messenger.waitForGroupJoin(numberOfNodes);
            }

            m_faultManager = new FaultDistributor(this);
            m_faultManager.registerFaultHandler(SiteFailureFault.SITE_FAILURE_CATALOG,
                    m_faultHandler,
                    FaultType.SITE_FAILURE);
            if (!m_faultManager.testPartitionDetectionDirectory(
                    m_catalogContext.cluster.getFaultsnapshots().get("CLUSTER_PARTITION"))) {
                VoltDB.crashLocalVoltDB("Unable to create partition detection snapshot directory at" +
                        m_catalogContext.cluster.getFaultsnapshots().get("CLUSTER_PARTITION"), false, null);
            }


            // Create the thread pool here. It's needed by buildClusterMesh()
            m_periodicWorkThread =
                    CoreUtils.getScheduledThreadPoolExecutor("Periodic Work", 1, CoreUtils.SMALL_STACK_SIZE);
            m_periodicPriorityWorkThread =
                    CoreUtils.getScheduledThreadPoolExecutor("Periodic Priority Work", 1, CoreUtils.SMALL_STACK_SIZE);

            m_licenseApi = MiscUtils.licenseApiFactory(m_config.m_pathToLicense);
            if (m_licenseApi == null) {
                VoltDB.crashLocalVoltDB("Failed to initialize license verifier. " +
                        "See previous log message for details.", false, null);
            }

            // Create the GlobalServiceElector.  Do this here so we can register the MPI with it
            // when we construct it below
            m_globalServiceElector = new GlobalServiceElector(m_messenger.getZK(), m_messenger.getHostId());

            Joiner joinCoordinator = null;
            if (m_joining) {
                Class<?> joinerClass = MiscUtils.loadProClass("org.voltdb.JoinerImpl", "Elastic", false);
                try {
                    Constructor<?> constructor = joinerClass.getConstructor(HostMessenger.class);
                    joinCoordinator = (Joiner) constructor.newInstance(m_messenger);
                    m_rejoinCoordinator = joinCoordinator;
                    m_messenger.registerMailbox(m_rejoinCoordinator);
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Failed to instantiate joiner", true, e);
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
             * On rejoin the rejoining node has to inspect the topology meta node to find out what is missing
             * and then update the topology listing itself as a replacement for one of the missing host ids.
             * Then it does a compare and set of the topology.
             */
            ClusterConfig clusterConfig = null;
            JSONObject topo = getTopology(config.m_startAction, joinCoordinator);
            m_partitionsToSitesAtStartupForExportInit = new ArrayList<Pair<Integer, Long>>();
            try {
                clusterConfig = new ClusterConfig(topo);
                m_configuredNumberOfPartitions = clusterConfig.getPartitionCount();

                // IV2 mailbox stuff
                if (isIV2Enabled()) {
                    m_cartographer = new Cartographer(m_messenger);
                    List<Integer> partitions = null;
                    if (isRejoin) {
                        partitions = m_cartographer.getIv2PartitionsToReplace(topo);
                        if (partitions.size() == 0) {
                            VoltDB.crashLocalVoltDB("The VoltDB cluster already has enough nodes to satisfy " +
                                    "the requested k-safety factor of " +
                                    clusterConfig.getReplicationFactor() + ".\n" +
                                    "No more nodes can join.", false, null);
                        }
                    }
                    else if (m_joining) {
                        // Ask the joiner for the new partitions to create on this node.
                        partitions = joinCoordinator.getPartitionsToAdd();
                    }
                    else {
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
                    // each node has an MPInitiator (and exactly 1 node has the master MPI).
                    long mpiBuddyHSId = m_iv2Initiators.get(0).getInitiatorHSId();
                    m_MPI = new MpInitiator(m_messenger, mpiBuddyHSId, m_statsAgent);
                    m_iv2Initiators.add(m_MPI);
                }

                clusterConfig = new ClusterConfig(topo);

                final long statsHSId = m_messenger.getHSIdForLocalSite(HostMessenger.STATS_SITE_ID);
                m_messenger.generateMailboxId(statsHSId);
                hostLog.info("Registering stats mailbox id " + CoreUtils.hsIdToString(statsHSId));

                // Make a list of HDIds to join
                List<Long> hsidsToRejoin = new ArrayList<Long>();
                for (Initiator init : m_iv2Initiators) {
                    if (init.isRejoinable()) {
                        hsidsToRejoin.add(init.getInitiatorHSId());
                    }
                }

                if (isRejoin && isIV2Enabled()) {
                    SnapshotSaveAPI.recoveringSiteCount.set(hsidsToRejoin.size());
                    hostLog.info("Set recovering site count to " + hsidsToRejoin.size());

                    m_rejoinCoordinator = new Iv2RejoinCoordinator(m_messenger, hsidsToRejoin,
                            m_catalogContext.cluster.getVoltroot(),
                            m_config.m_startAction == START_ACTION.LIVE_REJOIN);
                    m_messenger.registerMailbox(m_rejoinCoordinator);
                    if (m_config.m_startAction == START_ACTION.LIVE_REJOIN) {
                        hostLog.info("Using live rejoin.");
                    }
                    else {
                        hostLog.info("Using blocking rejoin.");
                    }
                } else if (m_joining) {
                    joinCoordinator.setSites(hsidsToRejoin);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            // do the many init tasks in the Inits class
            Inits inits = new Inits(this, 1);
            inits.doInitializationWork();

            if (config.m_backend.isIPC) {
                int eeCount = clusterConfig.getSitesPerHost();
                if (config.m_ipcPorts.size() != eeCount) {
                    hostLog.fatal("Specified an IPC backend but only supplied " + config.m_ipcPorts.size() +
                            " backend ports when " + eeCount + " are required");
                    System.exit(-1);
                }
            }

            collectLocalNetworkMetadata();

            /*
             * Construct an adhoc planner for the initial catalog
             */
            final CatalogSpecificPlanner csp = new CatalogSpecificPlanner(m_asyncCompilerAgent, m_catalogContext);

            // DR overflow directory
            File drOverflowDir = new File(m_catalogContext.cluster.getVoltroot(), "dr_overflow");
            if (m_config.m_isEnterprise) {
                try {
                    Class<?> ndrgwClass = Class.forName("org.voltdb.dr.InvocationBufferServer");
                    Constructor<?> ndrgwConstructor = ndrgwClass.getConstructor(File.class, boolean.class);
                    m_nodeDRGateway = (NodeDRGateway) ndrgwConstructor.newInstance(drOverflowDir,
                                                                                   m_replicationActive);
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Unable to load DR system", true, e);
                }
            }

            // Initialize stats
            m_ioStats = new IOStats();
            m_statsAgent.registerStatsSource(SysProcSelector.IOSTATS,
                    0, m_ioStats);
            m_memoryStats = new MemoryStats();
            m_statsAgent.registerStatsSource(SysProcSelector.MEMORY,
                    0, m_memoryStats);
            m_statsAgent.registerStatsSource(SysProcSelector.TOPO, 0, m_cartographer);
            m_partitionCountStats = new PartitionCountStats(m_cartographer);
            m_statsAgent.registerStatsSource(SysProcSelector.PARTITIONCOUNT,
                    0, m_partitionCountStats);
            m_initiatorStats = new InitiatorStats(m_myHostId);
            m_latencyStats = new LatencyStats(m_myHostId);

            /*
             * Initialize the command log on rejoin before configuring the IV2
             * initiators.  This will prevent them from receiving transactions
             * which need logging before the internal file writers are
             * initialized.  Root cause of ENG-4136.
             */
            if (m_commandLog != null && isRejoin) {
                //On rejoin the starting IDs are all 0 so technically it will load any snapshot
                //but the newest snapshot will always be the truncation snapshot taken after rejoin
                //completes at which point the node will mark itself as actually recovered.
                m_commandLog.initForRejoin(
                        m_catalogContext,
                        Long.MIN_VALUE,
                        m_iv2InitiatorStartingTxnIds,
                        true,
                        m_config.m_commandLogBinding);
            }

            /*
             * Configure and start all the IV2 sites
             */
            try {
                boolean usingCommandLog = m_config.m_isEnterprise &&
                    m_catalogContext.cluster.getLogconfig().get("log").getEnabled();
                m_leaderAppointer = new LeaderAppointer(
                        m_messenger,
                        clusterConfig.getPartitionCount(),
                        m_deployment.getCluster().getKfactor(),
                        m_catalogContext.cluster.getNetworkpartition(),
                        m_catalogContext.cluster.getFaultsnapshots().get("CLUSTER_PARTITION"),
                        usingCommandLog,
                        topo, m_MPI);
                m_globalServiceElector.registerService(m_leaderAppointer);

                for (Initiator iv2init : m_iv2Initiators) {
                    iv2init.configure(
                            getBackendTargetType(),
                            m_serializedCatalog,
                            m_catalogContext,
                            m_deployment.getCluster().getKfactor(),
                            csp,
                            clusterConfig.getPartitionCount(),
                            m_config.m_startAction,
                            m_statsAgent,
                            m_memoryStats,
                            m_commandLog,
                            m_nodeDRGateway,
                            m_config.m_executionCoreBindings.poll());
                }
            } catch (Exception e) {
                Throwable toLog = e;
                if (e instanceof ExecutionException) {
                    toLog = ((ExecutionException)e).getCause();
                }
                VoltDB.crashLocalVoltDB("Error configuring IV2 initiator.", true, toLog);
            }

            // Start the GlobalServiceElector.  Not sure where this will actually belong.
            try {
                m_globalServiceElector.start();
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unable to start GlobalServiceElector", true, e);
            }

            // Create the client interface
            int portOffset = 0;
            for (int i = 0; i < 1; i++) {
                try {
                    ClientInterface ci =
                        ClientInterface.create(m_messenger,
                                m_catalogContext,
                                m_config.m_replicationRole,
                                m_cartographer,
                                clusterConfig.getPartitionCount(),
                                config.m_port + portOffset,
                                config.m_adminPort + portOffset,
                                m_config.m_timestampTestingSalt);
                    portOffset += 2;
                    m_clientInterfaces.add(ci);
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
                portOffset += 2;
            }

            // Create the statistics manager and register it to JMX registry
            m_statsManager = null;
            try {
                final Class<?> statsManagerClass =
                        MiscUtils.loadProClass("org.voltdb.management.JMXStatsManager", "JMX", true);
                if (statsManagerClass != null) {
                    ArrayList<Long> localHSIds;
                    Long MPHSId;
                    if (isIV2Enabled()) {
                        localHSIds = new ArrayList<Long>();
                        for (Initiator iv2Initiator : m_iv2Initiators) {
                            localHSIds.add(iv2Initiator.getInitiatorHSId());
                        }
                        MPHSId = m_MPI.getInitiatorHSId();
                    } else {
                        localHSIds = new ArrayList<Long>(m_localSites.keySet());
                        MPHSId = null;
                    }
                    m_statsManager = (StatsManager)statsManagerClass.newInstance();
                    m_statsManager.initialize(localHSIds, MPHSId);
                }
            } catch (Exception e) {
                hostLog.error("Failed to instantiate the JMX stats manager: " + e.getMessage() +
                              ". Disabling JMX.");
                e.printStackTrace();
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
                    m_messenger.waitForAllHostsToBeReady(m_deployment.getCluster().getHostcount());
                } catch (Exception e) {
                    hostLog.fatal("Failed to announce ready state.");
                    VoltDB.crashLocalVoltDB("Failed to announce ready state.", false, null);
                }
            }

            if (!m_joining && (m_cartographer.getPartitions().size() - 1) !=
                    m_configuredNumberOfPartitions) {
                for (Map.Entry<Integer, ImmutableList<Long>> entry :
                    getSiteTrackerForSnapshot().m_partitionsToSitesImmutable.entrySet()) {
                    hostLog.info(entry.getKey() + " -- "
                            + CoreUtils.hsIdCollectionToString(entry.getValue()));
                }
                VoltDB.crashGlobalVoltDB("Mismatch between configured number of partitions (" +
                        m_configuredNumberOfPartitions + ") and actual (" +
                        (m_cartographer.getPartitions().size() - 1) + ")",
                        true, null);
            }

            schedulePeriodicWorks();
            m_clientInterfaces.get(0).schedulePeriodicWorks();

            // print out a bunch of useful system info
            logDebuggingInfo(m_config.m_adminPort, m_config.m_httpPort, m_httpPortExtraLogMessage, m_jsonEnabled);

            if (clusterConfig.getReplicationFactor() == 0) {
                hostLog.warn("Running without redundancy (k=0) is not recommended for production use.");
            }

            // warn if cluster is partitionable, but partition detection is off
            if ((m_catalogContext.cluster.getNetworkpartition() == false) &&
                    (clusterConfig.getReplicationFactor() > 0)) {
                hostLog.warn("Running a redundant (k-safe) cluster with network " +
                        "partition detection disabled is not recommended for production use.");
                // we decided not to include the stronger language below for the 3.0 version (ENG-4215)
                //hostLog.warn("With partition detection disabled, data may be lost or " +
                //      "corrupted by certain classes of network failures.");
            }

            assert(m_clientInterfaces.size() > 0);
            ClientInterface ci = m_clientInterfaces.get(0);
            ci.initializeSnapshotDaemon(m_messenger.getZK(), m_globalServiceElector);

            // set additional restore agent stuff
            if (m_restoreAgent != null) {
                ci.bindAdapter(m_restoreAgent.getAdapter());
                m_restoreAgent.setCatalogContext(m_catalogContext);
                m_restoreAgent.setInitiator(new Iv2TransactionCreator(m_clientInterfaces.get(0)));
            }
        }
    }

    // Get topology information.  If rejoining, get it directly from
    // ZK.  Otherwise, try to do the write/read race to ZK on startup.
    private JSONObject getTopology(START_ACTION startAction, Joiner joinCoordinator)
    {
        JSONObject topo = null;
        if (startAction == START_ACTION.JOIN) {
            assert(joinCoordinator != null);
            topo = joinCoordinator.getTopology();
        }
        else if (!VoltDB.createForRejoin(startAction)) {
            int sitesperhost = m_deployment.getCluster().getSitesperhost();
            int hostcount = m_deployment.getCluster().getHostcount();
            int kfactor = m_deployment.getCluster().getKfactor();
            ClusterConfig clusterConfig = new ClusterConfig(hostcount, sitesperhost, kfactor);
            if (!clusterConfig.validate()) {
                VoltDB.crashLocalVoltDB(clusterConfig.getErrorMsg(), false, null);
            }
            topo = registerClusterConfig(clusterConfig);
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

    private List<Initiator> createIv2Initiators(Collection<Integer> partitions,
                                                START_ACTION startAction,
                                                List<Pair<Integer, Long>> m_partitionsToSitesAtStartupForExportInit)
    {
        List<Initiator> initiators = new ArrayList<Initiator>();
        for (Integer partition : partitions)
        {
            Initiator initiator = new SpInitiator(m_messenger, partition, m_statsAgent,
                    m_snapshotCompletionMonitor, startAction);
            initiators.add(initiator);
            m_partitionsToSitesAtStartupForExportInit.add(Pair.of(partition, initiator.getInitiatorHSId()));
        }
        return initiators;
    }

    private JSONObject registerClusterConfig(ClusterConfig config)
    {
        // First, race to write the topology to ZK using Highlander rules
        // (In the end, there can be only one)
        JSONObject topo = null;
        try
        {
            topo = config.getTopology(m_messenger.getLiveHostIds());
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

    /**
     * Schedule all the periodic works
     */
    private void schedulePeriodicWorks() {
        // JMX stats broadcast
        scheduleWork(new Runnable() {
            @Override
            public void run() {
                m_statsManager.sendNotification();
            }
        }, 0, StatsManager.POLL_INTERVAL, TimeUnit.MILLISECONDS);

        // small stats samples
        scheduleWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(false, false);
            }
        }, 0, 5, TimeUnit.SECONDS);

        // medium stats samples
        scheduleWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(true, false);
            }
        }, 0, 1, TimeUnit.MINUTES);

        // large stats samples
        scheduleWork(new Runnable() {
            @Override
            public void run() {
                SystemStatsCollector.asyncSampleSystemNow(true, true);
            }
        }, 0, 6, TimeUnit.MINUTES);
        GCInspector.instance.start(m_periodicPriorityWorkThread);
    }

    int readDeploymentAndCreateStarterCatalogContext() {
        /*
         * Debate with the cluster what the deployment file should be
         */
        try {
            ZooKeeper zk = m_messenger.getZK();
            byte deploymentBytes[] = org.voltcore.utils.CoreUtils.urlToBytes(m_config.m_pathToDeployment);

            try {
                if (deploymentBytes != null) {
                    zk.create(VoltZK.deploymentBytes, deploymentBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    hostLog.info("URL of deployment info: " + m_config.m_pathToDeployment);
                } else {
                    throw new KeeperException.NodeExistsException();
                }
            } catch (KeeperException.NodeExistsException e) {
                byte deploymentBytesTemp[] = zk.getData(VoltZK.deploymentBytes, false, null);
                if (deploymentBytesTemp == null) {
                    throw new RuntimeException(
                            "Deployment file could not be found locally or remotely at "
                            + m_config.m_pathToDeployment);
                }
                PureJavaCrc32 crc = new PureJavaCrc32();
                crc.update(deploymentBytes);
                final long checksumHere = crc.getValue();
                crc.reset();
                crc.update(deploymentBytesTemp);
                if (checksumHere != crc.getValue()) {
                    hostLog.info("Deployment configuration was pulled from ZK, and the checksum did not match " +
                    "the locally supplied file");
                } else {
                    hostLog.info("Deployment configuration pulled from ZK");
                }
                deploymentBytes = deploymentBytesTemp;
            }

            m_deployment = CatalogUtil.getDeployment(new ByteArrayInputStream(deploymentBytes));
            // wasn't a valid xml deployment file
            if (m_deployment == null) {
                hostLog.error("Not a valid XML deployment file at URL: " + m_config.m_pathToDeployment);
                VoltDB.crashLocalVoltDB("Not a valid XML deployment file at URL: "
                        + m_config.m_pathToDeployment, false, null);
            }

            // note the heart beats are specified in seconds in xml, but ms internally
            HeartbeatType hbt = m_deployment.getHeartbeat();
            if (hbt != null) {
                m_config.m_deadHostTimeoutMS = hbt.getTimeout() * 1000;
                m_messenger.setDeadHostTimeout(m_config.m_deadHostTimeoutMS);
            }


            // create a dummy catalog to load deployment info into
            Catalog catalog = new Catalog();
            Cluster cluster = catalog.getClusters().add("cluster");
            Database db = cluster.getDatabases().add("database");

            // enable security if set on the deployment file
            SecurityType security = m_deployment.getSecurity();
            if (security != null) {
                cluster.setSecurityenabled(security.isEnabled());
            }


            // create groups as needed for users
            if (m_deployment.getUsers() != null) {
                for (UsersType.User user : m_deployment.getUsers().getUser()) {
                    Set<String> roles = CatalogUtil.mergeUserRoles(user);
                    if (roles.isEmpty()) {
                        continue;
                    }
                    for (String role : roles) {
                        if (db.getGroups().get(role) == null) {
                            db.getGroups().add(role);
                        }
                    }
                }
            }

            long depCRC = CatalogUtil.compileDeploymentAndGetCRC(catalog, m_deployment,
                    true, true);
            assert(depCRC != -1);

            m_catalogContext = new CatalogContext(
                    isIV2Enabled() ? TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId() : 0,//txnid
                            0,//timestamp
                            catalog, null, depCRC, 0, -1);

            int numberOfNodes = m_deployment.getCluster().getHostcount();
            if (numberOfNodes <= 0) {
                hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_InvalidHostCount.name(),
                        new Object[] { numberOfNodes }, null);
                VoltDB.crashLocalVoltDB("Invalid cluster size: " + numberOfNodes, false, null);
            }

            return numberOfNodes;
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
                LinkedList<NetworkInterface> interfaces = new LinkedList<NetworkInterface>();
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
            stringer.key("adminPort").value(m_config.m_adminPort);
            stringer.key("httpPort").value(m_config.m_httpPort);
            stringer.key("drPort").value(m_config.m_drAgentPortStart);
            stringer.endObject();
            JSONObject obj = new JSONObject(stringer.toString());
            // possibly atomic swap from null to realz
            m_localMetadata = obj.toString(4);
        } catch (Exception e) {
            hostLog.warn("Failed to collect data about lcoal network interfaces", e);
        }
    }

    /**
     * Start the voltcore HostMessenger. This joins the node
     * to the existing cluster. In the non rejoin case, this
     * function will return when the mesh is complete. If
     * rejoining, it will return when the node and agreement
     * site are synched to the existing cluster.
     */
    void buildClusterMesh(boolean isRejoin) {
        final String leaderAddress = m_config.m_leader;
        String hostname = MiscUtils.getHostnameFromHostnameColonPort(leaderAddress);
        int port = MiscUtils.getPortFromHostnameColonPort(leaderAddress, m_config.m_internalPort);

        org.voltcore.messaging.HostMessenger.Config hmconfig;

        hmconfig = new org.voltcore.messaging.HostMessenger.Config(hostname, port);
        hmconfig.internalPort = m_config.m_internalPort;
        hmconfig.internalInterface = m_config.m_internalInterface;
        hmconfig.zkInterface = m_config.m_zkInterface;
        hmconfig.deadHostTimeout = m_config.m_deadHostTimeoutMS;
        hmconfig.factory = new VoltDbMessageFactory();
        hmconfig.coreBindIds = m_config.m_networkCoreBindings;

        m_messenger = new org.voltcore.messaging.HostMessenger(hmconfig);

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

        // Semi-hacky check to see if we're attempting to rejoin to ourselves.
        // The leader node gets assigned host ID 0, always, so if we're the
        // leader and we're rejoining, this is clearly bad.
        if (m_myHostId == 0 && isRejoin) {
            VoltDB.crashLocalVoltDB("Unable to rejoin a node to itself.  " +
                    "Please check your command line and start action and try again.", false, null);
        }
    }

    void logDebuggingInfo(int adminPort, int httpPort, String httpPortExtraLogMessage, boolean jsonEnabled) {
        String startAction = m_config.m_startAction.toString();
        String startActionLog = "Database start action is " + (startAction.substring(0, 1).toUpperCase() +
                startAction.substring(1).toLowerCase()) + ".";
        if (!m_rejoining) {
            hostLog.info(startActionLog);
        }

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

        // replay command line args that we can see
        List<String> iargs = ManagementFactory.getRuntimeMXBean().getInputArguments();
        StringBuilder sb = new StringBuilder(2048).append("Available JVM arguments:");
        for (String iarg : iargs)
            sb.append(" ").append(iarg);
        if (iargs.size() > 0) hostLog.info(sb.toString());
        else hostLog.info("No JVM command line args known.");

        sb.delete(0, sb.length()).append("JVM class path: ");
        sb.append(System.getProperty("java.class.path", "[not available]"));
        hostLog.info(sb.toString());

        // java heap size
        long javamaxheapmem = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        javamaxheapmem /= (1024 * 1024);
        hostLog.info(String.format("Maximum usable Java heap set to %d mb.", javamaxheapmem));

        m_catalogContext.logDebuggingInfoFromCatalog();

        // print out a bunch of useful system info
        PlatformProperties pp = PlatformProperties.getPlatformProperties();
        String[] lines = pp.toLogLines().split("\n");
        for (String line : lines) {
            hostLog.info(line.trim());
        }

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

        Map<Integer, String> clusterMetadata = new HashMap<Integer, String>(0);
        /*
         * Spin and attempt to retrieve cluster metadata for all nodes in the cluster.
         */
        Set<Integer> metadataToRetrieve = new HashSet<Integer>(m_messenger.getLiveHostIds());
        metadataToRetrieve.remove(m_messenger.getHostId());
        while (!metadataToRetrieve.isEmpty()) {
            Map<Integer, ZKUtil.ByteArrayCallback> callbacks = new HashMap<Integer, ZKUtil.ByteArrayCallback>();
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
        hostLog.info("About to list cluster interfaces for all nodes with format [ip1 ip2 ... ipN] client-port:admin-port:http-port");
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


    public static String[] extractBuildInfo() {
        StringBuilder sb = new StringBuilder(64);
        String buildString = "VoltDB";
        String versionString = m_defaultVersionString;
        byte b = -1;
        try {
            InputStream buildstringStream =
                ClassLoader.getSystemResourceAsStream("buildstring.txt");
            if (buildstringStream == null) {
                throw new RuntimeException("Unreadable or missing buildstring.txt file.");
            }
            while ((b = (byte) buildstringStream.read()) != -1) {
                sb.append((char)b);
            }
            sb.append("\n");
            String parts[] = sb.toString().split(" ", 2);
            if (parts.length != 2) {
                throw new RuntimeException("Invalid buildstring.txt file.");
            }
            versionString = parts[0].trim();
            buildString = parts[1].trim();
        } catch (Exception ignored) {
            try {
                InputStream buildstringStream = new FileInputStream("version.txt");
                try {
                    while ((b = (byte) buildstringStream.read()) != -1) {
                        sb.append((char)b);
                    }
                    versionString = sb.toString().trim();
                } finally {
                    buildstringStream.close();
                }
            }
            catch (Exception ignored2) {
                log.l7dlog( Level.ERROR, LogKeys.org_voltdb_VoltDB_FailedToRetrieveBuildString.name(), null);
            }
        }
        return new String[] { versionString, buildString };
    }

    @Override
    public void readBuildInfo(String editionTag) {
        String buildInfo[] = extractBuildInfo();
        m_versionString = buildInfo[0];
        m_buildString = buildInfo[1];
        consoleLog.info(String.format("Build: %s %s %s", m_versionString, m_buildString, editionTag));
    }

    /**
     * Start all the site's event loops. That's it.
     */
    @Override
    public void run() {

        if (!isIV2Enabled()) {
            // start the separate EE threads
            for (ExecutionSiteRunner r : m_runners) {
                r.m_shouldStartRunning.countDown();
            }
        }

        if (m_restoreAgent != null) {
            // start restore process
            m_restoreAgent.restore();
        }
        else {
            onRestoreCompletion(Long.MIN_VALUE, m_iv2InitiatorStartingTxnIds);
        }

        // Start the rejoin coordinator
        if (m_rejoinCoordinator != null) {
            try {
                m_rejoinCoordinator.setClientInterface(m_clientInterfaces.get(0));

                if (!m_rejoinCoordinator.startJoin(m_catalogContext.database, m_cartographer)) {
                    VoltDB.crashLocalVoltDB("Failed to join the cluster", true, null);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Failed to join the cluster", true, e);
            }
        }

        // start one site in the current thread
        Thread.currentThread().setName("ExecutionSiteAndVoltDB");
        m_isRunning = true;
        try
        {
            if (!isIV2Enabled()) {
                m_currentThreadSite.run();
            }
            else {
                while (m_isRunning) {
                    Thread.sleep(3000);
                }
            }
        }
        catch (Throwable thrown)
        {
            String errmsg = " encountered an unexpected error and will die, taking this VoltDB node down.";
            hostLog.error(errmsg);
            // It's too easy for stdout to get lost, especially if we are crashing, so log FATAL, instead.
            // Logging also automatically prefixes lines with "ExecutionSite [X:Y] "
            // thrown.printStackTrace();
            hostLog.fatal("Stack trace of thrown exception: " + thrown.toString());
            for (StackTraceElement ste : thrown.getStackTrace()) {
                hostLog.fatal(ste.toString());
            }
            VoltDB.crashLocalVoltDB(errmsg, true, thrown);
        }
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
                // Things are going pear-shaped, tell the fault distributor to
                // shut its fat mouth
                m_faultManager.shutDown();
                m_snapshotCompletionMonitor.shutdown();
                m_periodicWorkThread.shutdown();
                m_periodicPriorityWorkThread.shutdown();

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
                for (ClientInterface ci : m_clientInterfaces) {
                    ci.shutdown();
                }

                if (!isIV2Enabled()) {
                    // tell all m_sites to stop their runloops
                    if (m_localSites != null) {
                        for (ExecutionSite site : m_localSites.values())
                            site.startShutdown();
                    }
                }

                // tell the iv2 sites to stop their runloop
                if (m_iv2Initiators != null) {
                    for (Initiator init : m_iv2Initiators)
                        init.shutdown();
                }

                if (m_cartographer != null) {
                    m_cartographer.shutdown();
                }

                if (!isIV2Enabled()) {
                    // try to join all threads but the main one
                    // probably want to check if one of these is the current thread
                    if (m_siteThreads != null) {
                        for (Thread siteThread : m_siteThreads.values()) {
                            if (Thread.currentThread().equals(siteThread) == false) {
                                // don't interrupt here. the site will start shutdown when
                                // it sees the shutdown flag set.
                                siteThread.join();
                            }
                        }
                    }

                    // try to join the main thread (possibly this one)
                    if (mainSiteThread != null) {
                        if (Thread.currentThread().equals(mainSiteThread) == false) {
                            // don't interrupt here. the site will start shutdown when
                            // it sees the shutdown flag set.
                            mainSiteThread.join();
                        }
                    }
                }

                // shut down Export and its connectors.
                ExportManager.instance().shutdown();

                // After sites are terminated, shutdown the InvocationBufferServer.
                // The IBS is shared by all sites; don't kill it while any site is active.
                if (m_nodeDRGateway != null) {
                    try {
                        m_nodeDRGateway.shutdown();
                    } catch (InterruptedException e) {
                        log.warn("Interrupted shutting down invocation buffer server", e);
                    }
                }

                // help the gc along
                m_localSites = null;
                m_currentThreadSite = null;
                m_siteThreads = null;
                m_runners = null;

                // shut down the network/messaging stuff
                // Close the host messenger first, which should close down all of
                // the ForeignHost sockets cleanly
                if (m_messenger != null)
                {
                    m_messenger.shutdown();
                }
                m_messenger = null;

                //Also for test code that expects a fresh stats agent
                if (m_statsAgent != null) {
                    m_statsAgent.shutdown();
                    m_statsAgent = null;
                }

                if (m_asyncCompilerAgent != null) {
                    m_asyncCompilerAgent.shutdown();
                    m_asyncCompilerAgent = null;
                }

                // The network iterates this list. Clear it after network's done.
                m_clientInterfaces.clear();

                ExportManager.instance().shutdown();
                m_computationService.shutdown();
                m_computationService.awaitTermination(1, TimeUnit.DAYS);
                m_computationService = null;
                m_catalogContext = null;
                m_initiatorStats = null;
                m_latencyStats = null;

                AdHocCompilerCache.clearVersionCache();
                org.voltdb.iv2.InitiatorMailbox.m_allInitiatorMailboxes.clear();

                // probably unnecessary
                System.gc();
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
        new HashMap<Long, ContextTracker>();

    @Override
    public Pair<CatalogContext, CatalogSpecificPlanner> catalogUpdate(
            String diffCommands,
            byte[] newCatalogBytes,
            int expectedCatalogVersion,
            long currentTxnId,
            long currentTxnUniqueId,
            long deploymentCRC)
    {
        synchronized(m_catalogUpdateLock) {
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

            // 0. A new catalog! Update the global context and the context tracker
            m_catalogContext =
                m_catalogContext.update(
                        currentTxnId,
                        currentTxnUniqueId,
                        newCatalogBytes,
                        diffCommands,
                        true,
                        deploymentCRC);
            final CatalogSpecificPlanner csp = new CatalogSpecificPlanner( m_asyncCompilerAgent, m_catalogContext);
            m_txnIdToContextTracker.put(currentTxnId,
                    new ContextTracker(
                            m_catalogContext,
                            csp));
            m_catalogContext.logDebuggingInfoFromCatalog();

            //Construct the list of partitions and sites because it simply doesn't exist anymore
            SiteTracker siteTracker = VoltDB.instance().getSiteTrackerForSnapshot();
            List<Long> sites = siteTracker.getSitesForHost(m_messenger.getHostId());

            List<Pair<Integer,Long>> partitions = new ArrayList<Pair<Integer, Long>>();
            for (Long site : sites) {
                Integer partition = siteTracker.getPartitionForSite(site);
                partitions.add(Pair.of(partition, site));
            }

            // 1. update the export manager.
            ExportManager.instance().updateCatalog(m_catalogContext, partitions);

            // 2. update client interface (asynchronously)
            //    CI in turn updates the planner thread.
            for (ClientInterface ci : m_clientInterfaces) {
                ci.notifyOfCatalogUpdate();
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
            m_statsAgent.notifyOfCatalogUpdate();

            // 5. MPIs don't run fragments. Update them here. Do
            // this after flushing the stats -- this will re-register
            // the MPI statistics.
            if (m_MPI != null) {
                m_MPI.updateCatalog(diffCommands, m_catalogContext, csp);
            }


            return Pair.of(m_catalogContext, csp);
        }
    }

    @Override
    public VoltDB.Configuration getConfig() {
        return m_config;
    }

    @Override
    public String getBuildString() {
        return m_buildString;
    }

    @Override
    public String getVersionString() {
        return m_versionString;
    }

    @Override
    public HostMessenger getHostMessenger() {
        return m_messenger;
    }

    @Override
    public ArrayList<ClientInterface> getClientInterfaces() {
        return m_clientInterfaces;
    }

    @Override
    public Map<Long, ExecutionSite> getLocalSites() {
        return m_localSites;
    }

    @Override
    public StatsAgent getStatsAgent() {
        return m_statsAgent;
    }

    @Override
    public MemoryStats getMemoryStatsSource() {
        return m_memoryStats;
    }

    @Override
    public FaultDistributorInterface getFaultDistributor()
    {
        return m_faultManager;
    }

    @Override
    public CatalogContext getCatalogContext() {
        synchronized(m_catalogUpdateLock) {
            return m_catalogContext;
        }
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

    /**
     * Debugging function - creates a record of the current state of the system.
     * @param out PrintStream to write report to.
     */
    public void createRuntimeReport(PrintStream out) {
        // This function may be running in its own thread.

        out.print("MIME-Version: 1.0\n");
        out.print("Content-type: multipart/mixed; boundary=\"reportsection\"");

        out.print("\n\n--reportsection\nContent-Type: text/plain\n\nClientInterface Report\n");
        for (ClientInterface ci : getClientInterfaces()) {
            out.print(ci.toString() + "\n");
        }

        out.print("\n\n--reportsection\nContent-Type: text/plain\n\nLocalSite Report\n");
        for(ExecutionSite es : getLocalSites().values()) {
            out.print(es.toString() + "\n");
        }

        out.print("\n\n--reportsection--");
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
        if (m_rejoinCoordinator != null) {
            m_rejoinCoordinator.close();
        }
        m_rejoinCoordinator = null;
        // Mark the data transfer as done so CL can make the right decision when a truncation snapshot completes
        m_rejoinDataPending = false;

        try {
            m_testBlockRecoveryCompletion.acquire();
        } catch (InterruptedException e) {}
        final long delta = ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000);
        final long megabytes = m_executionSiteRecoveryTransferred / (1024 * 1024);
        final double megabytesPerSecond = megabytes / ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000.0);
        for (ClientInterface intf : getClientInterfaces()) {
            intf.mayActivateSnapshotDaemon();
            try {
                intf.startAcceptingConnections();
            } catch (IOException e) {
                hostLog.l7dlog(Level.FATAL,
                        LogKeys.host_VoltDB_ErrorStartAcceptingConnections.name(),
                        e);
                VoltDB.crashLocalVoltDB("Error starting client interface.", true, e);
            }
        }

        if (m_config.m_startAction == START_ACTION.REJOIN) {
            consoleLog.info(
                    "Node data recovery completed after " + delta + " seconds with " + megabytes +
                    " megabytes transferred at a rate of " +
                    megabytesPerSecond + " megabytes/sec");
        }

        try {
            final ZooKeeper zk = m_messenger.getZK();
            boolean logRecoveryCompleted = false;
            if (getCommandLog().getClass().getName().equals("org.voltdb.CommandLogImpl")) {
                try {
                    if (m_rejoinTruncationReqId == null) {
                        m_rejoinTruncationReqId = java.util.UUID.randomUUID().toString();
                    }
                    zk.create(VoltZK.request_truncation_snapshot, m_rejoinTruncationReqId.getBytes(),
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {}
            } else {
                logRecoveryCompleted = true;
            }
            if (logRecoveryCompleted) {
                m_rejoining = false;
                m_joining = false;
                consoleLog.info("Node rejoin completed");
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to log host rejoin completion to ZK", true, e);
        }
        hostLog.info("Logging host rejoin completion to ZK");
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
                hostLog.info("Server is entering admin mode and pausing.");
            }
            else if (m_mode == OperationMode.PAUSED)
            {
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
        }
        m_config.m_replicationRole = role;
        for (ClientInterface ci : m_clientInterfaces) {
            ci.setReplicationRole(m_config.m_replicationRole);
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
         * Command log is already initialized if this is a rejoin
         */
        if ((m_commandLog != null) && (m_commandLog.needsInitialization())) {
            // Initialize command logger
            m_commandLog.init(m_catalogContext, txnId, perPartitionTxnIds, m_config.m_commandLogBinding);
        }

        /*
         * IV2: After the command log is initialized, force the writing of the initial
         * viable replay set.  Turns into a no-op with no command log, on the non-leader sites, and on the MPI.
         */
        for (Initiator initiator : m_iv2Initiators) {
            initiator.enableWritingIv2FaultLog();
        }

        /*
         * IV2: From this point on, not all node failures should crash global VoltDB.
         */
        if (m_leaderAppointer != null) {
            m_leaderAppointer.onReplayCompletion();
        }

        if (!m_rejoining && !m_joining) {
            for (ClientInterface ci : m_clientInterfaces) {
                try {
                    ci.startAcceptingConnections();
                } catch (IOException e) {
                    hostLog.l7dlog(Level.FATAL,
                                   LogKeys.host_VoltDB_ErrorStartAcceptingConnections.name(),
                                   e);
                    VoltDB.crashLocalVoltDB("Error starting client interface.", true, e);
                }
            }
        }

        // Start listening on the DR ports
        prepareReplication();

        if (m_startMode != null) {
            m_mode = m_startMode;
        } else {
            // Shouldn't be here, but to be safe
            m_mode = OperationMode.RUNNING;
        }
        consoleLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerCompletedInitialization.name(), null);
    }

    @Override
    public SnapshotCompletionMonitor getSnapshotCompletionMonitor() {
        return m_snapshotCompletionMonitor;
    }

    @Override
    public synchronized void recoveryComplete(String requestId) {
        assert(m_rejoinDataPending == false);

        if (m_rejoining) {
            if (requestId.equals(m_rejoinTruncationReqId)) {
                consoleLog.info("Node rejoin completed");
                m_rejoinTruncationReqId = null;
                m_rejoining = false;
            }
            else {
                // If we saw some other truncation request ID, then try the same one again.  As long as we
                // don't flip the m_rejoining state, all truncation snapshot completions will call back to here.
                try {
                    final ZooKeeper zk = m_messenger.getZK();
                    if (m_rejoinTruncationReqId == null) {
                        m_rejoinTruncationReqId = java.util.UUID.randomUUID().toString();
                    }
                    zk.create(VoltZK.request_truncation_snapshot, m_rejoinTruncationReqId.getBytes(),
                            Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
                catch (KeeperException.NodeExistsException e) {}
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Unable to retry post-rejoin truncation snapshot request.", true, e);
                }
            }
        }
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
        if (m_nodeDRGateway != null) {
            m_nodeDRGateway.start();
            m_nodeDRGateway.bindPorts();
        }
    }

    @Override
    public void setReplicationActive(boolean active)
    {
        if (m_replicationActive != active) {
            m_replicationActive = active;

            try {
                JSONStringer js = new JSONStringer();
                js.object();
                // Replication role should the be same across the cluster
                js.key("role").value(getReplicationRole().ordinal());
                js.key("active").value(m_replicationActive);
                js.endObject();

                getHostMessenger().getZK().setData(VoltZK.replicationconfig,
                                                   js.toString().getBytes("UTF-8"),
                                                   -1);
            } catch (Exception e) {
                e.printStackTrace();
                hostLog.error("Failed to write replication active state to ZK: " +
                              e.getMessage());
            }

            if (m_nodeDRGateway != null) {
                m_nodeDRGateway.setActive(active);
            }
        }
    }

    @Override
    public boolean getReplicationActive()
    {
        return m_replicationActive;
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
    static String setupDefaultDeployment() throws IOException {

        // Since there's apparently no deployment to override the path to voltdbroot it should be
        // safe to assume it's under the working directory.
        // CatalogUtil.getVoltDbRoot() creates the voltdbroot directory as needed.
        File voltDbRoot = CatalogUtil.getVoltDbRoot(null);
        String pathToDeployment = voltDbRoot.getPath() + File.separator + "deployment.xml";
        File deploymentXMLFile = new File(pathToDeployment);

        hostLog.info("Generating default deployment file \"" + deploymentXMLFile.getAbsolutePath() + "\"");
        BufferedWriter bw = new BufferedWriter(new FileWriter(deploymentXMLFile));
        for (String line : defaultDeploymentXML) {
            bw.write(line);
            bw.newLine();
        }
        bw.flush();
        bw.close();

        return deploymentXMLFile.getAbsolutePath();
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
                            VoltDB.crashGlobalVoltDB("Local build string \"" + buildString +
                                    "\" does not match cluster build string \"" +
                                    new String(data, "UTF-8")  + "\"", false, null);
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
}
