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
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.zip.CRC32;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.apache.zookeeper_voltpatches.data.Stat;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.COWMap;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.compiler.AsyncCompilerAgent;
import org.voltdb.compiler.ClusterConfig;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.dtxn.DtxnInitiatorMailbox;
import org.voltdb.dtxn.ExecutorTxnIdSafetyState;
import org.voltdb.dtxn.MailboxPublisher;
import org.voltdb.dtxn.MailboxTracker;
import org.voltdb.dtxn.MailboxUpdateHandler;
import org.voltdb.dtxn.SimpleDtxnInitiator;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.export.ExportManager;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.fault.FaultDistributorInterface;
import org.voltdb.fault.SiteFailureFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.messaging.VoltDbMessageFactory;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.ResponseSampler;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.VoltSampler;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * RealVoltDB initializes global server components, like the messaging
 * layer, ExecutionSite(s), and ClientInterface. It provides accessors
 * or references to those global objects. It is basically the global
 * namespace. A lot of the global namespace is described by VoltDBInterface
 * to allow test mocking.
 */
public class RealVoltDB implements VoltDBInterface, RestoreAgent.Callback, MailboxUpdateHandler
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
    private volatile boolean m_validateConfiguredNumberOfPartitionsOnMailboxUpdate;
    private int m_configuredNumberOfPartitions;
    CatalogContext m_catalogContext;
    volatile SiteTracker m_siteTracker;
    MailboxPublisher m_mailboxPublisher;
    MailboxTracker m_mailboxTracker;
    private String m_buildString;
    private static final String m_defaultVersionString = "2.7";
    private String m_versionString = m_defaultVersionString;
    HostMessenger m_messenger = null;
    final ArrayList<ClientInterface> m_clientInterfaces = new ArrayList<ClientInterface>();
    final ArrayList<SimpleDtxnInitiator> m_dtxns = new ArrayList<SimpleDtxnInitiator>();
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
    int m_myHostId;
    long m_depCRC = -1;
    String m_serializedCatalog;
    String m_httpPortExtraLogMessage = null;
    boolean m_jsonEnabled;
    DeploymentType m_deployment;

    // Should the execution sites be started in recovery mode
    // (used for joining a node to an existing cluster)
    // If CL is enabled this will be set to true
    // by the CL when the truncation snapshot completes
    // and this node is viable for replay
    volatile boolean m_recovering = false;
    boolean m_replicationActive = false;

    //Only restrict recovery completion during test
    static Semaphore m_testBlockRecoveryCompletion = new Semaphore(Integer.MAX_VALUE);
    private long m_executionSiteRecoveryFinish;
    private long m_executionSiteRecoveryTransferred;

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

    RestoreAgent m_restoreAgent = null;

    private volatile boolean m_isRunning = false;

    @Override
    public boolean recovering() { return m_recovering; }

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

    HeartbeatThread heartbeatThread;
    private ScheduledThreadPoolExecutor m_periodicWorkThread;

    // The configured license api: use to decide enterprise/cvommunity edition feature enablement
    LicenseApi m_licenseApi;

    @Override
    public LicenseApi getLicenseApi() {
        return m_licenseApi;
    }


    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    @Override
    public void initialize(VoltDB.Configuration config) {
        synchronized(m_startAndStopLock) {
            consoleLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartupString.name(), null);

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
            m_dtxns.clear();
            m_adminListener = null;
            m_commandLog = new DummyCommandLog();
            m_deployment = null;
            m_messenger = null;
            m_startMode = null;
            m_statsAgent = new StatsAgent();
            m_asyncCompilerAgent = new AsyncCompilerAgent();
            m_faultManager = null;
            m_validateConfiguredNumberOfPartitionsOnMailboxUpdate = false;
            m_snapshotCompletionMonitor = null;
            m_catalogContext = null;
            m_partitionCountStats = null;
            m_ioStats = null;
            m_memoryStats = null;
            m_statsManager = null;
            m_restoreAgent = null;
            m_siteTracker = null;
            m_mailboxTracker = null;
            m_recoveryStartTime = System.currentTimeMillis();
            m_hostIdWithStartupCatalog = 0;
            m_pathToStartupCatalog = m_config.m_pathToCatalog;
            m_replicationActive = false;

            // set up site structure
            m_localSites = new COWMap<Long, ExecutionSite>();
            m_siteThreads = new HashMap<Long, Thread>();
            m_runners = new ArrayList<ExecutionSiteRunner>();

            m_computationService = MoreExecutors.listeningDecorator(
                    Executors.newFixedThreadPool(
                        Runtime.getRuntime().availableProcessors(),
                        new ThreadFactory() {
                            private int threadIndex = 0;
                            @Override
                            public synchronized Thread  newThread(Runnable r) {
                                Thread t = new Thread(null, r, "Computation service thread - " + threadIndex++, 131072);
                                t.setDaemon(true);
                                return t;
                            }

                        })
                    );

            // determine if this is a rejoining node
            // (used for license check and later the actual rejoin)
            boolean isRejoin = config.m_rejoinToHostAndPort != null;
            m_recovering = isRejoin;

            // Set std-out/err to use the UTF-8 encoding and fail if UTF-8 isn't supported
            try {
                System.setOut(new PrintStream(System.out, true, "UTF-8"));
                System.setErr(new PrintStream(System.err, true, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                hostLog.fatal("Support for the UTF-8 encoding is required for VoltDB. This means you are likely running an unsupported JVM. Exiting.");
                System.exit(-1);
            }

            // check that this is a 64 bit VM
            if (System.getProperty("java.vm.name").contains("64") == false) {
                hostLog.fatal("You are running on an unsupported (probably 32 bit) JVM. Exiting.");
                System.exit(-1);
            }

            m_snapshotCompletionMonitor = new SnapshotCompletionMonitor();

            readBuildInfo(config.m_isEnterprise ? "Enterprise Edition" : "Community Edition");

            // start up the response sampler if asked to by setting the env var
            // VOLTDB_RESPONSE_SAMPLE_PATH to a valid path
            ResponseSampler.initializeIfEnabled();

            buildClusterMesh(isRejoin);
            m_mailboxPublisher = new MailboxPublisher(VoltZK.mailboxes + "/" + m_messenger.getHostId());
            final int numberOfNodes = readDeploymentAndCreateStarterCatalogContext();
            if (!isRejoin) {
                m_messenger.waitForGroupJoin(numberOfNodes);
            }

            m_faultManager = new FaultDistributor(this);
            m_faultManager.registerFaultHandler(SiteFailureFault.SITE_FAILURE_CATALOG,
                    m_faultHandler,
                    FaultType.SITE_FAILURE);
            if (!m_faultManager.testPartitionDetectionDirectory(
                    m_catalogContext.cluster.getFaultsnapshots().get("CLUSTER_PARTITION"))) {
                VoltDB.crashLocalVoltDB("Unalbe to create partition detection snapshot directory at" +
                        m_catalogContext.cluster.getFaultsnapshots().get("CLUSTER_PARTITION"), false, null);
            }


            // Create the thread pool here. It's needed by buildClusterMesh()
            final int availableProcessors = Runtime.getRuntime().availableProcessors();
            int poolSize = 1;
            if (availableProcessors > 4) {
                poolSize = 2;
            }
            m_periodicWorkThread = CoreUtils.getScheduledThreadPoolExecutor("Periodic Work", poolSize, 1024 * 128);

            m_licenseApi = MiscUtils.licenseApiFactory(m_config.m_pathToLicense);
            if (m_licenseApi == null) {
                VoltDB.crashLocalVoltDB("Failed to initialize license verifier. " +
                        "See previous log message for details.", false, null);
            }

            /*
             * Construct all the mailboxes for things that need to be globally addressable so they can be published
             * in one atomic shot.
             *
             * The starting state for partition assignments are statically derived from the host id generated
             * by host messenger and the k-factor/host count/sites per host. This starting state
             * is published to ZK as the toplogy metadata node.
             *
             * On rejoin the rejoining node has to inspect the topology meta node to find out what is missing
             * and then update the topology listing itself as a replacement for one of the missing host ids.
             * Then it does a compare and set of the topology.
             */
            ArrayDeque<Mailbox> siteMailboxes = null;
            ClusterConfig clusterConfig = null;
            DtxnInitiatorMailbox initiatorMailbox = null;
            long initiatorHSId = 0;
            try {
                /*
                 * Start mailbox tracker early here because it is required
                 * on rejoin to find the hosts that are missing from the cluster
                 */
                m_mailboxTracker = new MailboxTracker(m_messenger.getZK(), this);
                m_mailboxTracker.start();

                /*
                 * Will count this down at the right point on regular startup as well as rejoin
                 */
                CountDownLatch rejoinCompleteLatch = new CountDownLatch(1);
                Pair<ArrayDeque<Mailbox>, ClusterConfig> p;
                if (isRejoin) {
                    /*
                     * Need to lock the topology metadata
                     * so that it can be changed atomically with publishing the mailbox node
                     * for this process on a rejoin.
                     */
                    createRejoinBarrierAndWatchdog(rejoinCompleteLatch);

                    p = createMailboxesForSitesRejoin();
                    ExecutionSite.recoveringSiteCount.set(p.getFirst().size());
                    hostLog.info("Set recovering site count to " + p.getFirst().size());
                } else {
                    p = createMailboxesForSitesStartup();
                }

                siteMailboxes = p.getFirst();
                clusterConfig = p.getSecond();
                // This will set up site tracker
                initiatorHSId = registerInitiatorMailbox();
                final long statsHSId = m_messenger.getHSIdForLocalSite(HostMessenger.STATS_SITE_ID);
                m_messenger.generateMailboxId(statsHSId);
                hostLog.info("Registering stats mailbox id " + CoreUtils.hsIdToString(statsHSId));
                m_mailboxPublisher.registerMailbox(MailboxType.StatsAgent, new MailboxNodeContent(statsHSId, null));

                m_mailboxPublisher.publish(m_messenger.getZK());

                /*
                 * Now that we have published our changes to the toplogy it is safe for
                 * another node to come in and manipulate the toplogy metadata
                 */
                rejoinCompleteLatch.countDown();
                if (isRejoin) {
                    m_messenger.getZK().delete(VoltZK.rejoinLock, -1, new ZKUtil.VoidCallback(), null);
                }
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            /*
             * Before this barrier pretty much every remotely visible mailbox id has to have been
             * registered with host messenger and published with mailbox publisher
             */
            boolean siteTrackerInit = false;
            for (int ii = 0; ii < 4000; ii++) {
                boolean predicate = true;
                if (isRejoin) {
                    predicate = !m_siteTracker.getAllHosts().contains(m_messenger.getHostId());
                } else {
                    predicate = m_siteTracker.getAllHosts().size() < m_deployment.getCluster().getHostcount();
                }
                if (predicate) {
                    try {
                        Thread.sleep(5);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    siteTrackerInit = true;
                    break;
                }
            }
            if (!siteTrackerInit) {
                VoltDB.crashLocalVoltDB(
                        "Failed to initialize site tracker with all hosts before timeout", true, null);
            }

            initiatorMailbox = createInitiatorMailbox(initiatorHSId);


            // do the many init tasks in the Inits class
            Inits inits = new Inits(this, 1);
            inits.doInitializationWork();

            if (config.m_backend.isIPC) {
                int eeCount = m_siteTracker.getLocalSites().length;
                if (config.m_ipcPorts.size() != eeCount) {
                    hostLog.fatal("Specified an IPC backend but only supplied " + config.m_ipcPorts.size() +
                            " backend ports when " + eeCount + " are required");
                    System.exit(-1);
                }
            }

            collectLocalNetworkMetadata();

            /*
             * Create execution sites runners (and threads) for all exec sites except the first one.
             * This allows the sites to be set up in the thread that will end up running them.
             * Cache the first Site from the catalog and only do the setup once the other threads have been started.
             */
            Mailbox localThreadMailbox = siteMailboxes.poll();
            ((org.voltcore.messaging.SiteMailbox)localThreadMailbox).setCommandLog(m_commandLog);
            m_currentThreadSite = null;
            for (Mailbox mailbox : siteMailboxes) {
                long site = mailbox.getHSId();
                int sitesHostId = SiteTracker.getHostForSite(site);

                // start a local site
                if (sitesHostId == m_myHostId) {
                    ((org.voltcore.messaging.SiteMailbox)mailbox).setCommandLog(m_commandLog);
                    ExecutionSiteRunner runner =
                        new ExecutionSiteRunner(mailbox,
                                m_catalogContext,
                                m_serializedCatalog,
                                m_recovering,
                                m_replicationActive,
                                hostLog,
                                m_configuredNumberOfPartitions);
                    m_runners.add(runner);
                    Thread runnerThread = new Thread(runner, "Site " +
                            org.voltcore.utils.CoreUtils.hsIdToString(site));
                    runnerThread.start();
                    log.l7dlog(Level.TRACE, LogKeys.org_voltdb_VoltDB_CreatingThreadForSite.name(), new Object[] { site }, null);
                    m_siteThreads.put(site, runnerThread);
                }
            }

            /*
             * Now that the runners have been started and are doing setup of the other sites in parallel
             * this thread can set up its own execution site.
             */
            try {
                ExecutionSite siteObj =
                    new ExecutionSite(VoltDB.instance(),
                            localThreadMailbox,
                            m_serializedCatalog,
                            null,
                            m_recovering,
                            m_replicationActive,
                            m_catalogContext.m_transactionId,
                            m_configuredNumberOfPartitions);
                m_localSites.put(localThreadMailbox.getHSId(), siteObj);
                m_currentThreadSite = siteObj;
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            /*
             * Stop and wait for the runners to finish setting up and then put
             * the constructed ExecutionSites in the local site map.
             */
            for (ExecutionSiteRunner runner : m_runners) {
                try {
                    runner.m_siteIsLoaded.await();
                } catch (InterruptedException e) {
                    VoltDB.crashLocalVoltDB("Unable to wait on starting execution site.", true, e);
                }
                assert(runner.m_siteObj != null);
                m_localSites.put(runner.m_siteId, runner.m_siteObj);
            }

            /*
             * At this point all of the execution sites have been published to m_localSites
             * It is possible that while they were being created the mailbox tracker found additional
             * sites, but was unable to deliver the notification to some or all of the execution sites.
             * Since notifying them of new sites is idempotent (version number check), let's do that here so there
             * are no lost updates for additional sites. But... it must be done from the
             * mailbox tracker thread or there is a race with failure detection and handling.
             * Generally speaking it seems like retrieving a reference to a site tracker not via a message
             * from the mailbox tracker thread that builds the site tracker is bug. If it isn't delivered to you by
             * a site tracker then you lose sequential consistency.
             */
            try {
                m_mailboxTracker.executeTask(new Runnable() {
                    @Override
                    public void run() {
                        for (ExecutionSite es : m_localSites.values()) {
                            es.notifySitesAdded(m_siteTracker);
                        }
                    }
                }).get();
            } catch (InterruptedException e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            } catch (ExecutionException e) {
                VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
            }

            // Create the client interface
            int portOffset = 0;
            // TODO: fix
            //for (long site : m_siteTracker.getMailboxTracker().getAllInitiators()) {
            for (int i = 0; i < 1; i++) {
                // create DTXN and CI for each local non-EE site
                SimpleDtxnInitiator initiator =
                    new SimpleDtxnInitiator(initiatorMailbox,
                            m_catalogContext,
                            m_messenger,
                            m_myHostId,
                            m_myHostId, // fake initiator ID
                            m_config.m_timestampTestingSalt);

                try {
                    ClientInterface ci =
                        ClientInterface.create(m_messenger,
                                m_catalogContext,
                                m_config.m_replicationRole,
                                initiator,
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
                m_dtxns.add(initiator);
            }

            m_partitionCountStats = new PartitionCountStats( clusterConfig.getPartitionCount());
            m_statsAgent.registerStatsSource(SysProcSelector.PARTITIONCOUNT,
                    0, m_partitionCountStats);
            m_ioStats = new IOStats();
            m_statsAgent.registerStatsSource(SysProcSelector.IOSTATS,
                    0, m_ioStats);
            m_memoryStats = new MemoryStats();
            m_statsAgent.registerStatsSource(SysProcSelector.MEMORY,
                    0, m_memoryStats);
            // Create the statistics manager and register it to JMX registry
            m_statsManager = null;
            try {
                final Class<?> statsManagerClass =
                    Class.forName("org.voltdb.management.JMXStatsManager");
                m_statsManager = (StatsManager)statsManagerClass.newInstance();
                m_statsManager.initialize(new ArrayList<Long>(m_localSites.keySet()));
            } catch (Exception e) {}

            try {
                m_snapshotCompletionMonitor.init(m_messenger.getZK());
            } catch (Exception e) {
                hostLog.fatal("Error initializing snapshot completion monitor", e);
                VoltDB.crashLocalVoltDB("Error initializing snapshot completion monitor", true, e);
            }

            if (m_commandLog != null && isRejoin) {
                m_commandLog.initForRejoin(
                        m_catalogContext, Long.MIN_VALUE, true);
            }

            if (!isRejoin) {
                try {
                    m_messenger.waitForAllHostsToBeReady(m_deployment.getCluster().getHostcount());
                } catch (Exception e) {
                    hostLog.fatal("Failed to announce ready state.");
                    VoltDB.crashLocalVoltDB("Failed to announce ready state.", false, null);
                }
            }
            m_validateConfiguredNumberOfPartitionsOnMailboxUpdate = true;
            if (m_siteTracker.m_numberOfPartitions != m_configuredNumberOfPartitions) {
                for (Map.Entry<Integer, ImmutableList<Long>> entry :
                    m_siteTracker.m_partitionsToSitesImmutable.entrySet()) {
                    hostLog.info(entry.getKey() + " -- "
                            + CoreUtils.hsIdCollectionToString(entry.getValue()));
                }
                VoltDB.crashGlobalVoltDB("Mismatch between configured number of partitions (" +
                        m_configuredNumberOfPartitions + ") and actual (" +
                        m_siteTracker.m_numberOfPartitions + ")",
                        true, null);
            }

            heartbeatThread = new HeartbeatThread(m_clientInterfaces);
            heartbeatThread.start();
            schedulePeriodicWorks();

            // print out a bunch of useful system info
            logDebuggingInfo(m_config.m_adminPort, m_config.m_httpPort, m_httpPortExtraLogMessage, m_jsonEnabled);

            if (clusterConfig.getReplicationFactor() == 0) {
                hostLog.warn("Running without redundancy (k=0) is not recommended for production use.");
            }

            assert(m_clientInterfaces.size() > 0);
            ClientInterface ci = m_clientInterfaces.get(0);
            ci.initializeSnapshotDaemon(m_messenger.getZK());

            // set additional restore agent stuff
            TransactionInitiator initiator = m_dtxns.get(0);
            if (m_restoreAgent != null) {
                m_restoreAgent.setCatalogContext(m_catalogContext);
                m_restoreAgent.setSiteTracker(m_siteTracker);
                m_restoreAgent.setInitiator(initiator);
            }
        }
    }

    private void createRejoinBarrierAndWatchdog(final CountDownLatch cdl) {
        ZooKeeper zk = m_messenger.getZK();
        String lockPath = null;
        for (int ii = 0; ii < 120; ii++) {
            try {
                lockPath = zk.create(VoltZK.rejoinLock, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                break;
            } catch (KeeperException.NodeExistsException e) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        if (lockPath == null) {
            VoltDB.crashLocalVoltDB("Unable to acquire rejoin lock in ZK, " +
                    "it may be necessary to delete the lock from the ZK CLI if " +
                    "you are sure no other rejoin is in progress", false, null);
        }

        new Thread() {
            @Override
            public void run() {
                try {
                    if (!cdl.await(1, TimeUnit.MINUTES)) {
                        VoltDB.crashLocalVoltDB("Rejoin watchdog timed out after 60 seconds, rejoin hung", false, null);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    private Pair<ArrayDeque<Mailbox>, ClusterConfig> createMailboxesForSitesRejoin() throws Exception {
        ZooKeeper zk = m_messenger.getZK();
        ArrayDeque<Mailbox> mailboxes = new ArrayDeque<Mailbox>();

        Stat stat = new Stat();
        hostLog.debug(zk.getChildren("/db", false));
        JSONObject topology = new JSONObject(new String(zk.getData(VoltZK.topology, false, stat), "UTF-8"));

        // We're waiting for m_mailboxTracker to start(), which will
        // cause it to do an initial read of the mailboxes from ZK and
        // then create a new and up-to-date m_siteTracker when handleMailboxUpdate()
        // gets called
        while (m_siteTracker == null) {
            Thread.sleep(1);
        }

        Set<Integer> hostIdsInTopology = new HashSet<Integer>();
        JSONArray partitions = topology.getJSONArray("partitions");
        for (int ii = 0; ii < partitions.length(); ii++) {
            JSONObject partition = partitions.getJSONObject(ii);
            JSONArray replicas = partition.getJSONArray("replicas");
            for (int zz = 0; zz < replicas.length(); zz++) {
                hostIdsInTopology.add(replicas.getInt(zz));
            }
        }

        /*
         * Remove all the hosts that are still live
         */
        hostIdsInTopology.removeAll(m_siteTracker.m_allHostsImmutable);

        /*
         * Nothing to replace!
         */
        if (hostIdsInTopology.isEmpty()) {
            VoltDB.crashLocalVoltDB("Rejoin failed because there is no failed node to replace", false, null);
        }

        Integer hostToReplace = hostIdsInTopology.iterator().next();

        /*
         * Log all the partitions to replicate and replace the failed host with self
         */
        Set<Integer> partitionsToReplicate = new TreeSet<Integer>();
        for (int ii = 0; ii < partitions.length(); ii++) {
            JSONObject partition = partitions.getJSONObject(ii);
            JSONArray replicas = partition.getJSONArray("replicas");
            for (int zz = 0; zz < replicas.length(); zz++) {
                if (replicas.getInt(zz) == hostToReplace.intValue()) {
                    partitionsToReplicate.add(partition.getInt("partition_id"));
                    replicas.put(zz, m_messenger.getHostId());
                }
            }
        }

        zk.setData(VoltZK.topology, topology.toString(4).getBytes("UTF-8"), -1, new ZKUtil.StatCallback(), null);
        final int hostcount = topology.getInt("hostcount");
        final int sites_per_host = topology.getInt("sites_per_host");
        final int kfactor = topology.getInt("kfactor");
        ClusterConfig clusterConfig =
            new ClusterConfig(
                    hostcount,
                    sites_per_host,
                    kfactor);
        m_configuredNumberOfPartitions = clusterConfig.getPartitionCount();
        assert(partitionsToReplicate.size() == sites_per_host);
        for (Integer partition : partitionsToReplicate)
        {
            Mailbox mailbox = m_messenger.createMailbox();
            mailboxes.add(mailbox);
            MailboxNodeContent mnc = new MailboxNodeContent(mailbox.getHSId(), partition);
            m_mailboxPublisher.registerMailbox(MailboxType.ExecutionSite, mnc);
        }
        return Pair.of( mailboxes, clusterConfig);
    }

    private Pair<ArrayDeque<Mailbox>, ClusterConfig> createMailboxesForSitesStartup() throws Exception {
        ArrayDeque<Mailbox> mailboxes = new ArrayDeque<Mailbox>();
        int sitesperhost = m_deployment.getCluster().getSitesperhost();
        int hostcount = m_deployment.getCluster().getHostcount();
        int kfactor = m_deployment.getCluster().getKfactor();
        ClusterConfig clusterConfig = new ClusterConfig(hostcount, sitesperhost, kfactor);
        if (!clusterConfig.validate()) {
            VoltDB.crashLocalVoltDB(clusterConfig.getErrorMsg(), false, null);
        }
        JSONObject topo = registerClusterConfig(clusterConfig);
        List<Integer> partitions =
            ClusterConfig.partitionsForHost(topo, m_messenger.getHostId());

        m_configuredNumberOfPartitions = clusterConfig.getPartitionCount();
        assert(partitions.size() == sitesperhost);
        for (Integer partition : partitions)
        {
            Mailbox mailbox = m_messenger.createMailbox();
            mailboxes.add(mailbox);
            MailboxNodeContent mnc = new MailboxNodeContent(mailbox.getHSId(), partition);
            m_mailboxPublisher.registerMailbox(MailboxType.ExecutionSite, mnc);
        }
        return Pair.of( mailboxes, clusterConfig);
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

    /*
     * First register, then create.
     */
    private long registerInitiatorMailbox() throws Exception {
        long hsid = m_messenger.generateMailboxId(null);
        MailboxNodeContent mnc = new MailboxNodeContent(hsid, null);
        m_mailboxPublisher.registerMailbox(MailboxType.Initiator, mnc);
        return hsid;
    }

    private DtxnInitiatorMailbox createInitiatorMailbox(long hsid) {
        Map<Long, Integer> siteMap = m_siteTracker.getSitesToPartitions();
        ExecutorTxnIdSafetyState safetyState = new ExecutorTxnIdSafetyState(siteMap);
        DtxnInitiatorMailbox mailbox = new DtxnInitiatorMailbox(safetyState, m_messenger);
        mailbox.setHSId(hsid);
        m_messenger.registerMailbox(mailbox);
        return mailbox;
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
                CRC32 crc = new CRC32();
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

            // note the heatbeats are specified in seconds in xml, but ms internally
            HeartbeatType hbt = m_deployment.getHeartbeat();
            if (hbt != null)
                m_config.m_deadHostTimeoutMS = hbt.getTimeout() * 1000;

            // create a dummy catalog to load deployment info into
            Catalog catalog = new Catalog();
            Cluster cluster = catalog.getClusters().add("cluster");
            Database db = cluster.getDatabases().add("database");

            // create groups as needed for users
            if (m_deployment.getUsers() != null) {
                for (UsersType.User user : m_deployment.getUsers().getUser()) {
                    String groupsCSV = user.getGroups();
                    if (groupsCSV == null || groupsCSV.isEmpty()) {
                        continue;
                    }
                    String[] groups = groupsCSV.split(",");
                    for (String group : groups) {
                        if (db.getGroups().get(group) == null) {
                            db.getGroups().add(group);
                        }
                    }
                }
            }

            long depCRC = CatalogUtil.compileDeploymentAndGetCRC(catalog, m_deployment,
                    true, true);
            assert(depCRC != -1);
            m_catalogContext = new CatalogContext(0, catalog, null, depCRC, 0, -1);

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

        org.voltcore.messaging.HostMessenger.Config hmconfig;

        if (m_config.m_rejoinToHostAndPort != null) {
            hmconfig = new org.voltcore.messaging.HostMessenger.Config(
                    MiscUtils.getHostnameFromHostnameColonPort(m_config.m_rejoinToHostAndPort),
                    MiscUtils.getPortFromHostnameColonPort(
                            m_config.m_rejoinToHostAndPort, m_config.m_internalPort));
        } else {
            hmconfig = new org.voltcore.messaging.HostMessenger.Config(
                    leaderAddress,
                    m_config.m_leaderPort != null ? m_config.m_leaderPort : m_config.m_internalPort);
        }
        hmconfig.internalPort = m_config.m_internalPort;
        hmconfig.internalInterface = m_config.m_internalInterface;
        hmconfig.zkInterface = m_config.m_zkInterface;
        hmconfig.deadHostTimeout = m_config.m_deadHostTimeoutMS;
        hmconfig.factory = new VoltDbMessageFactory();

        m_messenger =
            new org.voltcore.messaging.HostMessenger(hmconfig);

        hostLog.info(String.format("Beginning inter-node communication on port %d.", m_config.m_internalPort));

        try {
            m_messenger.start();
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }

        VoltZK.createPersistentZKNodes(m_messenger.getZK());

        // Use the host messenger's hostId.
        m_myHostId = m_messenger.getHostId();
    }

    void logDebuggingInfo(int adminPort, int httpPort, String httpPortExtraLogMessage, boolean jsonEnabled) {
        String startAction = m_config.m_startAction.toString();
        String startActionLog = "Database start action is " + (startAction.substring(0, 1).toUpperCase() +
                startAction.substring(1).toLowerCase()) + ".";
        if (m_config.m_startAction == START_ACTION.START) {
            startActionLog += " Will create a new database if there is nothing to recover from.";
        }
        hostLog.info(startActionLog);

        // print out awesome network stuff
        hostLog.info(String.format("Listening for native wire protocol clients on port %d.", m_config.m_port));
        hostLog.info(String.format("Listening for admin wire protocol clients on port %d.", adminPort));

        if (m_startMode == OperationMode.PAUSED) {
            hostLog.info(String.format("Started in admin mode. Clients on port %d will be rejected in admin mode.", m_config.m_port));
        }

        if (m_config.m_replicationRole == ReplicationRole.REPLICA) {
            hostLog.info("Started as " + m_config.m_replicationRole.toString().toLowerCase() + " cluster. " +
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
        StringBuilder sb = new StringBuilder("Available JVM arguments:");
        for (String iarg : iargs)
            sb.append(" ").append(iarg);
        if (iargs.size() > 0) hostLog.info(sb.toString());
        else hostLog.info("No JVM command line args known.");

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
        HashSet<Integer> metadataToRetrieve = new HashSet<Integer>(m_siteTracker.getAllHosts());
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
        for (int hostId : m_siteTracker.getAllHosts()) {
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
                while ((b = (byte) buildstringStream.read()) != -1) {
                    sb.append((char)b);
                }
                versionString = sb.toString().trim();
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
        // start the separate EE threads
        for (ExecutionSiteRunner r : m_runners) {
            r.m_shouldStartRunning.countDown();
        }

        if (m_restoreAgent != null) {
            // start restore process
            m_restoreAgent.restore();
        }
        else {
            onRestoreCompletion(Long.MIN_VALUE);
        }

        // start one site in the current thread
        Thread.currentThread().setName("ExecutionSiteAndVoltDB");
        m_isRunning = true;
        try
        {
            m_currentThreadSite.run();
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
                m_mailboxTracker.shutdown();
                // Things are going pear-shaped, tell the fault distributor to
                // shut its fat mouth
                m_faultManager.shutDown();
                m_snapshotCompletionMonitor.shutdown();
                m_periodicWorkThread.shutdown();
                heartbeatThread.interrupt();
                heartbeatThread.join();

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

                // shut down Export and its connectors.
                ExportManager.instance().shutdown();

                // tell all m_sites to stop their runloops
                if (m_localSites != null) {
                    for (ExecutionSite site : m_localSites.values())
                        site.startShutdown();
                }

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

                // After sites are terminated, shutdown the InvocationBufferServer.
                // The IBS is shared by all sites; don't kill it while any site is active.
                PartitionDRGateway.shutdown();

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
                m_siteTracker = null;
                m_catalogContext = null;
                m_mailboxPublisher = null;

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
        ContextTracker(CatalogContext context) {
            m_dispensedSites = 1;
            m_context = context;
        }
        long m_dispensedSites;
        CatalogContext m_context;
    }

    /** Associate transaction ids to contexts */
    private final HashMap<Long, ContextTracker>m_txnIdToContextTracker =
        new HashMap<Long, ContextTracker>();

    @Override
    public CatalogContext catalogUpdate(
            String diffCommands,
            byte[] newCatalogBytes,
            int expectedCatalogVersion,
            long currentTxnId,
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
                int ttlsites = m_siteTracker.getSitesForHost(m_messenger.getHostId()).size();
                if (contextTracker.m_dispensedSites == ttlsites) {
                    m_txnIdToContextTracker.remove(currentTxnId);
                }
                return contextTracker.m_context;
            }
            else if (m_catalogContext.catalogVersion != expectedCatalogVersion) {
                throw new RuntimeException("Trying to update main catalog context with diff " +
                        "commands generated for an out-of date catalog. Expected catalog version: " +
                        expectedCatalogVersion + " does not match actual version: " + m_catalogContext.catalogVersion);
            }

            // 0. A new catalog! Update the global context and the context tracker
            m_catalogContext =
                m_catalogContext.update(currentTxnId, newCatalogBytes, diffCommands, true, deploymentCRC);
            m_txnIdToContextTracker.put(currentTxnId, new ContextTracker(m_catalogContext));
            m_catalogContext.logDebuggingInfoFromCatalog();

            // 1. update the export manager.
            ExportManager.instance().updateCatalog(m_catalogContext);

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

            return m_catalogContext;
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
    public synchronized void onExecutionSiteRecoveryCompletion(long transferred) {
        m_executionSiteRecoveryFinish = System.currentTimeMillis();
        m_executionSiteRecoveryTransferred = transferred;
        onRecoveryCompletion();
    }

    private void onRecoveryCompletion() {
        try {
            m_testBlockRecoveryCompletion.acquire();
        } catch (InterruptedException e) {}
        final long delta = ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000);
        final long megabytes = m_executionSiteRecoveryTransferred / (1024 * 1024);
        final double megabytesPerSecond = megabytes / ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000.0);
        for (ClientInterface intf : getClientInterfaces()) {
            intf.mayActivateSnapshotDaemon();
        }
        consoleLog.info(
                "Node data recovery completed after " + delta + " seconds with " + megabytes +
                " megabytes transferred at a rate of " +
                megabytesPerSecond + " megabytes/sec");
        try {
            final ZooKeeper zk = m_messenger.getZK();
            boolean logRecoveryCompleted = false;
            if (getCommandLog().getClass().getName().equals("org.voltdb.CommandLogImpl")) {
                try {
                    zk.create(VoltZK.request_truncation_snapshot, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {}
            } else {
                logRecoveryCompleted = true;
            }
            if (logRecoveryCompleted) {
                m_recovering = false;
                consoleLog.info("Node recovery completed");
            }
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Unable to log host recovery completion to ZK", true, e);
        }
        hostLog.info("Logging host recovery completion to ZK");
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
    public void onRestoreCompletion(long txnId) {

        /*
         * Command log is already initialized if this is a rejoin
         */
        if ((m_commandLog != null) && (m_commandLog.needsInitialization())) {
            // Initialize command logger
            m_commandLog.init(m_catalogContext, txnId);
        }

        /*
         * Enable the initiator to send normal heartbeats and accept client
         * connections
         */
        for (SimpleDtxnInitiator dtxn : m_dtxns) {
            dtxn.setSendHeartbeats(true);
        }

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
    public synchronized void recoveryComplete() {
        m_recovering = false;
        consoleLog.info("Node recovery completed");
    }

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
        if (m_localSites != null && !m_localSites.isEmpty()) {
            // get any site and start the DR server, it's static
            ExecutionSite site = m_localSites.values().iterator().next();
            site.getPartitionDRGateway().start();
        }
    }

    @Override
    public void setReplicationActive(boolean active)
    {
        if (m_replicationActive != active) {
            m_replicationActive = active;
            if (m_localSites != null) {
                for (ExecutionSite s : m_localSites.values()) {
                    s.getPartitionDRGateway().setActive(active);
                }
            }
        }
    }

    @Override
    public boolean getReplicationActive()
    {
        return m_replicationActive;
    }

    @Override
    public void handleMailboxUpdate(Map<MailboxType, List<MailboxNodeContent>> mailboxes) {
        SiteTracker oldTracker = m_siteTracker;
        m_siteTracker = new SiteTracker(m_myHostId, mailboxes, oldTracker != null ? oldTracker.m_version + 1 : 0);

        if (m_validateConfiguredNumberOfPartitionsOnMailboxUpdate) {
            if (m_siteTracker.m_numberOfPartitions != m_configuredNumberOfPartitions) {
                VoltDB.crashGlobalVoltDB(
                        "Configured number of partitions " + m_configuredNumberOfPartitions +
                        " is not the same as the number of partitions present " + m_siteTracker.m_numberOfPartitions,
                        true, null);
            }
            if (m_siteTracker.m_numberOfPartitions != oldTracker.m_numberOfPartitions) {
                VoltDB.crashGlobalVoltDB(
                        "Configured number of partitions in new tracker" + m_siteTracker.m_numberOfPartitions +
                        " is not the same as the number of partitions present " + oldTracker.m_numberOfPartitions,
                        true, null);
            }
        }

        if (oldTracker != null) {
            /*
             * Handle node failures first, then node additions. It is NOT
             * guaranteed that if a node failure and a node addition happen
             * concurrently, they'll appear separately in two watch fires,
             * because the new tracker contains the most up-to-date view of the
             * mailboxes, which may contain both changes. Consequently, we have
             * to handle both cases here.
             */
            HashSet<Long> deltaRemoved = new HashSet<Long>(oldTracker.m_allSitesImmutable);
            deltaRemoved.removeAll(m_siteTracker.m_allSitesImmutable);
            if (!deltaRemoved.isEmpty()) {
                m_faultManager.reportFault(new SiteFailureFault(new ArrayList<Long>(deltaRemoved)));
            }

            HashSet<Long> deltaAdded = new HashSet<Long>(m_siteTracker.m_allSitesImmutable);
            deltaAdded.removeAll(oldTracker.m_allSitesImmutable);
            if (!deltaAdded.isEmpty()) {
                for (SimpleDtxnInitiator dtxn : m_dtxns)
                {
                    Set<Long> copy = new HashSet<Long>(m_siteTracker.m_allExecutionSitesImmutable);
                    copy.retainAll(deltaAdded);
                    dtxn.notifyExecutionSiteRejoin(new ArrayList<Long>(copy));
                }
                for (ExecutionSite es : getLocalSites().values()) {
                    es.notifySitesAdded(m_siteTracker);
                }

                if (ExportManager.instance() != null) {
                    //Notify the export manager the cluster topology has changed
                    ExportManager.instance().notifyOfClusterTopologyChange();
                }
            }
        }
    }

    @Override
    public SiteTracker getSiteTracker() {
        return m_siteTracker;
    }

    @Override
    public MailboxPublisher getMailboxPublisher() {
        return m_mailboxPublisher;
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
}
