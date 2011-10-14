/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.agreement.AgreementSite;
import org.voltdb.agreement.ZKUtil;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.AsyncCompilerWorkThread;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.UsersType;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.export.ExportManager;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.fault.FaultDistributorInterface;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.messaging.Messenger;
import org.voltdb.network.VoltNetwork;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.ResponseSampler;
import org.voltdb.utils.SystemStatsCollector;
import org.voltdb.utils.VoltSampler;

public class RealVoltDB implements VoltDBInterface, RestoreAgent.Callback
{
    private static final VoltLogger log = new VoltLogger(VoltDB.class.getName());
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger recoveryLog = new VoltLogger("RECOVERY");

    static class RejoinCallback implements ProcedureCallback {
        ClientResponse response;

        @Override
        public synchronized void clientCallback(ClientResponse clientResponse)
                throws Exception {
            response = clientResponse;
            if (response.getStatus() != ClientResponse.SUCCESS) {
                hostLog.fatal(response.getStatusString());
                VoltDB.crashVoltDB();
            }
            VoltTable results[] = clientResponse.getResults();
            if (results.length > 0) {
                VoltTable errors = results[0];
                while (errors.advanceRow()) {
                    hostLog.fatal("Host " + errors.getLong(0) + " error: " + errors.getString(1));
                }
                VoltDB.crashVoltDB();
            }
            this.notify();
        }

        public synchronized ClientResponse waitForResponse(int timeout) throws InterruptedException {
            final long start = System.currentTimeMillis();
            while (response == null) {
                this.wait(timeout);
                long finish = System.currentTimeMillis();
                if (finish - start >= timeout) {
                    return null;
                }
            }
            return response;
        }
    }


    public VoltDB.Configuration m_config = new VoltDB.Configuration();
    CatalogContext m_catalogContext;
    private String m_buildString;
    private static final String m_defaultVersionString = "2.1";
    private String m_versionString = m_defaultVersionString;
    // fields accessed via the singleton
    HostMessenger m_messenger = null;
    final ArrayList<ClientInterface> m_clientInterfaces =
        new ArrayList<ClientInterface>();
    private Map<Integer, ExecutionSite> m_localSites;
    VoltNetwork m_network = null;
    AgreementSite m_agreementSite;
    HTTPAdminListener m_adminListener;
    private Map<Integer, Thread> m_siteThreads;
    private ArrayList<ExecutionSiteRunner> m_runners;
    private ExecutionSite m_currentThreadSite;
    private StatsAgent m_statsAgent = new StatsAgent();
    FaultDistributor m_faultManager;
    Object m_instanceId[];
    private PartitionCountStats m_partitionCountStats = null;
    private IOStats m_ioStats = null;
    private MemoryStats m_memoryStats = null;
    private StatsManager m_statsManager = null;
    ZooKeeper m_zk;
    private SnapshotCompletionMonitor m_snapshotCompletionMonitor;
    int m_myHostId;
    long m_depCRC = -1;
    String m_serializedCatalog;
    String m_httpPortExtraLogMessage = null;
    boolean m_jsonEnabled;
    CountDownLatch m_hasCatalog;
    AsyncCompilerWorkThread m_compilerThread = null;

    DeploymentType m_deployment;

    final HashSet<Integer> m_downHosts = new HashSet<Integer>();
    final Set<Integer> m_downNonExecSites = new HashSet<Integer>();
    //For command log only, will also mark self as faulted
    final Set<Integer> m_downSites = new HashSet<Integer>();

    // Should the execution sites be started in recovery mode
    // (used for joining a node to an existing cluster)
    // If CL is enabled this will be set to true
    // by the CL when the truncation snapshot completes
    // and this node is viable for replay
    volatile boolean m_recovering = false;

    //Only restrict recovery completion during test
    static Semaphore m_testBlockRecoveryCompletion = new Semaphore(Integer.MAX_VALUE);
    private boolean m_executionSitesRecovered = false;
    private boolean m_agreementSiteRecovered = false;
    private long m_executionSiteRecoveryFinish;
    private long m_executionSiteRecoveryTransferred;

    // the id of the host that is the leader, or the restore planner
    // says has the catalog
    int m_hostIdWithStartupCatalog;
    String m_pathToStartupCatalog;

    // Synchronize initialize and shutdown.
    private final Object m_startAndStopLock = new Object();

    // Synchronize updates of catalog contexts with context accessors.
    private final Object m_catalogUpdateLock = new Object();

    // add a random number to the sampler output to make it likely to be unique for this process.
    private final VoltSampler m_sampler = new VoltSampler(10, "sample" + String.valueOf(new Random().nextInt() % 10000) + ".txt");
    private final AtomicBoolean m_hasStartedSampler = new AtomicBoolean(false);

    final VoltDBNodeFailureFaultHandler m_faultHandler = new VoltDBNodeFailureFaultHandler(this);

    RestoreAgent m_restoreAgent = null;

    private volatile boolean m_isRunning = false;

    @Override
    public boolean recovering() { return m_recovering; }

    private long m_recoveryStartTime = System.currentTimeMillis();

    CommandLog m_commandLog;

    private volatile OperationMode m_mode = OperationMode.INITIALIZING;
    OperationMode m_startMode = null;

    // metadata is currently of the format:
    // IP:CIENTPORT:ADMINPORT:HTTPPORT
    volatile String m_localMetadata = "0.0.0.0:0:0:0";
    final Map<Integer, String> m_clusterMetadata = Collections.synchronizedMap(new HashMap<Integer, String>());

    // methods accessed via the singleton
    @Override
    public void startSampler() {
        if (m_hasStartedSampler.compareAndSet(false, true)) {
            m_sampler.start();
        }
    }

    HeartbeatThread heartbeatThread;
    private ScheduledExecutorService m_periodicWorkThread;

    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    @Override
    public void initialize(VoltDB.Configuration config) {
        // set the mode first thing
        m_mode = OperationMode.INITIALIZING;

        synchronized(m_startAndStopLock) {
            hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartupString.name(), null);

            m_config = config;

            // set a bunch of things to null/empty/new for tests
            // which reusue the process
            m_clientInterfaces.clear();
            m_agreementSite = null;
            m_adminListener = null;
            m_commandLog = new DummyCommandLog();
            m_deployment = null;
            m_messenger = null;
            m_statsAgent = new StatsAgent();
            m_faultManager = null;
            m_instanceId = null;
            m_zk = null;
            m_snapshotCompletionMonitor = null;
            m_catalogContext = null;
            m_partitionCountStats = null;
            m_ioStats = null;
            m_memoryStats = null;
            m_statsManager = null;
            m_restoreAgent = null;
            m_hasCatalog = new CountDownLatch(1);
            m_hostIdWithStartupCatalog = 0;
            m_pathToStartupCatalog = m_config.m_pathToCatalog;

            // determine if this is a rejoining node
            // (used for license check and later the actual rejoin)
            boolean isRejoin = config.m_rejoinToHostAndPort != null;

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

            readDeploymentAndCreateStarterCatalogContext();

            // Create the thread pool here. It's needed by buildClusterMesh()
            final int availableProcessors = Runtime.getRuntime().availableProcessors();
            int poolSize = 1;
            if (availableProcessors > 4) {
                poolSize = 2;
            }
            m_periodicWorkThread =
                    new ScheduledThreadPoolExecutor(poolSize, new ThreadFactory() {
                        @Override
                        public Thread newThread(Runnable r) {
                            return new Thread(r, "Periodic Work");
                        }
                    });

            buildClusterMesh(isRejoin);

            // do the many init tasks in the Inits class
            Inits inits = new Inits(this, 1);
            inits.doInitializationWork();

            // set up site structure
            m_localSites = Collections.synchronizedMap(new HashMap<Integer, ExecutionSite>());
            m_siteThreads = Collections.synchronizedMap(new HashMap<Integer, Thread>());
            m_runners = new ArrayList<ExecutionSiteRunner>();

            if (config.m_backend.isIPC) {
                int eeCount = 0;
                for (Site site : m_catalogContext.siteTracker.getUpSites()) {
                    if (site.getIsexec() &&
                            m_myHostId == Integer.parseInt(site.getHost().getTypeName())) {
                        eeCount++;
                    }
                }
                if (config.m_ipcPorts.size() != eeCount) {
                    hostLog.fatal("Specified an IPC backend but only supplied " + config.m_ipcPorts.size() +
                            " backend ports when " + eeCount + " are required");
                    System.exit(-1);
                }
            }

            /*
             * Create execution sites runners (and threads) for all exec sites except the first one.
             * This allows the sites to be set up in the thread that will end up running them.
             * Cache the first Site from the catalog and only do the setup once the other threads have been started.
             */
            Site siteForThisThread = null;
            m_currentThreadSite = null;
            for (Site site : m_catalogContext.siteTracker.getUpSites()) {
                int sitesHostId = Integer.parseInt(site.getHost().getTypeName());
                int siteId = Integer.parseInt(site.getTypeName());

                // start a local site
                if (sitesHostId == m_myHostId) {
                    if (site.getIsexec()) {
                        if (siteForThisThread == null) {
                            siteForThisThread = site;
                        } else {
                            ExecutionSiteRunner runner =
                                new ExecutionSiteRunner(
                                        siteId,
                                        m_catalogContext,
                                        m_serializedCatalog,
                                        m_recovering,
                                        m_downHosts,
                                        hostLog);
                            m_runners.add(runner);
                            Thread runnerThread = new Thread(runner, "Site " + siteId);
                            runnerThread.start();
                            log.l7dlog(Level.TRACE, LogKeys.org_voltdb_VoltDB_CreatingThreadForSite.name(), new Object[] { siteId }, null);
                            m_siteThreads.put(siteId, runnerThread);
                        }
                    }
                }
            }

            /*
             * Now that the runners have been started and are doing setup of the other sites in parallel
             * this thread can set up its own execution site.
             */
            int siteId = Integer.parseInt(siteForThisThread.getTypeName());
            ExecutionSite siteObj =
                new ExecutionSite(VoltDB.instance(),
                                  VoltDB.instance().getMessenger().createMailbox(
                                          siteId,
                                          VoltDB.DTXN_MAILBOX_ID,
                                          true),
                                  siteId,
                                  m_serializedCatalog,
                                  null,
                                  m_recovering,
                                  m_downHosts,
                                  m_catalogContext.m_transactionId);
            m_localSites.put(Integer.parseInt(siteForThisThread.getTypeName()), siteObj);
            m_currentThreadSite = siteObj;

            /*
             * Stop and wait for the runners to finish setting up and then put
             * the constructed ExecutionSites in the local site map.
             */
            for (ExecutionSiteRunner runner : m_runners) {
                synchronized (runner) {
                    if (!runner.m_isSiteCreated) {
                        try {
                            runner.wait();
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }
                    m_localSites.put(runner.m_siteId, runner.m_siteObj);
                }
            }

            // Create the client interfaces and associated dtxn initiators
            int portOffset = 0;
            for (Site site : m_catalogContext.siteTracker.getUpSites()) {
                int sitesHostId = Integer.parseInt(site.getHost().getTypeName());
                int currSiteId = Integer.parseInt(site.getTypeName());

                // create CI for each local non-EE site
                if ((sitesHostId == m_myHostId) && (site.getIsexec() == false)) {
                    ClientInterface ci =
                        ClientInterface.create(m_network,
                                               m_messenger,
                                               m_catalogContext,
                                               m_catalogContext.numberOfNodes,
                                               currSiteId,
                                               site.getInitiatorid(),
                                               config.m_port + portOffset,
                                               config.m_adminPort + portOffset,
                                               m_config.m_timestampTestingSalt);
                    portOffset++;
                    m_clientInterfaces.add(ci);
                    m_compilerThread = ci.getCompilerThread();
                }
            }

            m_partitionCountStats = new PartitionCountStats("Partition Count Stats",
                                                            m_catalogContext.numberOfPartitions);
            m_statsAgent.registerStatsSource(SysProcSelector.PARTITIONCOUNT,
                                             0, m_partitionCountStats);
            m_ioStats = new IOStats("IO Stats");
            m_statsAgent.registerStatsSource(SysProcSelector.IOSTATS,
                                             0, m_ioStats);
            m_memoryStats = new MemoryStats("Memory Stats");
            m_statsAgent.registerStatsSource(SysProcSelector.MEMORY,
                                             0, m_memoryStats);
            // Create the statistics manager and register it to JMX registry
            m_statsManager = null;
            try {
                final Class<?> statsManagerClass =
                    Class.forName("org.voltdb.management.JMXStatsManager");
                m_statsManager = (StatsManager)statsManagerClass.newInstance();
                m_statsManager.initialize(new ArrayList<Integer>(m_localSites.keySet()));
            } catch (Exception e) {}

            // in most cases, this work will already be done in inits code,
            // but not for rejoin, so double-make-sure it's done here
            startNetworkAndCreateZKClient();

            try {
                m_snapshotCompletionMonitor.init(m_zk);
            } catch (Exception e) {
                hostLog.fatal("Error initializing snapshot completion monitor", e);
                VoltDB.crashVoltDB();
            }

            if (m_commandLog != null && isRejoin) {
                m_commandLog.initForRejoin(
                        m_catalogContext, Long.MIN_VALUE,
                        m_messenger.getDiscoveredFaultSequenceNumber(),
                        m_downSites);
            }

            // tell other booting nodes that this node is ready. Primary purpose is to publish a hostname
            m_messenger.sendReadyMessage();

            // only needs to be done if this is an initial cluster startup, not a rejoin
            if (config.m_rejoinToHostAndPort == null) {
                // wait for all nodes to be ready
                m_messenger.waitForAllHostsToBeReady();
            }

            heartbeatThread = new HeartbeatThread(m_clientInterfaces);
            heartbeatThread.start();
            schedulePeriodicWorks();

            // print out a bunch of useful system info
            logDebuggingInfo(m_config.m_adminPort, m_config.m_httpPort, m_httpPortExtraLogMessage, m_jsonEnabled);

            int k = m_catalogContext.numberOfExecSites / m_catalogContext.numberOfPartitions;
            if (k == 1) {
                hostLog.warn("Running without redundancy (k=0) is not recommended for production use.");
            }

            assert(m_clientInterfaces.size() > 0);
            ClientInterface ci = m_clientInterfaces.get(0);
            ci.initializeSnapshotDaemon();

            // set additional restore agent stuff
            TransactionInitiator initiator = ci.getInitiator();
            if (m_restoreAgent != null) {
                m_restoreAgent.setCatalogContext(m_catalogContext);
                m_restoreAgent.setInitiator(initiator);
            }
        }
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

        // try to start the adhoc planner once
        scheduleWork(new Runnable() {
            @Override
            public void run() {
                try {
                    m_compilerThread.ensureLoadedPlanner();
                }
                catch (Exception ex) {
                    log.warn(ex.getMessage(), ex);
                }
            }
        }, 10, -1, TimeUnit.SECONDS);
    }

    void readDeploymentAndCreateStarterCatalogContext() {
        m_deployment = CatalogUtil.parseDeployment(m_config.m_pathToDeployment);
        // wasn't a valid xml deployment file
        if (m_deployment == null) {
            hostLog.error("Not a valid XML deployment file at URL: " + m_config.m_pathToDeployment);
            VoltDB.crashVoltDB();
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
    }

    void collectLocalNetworkMetadata() {
        String localMetadata = "";

        /*
         * If no interface was specified, do a ton of work
         * to identify all ipv4 interfaces and turn them into a
         * , separated list since the server will accept on
         * any of them.
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

            while (!interfaces.isEmpty()) {
                NetworkInterface intf = interfaces.poll();
                Enumeration<InetAddress> inetAddrs = intf.getInetAddresses();
                Inet4Address addr = null;
                while (inetAddrs.hasMoreElements()) {
                    InetAddress inetAddr = inetAddrs.nextElement();
                    if (inetAddr instanceof Inet4Address) {
                        addr = (Inet4Address)inetAddr;
                        break;
                    }
                }
                localMetadata += addr.getHostAddress();
                if (interfaces.peek() != null) {
                    localMetadata += ",";
                }
            }
        } else {
            localMetadata = m_config.m_externalInterface;
        }
        localMetadata += ":" + Integer.valueOf(m_config.m_port);
        localMetadata += ":" + Integer.valueOf(m_config.m_adminPort);
        localMetadata += ":" + Integer.valueOf(m_config.m_httpPort); // json
        // possibly atomic swap from null to realz
        m_localMetadata = localMetadata;
    }

    void startNetworkAndCreateZKClient() {
        // don't set this up twice
        if (m_zk != null)
            return;

        // Start running the socket handlers
        hostLog.l7dlog(Level.INFO,
                       LogKeys.host_VoltDB_StartingNetwork.name(),
                       new Object[] { m_network.threadPoolSize },
                       null);
        m_network.start();
        try {
            m_agreementSite.waitForRecovery();
            m_zk = org.voltdb.agreement.ZKUtil.getClient(m_config.m_zkInterface, 60 * 1000);
            if (m_zk == null) {
                throw new Exception("Timed out trying to connect local ZooKeeper instance");
            }
        } catch (Exception e) {
            hostLog.fatal("Unable to create a ZK client", e);
            VoltDB.crashVoltDB();
        }
    }

    void buildClusterMesh(boolean isRejoin) {
        // start the fault manager first
        m_faultManager = new FaultDistributor(this);
        // Install a handler for NODE_FAILURE faults to update the catalog
        // This should be the first handler to run when a node fails
        m_faultManager.registerFaultHandler(NodeFailureFault.NODE_FAILURE_CATALOG,
                m_faultHandler,
                FaultType.NODE_FAILURE);
        if (!m_faultManager.testPartitionDetectionDirectory(
                m_catalogContext.cluster.getFaultsnapshots().get("CLUSTER_PARTITION"))) {
            VoltDB.crashVoltDB();
        }

        // Prepare the network socket manager for work
        m_network = new VoltNetwork(m_periodicWorkThread);

        String leaderAddress = m_config.m_leader;
        int numberOfNodes = m_deployment.getCluster().getHostcount();
        long depCRC = CatalogUtil.getDeploymentCRC(m_config.m_pathToDeployment);

        if (!isRejoin) {
            // Create the intra-cluster mesh
            InetAddress leader = null;
            try {
                leader = InetAddress.getByName(leaderAddress);
            } catch (UnknownHostException ex) {
                hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_CouldNotRetrieveLeaderAddress.name(),
                        new Object[] { leaderAddress }, null);
                VoltDB.crashVoltDB();
            }
            // ensure at least one host (catalog compiler should check this too
            if (numberOfNodes <= 0) {
                hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_InvalidHostCount.name(),
                        new Object[] { numberOfNodes }, null);
                VoltDB.crashVoltDB();
            }

            hostLog.l7dlog( Level.TRACE, LogKeys.host_VoltDB_CreatingVoltDB.name(), new Object[] { numberOfNodes, leader }, null);
            hostLog.info(String.format("Beginning inter-node communication on port %d.", m_config.m_internalPort));
            m_messenger = new HostMessenger(m_network, leader,
                    numberOfNodes, 0, depCRC, hostLog);
            Object retval[] = m_messenger.waitForGroupJoin();
            m_instanceId = new Object[] { retval[0], retval[1] };
        }
        else {
            // rejoin case
            m_downHosts.addAll(rejoinExistingMesh(numberOfNodes, depCRC));
        }

        // Use the host messenger's hostId.
        m_myHostId = m_messenger.getHostId();

        if (isRejoin) {
            /**
             * Whatever hosts were reported as being down on rejoin should
             * be reported to the fault manager so that the fault can be distributed.
             * The execution sites were informed on construction so they don't have
             * to go through the agreement process.
             */
            for (Integer downHost : m_downHosts) {
                m_downNonExecSites.addAll(m_catalogContext.siteTracker.getNonExecSitesForHost(downHost));
                m_downSites.addAll(m_catalogContext.siteTracker.getNonExecSitesForHost(downHost));
                m_faultManager.reportFault(
                        new NodeFailureFault(
                            downHost,
                            m_catalogContext.siteTracker.getNonExecSitesForHost(downHost),
                            "UNKNOWN"));
            }
            try {
                m_faultHandler.m_waitForFaultReported.acquire(m_downHosts.size());
            } catch (InterruptedException e) {
                VoltDB.crashVoltDB();
            }
            ExecutionSite.recoveringSiteCount.set(
                    m_catalogContext.siteTracker.getLiveExecutionSitesForHost(m_messenger.getHostId()).size());
            m_downSites.addAll(m_catalogContext.siteTracker.getAllSitesForHost(m_messenger.getHostId()));
        }

        m_catalogContext.m_transactionId = m_messenger.getDiscoveredCatalogTxnId();
        assert(m_messenger.getDiscoveredCatalogTxnId() != 0);
    }

    HashSet<Integer> rejoinExistingMesh(int numberOfNodes, long deploymentCRC) {
        // sensible defaults (sorta)
        String rejoinHostCredentialString = null;
        String rejoinHostAddressString = null;

        //Client interface port of node that will receive @Rejoin invocation
        int rejoinPort = m_config.m_port;
        String rejoinHost = null;
        String rejoinUser = null;
        String rejoinPass = null;

        // this will cause the ExecutionSites to start in recovering mode
        m_recovering = true;

        // split a "user:pass@host:port" string into "user:pass" and "host:port"
        int atSignIndex = m_config.m_rejoinToHostAndPort.indexOf('@');
        if (atSignIndex == -1) {
            rejoinHostAddressString = m_config.m_rejoinToHostAndPort;
        }
        else {
            rejoinHostCredentialString = m_config.m_rejoinToHostAndPort.substring(0, atSignIndex).trim();
            rejoinHostAddressString = m_config.m_rejoinToHostAndPort.substring(atSignIndex + 1).trim();
        }

        int colonIndex = -1;
        // split a "user:pass" string into "user" and "pass"
        if (rejoinHostCredentialString != null) {
            colonIndex = rejoinHostCredentialString.indexOf(':');
            if (colonIndex == -1) {
                rejoinUser = rejoinHostCredentialString.trim();
                System.out.print("password: ");
                BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
                try {
                    rejoinPass = br.readLine();
                } catch (IOException e) {
                    hostLog.error("Unable to read passord for rejoining credentials from console.");
                    System.exit(-1);
                }
            }
            else {
                rejoinUser = rejoinHostCredentialString.substring(0, colonIndex).trim();
                rejoinPass = rejoinHostCredentialString.substring(colonIndex + 1).trim();
            }
        }

        // split a "host:port" string into "host" and "port"
        colonIndex = rejoinHostAddressString.indexOf(':');
        if (colonIndex == -1) {
            rejoinHost = rejoinHostAddressString.trim();
            // note rejoinPort has a default
        }
        else {
            rejoinHost = rejoinHostAddressString.substring(0, colonIndex).trim();
            rejoinPort = Integer.parseInt(rejoinHostAddressString.substring(colonIndex + 1).trim());
        }

        hostLog.info(String.format("Inter-node communication will use port %d.", m_config.m_internalPort));
        ServerSocketChannel listener = null;
        try {
            listener = ServerSocketChannel.open();
            listener.socket().bind(new InetSocketAddress(m_config.m_internalPort));
        } catch (IOException e) {
            hostLog.error("Problem opening listening rejoin socket: " + e.getMessage());
            System.exit(-1);
        }
        m_messenger = new HostMessenger(m_network, listener, numberOfNodes, 0, deploymentCRC, hostLog);

        // make empty strings null
        if ((rejoinUser != null) && (rejoinUser.length() == 0)) rejoinUser = null;
        if ((rejoinPass != null) && (rejoinPass.length() == 0)) rejoinPass = null;

        // URL Decode so usernames/passwords can contain weird stuff
        try {
            if (rejoinUser != null) rejoinUser = URLDecoder.decode(rejoinUser, "UTF-8");
            if (rejoinPass != null) rejoinPass = URLDecoder.decode(rejoinPass, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            hostLog.error("Problem URL-decoding credentials for rejoin authentication: " + e.getMessage());
            System.exit(-1);
        }

        ClientConfig clientConfig = new ClientConfig(rejoinUser, rejoinPass);
        Client client = ClientFactory.createClient(clientConfig);
        ClientResponse response = null;
        RejoinCallback rcb = new RejoinCallback() {

        };
        try {
            client.createConnection(rejoinHost, rejoinPort);
            InetSocketAddress inetsockaddr = new InetSocketAddress(rejoinHost, rejoinPort);
            SocketChannel socket = SocketChannel.open(inetsockaddr);
            String ip_addr = socket.socket().getLocalAddress().getHostAddress();
            socket.close();
            m_config.m_selectedRejoinInterface =
                m_config.m_internalInterface.isEmpty() ? ip_addr : m_config.m_internalInterface;
            client.callProcedure(
                    rcb,
                    "@Rejoin",
                    m_config.m_selectedRejoinInterface,
                    m_config.m_internalPort);
        }
        catch (Exception e) {
            hostLog.fatal("Problem connecting client: " + e.getMessage());
            VoltDB.crashVoltDB();
        }

        Object retval[] = m_messenger.waitForGroupJoin(60 * 1000);

        m_instanceId = new Object[] { retval[0], retval[1] };

        @SuppressWarnings("unchecked")
        HashSet<Integer> downHosts = (HashSet<Integer>)retval[2];
        hostLog.info("Down hosts are " + downHosts.toString());

        try {
            //Callback validates response asynchronously. Just wait for the response before continuing.
            //Timeout because a failure might result in the response not coming.
            response = rcb.waitForResponse(3000);
            if (response == null) {
                hostLog.fatal("Recovering node timed out rejoining");
                VoltDB.crashVoltDB();
            }
        }
        catch (InterruptedException e) {
            hostLog.fatal("Interrupted while attempting to rejoin cluster");
            VoltDB.crashVoltDB();
        }
        return downHosts;
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
        if (httpPortExtraLogMessage != null)
            hostLog.info(httpPortExtraLogMessage);
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

        collectLocalNetworkMetadata();
        m_clusterMetadata.put(m_messenger.getHostId(), getLocalMetadata());

        /*
         * Publish our cluster metadata, and then retrieve the metadata
         * for the rest of the cluster
         */
        try {
            m_zk.create(
                    "/cluster_metadata",
                    null,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT,
                    new ZKUtil.StringCallback(),
                    null);
            m_zk.create(
                    "/cluster_metadata/" + m_messenger.getHostId(),
                    getLocalMetadata().getBytes("UTF-8"),
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL,
                    new ZKUtil.StringCallback(),
                    null);
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Error creating \"/cluster_metadata\" node in ZK", true, e);
        }

        /*
         * Spin and attempt to retrieve cluster metadata for all nodes in the cluster.
         */
        HashSet<Integer> metadataToRetrieve = new HashSet<Integer>(m_catalogContext.siteTracker.getAllLiveHosts());
        metadataToRetrieve.remove(m_messenger.getHostId());
        while (!metadataToRetrieve.isEmpty()) {
            Map<Integer, ZKUtil.ByteArrayCallback> callbacks = new HashMap<Integer, ZKUtil.ByteArrayCallback>();
            for (Integer hostId : metadataToRetrieve) {
                ZKUtil.ByteArrayCallback cb = new ZKUtil.ByteArrayCallback();
                m_zk.getData("/cluster_metadata/" + hostId, false, cb, null);
                callbacks.put(hostId, cb);
            }

            for (Map.Entry<Integer, ZKUtil.ByteArrayCallback> entry : callbacks.entrySet()) {
                try {
                    ZKUtil.ByteArrayCallback cb = entry.getValue();
                    Integer hostId = entry.getKey();
                    m_clusterMetadata.put(hostId, new String(cb.getData(), "UTF-8"));
                    metadataToRetrieve.remove(hostId);
                } catch (KeeperException.NoNodeException e) {}
                  catch (Exception e) {
                      VoltDB.crashLocalVoltDB("Error retrieving cluster metadata", true, e);
                }
            }

        }

        // print out cluster membership
        hostLog.info("About to list cluster interfaces for all nodes with format ip:client-port:admin-port:http-port");
        for (int hostId : m_catalogContext.siteTracker.getAllLiveHosts()) {
            if (hostId == m_messenger.getHostId()) {
                hostLog.info(String.format("  Host id: %d with interfaces: %s [SELF]", hostId, getLocalMetadata()));
            }
            else {
                String hostMeta = m_clusterMetadata.get(hostId);
                hostLog.info(String.format("  Host id: %d with interfaces: %s [PEER]", hostId, hostMeta));
            }
        }
    }

    @Override
    public void writeNetworkCatalogToTmp(byte[] catalogBytes) {
        hostLog.debug("Got a catalog!");

        hostLog.debug(String.format("Got %d catalog bytes", catalogBytes.length));

        String prefix = String.format("catalog-host%d-", m_myHostId);

        try {
            File catalogFile = File.createTempFile(prefix, ".jar");
            catalogFile.deleteOnExit();
            FileOutputStream fos = new FileOutputStream(catalogFile);
            fos.write(catalogBytes);
            fos.flush();
            fos.close();
            m_config.m_pathToCatalog = catalogFile.getCanonicalPath();
        } catch (IOException e) {
            m_messenger.sendPoisonPill("Failed to write a temp catalog.");
            VoltDB.crashVoltDB();
        }

        // anyone waiting for a catalog can now zoom zoom
        m_hasCatalog.countDown();
    }

    public static String[] extractBuildInfo() {
        StringBuilder sb = new StringBuilder(64);
        String buildString = "VoltDB";
        String versionString = m_defaultVersionString;
        byte b = -1;
        try {
            InputStream buildstringStream =
                ClassLoader.getSystemResourceAsStream("buildstring.txt");
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
                log.l7dlog( Level.ERROR, LogKeys.org_voltdb_VoltDB_FailedToRetrieveBuildString.name(), ignored);
            }
        }
        return new String[] { versionString, buildString };
    }

    @Override
    public void readBuildInfo(String editionTag) {
        String buildInfo[] = extractBuildInfo();
        m_versionString = buildInfo[0];
        m_buildString = buildInfo[1];
        hostLog.info(String.format("Build: %s %s %s", m_versionString, m_buildString, editionTag));
    }

    /**
     * Start all the site's event loops. That's it.
     */
    @Override
    public void run() {
        // start the separate EE threads
        for (ExecutionSiteRunner r : m_runners) {
            synchronized (r) {
                assert(r.m_isSiteCreated) : "Site should already have been created by ExecutionSiteRunner";
                r.notifyAll();
            }
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
        catch (Throwable t)
        {
            String errmsg = "ExecutionSite: " + m_currentThreadSite.m_siteId +
            " encountered an " +
            "unexpected error and will die, taking this VoltDB node down.";
            System.err.println(errmsg);
            t.printStackTrace();
            hostLog.fatal(errmsg, t);
            VoltDB.crashVoltDB();
        }
    }

    /**
     * Try to shut everything down so they system is ready to call
     * initialize again.
     * @param mainSiteThread The thread that m_inititalized the VoltDB or
     * null if called from that thread.
     */
    @Override
    public void shutdown(Thread mainSiteThread) throws InterruptedException {
        synchronized(m_startAndStopLock) {
            m_mode = OperationMode.SHUTTINGDOWN;
            m_executionSitesRecovered = false;
            m_agreementSiteRecovered = false;
            m_snapshotCompletionMonitor.shutdown();
            m_periodicWorkThread.shutdown();
            heartbeatThread.interrupt();
            heartbeatThread.join();
            // Things are going pear-shaped, tell the fault distributor to
            // shut its fat mouth
            m_faultManager.shutDown();

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

            // help the gc along
            m_localSites = null;
            m_currentThreadSite = null;
            m_siteThreads = null;
            m_runners = null;
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        m_zk.close();
                    } catch (InterruptedException e) {
                    }
                }
            };
            t.start();
            m_agreementSite.shutdown();
            t.join();

            m_agreementSite = null;
            // shut down the network/messaging stuff
            // Close the host messenger first, which should close down all of
            // the ForeignHost sockets cleanly
            if (m_messenger != null)
            {
                m_messenger.shutdown();
            }
            if (m_network != null) {
                //Synchronized so the interruption won't interrupt the network thread
                //while it is waiting for the executor service to shutdown
                m_network.shutdown();
            }

            m_messenger = null;
            m_network = null;

            //Also for test code that expects a fresh stats agent
            m_statsAgent = new StatsAgent();

            // The network iterates this list. Clear it after network's done.
            m_clientInterfaces.clear();

            ExportManager.instance().shutdown();

            // probably unnecessary
            System.gc();
            m_isRunning = false;
        }
    }

    /** Last transaction ID at which the rejoin commit took place.
     * Also, use the intrinsic lock to safeguard access from multiple
     * execution site threads */
    private static Long lastNodeRejoinPrepare_txnId = 0L;
    @Override
    public synchronized String doRejoinPrepare(
            long currentTxnId,
            int rejoinHostId,
            String rejoiningHostname,
            int portToConnect,
            Set<Integer> liveHosts)
    {
        // another site already did this work.
        if (currentTxnId == lastNodeRejoinPrepare_txnId) {
            return null;
        }
        else if (currentTxnId < lastNodeRejoinPrepare_txnId) {
            throw new RuntimeException("Trying to rejoin (prepare) with an old transaction.");
        }

        // get the contents of the catalog for the rejoining node
        byte[] catalogBytes = null;
        try {
            catalogBytes = m_catalogContext.getCatalogJarBytes();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        // connect to the joining node, build a foreign host
        InetSocketAddress addr = new InetSocketAddress(rejoiningHostname, portToConnect);
        String ipAddr = addr.getAddress().toString();

        recoveryLog.info("Rejoining node with host id: " + rejoinHostId +
                         ", hostname: " + ipAddr +
                         " at txnid: " + currentTxnId);
        lastNodeRejoinPrepare_txnId = currentTxnId;

        HostMessenger messenger = getHostMessenger();

        // connect to the joining node, build a foreign host
        try {
            messenger.rejoinForeignHostPrepare(rejoinHostId, addr, 0,
                    m_catalogContext.deploymentCRC, liveHosts, m_commandLog.getFaultSequenceNumber(),
                    m_catalogContext.catalogVersion, m_catalogContext.m_transactionId, catalogBytes);
            return null;
        } catch (Exception e) {
            //e.printStackTrace();
            return e.getMessage() == null ? e.getClass().getName() : e.getMessage();
        }
    }

    /** Last transaction ID at which the rejoin commit took place.
     * Also, use the intrinsic lock to safeguard access from multiple
     * execution site threads */
    private static Long lastNodeRejoinFinish_txnId = 0L;
    @Override
    public synchronized String doRejoinCommitOrRollback(long currentTxnId, boolean commit) {
        if (m_recovering) {
            recoveryLog.fatal("Concurrent rejoins are not supported");
            VoltDB.crashVoltDB();
        }
        // another site already did this work.
        if (currentTxnId == lastNodeRejoinFinish_txnId) {
            return null;
        }
        else if (currentTxnId < lastNodeRejoinFinish_txnId) {
            throw new RuntimeException("Trying to rejoin (commit/rollback) with an old transaction.");
        }
        recoveryLog.info("Rejoining commit node with txnid: " + currentTxnId +
                         " lastNodeRejoinFinish_txnId: " + lastNodeRejoinFinish_txnId);
        HostMessenger messenger = getHostMessenger();
        if (commit) {
            // put the foreign host into the set of active ones
            HostMessenger.JoiningNodeInfo joinNodeInfo = messenger.rejoinForeignHostCommit();
            m_faultManager.reportFaultCleared(
                    new NodeFailureFault(
                            joinNodeInfo.hostId,
                            m_catalogContext.siteTracker.getNonExecSitesForHost(joinNodeInfo.hostId),
                            joinNodeInfo.hostName));
            try {
                m_faultHandler.m_waitForFaultClear.acquire();
            } catch (InterruptedException e) {
                VoltDB.crashVoltDB();//shouldn't happen
            }

            ArrayList<Integer> rejoiningSiteIds = new ArrayList<Integer>();
            ArrayList<Integer> rejoiningExecSiteIds = new ArrayList<Integer>();
            Cluster cluster = m_catalogContext.catalog.getClusters().get("cluster");
            for (Site site : cluster.getSites()) {
                int siteId = Integer.parseInt(site.getTypeName());
                int hostId = Integer.parseInt(site.getHost().getTypeName());
                if (hostId == joinNodeInfo.hostId) {
                    assert(site.getIsup() == false);
                    rejoiningSiteIds.add(siteId);
                    if (site.getIsexec() == true) {
                        rejoiningExecSiteIds.add(siteId);
                    }
                }
            }
            assert(rejoiningSiteIds.size() > 0);

            // get a string list of all the new sites
            StringBuilder newIds = new StringBuilder();
            for (int siteId : rejoiningSiteIds) {
                newIds.append(siteId).append(",");
            }
            // trim the last comma
            newIds.setLength(newIds.length() - 1);

            // change the catalog to reflect this change
            hostLog.info("Host joined, host id: " + joinNodeInfo.hostId + " hostname: " + joinNodeInfo.hostName);
            hostLog.info("  Adding sites to cluster: " + newIds);
            StringBuilder sb = new StringBuilder();
            for (int siteId : rejoiningSiteIds)
            {
                sb.append("set ");
                String site_path = VoltDB.instance().getCatalogContext().catalog.
                                   getClusters().get("cluster").getSites().
                                   get(Integer.toString(siteId)).getPath();
                sb.append(site_path).append(" ").append("isUp true");
                sb.append("\n");
            }
            String catalogDiffCommands = sb.toString();
            clusterUpdate(catalogDiffCommands);

            // update the SafteyState in the initiators
            for (ClientInterface ci : m_clientInterfaces) {
                TransactionInitiator initiator = ci.getInitiator();
                initiator.notifyExecutionSiteRejoin(rejoiningExecSiteIds);
            }

            //Notify the export manager the cluster topology has changed
            ExportManager.instance().notifyOfClusterTopologyChange();
        }
        else {
            // clean up any connections made
            messenger.rejoinForeignHostRollback();
        }
        recoveryLog.info("Setting lastNodeRejoinFinish_txnId to: " + currentTxnId);
        lastNodeRejoinFinish_txnId = currentTxnId;

        return null;
    }

    /** Last transaction ID at which the logging config updated.
     * Also, use the intrinsic lock to safeguard access from multiple
     * execution site threads */
    private static Long lastLogUpdate_txnId = 0L;
    @Override
    public void logUpdate(String xmlConfig, long currentTxnId)
    {
        synchronized(lastLogUpdate_txnId)
        {
            // another site already did this work.
            if (currentTxnId == lastLogUpdate_txnId) {
                return;
            }
            else if (currentTxnId < lastLogUpdate_txnId) {
                throw new RuntimeException("Trying to update logging config with an old transaction.");
            }
            System.out.println("Updating RealVoltDB logging config from txnid: " +
                               lastLogUpdate_txnId + " to " + currentTxnId);
            lastLogUpdate_txnId = currentTxnId;
            VoltLogger.configure(xmlConfig);
        }
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
    private HashMap<Long, ContextTracker>m_txnIdToContextTracker =
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
                int ttlsites = m_catalogContext.siteTracker.getLiveExecutionSitesForHost(m_messenger.getHostId()).size();
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
    public void clusterUpdate(String diffCommands)
    {
        synchronized(m_catalogUpdateLock)
        {
            //Reuse the txn id since this doesn't change schema/procs/export
            m_catalogContext = m_catalogContext.update(m_catalogContext.m_transactionId, null,
                                                       diffCommands, false, -1);
        }

        for (ClientInterface ci : m_clientInterfaces)
        {
            ci.notifyOfCatalogUpdate();
        }
    }

    @Override
    public ZooKeeper getZK() {
        return m_zk;
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
    public Messenger getMessenger() {
        return m_messenger;
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
    public Map<Integer, ExecutionSite> getLocalSites() {
        return m_localSites;
    }

    @Override
    public VoltNetwork getNetwork() {
        return m_network;
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
    public boolean ignoreCrash() {
        return false;
    }

    @Override
    public Object[] getInstanceId() {
        return m_instanceId;
    }

    @Override
    public BackendTarget getBackendTargetType() {
        return m_config.m_backend;
    }

    @Override
    public synchronized void onExecutionSiteRecoveryCompletion(long transferred) {
        m_executionSiteRecoveryFinish = System.currentTimeMillis();
        m_executionSiteRecoveryTransferred = transferred;
        m_executionSitesRecovered = true;
        onRecoveryCompletion();
    }

    private void onRecoveryCompletion() {
        if (!m_executionSitesRecovered || !m_agreementSiteRecovered) {
            return;
        }
        try {
            m_testBlockRecoveryCompletion.acquire();
        } catch (InterruptedException e) {}
        final long delta = ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000);
        final long megabytes = m_executionSiteRecoveryTransferred / (1024 * 1024);
        final double megabytesPerSecond = megabytes / ((m_executionSiteRecoveryFinish - m_recoveryStartTime) / 1000.0);
        for (ClientInterface intf : getClientInterfaces()) {
            intf.mayActivateSnapshotDaemon();
        }
        hostLog.info(
                "Node data recovery completed after " + delta + " seconds with " + megabytes +
                " megabytes transferred at a rate of " +
                megabytesPerSecond + " megabytes/sec");
        try {
            boolean logRecoveryCompleted = false;
            try {
                m_zk.create("/unfaulted_hosts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {}
            if (getCommandLog().getClass().getName().equals("org.voltdb.CommandLogImpl")) {
                try {
                    m_zk.create("/request_truncation_snapshot", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                } catch (KeeperException.NodeExistsException e) {}
            } else {
                logRecoveryCompleted = true;
            }
            ByteBuffer txnIdBuffer = ByteBuffer.allocate(8);
            txnIdBuffer.putLong(TransactionIdManager.makeIdFromComponents(System.currentTimeMillis(), 0, 1));
            m_zk.create(
                    "/unfaulted_hosts/" + m_messenger.getHostId(),
                    txnIdBuffer.array(),
                    Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            if (logRecoveryCompleted) {
                m_recovering = false;
                hostLog.info("Node recovery completed");
            }
        } catch (Exception e) {
            hostLog.fatal("Unable to log host recovery completion to ZK", e);
            VoltDB.crashVoltDB();
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

    /**
     * Get the metadata map for the wholes cluster.
     * Note: this may include failed nodes so check for live ones
     *  and filter this if needed.
     *
     * Metadata is currently of the format:
     * IP:CIENTPORT:ADMINPORT:HTTPPORT]
     */
    @Override
    public Map<Integer, String> getClusterMetadataMap() {
        return m_clusterMetadata;
    }

    /**
     * Metadata is currently of the format:
     * IP:CIENTPORT:ADMINPORT:HTTPPORT]
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
        for (ClientInterface ci : m_clientInterfaces) {
            ci.getInitiator().setSendHeartbeats(true);
            try {
                ci.startAcceptingConnections();
            } catch (IOException e) {
                hostLog.l7dlog(Level.FATAL,
                               LogKeys.host_VoltDB_ErrorStartAcceptingConnections.name(),
                               e);
                VoltDB.crashVoltDB();
            }
        }

        if (m_startMode != null) {
            m_mode = m_startMode;
        } else {
            // Shouldn't be here, but to be safe
            m_mode = OperationMode.RUNNING;
        }
        hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerCompletedInitialization.name(), null);
    }

    @Override
    public synchronized void onAgreementSiteRecoveryCompletion() {
        m_agreementSiteRecovered = true;
        onRecoveryCompletion();
    }

    @Override
    public AgreementSite getAgreementSite() {
        return m_agreementSite;
    }

    @Override
    public SnapshotCompletionMonitor getSnapshotCompletionMonitor() {
        return m_snapshotCompletionMonitor;
    }

    @Override
    public synchronized void recoveryComplete() {
        m_recovering = false;
        hostLog.info("Node recovery completed");
    }

    @Override
    public void scheduleWork(Runnable work,
                             long initialDelay,
                             long delay,
                             TimeUnit unit) {
        synchronized (m_periodicWorkThread) {
            if (delay > 0) {
                m_periodicWorkThread.scheduleWithFixedDelay(work,
                                                            initialDelay, delay,
                                                            unit);
            } else {
                m_periodicWorkThread.schedule(work, initialDelay, unit);
            }
        }
    }
}
