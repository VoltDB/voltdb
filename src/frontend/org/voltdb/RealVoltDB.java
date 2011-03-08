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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.export.ExportManager;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.fault.FaultDistributorInterface;
import org.voltdb.fault.FaultHandler;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.SiteMailbox;
import org.voltdb.messaging.VoltMessage;
import org.voltdb.network.VoltNetwork;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DumpManager;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltSampler;

public class RealVoltDB implements VoltDBInterface
{
    private static final VoltLogger log = new VoltLogger(VoltDB.class.getName());
    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger recoveryLog = new VoltLogger("RECOVERY");

    private class VoltDBNodeFailureFaultHandler implements FaultHandler
    {
        @Override
        public void faultOccured(Set<VoltFault> faults)
        {
            for (VoltFault fault : faults) {
                if (fault instanceof NodeFailureFault)
                {
                    NodeFailureFault node_fault = (NodeFailureFault) fault;
                    handleNodeFailureFault(node_fault);
                }
                VoltDB.instance().getFaultDistributor().reportFaultHandled(this, fault);
            }
        }

        private void handleNodeFailureFault(NodeFailureFault node_fault) {
            ArrayList<Integer> dead_sites =
                VoltDB.instance().getCatalogContext().
                siteTracker.getAllSitesForHost(node_fault.getHostId());
            Collections.sort(dead_sites);
            hostLog.error("Host failed, hostname: " + node_fault.getHostname());
            hostLog.error("  Host ID: " + node_fault.getHostId());
            hostLog.error("  Removing sites from cluster: " + dead_sites);
            StringBuilder sb = new StringBuilder();
            for (int site_id : dead_sites)
            {
                sb.append("set ");
                String site_path = VoltDB.instance().getCatalogContext().catalog.
                                   getClusters().get("cluster").getSites().
                                   get(Integer.toString(site_id)).getPath();
                sb.append(site_path).append(" ").append("isUp false");
                sb.append("\n");
            }
            VoltDB.instance().clusterUpdate(sb.toString());
            if (m_catalogContext.siteTracker.getFailedPartitions().size() != 0)
            {
                hostLog.fatal("Failure of host " + node_fault.getHostId() +
                              " has rendered the cluster unviable.  Shutting down...");
                VoltDB.crashVoltDB();
            }
            m_waitForFaultReported.release();
        }

        @Override
        public void faultCleared(Set<VoltFault> faults) {
            for (VoltFault fault : faults) {
                if (fault instanceof NodeFailureFault) {
                    m_waitForFaultClear.release();
                }
            }
        }

        /**
         * When clearing a fault, specifically a rejoining node, wait to make sure it is cleared
         * before proceeding because proceeding might generate new faults that should
         * be deduped by the FaultManager.
         */
        private final Semaphore m_waitForFaultClear = new Semaphore(0);

        /**
         * When starting up as a rejoining node a fault is reported
         * for every currently down node. Once this fault is handled
         * here by RealVoltDB's handler the catalog will be updated.
         * Then the rest of the system can init with the updated catalog.
         */
        private final Semaphore m_waitForFaultReported = new Semaphore(0);
    }

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

    /**
     * A class that instantiates an ExecutionSite and then waits for notification before
     * running the execution site. Would it be better if this extended Thread
     * so we don't have to have m_runners and m_siteThreads?
     */
    private static class ExecutionSiteRunner implements Runnable {

        private volatile boolean m_isSiteCreated = false;
        private final int m_siteId;
        private final String m_serializedCatalog;
        private volatile ExecutionSite m_siteObj;
        private final boolean m_recovering;
        private final HashSet<Integer> m_failedHostIds;

        public ExecutionSiteRunner(
                final int siteId,
                final CatalogContext context,
                final String serializedCatalog,
                boolean recovering,
                HashSet<Integer> failedHostIds) {
            m_siteId = siteId;
            m_serializedCatalog = serializedCatalog;
            m_recovering = recovering;
            m_failedHostIds = failedHostIds;
        }

        @Override
        public void run() {
            Mailbox mailbox = VoltDB.instance().getMessenger()
            .createMailbox(m_siteId, VoltDB.DTXN_MAILBOX_ID, VoltDB.instance().getCommitLog());

            m_siteObj =
                new ExecutionSite(VoltDB.instance(),
                                  mailbox,
                                  m_siteId,
                                  m_serializedCatalog,
                                  null,
                                  m_recovering,
                                  m_failedHostIds);
            synchronized (this) {
                m_isSiteCreated = true;
                this.notifyAll();
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            m_siteObj.run();
        }

    }

    public VoltDB.Configuration m_config = new VoltDB.Configuration();
    private CatalogContext m_catalogContext;
    private String m_buildString;
    private static final String m_defaultVersionString = "1.3.trunk";
    private String m_versionString = m_defaultVersionString;
    // fields accessed via the singleton
    private HostMessenger m_messenger = null;
    private final ArrayList<ClientInterface> m_clientInterfaces =
        new ArrayList<ClientInterface>();
    private Map<Integer, ExecutionSite> m_localSites;
    private VoltNetwork m_network = null;
    private HTTPAdminListener m_adminListener;
    private Map<Integer, Thread> m_siteThreads;
    private ArrayList<ExecutionSiteRunner> m_runners;
    private ExecutionSite m_currentThreadSite;
    private StatsAgent m_statsAgent = new StatsAgent();
    private FaultDistributor m_faultManager;
    private Object m_instanceId[];
    private PartitionCountStats m_partitionCountStats = null;
    private IOStats m_ioStats = null;
    private MemoryStats m_memoryStats = null;
    private StatsManager m_statsManager = null;

    // Should the execution sites be started in recovery mode
    // (used for joining a node to an existing cluster)
    private volatile boolean m_recovering = false;

    // Synchronize initialize and shutdown.
    private final Object m_startAndStopLock = new Object();

    // Synchronize updates of catalog contexts with context accessors.
    private final Object m_catalogUpdateLock = new Object();

    // add a random number to the sampler output to make it likely to be unique for this process.
    private final VoltSampler m_sampler = new VoltSampler(10, "sample" + String.valueOf(new Random().nextInt() % 10000) + ".txt");
    private final AtomicBoolean m_hasStartedSampler = new AtomicBoolean(false);

    private final VoltDBNodeFailureFaultHandler m_faultHandler = new VoltDBNodeFailureFaultHandler();

    private volatile boolean m_isRunning = false;

    @Override
    public boolean recovering() { return m_recovering; }

    private long m_recoveryStartTime = System.currentTimeMillis();

    private CommitLog m_commitLog;

    private volatile boolean m_inAdminMode = false;

    // metadata is currently of the format:
    // IP:CIENTPORT:ADMINPORT:HTTPPORT
    private volatile String m_localMetadata = "0.0.0.0:0:0:0";
    private final Map<Integer, String> m_clusterMetadata = Collections.synchronizedMap(new HashMap<Integer, String>());

    // methods accessed via the singleton
    @Override
    public void startSampler() {
        if (m_hasStartedSampler.compareAndSet(false, true)) {
            m_sampler.start();
        }
    }

    PeriodicWorkTimerThread fivems;

    private static File m_pidFile = null;

    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    @Override
    public void initialize(VoltDB.Configuration config) {
        synchronized(m_startAndStopLock) {
            if (config.m_useCommitLog) {
                try {
                    m_commitLog = new CommitLogImpl( config.m_commitLogDir, config.m_commitInterval, config.m_waitForCommit);
                } catch (IOException e) {
                    hostLog.fatal("Unable to intialize commit log", e);
                    System.exit(-1);
                }
            } else {
                m_commitLog = new CommitLog() {
                    @Override
                    public void logMessage(VoltMessage message, SiteMailbox mailbox) {
                        message.setDurable();
                        mailbox.deliver(message);
                    }

                    @Override
                    public void shutdown() throws InterruptedException {
                    }
                };
            }
            if (m_pidFile == null) {
                String name = java.lang.management.ManagementFactory.getRuntimeMXBean().getName();
                String pidString = name.substring(0, name.indexOf('@'));
                m_pidFile = new java.io.File("/var/tmp/voltpid." + pidString);
                try {
                    boolean success = m_pidFile.createNewFile();
                    if (!success) {
                        hostLog.error("Could not create PID file " + m_pidFile + " because it already exists");
                    }
                    m_pidFile.deleteOnExit();
                    FileOutputStream fos = new FileOutputStream(m_pidFile);
                    fos.write(pidString.getBytes("UTF-8"));
                    fos.close();
                } catch (IOException e) {
                    hostLog.error("Error creating PID file " + m_pidFile, e);
                }
            }

            // Set std-out/err to use the UTF-8 encoding and fail if UTF-8 isn't supported
            try {
                System.setOut(new PrintStream(System.out, true, "UTF-8"));
                System.setErr(new PrintStream(System.err, true, "UTF-8"));
            } catch (UnsupportedEncodingException e) {
                hostLog.fatal("Support for the UTF-8 encoding is required for VoltDB. This means you are likely running an unsupported JVM.");
                VoltDB.crashVoltDB();
            }

            // useful for debugging, but doesn't do anything unless VLog is enabled
            if (config.m_port != VoltDB.DEFAULT_PORT) {
                VLog.setPortNo(config.m_port);
            }
            VLog.log("\n### RealVoltDB.initialize() for port %d ###", config.m_port);

            hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartupString.name(), null);

            // start the dumper thread
            if (config.listenForDumpRequests)
                DumpManager.init();

            readBuildInfo();
            m_config = config;

            // Initialize the catalog and some common shortcuts
            if (m_config.m_pathToCatalog.startsWith("http")) {
                hostLog.info("Loading application catalog jarfile from " + m_config.m_pathToCatalog);
            }
            else {
                File f = new File(m_config.m_pathToCatalog);
                hostLog.info("Loading application catalog jarfile from " + f.getAbsolutePath());
            }

            String serializedCatalog = CatalogUtil.loadCatalogFromJar(m_config.m_pathToCatalog, hostLog);
            if ((serializedCatalog == null) || (serializedCatalog.length() == 0))
                VoltDB.crashVoltDB();

            /* N.B. node recovery requires discovering the current catalog version. */
            final int catalogVersion = 0;
            Catalog catalog = new Catalog();
            catalog.execute(serializedCatalog);

            // note if this fails it will print an error first
            long depCRC = CatalogUtil.compileDeploymentAndGetCRC(catalog, m_config.m_pathToDeployment);
            if (depCRC < 0)
                System.exit(-1);

            serializedCatalog = catalog.serialize();

            m_catalogContext = new CatalogContext(catalog, m_config.m_pathToCatalog, depCRC, catalogVersion, -1);

            // See if we should bring the server up in admin mode
            m_inAdminMode = false;
            if (m_catalogContext.cluster.getAdminenabled() &&
                m_catalogContext.cluster.getAdminstartup())
            {
                m_inAdminMode = true;
            }
            // set the adminPort from the deployment file
            int adminPort = m_catalogContext.cluster.getAdminport();
            // but allow command line override
            if (config.m_adminPort > 0)
                adminPort = config.m_adminPort;

            // requires a catalog context.
            m_faultManager = new FaultDistributor(this);
            // Install a handler for NODE_FAILURE faults to update the catalog
            // This should be the first handler to run when a node fails
            m_faultManager.registerFaultHandler(NodeFailureFault.NODE_FAILURE_CATALOG,
                                                m_faultHandler,
                                                FaultType.NODE_FAILURE);
            if (!m_faultManager.testPartitionDetectionDirectory(m_catalogContext.cluster.getFaultsnapshots().get("CLUSTER_PARTITION"))) {
                VoltDB.crashVoltDB();
            }

            // Initialize the complex partitioning scheme
            TheHashinator.initialize(catalog);

            // start the httpd dashboard/jsonapi
            int httpPort = m_catalogContext.cluster.getHttpdportno();
            boolean jsonEnabled = m_catalogContext.cluster.getJsonapi();

            // if not set by the user, just find a free port
            if (httpPort == 0) {
                // if not set by the user, start at 8080
                httpPort = 8080;

                for (; true; httpPort++) {
                    try {
                        m_adminListener = new HTTPAdminListener(jsonEnabled, httpPort);
                        break;
                    } catch (Exception e1) {}
                }
                if (httpPort == 8081)
                    hostLog.info("HTTP admin console unable to bind to port 8080");
                else if (httpPort > 8081)
                    hostLog.info("HTTP admin console unable to bind to ports 8080 through " + (httpPort - 1));
                hostLog.info("HTTP admin console listening on port " + httpPort);
            }
            else {
                try {
                    m_adminListener = new HTTPAdminListener(jsonEnabled, httpPort);
                    hostLog.info("HTTP admin console listening on port " + httpPort);
                } catch (Exception e1) {
                    hostLog.info("HTTP admin console unable to bind to port " + httpPort);
                }
            }

            // create the string that describes the public interface
            // format "XXX.XXX.XXX.XXX:clientport:adminport:httpport"
            InetAddress addr = null;
            try {
                addr = InetAddress.getLocalHost();
            } catch (UnknownHostException e1) {
                hostLog.fatal("Unable to discover local IP address. Usually a java permissions failure.");
                VoltDB.crashVoltDB();
            }
            String localMetadata = addr.getHostAddress();
            localMetadata += ":" + Integer.valueOf(config.m_port);
            localMetadata += ":" + Integer.valueOf(adminPort);
            localMetadata += ":" + Integer.valueOf(httpPort); // json
            // possibly atomic swap from null to realz
            m_localMetadata = localMetadata;

            // Prepare the network socket manager for work
            m_network = new VoltNetwork();
            final HashSet<Integer> downHosts = new HashSet<Integer>();
            if (config.m_rejoinToHostAndPort == null) {
                // Create the intra-cluster mesh
                InetAddress leader = null;
                try {
                    leader = InetAddress.getByName(m_catalogContext.cluster.getLeaderaddress());
                } catch (UnknownHostException ex) {
                    hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_CouldNotRetrieveLeaderAddress.name(), new Object[] { m_catalogContext.cluster.getLeaderaddress() }, null);
                    VoltDB.crashVoltDB();
                }
                // ensure at least one host (catalog compiler should check this too
                if (m_catalogContext.numberOfNodes <= 0) {
                    hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_InvalidHostCount.name(), new Object[] { m_catalogContext.numberOfNodes }, null);
                    VoltDB.crashVoltDB();
                }

                hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_CreatingVoltDB.name(), new Object[] { m_catalogContext.numberOfNodes, leader }, null);
                m_messenger = new HostMessenger(m_network, leader, m_catalogContext.numberOfNodes, m_catalogContext.catalogCRC, depCRC, hostLog);
                Object retval[] = m_messenger.waitForGroupJoin();
                m_instanceId = new Object[] { retval[0], retval[1] };
            }
            else {
                downHosts.addAll(initializeForRejoin(config, m_catalogContext.catalogCRC, depCRC));
                /**
                 * Whatever hosts were reported as being down on rejoin should
                 * be reported to the fault manager so that the fault can be distributed.
                 * The execution sites were informed on construction so they don't have
                 * to go through the agreement process.
                 */
                for (Integer downHost : downHosts) {
                    m_faultManager.reportFault(new NodeFailureFault( downHost, "UNKNOWN"));
                }
                try {
                    m_faultHandler.m_waitForFaultReported.acquire(downHosts.size());
                } catch (InterruptedException e) {
                    VoltDB.crashVoltDB();
                }
                ExecutionSite.recoveringSiteCount.set(
                        m_catalogContext.siteTracker.getLiveExecutionSitesForHost(m_messenger.getHostId()).size());
            }

            // Use the host messenger's hostId.
            int myHostId = m_messenger.getHostId();

            // make sure the local entry for metadata is current
            // it's possible it could get overwritten in a rejoin scenario
            m_clusterMetadata.put(myHostId, m_localMetadata);

            // Let the Export system read its configuration from the catalog.
            try {
                ExportManager.initialize(myHostId, m_catalogContext);
            } catch (ExportManager.SetupException e) {
                hostLog.l7dlog(Level.FATAL, LogKeys.host_VoltDB_ExportInitFailure.name(), e);
                System.exit(-1);
            }

            // set up site structure
            m_localSites = Collections.synchronizedMap(new HashMap<Integer, ExecutionSite>());
            m_siteThreads = Collections.synchronizedMap(new HashMap<Integer, Thread>());
            m_runners = new ArrayList<ExecutionSiteRunner>();

            if (config.m_backend.isIPC) {
                int eeCount = 0;
                for (Site site : m_catalogContext.siteTracker.getUpSites()) {
                    if (site.getIsexec() &&
                            myHostId == Integer.parseInt(site.getHost().getTypeName())) {
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
                if (sitesHostId == myHostId) {
                    log.l7dlog( Level.TRACE, LogKeys.org_voltdb_VoltDB_CreatingLocalSite.name(), new Object[] { siteId }, null);
                    m_messenger.createLocalSite(siteId);
                    if (site.getIsexec()) {
                        if (siteForThisThread == null) {
                            siteForThisThread = site;
                        } else {
                            ExecutionSiteRunner runner =
                                new ExecutionSiteRunner(
                                        siteId,
                                        m_catalogContext,
                                        serializedCatalog,
                                        m_recovering,
                                        downHosts);
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
                                          m_commitLog),
                                  siteId,
                                  serializedCatalog,
                                  null,
                                  m_recovering,
                                  downHosts);
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


            // set up profiling and tracing
            // hack to prevent profiling on multiple machines
            if (m_localSites.size() == 1) {
                if (m_config.m_profilingLevel != ProcedureProfiler.Level.DISABLED)
                    hostLog.l7dlog(
                                   Level.INFO,
                                   LogKeys.host_VoltDB_ProfileLevelIs.name(),
                                   new Object[] { m_config.m_profilingLevel },
                                   null);
                ProcedureProfiler.profilingLevel = m_config.m_profilingLevel;
            }
            else {
                hostLog.l7dlog(
                               Level.INFO,
                               LogKeys.host_VoltDB_InternalProfilingDisabledOnMultipartitionHosts.name(),
                               null);
            }

            // if a workload tracer is specified, start her up!
            ProcedureProfiler.initializeWorkloadTrace(catalog);

            // Create the client interfaces and associated dtxn initiators
            int portOffset = 0;
            for (Site site : m_catalogContext.siteTracker.getUpSites()) {
                int sitesHostId = Integer.parseInt(site.getHost().getTypeName());
                int currSiteId = Integer.parseInt(site.getTypeName());

                // create CI for each local non-EE site
                if ((sitesHostId == myHostId) && (site.getIsexec() == false)) {
                    ClientInterface ci =
                        ClientInterface.create(m_network,
                                               m_messenger,
                                               m_catalogContext,
                                               m_catalogContext.numberOfNodes,
                                               currSiteId,
                                               site.getInitiatorid(),
                                               config.m_port + portOffset,
                                               adminPort + portOffset,
                                               m_config.m_timestampTestingSalt);
                    portOffset++;
                    m_clientInterfaces.add(ci);
                    try {
                        ci.startAcceptingConnections();
                    } catch (IOException e) {
                        hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_ErrorStartAcceptingConnections.name(), e);
                        VoltDB.crashVoltDB();
                    }
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

            // Start running the socket handlers
            hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartingNetwork.name(), null);
            m_network.start();

            // tell other booting nodes that this node is ready. Primary purpose is to publish a hostname
            m_messenger.sendReadyMessage();

            // only needs to be done if this is an initial cluster startup, not a rejoin
            if (config.m_rejoinToHostAndPort == null) {
                // wait for all nodes to be ready
                m_messenger.waitForAllHostsToBeReady();
            }

            fivems = new PeriodicWorkTimerThread(m_clientInterfaces,
                                                 m_statsManager);
            fivems.start();

            hostLog.info(String.format("The Server is%s running in admin mode on port %d", m_inAdminMode ? "" : " not", adminPort));

            hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerCompletedInitialization.name(), null);
        }
    }

    public HashSet<Integer> initializeForRejoin(VoltDB.Configuration config, long catalogCRC, long deploymentCRC) {
        // sensible defaults (sorta)
        String rejoinHostCredentialString = null;
        String rejoinHostAddressString = null;

        //Client interface port of node that will receive @Rejoin invocation
        int rejoinPort = config.m_port;
        String rejoinHost = null;
        String rejoinUser = null;
        String rejoinPass = null;

        // this will cause the ExecutionSites to start in recovering mode
        m_recovering = true;

        // split a "user:pass@host:port" string into "user:pass" and "host:port"
        int atSignIndex = config.m_rejoinToHostAndPort.indexOf('@');
        if (atSignIndex == -1) {
            rejoinHostAddressString = config.m_rejoinToHostAndPort;
        }
        else {
            rejoinHostCredentialString = config.m_rejoinToHostAndPort.substring(0, atSignIndex).trim();
            rejoinHostAddressString = config.m_rejoinToHostAndPort.substring(atSignIndex + 1).trim();
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

        ServerSocketChannel listener = null;
        try {
            listener = ServerSocketChannel.open();
            listener.socket().bind(new InetSocketAddress(config.m_internalPort));
        } catch (IOException e) {
            hostLog.error("Problem opening listening rejoin socket: " + e.getMessage());
            System.exit(-1);
        }
        m_messenger = new HostMessenger(m_network, listener, m_catalogContext.numberOfNodes, catalogCRC, deploymentCRC, hostLog);

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
            String hostname = socket.socket().getLocalAddress().getCanonicalHostName();
            socket.close();
            config.m_selectedRejoinInterface =
                config.m_internalInterface.isEmpty() ? hostname : config.m_internalInterface;
            client.callProcedure(
                    rcb,
                    "@Rejoin",
                    config.m_selectedRejoinInterface,
                    config.m_internalPort);
        }
        catch (Exception e) {
            recoveryLog.fatal("Problem connecting client: " + e.getMessage());
            VoltDB.crashVoltDB();
        }

        Object retval[] = m_messenger.waitForGroupJoin(60 * 1000);

        m_catalogContext = new CatalogContext(
                m_catalogContext.catalog,
                m_catalogContext.pathToCatalogJar,
                deploymentCRC,
                m_messenger.getDiscoveredCatalogVersion(),
                0);

        m_instanceId = new Object[] { retval[0], retval[1] };

        @SuppressWarnings("unchecked")
        HashSet<Integer> downHosts = (HashSet<Integer>)retval[2];
        recoveryLog.info("Down hosts are " + downHosts.toString());

        try {
            //Callback validates response asynchronously. Just wait for the response before continuing.
            //Timeout because a failure might result in the response not coming.
            response = rcb.waitForResponse(3000);
            if (response == null) {
                recoveryLog.fatal("Recovering node timed out rejoining");
                VoltDB.crashVoltDB();
            }
        }
        catch (InterruptedException e) {
            recoveryLog.fatal("Interrupted while attempting to rejoin cluster");
            VoltDB.crashVoltDB();
        }
        return downHosts;
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
    public void readBuildInfo() {
        String buildInfo[] = extractBuildInfo();
        m_versionString = buildInfo[0];
        m_buildString = buildInfo[1];
        hostLog.info("Build: " + m_versionString + " " + m_buildString);
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
        // start one site in the current thread
        Thread.currentThread().setName("ExecutionSiteAndVoltDB");
        m_isRunning = true;
        m_currentThreadSite.run();
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
            fivems.interrupt();
            fivems.join();
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
            HashSet<Integer> liveHosts)
    {
        // another site already did this work.
        if (currentTxnId == lastNodeRejoinPrepare_txnId) {
            return null;
        }
        else if (currentTxnId < lastNodeRejoinPrepare_txnId) {
            throw new RuntimeException("Trying to rejoin (prepare) with an old transaction.");
        }
        System.out.printf("Rejoining node with host id: %d at txnid: %d\n", rejoinHostId, currentTxnId);
        lastNodeRejoinPrepare_txnId = currentTxnId;

        HostMessenger messenger = getHostMessenger();

        // connect to the joining node, build a foreign host
        InetSocketAddress addr = new InetSocketAddress(rejoiningHostname, portToConnect);
        try {
            messenger.rejoinForeignHostPrepare(rejoinHostId, addr, m_catalogContext.catalogCRC,
                    m_catalogContext.deploymentCRC, liveHosts, m_catalogContext.catalogVersion);
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
        // another site already did this work.
        if (currentTxnId == lastNodeRejoinFinish_txnId) {
            return null;
        }
        else if (currentTxnId < lastNodeRejoinFinish_txnId) {
            throw new RuntimeException("Trying to rejoin (commit/rollback) with an old transaction.");
        }
        System.out.printf("Rejoining commit node with txnid: %d lastNodeRejoinFinish_txnId: %d\n", currentTxnId, lastNodeRejoinFinish_txnId);
        HostMessenger messenger = getHostMessenger();
        if (commit) {
            // put the foreign host into the set of active ones
            HostMessenger.JoiningNodeInfo joinNodeInfo = messenger.rejoinForeignHostCommit();
            m_faultManager.reportFaultCleared(new NodeFailureFault(joinNodeInfo.hostId, joinNodeInfo.hostName));
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
            hostLog.error("Host joined, hostname: " + joinNodeInfo.hostName);
            hostLog.error("  Host ID: " + joinNodeInfo.hostId);
            hostLog.error("  Adding sites to cluster: " + newIds);
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
        }
        else {
            // clean up any connections made
            messenger.rejoinForeignHostRollback();
        }
        System.out.printf("Setting lastNodeRejoinFinish_txnId: %d\n", lastNodeRejoinFinish_txnId);
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



    /** Last transaction ID at which the catalog updated. */
    private static long lastCatalogUpdate_txnId = 0;

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
            String newCatalogURL,
            int expectedCatalogVersion,
            long currentTxnId,
            long deploymentCRC)
    {
        synchronized(m_catalogUpdateLock) {
            // A site is catching up with catalog updates
            if (currentTxnId <= lastCatalogUpdate_txnId) {
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
            lastCatalogUpdate_txnId = currentTxnId;
            m_catalogContext = m_catalogContext.update(newCatalogURL, diffCommands, true, deploymentCRC);
            m_txnIdToContextTracker.put(currentTxnId, new ContextTracker(m_catalogContext));

            // 1. update the export manager.
            ExportManager.instance().updateCatalog(m_catalogContext);

            // 2. update client interface (asynchronously)
            //    CI in turn updates the planner thread.
            for (ClientInterface ci : m_clientInterfaces) {
                ci.notifyOfCatalogUpdate();
            }

            return m_catalogContext;
        }
    }

    @Override
    public void clusterUpdate(String diffCommands)
    {
        synchronized(m_catalogUpdateLock)
        {
            m_catalogContext = m_catalogContext.update(CatalogContext.NO_PATH,
                                                       diffCommands, false, -1);
        }

        for (ClientInterface ci : m_clientInterfaces)
        {
            ci.notifyOfCatalogUpdate();
        }
    }

    @Override
    public VoltDB.Configuration getConfig()
    {
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
    public void onRecoveryCompletion(long transferred) {
        final long now = System.currentTimeMillis();
        final long delta = ((now - m_recoveryStartTime) / 1000);
        final long megabytes = transferred / (1024 * 1024);
        final double megabytesPerSecond = megabytes / ((now - m_recoveryStartTime) / 1000.0);
        m_recovering = false;
        for (ClientInterface intf : getClientInterfaces()) {
            intf.mayActivateSnapshotDaemon();
        }
        hostLog.info(
                "Node recovery completed after " + delta + " seconds with " + megabytes +
                " megabytes transferred at a rate of " +
                megabytesPerSecond + " megabytes/sec");
    }

    @Override
    public CommitLog getCommitLog() {
        return m_commitLog;
    }

    @Override
    public boolean inAdminMode()
    {
        return m_inAdminMode;
    }

    @Override
    public void setAdminMode(boolean inAdminMode)
    {
        m_inAdminMode = inAdminMode;
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
}
