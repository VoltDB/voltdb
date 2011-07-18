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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.agreement.AgreementSite;
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
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.messaging.InitiateTaskMessage;
import org.voltdb.messaging.Mailbox;
import org.voltdb.messaging.Messenger;
import org.voltdb.network.VoltNetwork;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DumpManager;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.ResponseSampler;
import org.voltdb.utils.VoltSampler;

public class RealVoltDB implements VoltDBInterface, RestoreAgent.Callback
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
            hostLog.error("Host failed, host id: " + node_fault.getHostId() +
                    " hostname: " + node_fault.getHostname());
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

            /*
             * Use a new thread since this is a asynchronous (and infrequent)
             * task and locks are being held by the fault distributor.
             */
            new Thread() {
                @Override
                public void run() {
                    //Notify the export manager the cluster topology has changed
                    ExportManager.instance().notifyOfClusterTopologyChange();
                }
            }.start();
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
        private final long m_txnId;

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
            m_txnId = context.m_transactionId;
        }

        @Override
        public void run() {
            Mailbox mailbox = VoltDB.instance().getMessenger()
            .createMailbox(m_siteId, VoltDB.DTXN_MAILBOX_ID, true);

            m_siteObj =
                new ExecutionSite(VoltDB.instance(),
                                  mailbox,
                                  m_siteId,
                                  m_serializedCatalog,
                                  null,
                                  m_recovering,
                                  m_failedHostIds,
                                  m_txnId);
            synchronized (this) {
                m_isSiteCreated = true;
                this.notifyAll();
                try {
                    wait();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            try
            {
                m_siteObj.run();
            }
            catch (OutOfMemoryError e)
            {
                // Even though OOM should be caught by the Throwable section below,
                // it sadly needs to be handled seperately. The goal here is to make
                // sure VoltDB crashes.

                String errmsg = "ExecutionSite: " + m_siteId + " ran out of Java memory. " +
                    "This node will shut down.";
                hostLog.fatal(errmsg, e);
                VoltDB.crashVoltDB();
            }
            catch (Throwable t)
            {
                String errmsg = "ExecutionSite: " + m_siteId + " encountered an " +
                    "unexpected error and will die, taking this VoltDB node down.";
                System.err.println(errmsg);
                t.printStackTrace();
                hostLog.fatal(errmsg, t);
                VoltDB.crashVoltDB();
            }
        }

    }

    public VoltDB.Configuration m_config = new VoltDB.Configuration();
    private CatalogContext m_catalogContext;
    private String m_buildString;
    private static final String m_defaultVersionString = "1.3.5";
    private String m_versionString = m_defaultVersionString;
    // fields accessed via the singleton
    private HostMessenger m_messenger = null;
    private final ArrayList<ClientInterface> m_clientInterfaces =
        new ArrayList<ClientInterface>();
    private Map<Integer, ExecutionSite> m_localSites;
    private VoltNetwork m_network = null;
    private AgreementSite m_agreementSite;
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
    private ZooKeeper m_zk;
    private SnapshotCompletionMonitor m_snapshotCompletionMonitor;

    // Should the execution sites be started in recovery mode
    // (used for joining a node to an existing cluster)
    private volatile boolean m_recovering = false;
    //Only restrict recovery completion during test
    static Semaphore m_testBlockRecoveryCompletion = new Semaphore(Integer.MAX_VALUE);
    private boolean m_executionSitesRecovered = false;
    private boolean m_agreementSiteRecovered = false;
    private long m_executionSiteRecoveryFinish;
    private long m_executionSiteRecoveryTransferred;

    // Synchronize initialize and shutdown.
    private final Object m_startAndStopLock = new Object();

    // Synchronize updates of catalog contexts with context accessors.
    private final Object m_catalogUpdateLock = new Object();

    // add a random number to the sampler output to make it likely to be unique for this process.
    private final VoltSampler m_sampler = new VoltSampler(10, "sample" + String.valueOf(new Random().nextInt() % 10000) + ".txt");
    private final AtomicBoolean m_hasStartedSampler = new AtomicBoolean(false);

    private final VoltDBNodeFailureFaultHandler m_faultHandler = new VoltDBNodeFailureFaultHandler();

    private RestoreAgent m_restoreAgent = null;

    private volatile boolean m_isRunning = false;

    @Override
    public boolean recovering() { return m_recovering; }

    private long m_recoveryStartTime = System.currentTimeMillis();

    private CommandLog m_commandLog = new CommandLog() {

        @Override
        public void init(CatalogContext context, long txnId) {}

        @Override
        public void log(InitiateTaskMessage message) {}

        @Override
        public void shutdown() throws InterruptedException {}

        @Override
        public Semaphore logFault(Set<Integer> failedSites,
                Set<Long> faultedTxns) {
            return new Semaphore(1);
        }

        @Override
        public void logHeartbeat(final long txnId) {
            // TODO Auto-generated method stub

        }

        @Override
        public long getFaultSequenceNumber() {
            return 0;
        }

        @Override
        public void initForRejoin(CatalogContext context, long txnId,
                long faultSequenceNumber, Set<Integer> failedSites) {
            // TODO Auto-generated method stub

        }
    };

    private volatile OperationMode m_mode = OperationMode.INITIALIZING;
    private OperationMode m_startMode = null;

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
        // set the mode first thing
        m_mode = OperationMode.INITIALIZING;

        synchronized(m_startAndStopLock) {
            m_snapshotCompletionMonitor = new SnapshotCompletionMonitor();
            // start (asynchronously) getting platform info
            // this will start a thread that should die in less
            // than a second
            PlatformProperties.fetchPlatformProperties();

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

            // useful for debugging, but doesn't do anything unless VLog is enabled
            if (config.m_port != VoltDB.DEFAULT_PORT) {
                VLog.setPortNo(config.m_port);
            }
            VLog.log("\n### RealVoltDB.initialize() for port %d ###", config.m_port);

            hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartupString.name(), null);

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
            long depCRC = -1;
            try {
                depCRC = CatalogUtil.compileDeploymentAndGetCRC(catalog, m_config.m_pathToDeployment, true);
                if (depCRC < 0)
                    System.exit(-1);
            } catch (Exception e) {
                hostLog.fatal("Error parsing deployment file", e);
                System.exit(-1);
            }

            serializedCatalog = catalog.serialize();
            m_catalogContext = new CatalogContext(
                    0,
                    catalog, m_config.m_pathToCatalog, depCRC, catalogVersion, -1);

            // determine if this is a rejoining node
            // (used for license check and later the actual rejoin)
            boolean isRejoin = config.m_rejoinToHostAndPort != null;

            // If running commercial code (of value) and not rejoining, enforce licensing.
            Class<?> proClass = MiscUtils.loadProClass(
                    "org.voltdb.CommandLogImpl",
                    "Command logging", true);
            if ((proClass != null) && !isRejoin) {
                assert(m_config != null);
                assert(m_catalogContext != null);
                if (!validateLicense(m_config.m_pathToLicense, m_catalogContext.numberOfNodes)) {
                    // validateLicense logs as appropriate. Exit call is here for testability.

                    // TOOD: Stop running here!
                    VoltDB.crashVoltDB();
                }
            }

            if (m_catalogContext.cluster.getLogconfig().get("log").getEnabled()) {
                try {
                    Class<?> loggerClass = MiscUtils.loadProClass("org.voltdb.CommandLogImpl",
                                                               "Command logging", false);
                    if (loggerClass != null) {
                        m_commandLog = (CommandLog)loggerClass.newInstance();
                    }
                } catch (InstantiationException e) {
                    hostLog.fatal("Unable to instantiate command log", e);
                    VoltDB.crashVoltDB();
                } catch (IllegalAccessException e) {
                    hostLog.fatal("Unable to instantiate command log", e);
                    VoltDB.crashVoltDB();
                }
            } else {
                hostLog.info("Command logging is disabled");
            }

            // start up the response sampler if asked to by setting the env var
            // VOLTDB_RESPONSE_SAMPLE_PATH to a valid path
            ResponseSampler.initializeIfEnabled();

            // See if we should bring the server up in admin mode
            if (m_catalogContext.cluster.getAdminstartup())
            {
                m_startMode = OperationMode.PAUSED;
            }

            // set the adminPort from the deployment file
            int adminPort = m_catalogContext.cluster.getAdminport();
            // but allow command line override
            if (config.m_adminPort > 0)
                adminPort = config.m_adminPort;
            // other places might use config to figure out the port
            config.m_adminPort = adminPort;

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

            // start the httpd dashboard/jsonapi. A port value of -1 means disabled
            // by the deployment.xml configuration.
            int httpPort = m_catalogContext.cluster.getHttpdportno();
            boolean jsonEnabled = m_catalogContext.cluster.getJsonapi();
            String httpPortExtraLogMessage = null;
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
                    httpPortExtraLogMessage = "HTTP admin console unable to bind to port 8080";
                else if (httpPort > 8081)
                    httpPortExtraLogMessage = "HTTP admin console unable to bind to ports 8080 through " + String.valueOf(httpPort - 1);
            }
            else if (httpPort != -1) {
                try {
                    m_adminListener = new HTTPAdminListener(jsonEnabled, httpPort);
                } catch (Exception e1) {
                    hostLog.info("HTTP admin console unable to bind to port " + httpPort + ". Exiting.");
                    System.exit(-1);
                }
            }

            // create the string that describes the public interface
            // format "XXX.XXX.XXX.XXX:clientport:adminport:httpport"
            InetAddress addr = null;
            try {
                addr = InetAddress.getLocalHost();
            } catch (UnknownHostException e1) {
                hostLog.fatal("Unable to discover local IP address by invoking Java's InetAddress.getLocalHost() method. Usually this is because the hostname of this node fails to resolve (for example \"ping `hostname`\" would fail). VoltDB requires that the hostname of every node resolves correctly at that node as well as every other node.");
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
            final Set<Integer> downNonExecSites = new HashSet<Integer>();
            //For command log only, will also mark self as faulted
            final Set<Integer> downSites = new HashSet<Integer>();
            if (!isRejoin) {
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

                hostLog.l7dlog( Level.TRACE, LogKeys.host_VoltDB_CreatingVoltDB.name(), new Object[] { m_catalogContext.numberOfNodes, leader }, null);
                hostLog.info(String.format("Beginning inter-node communication on port %d.", config.m_internalPort));
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
                    downNonExecSites.addAll(m_catalogContext.siteTracker.getNonExecSitesForHost(downHost));
                    downSites.addAll(m_catalogContext.siteTracker.getNonExecSitesForHost(downHost));
                    m_faultManager.reportFault(
                            new NodeFailureFault(
                                downHost,
                                m_catalogContext.siteTracker.getNonExecSitesForHost(downHost),
                                "UNKNOWN"));
                }
                try {
                    m_faultHandler.m_waitForFaultReported.acquire(downHosts.size());
                } catch (InterruptedException e) {
                    VoltDB.crashVoltDB();
                }
                ExecutionSite.recoveringSiteCount.set(
                        m_catalogContext.siteTracker.getLiveExecutionSitesForHost(m_messenger.getHostId()).size());
                downSites.addAll(m_catalogContext.siteTracker.getAllSitesForHost(m_messenger.getHostId()));
            }
            m_catalogContext.m_transactionId = m_messenger.getDiscoveredCatalogTxnId();
            assert(m_messenger.getDiscoveredCatalogTxnId() != 0);

            // Use the host messenger's hostId.
            final int myHostId = m_messenger.getHostId();

            /*
             * Initialize the agreement site with the same site id as the CI/SDTXN,
             * but a different mailbox.
             */
            {
                int myAgreementSiteId = -1;
                HashSet<Integer> agreementSiteIds = new HashSet<Integer>();
                int myAgreementInitiatorId = 0;
                for (Site site : m_catalogContext.siteTracker.getAllSites()) {
                    int sitesHostId = Integer.parseInt(site.getHost().getTypeName());
                    int currSiteId = Integer.parseInt(site.getTypeName());

                    if (sitesHostId == myHostId) {
                        log.l7dlog( Level.TRACE, LogKeys.org_voltdb_VoltDB_CreatingLocalSite.name(), new Object[] { currSiteId }, null);
                        m_messenger.createLocalSite(currSiteId);
                    }
                    // Create an agreement site for every initiator
                    if (site.getIsexec() == false) {
                        agreementSiteIds.add(currSiteId);
                        if (sitesHostId == myHostId) {
                            myAgreementSiteId = currSiteId;
                            myAgreementInitiatorId = site.getInitiatorid();
                        }
                    }
                }

                assert(m_agreementSite == null);
                assert(myAgreementSiteId != -1);
                Mailbox agreementMailbox =
                    m_messenger.createMailbox(myAgreementSiteId, VoltDB.AGREEMENT_MAILBOX_ID, false);
                try {
                    m_agreementSite =
                        new AgreementSite(
                                myAgreementSiteId,
                                agreementSiteIds,
                                myAgreementInitiatorId,
                                downNonExecSites,
                                agreementMailbox,
                                new InetSocketAddress(
                                        m_config.m_zkInterface.split(":")[0],
                                        Integer.parseInt(m_config.m_zkInterface.split(":")[1])),
                                m_faultManager,
                                m_recovering);
                    m_agreementSite.start();
                } catch (Exception e) {
                    hostLog.fatal(null, e);
                    System.exit(-1);
                }
            }

            // make sure the local entry for metadata is current
            // it's possible it could get overwritten in a rejoin scenario
            m_clusterMetadata.put(myHostId, m_localMetadata);

            // Let the Export system read its configuration from the catalog.
            try {
                ExportManager.initialize(myHostId, m_catalogContext, isRejoin);
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
                                          true),
                                  siteId,
                                  serializedCatalog,
                                  null,
                                  m_recovering,
                                  downHosts,
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


            // set up profiling and tracing
            // hack to prevent profiling on multiple machines
            if (m_config.m_profilingLevel != ProcedureProfiler.Level.DISABLED) {
                if (m_localSites.size() == 1) {
                    hostLog.l7dlog(Level.INFO,
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
                        downSites);
            }

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

            // print out a bunch of useful system info
            logDebuggingInfo(adminPort, httpPort, httpPortExtraLogMessage, jsonEnabled);

            int k = m_catalogContext.numberOfExecSites / m_catalogContext.numberOfPartitions;
            if (k == 1) {
                hostLog.warn("Running without redundancy (k=0) is not recommended for production use.");
            }

            assert(m_clientInterfaces.size() > 0);
            ClientInterface ci = m_clientInterfaces.get(0);
            ci.initializeSnapshotDaemon();
            TransactionInitiator initiator = ci.getInitiator();

            /*
             * Had to initialize the variable here, some tests reuse VoltDB
             * instance
             */
            m_restoreAgent = null;

            if (!isRejoin && !m_config.m_isRejoinTest) {
                try {
                    m_restoreAgent = new RestoreAgent(m_catalogContext, initiator,
                                                      m_zk, getSnapshotCompletionMonitor(),
                                                      this, myHostId,
                                                      config.m_startAction);
                } catch (IOException e) {
                    hostLog.fatal("Unable to establish a ZooKeeper connection: " +
                                  e.getMessage());
                    VoltDB.crashVoltDB();
                }
            } else {
                onRestoreCompletion(Long.MIN_VALUE, !isRejoin);
            }
        }
    }

    /**
     * Validate the signature and business logic enforcement for a license.
     * @param pathToLicense
     * @param numberOfNodes
     * @return true if the licensing constraints are met
     */
    boolean validateLicense(String pathToLicense, int numberOfNodes) {
        // verify the file exists.
        File licenseFile = new File(pathToLicense);
        if (licenseFile.exists() == false) {
            hostLog.fatal("Unable to open license file: " + m_config.m_pathToLicense);
            return false;
        }

        // boilerplate to create a license api interface
        LicenseApi licenseApi = null;
        Class<?> licApiKlass = MiscUtils.loadProClass("org.voltdb.licensetool.LicenseApiImpl",
                "License API", false);

        if (licApiKlass != null) {
            try {
                licenseApi = (LicenseApi)licApiKlass.newInstance();
            } catch (InstantiationException e) {
                hostLog.fatal("Unable to process license file: could not create license API.");
                return false;
            } catch (IllegalAccessException e) {
                hostLog.fatal("Unable to process license file: could not create license API.");
                return false;
            }
        }

        if (licenseApi == null) {
            hostLog.fatal("Unable to load license file: could not create license API.");
            return false;
        }

        // Initialize the API. This parses the file but does NOT verify signatures.
        if (licenseApi.initializeFromFile(licenseFile) == false) {
            hostLog.fatal("Unable to load license file: could not parse license.");
            return false;
        }

        // Perform signature verification - detect modified files
        if (licenseApi.verify() == false) {
            hostLog.fatal("Unable to load license file: could not verify license signature.");
            return false;
        }

        Calendar now = GregorianCalendar.getInstance();
        SimpleDateFormat sdf = new SimpleDateFormat("MMM d, yyyy");
        String expiresStr = sdf.format(licenseApi.expires().getTime());
        boolean valid = true;

        if (now.after(licenseApi.expires())) {
            if (licenseApi.isTrial()) {
                hostLog.fatal("VoltDB trial license expired on " + expiresStr + ".");
                return false;
            }
            else {
                hostLog.error("Warning, VoltDB commercial license expired on " + expiresStr + ".");
                valid = false;
            }
        }

        // print out trial success message
        if (licenseApi.isTrial()) {
            hostLog.info("Starting VoltDB with trial license. License expires on " + expiresStr + ".");
            return true;
        }

        // ASSUME CUSTOMER LICENSE HERE

        if (numberOfNodes > licenseApi.maxHostcount()) {
            hostLog.error("Warning, VoltDB commercial license for " + licenseApi.maxHostcount() +
                    " nodes, starting cluster with " + numberOfNodes + " nodes.");
            valid = false;
        }

        // this gets printed even if there are non-fatal problems, so it
        // injects the word "invalid" to make it clear this is the case
        String msg = String.format("Starting VoltDB with %scommercial license. " +
                "License for %d nodes expires on %s.",
                (valid ? "" : "invalid "),
                licenseApi.maxHostcount(),
                expiresStr);
        hostLog.info(msg);

        return true;
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

        hostLog.info(String.format("Inter-node communication will use port %d.", config.m_internalPort));
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
            String ip_addr = socket.socket().getLocalAddress().getHostAddress();
            socket.close();
            config.m_selectedRejoinInterface =
                config.m_internalInterface.isEmpty() ? ip_addr : config.m_internalInterface;
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
                TransactionIdManager.makeIdFromComponents(System.currentTimeMillis(), 0, 0),
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

    void logDebuggingInfo(int adminPort, int httpPort, String httpPortExtraLogMessage, boolean jsonEnabled) {
        String startAction = m_config.m_startAction.toString();
        String startActionLog = (startAction.substring(0, 1).toUpperCase() +
                                 startAction.substring(1).toLowerCase() +
                                 " database.");
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

        if (m_restoreAgent != null) {
            // start restore process
            m_restoreAgent.restore();
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
            messenger.rejoinForeignHostPrepare(rejoinHostId, addr, m_catalogContext.catalogCRC,
                    m_catalogContext.deploymentCRC, liveHosts, m_commandLog.getFaultSequenceNumber(),
                    m_catalogContext.catalogVersion, m_catalogContext.m_transactionId);
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
            String newCatalogURL,
            int expectedCatalogVersion,
            long currentTxnId,
            long deploymentCRC)
    {
        synchronized(m_catalogUpdateLock) {
            // A site is catching up with catalog updates
            if (currentTxnId <= m_catalogContext.m_transactionId) {
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
                m_catalogContext.update(currentTxnId, newCatalogURL, diffCommands, true, deploymentCRC);
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
            m_catalogContext = m_catalogContext.update( m_catalogContext.m_transactionId, CatalogContext.NO_PATH,
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
        m_recovering = false;
        for (ClientInterface intf : getClientInterfaces()) {
            intf.mayActivateSnapshotDaemon();
        }
        hostLog.info(
                "Node recovery completed after " + delta + " seconds with " + megabytes +
                " megabytes transferred at a rate of " +
                megabytesPerSecond + " megabytes/sec");
        hostLog.info("Logging host recovery completion to ZK");
        try {
            try {
                m_zk.create("/unfaulted_hosts", null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {}
            m_zk.create("/unfaulted_hosts/" + m_messenger.getHostId(), null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            hostLog.fatal("Unable to log host recovery completion to ZK", e);
            VoltDB.crashVoltDB();
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
    public void onRestoreCompletion(long txnId, boolean initCommandLog) {

        /*
         * Command log is already initialized if this is a rejoin
         */
        if (initCommandLog) {
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
}
