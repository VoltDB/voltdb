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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.net.UnknownHostException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

import org.voltdb.RealVoltDB.RejoinCallback;
import org.voltdb.agreement.AgreementSite;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Site;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HeartbeatType;
import org.voltdb.compiler.deploymentfile.UsersType.User;
import org.voltdb.export.ExportManager;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.fault.NodeFailureFault;
import org.voltdb.fault.VoltFault.FaultType;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.HostMessenger;
import org.voltdb.messaging.Mailbox;
import org.voltdb.network.VoltNetwork;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;

/**
 * This breaks up VoltDB initialization tasks into discrete units.
 * To add a task, create a nested subclass of InitWork in the Inits class.
 * You can specify other tasks as dependencies in the constructor.
 *
 */
public class Inits {

    private static final VoltLogger hostLog = new VoltLogger("HOST");

    final RealVoltDB m_rvdb;
    final VoltDB.Configuration m_config;
    final boolean m_isRejoin;
    DeploymentType m_deployment = null;

    final Map<Class<? extends InitWork>, InitWork> m_jobs = new HashMap<Class<? extends InitWork>, InitWork>();
    final PriorityBlockingQueue<InitWork> m_readyJobs = new PriorityBlockingQueue<InitWork>();
    final int m_threadCount;
    final Set<Thread> m_initThreads = new HashSet<Thread>();

    abstract class InitWork implements Comparable<InitWork>, Runnable {
        Set<Class<? extends InitWork>> m_blockers = new HashSet<Class<? extends InitWork>>();
        Set<Class<? extends InitWork>> m_blockees = new HashSet<Class<? extends InitWork>>();

        protected void dependsOn(Class<? extends InitWork> cls) {
            m_blockers.add(cls);
        }

        @Override
        public int compareTo(InitWork iw) {
            return m_blockees.size() - iw.m_blockees.size();
        }
    }

    class InitializerWorker implements Runnable {
        @Override
        public void run() {
            while (true) {
                InitWork iw = null;
                try {
                    iw = m_readyJobs.take();
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                    VoltDB.crashVoltDB();
                }
                if (iw instanceof COMPLETION_WORK)
                    return;
                iw.run();
                completeInitWork(iw);
            }
        }
    }

    Inits(RealVoltDB rvdb, int threadCount) {
        m_rvdb = rvdb;
        m_config = rvdb.m_config;
        // determine if this is a rejoining node
        // (used for license check and later the actual rejoin)
        m_isRejoin = m_config.m_rejoinToHostAndPort != null;
        m_threadCount = threadCount;

        // find all the InitWork subclasses using reflection and load them up
        Class<?>[] declaredClasses = Inits.class.getDeclaredClasses();
        for (Class<?> cls : declaredClasses) {
            // skip base classes and fake classes
            if (cls == InitWork.class) continue;
            if (cls == COMPLETION_WORK.class) continue;

            if (InitWork.class.isAssignableFrom(cls)) {
                InitWork instance = null;
                try {
                    Constructor<?> constructor = cls.getDeclaredConstructor(Inits.class);
                    instance = (InitWork) constructor.newInstance(this);
                } catch (Exception e) {
                    hostLog.fatal("Critical error loading class " + cls.getName(), e);
                    VoltDB.crashVoltDB();
                }
                m_jobs.put(instance.getClass(), instance);
            }
        }

        // make blockers and blockees symmetrical
        for (InitWork iw : m_jobs.values()) {
            for (Class<? extends InitWork> cls : iw.m_blockers) {
                InitWork blocker = m_jobs.get(cls);
                blocker.m_blockees.add(iw.getClass());
            }
        }

        // collect initially ready jobs
        List<Class<? extends InitWork>> toRemove = new ArrayList<Class<? extends InitWork>>();
        for (Entry<Class<? extends InitWork>, InitWork> e : m_jobs.entrySet()) {
            if (e.getValue().m_blockers.size() == 0) {
                toRemove.add(e.getKey());
                m_readyJobs.add(e.getValue());
            }
        }
    }

    synchronized void completeInitWork(InitWork iw) {
        m_jobs.remove(iw.getClass());
        for (Class<? extends InitWork> cls : iw.m_blockees) {
            InitWork blockee = m_jobs.get(cls);
            boolean success = blockee.m_blockers.remove(iw.getClass());
            assert(success);
            if (blockee.m_blockers.size() == 0) {
                m_readyJobs.add(blockee);
            }
        }
        if (m_jobs.size() == 0) {
            // tell all the threads to stop
            for (int i = 0; i < m_threadCount; ++i) {
                m_readyJobs.add(new COMPLETION_WORK());
            }
        }
    }

    void doInitializationWork() {
        for (int i = 0; i < m_threadCount - 1; ++i) {
            Thread t = new Thread(new InitializerWorker());
            t.start();
            m_initThreads.add(t);
        }
        new InitializerWorker().run();
        // when any worker finishes, that means they're all done
    }

    /**
     * Magic bit of work that tells init threads to stop
     */
    class COMPLETION_WORK extends InitWork {
        @Override
        public void run() {}
    }

    ///////////////////////////////////////////////////////////////
    //
    // ACTUAL INITIALIZERS BEGIN HERE
    //
    ///////////////////////////////////////////////////////////////

    class CollectPlatformInfo extends InitWork {
        @Override
        public void run() {
            PlatformProperties.getPlatformProperties();
        }
    }

    class ReadDeploymentFile extends InitWork {
        @Override
        public void run() {
            m_deployment = CatalogUtil.parseDeployment(m_config.m_pathToDeployment);
            // wasn't a valid xml deployment file
            if (m_deployment == null) {
                hostLog.error("Not a valid XML deployment file at URL: " + m_config.m_pathToDeployment);
                VoltDB.crashVoltDB();
            }
            m_rvdb.m_deployment = m_deployment;
            HeartbeatType hbt = m_deployment.getHeartbeat();
            if (hbt != null)
                m_config.m_deadHostTimeoutMS = hbt.getTimeout();

            // create a dummy catalog to load deployment info into
            Catalog catalog = new Catalog();
            Cluster cluster = catalog.getClusters().add("cluster");
            Database db = cluster.getDatabases().add("database");

            // create groups as needed for users
            if (m_deployment.getUsers() != null) {
                for (User user : m_deployment.getUsers().getUser()) {
                    String groupsCSV = user.getGroups();
                    String[] groups = groupsCSV.split(",");
                    for (String group : groups) {
                        if (db.getGroups().get(group) == null) {
                            db.getGroups().add(group);
                        }
                    }
                }
            }

            long depCRC = CatalogUtil.compileDeploymentAndGetCRC(catalog, m_deployment, true);
            assert(depCRC != -1);
            m_rvdb.m_catalogContext = new CatalogContext(0, catalog, CatalogContext.NO_PATH, depCRC, 0, -1);
        }
    }

    class LoadCatalog extends InitWork {
        LoadCatalog() {
            dependsOn(ReadDeploymentFile.class);
            dependsOn(JoinAndInitNetwork.class);
        }

        @Override
        public void run() {
            // Initialize the catalog and some common shortcuts
            if (m_config.m_pathToCatalog.startsWith("http")) {
                hostLog.info("Loading application catalog jarfile from " + m_config.m_pathToCatalog);
            }
            else {
                File f = new File(m_config.m_pathToCatalog);
                hostLog.info("Loading application catalog jarfile from " + f.getAbsolutePath());
            }

            m_rvdb.m_serializedCatalog = CatalogUtil.loadCatalogFromJar(m_config.m_pathToCatalog, hostLog);
            if ((m_rvdb.m_serializedCatalog == null) || (m_rvdb.m_serializedCatalog.length() == 0))
                VoltDB.crashVoltDB();

            /* N.B. node recovery requires discovering the current catalog version. */
            Catalog catalog = new Catalog();
            catalog.execute(m_rvdb.m_serializedCatalog);

            // note if this fails it will print an error first
            try {
                m_rvdb.m_depCRC = CatalogUtil.compileDeploymentAndGetCRC(catalog, m_deployment, true);
                if (m_rvdb.m_depCRC < 0)
                    System.exit(-1);
            } catch (Exception e) {
                hostLog.fatal("Error parsing deployment file", e);
                System.exit(-1);
            }

            // copy the existing cluster up/down status to the new catalog
            Cluster newCluster = catalog.getClusters().get("cluster");
            Cluster oldCluster = m_rvdb.m_catalogContext.cluster;
            for (Site site : oldCluster.getSites()) {
                newCluster.getSites().get(site.getTypeName()).setIsup(site.getIsup());
            }

            // if the dummy catalog doesn't have a 0 txnid (like from a rejoin), use that one
            long existingCatalogTxnId = m_rvdb.m_catalogContext.m_transactionId;
            int existingCatalogVersion = m_rvdb.m_messenger.getDiscoveredCatalogVersion();

            m_rvdb.m_serializedCatalog = catalog.serialize();
            m_rvdb.m_catalogContext = new CatalogContext(
                    existingCatalogTxnId,
                    catalog, m_config.m_pathToCatalog, m_rvdb.m_depCRC, existingCatalogVersion, -1);
        }
    }

    class EnforceLicensing extends InitWork {
        EnforceLicensing() {
            dependsOn(ReadDeploymentFile.class);
        }

        @Override
        public void run() {
            // If running commercial code (of value) and not rejoining, enforce licensing.
            if (m_config.m_isEnterprise && !m_isRejoin) {
                assert(m_config != null);
                if (!MiscUtils.validateLicense(m_config.m_pathToLicense, m_deployment.getCluster().getHostcount())) {
                    // validateLicense logs as appropriate. Exit call is here for testability.

                    // TOOD: Stop running here!
                    VoltDB.crashVoltDB();
                }
            }
        }
    }

    class SetupCommandLogging extends InitWork {
        SetupCommandLogging() {
            dependsOn(LoadCatalog.class);
        }

        @Override
        public void run() {

            boolean logEnabled = false;
            if ((m_deployment.getCommandlog() != null) &&
                    (m_deployment.getCommandlog().isEnabled())) {
                logEnabled = true;
            }

            if (logEnabled) {
                if (!m_config.m_isEnterprise) {
                    hostLog.warn(
                            "Command logging requested in deployment file but can't be enabled in Community Edition.");
                }
                else {
                    try {
                        Class<?> loggerClass = MiscUtils.loadProClass("org.voltdb.CommandLogImpl",
                                                                   "Command logging", false);
                        if (loggerClass != null) {
                            m_rvdb.m_commandLog = (CommandLog)loggerClass.newInstance();
                        }
                    } catch (InstantiationException e) {
                        hostLog.fatal("Unable to instantiate command log", e);
                        VoltDB.crashVoltDB();
                    } catch (IllegalAccessException e) {
                        hostLog.fatal("Unable to instantiate command log", e);
                        VoltDB.crashVoltDB();
                    }
                }
            }
        }
    }

    class StartHTTPServer extends InitWork {
        StartHTTPServer() {
            dependsOn(ReadDeploymentFile.class);
        }

        @Override
        public void run() {
            // start the httpd dashboard/jsonapi. A port value of -1 means disabled
            // by the deployment.xml configuration.
            int httpPort = -1;
            m_rvdb.m_jsonEnabled = false;
            if (m_deployment.getHttpd().isEnabled()) {
                httpPort = m_deployment.getHttpd().getPort();
                m_rvdb.m_jsonEnabled = m_deployment.getHttpd().getJsonapi().isEnabled();
            }

            // if not set by the user, just find a free port
            if (httpPort == 0) {
                // if not set by the user, start at 8080
                httpPort = 8080;

                for (; true; httpPort++) {
                    try {
                        m_rvdb.m_adminListener = new HTTPAdminListener(m_rvdb.m_jsonEnabled, httpPort);
                        break;
                    } catch (Exception e1) {}
                }
                if (httpPort == 8081)
                    m_rvdb.m_httpPortExtraLogMessage = "HTTP admin console unable to bind to port 8080";
                else if (httpPort > 8081)
                    m_rvdb.m_httpPortExtraLogMessage = "HTTP admin console unable to bind to ports 8080 through " + String.valueOf(httpPort - 1);
            }
            else if (httpPort != -1) {
                try {
                    m_rvdb.m_adminListener = new HTTPAdminListener(m_rvdb.m_jsonEnabled, httpPort);
                } catch (Exception e1) {
                    hostLog.info("HTTP admin console unable to bind to port " + httpPort + ". Exiting.");
                    System.exit(-1);
                }
            }
            m_config.m_httpPort = httpPort;
        }
    }

    class SetupAdminMode extends InitWork {
        SetupAdminMode() {
            dependsOn(ReadDeploymentFile.class);
            dependsOn(JoinAndInitNetwork.class);
        }

        @Override
        public void run() {
            int adminPort = VoltDB.DEFAULT_ADMIN_PORT;;

            // See if we should bring the server up in admin mode
            if (m_deployment.getAdminMode() != null) {
                // rejoining nodes figure out admin mode from other nodes
                if (m_isRejoin == false) {
                    if (m_deployment.getAdminMode().isAdminstartup()) {
                        m_rvdb.setStartMode(OperationMode.PAUSED);
                    }
                }

                // set the adminPort from the deployment file
                adminPort = m_deployment.getAdminMode().getPort();
            }

            // allow command line override
            if (m_config.m_adminPort > 0)
                adminPort = m_config.m_adminPort;
            // other places might use config to figure out the port
            m_config.m_adminPort = adminPort;
        }
    }

    class InitFaultManager extends InitWork {
        InitFaultManager() {
            dependsOn(LoadCatalog.class);
        }

        @Override
        public void run() {
            // requires a catalog context.
            m_rvdb.m_faultManager = new FaultDistributor(m_rvdb);
            // Install a handler for NODE_FAILURE faults to update the catalog
            // This should be the first handler to run when a node fails
            m_rvdb.m_faultManager.registerFaultHandler(NodeFailureFault.NODE_FAILURE_CATALOG,
                    m_rvdb.m_faultHandler,
                    FaultType.NODE_FAILURE);
            if (!m_rvdb.m_faultManager.testPartitionDetectionDirectory(
                    m_rvdb.m_catalogContext.cluster.getFaultsnapshots().get("CLUSTER_PARTITION"))) {
                VoltDB.crashVoltDB();
            }
        }
    }

    class InitExport extends InitWork {
        InitExport() {
            dependsOn(LoadCatalog.class);
            dependsOn(JoinAndInitNetwork.class);
        }

        @Override
        public void run() {
            // Let the Export system read its configuration from the catalog.
            try {
                ExportManager.initialize(m_rvdb.m_myHostId, m_rvdb.m_catalogContext, m_isRejoin);
            } catch (ExportManager.SetupException e) {
                hostLog.l7dlog(Level.FATAL, LogKeys.host_VoltDB_ExportInitFailure.name(), e);
                System.exit(-1);
            }
        }
    }

    class InitHashinator extends InitWork {
        InitHashinator() {
            dependsOn(ReadDeploymentFile.class);
        }

        @Override
        public void run() {
            int hostCount = m_deployment.getCluster().getHostcount();
            int kFactor = m_deployment.getCluster().getKfactor();
            int sitesPerHost = m_deployment.getCluster().getSitesperhost();

            // Initialize the complex partitioning scheme
            TheHashinator.initialize((hostCount * sitesPerHost) / (kFactor + 1));
        }
    }

    class CollectLocalNetworkMetadata extends InitWork {
        CollectLocalNetworkMetadata() {
            dependsOn(StartHTTPServer.class);
            dependsOn(SetupAdminMode.class);
        }

        @Override
        public void run() {
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
            localMetadata += ":" + Integer.valueOf(m_config.m_port);
            localMetadata += ":" + Integer.valueOf(m_config.m_adminPort);
            localMetadata += ":" + Integer.valueOf(m_config.m_httpPort); // json
            // possibly atomic swap from null to realz
            m_rvdb.m_localMetadata = localMetadata;
        }
    }

    class JoinAndInitNetwork extends InitWork {
        JoinAndInitNetwork() {
            dependsOn(ReadDeploymentFile.class);
        }

        @Override
        public void run() {
            // Prepare the network socket manager for work
            m_rvdb.m_network = new VoltNetwork();

            String leaderAddress = m_deployment.getCluster().getLeader();
            int numberOfNodes = m_deployment.getCluster().getHostcount();
            long depCRC = CatalogUtil.getDeploymentCRC(m_config.m_pathToDeployment);

            if (!m_isRejoin) {
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
                m_rvdb.m_messenger = new HostMessenger(m_rvdb.m_network, leader,
                        numberOfNodes, 0, depCRC, hostLog);
                Object retval[] = m_rvdb.m_messenger.waitForGroupJoin();
                m_rvdb.m_instanceId = new Object[] { retval[0], retval[1] };
            }
            else {
                // rejoin case
                m_rvdb.m_downHosts.addAll(initializeForRejoin(m_config, numberOfNodes, depCRC));
            }

            // Use the host messenger's hostId.
            m_rvdb.m_myHostId = m_rvdb.m_messenger.getHostId();

            // make sure the local entry for metadata is current
            // it's possible it could get overwritten in a rejoin scenario
            m_rvdb.m_clusterMetadata.put(m_rvdb.m_myHostId, m_rvdb.m_localMetadata);

            if (m_isRejoin) {
                /**
                 * Whatever hosts were reported as being down on rejoin should
                 * be reported to the fault manager so that the fault can be distributed.
                 * The execution sites were informed on construction so they don't have
                 * to go through the agreement process.
                 */
                for (Integer downHost : m_rvdb.m_downHosts) {
                    m_rvdb.m_downNonExecSites.addAll(m_rvdb.m_catalogContext.siteTracker.getNonExecSitesForHost(downHost));
                    m_rvdb.m_downSites.addAll(m_rvdb.m_catalogContext.siteTracker.getNonExecSitesForHost(downHost));
                    m_rvdb.m_faultManager.reportFault(
                            new NodeFailureFault(
                                downHost,
                                m_rvdb.m_catalogContext.siteTracker.getNonExecSitesForHost(downHost),
                                "UNKNOWN"));
                }
                try {
                    m_rvdb.m_faultHandler.m_waitForFaultReported.acquire(m_rvdb.m_downHosts.size());
                } catch (InterruptedException e) {
                    VoltDB.crashVoltDB();
                }
                ExecutionSite.recoveringSiteCount.set(
                        m_rvdb.m_catalogContext.siteTracker.getLiveExecutionSitesForHost(m_rvdb.m_messenger.getHostId()).size());
                m_rvdb.m_downSites.addAll(m_rvdb.m_catalogContext.siteTracker.getAllSitesForHost(m_rvdb.m_messenger.getHostId()));
            }

            m_rvdb.m_catalogContext.m_transactionId = m_rvdb.m_messenger.getDiscoveredCatalogTxnId();
            assert(m_rvdb.m_messenger.getDiscoveredCatalogTxnId() != 0);
        }

        HashSet<Integer> initializeForRejoin(VoltDB.Configuration config, int numberOfNodes, long deploymentCRC) {
            // sensible defaults (sorta)
            String rejoinHostCredentialString = null;
            String rejoinHostAddressString = null;

            //Client interface port of node that will receive @Rejoin invocation
            int rejoinPort = config.m_port;
            String rejoinHost = null;
            String rejoinUser = null;
            String rejoinPass = null;

            // this will cause the ExecutionSites to start in recovering mode
            m_rvdb.m_recovering = true;

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
            m_rvdb.m_messenger = new HostMessenger(m_rvdb.m_network, listener, numberOfNodes, 0, deploymentCRC, hostLog);

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
                hostLog.fatal("Problem connecting client: " + e.getMessage());
                VoltDB.crashVoltDB();
            }

            Object retval[] = m_rvdb.m_messenger.waitForGroupJoin(60 * 1000);

            m_rvdb.m_instanceId = new Object[] { retval[0], retval[1] };

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
    }

    class InitAgreementSite extends InitWork {
        InitAgreementSite() {
            dependsOn(LoadCatalog.class);
            dependsOn(JoinAndInitNetwork.class);
        }

        @Override
        public void run() {
            /*
             * Initialize the agreement site with the same site id as the CI/SDTXN,
             * but a different mailbox.
             */
            int myAgreementSiteId = -1;
            HashSet<Integer> agreementSiteIds = new HashSet<Integer>();
            int myAgreementInitiatorId = 0;
            for (Site site : m_rvdb.m_catalogContext.siteTracker.getAllSites()) {
                int sitesHostId = Integer.parseInt(site.getHost().getTypeName());
                int currSiteId = Integer.parseInt(site.getTypeName());

                if (sitesHostId == m_rvdb.m_myHostId) {
                    hostLog.l7dlog( Level.TRACE, LogKeys.org_voltdb_VoltDB_CreatingLocalSite.name(), new Object[] { currSiteId }, null);
                    m_rvdb.m_messenger.createLocalSite(currSiteId);
                }
                // Create an agreement site for every initiator
                if (site.getIsexec() == false) {
                    agreementSiteIds.add(currSiteId);
                    if (sitesHostId == m_rvdb.m_myHostId) {
                        myAgreementSiteId = currSiteId;
                        myAgreementInitiatorId = site.getInitiatorid();
                    }
                }
            }

            assert(m_rvdb.m_agreementSite == null);
            assert(myAgreementSiteId != -1);
            Mailbox agreementMailbox =
                    m_rvdb.m_messenger.createMailbox(myAgreementSiteId, VoltDB.AGREEMENT_MAILBOX_ID, false);
            try {
                m_rvdb.m_agreementSite =
                    new AgreementSite(
                            myAgreementSiteId,
                            agreementSiteIds,
                            myAgreementInitiatorId,
                            m_rvdb.m_downNonExecSites,
                            agreementMailbox,
                            new InetSocketAddress(
                                    m_config.m_zkInterface.split(":")[0],
                                    Integer.parseInt(m_config.m_zkInterface.split(":")[1])),
                            m_rvdb.m_faultManager,
                            m_rvdb.m_recovering);
                m_rvdb.m_agreementSite.start();
            } catch (Exception e) {
                hostLog.fatal(null, e);
                System.exit(-1);
            }
        }
    }
}
