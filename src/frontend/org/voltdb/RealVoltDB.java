/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.elt.ELTManager;
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
import org.voltdb.network.VoltNetwork;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DumpManager;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.JarReader;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltSampler;

public class RealVoltDB implements VoltDBInterface
{
    private static final VoltLogger log = new VoltLogger(VoltDB.class.getName());
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private class VoltDBNodeFailureFaultHandler implements FaultHandler
    {
        @Override
        public void faultOccured(Set<VoltFault> faults)
        {
            for (VoltFault fault : faults) {
                if (fault instanceof NodeFailureFault)
                {
                    NodeFailureFault node_fault = (NodeFailureFault) fault;
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
                }
                VoltDB.instance().getFaultDistributor().reportFaultHandled(this, fault);
            }
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

        public ExecutionSiteRunner(final int siteId, final CatalogContext context, final String serializedCatalog) {
            m_siteId = siteId;
            m_serializedCatalog = serializedCatalog;
        }

        @Override
        public void run() {
            Mailbox mailbox = VoltDB.instance().getMessenger()
            .createMailbox(m_siteId, VoltDB.DTXN_MAILBOX_ID);

            m_siteObj =
                new ExecutionSite(VoltDB.instance(),
                                  mailbox,
                                  m_siteId,
                                  m_serializedCatalog,
                                  null);
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
    private String m_versionString = "1.2.01";
    // fields accessed via the singleton
    private HostMessenger m_messenger = null;
    private final ArrayList<ClientInterface> m_clientInterfaces =
        new ArrayList<ClientInterface>();
    private Hashtable<Integer, ExecutionSite> m_localSites;
    private VoltNetwork m_network = null;
    private HTTPAdminListener m_adminListener;
    private Hashtable<Integer, Thread> m_siteThreads;
    private ArrayList<ExecutionSiteRunner> m_runners;
    private ExecutionSite m_currentThreadSite;
    private StatsAgent m_statsAgent = new StatsAgent();
    private FaultDistributor m_faultManager;
    private Object m_instanceId[];
    private PartitionCountStats m_partitionCountStats = null;
    private IOStats m_ioStats = null;
    private StatsManager m_statsManager = null;

    // Synchronize initialize and shutdown.
    private final Object m_startAndStopLock = new Object();

    // Synchronize updates of catalog contexts with context accessors.
    private final Object m_catalogUpdateLock = new Object();

    // add a random number to the sampler output to make it likely to be unique for this process.
    private final VoltSampler m_sampler = new VoltSampler(10, "sample" + String.valueOf(new Random().nextInt() % 10000) + ".txt");
    private final AtomicBoolean m_hasStartedSampler = new AtomicBoolean(false);

    private volatile boolean m_isRunning = false;

    // methods accessed via the singleton
    public void startSampler() {
        if (m_hasStartedSampler.compareAndSet(false, true)) {
            m_sampler.start();
        }
    }

    PeriodicWorkTimerThread fivems;

    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    public void initialize(VoltDB.Configuration config) {
        synchronized(m_startAndStopLock) {
            hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartupString.name(), null);

            m_faultManager = new FaultDistributor();
            // Install a handler for NODE_FAILURE faults to update the catalog
            // This should be the first handler to run when a node fails
            m_faultManager.registerFaultHandler(FaultType.NODE_FAILURE,
                                                new VoltDBNodeFailureFaultHandler(),
                                                NodeFailureFault.NODE_FAILURE_CATALOG);

            // start the dumper thread
            if (config.listenForDumpRequests)
                DumpManager.init();

            readBuildInfo();
            m_config = config;

            // start the admin console
            int port;
            for (port = config.m_httpAdminPort; true; port++) {
                try {
                    m_adminListener = new HTTPAdminListener(port);
                    break;
                } catch (IOException e1) {}
            }
            if (port == 8081)
                hostLog.info("HTTP admin console unable to bind to port 8080");
            else if (port > 8081)
                hostLog.info("HTTP admin console unable to bind to ports 8080 through " + (port - 1));
            hostLog.info("HTTP admin console listening on port " + port);

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

            // get a CRC for the jarfile to check if everyone has the same one
            long catalogCRC = 0;
            try {
                catalogCRC = JarReader.crcForJar(m_config.m_pathToCatalog);
            } catch (IOException e1) {
                VoltDB.crashVoltDB();
            }

            /* N.B. node recovery requires discovering the current catalog version. */
            final int catalogVersion = 0;
            Catalog catalog = new Catalog();
            catalog.execute(serializedCatalog);

            // note if this fails it will print an error first
            if (!CatalogUtil.compileDeployment(catalog, m_config.m_pathToDeployment))
                System.exit(-1);

            serializedCatalog = catalog.serialize();

            m_catalogContext = new CatalogContext(catalog, m_config.m_pathToCatalog, catalogVersion);
            final SnapshotSchedule schedule =
                m_catalogContext.database.getSnapshotschedule().get("default");

            // Initialize the complex partitioning scheme
            TheHashinator.initialize(catalog);

            // Prepare the network socket manager for work
            m_network = new VoltNetwork(/* new Runnable[] { new Runnable() {
                @Override
                public void run() {
                    for (final ClientInterface ci : m_clientInterfaces) {
                        ci.processPeriodicWork();
                    }
                }
            }}*/);

            // Create the intra-cluster mesh
            InetAddress leader = null;
            try {
                leader = InetAddress.getByName(m_catalogContext.cluster.getLeaderaddress());
            } catch (UnknownHostException ex) {
                hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_CouldNotRetrieveLeaderAddress.name(), new Object[] { m_catalogContext.cluster.getLeaderaddress() }, ex);
                throw new RuntimeException(ex);
            }
            // ensure at least one host (catalog compiler should check this too
            if (m_catalogContext.numberOfNodes <= 0) {
                hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_InvalidHostCount.name(), new Object[] { m_catalogContext.numberOfNodes }, null);
                VoltDB.crashVoltDB();
            }

            hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_CreatingVoltDB.name(), new Object[] { m_catalogContext.numberOfNodes, leader }, null);
            m_messenger = new HostMessenger(m_network, leader, m_catalogContext.numberOfNodes, catalogCRC, hostLog);
            m_instanceId = m_messenger.waitForGroupJoin();

            // Use the host messenger's hostId.
            int myHostId = m_messenger.getHostId();

            // Let the ELT system read its configuration from the catalog.
            try {
                ELTManager.initialize(myHostId, m_catalogContext);
            } catch (ELTManager.SetupException e) {
                hostLog.l7dlog(Level.FATAL, LogKeys.host_VoltDB_ELTInitFailure.name(), e);
                System.exit(-1);
            }

            // set up site structure
            m_localSites = new Hashtable<Integer, ExecutionSite>();
            m_siteThreads = new Hashtable<Integer, Thread>();
            m_runners = new ArrayList<ExecutionSiteRunner>();

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
                            ExecutionSiteRunner runner = new ExecutionSiteRunner(siteId, m_catalogContext, serializedCatalog);
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
                                  VoltDB.instance().getMessenger().createMailbox(siteId, VoltDB.DTXN_MAILBOX_ID),
                                  siteId,
                                  serializedCatalog,
                                  null);
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
                        ClientInterface.create(
                                               m_network,
                                               m_messenger,
                                               m_catalogContext,
                                               m_catalogContext.numberOfNodes,
                                               currSiteId,
                                               site.getInitiatorid(),
                                               config.m_port + portOffset++,
                                               schedule);
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

            m_messenger.sendReadyMessage();
            m_messenger.waitForAllHostsToBeReady();

            fivems = new PeriodicWorkTimerThread(m_clientInterfaces,
                                                 m_statsManager);
            fivems.start();

            hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerCompletedInitialization.name(), null);
        }
    }

    public void readBuildInfo() {
        StringBuilder sb = new StringBuilder(64);
        m_buildString = "VoltDB";
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
            m_versionString = parts[0].trim();
            m_buildString = parts[1].trim();
        } catch (Exception ignored) {
            try {
                InputStream buildstringStream = new FileInputStream("version.txt");
                while ((b = (byte) buildstringStream.read()) != -1) {
                    sb.append((char)b);
                }
                m_versionString = sb.toString().trim();
            }
            catch (Exception ignored2) {
                log.l7dlog( Level.ERROR, LogKeys.org_voltdb_VoltDB_FailedToRetrieveBuildString.name(), ignored);
            }
        }
        hostLog.info("Build: " + m_versionString + " " + m_buildString);
    }

    /**
     * Start all the site's event loops. That's it.
     */
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

            // shutdown the web monitoring
            m_adminListener.shutdown(true);

            // shut down the client interface
            for (ClientInterface ci : m_clientInterfaces) {
                ci.shutdown();
            }

            // shut down ELT and its connectors.
            ELTManager.instance().shutdown();

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
    public synchronized String doRejoinPrepare(long currentTxnId, int rejoinHostId, String rejoiningHostname, int portToConnect)
    {
        // another site already did this work.
        if (currentTxnId == lastNodeRejoinPrepare_txnId) {
            return null;
        }
        else if (currentTxnId < lastNodeRejoinPrepare_txnId) {
            throw new RuntimeException("Trying to rejoin (prepare) with an old transaction.");
        }
        System.out.printf("Rejoining node with host id: %s at txnid: %d\n", 0, currentTxnId);
        lastNodeRejoinPrepare_txnId = currentTxnId;

        HostMessenger messenger = getHostMessenger();

        // connect to the joining node, build a foreign host
        InetSocketAddress addr = new InetSocketAddress(rejoiningHostname, portToConnect);
        try {
            messenger.rejoinForeignHostPrepare(rejoinHostId, addr);
            return null;
        } catch (Exception e) {
            e.printStackTrace();
            return e.getMessage();
        }
    }

    /** Last transaction ID at which the rejoin commit took place.
     * Also, use the intrinsic lock to safeguard access from multiple
     * execution site threads */
    private static Long lastNodeRejoinFinish_txnId = 0L;
    @Override
    public synchronized String doRejoinCommitOrRollback(long currentTxnId, boolean commit)
    {
        // another site already did this work.
        if (currentTxnId == lastNodeRejoinFinish_txnId) {
            return null;
        }
        else if (currentTxnId < lastNodeRejoinFinish_txnId) {
            throw new RuntimeException("Trying to rejoin (commit/rollback) with an old transaction.");
        }

        HostMessenger messenger = getHostMessenger();
        if (commit) {
            // put the foreign host into the set of active ones
            messenger.rejoinForeignHostCommit();
        }
        else {
            // clean up any connections made
            messenger.rejoinForeginHostRollback();
        }

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
    @Override
    public void catalogUpdate(
            String diffCommands,
            String newCatalogURL,
            int expectedCatalogVersion,
            long currentTxnId)
    {
        synchronized(m_catalogUpdateLock) {
            // another site already did this work.
            if (currentTxnId == lastCatalogUpdate_txnId) {
                return;
            }
            else if (currentTxnId < lastCatalogUpdate_txnId) {
                throw new RuntimeException("Trying to update main catalog context with an old transaction.");
            }
            else if (m_catalogContext.catalog.getCatalogVersion() != expectedCatalogVersion) {
                throw new RuntimeException("Trying to update main catalog context with diff " +
                "commands generated for an out-of date catalog. Expected catalog version: " +
                expectedCatalogVersion + " does not match actual version: " + m_catalogContext.catalog.getCatalogVersion());
            }

            // 0. update the global context
            lastCatalogUpdate_txnId = currentTxnId;
            m_catalogContext = m_catalogContext.update(newCatalogURL, diffCommands);

            // 1. update the elt manager.
            ELTManager.instance().updateCatalog(m_catalogContext);

            // 2. update client interface (asynchronously)
            //    CI in turn updates the planner thread.
            for (ClientInterface ci : m_clientInterfaces)
                ci.notifyOfCatalogUpdate();
        }
    }

    @Override
    public void clusterUpdate(String diffCommands)
    {
        synchronized(m_catalogUpdateLock)
        {
            m_catalogContext = m_catalogContext.update(CatalogContext.NO_PATH,
                                                       diffCommands);
        }

        for (ClientInterface ci : m_clientInterfaces)
        {
            ci.notifyOfCatalogUpdate();
        }
    }

    public VoltDB.Configuration getConfig()
    {
        return m_config;
    }

    public String getBuildString() {
        return m_buildString;
    }

    public String getVersionString() {
        return m_versionString;
    }

    public Messenger getMessenger() {
        return m_messenger;
    }

    public HostMessenger getHostMessenger() {
        return m_messenger;
    }

    public ArrayList<ClientInterface> getClientInterfaces() {
        return m_clientInterfaces;
    }

    public Hashtable<Integer, ExecutionSite> getLocalSites() {
        return m_localSites;
    }

    public VoltNetwork getNetwork() {
        return m_network;
    }

    public StatsAgent getStatsAgent() {
        return m_statsAgent;
    }

    public FaultDistributorInterface getFaultDistributor()
    {
        return m_faultManager;
    }

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
}
