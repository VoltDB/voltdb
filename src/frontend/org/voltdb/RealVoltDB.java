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
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.elt.ELTManager;
import org.voltdb.fault.FaultDistributor;
import org.voltdb.messaging.Messenger;
import org.voltdb.messaging.impl.HostMessenger;
import org.voltdb.network.VoltNetwork;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.DumpManager;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.VoltLoggerFactory;
import org.voltdb.utils.VoltSampler;

public class RealVoltDB implements VoltDBInterface
{
    private static final Logger log =
        Logger.getLogger(VoltDB.class.getName(), VoltLoggerFactory.instance());
    private static final Logger hostLog =
        Logger.getLogger("HOST", VoltLoggerFactory.instance());

    /**
     * A class that instantiates an ExecutionSite and then waits for notification before
     * running the execution site. Would it be better if this extended Thread
     * so we don't have to have m_runners and m_siteThreads?
     */
    private static class ExecutionSiteRunner implements Runnable {

        private volatile boolean m_isSiteCreated = false;
        private final int m_siteId;
        private final CatalogContext m_context;
        private final String m_serializedCatalog;
        private volatile ExecutionSite m_siteObj;

        public ExecutionSiteRunner(final int siteId, final CatalogContext context, final String serializedCatalog) {
            m_siteId = siteId;
            m_context = context;
            m_serializedCatalog = serializedCatalog;
        }

        @Override
        public void run() {
            m_siteObj =
                new ExecutionSite(m_siteId, m_context, m_serializedCatalog);
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
    private String m_versionString = "0.6.01";
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

    /**
     * Initialize all the global components, then initialize all the m_sites.
     */
    public synchronized void initialize(VoltDB.Configuration config) {
        hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartupString.name(), null);

        m_faultManager = new FaultDistributor();

        // start the dumper thread
        if (config.listenForDumpRequests)
            DumpManager.init();

        readBuildInfo();
        m_config = config;

        // start the admin console
        int port;
        for (port = 8080; true; port++) {
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

        final String serializedCatalog = CatalogUtil.loadCatalogFromJar(m_config.m_pathToCatalog, hostLog);
        if ((serializedCatalog == null) || (serializedCatalog.length() == 0))
            VoltDB.crashVoltDB();

        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);
        m_catalogContext = new CatalogContext(catalog, m_config.m_pathToCatalog);
        final SnapshotSchedule schedule = m_catalogContext.database.getSnapshotschedule().get("default");

        /*
         * The lowest non-exec siteId (ClientInterface) is tasked with
         * running a SnapshotDaemon.
         */
        int lowestNonExecSiteId = -1;
        for (Site site : m_catalogContext.sites) {
            if (!site.getIsexec()) {
                if (lowestNonExecSiteId == -1) {
                    lowestNonExecSiteId = Integer.parseInt(site.getTypeName());
                } else {
                    lowestNonExecSiteId = Math.min(lowestNonExecSiteId, Integer.parseInt(site.getTypeName()));
                }
            }
        }

        // Initialize the complex partitioning scheme
        TheHashinator.initialize(catalog);

        // Prepare the network socket manager for work
        m_network = new VoltNetwork( new Runnable[] { new Runnable() {
            @Override
            public void run() {
                for (final ClientInterface ci : m_clientInterfaces) {
                    ci.processPeriodicWork();
                }
            }
        }});

        // Let the ELT system read its configuration from the catalog.
        try {
            ELTManager.initialize(catalog);
        } catch (ELTManager.SetupException e) {
            hostLog.l7dlog(Level.FATAL, LogKeys.host_VoltDB_ELTInitFailure.name(), e);
            System.exit(-1);
        }

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
        m_messenger = new HostMessenger(m_network, leader, m_catalogContext.numberOfNodes, hostLog);
        m_instanceId = m_messenger.waitForGroupJoin();

        // Use the host messenger's hostId.
        int myHostId = m_messenger.getHostId();

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
        for (Site site : m_catalogContext.sites) {
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
        ExecutionSite siteObj =
            new ExecutionSite(Integer.parseInt(siteForThisThread.getTypeName()), m_catalogContext, serializedCatalog);
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
        for (Site site : m_catalogContext.sites) {
            int sitesHostId = Integer.parseInt(site.getHost().getTypeName());
            int siteId = Integer.parseInt(site.getTypeName());

            // create CI for each local non-EE site
            if ((sitesHostId == myHostId) && (site.getIsexec() == false)) {
                ClientInterface ci =
                    ClientInterface.create(
                            m_network,
                            m_messenger,
                            m_catalogContext,
                            m_catalogContext.numberOfNodes,
                            siteId,
                            site.getInitiatorid(),
                            config.m_port + portOffset++,
                            siteId == lowestNonExecSiteId ? schedule : null);
                m_clientInterfaces.add(ci);
                try {
                    ci.startAcceptingConnections();
                } catch (IOException e) {
                    hostLog.l7dlog( Level.FATAL, LogKeys.host_VoltDB_ErrorStartAcceptingConnections.name(), e);
                    VoltDB.crashVoltDB();
                }
            }
        }

        // Start running the socket handlers
        hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_StartingNetwork.name(), null);
        m_network.start();

        m_messenger.sendReadyMessage();
        m_messenger.waitForAllHostsToBeReady();

        hostLog.l7dlog( Level.INFO, LogKeys.host_VoltDB_ServerCompletedInitialization.name(), null);
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
            String parts[] = sb.toString().split(" ");
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
    public synchronized void shutdown(Thread mainSiteThread) throws InterruptedException {

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
                    siteThread.interrupt();
                    siteThread.join();
                }
            }
        }

        // try to join the main thread (possibly this one)
        if (mainSiteThread != null) {
            if (Thread.currentThread().equals(mainSiteThread) == false) {
                mainSiteThread.interrupt();
                mainSiteThread.join();
            }
        }

        // help the gc along
        m_localSites = null;
        m_currentThreadSite = null;
        m_siteThreads = null;
        m_messenger = null;
        m_runners = null;

        // shut down the network/messaging stuff
        if (m_network != null) {
            //Synchronized so the interruption won't interrupt the network thread
            //while it is waiting for the executor service to shutdown
            m_network.shutdown();
        }

        m_network = null;

        //Also for test code that expects a fresh stats agent
        m_statsAgent = new StatsAgent();

        // The network iterates this list. Clear it after network's done.
        m_clientInterfaces.clear();

        // probably unnecessary
        System.gc();

        m_isRunning = false;
    }

    @Override
    public synchronized void catalogUpdate(String diffCommands,
            String newCatalogURL, int expectedCatalogVersion) {

        if (m_catalogContext.catalog.getSubTreeVersion() != expectedCatalogVersion)
            throw new RuntimeException("Trying to update main catalog context with diff " +
                    "commands generated for an out-of date catalog.");

        m_catalogContext = m_catalogContext.update(newCatalogURL, diffCommands);
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

    public FaultDistributor getFaultDistributor()
    {
        return m_faultManager;
    }

    public synchronized CatalogContext getCatalogContext() {
        return m_catalogContext;
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
}
