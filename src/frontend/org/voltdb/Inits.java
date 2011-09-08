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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.PriorityBlockingQueue;

import org.voltdb.agreement.AgreementSite;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.export.ExportManager;
import org.voltdb.logging.Level;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.Mailbox;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.LogKeys;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.Pair;
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
                //hostLog.info("Running InitWorker: " + iw.getClass().getName());
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
        m_deployment = rvdb.m_deployment;

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

    /*
        The Inits functions are run via reflection in an order
        that satisfies all dependencies.

        Per John, these all run after the socket joiner complete.

        The dependency DAG (a depends on <- b) is:

        SetupAdminMode
        StartHTTPServer
        InitHashinator
        InitAgreementSite

        CreateRestoreAgentAndPlan <- InitAgreementSite
        DistributeCatalog <- InitAgreementSite, CreateRestoreAgentAndPlan
        EnforceLicensing <- CreateRestoreAgentAndPlan
        LoadCatalog <- DistributeCatalog
        SetupCommandLogging <- LoadCatalog
        InitExport <- LoadCatalog

     */


    class CollectPlatformInfo extends InitWork {
        @Override
        public void run() {
            PlatformProperties.getPlatformProperties();
        }
    }

    class DistributeCatalog extends InitWork {
        DistributeCatalog() {
            dependsOn(InitAgreementSite.class);
            dependsOn(CreateRestoreAgentAndPlan.class);
        }

        @Override
        public void run() {
            // if I'm the leader, send out the catalog
            if (m_rvdb.m_myHostId == m_rvdb.m_hostIdWithStartupCatalog) {
                final int MAX_CATALOG_SIZE = 40 * 1024 * 1024; // 40mb

                if (m_rvdb.m_pathToStartupCatalog == null) {
                    VoltDB.crashGlobalVoltDB("The catalog file location is missing, " +
                                             " please see usage for more information",
                                             false, null);
                }

                try {
                    InputStream fin = null;
                    try {
                        URL url = new URL(m_rvdb.m_pathToStartupCatalog);
                        fin = url.openStream();
                    } catch (MalformedURLException ex) {
                        // Invalid URL. Try as a file.
                        fin = new FileInputStream(m_rvdb.m_pathToStartupCatalog);
                    }
                    byte[] buffer = new byte[MAX_CATALOG_SIZE];
                    int readBytes = 0;
                    int totalBytes = 0;
                    try {
                        while (readBytes >= 0) {
                            totalBytes += readBytes;
                            readBytes = fin.read(buffer, totalBytes, buffer.length - totalBytes - 1);
                        }
                    } finally {
                        fin.close();
                    }
                    byte[] catalogBytes = Arrays.copyOf(buffer, totalBytes);
                    hostLog.debug(String.format("Sending %d catalog bytes", catalogBytes.length));

                    m_rvdb.m_messenger.sendCatalog(catalogBytes);
                }
                catch (IOException e) {
                    VoltDB.crashGlobalVoltDB("Unable to distribute catalog.", false, e);
                }
            }
        }
    }

    class LoadCatalog extends InitWork {
        LoadCatalog() {
            dependsOn(DistributeCatalog.class);
        }

        @Override
        public void run() {
            // wait till RealVoltDB says a catalog has been found
            try {
                m_rvdb.m_hasCatalog.await();
            } catch (InterruptedException e1) {
                hostLog.fatal("System was interrupted while waiting for a catalog.");
                VoltDB.crashVoltDB();
            }

            // Initialize the catalog and some common shortcuts
            if (m_config.m_pathToCatalog.startsWith("http")) {
                hostLog.info("Loading application catalog jarfile from " + m_config.m_pathToCatalog);
            }
            else {
                File f = new File(m_config.m_pathToCatalog);
                hostLog.info("Loading application catalog jarfile from " + f.getAbsolutePath());
            }

            byte[] catalogBytes = null;
            try {
                catalogBytes = CatalogUtil.toBytes(new File(m_config.m_pathToCatalog));
            } catch (IOException e) {
                hostLog.fatal("Failed to read catalog: " + e.getMessage());
                VoltDB.crashVoltDB();
            }
            m_rvdb.m_serializedCatalog = CatalogUtil.loadCatalogFromJar(catalogBytes, hostLog);
            if ((m_rvdb.m_serializedCatalog == null) || (m_rvdb.m_serializedCatalog.length() == 0))
                VoltDB.crashVoltDB();

            /* N.B. node recovery requires discovering the current catalog version. */
            Catalog catalog = new Catalog();
            catalog.execute(m_rvdb.m_serializedCatalog);

            // note if this fails it will print an error first
            try {
                m_rvdb.m_depCRC = CatalogUtil.compileDeploymentAndGetCRC(catalog, m_deployment,
                                                                         true, false);
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
                    catalog, catalogBytes, m_rvdb.m_depCRC, existingCatalogVersion, -1);
        }
    }

    class EnforceLicensing extends InitWork {
        EnforceLicensing() {
            // Requires the network to be established in order to
            // cleanly shutdown the cluster. The network is created
            // in CreateRestoreAgentAndPlan ...
            dependsOn(CreateRestoreAgentAndPlan.class);
        }

        @Override
        public void run() {
            // If running commercial code (of value) and not rejoining, enforce licensing.
            // Make the leader the only license enforcer.
            boolean isLeader = (m_rvdb.m_myHostId == 0);
            if (m_config.m_isEnterprise && isLeader && !m_isRejoin) {
                assert(m_config != null);
                if (!MiscUtils.validateLicense(m_config.m_pathToLicense, m_deployment.getCluster().getHostcount())) {
                    // validateLicense logs as appropriate. Exit call is here for testability.
                    VoltDB.crashGlobalVoltDB("VoltDB license constraints are not met.", false, null);
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
        }

        @Override
        public void run() {
            // start the httpd dashboard/jsonapi. A port value of -1 means disabled
            // by the deployment.xml configuration.
            int httpPort = -1;
            m_rvdb.m_jsonEnabled = false;
            if ((m_deployment.getHttpd() != null) && (m_deployment.getHttpd().isEnabled())) {
                httpPort = m_deployment.getHttpd().getPort();
                if (m_deployment.getHttpd().getJsonapi() != null) {
                    m_rvdb.m_jsonEnabled = m_deployment.getHttpd().getJsonapi().isEnabled();
                }
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

    class InitExport extends InitWork {
        InitExport() {
            dependsOn(LoadCatalog.class);
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

    class InitAgreementSite extends InitWork {
        InitAgreementSite() {
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

    class CreateRestoreAgentAndPlan extends InitWork {
        public CreateRestoreAgentAndPlan() {
            dependsOn(InitAgreementSite.class);
        }

        @Override
        public void run() {
            if (!m_isRejoin && !m_config.m_isRejoinTest) {
                m_rvdb.startNetworkAndCreateZKClient();

                String snapshotPath = null;
                if (m_rvdb.m_catalogContext.cluster.getDatabases().get("database").getSnapshotschedule().get("default") != null) {
                    snapshotPath = m_rvdb.m_catalogContext.cluster.getDatabases().get("database").getSnapshotschedule().get("default").getPath();
                }

                int lowestSite = m_rvdb.m_catalogContext.siteTracker.getLowestLiveNonExecSiteId();
                int lowestHostId = m_rvdb.m_catalogContext.siteTracker.getHostForSite(lowestSite);

                int[] allPartitions = new int[m_rvdb.m_catalogContext.numberOfPartitions];
                int i = 0;
                for (Partition p : m_rvdb.m_catalogContext.cluster.getPartitions()) {
                    allPartitions[i++] = Integer.parseInt(p.getTypeName());
                }

                org.voltdb.catalog.CommandLog cl = m_rvdb.m_catalogContext.cluster.getLogconfig().get("log");

                try {
                    m_rvdb.m_restoreAgent = new RestoreAgent(
                                                      m_rvdb.m_zk,
                                                      m_rvdb.getSnapshotCompletionMonitor(),
                                                      m_rvdb,
                                                      m_rvdb.m_myHostId,
                                                      m_config.m_startAction,
                                                      m_rvdb.m_catalogContext.numberOfPartitions,
                                                      cl.getEnabled(),
                                                      cl.getLogpath(),
                                                      cl.getInternalsnapshotpath(),
                                                      snapshotPath,
                                                      lowestHostId,
                                                      allPartitions,
                                                      m_rvdb.m_catalogContext.siteTracker.getAllLiveHosts());
                } catch (IOException e) {
                    hostLog.fatal("Unable to establish a ZooKeeper connection: " +
                                  e.getMessage());
                    VoltDB.crashVoltDB();
                }

                m_rvdb.m_restoreAgent.setCatalogContext(m_rvdb.m_catalogContext);
                // Generate plans and get (hostID, catalogPath) pair
                Pair<Integer,String> catalog = m_rvdb.m_restoreAgent.findRestoreCatalog();

                // if the restore agent found a catalog, set the following info
                // so the right node can send it out to the others
                if (catalog != null) {
                    hostLog.debug("Found catalog to load on host " + catalog.getFirst() +
                                  ": " + catalog.getSecond());
                    m_rvdb.m_hostIdWithStartupCatalog = catalog.getFirst().intValue();
                    assert(m_rvdb.m_hostIdWithStartupCatalog >= 0);
                    m_rvdb.m_pathToStartupCatalog = catalog.getSecond();
                    assert(m_rvdb.m_pathToStartupCatalog != null);
                }
            }
        }
    }
}
