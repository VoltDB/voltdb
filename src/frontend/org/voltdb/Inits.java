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

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
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

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONObject;

import org.voltcore.utils.Pair;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Site;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.export.ExportManager;
import org.voltcore.agreement.ZKUtil;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
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
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
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
                    VoltDB.crashLocalVoltDB("Critical error loading class " + cls.getName(), true, e);
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
        InitStatsAgent
        SetupReplicationRole
        CreateRestoreAgentAndPlan
        DistributeCatalog <- CreateRestoreAgentAndPlan
        EnforceLicensing <- CreateRestoreAgentAndPlan, SetupReplicationRole
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

                    // publish the catalog bytes to ZK
                    m_rvdb.getHostMessenger().getZK().create(VoltZK.catalogbytes,
                            catalogBytes, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

                }
                catch (IOException e) {
                    VoltDB.crashGlobalVoltDB("Unable to distribute catalog.", false, e);
                }
                catch (org.apache.zookeeper_voltpatches.KeeperException e) {
                    VoltDB.crashGlobalVoltDB("Unable to publish catalog.", false, e);
                }
                catch (InterruptedException e) {
                    VoltDB.crashGlobalVoltDB("Interrupted while publishing catalog.", false, e);
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
            byte[] catalogBytes = null;
            do {
                try {
                    catalogBytes = m_rvdb.getHostMessenger().getZK().getData(VoltZK.catalogbytes, false, null);
                }
                catch (org.apache.zookeeper_voltpatches.KeeperException.NoNodeException e) {
                }
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB("System was interrupted while waiting for a catalog.", false, null);
                }
            } while (catalogBytes == null);

            m_rvdb.m_serializedCatalog = CatalogUtil.loadCatalogFromJar(catalogBytes, hostLog);
            if ((m_rvdb.m_serializedCatalog == null) || (m_rvdb.m_serializedCatalog.length() == 0))
                VoltDB.crashLocalVoltDB("Catalog loading failure", false, null);

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

            try {
                long catalogTxnId = org.voltdb.TransactionIdManager.makeIdFromComponents(System.currentTimeMillis(), 0, 0);
                ZooKeeper zk = m_rvdb.getHostMessenger().getZK();
                zk.create(
                        VoltZK.initial_catalog_txnid,
                        String.valueOf(catalogTxnId).getBytes("UTF-8"),
                        Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT, new ZKUtil.StringCallback(), null);
                ZKUtil.ByteArrayCallback cb = new ZKUtil.ByteArrayCallback();
                zk.getData(VoltZK.initial_catalog_txnid, false, cb, null);
                catalogTxnId = Long.valueOf(new String(cb.getData(), "UTF-8"));
                m_rvdb.m_serializedCatalog = catalog.serialize();
                m_rvdb.m_catalogContext = new CatalogContext(
                        catalogTxnId,
                        catalog, catalogBytes, m_rvdb.m_depCRC, 0, -1);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Error agreeing on starting catalog version", false, e);
            }
        }
    }

    class EnforceLicensing extends InitWork {
        EnforceLicensing() {
            // Requires the network to be established in order to
            // cleanly shutdown the cluster. The network is created
            // in CreateRestoreAgentAndPlan ...
            dependsOn(CreateRestoreAgentAndPlan.class);
            dependsOn(SetupReplicationRole.class);
        }

        @Override
        public void run() {
            // If running commercial code (of value) and not rejoining, enforce licensing.
            // Make the leader the only license enforcer.
            boolean isLeader = (m_rvdb.m_myHostId == 0);
            if (m_config.m_isEnterprise && isLeader && !m_isRejoin) {

                if (!MiscUtils.validateLicense(m_rvdb.getLicenseApi(),
                                               m_deployment.getCluster().getHostcount(),
                                               m_rvdb.getReplicationRole()))
                {
                    // validateLicense logs. Exit call is here for testability.
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
                        VoltDB.crashLocalVoltDB("Unable to instantiate command log", true, e);
                    } catch (IllegalAccessException e) {
                        VoltDB.crashLocalVoltDB("Unable to instantiate command log", true, e);
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
            int adminPort = VoltDB.DEFAULT_ADMIN_PORT;

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

    /**
     * Set the port used for replication.
     * Command line is highest precedence, followed by deployment xml,
     * finally followed by the default value of 5555.
     *
     */
    class PickReplicationPort extends InitWork {
        PickReplicationPort() {
        }

        @Override
        public void run() {
            int replicationPort = VoltDB.DEFAULT_DR_PORT;

            if (m_deployment.getReplication() != null) {
                // set the replication port from the deployment file
                replicationPort = m_deployment.getReplication().getPort();
            }

            // allow command line override
            if (m_config.m_drAgentPortStart > 0)
                replicationPort = m_config.m_drAgentPortStart;

            // other places use config to figure out the port
            m_config.m_drAgentPortStart = replicationPort;
        }
    }

    class SetupReplicationRole extends InitWork {
        SetupReplicationRole() {
        }

        @Override
        public void run() {
            // rejoining nodes figure out the replication role from other nodes
            if (!m_isRejoin)
            {
                // See if we should bring the server up in WAN replication mode
                m_rvdb.setReplicationRole(m_config.m_replicationRole);
            }
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

    class InitStatsAgent extends InitWork {
        InitStatsAgent() {
        }

        @Override
        public void run() {
            try {
                final long statsAgentHSId = m_rvdb.getHostMessenger().getHSIdForLocalSite(HostMessenger.STATS_SITE_ID);
                m_rvdb.getStatsAgent().getMailbox(
                            VoltDB.instance().getHostMessenger(),
                            statsAgentHSId);
                JSONObject jsObj = new JSONObject();
                jsObj.put("HSId", statsAgentHSId);
                byte[] payload = jsObj.toString(4).getBytes("UTF-8");
                m_rvdb.getHostMessenger().getZK().create(VoltZK.mailboxes_statsagents_agents, payload,
                                           Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                m_rvdb.getAsyncCompilerAgent().createMailbox(
                            VoltDB.instance().getHostMessenger(),
                            m_rvdb.getHostMessenger().getHSIdForLocalSite(HostMessenger.ASYNC_COMPILER_SITE_ID));
            } catch (Exception e) {
                hostLog.fatal(null, e);
                System.exit(-1);
            }
        }
    }

    class CreateRestoreAgentAndPlan extends InitWork {
        public CreateRestoreAgentAndPlan() {
        }

        @Override
        public void run() {
            if (!m_isRejoin && !m_config.m_isRejoinTest) {
                String snapshotPath = null;
                if (m_rvdb.m_catalogContext.cluster.getDatabases().get("database").getSnapshotschedule().get("default") != null) {
                    snapshotPath = m_rvdb.m_catalogContext.cluster.getDatabases().get("database").getSnapshotschedule().get("default").getPath();
                }

                int[] allPartitions = new int[m_rvdb.m_catalogContext.numberOfPartitions];
                int i = 0;
                for (Partition p : m_rvdb.m_catalogContext.cluster.getPartitions()) {
                    allPartitions[i++] = Integer.parseInt(p.getTypeName());
                }

                org.voltdb.catalog.CommandLog cl = m_rvdb.m_catalogContext.cluster.getLogconfig().get("log");

                try {
                    m_rvdb.m_restoreAgent = new RestoreAgent(
                                                      m_rvdb.m_messenger.getZK(),
                                                      m_rvdb.getSnapshotCompletionMonitor(),
                                                      m_rvdb,
                                                      m_rvdb.m_myHostId,
                                                      m_config.m_startAction,
                                                      m_rvdb.m_catalogContext.numberOfPartitions,
                                                      cl.getEnabled(),
                                                      cl.getLogpath(),
                                                      cl.getInternalsnapshotpath(),
                                                      snapshotPath,
                                                      allPartitions,
                                                      m_rvdb.m_siteTracker.getAllHosts());
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Unable to establish a ZooKeeper connection: " +
                            e.getMessage(), false, e);
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
