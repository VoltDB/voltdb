/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.File;
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
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.Pair;
import org.voltdb.catalog.Catalog;
import org.voltdb.common.Constants;
import org.voltdb.common.NodeState;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.HttpsType;
import org.voltdb.export.ExportManager;
import org.voltdb.importer.ImportManager;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.iv2.UniqueIdGenerator;
import org.voltdb.modular.ModuleManager;
import org.voltdb.settings.DbSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndIds;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.InMemoryJarfile;
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
    final NodeStateTracker m_statusTracker;
    final VoltDB.Configuration m_config;
    final boolean m_isRejoin;
    DeploymentType m_deployment = null;

    final Map<Class<? extends InitWork>, InitWork> m_jobs = new HashMap<>();
    final PriorityBlockingQueue<InitWork> m_readyJobs = new PriorityBlockingQueue<>();
    final int m_threadCount;
    final Set<Thread> m_initThreads = new HashSet<>();

    abstract class InitWork implements Comparable<InitWork>, Runnable {
        Set<Class<? extends InitWork>> m_blockers = new HashSet<>();
        Set<Class<? extends InitWork>> m_blockees = new HashSet<>();

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

    Inits(NodeStateTracker statusTracker, RealVoltDB rvdb, int threadCount) {
        m_rvdb = rvdb;
        m_statusTracker = statusTracker;
        m_config = rvdb.getConfig();
        // determine if this is a rejoining node
        // (used for license check and later the actual rejoin)
        m_isRejoin = m_config.m_startAction.doesRejoin();
        m_threadCount = threadCount;
        m_deployment = rvdb.m_catalogContext.getDeployment();

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
        List<Class<? extends InitWork>> toRemove = new ArrayList<>();
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
        InitAsyncCompilerAgent
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

    /**
     * Read catalog bytes from URL
     *
     * @param catalogUrl
     * @return catalog bytes
     * @throws IOException
     */
    private static byte[] readCatalog(String catalogUrl) throws IOException
    {
        assert (catalogUrl != null);

        final int MAX_CATALOG_SIZE = 40 * 1024 * 1024; // 40mb

        InputStream fin = null;
        try {
            URL url = new URL(catalogUrl);
            fin = url.openStream();
        } catch (MalformedURLException ex) {
            // Invalid URL. Try as a file.
            fin = new FileInputStream(catalogUrl);
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

        return Arrays.copyOf(buffer, totalBytes);
    }

    class DistributeCatalog extends InitWork {
        DistributeCatalog() {
            dependsOn(CreateRestoreAgentAndPlan.class);
        }

        @Override
        public void run() {
            // if I'm the leader, send out the catalog
            if (m_rvdb.m_myHostId == m_rvdb.m_hostIdWithStartupCatalog) {

                try {
                    // If no catalog was supplied provide an empty one.
                    if (m_rvdb.m_pathToStartupCatalog == null) {
                        try {
                            File emptyJarFile = CatalogUtil.createTemporaryEmptyCatalogJarFile();
                            if (emptyJarFile == null) {
                                VoltDB.crashLocalVoltDB("Failed to generate empty catalog.");
                            }
                            m_rvdb.m_pathToStartupCatalog = emptyJarFile.getAbsolutePath();
                        }
                        catch (IOException e) {
                            VoltDB.crashLocalVoltDB("I/O exception while creating empty catalog jar file.", false, e);
                        }
                    }

                    // Get the catalog bytes and byte count.
                    byte[] catalogBytes = readCatalog(m_rvdb.m_pathToStartupCatalog);

                    //Export needs a cluster global unique id for the initial catalog version
                    long catalogUniqueId =
                            UniqueIdGenerator.makeIdFromComponents(
                                    System.currentTimeMillis(),
                                    0,
                                    MpInitiator.MP_INIT_PID);
                    hostLog.debug(String.format("Sending %d catalog bytes", catalogBytes.length));

                    long catalogTxnId;
                    catalogTxnId = TxnEgo.makeZero(MpInitiator.MP_INIT_PID).getTxnId();

                    // Need to get the deployment bytes from the starter catalog context
                    byte[] deploymentBytes = m_rvdb.getCatalogContext().getDeploymentBytes();

                    // publish the catalog bytes to ZK
                    CatalogUtil.updateCatalogToZK(
                            m_rvdb.getHostMessenger().getZK(),
                            0, catalogTxnId,
                            catalogUniqueId,
                            catalogBytes,
                            null,
                            deploymentBytes);
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
            CatalogAndIds catalogStuff = null;
            do {
                try {
                    catalogStuff = CatalogUtil.getCatalogFromZK(m_rvdb.getHostMessenger().getZK());
                }
                catch (org.apache.zookeeper_voltpatches.KeeperException.NoNodeException e) {
                }
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB("System was interrupted while waiting for a catalog.", false, null);
                }
            } while (catalogStuff == null || catalogStuff.catalogBytes.length == 0);

            String serializedCatalog = null;
            byte[] catalogJarBytes = null;
            byte[] catalogJarHash = null;
            try {
                Pair<InMemoryJarfile, String> loadResults =
                    CatalogUtil.loadAndUpgradeCatalogFromJar(catalogStuff.catalogBytes);
                serializedCatalog =
                    CatalogUtil.getSerializedCatalogStringFromJar(loadResults.getFirst());
                catalogJarBytes = loadResults.getFirst().getFullJarBytes();
                catalogJarHash = loadResults.getFirst().getSha1Hash();
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Unable to load catalog", false, e);
            }

            if ((serializedCatalog == null) || (serializedCatalog.length() == 0))
                VoltDB.crashLocalVoltDB("Catalog loading failure", false, null);

            /* N.B. node recovery requires discovering the current catalog version. */
            Catalog catalog = new Catalog();
            catalog.execute(serializedCatalog);
            serializedCatalog = null;

            String result = CatalogUtil.checkLicenseConstraint(catalog, m_rvdb.getLicenseApi());
            if (result != null) {
                VoltDB.crashLocalVoltDB(result);
            }

            // note if this fails it will print an error first
            // This is where we compile real catalog and create runtime
            // catalog context. To validate deployment we compile and create
            // a starter context which uses a placeholder catalog.
            result = CatalogUtil.compileDeployment(catalog, m_deployment, false);
            if (result != null) {
                VoltDB.crashLocalVoltDB(result);
            }

            try {
                m_rvdb.m_catalogContext = new CatalogContext(
                        catalogStuff.txnId,
                        catalogStuff.uniqueId,
                        catalog,
                        new DbSettings(m_rvdb.m_clusterSettings, m_rvdb.m_nodeSettings),
                        catalogJarBytes,
                        catalogJarHash,
                        // Our starter catalog has set the deployment stuff, just yoink it out for now
                        m_rvdb.m_catalogContext.getDeploymentBytes(),
                        catalogStuff.version);
            } catch (Exception e) {
                VoltDB.crashLocalVoltDB("Error agreeing on starting catalog version", true, e);
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
                                               m_rvdb.m_clusterSettings.get().hostcount(),
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
            final org.voltdb.catalog.CommandLog logConfig = m_rvdb.m_catalogContext.cluster.getLogconfig().get("log");
            assert logConfig != null;

            if (logConfig.getEnabled()) {
                if (m_config.m_isEnterprise) {
                    try {
                        Class<?> loggerClass = MiscUtils.loadProClass("org.voltdb.CommandLogImpl",
                                                                   "Command logging", false);
                        if (loggerClass != null) {
                            final Constructor<?> constructor = loggerClass.getConstructor(boolean.class,
                                                                                          int.class,
                                                                                          int.class,
                                                                                          String.class,
                                                                                          String.class);
                            m_rvdb.m_commandLog = (CommandLog) constructor.newInstance(logConfig.getSynchronous(),
                                                                                       logConfig.getFsyncinterval(),
                                                                                       logConfig.getMaxtxns(),
                                                                                       VoltDB.instance().getCommandLogPath(),
                                                                                       VoltDB.instance().getCommandLogSnapshotPath());
                        }
                    } catch (Exception e) {
                        VoltDB.crashLocalVoltDB("Unable to instantiate command log", true, e);
                    }
                }
            }
        }
    }

    class SetupSNMP extends InitWork {
        SetupSNMP() {
        }

        @Override
        public void run() {
            if (m_config.m_isEnterprise && m_deployment.getSnmp() != null && m_deployment.getSnmp().isEnabled()) {
                try {
                    Class<?> loggerClass = MiscUtils.loadProClass("org.voltdb.snmp.SnmpTrapSenderImpl",
                                                               "SNMP Adapter", false);
                    if (loggerClass != null) {
                        final Constructor<?> constructor = loggerClass.getConstructor();
                        m_rvdb.m_snmp = (SnmpTrapSender) constructor.newInstance();
                        m_rvdb.m_snmp.initialize(
                                m_deployment.getSnmp(),
                                m_rvdb.getHostMessenger(),
                                m_rvdb.getCatalogContext().cluster.getDrclusterid());
                    }
                } catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Unable to instantiate SNMP", true, e);
                }
            }
        }
    }

    class StartHTTPServer extends InitWork {
        StartHTTPServer() {
        }

        //Setup http server with given port and interface
        private void setupHttpServer(String httpInterface, String publicInterface,
                int httpPortStart, boolean findAny, boolean mustListen) {

            boolean success = false;
            int httpPort = httpPortStart;
            HttpsType httpsType = ((m_deployment.getHttpd() != null) && (m_deployment.getHttpd().isEnabled())) ?
                    m_deployment.getHttpd().getHttps() : null;
            for (; true; httpPort++) {
                try {
                    m_rvdb.m_adminListener = new HTTPAdminListener(
                            m_rvdb.m_jsonEnabled, httpInterface, publicInterface, httpPort, httpsType, mustListen
                            );
                    success = true;
                    break;
                } catch (Exception e1) {
                    if (mustListen) {
                        if (httpsType != null && httpsType.isEnabled()) {
                            hostLog.fatal("HTTP service unable to bind to port " + httpPort + " or SSL Configuration is invalid. Exiting.", e1);
                        } else {
                            hostLog.fatal("HTTP service unable to bind to port " + httpPort + ". Exiting.", e1);
                        }
                        System.exit(-1);
                    }
                }
                if (!findAny) {
                    break;
                }
            }
            if (!success) {
                m_rvdb.m_httpPortExtraLogMessage = String.format(
                        "HTTP service unable to bind to ports %d through %d",
                        httpPortStart, httpPort - 1);
                if (mustListen) {
                    System.exit(-1);
                }
                m_config.m_httpPort = Constants.HTTP_PORT_DISABLED;
                return;
            }
            m_config.m_httpPort = httpPort;
        }

        @Override
        public void run() {
            // start the httpd dashboard/jsonapi. A port value of -1 means disabled
            // by the deployment.xml configuration.
            int httpPort = -1;
            m_rvdb.m_jsonEnabled = false;
            boolean httpsEnabled = false;
            if ((m_deployment.getHttpd() != null) && (m_deployment.getHttpd().isEnabled())) {
                if (m_deployment.getHttpd().getHttps()!=null && m_deployment.getHttpd().getHttps().isEnabled()) {
                    httpsEnabled = true;
                }
                httpPort = (m_deployment.getHttpd().getPort()==null) ?
                        (httpsEnabled ? VoltDB.DEFAULT_HTTPS_PORT : VoltDB.DEFAULT_HTTP_PORT) :
                        m_deployment.getHttpd().getPort();
                if (m_deployment.getHttpd().getJsonapi() != null) {
                    m_rvdb.m_jsonEnabled = m_deployment.getHttpd().getJsonapi().isEnabled();
                }
            }
            // if set by cli use that.
            if (m_config.m_httpPort != Constants.HTTP_PORT_DISABLED) {
                setupHttpServer(m_config.m_httpPortInterface, m_config.m_publicInterface, m_config.m_httpPort, false, true);
                // if not set by the user, just find a free port
            } else if (httpPort == Constants.HTTP_PORT_AUTO) {
                // if not set scan for an open port starting with the default
                httpPort = httpsEnabled ? VoltDB.DEFAULT_HTTPS_PORT : VoltDB.DEFAULT_HTTP_PORT;
                setupHttpServer("", "", httpPort, true, false);
            } else if (httpPort != Constants.HTTP_PORT_DISABLED) {
                if (!m_deployment.getHttpd().isEnabled()) {
                    return;
                }
                setupHttpServer(m_config.m_httpPortInterface, m_config.m_publicInterface, httpPort, false, true);
            }
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
            //Allow cli to set admin mode otherwise use whats in deployment for backward compatibility
            if (m_config.m_isPaused) {
                m_rvdb.setStartMode(OperationMode.PAUSED);
            }
        }
    }

    class SetupReplicationRole extends InitWork {
        SetupReplicationRole() {
        }

        @Override
        public void run() {
            try {
                JSONStringer js = new JSONStringer();
                js.object();
                js.keySymbolValuePair("role", m_config.m_replicationRole.ordinal());
                js.keySymbolValuePair("active", m_rvdb.getReplicationActive());
                js.endObject();

                ZooKeeper zk = m_rvdb.getHostMessenger().getZK();
                // rejoining nodes figure out the replication role from other nodes
                if (!m_isRejoin)
                {
                    try {
                        zk.create(
                                VoltZK.replicationconfig,
                                js.toString().getBytes("UTF-8"),
                                Ids.OPEN_ACL_UNSAFE,
                                CreateMode.PERSISTENT);
                    } catch (KeeperException.NodeExistsException e) {}
                    String discoveredReplicationConfig =
                        new String(zk.getData(VoltZK.replicationconfig, false, null), "UTF-8");
                    JSONObject discoveredjsObj = new JSONObject(discoveredReplicationConfig);
                    ReplicationRole discoveredRole = ReplicationRole.get((byte) discoveredjsObj.getLong("role"));
                    if (!discoveredRole.equals(m_config.m_replicationRole)) {
                        VoltDB.crashGlobalVoltDB("Discovered replication role " + discoveredRole +
                                " doesn't match locally specified replication role " + m_config.m_replicationRole,
                                true, null);
                    }

                    // See if we should bring the server up in WAN replication mode
                    m_rvdb.setReplicationRole(discoveredRole);
                } else {
                    String discoveredReplicationConfig =
                            new String(zk.getData(VoltZK.replicationconfig, false, null), "UTF-8");
                    JSONObject discoveredjsObj = new JSONObject(discoveredReplicationConfig);
                    ReplicationRole discoveredRole = ReplicationRole.get((byte) discoveredjsObj.getLong("role"));
                    boolean replicationActive = discoveredjsObj.getBoolean("active");
                    // See if we should bring the server up in WAN replication mode
                    m_rvdb.setReplicationRole(discoveredRole);
                    m_rvdb.setReplicationActive(replicationActive);
                }
            } catch (Exception e) {
                VoltDB.crashGlobalVoltDB("Error discovering replication role", false, e);
            }
        }
    }

    class InitModuleManager extends InitWork {
        InitModuleManager() {
        }

        @Override
        public void run() {
            ModuleManager.initializeCacheRoot(new File(m_config.m_voltdbRoot, VoltDB.MODULE_CACHE));
            // TODO: start foundation bundles
        }
    }

    class InitExport extends InitWork {
        InitExport() {
            dependsOn(LoadCatalog.class);
            dependsOn(InitModuleManager.class);
        }

        @Override
        public void run() {
            // Let the Export system read its configuration from the catalog.
            try {
                ExportManager.initialize(
                        m_rvdb.m_myHostId,
                        m_rvdb.m_catalogContext,
                        m_isRejoin,
                        (m_config.m_startAction==StartAction.CREATE && m_config.m_forceVoltdbCreate),
                        m_rvdb.m_messenger,
                        m_rvdb.m_partitionsToSitesAtStartupForExportInit
                        );
            } catch (Throwable t) {
                VoltDB.crashLocalVoltDB("Error setting up export", true, t);
            }
        }
    }

    class InitImport extends InitWork {
        InitImport() {
            dependsOn(LoadCatalog.class);
            dependsOn(InitModuleManager.class);
        }

        @Override
        public void run() {
            // Let the Import system read its configuration from the catalog.
            try {
                ImportManager.initialize(m_rvdb.m_myHostId, m_rvdb.m_catalogContext, m_rvdb.m_messenger);
            } catch (Throwable t) {
                VoltDB.crashLocalVoltDB("Error setting up import", true, t);
            }
        }
    }

    class InitHashinator extends InitWork {
        InitHashinator() {
        }

        @Override
        public void run() {
            // Initialize the complex partitioning scheme
            int partitionCount;
            if (m_config.m_startAction == StartAction.JOIN) {
                // Initialize the hashinator with the existing partition count in the cluster,
                // don't include the partitions that we're adding because they shouldn't contain
                // any ranges yet.
                partitionCount = m_rvdb.m_cartographer.getPartitionCount();
            } else {
                partitionCount = m_rvdb.m_configuredNumberOfPartitions;
            }

            TheHashinator.initialize(
                TheHashinator.getConfiguredHashinatorClass(),
                TheHashinator.getConfigureBytes(partitionCount));
        }
    }

    class InitAsyncCompilerAgent extends InitWork {
        InitAsyncCompilerAgent() {
        }

        @Override
        public void run() {
            try {
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
            if (!m_isRejoin && !m_config.m_isRejoinTest && !m_rvdb.m_joining) {
                String snapshotPath = null;
                if (m_rvdb.m_catalogContext.cluster.getDatabases().get("database").getSnapshotschedule().get("default") != null) {
                    snapshotPath = VoltDB.instance().getSnapshotPath();
                }

                int[] allPartitions = new int[m_rvdb.m_configuredNumberOfPartitions];
                for (int ii = 0; ii < allPartitions.length; ii++) {
                    allPartitions[ii] = ii;
                }

                org.voltdb.catalog.CommandLog cl = m_rvdb.m_catalogContext.cluster.getLogconfig().get("log");
                if (cl == null || !cl.getEnabled()) return;
                NodeSettings paths = m_rvdb.m_nodeSettings;
                try {
                    m_rvdb.m_restoreAgent = new RestoreAgent(
                                                      m_rvdb.m_messenger,
                                                      m_rvdb.getSnapshotCompletionMonitor(),
                                                      m_rvdb,
                                                      m_config.m_startAction,
                                                      cl.getEnabled(),
                                                      paths.resolve(paths.getCommandLog()).getPath(),
                                                      paths.resolve(paths.getCommandLogSnapshot()).getPath(),
                                                      snapshotPath,
                                                      allPartitions,
                                                      paths.getVoltDBRoot().getPath(),
                                                      m_rvdb.m_terminusNonce);
                } catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Unable to construct the RestoreAgent", true, e);
                }

                m_rvdb.m_globalServiceElector.registerService(m_rvdb.m_restoreAgent);
                // Generate plans and get (hostID, catalogPath) pair
                Pair<Integer,String> catalog = m_rvdb.m_restoreAgent.findRestoreCatalog();
                if (catalog != null) {
                    m_statusTracker.setNodeState(NodeState.RECOVERING);
                }
                // if the restore agent found a catalog, set the following info
                // so the right node can send it out to the others.
                if (catalog != null) {
                    // Make sure the catalog corresponds to the current server version.
                    // Prevent automatic upgrades by rejecting mismatched versions.
                    int hostId = catalog.getFirst().intValue();
                    String catalogPath = catalog.getSecond();
                    // Perform a version check when the catalog jar is available
                    // on the current host.
                    // Check that this host is the one providing the catalog.
                    if (m_rvdb.m_myHostId == hostId) {
                        try {
                            byte[] catalogBytes = readCatalog(catalogPath);
                            InMemoryJarfile inMemoryJar = CatalogUtil.loadInMemoryJarFile(catalogBytes);
                            // This call pre-checks and returns the build info/version.
                            String[] buildInfo = CatalogUtil.getBuildInfoFromJar(inMemoryJar);
                            String catalogVersion = buildInfo[0];
                            String serverVersion = m_rvdb.getVersionString();
                            if (!catalogVersion.equals(serverVersion)) {
                                if (!m_rvdb.m_restoreAgent.willRestoreShutdownSnaphot()) {
                                    VoltDB.crashLocalVoltDB(String.format(
                                            "Unable to load version %s catalog \"%s\" "
                                                    + "from snapshot into a version %s server.",
                                                    catalogVersion, catalogPath, serverVersion), false, null);
                                    return;
                                }
                                // upgrade the catalog - the following will save the recpmpiled catalog
                                // under voltdbroot/catalog-[serverVersion].jar
                                CatalogUtil.loadAndUpgradeCatalogFromJar(catalogBytes);
                                NodeSettings pathSettings = m_rvdb.m_nodeSettings;
                                File recoverCatalogFH = new File(pathSettings.getVoltDBRoot(), "catalog-" + serverVersion + ".jar");
                                catalogPath = recoverCatalogFH.getPath();
                            }
                        }
                        catch (IOException e) {
                            // Make it non-fatal with no check performed.
                            hostLog.warn(String.format(
                                    "Unable to load catalog for version check due to exception: %s.",
                                    e.getMessage()), e);
                        }
                    }
                    if (hostLog.isDebugEnabled()) {
                        hostLog.debug("Found catalog to load on host " + hostId + ": " + catalogPath);
                    }
                    m_rvdb.m_hostIdWithStartupCatalog = hostId;
                    assert(m_rvdb.m_hostIdWithStartupCatalog >= 0);
                    m_rvdb.m_pathToStartupCatalog = catalogPath;
                    assert(m_rvdb.m_pathToStartupCatalog != null);
                }
            }
        }
    }
}
