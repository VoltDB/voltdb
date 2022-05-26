/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.nio.ByteBuffer;
import java.nio.file.Paths;
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
import org.voltcore.utils.Pair;
import org.voltcore.zk.ZKUtil.ZKCatalogStatus;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogException;
import org.voltdb.common.Constants;
import org.voltdb.common.NodeState;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.compiler.deploymentfile.FeaturesType;
import org.voltdb.export.ExportManagerInterface;
import org.voltdb.importer.ImportManager;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.UniqueIdGenerator;
import org.voltdb.largequery.LargeBlockManager;
import org.voltdb.modular.ModuleManager;
import org.voltdb.settings.DbSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CatalogUtil.CatalogAndDeployment;
import org.voltdb.utils.CatalogUtil.SegmentedCatalog;
import org.voltdb.utils.HTTPAdminListener;
import org.voltdb.utils.InMemoryJarfile;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.PlatformProperties;
import org.voltdb.utils.ProClass;

import com.google_voltpatches.common.io.ByteStreams;

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
    final boolean m_durable;

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
                if (iw instanceof COMPLETION_WORK) {
                    return;
                }
                //hostLog.info("Running InitWorker: " + iw.getClass().getName());
                iw.run();
                completeInitWork(iw);
            }
        }
    }

    Inits(NodeStateTracker statusTracker, RealVoltDB rvdb, int threadCount, boolean durable) {
        m_rvdb = rvdb;
        m_statusTracker = statusTracker;
        m_config = rvdb.getConfig();
        // determine if this is a rejoining node
        // (used for license check and later the actual rejoin)
        m_isRejoin = m_config.m_startAction.doesRejoin();
        m_threadCount = threadCount;
        m_durable = durable;
        m_deployment = rvdb.m_catalogContext.getDeployment();

        // find all the InitWork subclasses using reflection and load them up
        Class<?>[] declaredClasses = Inits.class.getDeclaredClasses();
        for (Class<?> cls : declaredClasses) {
            // skip base classes and fake classes
            if (cls == InitWork.class) {
                continue;
            }
            if (cls == COMPLETION_WORK.class) {
                continue;
            }

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

        Per John, these all run after the socket joiner completes.

        The dependency DAG (a depends on <- b) is as follows;
        classes are listed in order of appearance in this file.

        CollectPlatformInfo
        DistributeCatalog <- CreateRestoreAgentAndPlan
        LoadCatalog <- DistributeCatalog
        EnforceLicensing <- CreateRestoreAgentAndPlan, SetupReplicationRole
        SetupCommandLogging <- LoadCatalog
        SetupSNMP
        StartHTTPServer
        SetupAdminMode
        SetupReplicationRole <- LoadCatalog
        InitModuleManager
        InitExport <- LoadCatalog, InitModuleManager
        InitImport <- LoadCatalog, InitModuleManager, SetupAdminMode
        InitHashinator
        CreateRestoreAgentAndPlan
        InitLargeBlockManager

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

        InputStream fin = null;
        try {
            URL url = new URL(catalogUrl);
            fin = url.openStream();
        } catch (MalformedURLException ex) {
            // Invalid URL. Try as a file.
            fin = new FileInputStream(catalogUrl);
        }
        try {
            return ByteStreams.toByteArray(fin);
        } finally {
            fin.close();
        }
    }

    /**
     * Read catalog bytes from URL in chunks, each chunk is less than 52MB
     *
     * @param catalogUrl
     * @return catalog bytes
     * @throws IOException
     */
    private static SegmentedCatalog readCatalogInChunks(String catalogUrl, byte[] deploymentBytes) throws IOException
    {
        assert (catalogUrl != null);

        List<ByteBuffer> chunks = new ArrayList<ByteBuffer>();

        InputStream fin = null;
        try {
            URL url = new URL(catalogUrl);
            fin = url.openStream();
        } catch (MalformedURLException ex) {
            // Invalid URL. Try as a file.
            fin = new FileInputStream(catalogUrl);
        }
        byte[] buffer = new byte[CatalogUtil.MAX_CATALOG_CHUNK_SIZE];
        int readBytes = 0;
        // Reserve the header space for first buffer
        int chunkOffset = CatalogUtil.CATALOG_BUFFER_HEADER;
        // Copy deployment bytes into first buffer (deployment file is small enough to fit in a buffer)
        System.arraycopy(deploymentBytes, 0, buffer, chunkOffset, deploymentBytes.length);
        chunkOffset += deploymentBytes.length;
        int totalCatalogBytes = 0;
        try {
            while (readBytes >= 0) {
                chunkOffset += readBytes;
                totalCatalogBytes += readBytes;
                if (chunkOffset == buffer.length) {
                    chunkOffset = 0;
                    chunks.add(ByteBuffer.wrap(buffer));
                    buffer = new byte[CatalogUtil.MAX_CATALOG_CHUNK_SIZE];
                }
                readBytes = fin.read(buffer, chunkOffset, buffer.length - chunkOffset);
            }
        } finally {
            fin.close();
        }
        byte[] lastBuffer = Arrays.copyOf(buffer, chunkOffset);
        chunks.add(ByteBuffer.wrap(lastBuffer));

        return new SegmentedCatalog(chunks, deploymentBytes.length, totalCatalogBytes, new byte[] {0});
    }

    private static File createEmptyStartupJarFile(String drRole){
        File emptyJarFile = null;
        try {
            emptyJarFile = CatalogUtil.createTemporaryEmptyCatalogJarFile(DrRoleType.XDCR.value().equals(drRole));
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB("I/O exception while creating empty catalog jar file.", false, e);
        }
        if (emptyJarFile == null) {
            VoltDB.crashLocalVoltDB("Failed to generate empty catalog.");
        }
        return emptyJarFile;
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
                        String drRole = m_rvdb.getCatalogContext().getCluster().getDrrole();
                        m_rvdb.m_pathToStartupCatalog = Inits.createEmptyStartupJarFile(drRole).getAbsolutePath();
                    }

                    // Need to get the deployment bytes from the starter catalog context
                    byte[] deploymentBytes = m_rvdb.getCatalogContext().getDeploymentBytes();

                    // Get the catalog bytes and byte count.
                    SegmentedCatalog catalogAndDeployment = readCatalogInChunks(m_rvdb.m_pathToStartupCatalog, deploymentBytes);

                    //Export needs a cluster global unique id for the initial catalog version
                    long exportInitialGenerationUniqueId =
                            UniqueIdGenerator.makeIdFromComponents(
                                    System.currentTimeMillis(),
                                    0,
                                    MpInitiator.MP_INIT_PID);

                    // publish the catalog bytes to ZK
                    CatalogUtil.updateCatalogToZK(
                            m_rvdb.getHostMessenger().getZK(),
                            0, // Initial version
                            exportInitialGenerationUniqueId,
                            catalogAndDeployment,
                            ZKCatalogStatus.COMPLETE,
                            -1); // dummy txnId
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
            CatalogAndDeployment catalogStuff = null;
            hostLog.info("LoadCatalog");
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

            InMemoryJarfile thisNodeCatalog = null;
            assert( m_rvdb.getStartAction() != StartAction.PROBE );
            if (m_rvdb.getStartAction() == StartAction.CREATE) {
                // We may have received a staged catalog from the leader.
                // Check if it matches ours.
                if (m_rvdb.m_pathToStartupCatalog == null) {
                    String drRole = m_rvdb.getCatalogContext().getCluster().getDrrole();
                    m_rvdb.m_pathToStartupCatalog = Inits.createEmptyStartupJarFile(drRole).getAbsolutePath();
                }
                try {
                    thisNodeCatalog = new InMemoryJarfile(m_rvdb.m_pathToStartupCatalog);
                } catch (IOException e){
                    VoltDB.crashLocalVoltDB("Failed to load initialized schema: " + e.getMessage(), false, e);
                }
                InMemoryJarfile remoteNodeCatalog = null;
                try {
                    // Compute the remote catalog hash,
                    remoteNodeCatalog = new InMemoryJarfile(catalogStuff.catalogBytes);
                }
                catch (IOException e) {
                    VoltDB.crashLocalVoltDB("Failed to load remote schema: " + e.getMessage(), false, e);
                }
                if (!Arrays.equals(remoteNodeCatalog.getSha1Hash(), thisNodeCatalog.getSha1Hash())) {
                    VoltDB.crashGlobalVoltDB("Nodes have been initialized with different schemas. All nodes must initialize with identical schemas.", false, null);
                }
            }

            String serializedCatalog = null;
            byte[] catalogJarBytes = null;
            byte[] catalogJarHash = null;
            try {
                Pair<InMemoryJarfile, String> loadResults =
                    CatalogUtil.loadAndUpgradeCatalogFromJar(catalogStuff.catalogBytes,
                                                             DrRoleType.XDCR.value().equals(m_rvdb.getCatalogContext().getCluster().getDrrole()));
                thisNodeCatalog = loadResults.getFirst();
                serializedCatalog =
                    CatalogUtil.getSerializedCatalogStringFromJar(thisNodeCatalog);
                catalogJarBytes = thisNodeCatalog.getFullJarBytes();
                catalogJarHash = thisNodeCatalog.getSha1Hash();
            } catch (IOException e) {
                VoltDB.crashLocalVoltDB("Unable to load catalog", false, e);
            }

            if ((serializedCatalog == null) || (serializedCatalog.length() == 0)) {
                VoltDB.crashLocalVoltDB("Catalog loading failure", false, null);
            }

            /* N.B. node recovery requires discovering the current catalog version. */
            Catalog catalog = new Catalog();
            try {
                catalog.execute(serializedCatalog);
            } catch (CatalogException e) {
                // Disallow recovering from an incompatible Enterprise catalog.
                VoltDB.crashLocalVoltDB(e.getLocalizedMessage());
            }
            serializedCatalog = null;

            // Build validators and validate deployment
            // Note: the validation an catalog compilation sequence is identical to that of
            // {@link org.voltdb.sysprocs.UpdateApplicationBase#prepareApplicationCatalogDiff}
            m_rvdb.buildCatalogValidators(m_config.m_isEnterprise);
            CatalogChangeResult ccr = new CatalogChangeResult();
            try {
                if (!m_rvdb.validateDeployment(catalog, m_deployment, null, ccr)) {
                    VoltDB.declineCrashFile = "No crash file generated for deployment validation failures";
                    VoltDB.crashLocalVoltDB(ccr.errorMsg);
                }
            }
            catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unexpected error validating deployment", true, e);
            }

            // Compile deployment into catalog
            // note if this fails it will print an error first
            // This is where we compile real catalog and create runtime
            // catalog context.
            String result = CatalogUtil.compileDeployment(catalog, m_deployment, false);
            if (result != null) {
                VoltDB.declineCrashFile = "No crash file generated for deployment compilation exceptions";
                VoltDB.crashLocalVoltDB(result);
            }

            // Validate full configuration
            try {
                if (!m_rvdb.validateConfiguration(catalog, m_deployment, thisNodeCatalog, null, ccr)) {
                    VoltDB.crashLocalVoltDB(ccr.errorMsg);
                }
            }
            catch (Exception e) {
                VoltDB.crashLocalVoltDB("Unexpected error validating configuration", true, e);
            }

            try {
                m_rvdb.m_catalogContext = new CatalogContext(
                        catalog,
                        new DbSettings(m_rvdb.m_clusterSettings, m_rvdb.m_nodeSettings),
                        catalogStuff.version, // catalog version from zk (rejoin node needs the latest version)
                        catalogStuff.genId,
                        catalogJarBytes,
                        catalogJarHash,
                        m_rvdb.m_catalogContext.getDeploymentBytes(),
                        m_rvdb.m_messenger);
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
                m_rvdb.getLicensing().validateLicense();
            }
        }
    }

    class SetupCommandLogging extends InitWork {
        SetupCommandLogging() {
            dependsOn(LoadCatalog.class);
        }

        @Override
        public void run() {
            hostLog.info("LoadCatalog");
            final org.voltdb.catalog.CommandLog logConfig = m_rvdb.m_catalogContext.cluster.getLogconfig().get("log");
            assert logConfig != null;

            if (logConfig.getEnabled()) {
                if (m_config.m_isEnterprise) {
                    m_rvdb.m_commandLog = ProClass.newInstanceOf("org.voltdb.CommandLogImpl", "Command logging",
                            ProClass.HANDLER_LOG, logConfig.getSynchronous(), logConfig.getFsyncinterval(),
                            logConfig.getMaxtxns(), VoltDB.instance().getCommandLogPath(),
                            VoltDB.instance().getCommandLogSnapshotPath());
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
                m_rvdb.m_snmp = ProClass.newInstanceOf("org.voltdb.snmp.SnmpTrapSenderImpl", "SNMP Adapter",
                        ProClass.HANDLER_LOG);
                if (m_rvdb.m_snmp != null) {
                        m_rvdb.m_snmp.initialize(
                                m_deployment.getSnmp(),
                                m_rvdb.getHostMessenger(),
                                m_rvdb.getCatalogContext().cluster.getDrclusterid());
                }
            }
        }
    }

    class StartHTTPServer extends InitWork {

        //Setup http server with given port and interface
        private void setupHttpServer(String httpInterface, String publicInterface,
                int httpPortStart, boolean findAny, boolean mustListen) {

            boolean success = false;
            int httpPort = httpPortStart;
            for (; true; httpPort++) {
                try {
                    m_rvdb.m_adminListener = new HTTPAdminListener(
                            m_rvdb.m_jsonEnabled, httpInterface, publicInterface, httpPort,
                            m_config.m_sslContextFactory, mustListen);
                    success = true;
                    break;
                } catch (Exception e1) {
                    if (mustListen) {
                        if (m_config.m_sslContextFactory != null) {
                            hostLog.fatal("HTTP service unable to bind to port " + httpPort + " or SSL Configuration is invalid. Exiting.", e1);
                        } else {
                            hostLog.fatal("HTTP service unable to bind to port " + httpPort + ". Exiting.", e1);
                        }
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
            boolean sslEnabled = false;
            if ((m_deployment.getHttpd() != null) && (m_deployment.getHttpd().isEnabled())) {
                if (m_config.m_sslContextFactory != null) {
                    sslEnabled = true;
                }
                httpPort = m_deployment.getHttpd().getPort() == null
                        ? sslEnabled
                            ? VoltDB.DEFAULT_HTTPS_PORT
                            : VoltDB.DEFAULT_HTTP_PORT
                        : m_deployment.getHttpd().getPort();
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
                httpPort = sslEnabled ? VoltDB.DEFAULT_HTTPS_PORT : VoltDB.DEFAULT_HTTP_PORT;
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

            // allow command line override
            if (m_config.m_adminPort > 0) {
                adminPort = m_config.m_adminPort;
            }
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
            dependsOn(LoadCatalog.class);
        }

        @Override
        public void run() {
            try {
                JSONStringer js = new JSONStringer();
                js.object();
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
                } else {
                    String discoveredReplicationConfig =
                            new String(zk.getData(VoltZK.replicationconfig, false, null), "UTF-8");
                    JSONObject discoveredjsObj = new JSONObject(discoveredReplicationConfig);
                    boolean replicationActive = discoveredjsObj.getBoolean("active");
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
                FeaturesType features = m_rvdb.m_catalogContext.getDeployment().getFeatures();
                ExportManagerInterface emi = ExportManagerInterface.initialize(
                        features,
                        m_rvdb.m_myHostId,
                        m_config,
                        m_rvdb.m_catalogContext,
                        m_isRejoin,
                        //If durability is off and we are told not to join but create by mesh clear overflow.
                        (m_config.m_startAction==StartAction.CREATE && (m_config.m_forceVoltdbCreate || !m_durable)),
                        m_rvdb.m_messenger,
                        m_rvdb.getPartitionToSiteMap()
                        );
                m_rvdb.m_globalServiceElector.registerService(emi);
            } catch (Throwable t) {
                VoltDB.crashLocalVoltDB("Error setting up export", true, t);
            }
        }
    }

    class InitImport extends InitWork {
        InitImport() {
            dependsOn(LoadCatalog.class);
            dependsOn(InitModuleManager.class);
            dependsOn(SetupAdminMode.class);
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

                NodeSettings paths = m_rvdb.m_nodeSettings;
                org.voltdb.catalog.CommandLog cl = m_rvdb.m_catalogContext.cluster.getLogconfig().get("log");
                String clPath = null;
                String clSnapshotPath = null;
                boolean clenabled = true;
                if (cl == null || !cl.getEnabled()) {
                     clenabled = false;
                 } else {
                     clPath = paths.resolveToAbsolutePath(paths.getCommandLog()).getPath();
                     clSnapshotPath = paths.resolveToAbsolutePath(paths.getCommandLogSnapshot()).getPath();
                }
                try {
                    m_rvdb.m_restoreAgent = new RestoreAgent(
                                                      m_rvdb.m_messenger,
                                                      m_rvdb.getSnapshotCompletionMonitor(),
                                                      m_rvdb,
                                                      m_config.m_startAction,
                                                      clenabled,
                                                      clPath,
                                                      clSnapshotPath,
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

                // if the restore agent found a catalog, set the following info
                // so the right node can send it out to the others.
                if (catalog != null) {
                    m_statusTracker.set(NodeState.RECOVERING);

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
                                // Only do version check when c/l is enabled.
                                // Recover from a terminus snapshot generated from a different version,
                                // the terminus snapshot marker will be deleted. If c/l is not enabled,
                                // either on community edition or enterprise edition but with the feature
                                // turned off, there is no truncation snapshot when cluster completes
                                // initialization. So if cluster crashes before new snapshot is written,
                                // it will have no viable snapshot to recover again. Bypass version check
                                // when c/l is disabled resolves this issue.
                                if (clenabled == true && !m_rvdb.m_restoreAgent.willRestoreShutdownSnaphot()) {
                                    VoltDB.crashLocalVoltDB(String.format(
                                                "Cannot load command logs from one version (%s) into a different version of VoltDB (%s). " +
                                                "To upgrade the VoltDB software, first use \"voltadmin shutdown --save\", then " +
                                                "upgrade and restart.", catalogVersion, serverVersion),
                                            false, null);
                                    return;
                                }
                                // upgrade the catalog - the following will save the recpmpiled catalog
                                // under voltdbroot/catalog-[serverVersion].jar
                                CatalogUtil.loadAndUpgradeCatalogFromJar(catalogBytes,
                                                                         DrRoleType.XDCR.value().equals(m_rvdb.getCatalogContext().getCluster().getDrrole()));
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

    class InitLargeBlockManager extends InitWork {
        public InitLargeBlockManager() {
        }

        @Override
        public void run() {
            try {
                LargeBlockManager.startup(Paths.get(m_rvdb.getLargeQuerySwapPath()));
            }
            catch (Exception e) {
                hostLog.fatal(e.getMessage());
                VoltDB.crashLocalVoltDB(e.getMessage());
            }

        }
    }
}
