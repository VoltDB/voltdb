/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.zk.ZKUtil;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Topic;
import org.voltdb.common.NodeState;
import org.voltdb.compiler.deploymentfile.DeploymentType;
import org.voltdb.compiler.deploymentfile.PathsType;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.elastic.ElasticService;
import org.voltdb.iv2.Cartographer;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.licensing.Licensing;
import org.voltdb.serdes.AvroSerde;
import org.voltdb.settings.ClusterSettings;
import org.voltdb.settings.DbSettings;
import org.voltdb.settings.NodeSettings;
import org.voltdb.snmp.DummySnmpTrapSender;
import org.voltdb.snmp.SnmpTrapSender;
import org.voltdb.task.TaskManager;
import org.voltdb.utils.HTTPAdminListener;

import com.google_voltpatches.common.base.Throwables;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;
import com.google_voltpatches.common.util.concurrent.MoreExecutors;

public class MockVoltDB implements VoltDBInterface
{
    private static final VoltLogger logger = new VoltLogger("MockVoltDB");
    private Catalog m_catalog;
    private CatalogContext m_context;
    final String m_clusterName = "cluster";
    final String m_databaseName = "database";
    StatsAgent m_statsAgent = null;
    HostMessenger m_hostMessenger = new HostMessenger(new HostMessenger.Config(false), null,"hostDisplayName" );
    private OperationMode m_mode = OperationMode.RUNNING;
    private volatile String m_localMetadata;
    final SnapshotCompletionMonitor m_snapshotCompletionMonitor = new SnapshotCompletionMonitor();
    boolean m_noLoadLib = false;
    OperationMode m_startMode = OperationMode.RUNNING;
    ReplicationRole m_replicationRole = ReplicationRole.NONE;
    long m_clusterCreateTime = 0;
    private Instant m_hostStartTime = Instant.now();
    VoltDB.Configuration voltconfig = null;
    private final ListeningExecutorService m_es = MoreExecutors.listeningDecorator(CoreUtils.getSingleThreadExecutor("Mock Computation Service"));
    private ScheduledThreadPoolExecutor m_periodicWorkThread = CoreUtils.getScheduledThreadPoolExecutor("Periodic Work", 1, CoreUtils.SMALL_STACK_SIZE);;
    public int m_hostId = 0;
    private SiteTracker m_siteTracker;
    private final Map<MailboxType, List<MailboxNodeContent>> m_mailboxMap =
            new HashMap<>();
    private boolean m_replicationActive = false;
    private CommandLog m_cl = null;
    private int m_kfactor;

    public MockVoltDB() {
        this(VoltDB.DEFAULT_PORT, VoltDB.DEFAULT_ADMIN_PORT, -1, VoltDB.DEFAULT_DR_PORT);
    }

    /*
     * Fake do nothing constructor...
     */
    public MockVoltDB(Object foo, Object bar) {

    }

    public MockVoltDB(int clientPort, int adminPort, int httpPort, int drPort)
    {
        try {
            JSONObject obj = new JSONObject();
            JSONArray jsonArray = new JSONArray();
            jsonArray.put("127.0.0.1");
            obj.put("interfaces", jsonArray);
            obj.put("clientPort", clientPort);
            obj.put("adminPort", adminPort);
            obj.put("httpPort", httpPort);
            obj.put("drPort", drPort);
            obj.put("drInterface", "127.0.0.1");
            obj.put(VoltZK.drPublicHostProp, "");
            obj.put(VoltZK.drPublicPortProp, Integer.toString(VoltDB.DISABLED_PORT));

            m_localMetadata = obj.toString(4);

            m_catalog = new Catalog();
            m_catalog.execute(String.format("add / clusters %s", m_clusterName));
            m_catalog.execute(String.format("add /clusters#%s databases %s", m_clusterName, m_databaseName));
            Cluster cluster = m_catalog.getClusters().get(m_clusterName);
            // Set a sane default for TestMessaging (at least)
            cluster.setHeartbeattimeout(10000);
            assert(cluster != null);

            m_hostMessenger.start();
            VoltZK.createPersistentZKNodes(m_hostMessenger.getZK());
            m_hostMessenger.getZK().create(
                    VoltZK.cluster_metadata + "/" + m_hostMessenger.getHostId(),
                    getLocalMetadata().getBytes("UTF-8"),
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);

            m_hostMessenger.getZK().create(
                    VoltZK.start_action,
                    null,
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT,
                    new ZKUtil.StringCallback(),
                    null);

            m_statsAgent = new StatsAgent();
            long hsId = m_hostMessenger.getHSIdForLocalSite(HostMessenger.STATS_SITE_ID);
            // Use generate to install the dummy mailbox which is expected to be there by OpsAgent.registerMailbox
            m_hostMessenger.generateMailboxId(hsId);
            m_statsAgent.registerMailbox(m_hostMessenger, hsId);
            for (MailboxType type : MailboxType.values()) {
                m_mailboxMap.put(type, new LinkedList<MailboxNodeContent>());
            }
            m_mailboxMap.get(MailboxType.StatsAgent).add(
                    new MailboxNodeContent(m_hostMessenger.getHSIdForLocalSite(HostMessenger.STATS_SITE_ID), null));
            m_siteTracker = new SiteTracker(m_hostId, m_mailboxMap);
        } catch (Exception e) {
            Throwables.throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public Procedure addProcedureForTest(String name)
    {
        Procedure retval = getCluster().getDatabases().get(m_databaseName).getProcedures().add(name);
        retval.setClassname(name);
        retval.setHasjava(true);
        retval.setSystemproc(false);
        retval.setDefaultproc(false);
        return retval;
    }

    public void addSite(long siteId, MailboxType type) {
        m_mailboxMap.get(type).add(new MailboxNodeContent(siteId, null));
        m_siteTracker = new SiteTracker(m_hostId, m_mailboxMap);
    }

    public void addSite(long siteId, int partitionId)
    {
        MailboxNodeContent mnc = new MailboxNodeContent( siteId, partitionId);
        m_mailboxMap.get(MailboxType.ExecutionSite).add(mnc);
        m_siteTracker = new SiteTracker(m_hostId, m_mailboxMap);
    }

    public synchronized void killSite(long siteId) {
        m_catalog = m_catalog.deepCopy();
        for (List<MailboxNodeContent> lmnc : m_mailboxMap.values()) {
            Iterator<MailboxNodeContent> iter = lmnc.iterator();
            while (iter.hasNext()) {
                if (iter.next().HSId == siteId) {
                    iter.remove();
                }
            }
        }
        m_siteTracker = new SiteTracker(m_hostId, m_mailboxMap);
    }

    public void addTable(String tableName, boolean isReplicated)
    {
        getDatabase().getTables().add(tableName);
        getTable(tableName).setIsreplicated(isReplicated);
        getTable(tableName).setSignature(tableName);
    }

    public void addTopic(String topicName) {
        addTable(topicName, false);
        getDatabase().getTopics().add(topicName);
        getTable(topicName).setTopicname(topicName);
    }

    public void setDRProducerEnabled()
    {
        getCluster().setDrproducerenabled(true);
    }

    public void setDRConsumerConnectionEnabled(boolean enabled) {
        getCluster().setDrconsumerenabled(enabled);
    }

    String m_clSnapshotPath = "command_log_snapshot";
    public void configureLogging(boolean enabled, boolean sync,
            int fsyncInterval, int maxTxns, String logPath, String snapshotPath) {
        org.voltdb.catalog.CommandLog logConfig = getCluster().getLogconfig().get("log");
        if (logConfig == null) {
            logConfig = getCluster().getLogconfig().add("log");
        }
        logConfig.setEnabled(enabled);
        logConfig.setSynchronous(sync);
        logConfig.setFsyncinterval(fsyncInterval);
        logConfig.setMaxtxns(maxTxns);
        m_clSnapshotPath = snapshotPath;
    }
    @Override
    public String getCommandLogSnapshotPath() {
        return m_clSnapshotPath;
    }

    String m_autoSnapshotPath = "snapshots";
    public void configureSnapshotSchedulePath(String autoSnapshotPath) {
        org.voltdb.catalog.SnapshotSchedule scheduleConfig = getDatabase().getSnapshotschedule().get("default");
        if (scheduleConfig == null) {
            scheduleConfig = getDatabase().getSnapshotschedule().add("default");
        }
        m_autoSnapshotPath = autoSnapshotPath;
    }
    @Override
    public String getSnapshotPath() {
        return m_autoSnapshotPath;
    }

    public void addColumnToTable(String tableName, String columnName,
            VoltType columnType,
            boolean isNullable, String defaultValue,
            VoltType defaultType)
    {
        int index = getTable(tableName).getColumns().size();
        getTable(tableName).getColumns().add(columnName);
        Column column = getColumnFromTable(tableName, columnName);
        column.setIndex(index);
        column.setType(columnType.getValue());
        column.setNullable(isNullable);
        column.setName(columnName);
        column.setDefaultvalue(defaultValue);
        column.setDefaulttype(defaultType.getValue());
        if (!columnType.isVariableLength()) {
            column.setSize(columnType.getLengthInBytesForFixedTypesWithoutCheck());
        }
    }

    public Cluster getCluster()
    {
        return m_catalog.getClusters().get(m_clusterName);
    }

    public Database getDatabase()
    {
        return getCluster().getDatabases().get(m_databaseName);
    }

    public Table getTable(String tableName)
    {
        return getDatabase().getTables().get(tableName);
    }

    public Topic getTopic(String topicName)
    {
        return getDatabase().getTopics().get(topicName);
    }

    Column getColumnFromTable(String tableName, String columnName)
    {
        return getTable(tableName).getColumns().get(columnName);
    }

    @Override
    public String getBuildString()
    {
        return "MOCK_VOLTDB";
    }

    @Override
    public CatalogContext getCatalogContext()
    {
        long now = System.currentTimeMillis();
        DbSettings settings = new DbSettings(ClusterSettings.create().asSupplier(), NodeSettings.create());

        m_context = new CatalogContext(m_catalog, settings, 0, now,
                new byte[] {}, null, new byte[] {}, m_hostMessenger) {
            @Override
            public long getCatalogCRC() {
                return 13;
            }
        };
        return m_context;
    }

    @Override
    public ClientInterface getClientInterface()
    {
        return null;
    }

    public void setConfig(VoltDB.Configuration config)
    {
        voltconfig = config;
    }

    @Override
    public Configuration getConfig()
    {
        if (voltconfig == null)
        {
            voltconfig = new VoltDB.Configuration();
        }
        return voltconfig;
    }

    public void setHostMessenger(HostMessenger msg) {
        m_hostMessenger = msg;
    }

    @Override
    public HostMessenger getHostMessenger()
    {
        return m_hostMessenger;
    }

    public void setStatsAgent(StatsAgent agent)
    {
        m_statsAgent = agent;
    }

    @Override
    public OpsAgent getOpsAgent(OpsSelector selector)
    {
        return null;
    }

    @Override
    public StatsAgent getStatsAgent()
    {
        return m_statsAgent;
    }

    @Override
    public MemoryStats getMemoryStatsSource() {
        return null;
    }

    @Override
    public String getVersionString()
    {
        if (!m_noLoadLib) {
            return new RealVoltDB().getVersionString();
        } else {
            return null;
        }
    }

    @Override
    public String getEELibraryVersionString() {
        return getVersionString();
    }

    @Override
    public boolean isCompatibleVersionString(String versionString) {
        return true;
    }

    @Override
    public boolean isRunningWithOldVerbs() {
        return voltconfig.m_startAction.isLegacy();
    }

    @Override
    public void initialize(Configuration config)
    {
        m_noLoadLib = config.m_noLoadLibVOLTDB;
        voltconfig = config;
    }

    @Override
    public void cli(Configuration config)
    {
        m_noLoadLib = config.m_noLoadLibVOLTDB;
        voltconfig = config;
    }

    public void createStartActionNode(int index, StartAction action) {
        VoltZK.createStartActionNode(m_hostMessenger.getZK(), m_hostMessenger.getHostId() + index, action);
    }

    class StartActionWatcher implements Watcher {
        @Override
        public void process(WatchedEvent event) {
            m_es.submit(new Runnable() {
                @Override
                public void run() {
                    validateStartAction();
                }
            });
        }
    }

    public void validateStartAction() {
        try {
            ZooKeeper zk = m_hostMessenger.getZK();
            boolean initCompleted = zk.exists(VoltZK.init_completed, false) != null;
            List<String> children = zk.getChildren(VoltZK.start_action, new StartActionWatcher(), null);
            if (!children.isEmpty()) {
                for (String child : children) {
                    byte[] data = zk.getData(VoltZK.start_action + "/" + child, false, null);
                    if (data == null) {
                        VoltDB.crashLocalVoltDB("Couldn't find " + VoltZK.start_action + "/" + child);
                    }
                    String startAction = new String(data);
                    if ((startAction.equals(StartAction.JOIN.toString()) ||
                            startAction.equals(StartAction.REJOIN.toString()) ||
                            startAction.equals(StartAction.LIVE_REJOIN.toString())) &&
                            !initCompleted) {
                        int nodeId = VoltZK.getHostIDFromChildName(child);
                        if (nodeId == m_hostMessenger.getHostId()) {
                            VoltDB.crashLocalVoltDB("This node was started with start action " + startAction +
                                    " during cluster creation. All nodes should be started with matching "
                                    + "create or recover actions when bring up a cluster. Join and Rejoin "
                                    + "are for adding nodes to an already running cluster.");
                        } else {
                            logger.warn("Node " + nodeId + " tried to " + startAction + " cluster but it is not allowed during cluster creation. "
                                    + "All nodes should be started with matching create or recover actions when bring up a cluster. "
                                    + "Join and rejoin are for adding nodes to an already running cluster.");
                        }
                    }
                }
            }
        } catch (KeeperException e) {
            logger.error("Failed to validate the start actions:" + e.getMessage());
        } catch (InterruptedException e) {
            VoltDB.crashLocalVoltDB("Interrupted during start action validation:" + e.getMessage(), true, e);
        }
    }

    @Override
    public boolean isRunning()
    {
        return false;
    }

    @Override
    public void readBuildInfo()
    {
    }

    @Override
    public void run()
    {
    }

    @Override
    public boolean shutdown(Thread mainSiteThread) throws InterruptedException
    {
        VoltDB.wasCrashCalled = false;
        VoltDB.crashMessage = null;
        m_snapshotCompletionMonitor.shutdown();
        m_es.shutdown();
        m_es.awaitTermination( 1, TimeUnit.DAYS);
        m_statsAgent.shutdown();
        m_hostMessenger.shutdown();
        return true;
    }

    @Override
    public boolean isMpSysprocSafeToExecute(long txnId)
    {
        return true;
    }

    @Override
    public StartAction getStartAction() {
        return voltconfig.m_startAction;
    }

    @Override
    public void startSampler()
    {
    }

    @Override
    public CatalogContext catalogUpdate(String diffCommands,
            int expectedCatalogVersion, int nextCatalogVersion, long genId,
            boolean isForReplay, boolean requireCatalogDiffCmdsApplyToEE,
            boolean hasSchemaChange, boolean requiresNewExportGeneration, boolean hasSecurityUserChange,
            Consumer<Map<Byte, String[]>> replicableTablesConsumer)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public CatalogContext settingsUpdate(ClusterSettings settings, int expectedVersionId)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public BackendTarget getBackendTargetType() {
        return BackendTarget.NONE;
    }

    @Override
    public void logUpdate(String xmlConfig, long currentTxnId, File voltroot)
    {
    }

    @Override
    public void onExecutionSiteRejoinCompletion(long transferred) {
    }

    @Override
    public CommandLog getCommandLog() {
        if (m_cl != null) {
            return m_cl;
        } else {
            return new DummyCommandLog();
        }
    }

    public void setCommandLog(CommandLog cl) {
        m_cl = cl;
    }

    @Override
    public boolean rejoining() {
        return false;
    }

    @Override
    public String getVoltDBRootPath(PathsType.Voltdbroot path) { return path.getPath(); }
    @Override
    public String getCommandLogPath(PathsType.Commandlog path) { return path.getPath(); }
    @Override
    public String getCommandLogSnapshotPath(PathsType.Commandlogsnapshot path) { return path.getPath(); }
    @Override
    public String getSnapshotPath(PathsType.Snapshots path) { return path.getPath(); }
    @Override
    public String getExportOverflowPath(PathsType.Exportoverflow path) { return path.getPath(); }
    @Override
    public String getDROverflowPath(PathsType.Droverflow path) { return path.getPath(); }
    @Override
    public String getLargeQuerySwapPath(PathsType.Largequeryswap path) { return path.getPath(); }
    @Override
    public String getExportCursorPath(PathsType.Exportcursor path) { return path.getPath(); }

    @Override
    public String getCommandLogPath() {
        return "command_log";
    }

    @Override
    public void loadLegacyPathProperties(DeploymentType deployment) throws IOException {
    }

    @Override
    public String getVoltDBRootPath() {
        return "voltdbroot";
    }

    @Override
    public File getExportOverflowPath() {
        return new File("export_overflow");
    }

    @Override
    public String getDROverflowPath() {
        return "dr_overflow";
    }

    @Override
    public String getLargeQuerySwapPath() {
        return "large_query_swap";
    }

    @Override
    public String getExportCursorPath() {
        return  "export_cursor";
    }

    @Override
    public File getTopicsDataPath() {
        return new File("topic_data");
    }

    @Override
    public boolean isBare() {
        return false;
    }

    @Override
    public boolean rejoinDataPending() {
        return false;
    }

    @Override
    public OperationMode getMode()
    {
        return m_mode;
    }

    @Override
    public void setMode(OperationMode mode)
    {
        m_mode = mode;
    }

    @Override
    public String getLocalMetadata() {
        return m_localMetadata;
    }

    @Override
    public void setStartMode(OperationMode mode) {
        m_startMode = mode;
    }

    @Override
    public OperationMode getStartMode()
    {
        return m_startMode;
    }

    @Override
    public NodeState getNodeState()
    {
        return NodeState.UP; // a small lie
    }

    @Override
    public boolean getNodeStartupComplete()
    {
        return true;
    }

    @Override
    public int[] getNodeStartupProgress()
    {
        int[] p = { 1, 1 };
        return p;
    }

    @Override
    public void reportNodeStartupProgress(int c, int t)
    {
    }

    @Override
    public int getMyHostId()
    {
        return m_hostId;
    }

    @Override
    public int getVoltPid()
    {
        return 9999; // no-one cares
    }

    @Override
    public void promoteToMaster()
    {
        m_replicationRole = ReplicationRole.NONE;
    }

    @Override
    public ReplicationRole getReplicationRole()
    {
        return m_replicationRole;
    }

    @Override
    public SnapshotCompletionMonitor getSnapshotCompletionMonitor() {
        return m_snapshotCompletionMonitor;
    }

    @Override
    public ScheduledExecutorService getSES(boolean priority) {
        return null;
    }

    @Override
    public ScheduledFuture<?> scheduleWork(Runnable work, long initialDelay, long delay, TimeUnit unit) {
        if (delay > 0) {
            return m_periodicWorkThread.scheduleWithFixedDelay(work, initialDelay, delay, unit);
        } else {
            return m_periodicWorkThread.schedule(work, initialDelay, unit);
        }
    }

    @Override
    public ListeningExecutorService getComputationService() {
        return m_es;
    }

    @Override
    public void setReplicationActive(boolean active)
    {
        m_replicationActive = active;
    }

    @Override
    public boolean getReplicationActive()
    {
        return m_replicationActive;
    }

    @Override
    public ProducerDRGateway getNodeDRGateway()
    {
        return null;
    }

    @Override
    public SiteTracker getSiteTrackerForSnapshot() {
        return m_siteTracker;
    }

    @Override
    public void recoveryComplete(String requestId) {
    }

    @Override
    public Licensing getLicensing() {
        // So far, we have no need to provide this interface
        throw new UnsupportedOperationException("MockVoltDB.getLicensing called, but is not implemented");
    }

    @Override
    public <T> ListenableFuture<T> submitSnapshotIOWork(Callable<T> work)
    {
        return null;
    }

    @Override
    public ScheduledFuture<?> schedulePriorityWork(Runnable work,
            long initialDelay, long delay, TimeUnit unit) {
        return m_periodicWorkThread.scheduleWithFixedDelay(work, initialDelay, delay, unit);
    }

    @Override
    public long getClusterUptime() {
        return 0;
    }

    @Override
    public long getClusterCreateTime() {
        return m_clusterCreateTime;
    }

    @Override
    public void setClusterCreateTime(long clusterCreateTime) {
        m_clusterCreateTime = clusterCreateTime;
    }

    @Override
    public Instant getHostStartTime() {
        return m_hostStartTime;
    }

    @Override
    public void halt() {
        assert (true);
    }

    @Override
    public ConsumerDRGateway getConsumerDRGateway() {
        return null;
    }

    @Override
    public void configureDurabilityUniqueIdListener(Integer partition, DurableUniqueIdListener listener, boolean install) {
    }

    @Override
    public void onSyncSnapshotCompletion() {
    }

    @Override
    public boolean isPreparingShuttingdown() {
        return false;
    }

    @Override
    public void setShuttingdown(boolean shuttingdown) {
    }

    @Override
    public Cartographer getCartographer() {
        return null;
    }

    @Override
    public SnmpTrapSender getSnmpTrapSender() {
        return new DummySnmpTrapSender();
    }

    @Override
    public void swapTables(String oneTable, String otherTable) {
    }

    @Override
    public HTTPAdminListener getHttpAdminListener() {
        return null;
    }

    @Override
    public long getLowestSiteId() {
        return 0;
    }

    @Override
    public int getLowestPartitionId() {
        return 0;
    }

    @Override
    public int getKFactor() {
        return m_kfactor;
    }

    public void setKFactor(int kfactor) {
        m_kfactor = kfactor;
    }

    @Override
    public boolean isJoining() {return false;}

    @Override
    public ElasticService getElasticService() {
        return null;
    }

    @Override
    public boolean isClusterComplete() {
        return true;
    }

    @Override
    public TaskManager getTaskManager() {
        return null;
    }

    @Override
    public void notifyOfShutdown() {
     }

    @Override
    public boolean isMasterOnly() {
        return false;
    }

    @Override
    public void setMasterOnly() {}

    @Override
    public AvroSerde getAvroSerde() {
        throw new UnsupportedOperationException();
    }

    @Override
    public DrProducerCatalogCommands getDrCatalogCommands() {
        throw new UnsupportedOperationException();
    }
}
