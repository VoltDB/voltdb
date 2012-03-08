/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltZK.MailboxType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.dtxn.MailboxPublisher;
import org.voltdb.dtxn.SiteTracker;
import org.voltdb.fault.FaultDistributorInterface;

public class MockVoltDB implements VoltDBInterface
{
    private Catalog m_catalog;
    private CatalogContext m_context;
    final String m_clusterName = "cluster";
    final String m_databaseName = "database";
    StatsAgent m_statsAgent = null;
    int m_howManyCrashes = 0;
    FaultDistributorInterface m_faultDistributor = null;
    HostMessenger m_hostMessenger = new HostMessenger(new HostMessenger.Config());
    private OperationMode m_mode = OperationMode.RUNNING;
    private volatile String m_localMetadata;
    final SnapshotCompletionMonitor m_snapshotCompletionMonitor = new SnapshotCompletionMonitor();
    boolean m_noLoadLib = false;
    public boolean shouldIgnoreCrashes = false;
    OperationMode m_startMode = OperationMode.RUNNING;
    ReplicationRole m_replicationRole = ReplicationRole.NONE;
    private final ExecutorService m_es = Executors.newSingleThreadExecutor();
    public int m_hostId = 0;
    private SiteTracker m_siteTracker;
    private final Map<MailboxType, List<MailboxNodeContent>> m_mailboxMap =
        new HashMap<MailboxType, List<MailboxNodeContent>>();
    private final MailboxPublisher m_mailboxPublisher = new MailboxPublisher(VoltZK.mailboxes + "/0");

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
            m_localMetadata = obj.toString(4);

            m_catalog = new Catalog();
            m_catalog.execute("add / clusters " + m_clusterName);
            m_catalog.execute("add " + m_catalog.getClusters().get(m_clusterName).getPath() + " databases " +
                              m_databaseName);
            Cluster cluster = m_catalog.getClusters().get(m_clusterName);
            // Set a sane default for TestMessaging (at least)
            cluster.setHeartbeattimeout(10000);
            assert(cluster != null);

            try {
                m_hostMessenger.start();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            VoltZK.createPersistentZKNodes(m_hostMessenger.getZK());
            m_hostMessenger.getZK().create(
                    VoltZK.cluster_metadata + "/" + m_hostMessenger.getHostId(),
                    getLocalMetadata().getBytes("UTF-8"),
                    Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);

            m_hostMessenger.generateMailboxId(m_hostMessenger.getHSIdForLocalSite(HostMessenger.STATS_SITE_ID));
            m_statsAgent = new StatsAgent();
            m_statsAgent.getMailbox(m_hostMessenger,
                    m_hostMessenger.getHSIdForLocalSite(HostMessenger.STATS_SITE_ID));
            for (MailboxType type : MailboxType.values()) {
                m_mailboxMap.put(type, new LinkedList<MailboxNodeContent>());
            }
            m_mailboxMap.get(MailboxType.StatsAgent).add(
                    new MailboxNodeContent(m_hostMessenger.getHSIdForLocalSite(HostMessenger.STATS_SITE_ID), null));
            m_siteTracker = new SiteTracker(m_hostId, m_mailboxMap);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Procedure addProcedureForTest(String name)
    {
        Procedure retval = getCluster().getDatabases().get(m_databaseName).getProcedures().add(name);
        retval.setClassname(name);
        retval.setHasjava(true);
        retval.setSystemproc(false);
        return retval;
    }

    private final Hashtable<Long, ExecutionSite> m_localSites =
        new Hashtable<Long, ExecutionSite>();

    public void addSite(long siteId, MailboxType type) {
        m_mailboxMap.get(type).add(new MailboxNodeContent(siteId, null));
        m_siteTracker = new SiteTracker(m_hostId, m_mailboxMap);
    }

    public void addSite(long siteId, int partitionId)
    {
        if (((int)siteId) == 0) {
            m_localSites.put(siteId, new ExecutionSite(partitionId));
        }
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
        logConfig.setLogpath(logPath);
        logConfig.setInternalsnapshotpath(snapshotPath);
    }

    public void addColumnToTable(String tableName, String columnName,
                                    VoltType columnType,
                                    boolean isNullable, String defaultValue,
                                    VoltType defaultType)
    {
        int index = getTable(tableName).getColumns().size();
        getTable(tableName).getColumns().add(columnName);
        getColumnFromTable(tableName, columnName).setIndex(index);
        getColumnFromTable(tableName, columnName).setType(columnType.getValue());
        getColumnFromTable(tableName, columnName).setNullable(isNullable);
        getColumnFromTable(tableName, columnName).setName(columnName);
        getColumnFromTable(tableName, columnName).setDefaultvalue(defaultValue);
        getColumnFromTable(tableName, columnName).setDefaulttype(defaultType.getValue());
    }

    public Cluster getCluster()
    {
        return m_catalog.getClusters().get(m_clusterName);
    }

    public int getCrashCount() {
        return m_howManyCrashes;
    }

    public Database getDatabase()
    {
        return getCluster().getDatabases().get(m_databaseName);
    }

    public Table getTable(String tableName)
    {
        return getDatabase().getTables().get(tableName);
    }

    Column getColumnFromTable(String tableName, String columnName)
    {
        return getTable(tableName).getColumns().get(columnName);
    }

    @Override
    public String getBuildString()
    {
        return null;
    }

    @Override
    public CatalogContext getCatalogContext()
    {
        m_context = new CatalogContext( System.currentTimeMillis(), m_catalog, null, 0, 0, 0) {
            @Override
            public long getCatalogCRC() {
                return 13;
            }
        };
        return m_context;
    }

    @Override
    public ArrayList<ClientInterface> getClientInterfaces()
    {
        return null;
    }

    @Override
    public Configuration getConfig()
    {
        return new VoltDB.Configuration();
    }

    public void setFaultDistributor(FaultDistributorInterface distributor)
    {
        m_faultDistributor = distributor;
    }

    @Override
    public FaultDistributorInterface getFaultDistributor()
    {
        return m_faultDistributor;
    }

    public void setHostMessenger(HostMessenger msg) {
        m_hostMessenger = msg;
    }

    @Override
    public HostMessenger getHostMessenger()
    {
        return m_hostMessenger;
    }

    @Override
    public Hashtable<Long, ExecutionSite> getLocalSites()
    {
        return m_localSites;
    }

    public void setStatsAgent(StatsAgent agent)
    {
        m_statsAgent = agent;
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
    public boolean ignoreCrash()
    {
        if (shouldIgnoreCrashes) {
            m_howManyCrashes++;
            return true;
        } else {
            return false;
        }
    }

    @Override
    public void initialize(Configuration config)
    {
        m_noLoadLib = config.m_noLoadLibVOLTDB;
    }

    @Override
    public boolean isRunning()
    {
        return false;
    }

    @Override
    public void readBuildInfo(String editionTag)
    {
    }

    @Override
    public void run()
    {
    }

    @Override
    public void shutdown(Thread mainSiteThread) throws InterruptedException
    {
        if (m_faultDistributor != null) {
            m_faultDistributor.shutDown();
        }
        m_snapshotCompletionMonitor.shutdown();
        m_es.shutdown();
        m_es.awaitTermination( 1, TimeUnit.DAYS);
        m_statsAgent.shutdown();
        m_hostMessenger.shutdown();
    }

    @Override
    public void startSampler()
    {
    }

    @Override
    public CatalogContext catalogUpdate(String diffCommands,
            byte[] catalogBytes, int expectedCatalogVersion,
            long currentTxnId, long deploymentCRC)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public BackendTarget getBackendTargetType() {
        return BackendTarget.NONE;
    }

    @Override
    public void logUpdate(String xmlConfig, long currentTxnId)
    {
    }

    @Override
    public String doRejoinCommitOrRollback(long currentTxnId, boolean commit) {
        return null;
    }

    @Override
    public String doRejoinPrepare(long currentTxnId, int rejoinHostId,
            String rejoiningHostname, int portToConnect, Set<Integer> liveHosts) {
        return null;
    }

    @Override
    public void onExecutionSiteRecoveryCompletion(long transferred) {
    }

    @Override
    public CommandLog getCommandLog() {
        return new DummyCommandLog();
    }

    @Override
    public boolean recovering() {
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
    public void setReplicationRole(ReplicationRole role)
    {
        m_replicationRole = role;
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
    public ScheduledFuture<?> scheduleWork(Runnable work, long initialDelay, long delay, TimeUnit unit) {
        return null;
    }

    @Override
    public ExecutorService getComputationService() {
        return m_es;
    }

    @Override
    public void setReplicationActive(boolean active)
    {
    }

    @Override
    public boolean getReplicationActive()
    {
        return false;
    }

    @Override
    public SiteTracker getSiteTracker() {
        return m_siteTracker;
    }

    @Override
    public MailboxPublisher getMailboxPublisher() {
        return m_mailboxPublisher;
    }

    @Override
    public void recoveryComplete() {
        // TODO Auto-generated method stub
    }

    public LicenseApi getLicenseApi()
    {
        return new LicenseApi() {
            @Override
            public boolean initializeFromFile(File license) {
                return true;
            }

            @Override
            public boolean isTrial() {
                return false;
            }

            @Override
            public int maxHostcount() {
                return Integer.MAX_VALUE;
            }

            @Override
            public Calendar expires() {
                Calendar result = Calendar.getInstance();
                result.add(Calendar.YEAR, 20); // good enough?
                return result;
            }

            @Override
            public boolean verify() {
                return true;
            }

            @Override
            public boolean isWanReplicationAllowed() {
                // TestExecutionSite (and probably others)
                // use MockVoltDB without requiring unique
                // zmq ports for the wan replicator. Note
                // that getReplicationActive(), above, is
                // hardcoded to false, too.
                return false;
            }

            @Override
            public boolean isCommandLoggingAllowed() {
                return true;
            }
        };
    }
}
