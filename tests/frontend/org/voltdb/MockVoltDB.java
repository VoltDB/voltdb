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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONObject;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
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
//    {
//        @Override
//        public void send(final int siteId, final int mailboxId, final VoltMessage message)
//            throws MessagingException {
//            Mailbox mailbox = MockMailbox.postoffice.get(mailboxId).get(siteId);
//            if (mailbox != null) {
//                mailbox.deliver(message);
//            }
//        }
//
//        @Override
//        public void send(int[] siteIds, int mailboxId, final VoltMessage message)
//            throws MessagingException {
//            for (int i : siteIds) {
//                Mailbox mailbox = MockMailbox.postoffice.get(mailboxId).get(i);
//                if (mailbox != null) {
//                    mailbox.deliver(message);
//                }
//            }
//        }
//
//        @Override
//        public int getHostId() {
//            return 1;
//        }
//    };
    private OperationMode m_mode = OperationMode.RUNNING;
    private volatile String m_localMetadata;
    private final Map<Integer, String> m_clusterMetadata = Collections.synchronizedMap(new HashMap<Integer, String>());
    final SnapshotCompletionMonitor m_snapshotCompletionMonitor = new SnapshotCompletionMonitor();
    boolean m_noLoadLib = false;
    public boolean shouldIgnoreCrashes = false;
    OperationMode m_startMode = OperationMode.RUNNING;
    ReplicationRole m_replicationRole = ReplicationRole.NONE;
    private final ExecutorService m_es = Executors.newSingleThreadExecutor();

    public MockVoltDB()
    {
        try {
            JSONObject obj = new JSONObject();
            JSONArray jsonArray = new JSONArray();
            jsonArray.put("127.0.0.1");
            obj.put("interfaces", jsonArray);
            obj.put("clientPort", 21212);
            obj.put("adminPort", 21211);
            obj.put("httpPort", -1);
            obj.put("drPort", 5555);
            m_localMetadata = obj.toString(4);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        m_catalog = new Catalog();
        m_catalog.execute("add / clusters " + m_clusterName);
        m_catalog.execute("add " + m_catalog.getClusters().get(m_clusterName).getPath() + " databases " +
                          m_databaseName);
        Cluster cluster = m_catalog.getClusters().get(m_clusterName);
        // Set a sane default for TestMessaging (at least)
        cluster.setHeartbeattimeout(10000);
        assert(cluster != null);

        /*Host host = cluster.getHosts().add("0");
        Site execSite = cluster.getSites().add("1");
        Site initSite = cluster.getSites().add("0");
        Partition partition = cluster.getPartitions().add("0");

        host.setIpaddr("localhost");

        initSite.setHost(host);
        initSite.setIsexec(false);
        initSite.setInitiatorid(0);
        initSite.setIsup(true);

        execSite.setHost(host);
        execSite.setIsexec(true);
        execSite.setIsup(true);
        execSite.setPartition(partition);*/

//        MockMailbox.postoffice.get(VoltDB.STATS_MAILBOX_ID).put(1, mbox);
//        MockMailbox mailbox = new MockMailbox();
//        MockMailbox.registerMailbox(1, VoltDB.AGREEMENT_MAILBOX_ID, mailbox);
/*
         try {
            m_agreementSite =
                new AgreementSite(
                    1,
                    new HashSet<Integer>(Arrays.asList(1)),
                    1,
                    new HashSet<Integer>(),
                    mailbox,
                    new InetSocketAddress(2181),
                    null,
                    false);
            m_agreementSite.start();
            m_zk = org.voltdb.agreement.ZKUtil.getClient("localhost:2181", 60 * 1000);
            m_snapshotCompletionMonitor.init(m_zk);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        */
        try {
            m_hostMessenger.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        VoltZK.createPersistentZKNodes(m_hostMessenger.getZK());

        m_statsAgent = new StatsAgent();
        m_statsAgent.getMailbox(m_hostMessenger,
                m_hostMessenger.getHSIdForLocalSite(HostMessenger.STATS_SITE_ID));
    }

    public Procedure addProcedureForTest(String name)
    {
        Procedure retval = getCluster().getDatabases().get(m_databaseName).getProcedures().add(name);
        retval.setClassname(name);
        retval.setHasjava(true);
        retval.setSystemproc(false);
        return retval;
    }

    public void addHost(int hostId)
    {
        getCluster().getHosts().add(Integer.toString(hostId));
        m_clusterMetadata.put(hostId, m_localMetadata);
    }

    public void addPartition(int partitionId)
    {
        getCluster().getPartitions().add(Integer.toString(partitionId));
    }

    private final Hashtable<Long, ExecutionSite> m_localSites =
        new Hashtable<Long, ExecutionSite>();

    public void addSite(long siteId, int hostId, int partitionId, boolean isExec)
    {
        if (hostId == 0) {
            m_localSites.put(siteId, new ExecutionSite(partitionId));
        }
        getCluster().getSites().add(Long.toString(siteId));
        getSite(siteId).setHost(getHost(hostId));
        getSite(siteId).setIsexec(isExec);
        if (isExec)
        {
            getSite(siteId).setPartition(getPartition(partitionId));
        }
        getSite(siteId).setIsup(true);
    }

    public synchronized void killSite(long siteId) {
        m_catalog = m_catalog.deepCopy();
        getSite(siteId).setIsup(false);
    }

    public void addSite(int siteId, int hostId, int partitionId, boolean isExec,
                        boolean isUp)
    {
        addSite(siteId, hostId, partitionId, isExec);
        getSite(siteId).setIsup(isUp);
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

    Host getHost(int hostId)
    {
        return getCluster().getHosts().get(String.valueOf(hostId));
    }

    Partition getPartition(int partitionId)
    {
        return getCluster().getPartitions().get(String.valueOf(partitionId));
    }

    public Site getSite(long siteId)
    {
        return getCluster().getSites().get(String.valueOf(siteId));
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
    public Object[] getInstanceId() {
        return new Object[] { new Long(0), new Integer(0) };
    }

    @Override
    public BackendTarget getBackendTargetType() {
        return BackendTarget.NONE;
    }

    @Override
    public void clusterUpdate(String diffCommands) {
        m_context = m_context.update(System.currentTimeMillis(),
                                     null,
                                     diffCommands, false, -1);
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
    public Map<Integer, String> getClusterMetadataMap() {
        return m_clusterMetadata;
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
    public void onAgreementSiteRecoveryCompletion() {}

    @Override
    public SnapshotCompletionMonitor getSnapshotCompletionMonitor() {
        return m_snapshotCompletionMonitor;
    }

    @Override
    public void recoveryComplete() {}

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
}
