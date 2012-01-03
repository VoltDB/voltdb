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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.zookeeper_voltpatches.WatchedEvent;
import org.apache.zookeeper_voltpatches.Watcher;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.voltdb.RestoreAgent.SnapshotInfo;
import org.voltdb.VoltDB.START_ACTION;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Partition;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.network.WriteStream;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltFile;
import org.voltdb.zk.ZKTestBase;

@RunWith(Parameterized.class)
public class TestRestoreAgent extends ZKTestBase implements RestoreAgent.Callback {
    static int uid = 0;

    @Parameters
    public static Collection<Object[]> startActions() {
        return Arrays.asList(new Object[][] {{START_ACTION.CREATE},
                                             {START_ACTION.START},
                                             {START_ACTION.RECOVER}});
    }

    /**
     * The start action to use for some of the tests
     */
    protected final START_ACTION action;
    public TestRestoreAgent(START_ACTION action) {
        this.action = action;
    }

    class MockSnapshotMonitor extends SnapshotCompletionMonitor {
        @Override
        public void init(final ZooKeeper zk) {
            try {
                Watcher watcher = new Watcher() {
                    @Override
                    public void process(WatchedEvent event) {
                        try {
                            List<String> children = zk.getChildren("/", this);
                            for (String s : children) {
                                if (s.equals("request_truncation_snapshot")) {
                                    snapshotted = true;
                                    LinkedList<SnapshotCompletionInterest> interests =
                                            new LinkedList<SnapshotCompletionInterest>(m_interests);
                                    for (SnapshotCompletionInterest i : interests) {
                                        i.snapshotCompleted( "", 0, true);
                                    }
                                    break;
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                };

                zk.getChildren("/", watcher);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    List<File> paths = new ArrayList<File>();

    SnapshotCompletionMonitor snapshotMonitor = null;
    boolean snapshotted = false;

    CatalogContext context;
    File catalogJarFile;
    String deploymentPath;

    private final AtomicInteger m_count = new AtomicInteger();
    volatile int m_hostCount = 0;
    volatile boolean m_done = false;
    final Set<String> m_unexpectedSPIs = new HashSet<String>();
    protected Long snapshotTxnId = null;

    /**
     * A mock initiator that checks if the we have all the initiations we
     * expect.
     */
    class MockInitiator extends TransactionInitiator {
        private final Map<String, Long> procCounts = new HashMap<String, Long>();

        public MockInitiator(Set<String> procNames) {
            if (procNames != null) {
                for (String proc : procNames) {
                    procCounts.put(proc, 0l);
                }
            }
        }

        public Map<String, Long> getProcCounts() {
            return procCounts;
        }

        @Override
        public boolean createTransaction(long connectionId,
                                      String connectionHostname,
                                      boolean adminConnection,
                                      StoredProcedureInvocation invocation,
                                      boolean isReadOnly,
                                      boolean isSinglePartition,
                                      boolean isEverySite,
                                      int[] partitions,
                                      int numPartitions,
                                      Object clientData,
                                      int messageSize,
                                      long now) {
            createTransaction(connectionId, connectionHostname, adminConnection,
                              0, invocation, isReadOnly, isSinglePartition,
                              isEverySite, partitions, numPartitions,
                              clientData, messageSize, now);
            return true;
        }

        @Override
        public boolean createTransaction(long connectionId,
                                      String connectionHostname,
                                      boolean adminConnection,
                                      long txnId,
                                      StoredProcedureInvocation invocation,
                                      boolean isReadOnly,
                                      boolean isSinglePartition,
                                      boolean isEverySite,
                                      int[] partitions,
                                      int numPartitions,
                                      Object clientData,
                                      int messageSize,
                                      long now) {
            String procName = invocation.procName;
            if (!procCounts.containsKey(procName)) {
                m_unexpectedSPIs.add(procName);
            } else {
                procCounts.put(procName, procCounts.get(procName) + 1);
            }

            // Fake success
            ColumnInfo[] columns = new ColumnInfo[] {new ColumnInfo("RESULT", VoltType.STRING)};
            VoltTable result = new VoltTable(columns);
            result.addRow("SUCCESS");
            VoltTable[] results = new VoltTable[] {result};
            ClientResponseImpl response = new ClientResponseImpl(ClientResponse.SUCCESS,
                                                                 results, null);
            ((WriteStream) clientData).enqueue(response);
            return true;
        }

        @Override
        public long tick() {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getMostRecentTxnId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void notifyExecutionSiteRejoin(ArrayList<Integer> executorSiteIds) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<Long, long[]> getOutstandingTxnStats() {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void increaseBackpressure(int messageSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected void reduceBackpressure(int messageSize) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setSendHeartbeats(boolean val) {

        }

        @Override
        public void sendHeartbeat(long txnId) {

        }

        @Override
        public boolean isOnBackPressure() {
            return false;
        }

        @Override
        public void removeConnectionStats(long connectionId) {

        }
    }

    void buildCatalog(int hostCount, int sitesPerHost, int kfactor, String voltroot,
                      boolean excludeProcs, boolean rebuildAll)
    throws IOException {
        /*
         * This suite is intended only to test RestoreAgentWithout command logging.
         * They fail with the pro build and command logging enabled because they are
         * testing for community before. TestRestoreAgentWithReplay tests with CL enabled.
         * We do want to check that the community edition doesn't barf if command logging is
         * accidentally request, and we want to run these tests with the pro build as well
         * so we switch CL enabled on whether this is a pro build.
         */
        buildCatalog(hostCount, sitesPerHost, kfactor, voltroot, isEnterprise() ? false : true,
                     excludeProcs, rebuildAll);
    }

    /**
     * Build a new catalog context.
     *
     * @param hostCount
     * @param sitesPerHost
     * @param kfactor
     * @param voltroot
     * @param excludeProcs  used to create a different catalog to check CRC match
     * @param rebuildAll TODO
     * @throws IOException
     */
    void buildCatalog(int hostCount, int sitesPerHost, int kfactor, String voltroot,
                      boolean commandLog, boolean excludeProcs, boolean rebuildAll)
    throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "create table A (i integer not null, s varchar(30), sh smallint, l bigint, primary key (i));";
        builder.addLiteralSchema(schema);
        builder.addPartitionInfo("A", "i");
        builder.addStmtProcedure("hello", "select * from A where i = ? and s = ?", "A.i: 0");
        builder.addStmtProcedure("world", "select * from A where i = ? and sh = ? and s = ?", "A.i: 0");
        if (!excludeProcs)
        {
            builder.addStmtProcedure("bid", "select * from A where i = ? and sh = ? and s = ?", "A.i: 0");
            builder.addStmtProcedure("sum", "select * from A where i = ? and sh = ? and s = ?", "A.i: 0");
            builder.addStmtProcedure("calc", "select * from A where i = ? and sh = ? and s = ?", "A.i: 0");
            builder.addStmtProcedure("HowWillAppleNameItsVaccuumCleaner", "select * from A where l = ? and sh = ? and s = ?");
            builder.addStmtProcedure("Dedupe", "select * from A where l = ? and sh = ?");
            builder.addStmtProcedure("Cill", "select * from A where l = ? and s = ?");
        }
        builder.configureLogging(voltroot, voltroot, false, commandLog, 200, 20000, 300);


        File cat = File.createTempFile("temp-restore", "catalog");
        cat.deleteOnExit();
        assertTrue(builder.compile(cat.getAbsolutePath(), sitesPerHost,
                                   hostCount, kfactor, voltroot));
        deploymentPath = builder.getPathToDeployment();

        File cat_to_use = cat;
        if (rebuildAll)
        {
            catalogJarFile = cat;
        }
        else
        {
            cat_to_use = catalogJarFile;
        }

        byte[] bytes = CatalogUtil.toBytes(cat_to_use);
        String serializedCat = CatalogUtil.loadCatalogFromJar(bytes, null);
        assertNotNull(serializedCat);
        Catalog catalog = new Catalog();
        catalog.execute(serializedCat);

        long crc = CatalogUtil.compileDeploymentAndGetCRC(catalog, deploymentPath,
                                                          true);
        context = new CatalogContext(0, catalog, bytes, crc, 0, 0);
    }

    /**
     * Take a snapshot
     * @throws IOException
     */
    void snapshot() throws IOException {
        String path = context.cluster.getVoltroot() + File.separator + "snapshots";
        ClientConfig clientConfig = new ClientConfig();
        Client client = ClientFactory.createClient(clientConfig);
        client.createConnection("localhost");
        ClientResponse response = null;
        try {
            response = client.callProcedure("@SnapshotSave",
                                            path,
                                            "hello",
                                            1);
        } catch (ProcCallException e) {
            fail(e.getMessage());
        }

        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(1, response.getResults().length);
        assertTrue(response.getResults()[0].advanceRow());
        assertEquals("SUCCESS", response.getResults()[0].getString("RESULT"));
    }

    /**
     * Generate a new voltroot with the given suffix. Will be automatically
     * cleaned after the test.
     *
     * @param suffix
     * @return The new voltroot
     * @throws IOException
     */
    String newVoltRoot(String suffix) throws IOException {
        if (suffix == null) {
            suffix = "";
        }
        File path = File.createTempFile("temp", "restore-test-" + suffix);
        path.delete();
        path = new VoltFile(path.getAbsolutePath());
        assertTrue(path.mkdir());
        paths.add(path);

        return path.getAbsolutePath();
    }

    @Before
    public void setUp() throws Exception {
        m_count.set(0);
        m_done = false;
        snapshotted = false;
        m_unexpectedSPIs.clear();
        setUpZK(1);
        snapshotMonitor = new MockSnapshotMonitor();
        snapshotMonitor.init(getClient(0));
    }

    @After
    public void tearDown() throws Exception {
        String msg = "";
        for (String name : m_unexpectedSPIs) {
            msg += name + ", ";
        }
        assertTrue(msg, m_unexpectedSPIs.isEmpty());
        for (File p : paths) {
            VoltFile.recursivelyDelete(p);
        }
        tearDownZK();
    }

    /**
     * Check if nothing is recovered if the action is create
     * @param initiator
     */
    protected void createCheck(MockInitiator initiator) {
        assertEquals(Long.MIN_VALUE, snapshotTxnId.longValue());
        assertFalse(snapshotted);
        if (!initiator.getProcCounts().isEmpty()) {
            for (long count : initiator.getProcCounts().values()) {
                assertEquals(0, count);
            }
        }
    }


    private boolean isEnterprise() {
        return VoltDB.instance().getConfig().m_isEnterprise;
    }

    protected RestoreAgent getRestoreAgent(MockInitiator initiator, int hostId) throws Exception {
        String snapshotPath = null;
        if (context.cluster.getDatabases().get("database").getSnapshotschedule().get("default") != null) {
            snapshotPath = context.cluster.getDatabases().get("database").getSnapshotschedule().get("default").getPath();
        }

        int lowestSite = context.siteTracker.getLowestLiveNonExecSiteId();
        int lowestHostId = context.siteTracker.getHostForSite(lowestSite);

        int[] allPartitions = new int[context.numberOfPartitions];
        int i = 0;
        for (Partition p : context.cluster.getPartitions()) {
            allPartitions[i++] = Integer.parseInt(p.getTypeName());
        }

        org.voltdb.catalog.CommandLog cl = context.cluster.getLogconfig().get("log");

        RestoreAgent restoreAgent = new RestoreAgent(getClient(0),
                                                     snapshotMonitor, this,
                                                     hostId, this.action,
                                                     context.numberOfPartitions,
                                                     cl.getEnabled(),
                                                     cl.getLogpath(),
                                                     cl.getInternalsnapshotpath(),
                                                     snapshotPath,
                                                     lowestHostId,
                                                     allPartitions,
                                                     context.siteTracker.getAllLiveHosts());
        restoreAgent.setCatalogContext(context);
        assert(initiator != null);
        restoreAgent.setInitiator(initiator);
        return restoreAgent;
    }


    @Test
    public void testSingleHostEmptyRestore() throws Exception {
        m_hostCount = 1;
        buildCatalog(m_hostCount, 8, 0, newVoltRoot(null), false, true);
        MockInitiator initiator = new MockInitiator(null);
        RestoreAgent restoreAgent = getRestoreAgent(initiator, 0);
        restoreAgent.createZKDirectory(RestoreAgent.RESTORE);
        restoreAgent.createZKDirectory(RestoreAgent.RESTORE_BARRIER);
        restoreAgent.enterRestore();
        assertNull(restoreAgent.generatePlans());
    }

    @Test
    public void testMultipleHostEmptyRestore() throws Exception {
        m_hostCount = 3;
        buildCatalog(m_hostCount, 8, 0, newVoltRoot(null), false, true);
        MockInitiator initiator = new MockInitiator(null);
        List<RestoreAgent> agents = new ArrayList<RestoreAgent>();

        for (int i = 0; i < m_hostCount; i++) {
            agents.add(getRestoreAgent(initiator, i));
        }

        ExecutorService ex = Executors.newFixedThreadPool(3);
        final AtomicInteger failure = new AtomicInteger();
        for (final RestoreAgent agent : agents) {
            ex.submit(new Runnable() {
                @Override
                public void run() {
                    agent.createZKDirectory(RestoreAgent.RESTORE);
                    agent.createZKDirectory(RestoreAgent.RESTORE_BARRIER);

                    agent.enterRestore();

                    try {
                        if (agent.generatePlans() != null) {
                            failure.incrementAndGet();
                        }
                    } catch (Exception e) {
                        failure.incrementAndGet();
                    }
                }
            });
        }
        ex.shutdown();
        assertTrue(ex.awaitTermination(10, TimeUnit.SECONDS));
        assertEquals(0, failure.get());
    }

    @Test
    public void testMultipleHostAgreementFailure() throws Exception {
        // Don't run this test if we are in recovery mode
        if (action == START_ACTION.RECOVER) {
            return;
        }

        m_hostCount = 3;
        buildCatalog(m_hostCount, 8, 0, newVoltRoot(null), false, true);
        MockInitiator initiator = new MockInitiator(null);
        List<RestoreAgent> agents = new ArrayList<RestoreAgent>();

        for (int i = 0; i < m_hostCount - 1; i++) {
            agents.add(getRestoreAgent(initiator, i));
        }
        for (RestoreAgent agent : agents) {
            agent.restore();
        }

        int count = 0;
        while (!m_done && count++ < 10) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
        }

        if (m_done) {
            fail();
        }

        // Start the last restore agent, should be able to reach agreement now
        RestoreAgent agent = getRestoreAgent(initiator, m_hostCount - 1);
        agent.restore();

        count = 0;
        while (!m_done && count++ < 10) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {}
        }

        if (!m_done) {
            fail();
        }

        assertFalse(snapshotted);
        assertEquals(Long.MIN_VALUE, snapshotTxnId.longValue());
    }

    @Test
    public void testSingleHostSnapshotRestore() throws Exception {
        m_hostCount = 1;
        buildCatalog(m_hostCount, 8, 0, newVoltRoot(null), false, true);
        ServerThread server = new ServerThread(catalogJarFile.getAbsolutePath(),
                                               deploymentPath,
                                               BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();
        snapshot();
        server.shutdown();

        HashSet<String> procs = new HashSet<String>();
        procs.add("@SnapshotRestore");
        MockInitiator initiator = new MockInitiator(procs);
        RestoreAgent restoreAgent = getRestoreAgent(initiator, 0);
        restoreAgent.restore();
        while (!m_done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
        }

        if (action != START_ACTION.CREATE) {
            Long count = initiator.getProcCounts().get("@SnapshotRestore");
            assertEquals(new Long(1), count);
            assertEquals(Long.MIN_VALUE, snapshotTxnId.longValue());
        } else {
            createCheck(initiator);
        }
    }

    @Test
    public void testSingleHostSnapshotRestoreCatalogChange() throws Exception {
        m_hostCount = 1;
        String voltroot = newVoltRoot(null);
        buildCatalog(m_hostCount, 8, 0, voltroot, false, true);
        ServerThread server = new ServerThread(catalogJarFile.getAbsolutePath(),
                                               deploymentPath,
                                               BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();
        snapshot();
        server.shutdown();

        buildCatalog(m_hostCount, 8, 0, voltroot, true, true);
        HashSet<String> procs = new HashSet<String>();
        procs.add("@SnapshotRestore");
        MockInitiator initiator = new MockInitiator(procs);
        RestoreAgent restoreAgent = getRestoreAgent(initiator, 0);
        restoreAgent.restore();
        while (!m_done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
        }

        if (action != START_ACTION.CREATE) {
            Long count = initiator.getProcCounts().get("@SnapshotRestore");
            assertEquals(new Long(1), count);
            assertEquals(Long.MIN_VALUE, snapshotTxnId.longValue());
        } else {
            createCheck(initiator);
        }
    }

    @Test
    public void testMultiHostSnapshotRestore() throws Exception {
        m_hostCount = 1;
        String voltroot = newVoltRoot(null);
        buildCatalog(m_hostCount, 8, 0, voltroot, false, true);
        ServerThread server = new ServerThread(catalogJarFile.getAbsolutePath(),
                                               deploymentPath,
                                               BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();
        snapshot();
        server.shutdown();

        m_hostCount = 3;
        buildCatalog(m_hostCount, 8, 1, voltroot, false, true);
        HashSet<String> procs = new HashSet<String>();
        procs.add("@SnapshotRestore");
        MockInitiator initiator = new MockInitiator(procs);
        List<RestoreAgent> agents = new ArrayList<RestoreAgent>();

        for (int i = 0; i < m_hostCount; i++) {
            agents.add(getRestoreAgent(initiator, i));
        }
        for (RestoreAgent agent : agents) {
            agent.restore();
        }

        while (!m_done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
        }

        if (action != START_ACTION.CREATE) {
            Long count = initiator.getProcCounts().get("@SnapshotRestore");
            assertEquals(new Long(1), count);
            assertEquals(Long.MIN_VALUE, snapshotTxnId.longValue());
        } else {
            createCheck(initiator);
        }
    }

    @Test
    public void testConsistentRestorePlan() {
        List<SnapshotInfo> infos = new ArrayList<SnapshotInfo>();
        SnapshotInfo info1 = new SnapshotInfo(0, "blah", "nonce", 3, 0, 0);
        SnapshotInfo info2 = new SnapshotInfo(0, "blah", "nonce", 3, 0, 1);

        infos.add(info1);
        infos.add(info2);
        SnapshotInfo pickedInfo = RestoreAgent.pickSnapshotInfo(infos);
        assertNotNull(pickedInfo);
        assertEquals(0, pickedInfo.hostId);

        // Inverse the order we add infos
        infos.clear();
        infos.add(info2);
        infos.add(info1);
        pickedInfo = RestoreAgent.pickSnapshotInfo(infos);
        assertNotNull(pickedInfo);
        assertEquals(0, pickedInfo.hostId);
    }

    @Override
    public void onRestoreCompletion(long txnId) {
        if (snapshotTxnId != null) {
            assertEquals(snapshotTxnId.longValue(), txnId);
        }
        snapshotTxnId = txnId;

        if (m_count.incrementAndGet() == m_hostCount) {
            m_done = true;
        }
    }

    void addSnapshotInfo(Map<Long, Set<SnapshotInfo>> frags,
                         long txnid, long crc)
    {
        Set<SnapshotInfo> si = null;
        if (frags.containsKey(txnid))
        {
            si = frags.get(txnid);
        }
        else
        {
            si = new HashSet<SnapshotInfo>();
            frags.put(txnid, si);
        }
        si.add(new SnapshotInfo(txnid, "dummy", "dummy", 1, crc));
    }
}
