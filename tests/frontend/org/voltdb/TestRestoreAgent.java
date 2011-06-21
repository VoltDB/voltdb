/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Catalog;
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

public class TestRestoreAgent implements RestoreAgent.Callback {
    static int uid = 0;

    List<File> paths = new ArrayList<File>();

    CatalogContext context;
    String deploymentPath;

    private AtomicInteger m_count = new AtomicInteger();
    volatile int m_hostCount = 0;
    volatile boolean m_done = false;
    final Set<String> m_unexpectedSPIs = new HashSet<String>();

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
        public void createTransaction(long connectionId,
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
        }

        @Override
        public void createTransaction(long connectionId,
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
    }

    /**
     * Build a new catalog context.
     *
     * @param hostCount
     * @param sitesPerHost
     * @param kfactor
     * @param voltroot
     * @throws IOException
     */
    void buildCatalog(int hostCount, int sitesPerHost, int kfactor, String voltroot)
    throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "create table A (i integer not null, s varchar(30), sh smallint, l bigint, primary key (i));";
        builder.addLiteralSchema(schema);
        builder.addPartitionInfo("A", "i");
        builder.addStmtProcedure("hello", "select * from A where i = ? and s = ?", "A.i: 0");
        builder.addStmtProcedure("world", "select * from A where i = ? and sh = ? and s = ?", "A.i: 0");
        builder.addStmtProcedure("bid", "select * from A where i = ? and sh = ? and s = ?", "A.i: 0");
        builder.addStmtProcedure("sum", "select * from A where i = ? and sh = ? and s = ?", "A.i: 0");
        builder.addStmtProcedure("calc", "select * from A where i = ? and sh = ? and s = ?", "A.i: 0");
        builder.addStmtProcedure("HowWillAppleNameItsVaccuumCleaner", "select * from A where l = ? and sh = ? and s = ?");
        builder.addStmtProcedure("Dedupe", "select * from A where l = ? and sh = ?");
        builder.addStmtProcedure("Cill", "select * from A where l = ? and s = ?");
        builder.configureLogging(voltroot, voltroot, false, false, 200, 20000);


        File cat = File.createTempFile("temp-restore", "catalog");
        cat.deleteOnExit();
        assertTrue(builder.compile(cat.getAbsolutePath(), sitesPerHost,
                                   hostCount, kfactor, "localhost",
                                   voltroot));
        deploymentPath = builder.getPathToDeployment();

        String serializedCat = CatalogUtil.loadCatalogFromJar(cat.getAbsolutePath(),
                                                              null);
        assertNotNull(serializedCat);
        Catalog catalog = new Catalog();
        catalog.execute(serializedCat);

        long crc = CatalogUtil.compileDeploymentAndGetCRC(catalog, deploymentPath,
                                                          true);
        context = new CatalogContext(0, catalog, cat.getAbsolutePath(), crc, 0, 0);
    }

    /**
     * Take a snapshot
     * @throws IOException
     */
    void snapshot() throws IOException {
        String path = context.cluster.getLogconfig().get("log").getLogpath();
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
    public void setUp() {
        m_count.set(0);
        m_done = false;
        m_unexpectedSPIs.clear();
    }

    @After
    public void tearDown() throws IOException {
        String msg = "";
        for (String name : m_unexpectedSPIs) {
            msg += name + ", ";
        }
        assertTrue(msg, m_unexpectedSPIs.isEmpty());
        for (File p : paths) {
            VoltFile.recursivelyDelete(p);
        }
    }

    @Test
    public void testSingleHostEmptyRestore() throws IOException {
        m_hostCount = 1;
        buildCatalog(m_hostCount, 8, 0, newVoltRoot(null));
        MockInitiator initiator = new MockInitiator(null);
        RestoreAgent restoreAgent = new RestoreAgent(context, initiator, this,
                                                     0, "all");
        restoreAgent.restore();
        while (!m_done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
        }
    }

    @Test
    public void testMultipleHostEmptyRestore() throws IOException {
        m_hostCount = 3;
        buildCatalog(m_hostCount, 8, 0, newVoltRoot(null));
        MockInitiator initiator = new MockInitiator(null);
        List<RestoreAgent> agents = new ArrayList<RestoreAgent>();

        for (int i = 0; i < m_hostCount; i++) {
            agents.add(new RestoreAgent(context, initiator, this, i, "all"));
        }
        for (RestoreAgent agent : agents) {
            agent.restore();
        }

        int count = 0;
        while (!m_done && count++ < 50) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
        }

        if (!m_done) {
            fail("Timed out");
        }
    }

    @Test
    public void testMultipleHostAgreementFailure() throws IOException {
        m_hostCount = 3;
        buildCatalog(m_hostCount, 8, 0, newVoltRoot(null));
        MockInitiator initiator = new MockInitiator(null);
        List<RestoreAgent> agents = new ArrayList<RestoreAgent>();

        for (int i = 0; i < m_hostCount - 1; i++) {
            agents.add(new RestoreAgent(context, initiator, this, i, "all"));
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
        RestoreAgent agent = new RestoreAgent(context, initiator, this,
                                              m_hostCount - 1, "all");
        agent.restore();

        count = 0;
        while (!m_done && count++ < 10) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
        }

        if (!m_done) {
            fail();
        }
    }

    @Test
    public void testSingleHostSnapshotRestore() throws IOException, InterruptedException {
        m_hostCount = 1;
        buildCatalog(m_hostCount, 8, 0, newVoltRoot(null));
        ServerThread server = new ServerThread(context.pathToCatalogJar,
                                               deploymentPath,
                                               BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();
        snapshot();
        server.shutdown();

        HashSet<String> procs = new HashSet<String>();
        procs.add("@SnapshotRestore");
        MockInitiator initiator = new MockInitiator(procs);
        RestoreAgent restoreAgent = new RestoreAgent(context, initiator, this,
                                                     0, "all");
        restoreAgent.restore();
        while (!m_done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
        }

        Long count = initiator.getProcCounts().get("@SnapshotRestore");
        assertEquals(new Long(1), count);
    }

    @Test
    public void testMultiHostSnapshotRestore() throws IOException, InterruptedException {
        m_hostCount = 1;
        String voltroot = newVoltRoot(null);
        buildCatalog(m_hostCount, 8, 0, voltroot);
        ServerThread server = new ServerThread(context.pathToCatalogJar,
                                               deploymentPath,
                                               BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();
        snapshot();
        server.shutdown();

        m_hostCount = 3;
        buildCatalog(m_hostCount, 8, 1, voltroot);
        HashSet<String> procs = new HashSet<String>();
        procs.add("@SnapshotRestore");
        MockInitiator initiator = new MockInitiator(procs);
        List<RestoreAgent> agents = new ArrayList<RestoreAgent>();

        for (int i = 0; i < m_hostCount; i++) {
            agents.add(new RestoreAgent(context, initiator, this, i, "all"));
        }
        for (RestoreAgent agent : agents) {
            agent.restore();
        }

        while (!m_done) {
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {}
        }

        Long count = initiator.getProcCounts().get("@SnapshotRestore");
        assertEquals(new Long(1), count);
    }

    @Override
    public void onRestoreCompletion() {
        if (m_count.incrementAndGet() == m_hostCount) {
            m_done = true;
        }
    }
}
