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
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.network.Connection;
import org.voltcore.network.VoltNetworkPool;
import org.voltdb.ClientInterface.ClientInputHandler;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureInvocationType;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.AdHocPlannerWork;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.CatalogChangeWork;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.dtxn.MailboxPublisher;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.iv2.Cartographer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.planner.CorePlan;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

public class TestClientInterface {
    // mocked objects that CI requires
    private VoltDBInterface m_volt;
    private StatsAgent m_statsAgent;
    private HostMessenger m_messenger;
    private TransactionInitiator m_initiator;
    private ClientInputHandler m_handler;
    private Cartographer m_cartographer;
    private ZooKeeper m_zk;

    // real context
    private static CatalogContext m_context = null;

    // real CI, but spied on using mockito
    private static ClientInterface m_ci = null;
    // the mailbox in CI
    //private static Mailbox m_mb = null;

    private static int[] m_allPartitions = new int[] {0, 1, 2};

    @BeforeClass
    public static void setUpOnce() throws Exception {
        buildCatalog();

    }

    @Before
    public void setUp() throws Exception {
        // Set up CI with the mock objects.
        m_volt = mock(VoltDBInterface.class);
        m_statsAgent = mock(StatsAgent.class);
        m_messenger = mock(HostMessenger.class);
        m_initiator = mock(TransactionInitiator.class);
        m_handler = mock(ClientInputHandler.class);
        m_cartographer = mock(Cartographer.class);
        m_zk = mock(ZooKeeper.class);

        /*
         * Setup the mock objects so that they return expected objects in CI
         * construction
         */
        VoltDB.replaceVoltDBInstanceForTest(m_volt);
        doReturn(m_statsAgent).when(m_volt).getStatsAgent();
        doReturn(mock(SnapshotCompletionMonitor.class)).when(m_volt).getSnapshotCompletionMonitor();
        doReturn(m_messenger).when(m_volt).getHostMessenger();
        doReturn(mock(VoltNetworkPool.class)).when(m_messenger).getNetwork();
        doReturn(m_zk).when(m_messenger).getZK();
        doReturn(mock(Configuration.class)).when(m_volt).getConfig();
        doReturn(32L).when(m_messenger).getHSIdForLocalSite(HostMessenger.ASYNC_COMPILER_SITE_ID);
        doReturn(mock(MailboxPublisher.class)).when(m_volt).getMailboxPublisher();
        m_ci = spy(new ClientInterface(VoltDB.DEFAULT_PORT, VoltDB.DEFAULT_ADMIN_PORT,
                                       m_context, m_messenger, ReplicationRole.NONE, m_initiator,
                                       m_cartographer, m_allPartitions));

        //m_mb = m_ci.m_mailbox;
    }

    private static void buildCatalog() throws IOException {
        // build a real catalog
        File cat = File.createTempFile("temp-log-reinitiator", "catalog");
        cat.deleteOnExit();

        VoltProjectBuilder builder = new VoltProjectBuilder();
        String schema = "create table A (i integer not null, primary key (i));";
        builder.addLiteralSchema(schema);
        builder.addPartitionInfo("A", "i");
        builder.addStmtProcedure("hello", "select * from A where i = ?", "A.i: 0");

        if (!builder.compile(cat.getAbsolutePath())) {
            throw new IOException();
        }

        byte[] bytes = CatalogUtil.toBytes(cat);
        String serializedCat = CatalogUtil.loadCatalogFromJar(bytes, null);
        assertNotNull(serializedCat);
        Catalog catalog = new Catalog();
        catalog.execute(serializedCat);

        String deploymentPath = builder.getPathToDeployment();
        CatalogUtil.compileDeploymentAndGetCRC(catalog, deploymentPath, true);

        m_context = new CatalogContext(0, 0, catalog, bytes, 0, 0, 0);
        TheHashinator.initialize(3);
    }

    @After
    public void tearDown() {
        reset(m_messenger);
        reset(m_initiator);
        reset(m_handler);
    }

    private static ByteBuffer createMsg(String name, final Object...params) throws IOException {
        return createMsg(null, name, params);
    }

    /**
     * Create a VoltMessage that can be fed into CI's handleRead() method.
     * @param origTxnId The original txnId if it's a replicated transaction
     * @param name The procedure name
     * @param params Procedure parameters
     * @return
     * @throws IOException
     */
    private static ByteBuffer createMsg(Long origTxnId, String name,
                                        final Object...params) throws IOException {
        FastSerializer fs = new FastSerializer();
        StoredProcedureInvocation proc = new StoredProcedureInvocation();
        proc.setProcName(name);
        if (origTxnId != null)
            proc.setOriginalTxnId(origTxnId);
        proc.setParams(params);
        fs.writeObject(proc);
        return fs.getBuffer();
    }

    /**
     * Pass the VoltMessage to CI's handleRead() and inspect if the expected
     * parameters are passed to the initiator's createTranction() method. This
     * is a convenient method if the caller expects the result of handling this
     * message is to create a new transaction.
     *
     * @param msg
     * @param procName
     * @param partitionParam null if it's a multi-part txn
     * @param isAdmin
     * @param isReadonly
     * @param isSinglePart
     * @param isEverySite
     * @return StoredProcedureInvocation object passed to createTransaction()
     * @throws IOException
     */
    private StoredProcedureInvocation readAndCheck(ByteBuffer msg, String procName, Object partitionParam,
                                                   boolean isAdmin, boolean isReadonly, boolean isSinglePart,
                                                   boolean isEverySite) throws IOException {
        when(m_initiator.createTransaction(anyLong(), anyString(), anyBoolean(),
                                           any(StoredProcedureInvocation.class),
                                           anyBoolean(), anyBoolean(), anyBoolean(),
                                           any(int[].class), anyInt(), anyObject(),
                                           anyInt(), anyLong(), anyBoolean())).thenReturn(true);

        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);

        ArgumentCaptor<Boolean> boolCaptor = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<StoredProcedureInvocation> invocationCaptor =
                ArgumentCaptor.forClass(StoredProcedureInvocation.class);
        ArgumentCaptor<int[]> partitionCaptor = ArgumentCaptor.forClass(int[].class);
        verify(m_initiator).createTransaction(anyLong(), anyString(), boolCaptor.capture(),
                                              invocationCaptor.capture(),
                                              boolCaptor.capture(), boolCaptor.capture(),
                                              boolCaptor.capture(),
                                              partitionCaptor.capture(),
                                              anyInt(), anyObject(), anyInt(), anyLong(),
                                              boolCaptor.capture());
        List<Boolean> boolValues = boolCaptor.getAllValues();
        assertEquals(isAdmin, boolValues.get(0)); // is admin
        assertEquals(isReadonly, boolValues.get(1)); // readonly
        assertEquals(isSinglePart, boolValues.get(2)); // single-part
        assertEquals(isEverySite, boolValues.get(3)); // every site
        assertEquals(procName, invocationCaptor.getValue().getProcName());
        if (isSinglePart) {
            int expected = TheHashinator.hashToPartition(partitionParam);
            assertEquals(1, partitionCaptor.getValue().length);
            assertEquals(expected, partitionCaptor.getValue()[0]);
        } else {
            assertEquals(m_allPartitions, partitionCaptor.getValue());
        }
        return invocationCaptor.getValue();
    }

    @Test
    public void testExplain() throws IOException {
        ByteBuffer msg = createMsg("@Explain", "select * from a");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);
        ArgumentCaptor<LocalObjectMessage> captor = ArgumentCaptor.forClass(LocalObjectMessage.class);
        verify(m_messenger).send(eq(32L), captor.capture());
        assertTrue(captor.getValue().payload instanceof AdHocPlannerWork );
        System.out.println( captor.getValue().payload.toString() );
        assertTrue(captor.getValue().payload.toString().contains("partition param: null"));
    }

    @Test
    public void testAdHoc() throws IOException {
        ByteBuffer msg = createMsg("@AdHoc", "select * from a");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);
        ArgumentCaptor<LocalObjectMessage> captor = ArgumentCaptor.forClass(LocalObjectMessage.class);
        verify(m_messenger).send(eq(32L), captor.capture());
        assertTrue(captor.getValue().payload instanceof AdHocPlannerWork);
        assertTrue(captor.getValue().payload.toString().contains("partition param: null"));

        // single-part adhoc
        reset(m_messenger);
        msg = createMsg("@AdHoc", "select * from a where i = 3", 3);
        resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);
        verify(m_messenger).send(eq(32L), captor.capture());
        assertTrue(captor.getValue().payload instanceof AdHocPlannerWork);
        assertTrue(captor.getValue().payload.toString().contains("partition param: 3"));
    }

    /**
     * Fake an adhoc compiler result and return it to the CI, see if CI
     * initiates the txn.
     */
    @Test
    public void testFinishedAdHocPlanning() throws Exception {
        when(m_initiator.createTransaction(anyLong(), anyString(), anyBoolean(),
                                           any(StoredProcedureInvocation.class),
                                           anyBoolean(), anyBoolean(), anyBoolean(),
                                           any(int[].class), anyInt(), anyObject(),
                                           anyInt(), anyLong(), anyBoolean())).thenReturn(true);

        // Need a batch and a statement
        AdHocPlannedStmtBatch plannedStmtBatch = new AdHocPlannedStmtBatch(
                "select * from a", null, 0, 0, "localhost", false, ProcedureInvocationType.ORIGINAL, 0, 0, null);
        AdHocPlannedStatement s = new AdHocPlannedStatement("select * from a".getBytes(VoltDB.UTF8ENCODING),
                                                            new CorePlan(new byte[0],
                                                                         new byte[0],
                                                                         false,
                                                                         false,
                                                                         true,
                                                                         new VoltType[0],
                                                                         0),
                                                            new ParameterSet(),
                                                            null,
                                                            null,
                                                            null);
        plannedStmtBatch.addStatement(s);
        m_ci.processFinishedCompilerWork(plannedStmtBatch).run();

        ArgumentCaptor<Boolean> boolCaptor = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<StoredProcedureInvocation> invocationCaptor =
                ArgumentCaptor.forClass(StoredProcedureInvocation.class);
        verify(m_initiator).createTransaction(anyLong(), anyString(), boolCaptor.capture(),
                                              invocationCaptor.capture(),
                                              boolCaptor.capture(), boolCaptor.capture(),
                                              boolCaptor.capture(),
                                              any(int[].class), anyInt(),
                                              anyObject(), anyInt(), anyLong(),
                                              boolCaptor.capture());
        List<Boolean> boolValues = boolCaptor.getAllValues();
        assertFalse(boolValues.get(0)); // is admin
        assertTrue(boolValues.get(1));  // readonly
        assertFalse(boolValues.get(2)); // single-part
        assertFalse(boolValues.get(3)); // every site
        assertEquals("@AdHoc_RO_MP", invocationCaptor.getValue().getProcName());

        byte[] serializedData = (byte[]) invocationCaptor.getValue().getParameterAtIndex(0);
        AdHocPlannedStatement[] statements = AdHocPlannedStmtBatch.planArrayFromBuffer(ByteBuffer.wrap(serializedData));
        assertEquals(1, statements.length);
        String sql = new String(statements[0].sql, VoltDB.UTF8ENCODING);
        assertEquals("select * from a", sql);
    }

    @Test
    public void testUpdateCatalog() throws IOException {
        String catalogHex = Encoder.hexEncode("blah");
        ByteBuffer msg = createMsg("@UpdateApplicationCatalog", catalogHex, "blah");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);
        ArgumentCaptor<LocalObjectMessage> captor = ArgumentCaptor.forClass(LocalObjectMessage.class);
        verify(m_messenger).send(eq(32L), // A fixed number set in setUpOnce()
                                 captor.capture());
        assertTrue(captor.getValue().payload instanceof CatalogChangeWork);
    }

    @Test
    public void testNegativeUpdateCatalog() throws IOException {
        ByteBuffer msg = createMsg("@UpdateApplicationCatalog", new Integer(1), new Long(0));
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        // expect an error response from handleRead.
        assertNotNull(resp);
        assertTrue(resp.getStatus() != 0);
    }


    /**
     * Fake a catalog diff compiler result and send it back to the CI, see if CI
     * initiates a new txn.
     */
    @Test
    public void testFinishedCatalogDiffing() {
        when(m_initiator.createTransaction(anyLong(), anyString(), anyBoolean(),
                                           any(StoredProcedureInvocation.class),
                                           anyBoolean(), anyBoolean(), anyBoolean(),
                                           any(int[].class), anyInt(), anyObject(),
                                           anyInt(), anyLong(), anyBoolean())).thenReturn(true);

        CatalogChangeResult catalogResult = new CatalogChangeResult();
        catalogResult.clientData = null;
        catalogResult.clientHandle = 0;
        catalogResult.connectionId = 0;
        catalogResult.adminConnection = false;
        catalogResult.hostname = "localhost";
        // catalog change specific boiler plate
        catalogResult.catalogBytes = "blah".getBytes();
        catalogResult.deploymentString = "blah";
        catalogResult.deploymentCRC = 1234l;
        catalogResult.expectedCatalogVersion = 3;
        catalogResult.encodedDiffCommands = "diff";
        m_ci.processFinishedCompilerWork(catalogResult).run();

        ArgumentCaptor<Boolean> boolCaptor = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<StoredProcedureInvocation> invocationCaptor =
                ArgumentCaptor.forClass(StoredProcedureInvocation.class);
        verify(m_initiator).createTransaction(anyLong(), anyString(), boolCaptor.capture(),
                                              invocationCaptor.capture(),
                                              boolCaptor.capture(), boolCaptor.capture(),
                                              boolCaptor.capture(),
                                              any(int[].class),
                                              anyInt(), anyObject(), anyInt(), anyLong(),
                                              boolCaptor.capture());
        List<Boolean> boolValues = boolCaptor.getAllValues();
        assertFalse(boolValues.get(0)); // is admin
        assertFalse(boolValues.get(1)); // readonly
        assertTrue(boolValues.get(2)); // single-part
        assertTrue(boolValues.get(3)); // every site
        assertEquals("@UpdateApplicationCatalog", invocationCaptor.getValue().getProcName());
        assertEquals("diff", invocationCaptor.getValue().getParameterAtIndex(0));
        assertTrue(Arrays.equals("blah".getBytes(), (byte[]) invocationCaptor.getValue().getParameterAtIndex(1)));
        assertEquals(3, invocationCaptor.getValue().getParameterAtIndex(2));
        assertEquals("blah", invocationCaptor.getValue().getParameterAtIndex(3));
        assertEquals(1234l, invocationCaptor.getValue().getParameterAtIndex(4));
    }

    @Test
    public void testUserProc() throws IOException {
        ByteBuffer msg = createMsg("hello", 1);
        StoredProcedureInvocation invocation =
                readAndCheck(msg, "hello", 1, false, true, true, false);
        assertEquals(1, invocation.getParameterAtIndex(0));
    }

    @Test
    public void testSystemInformation() throws IOException {
        ByteBuffer msg = createMsg("@SystemInformation");
        StoredProcedureInvocation invocation =
                readAndCheck(msg, "@SystemInformation", 1, false, true, false, false);
        assertEquals("OVERVIEW", invocation.getParams().toArray()[0]);
    }

    /**
     * DR stats is not a txn, it goes to the stats agent directly.
     * @throws Exception
     */
    @Test
    public void testDRStats() throws Exception {
        ByteBuffer msg = createMsg("@Statistics", "DR", 0);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(m_statsAgent).collectStats(any(Connection.class), anyInt(), captor.capture());
        assertEquals("DR", captor.getValue());
    }

    @Test
    public void testStatisticsProc() throws IOException {
        ByteBuffer msg = createMsg("@Statistics", "table", 0);
        StoredProcedureInvocation invocation =
                readAndCheck(msg, "@Statistics", null, false, true, false, false);
        assertEquals("table", invocation.getParameterAtIndex(0));
    }

    @Test
    public void testLoadSinglePartTable() throws IOException {
        VoltTable table = new VoltTable(new ColumnInfo("i", VoltType.INTEGER));
        table.addRow(1);
        ByteBuffer msg = createMsg("@LoadSinglepartitionTable", "a", table);
        readAndCheck(msg, "@LoadSinglepartitionTable", 1, false, false, true, false);
    }

    @Test
    public void testPausedMode() throws IOException {
        // pause the node
        when(m_volt.getMode()).thenReturn(OperationMode.PAUSED);
        ByteBuffer msg = createMsg("hello", 1);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponse.SERVER_UNAVAILABLE, resp.getStatus());
        when(m_volt.getMode()).thenReturn(OperationMode.RUNNING);
    }

    @Test
    public void testInvalidProcedure() throws IOException {
        ByteBuffer msg = createMsg("hellooooo", 1);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, resp.getStatus());
    }

    @Test
    public void testAdminProcsOnNonAdminPort() throws IOException {
        ByteBuffer msg = createMsg("@Pause");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, resp.getStatus());

        msg = createMsg("@Resume");
        resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, resp.getStatus());
    }

    @Test
    public void testRejectDupInvocation() throws IOException {
        // by default, the mock initiator returns false for createTransaction()
        ByteBuffer msg = createMsg(12345l, "hello", 1);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, resp.getStatus());
    }

    @Test
    public void testPolicyRejection() throws IOException {
        // incorrect parameters to @AdHoc proc
        ByteBuffer msg = createMsg("@AdHoc", 1, 3, 3);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponse.GRACEFUL_FAILURE, resp.getStatus());
    }

    @Test
    public void testPromoteWithoutCommandLogging() throws Exception {
        final ByteBuffer msg = createMsg("@Promote");
        m_ci.handleRead(msg, m_handler, null);
        // Verify that the truncation request node was not created.
        verify(m_zk, never()).create(eq(VoltZK.request_truncation_snapshot), any(byte[].class),
                                     eq(Ids.OPEN_ACL_UNSAFE), eq(CreateMode.PERSISTENT));
    }

    @Test
    public void testPromoteWithCommandLogging() throws Exception {
        org.voltdb.catalog.CommandLog logConfig = m_context.cluster.getLogconfig().get("log");
        boolean wasEnabled = logConfig.getEnabled();
        logConfig.setEnabled(true);
        try {
            final ByteBuffer msg = createMsg("@Promote");
            m_ci.handleRead(msg, m_handler, null);
            // Verify that the truncation request node was created.
            verify(m_zk).create(eq(VoltZK.request_truncation_snapshot), any(byte[].class),
                                eq(Ids.OPEN_ACL_UNSAFE), eq(CreateMode.PERSISTENT));
        }
        finally {
            logConfig.setEnabled(wasEnabled);
        }
    }
}
