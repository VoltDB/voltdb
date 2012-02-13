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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.voltdb.ClientInterface.ClientInputHandler;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.AdHocPlannedStmt;
import org.voltdb.compiler.AdHocPlannerWork;
import org.voltdb.compiler.CatalogChangeResult;
import org.voltdb.compiler.CatalogChangeWork;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.dtxn.TransactionInitiator;
import org.voltdb.messaging.FastSerializer;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.network.Connection;
import org.voltcore.network.VoltNetworkPool;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestClientInterface {
    // mocked objects that CI requires
    private static final VoltDBInterface m_volt = mock(VoltDBInterface.class);
    private static final StatsAgent m_statsAgent = mock(StatsAgent.class);
    private static final HostMessenger m_messenger = mock(HostMessenger.class);
    private static final TransactionInitiator m_initiator = mock(TransactionInitiator.class);
    private static final ClientInputHandler m_handler = mock(ClientInputHandler.class);

    // real context
    private static CatalogContext m_context = null;

    // real CI, but spied on using mockito
    private static ClientInterface m_ci = null;
    // the mailbox in CI
    private static Mailbox m_mb = null;

    private static int[] m_allPartitions = new int[] {0, 1, 2};

    @BeforeClass
    public static void setUpOnce() throws Exception {
        buildCatalog();

        /*
         * Setup the mock objects so that they return expected objects in CI
         * construction
         */
        VoltDB.replaceVoltDBInstanceForTest(m_volt);
        doReturn(m_statsAgent).when(m_volt).getStatsAgent();
        doReturn(mock(SnapshotCompletionMonitor.class)).when(m_volt).getSnapshotCompletionMonitor();
        doReturn(m_messenger).when(m_volt).getHostMessenger();
        doReturn(mock(VoltNetworkPool.class)).when(m_messenger).getNetwork();
        doReturn(mock(ZooKeeper.class)).when(m_messenger).getZK();
        doReturn(mock(Configuration.class)).when(m_volt).getConfig();
        doReturn(32L).when(m_messenger).getHSIdForLocalSite(HostMessenger.ASYNC_COMPILER_SITE_ID);

        // Set up CI with the mock objects.
        m_ci = spy(new ClientInterface(VoltDB.DEFAULT_PORT, VoltDB.DEFAULT_ADMIN_PORT,
                                       m_context, m_messenger, ReplicationRole.NONE, m_initiator, m_allPartitions));

        m_mb = m_ci.m_mailbox;
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

        m_context = new CatalogContext(0, catalog, bytes, 0, 0, 0);
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
     * @throws MessagingException
     */
    private StoredProcedureInvocation readAndCheck(ByteBuffer msg, String procName, Object partitionParam,
                                                   boolean isAdmin, boolean isReadonly, boolean isSinglePart,
                                                   boolean isEverySite) throws IOException, MessagingException {
        when(m_initiator.createTransaction(anyLong(), anyString(), anyBoolean(),
                                           any(StoredProcedureInvocation.class),
                                           anyBoolean(), anyBoolean(), anyBoolean(),
                                           any(int[].class), anyInt(), anyObject(),
                                           anyInt(), anyLong())).thenReturn(true);

        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);

        ArgumentCaptor<Boolean> boolCaptor = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<StoredProcedureInvocation> invocationCaptor =
                ArgumentCaptor.forClass(StoredProcedureInvocation.class);
        ArgumentCaptor<int[]> partitionCaptor = ArgumentCaptor.forClass(int[].class);
        verify(m_initiator).createTransaction(anyLong(), anyString(), boolCaptor.capture(),
                                              invocationCaptor.capture(),
                                              boolCaptor.capture(), boolCaptor.capture(),
                                              boolCaptor.capture(), partitionCaptor.capture(),
                                              anyInt(), anyObject(), anyInt(), anyLong());
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
    public void testAdHoc() throws IOException, MessagingException {
        ByteBuffer msg = createMsg("@AdHoc", "select * from a");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);
        ArgumentCaptor<LocalObjectMessage> captor = ArgumentCaptor.forClass(LocalObjectMessage.class);
        verify(m_messenger).send(eq(32L),
                                 captor.capture());
        assertTrue(captor.getValue().payload instanceof AdHocPlannerWork);
        assertTrue(captor.getValue().payload.toString().contains("partition param: null"));

        // single-part adhoc
        reset(m_messenger);
        msg = createMsg("@AdHoc", "select * from a where i = 3", 3);
        resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);
        verify(m_messenger).send(eq(32L),
                                 captor.capture());
        assertTrue(captor.getValue().payload instanceof AdHocPlannerWork);
        assertTrue(captor.getValue().payload.toString().contains("partition param: 3"));
    }

    /**
     * Fake an adhoc compiler result and return it to the CI, see if CI
     * initiates the txn.
     */
    @Test
    public void testFinishedAdHocPlanning() {
        when(m_initiator.createTransaction(anyLong(), anyString(), anyBoolean(),
                                           any(StoredProcedureInvocation.class),
                                           anyBoolean(), anyBoolean(), anyBoolean(),
                                           any(int[].class), anyInt(), anyObject(),
                                           anyInt(), anyLong())).thenReturn(true);

        AdHocPlannedStmt plannedStmt = new AdHocPlannedStmt();
        plannedStmt.catalogVersion = 0;
        plannedStmt.clientHandle = 0;
        plannedStmt.connectionId = 0;
        plannedStmt.hostname = "localhost";
        plannedStmt.adminConnection = false;
        plannedStmt.clientData = null;
        plannedStmt.aggregatorFragment = null;
        plannedStmt.collectorFragment = null;
        plannedStmt.isReplicatedTableDML = false;
        plannedStmt.sql = "select * from a";
        plannedStmt.partitionParam = null;
        m_mb.deliver(new LocalObjectMessage(plannedStmt));
        m_ci.checkForFinishedCompilerWork();

        ArgumentCaptor<Boolean> boolCaptor = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<StoredProcedureInvocation> invocationCaptor =
                ArgumentCaptor.forClass(StoredProcedureInvocation.class);
        verify(m_initiator).createTransaction(anyLong(), anyString(), boolCaptor.capture(),
                                              invocationCaptor.capture(),
                                              boolCaptor.capture(), boolCaptor.capture(),
                                              boolCaptor.capture(), any(int[].class),
                                              anyInt(), anyObject(), anyInt(), anyLong());
        List<Boolean> boolValues = boolCaptor.getAllValues();
        assertFalse(boolValues.get(0)); // is admin
        assertFalse(boolValues.get(1)); // readonly
        assertFalse(boolValues.get(2)); // single-part
        assertFalse(boolValues.get(3)); // every site
        assertEquals("@AdHoc", invocationCaptor.getValue().getProcName());
        assertEquals("select * from a", invocationCaptor.getValue().getParameterAtIndex(2));
    }

    @Test
    public void testUpdateCatalog() throws IOException, MessagingException {
        String catalogHex = Encoder.hexEncode("blah");
        ByteBuffer msg = createMsg("@UpdateApplicationCatalog", catalogHex, "blah");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);
        ArgumentCaptor<LocalObjectMessage> captor = ArgumentCaptor.forClass(LocalObjectMessage.class);
        verify(m_messenger).send(eq(32L), // A fixed number set in setUpOnce()
                                 captor.capture());
        assertTrue(captor.getValue().payload instanceof CatalogChangeWork);
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
                                           anyInt(), anyLong())).thenReturn(true);

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
        m_mb.deliver(new LocalObjectMessage(catalogResult));
        m_ci.checkForFinishedCompilerWork();

        ArgumentCaptor<Boolean> boolCaptor = ArgumentCaptor.forClass(Boolean.class);
        ArgumentCaptor<StoredProcedureInvocation> invocationCaptor =
                ArgumentCaptor.forClass(StoredProcedureInvocation.class);
        verify(m_initiator).createTransaction(anyLong(), anyString(), boolCaptor.capture(),
                                              invocationCaptor.capture(),
                                              boolCaptor.capture(), boolCaptor.capture(),
                                              boolCaptor.capture(), any(int[].class),
                                              anyInt(), anyObject(), anyInt(), anyLong());
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
    public void testUserProc() throws IOException, MessagingException {
        ByteBuffer msg = createMsg("hello", 1);
        StoredProcedureInvocation invocation =
                readAndCheck(msg, "hello", 1, false, true, true, false);
        assertEquals(1, invocation.getParameterAtIndex(0));
    }

    @Test
    public void testSystemInformation() throws IOException, MessagingException {
        ByteBuffer msg = createMsg("@SystemInformation");
        StoredProcedureInvocation invocation =
                readAndCheck(msg, "@SystemInformation", 1, false, true, false, false);
        assertEquals("OVERVIEW", invocation.getParams().m_params[0]);
    }

    /**
     * WAN stats is not a txn, it goes to the stats agent directly.
     * @throws Exception
     */
    @Test
    public void testWANStats() throws Exception {
        ByteBuffer msg = createMsg("@Statistics", "WAN", 0);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNull(resp);
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);
        verify(m_statsAgent).collectStats(any(Connection.class), anyInt(), captor.capture());
        assertEquals("WAN", captor.getValue());
    }

    @Test
    public void testStatisticsProc() throws IOException, MessagingException {
        ByteBuffer msg = createMsg("@Statistics", "table", 0);
        StoredProcedureInvocation invocation =
                readAndCheck(msg, "@Statistics", null, false, true, false, false);
        assertEquals("table", invocation.getParameterAtIndex(0));
    }

    @Test
    public void testLoadSinglePartTable() throws IOException, MessagingException {
        VoltTable table = new VoltTable(new ColumnInfo("i", VoltType.INTEGER));
        table.addRow(1);
        ByteBuffer msg = createMsg("@LoadSinglepartitionTable", "a", table);
        readAndCheck(msg, "@LoadSinglepartitionTable", 1, false, false, true, false);
    }

    @Test
    public void testPausedMode() throws IOException, MessagingException {
        // pause the node
        when(m_volt.getMode()).thenReturn(OperationMode.PAUSED);
        ByteBuffer msg = createMsg("hello", 1);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponseImpl.SERVER_UNAVAILABLE, resp.getStatus());
        when(m_volt.getMode()).thenReturn(OperationMode.RUNNING);
    }

    @Test
    public void testInvalidProcedure() throws IOException {
        ByteBuffer msg = createMsg("hellooooo", 1);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponseImpl.UNEXPECTED_FAILURE, resp.getStatus());
    }

    @Test
    public void testAdminProcsOnNonAdminPort() throws IOException {
        ByteBuffer msg = createMsg("@Pause");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponseImpl.UNEXPECTED_FAILURE, resp.getStatus());

        msg = createMsg("@Resume");
        resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponseImpl.UNEXPECTED_FAILURE, resp.getStatus());
    }

    @Test
    public void testRejectDupInvocation() throws IOException {
        // by default, the mock initiator returns false for createTransaction()
        ByteBuffer msg = createMsg(12345l, "hello", 1);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponseImpl.UNEXPECTED_FAILURE, resp.getStatus());
    }

    @Test
    public void testPolicyRejection() throws IOException {
        // incorrect parameters to @AdHoc proc
        ByteBuffer msg = createMsg("@AdHoc", 1, 3, 3);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, null);
        assertNotNull(resp);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
    }
}
