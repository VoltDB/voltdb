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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
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
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.apache.zookeeper_voltpatches.ZooDefs.Ids;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.network.Connection;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;
import org.voltdb.ClientInterface.ClientInputHandler;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.Catalog;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.iv2.Cartographer;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.settings.DbSettings;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestClientInterface {
    // mocked objects that CI requires
    private VoltDBInterface m_volt;
    private Queue<DeferredSerialization> statsAnswers = new ArrayDeque<>();
    private int drStatsInvoked = 0;
    private StatsAgent m_statsAgent = new StatsAgent() {
        @Override
        public void performOpsAction(final Connection c, final long clientHandle, final OpsSelector selector,
                                     final ParameterSet params) {
            final String stat = (String)params.toArray()[0];
            if (stat.equals("TOPO") && !statsAnswers.isEmpty()) {
                c.writeStream().enqueue(statsAnswers.poll());
            } else if (stat.equals("DR")) {
                drStatsInvoked++;
            }
        }
    };
    private SystemInformationAgent m_sysinfoAgent;
    private HostMessenger m_messenger;
    private ClientInputHandler m_handler;
    private Cartographer m_cartographer;
    private SimpleClientResponseAdapter m_cxn;
    private ZooKeeper m_zk;
    private ScheduledThreadPoolExecutor m_periodicWorkThread;


    // real context
    private static CatalogContext m_context = null;

    // real CI, but spied on using mockito
    private static ClientInterface m_ci = null;
    // the mailbox in CI
    //private static Mailbox m_mb = null;

    @BeforeClass
    public static void setUpOnce() throws Exception {
        buildCatalog();
    }

    BlockingQueue<ByteBuffer> responses = new LinkedTransferQueue<>();
    BlockingQueue<DeferredSerialization> responsesDS = new LinkedTransferQueue<>();
    private static final ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);

    @Before
    public void setUp() throws Exception {
        m_periodicWorkThread = CoreUtils.getScheduledThreadPoolExecutor("Periodic Work", 1, CoreUtils.SMALL_STACK_SIZE);
        // Set up CI with the mock objects.
        m_volt = mock(VoltDBInterface.class);
        m_sysinfoAgent = mock(SystemInformationAgent.class);
        m_messenger = mock(HostMessenger.class);
        m_handler = mock(ClientInputHandler.class);
        m_cartographer = mock(Cartographer.class);

        m_zk = mock(ZooKeeper.class);
        responses = new LinkedTransferQueue<>();
        responsesDS = new LinkedTransferQueue<>();
        //m_cxn = mock(SimpleClientResponseAdapter.class);
        drStatsInvoked = 0;
        m_cxn = new SimpleClientResponseAdapter(0, "foo") {
            @Override
            public void enqueue(ByteBuffer buf) {responses.offer(buf);}
            @Override
            public void enqueue(ByteBuffer bufs[]) {responses.offer(bufs[0]);}
            @Override
            public void enqueue(DeferredSerialization ds) {responsesDS.offer(ds);}
            @Override
            public void queueTask(Runnable r) {}
        };


        /*
         * Setup the mock objects so that they return expected objects in CI
         * construction
         */
        VoltDB.replaceVoltDBInstanceForTest(m_volt);

        when(m_handler.connectionId()).thenReturn(0L);
        when(m_handler.isAdmin()).thenReturn(false);
        when(m_volt.getSES(anyBoolean())).thenReturn(m_periodicWorkThread);
        when(m_volt.getCommandLogSnapshotPath()).thenReturn("/tmp");
        when(m_volt.getSnapshotPath()).thenReturn("/tmp");

        doReturn(m_statsAgent).when(m_volt).getStatsAgent();
        doReturn(m_statsAgent).when(m_volt).getOpsAgent(OpsSelector.STATISTICS);
        doReturn(m_sysinfoAgent).when(m_volt).getOpsAgent(OpsSelector.SYSTEMINFORMATION);
        doReturn(mock(SnapshotCompletionMonitor.class)).when(m_volt).getSnapshotCompletionMonitor();
        doReturn(m_messenger).when(m_volt).getHostMessenger();
        doReturn(mock(VoltNetworkPool.class)).when(m_messenger).getNetwork();
        doReturn(m_zk).when(m_messenger).getZK();
        doReturn(mock(Configuration.class)).when(m_volt).getConfig();
        doReturn(32L).when(m_messenger).getHSIdForLocalSite(HostMessenger.ASYNC_COMPILER_SITE_ID);
        doReturn(ReplicationRole.NONE).when(m_volt).getReplicationRole();
        doReturn(m_context).when(m_volt).getCatalogContext();
        doAnswer(new Answer<Object>() {
            @Override
            public Object answer(InvocationOnMock invocation)
            {
                Object args[] = invocation.getArguments();
                return ses.scheduleAtFixedRate((Runnable) args[0], (long) args[1], (long) args[2], (TimeUnit) args[3]);
            }
        }).when(m_volt).scheduleWork(any(Runnable.class), anyLong(), anyLong(), any());

        m_ci = spy(new ClientInterface(null, VoltDB.DEFAULT_PORT, null, VoltDB.DEFAULT_ADMIN_PORT,
                m_context, m_messenger, ReplicationRole.NONE,
                m_cartographer));
        m_ci.bindAdapter(m_cxn, null);

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
        ProcedurePartitionData data = new ProcedurePartitionData("A", "i");
        builder.addStmtProcedure("hello", "select * from A where i = ?", data);
        builder.addStmtProcedure("hellorw", "delete from A where i = ?", data);

        if (!builder.compile(cat.getAbsolutePath())) {
            throw new IOException();
        }

        byte[] bytes = MiscUtils.fileToBytes(cat);
        String serializedCat =
            CatalogUtil.getSerializedCatalogStringFromJar(CatalogUtil.loadAndUpgradeCatalogFromJar(bytes, false).getFirst());
        assertNotNull(serializedCat);
        Catalog catalog = new Catalog();
        catalog.execute(serializedCat);

        String deploymentPath = builder.getPathToDeployment();
        CatalogUtil.compileDeployment(catalog, deploymentPath, false);
        DbSettings dbSettings = CatalogUtil.asDbSettings(deploymentPath);
        m_context = new CatalogContext(catalog, dbSettings, 0, 0, bytes, null, new byte[] {}, mock(HostMessenger.class));
        TheHashinator.initialize(TheHashinator.getConfiguredHashinatorClass(), TheHashinator.getConfigureBytes(3));
    }

    @After
    public void tearDown() throws Exception {
        reset(m_messenger);
        reset(m_handler);
        m_periodicWorkThread.shutdown();
        m_periodicWorkThread.awaitTermination(356, TimeUnit.DAYS);

    }

    /**
     * Create a VoltMessage that can be fed into CI's handleRead() method.
     * @param origTxnId The original txnId if it's a replicated transaction
     * @param name The procedure name
     * @param params Procedure parameters
     * @return
     * @throws IOException
     */
    private static ByteBuffer createMsg(String name, final Object...params) throws IOException
    {
        StoredProcedureInvocation proc = new StoredProcedureInvocation();
        proc.setProcName(name);
        proc.setParams(params);
        ByteBuffer buf = ByteBuffer.allocate(proc.getSerializedSize());
        proc.flattenToBuffer(buf);
        buf.flip();
        return buf;
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
    private Iv2InitiateTaskMessage readAndCheck(ByteBuffer msg, String procName, Object partitionParam,
                                                boolean isReadonly, boolean isSinglePart) throws Exception {
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNull(resp);

        return checkInitMsgSent(procName, partitionParam, isReadonly, isSinglePart);
    }

    private Iv2InitiateTaskMessage checkInitMsgSent(String procName, Object partitionParam,
                                                    boolean isReadonly, boolean isSinglePart)
    {

        ArgumentCaptor<Long> destinationCaptor =
            ArgumentCaptor.forClass(Long.class);
        ArgumentCaptor<Iv2InitiateTaskMessage> messageCaptor =
            ArgumentCaptor.forClass(Iv2InitiateTaskMessage.class);
        verify(m_messenger).send(destinationCaptor.capture(), messageCaptor.capture());

        Iv2InitiateTaskMessage message = messageCaptor.getValue();
        assertEquals(isReadonly, message.isReadOnly()); // readonly
        assertEquals(isSinglePart, message.isSinglePartition()); // single-part
        assertEquals(procName, message.getStoredProcedureName());
        if (isSinglePart) {
            int expected = TheHashinator.getPartitionForParameter(VoltType.typeFromObject(partitionParam).getValue(),
                                                                  partitionParam);
            assertEquals(new Long(m_cartographer.getHSIdForMaster(expected)), destinationCaptor.getValue());
        } else {
            assertEquals(new Long(m_cartographer.getHSIdForMultiPartitionInitiator()), destinationCaptor.getValue());
        }
        return message;
    }

    @Test
    public void testUserProc() throws Exception {
        ByteBuffer msg = createMsg("hello", 1);
        StoredProcedureInvocation invocation =
                readAndCheck(msg, "hello", 1, true, true).getStoredProcedureInvocation();
        assertEquals(1, invocation.getParameterAtIndex(0));
    }

    @Test
    public void testGC() throws Exception {
        ByteBuffer msg = createMsg("@GC");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNull(resp);

        DeferredSerialization ds = responsesDS.take();
        ByteBuffer b = ByteBuffer.allocate(ds.getSerializedSize());
        ds.serialize(b);

        resp = new ClientResponseImpl();
        b.position(4);
        resp.initFromBuffer(b);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable vt = resp.getResults()[0];
        assertTrue(vt.advanceRow());
        //System.gc() should take at least a little time
        assertTrue(resp.getResults()[0].getLong(0) > 10000);
    }

    @Test
    public void testSystemInformation() throws Exception {
        ByteBuffer msg = createMsg("@SystemInformation");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNull(resp);
        verify(m_sysinfoAgent).performOpsAction(any(Connection.class), anyLong(), eq(OpsSelector.SYSTEMINFORMATION),
                any(ParameterSet.class));
    }

    /**
     * DR stats is not a txn, it goes to the stats agent directly.
     * @throws Exception
     */
    @Test
    public void testDRStats() throws Exception {
        ByteBuffer msg = createMsg("@Statistics", "DR", 0);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNull(resp);
        assertEquals(drStatsInvoked, 1);
    }

    @Test
    public void testLoadSinglePartTableInsert() throws Exception {
        VoltTable table = new VoltTable(new ColumnInfo("i", VoltType.INTEGER));
        table.addRow(1);

        byte[] partitionParam = {0, 0, 0, 0, 0, 0, 0, 4};
        ByteBuffer msg = createMsg("@LoadSinglepartitionTable", partitionParam, "a", (byte) 0, table);
        StoredProcedureInvocation invocation =
                readAndCheck(msg, "@LoadSinglepartitionTable", partitionParam, false, true).getStoredProcedureInvocation();
        assertEquals((byte) 0, invocation.getParameterAtIndex(2));
    }

    @Test
    public void testLoadSinglePartTableUpsert() throws Exception {
        VoltTable table = new VoltTable(new ColumnInfo("i", VoltType.INTEGER));
        table.addRow(1);

        byte[] partitionParam = {0, 0, 0, 0, 0, 0, 0, 4};
        ByteBuffer msg = createMsg("@LoadSinglepartitionTable", partitionParam, "a", (byte) 1, table);
        StoredProcedureInvocation invocation =
                readAndCheck(msg, "@LoadSinglepartitionTable", partitionParam, false, true).getStoredProcedureInvocation();
        assertEquals((byte) 1, invocation.getParameterAtIndex(2));
    }

    @Test
    public void testPausedMode() throws IOException {
        runPausedMode(false);
    }

    @Test
    public void testPausedModeAdmin() throws IOException {
        when(m_handler.isAdmin()).thenReturn(true);
        runPausedMode(true);
        when(m_handler.isAdmin()).thenReturn(false);
    }

    private void runPausedMode(boolean isAdmin) throws IOException {
        // pause the node
        when(m_volt.getMode()).thenReturn(OperationMode.PAUSED);

        // reads are allowed
        ByteBuffer msg = createMsg("hello", 1);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNull(resp);

        // writes are not allowed
        msg = createMsg("hellorw", "10");
        resp = m_ci.handleRead(msg, m_handler, m_cxn);
        if (isAdmin) {
            assertNull(resp);
        } else {
            assertNotNull(resp);
            assertEquals(ClientResponse.SERVER_UNAVAILABLE, resp.getStatus());
            assert(resp.getStatusString().startsWith("Server is paused"));
        }

        when(m_volt.getMode()).thenReturn(OperationMode.RUNNING);
    }

    @Test
    public void testInvalidProcedure() throws IOException {
        ByteBuffer msg = createMsg("hellooooo", 1);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, resp.getStatus());
    }

    @Test
    public void testAdminProcsOnNonAdminPort() throws IOException {
        ByteBuffer msg = createMsg("@Pause");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, resp.getStatus());

        msg = createMsg("@Resume");
        resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.UNEXPECTED_FAILURE, resp.getStatus());
    }

    @Test
    public void testPolicyRejection() throws IOException {
        // incorrect parameters to @AdHoc proc
        ByteBuffer msg = createMsg("@AdHoc", 1, 3, 3);
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.GRACEFUL_FAILURE, resp.getStatus());
    }

    @Test
    public void testPromoteWithoutCommandLogging() throws Exception {
        final ByteBuffer msg = createMsg("@Promote");
        m_ci.handleRead(msg, m_handler, m_cxn);
        // Verify that the truncation request node was not created.
        verify(m_zk, never()).create(eq(VoltZK.request_truncation_snapshot_node), any(byte[].class),
                                     eq(Ids.OPEN_ACL_UNSAFE), eq(CreateMode.PERSISTENT));
    }

    @Test
    public void testPromoteWithCommandLogging() throws Exception {
        org.voltdb.catalog.CommandLog logConfig = m_context.cluster.getLogconfig().get("log");
        boolean wasEnabled = logConfig.getEnabled();
        logConfig.setEnabled(true);
        try {
            final ByteBuffer msg = createMsg("@Promote");
            m_ci.handleRead(msg, m_handler, m_cxn);
            // Verify that the truncation request node was created.
            verify(m_zk, never()).create(eq(VoltZK.request_truncation_snapshot_node), any(byte[].class),
                                eq(Ids.OPEN_ACL_UNSAFE), eq(CreateMode.PERSISTENT_SEQUENTIAL));
        }
        finally {
            logConfig.setEnabled(wasEnabled);
        }
    }

    @Test
    public void testTransactionRestart() throws Exception {
        initMsgAndSendRestartResp(true);
    }

    @Test
    public void testTransactionRestartIgnored() throws Exception {
        // fake operation mode as command log recovery so that it won't restart the txn
        doReturn(OperationMode.INITIALIZING).when(m_volt).getMode();
        initMsgAndSendRestartResp(false);


    }

    private void initMsgAndSendRestartResp(boolean shouldRestart) throws Exception
    {
        // restart will update the hashinator config, initialize it now
        TheHashinator.constructHashinator(TheHashinator.getConfiguredHashinatorClass(),
                                          TheHashinator.getConfigureBytes(3),
                                          false);
        Pair<Long, byte[]> hashinatorConfig = TheHashinator.getCurrentVersionedConfig();
        long newHashinatorVersion = hashinatorConfig.getFirst() + 1;

        ByteBuffer msg = createMsg("hello", 1);
        Iv2InitiateTaskMessage initMsg = readAndCheck(msg, "hello", 1, true, true);
        assertEquals(1, initMsg.getStoredProcedureInvocation().getParameterAtIndex(0));

        // fake a restart response
        InitiateResponseMessage respMsg = new InitiateResponseMessage(initMsg);
        respMsg.setMispartitioned(true, initMsg.getStoredProcedureInvocation(),
                                  Pair.of(newHashinatorVersion, hashinatorConfig.getSecond()));

        // reset the message so that we can check for restart later
        reset(m_messenger);

        // Deliver a restart response
        m_ci.m_mailbox.deliver(respMsg);

        // Make sure that the txn is NOT restarted
        DeferredSerialization resp = responsesDS.take();

        if (shouldRestart) {
            assertEquals(-1, resp.getSerializedSize());
            checkInitMsgSent("hello", 1, true, true);
        } else {
            assertTrue(-1 != resp.getSerializedSize());
            verify(m_messenger, never()).send(anyLong(), any(VoltMessage.class));
        }

        // the hashinator should've been updated in either case
        assertEquals(newHashinatorVersion, TheHashinator.getCurrentVersionedConfig().getFirst().longValue());
    }

    @Test
    public void testGetPartitionKeys() throws IOException {
        //Unsupported type
        ByteBuffer msg = createMsg("@GetPartitionKeys", "BIGINT");
        ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.GRACEFUL_FAILURE, resp.getStatus());

        //Null param
        msg = createMsg("@GetPartitionKeys", new Object[] { null });
        resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.GRACEFUL_FAILURE, resp.getStatus());

        //Empty string param
        msg = createMsg("@GetPartitionKeys", "");
        resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.GRACEFUL_FAILURE, resp.getStatus());

        //Junk string
        msg = createMsg("@GetPartitionKeys", "ryanlikestheyankees");
        resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.GRACEFUL_FAILURE, resp.getStatus());

        //No param
        msg = createMsg("@GetPartitionKeys");
        resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.GRACEFUL_FAILURE, resp.getStatus());

        //Extra param
        msg = createMsg("@GetPartitionKeys", "INTEGER", 99);
        resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.GRACEFUL_FAILURE, resp.getStatus());

        //Correct param with no case sensitivity
        msg = createMsg("@GetPartitionKeys", "InTeGeR");
        resp = m_ci.handleRead(msg, m_handler, m_cxn);
        assertNotNull(resp);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable vt = resp.getResults()[0];
        assertEquals(3, vt.getRowCount());
        assertEquals(VoltType.INTEGER, vt.getColumnType(1));

        Set<Integer> partitions = new HashSet<>(Arrays.asList( 0, 1, 2));
        while (vt.advanceRow()) {
            int partition = TheHashinator.getPartitionForParameter(VoltType.INTEGER.getValue(), vt.getLong(1));
            assertTrue(partitions.remove(partition));
        }
        assertTrue(partitions.isEmpty());
    }

    @Test
    public void testSubscribe() throws Exception {
        RateLimitedClientNotifier.WARMUP_MS = 0;
        ClientInterface.TOPOLOGY_CHANGE_CHECK_MS = 1;
        try {
            m_ci.startAcceptingConnections();
            ByteBuffer msg = createMsg("@Subscribe", "TOPOLOGY");
            ClientResponseImpl resp = m_ci.handleRead(msg, m_handler, m_cxn);
            assertNotNull(resp);
            assertEquals(ClientResponse.SUCCESS, resp.getStatus());
            statsAnswers.offer(dsOf(getClientResponse("foo")));
            m_ci.schedulePeriodicWorks();

            //Shouldn't get anything
            assertNull(responsesDS.poll(50, TimeUnit.MILLISECONDS));

            statsAnswers.offer(dsOf(getClientResponse("foo")));
            assertNull(responsesDS.poll(50, TimeUnit.MILLISECONDS));

            //Change the bytes of the topology results and expect a topology update
            //to make its way to the client
            ByteBuffer expectedBuf = getClientResponse("bar");
            statsAnswers.offer(dsOf(expectedBuf));
            DeferredSerialization ds = responsesDS.take();
            ByteBuffer actualBuf = ByteBuffer.allocate(ds.getSerializedSize());
            ds.serialize(actualBuf);
            assertEquals(expectedBuf, actualBuf);
        } finally {
            RateLimitedClientNotifier.WARMUP_MS = 1000;
            ClientInterface.TOPOLOGY_CHANGE_CHECK_MS = 5000;
            m_ci.shutdown();
        }
    }

    private DeferredSerialization dsOf(final ByteBuffer buf) {
        return new DeferredSerialization() {
            @Override
            public void serialize(final ByteBuffer outbuf) throws IOException {
                outbuf.put(buf);
            }
            @Override
            public void cancel() {}
            @Override
            public int getSerializedSize() {
                return buf.remaining();
            }
        };
    }

    public ByteBuffer getClientResponse(String str) {
        ClientResponseImpl response = new ClientResponseImpl(ClientResponse.SUCCESS,
                new VoltTable[0], str, ClientInterface.ASYNC_TOPO_HANDLE);
        ByteBuffer buf = ByteBuffer.allocate(response.getSerializedSize() + 4);
        buf.putInt(buf.capacity() - 4);
        response.flattenToBuffer(buf);
        buf.flip();
        return buf;
    }
}
