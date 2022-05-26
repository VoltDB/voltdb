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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.mockito.ArgumentCaptor;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.network.Connection;
import org.voltcore.network.WriteStream;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.saverestore.SnapshotPathType;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.VoltSnapshotFile;

import com.google_voltpatches.common.util.concurrent.Callables;

public class TestSnapshotDaemon {

    static class Initiator implements SnapshotDaemon.DaemonInitiator {
        private final SnapshotDaemon daemon;

        String procedureName;
        long clientData;
        Object[] params;

        public Initiator() {
            this(null);
        }

        public Initiator(SnapshotDaemon daemon) {
            this.daemon = daemon;
        }

        @Override
        public void initiateSnapshotDaemonWork(String procedureName,
                long clientData, Object[] params) {
            this.procedureName = procedureName;
            this.clientData = clientData;
            this.params = params;

            /*
             * Fake a snapshot response if the daemon is set
             */
            if (this.daemon != null) {
                /*
                 * We need at least two columns so that the snapshot daemon won't
                 * think that this is an error.
                 */
                ColumnInfo column1 = new ColumnInfo("RESULT", VoltType.STRING);
                ColumnInfo column2 = new ColumnInfo("ERR_MSG", VoltType.STRING);
                VoltTable result = new VoltTable(new ColumnInfo[] {column1, column2});
                result.addRow("SUCCESS", "BLAH");

                JSONStringer stringer = new JSONStringer();
                try {
                    stringer.object();
                    stringer.key("txnId").value(1);
                    stringer.endObject();
                } catch (JSONException e) {
                    e.printStackTrace();
                }

                ClientResponseImpl response =
                        new ClientResponseImpl(ClientResponse.SUCCESS,
                                               (byte) 0, stringer.toString(),
                                               new VoltTable[] {result}, null);
                response.setClientHandle(clientData);
                this.daemon.processClientResponse(Callables.returning(response));
            }
        }

        public void clear() {
            procedureName = null;
            clientData = Long.MIN_VALUE;
            params = null;
        }
    }

    private static File tempDir = new VoltSnapshotFile("/tmp/" + System.getProperty("user.name"));
    protected Initiator m_initiator;
    protected SnapshotDaemon m_daemon;
    protected SnapshotIOAgent m_ioAgent;
    protected MockVoltDB m_mockVoltDB;

    @Rule
    public final TestName m_name = new TestName();

    @Before
    public void setUp() throws Exception {
        System.out.printf("-=-=-=--=--=-=- Start of test %s -=-=-=--=--=-=-\n", m_name.getMethodName());
        SnapshotDaemon.m_periodicWorkInterval = 100;
        SnapshotDaemon.m_minTimeBetweenSysprocs = 1000;
    }

    @After
    public void tearDown() throws Exception {
        m_daemon.shutdown();
        m_mockVoltDB.shutdown(null);
        m_mockVoltDB = null;
        m_daemon = null;
        m_initiator = null;
        System.out.printf("-=-=-=--=--=-=- End of test %s -=-=-=--=--=-=-\n", m_name.getMethodName());
    }

    public SnapshotDaemon getSnapshotDaemon(boolean wantFakeResponses) throws Exception {
        if (m_ioAgent != null) {
            m_ioAgent.shutdown();
            m_ioAgent = null;
        }
        if (m_daemon != null) {
            m_daemon.shutdown();
            m_mockVoltDB.shutdown(null);
        }
        m_mockVoltDB = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        m_mockVoltDB.addSite(CoreUtils.getHSIdFromHostAndSite(0, 1), 0);
        m_mockVoltDB.addTable("partitioned", false);
        m_mockVoltDB.configureLogging(true, true, 1000, 5, tempDir.getPath(), "/tmp");
        m_mockVoltDB.configureSnapshotSchedulePath("/tmp");

        HostMessenger messenger = m_mockVoltDB.getHostMessenger();
        m_ioAgent =
                new SnapshotIOAgentImpl(messenger, messenger.getHSIdForLocalSite(HostMessenger.SNAPSHOT_IO_AGENT_ID));
        messenger.createMailbox(m_ioAgent.getHSId(), m_ioAgent);

        m_daemon = new SnapshotDaemon(m_mockVoltDB.getCatalogContext());
        if (wantFakeResponses) {
            m_initiator = new Initiator(m_daemon);
        } else {
            m_initiator = new Initiator();
        }
        m_daemon.init(m_initiator, m_mockVoltDB.getHostMessenger(), null, null);
        return m_daemon;
    }

    private ClientResponseImpl getResponseFromConnectionMock(Connection c) throws IOException
    {
        ArgumentCaptor<ByteBuffer> buf = ArgumentCaptor.forClass(ByteBuffer.class);
        verify(c.writeStream(), timeout(10000)).enqueue(buf.capture());
        ClientResponseImpl resp = new ClientResponseImpl();
        ByteBuffer b = buf.getValue();
        b.position(4);
        resp.initFromBuffer(b);
        reset(c.writeStream());
        return resp;
    }

    @Test
    public void testParamCount() throws Exception {
        System.out.println("--------------\n  testParamCount\n---------------");
        SnapshotDaemon dut = getSnapshotDaemon(false);
        Connection c = mock(Connection.class);
        WriteStream ws = mock(WriteStream.class);
        when(c.writeStream()).thenReturn(ws);
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        // test 2 fail
        Object[] params = new Object[2];
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        ClientResponseImpl resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("Invalid number of parameters"));
        // test 4 fail and call it good
        params = new Object[4];
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("Invalid number of parameters"));
    }

    // Quick unit test to check invalid legacy param cases
    @Test
    public void testLegacyParams() throws Exception {
        System.out.println("--------------\n  testLegacyParams\n---------------");
        SnapshotDaemon dut = getSnapshotDaemon(false);
        Connection c = mock(Connection.class);
        WriteStream ws = mock(WriteStream.class);
        when(c.writeStream()).thenReturn(ws);
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        Object[] params = new Object[3];
        // Test path null fail
        params[0] = null;
        params[1] = null;
        params[2] = null;
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        ClientResponseImpl resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("@SnapshotSave path is null"));
        // Test nonce null fail
        params[0] = "haha";
        params[1] = null;
        params[2] = null;
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("@SnapshotSave nonce is null"));
        // Test blocking null fail
        params[0] = "haha";
        params[1] = "hoho";
        params[2] = null;
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("@SnapshotSave blocking is null"));
        // Test path is a string fail
        params[0] = 0l;
        params[1] = "hoho";
        params[2] = 0;
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("@SnapshotSave path param is a"));
        // Test nonce is a string fail
        params[0] = "haha";
        params[1] = 0l;
        params[2] = 0;
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("@SnapshotSave nonce param is a"));
        // Test blocking is a number fail
        params[0] = "haha";
        params[1] = "hoho";
        params[2] = "0";
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("@SnapshotSave blocking param is a"));
        // Test invalid nonce character '-'
        params[0] = "haha";
        params[1] = "ho-ho";
        params[2] = 0;
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("contains a prohibited character"));
        // Test invalid nonce character ','
        params[0] = "haha";
        params[1] = "ho,ho";
        params[2] = 0;
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString().contains("contains a prohibited character"));
    }

    // Quick unit test to check invalid JSON param cases
    @Test
    public void testJsonParams() throws Exception {
        System.out.println("--------------\n  testJsonParams\n---------------");
        SnapshotDaemon dut = getSnapshotDaemon(false);
        Connection c = mock(Connection.class);
        WriteStream ws = mock(WriteStream.class);
        when(c.writeStream()).thenReturn(ws);
        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        // Test null fail
        Object[] params = new Object[1];
        params[0] = null;
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        ClientResponseImpl resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString(), resp.getStatusString().contains("@SnapshotSave JSON blob is null"));
        // Test not a string
        params[0] = 0l;
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString(), resp.getStatusString().contains("@SnapshotSave JSON blob is a"));
        // Test empty uripath
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        stringer.key(SnapshotUtil.JSON_URIPATH).value("");
        stringer.endObject();
        params[0] = stringer.toString();
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString(), resp.getStatusString().contains("uripath cannot be empty"));
        // Test invalid uripath null scheme
        stringer = new JSONStringer();
        stringer.object();
        stringer.key(SnapshotUtil.JSON_URIPATH).value("good.luck.chuck");
        stringer.endObject();
        params[0] = stringer.toString();
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString(), resp.getStatusString().contains("URI scheme cannot be null"));
        // Test invalid uripath null scheme
        stringer = new JSONStringer();
        stringer.object();
        stringer.key(SnapshotUtil.JSON_URIPATH).value("http://good.luck.chuck");
        stringer.endObject();
        params[0] = stringer.toString();
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString(), resp.getStatusString().contains("Unsupported URI scheme"));
        // Test empty nonce
        stringer = new JSONStringer();
        stringer.object();
        stringer.key(SnapshotUtil.JSON_URIPATH).value("file://good.luck.chuck");
        stringer.key(SnapshotUtil.JSON_NONCE).value("");
        stringer.endObject();
        params[0] = stringer.toString();
        spi.setParams(params);
        dut.requestUserSnapshot(spi, c);
        resp = getResponseFromConnectionMock(c);
        assertEquals(ClientResponseImpl.GRACEFUL_FAILURE, resp.getStatus());
        assertTrue(resp.getStatusString(), resp.getStatusString().contains("nonce cannot be empty"));
    }

    @Test
    public void testBadFrequencyAndBasicInit() throws Exception {
        System.out.println("--------------\n  testBadFrequencyAndBasicInit\n---------------");
        SnapshotDaemon noSnapshots = getSnapshotDaemon(false);
        Thread.sleep(60);
        assertNull(m_initiator.procedureName);
        boolean threwException = false;
        try {
            Future<Void> future = noSnapshots.processClientResponse(null);
            future.get();
        } catch (Throwable t) {
            threwException = true;
        }
        //Changed behavior to not throw ever, log instead
        assertFalse(threwException);


        final SnapshotSchedule schedule = new SnapshotSchedule();
        schedule.setEnabled(true);
        schedule.setFrequencyunit("q");
        threwException = false;
        SnapshotDaemon d = getSnapshotDaemon(false);
        try {
            Future<Void> future = d.mayGoActiveOrInactive(schedule);
            future.get();
        } catch (Throwable t) {
            threwException = true;
        }
        assertTrue(threwException);

        schedule.setFrequencyunit("s");
        d.mayGoActiveOrInactive(schedule);

        schedule.setFrequencyunit("m");
        d.mayGoActiveOrInactive(schedule);

        schedule.setFrequencyunit("h");

        d.mayGoActiveOrInactive(schedule);
        threwException = false;
        try {
            Future<Void> future = d.processClientResponse(null);
            future.get();
        } catch (Throwable t) {
            threwException = true;
        }
        //Changed behavior to not throw ever, log instead
        assertFalse(threwException);
    }

    public SnapshotDaemon getBasicDaemon(boolean wantFakeResponses) throws Exception {
        final SnapshotSchedule schedule = new SnapshotSchedule();
        schedule.setFrequencyunit("s");
        schedule.setFrequencyvalue(1);
        schedule.setPrefix("woobie");
        schedule.setRetain(2);
        schedule.setEnabled(true);
        SnapshotDaemon d = getSnapshotDaemon(wantFakeResponses);
        d.mayGoActiveOrInactive(schedule);
        checkForSnapshotScan(m_initiator);
        return d;
    }

    private static void checkForSnapshotScan(Initiator initiator) throws Exception {
        for (int ii = 0; ii < 30; ii++) {
            Thread.sleep(60);
            if (initiator.procedureName != null) break;
        }
        assertNotNull(initiator.procedureName);
        assertTrue(initiator.procedureName.equals("@SnapshotScan"));
    }

    public Callable<ClientResponseImpl> getFailureResponse(final long handle) {
        ClientResponseImpl response = new ClientResponseImpl() {

            @Override
            public String getStatusString() {
                return "Super fail";
            }

            @Override
            public VoltTable[] getResults() {
                return null;
            }

            @Override
            public byte getStatus() {
                return ClientResponse.UNEXPECTED_FAILURE;
            }

            @Override
            public int getClusterRoundtrip() {
                return 0;
            }

            @Override
            public int getClientRoundtrip() {
                return 0;
            }

            @Override
            public byte getAppStatus() {
                return 0;
            }

            @Override
            public String getAppStatusString() {
                return null;
            }

            @Override
            public long getClientHandle() {
                return handle;
            }

        };
        return Callables.returning(response);
    }

    public Callable<ClientResponseImpl> getSuccessResponse(final long txnId, final long handle) {
        ClientResponseImpl response = new ClientResponseImpl() {

            @Override
            public byte getStatus() {
                return ClientResponse.SUCCESS;
            }

            @Override
            public byte getAppStatus() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public VoltTable[] getResults() {
                VoltTable result =
                    new VoltTable(
                            new ColumnInfo("ERR_MSG", VoltType.STRING),
                            new ColumnInfo("RESULT", VoltType.STRING));
                return new VoltTable[] { result };
            }

            @Override
            public String getStatusString() {
                // TODO Auto-generated method stub
                return null;
            }

            @Override
            public String getAppStatusString() {
                try {
                    JSONStringer stringer = new JSONStringer();
                    stringer.object();
                    stringer.key("txnId").value(Long.toString(txnId));
                    stringer.endObject();
                    return stringer.toString();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return null;
            }

            @Override
            public int getClusterRoundtrip() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public int getClientRoundtrip() {
                // TODO Auto-generated method stub
                return 0;
            }

            @Override
            public long getClientHandle() {
                return handle;
            }

        };

        return Callables.returning(response);
    }

    public Callable<ClientResponseImpl> getErrMsgResponse(final long handle) {
        ClientResponseImpl response =  new ClientResponseImpl() {

            @Override
            public VoltTable[] getResults() {
                VoltTable resultTable = new VoltTable(new ColumnInfo("ERR_MSG", VoltType.STRING));
                resultTable.addRow("It's a fail!");
                return  new VoltTable[] { resultTable };
            }

            @Override
            public byte getStatus() {
                return ClientResponse.SUCCESS;
            }

            @Override
            public int getClusterRoundtrip() {
                return 0;
            }

            @Override
            public int getClientRoundtrip() {
                return 0;
            }

            @Override
            public byte getAppStatus() {
                return 0;
            }

            @Override
            public String getAppStatusString() {
                return null;
            }

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public long getClientHandle() {
                return handle;
            }
        };
        return Callables.returning(response);
    }

    @Test
    public void testFailedScan() throws Exception {
        System.out.println("--------------\n  testFailedScan\n---------------");

        SnapshotDaemon daemon = getBasicDaemon(false);

        long handle = m_initiator.clientData;
        assertTrue("@SnapshotScan".equals(m_initiator.procedureName));
        assertEquals(1, m_initiator.params.length);
        assertTrue("/tmp".equals(m_initiator.params[0]));

        m_initiator.clear();
        Thread.sleep(60);

        assertNull(m_initiator.params);

        daemon.processClientResponse(getFailureResponse(handle));
        Thread.sleep(60);
        assertNull(m_initiator.params);

        daemon.processClientResponse(null);
        Thread.sleep(60);
        assertNull(m_initiator.params);

        daemon = getBasicDaemon(false);

        assertNotNull(m_initiator.params);
        daemon.processClientResponse(getErrMsgResponse(m_initiator.clientData)).get();
        assertEquals(SnapshotDaemon.State.WAITING, daemon.getState());
    }

    public Callable<ClientResponseImpl> getSuccessfulScanOneResult(final long handle) {
        ClientResponseImpl response = new ClientResponseImpl() {

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                VoltTable resultTable = new VoltTable(SnapshotScanAgent.clientColumnInfo);
                resultTable.addRow(
                        "/tmp",
                        "woobie_",
                        0,
                        1,
                        0,
                        "",
                        "",
                        "",
                        "",
                        SnapshotPathType.SNAP_PATH.toString()
                );
                return new VoltTable[] { resultTable, null, null };
            }

            @Override
            public byte getStatus() {
                return ClientResponse.SUCCESS;
            }

            @Override
            public int getClusterRoundtrip() {
                return 0;
            }

            @Override
            public int getClientRoundtrip() {
                return 0;
            }

            @Override
            public byte getAppStatus() {
                return 0;
            }

            @Override
            public String getAppStatusString() {
                return null;
            }

            @Override
            public long getClientHandle() {
                return handle;
            }
        };
        return Callables.returning(response);
    }

    public Callable<ClientResponseImpl> getSuccessfulScanThreeResults(final long handle) {
        return threeResults("woobie", handle, SnapshotPathType.SNAP_PATH);
    }

    public Callable<ClientResponseImpl> getShutdownScanThreeResults(final long handle) {
        return threeResults("SHUTDOWN", handle, SnapshotPathType.SNAP_AUTO);
    }

    private Callable<ClientResponseImpl> threeResults(String prefix, long handle, SnapshotPathType type) {

        ClientResponseImpl response = new ClientResponseImpl() {

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                VoltTable resultTable = new VoltTable(SnapshotScanAgent.clientColumnInfo);
                resultTable.addRow(
                        "/tmp",
                        prefix + "_2",
                        2, // txnid
                        2, // created
                        0,
                        "",
                        "",
                        "",
                        "",
                        type.toString()
                        );
                resultTable.addRow(
                        "/tmp",
                        prefix + "_5",
                        5,
                        5,
                        0,
                        "",
                        "",
                        "",
                        "",
                        type.toString()
                        );
                resultTable.addRow(
                        "/tmp",
                        prefix + "_3",
                        3,
                        3,
                        0,
                        "",
                        "",
                        "",
                        "",
                        type.toString()
                        );
                return new VoltTable[] { resultTable, null, null };
            }

            @Override
            public byte getStatus() {
                return ClientResponse.SUCCESS;
            }

            @Override
            public int getClusterRoundtrip() {
                return 0;
            }

            @Override
            public int getClientRoundtrip() {
                return 0;
            }

            @Override
            public byte getAppStatus() {
                return 0;
            }

            @Override
            public String getAppStatusString() {
                return null;
            }

            @Override
            public long getClientHandle() {
                return handle;
            }
        };
        return Callables.returning(response);
    }

    @Test
    public void testSuccessfulScan() throws Exception {
        System.out.println("--------------\n  testSuccessfulScan\n---------------");
        SnapshotDaemon daemon = getBasicDaemon(false);

        long handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getSuccessfulScanOneResult(handle)).get();
        Thread.sleep(60);
        assertNull(m_initiator.procedureName);

        daemon = getBasicDaemon(false);

        handle = m_initiator.clientData;
        daemon.processClientResponse(getSuccessfulScanThreeResults(handle)).get();
        assertNotNull(m_initiator.procedureName);
        assertTrue("@SnapshotDelete".equals(m_initiator.procedureName));
        String path = ((String[])m_initiator.params[0])[0];
        String nonce = ((String[])m_initiator.params[1])[0];
        assertTrue("/tmp".equals(path));
        assertTrue("woobie_2".equals(nonce));
        handle = m_initiator.clientData;
        m_initiator.clear();
        Thread.sleep(60);
        assertNull(m_initiator.procedureName);

        daemon.processClientResponse(getFailureResponse(handle)).get();
        assertNull(m_initiator.procedureName);
        assertEquals(SnapshotDaemon.State.WAITING, daemon.getState());

        daemon = getBasicDaemon(false);

        handle = m_initiator.clientData;
        daemon.processClientResponse(getSuccessfulScanThreeResults(handle)).get();
        daemon.processClientResponse(getErrMsgResponse(1));
        Thread.sleep(60);
        assertEquals(SnapshotDaemon.State.WAITING, daemon.getState());
    }

    @Test
    public void testDoSnapshot() throws Exception {
        System.out.println("--------------\n  testDoSnapshot\n---------------");
        SnapshotDaemon daemon = getBasicDaemon(false);

        long handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getSuccessfulScanOneResult(handle)).get();
        assertNull(m_initiator.procedureName);
        Thread.sleep(500);
        assertNull(m_initiator.procedureName);
        while (m_initiator.procedureName == null) {
            Thread.yield();
        }
        assertTrue("@SnapshotSave".equals(m_initiator.procedureName));
        JSONObject jsObj = new JSONObject((String)m_initiator.params[0]);
        assertTrue(jsObj.getString("path").equals("/tmp"));
        assertTrue(jsObj.getString("nonce").startsWith("woobie_"));
        assertTrue(jsObj.length() == 4);

        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getFailureResponse(handle)).get();
        assertNull(m_initiator.procedureName);
        assertEquals(SnapshotDaemon.State.WAITING, daemon.getState());

        daemon = getBasicDaemon(false);

        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getSuccessfulScanOneResult(handle));
        Thread.sleep(1500); // 1200 was insufficient to pass on my VM --paul
        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getErrMsgResponse(handle)).get();
        assertEquals(SnapshotDaemon.State.WAITING, daemon.getState());

        daemon = getBasicDaemon(false);

        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getSuccessfulScanThreeResults(handle)).get();
        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getErrMsgResponse(handle)).get();
        assertNull(m_initiator.procedureName);
        Thread.sleep(1200);
        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        Thread.sleep(1200);
        assertNull(m_initiator.procedureName);
        final long handleForClosure1 = handle;
        ClientResponseImpl response = new ClientResponseImpl() {

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                VoltTable result = new VoltTable(SnapshotUtil.nodeResultsColumns);
                result.addRow(0, "desktop", "0", "FAILURE", "epic fail");
                return new VoltTable[] { result };
            }

            @Override
            public byte getStatus() {
                return ClientResponse.SUCCESS;
            }

            @Override
            public int getClusterRoundtrip() {
                return 0;
            }

            @Override
            public int getClientRoundtrip() {
                return 0;
            }

            @Override
            public byte getAppStatus() {
                return 0;
            }

            @Override
            public String getAppStatusString() {
                return null;
            }

            @Override
            public long getClientHandle() {
                return handleForClosure1;
            }
        };
        daemon.processClientResponse(Callables.returning(response)).get();
        assertEquals(SnapshotDaemon.State.WAITING, daemon.getState());
        assertNull(m_initiator.procedureName);

        Thread.sleep(1200);
        assertNotNull(m_initiator.procedureName);
        assertTrue("@SnapshotSave".equals(m_initiator.procedureName));
        handle = m_initiator.clientData;
        m_initiator.clear();
        final long handleForClosure2 = handle;
        response = new ClientResponseImpl() {

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                VoltTable result = new VoltTable(SnapshotUtil.nodeResultsColumns);
                result.addRow(0, "desktop", "0", "SUCCESS", "epic success");
                return new VoltTable[] { result };
            }

            @Override
            public byte getStatus() {
                return ClientResponse.SUCCESS;
            }

            @Override
            public int getClusterRoundtrip() {
                return 0;
            }

            @Override
            public int getClientRoundtrip() {
                return 0;
            }

            @Override
            public byte getAppStatus() {
                return 0;
            }

            @Override
            public String getAppStatusString() {
                return null;
            }

            @Override
            public long getClientHandle() {
                return handleForClosure2;
            }
        };
        daemon.processClientResponse(Callables.returning(response));

        Thread.sleep(1200);
        assertNotNull(m_initiator.procedureName);
        assertTrue("@SnapshotDelete".equals(m_initiator.procedureName));
    }

    @Test
    public void testShutdownGrooming() throws Exception {
        System.out.println("--------------\n  testShutdownGrooming\n---------------");
        SnapshotDaemon daemon = getBasicDaemon(false);
        long handle = m_initiator.clientData;
        m_initiator.clear();

        // Process @SnapshotScan results: 3 shutdown snapshots found.
        // Expect daemon should delete oldest one.
        daemon.processClientResponse(getShutdownScanThreeResults(handle)).get();
        assertNotNull(m_initiator.procedureName);
        assertTrue("@SnapshotDelete".equals(m_initiator.procedureName));

        String[] paths = (String[])(m_initiator.params[0]);
        String[] nonces = (String[])(m_initiator.params[1]);
        assertEquals("bad paths count", 1, paths.length);
        assertEquals("bad nonces count", 1, nonces.length);
        assertTrue("bad path", "/tmp".equals(paths[0]));
        assertTrue("bad nonce", "SHUTDOWN_2".equals(nonces[0]));

        handle = m_initiator.clientData;
        m_initiator.clear();
        Thread.sleep(60);
        assertNull(m_initiator.procedureName);

        // Process @SnapshotDelete results.
        // Expect daemon to enter WAITING state
        daemon.processClientResponse(getSuccessResponse(999, handle)).get();
        assertNull(m_initiator.procedureName);
        assertEquals(SnapshotDaemon.State.WAITING, daemon.getState());
    }
}
