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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Future;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.json_voltpatches.JSONStringer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.SnapshotSchedule;
import org.voltdb.client.ClientResponse;
import org.voltdb.sysprocs.SnapshotSave;
import org.voltdb.sysprocs.SnapshotScan;

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

                ClientResponse response =
                        new ClientResponseImpl(ClientResponse.SUCCESS,
                                               (byte) 0, stringer.toString(),
                                               new VoltTable[] {result}, null);

                this.daemon.processClientResponse(response, clientData);
            }
        }

        public void clear() {
            procedureName = null;
            clientData = Long.MIN_VALUE;
            params = null;
        }
    }

    protected Initiator m_initiator;
    protected SnapshotDaemon m_daemon;
    protected MockVoltDB m_mockVoltDB;

    @Before
    public void setUp() throws Exception {
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
    }

    public SnapshotDaemon getSnapshotDaemon() throws Exception {
        if (m_daemon != null) {
            m_daemon.shutdown();
            m_mockVoltDB.shutdown(null);
        }
        m_mockVoltDB = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        m_initiator = new Initiator();
        m_daemon = new SnapshotDaemon();
        m_daemon.init(m_initiator, m_mockVoltDB.getHostMessenger().getZK());
        return m_daemon;
    }

    @Test
    public void testBadFrequencyAndBasicInit() throws Exception {
        SnapshotDaemon noSnapshots = getSnapshotDaemon();
        Thread.sleep(60);
        assertNull(m_initiator.procedureName);
        boolean threwException = false;
        try {
            Future<Void> future = noSnapshots.processClientResponse(null, 0);
            future.get();
        } catch (Throwable t) {
            threwException = true;
        }
        assertTrue(threwException);


        final SnapshotSchedule schedule = new SnapshotSchedule();
        schedule.setFrequencyunit("q");
        threwException = false;
        SnapshotDaemon d = getSnapshotDaemon();
        try {
            Future<Void> future = d.makeActive(schedule);
            future.get();
        } catch (Throwable t) {
            threwException = true;
        }
        assertTrue(threwException);

        schedule.setFrequencyunit("s");
        d.makeActive(schedule);

        schedule.setFrequencyunit("m");
        d.makeActive(schedule);

        schedule.setFrequencyunit("h");

        d.makeActive(schedule);
        threwException = false;
        try {
            Future<Void> future = d.processClientResponse(null, 0);
            future.get();
        } catch (Throwable t) {
            threwException = true;
        }
        assertTrue(threwException);
    }

    public SnapshotDaemon getBasicDaemon() throws Exception {
        final SnapshotSchedule schedule = new SnapshotSchedule();
        schedule.setFrequencyunit("s");
        schedule.setFrequencyvalue(1);
        schedule.setPath("/tmp");
        schedule.setPrefix("woobie");
        schedule.setRetain(2);
        SnapshotDaemon d = getSnapshotDaemon();
        d.makeActive(schedule);
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

    public ClientResponse getFailureResponse() {
        return new ClientResponse() {

            @Override
            public Exception getException() {
                return null;
            }

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

        };
    }

    public ClientResponse getSuccessResponse(final long txnId) {
        return new ClientResponse() {

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
            public Exception getException() {
                // TODO Auto-generated method stub
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

        };
    }

    public ClientResponse getErrMsgResponse() {
        return new ClientResponse() {

            @Override
            public Exception getException() {
                return null;
            }

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

        };
    }

    @Test
    public void testFailedScan() throws Exception {

        SnapshotDaemon daemon = getBasicDaemon();

        long handle = m_initiator.clientData;
        assertTrue("@SnapshotScan".equals(m_initiator.procedureName));
        assertEquals(1, m_initiator.params.length);
        assertTrue("/tmp".equals(m_initiator.params[0]));

        m_initiator.clear();
        Thread.sleep(60);

        assertNull(m_initiator.params);

        daemon.processClientResponse(getFailureResponse(), handle);
        Thread.sleep(60);
        assertNull(m_initiator.params);

        daemon.processClientResponse(null, 0);
        Thread.sleep(60);
        assertNull(m_initiator.params);

        daemon = getBasicDaemon();

        assertNotNull(m_initiator.params);
        daemon.processClientResponse(getErrMsgResponse(), m_initiator.clientData).get();
        assertEquals(SnapshotDaemon.State.FAILURE, daemon.getState());
    }

    public ClientResponse getSuccessfulScanOneResult() {
        return new ClientResponse() {

            @Override
            public Exception getException() {
                return null;
            }

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                VoltTable resultTable = new VoltTable(SnapshotScan.clientColumnInfo);
                resultTable.addRow(
                        "/tmp",
                        "woobie_",
                        0,
                        1,
                        0,
                        "",
                        "",
                        "",
                        "");
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

        };
    }

    public ClientResponse getSuccessfulScanThreeResults() {
        return new ClientResponse() {

            @Override
            public Exception getException() {
                return null;
            }

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                VoltTable resultTable = new VoltTable(SnapshotScan.clientColumnInfo);
                resultTable.addRow(
                        "/tmp",
                        "woobie_2",
                        2,
                        2,
                        0,
                        "",
                        "",
                        "",
                        "");
                resultTable.addRow(
                        "/tmp",
                        "woobie_5",
                        5,
                        5,
                        0,
                        "",
                        "",
                        "",
                        "");
                resultTable.addRow(
                        "/tmp",
                        "woobie_3",
                        3,
                        3,
                        0,
                        "",
                        "",
                        "",
                        "");
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

        };
    }

    @Test
    public void testSuccessfulScan() throws Exception {
        SnapshotDaemon daemon = getBasicDaemon();

        long handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getSuccessfulScanOneResult(), handle).get();
        Thread.sleep(60);
        assertNull(m_initiator.procedureName);

        daemon = getBasicDaemon();

        handle = m_initiator.clientData;
        daemon.processClientResponse(getSuccessfulScanThreeResults(), handle).get();
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

        daemon.processClientResponse(getFailureResponse(), handle).get();
        assertNull(m_initiator.procedureName);
        assertEquals(daemon.getState(), SnapshotDaemon.State.FAILURE);

        daemon = getBasicDaemon();

        handle = m_initiator.clientData;
        daemon.processClientResponse(getSuccessfulScanThreeResults(), handle).get();
        daemon.processClientResponse(getErrMsgResponse(), 1);
        Thread.sleep(60);
        assertEquals(daemon.getState(), SnapshotDaemon.State.WAITING);
    }

    @Test
    public void testDoSnapshot() throws Exception {
        SnapshotDaemon daemon = getBasicDaemon();

        long handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getSuccessfulScanOneResult(), handle).get();
        assertNull(m_initiator.procedureName);
        Thread.sleep(500);
        assertNull(m_initiator.procedureName);
        Thread.sleep(800);
        assertNotNull(m_initiator.procedureName);
        assertTrue("@SnapshotSave".equals(m_initiator.procedureName));
        JSONObject jsObj = new JSONObject((String)m_initiator.params[0]);
        assertTrue(jsObj.getString("path").equals("/tmp"));
        assertTrue(jsObj.getString("nonce").startsWith("woobie_"));
        assertTrue(jsObj.length() == 2);

        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getFailureResponse(), handle).get();
        assertNull(m_initiator.procedureName);
        assertEquals(SnapshotDaemon.State.FAILURE, daemon.getState());

        daemon = getBasicDaemon();

        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getSuccessfulScanOneResult(), handle);
        Thread.sleep(1200);
        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getErrMsgResponse(), handle).get();
        assertEquals(daemon.getState(), SnapshotDaemon.State.WAITING);

        daemon = getBasicDaemon();

        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getSuccessfulScanThreeResults(), handle).get();
        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(getErrMsgResponse(), handle).get();
        assertNull(m_initiator.procedureName);
        Thread.sleep(1200);
        assertNotNull(m_initiator.procedureName);
        handle = m_initiator.clientData;
        m_initiator.clear();
        Thread.sleep(1200);
        assertNull(m_initiator.procedureName);
        daemon.processClientResponse(new ClientResponse() {

            @Override
            public Exception getException() {
                return null;
            }

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                VoltTable result = new VoltTable(SnapshotSave.nodeResultsColumns);
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

        }, handle).get();
        assertEquals(SnapshotDaemon.State.WAITING, daemon.getState());
        assertNull(m_initiator.procedureName);

        Thread.sleep(1200);
        assertNotNull(m_initiator.procedureName);
        assertTrue("@SnapshotSave".equals(m_initiator.procedureName));
        handle = m_initiator.clientData;
        m_initiator.clear();
        daemon.processClientResponse(new ClientResponse() {

            @Override
            public Exception getException() {
                return null;
            }

            @Override
            public String getStatusString() {
                return null;
            }

            @Override
            public VoltTable[] getResults() {
                VoltTable result = new VoltTable(SnapshotSave.nodeResultsColumns);
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

        }, handle);

        Thread.sleep(1200);
        assertNotNull(m_initiator.procedureName);
        assertTrue("@SnapshotDelete".equals(m_initiator.procedureName));
    }
}
