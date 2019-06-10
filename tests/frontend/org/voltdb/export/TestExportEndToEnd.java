/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.export;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.TestExportBaseSocketExport.ServerListener;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.Maps;

public class TestExportEndToEnd extends ExportLocalClusterBase {

    private LocalCluster m_cluster;

    private static int KFACTOR = 1;
    private static int HOST_COUNT = 3;
    private static int SPH = 2;
    private static final String T1_SCHEMA =
            "CREATE STREAM t_1 "
            + "PARTITION ON COLUMN a "
            + "EXPORT TO TARGET export_target_a ("
            + "     a integer not null, "
            + "     b integer not null"
            + ");";

    private static final String T2_SCHEMA =
                "CREATE STREAM t_2 "
                + "EXPORT TO TARGET export_target_b ("
                + "     a integer not null, "
                + "     b integer not null"
                + ");";

    @Before
    public void setUp() throws Exception
    {
        resetDir();
        VoltFile.resetSubrootForThisProcess();

        VoltProjectBuilder builder = null;
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(T1_SCHEMA);
        builder.addLiteralSchema(T2_SCHEMA);
        builder.setUseDDLSchema(true);
        builder.setPartitionDetectionEnabled(true);
        builder.setDeadHostTimeout(30);
//        builder.configureLogging(true, true, 2, 2, 64);
        // Each stream needs an exporter configuration
        builder.addExport(true /* enabled */,
                         ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                         createSocketExportProperties("t_1", false /* is replicated stream? */),
                         "export_target_a");
        builder.addExport(true /* enabled */,
                ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                createSocketExportProperties("t_2", false /* is replicated stream? */),
                "export_target_b");
        // Start socket exporter client
        startListener();

        m_cluster = new LocalCluster("testFlushExportBuffer.jar", SPH, HOST_COUNT, KFACTOR, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setNewCli(true);
        m_cluster.setHasLocalServer(false);
        m_cluster.overrideAnyRequestForValgrind();
        // Config custom socket exporter
        boolean success = m_cluster.compile(builder);
        assertTrue(success);
        m_cluster.startUp(true);

        // TODO: verifier should be created based on socket exporter settings
        m_verifier = new ExportTestExpectedData(m_serverSockets, false /*is replicated stream? */, true, KFACTOR + 1);
    }

    @After
    public void tearDown() throws Exception {
        System.out.println("Shutting down client and server");
        for (Entry<String, ServerListener> entry : m_serverSockets.entrySet()) {
            ServerListener serverSocket = entry.getValue();
            if (serverSocket != null) {
                serverSocket.closeClient();
                serverSocket.close();
            }
        }
        m_cluster.shutDown();
    }

    @Test
    public void testExportRejoinThenDropStream_ENG_15740() throws Exception
    {
        Client client = getClient(m_cluster);
        // Generate PBD files
        Object[] data = new Object[3];
        Arrays.fill(data, 1);
        insertToStream("t_1", 0, 100, client, data);
        insertToStream("t_2", 0, 100, client, data);

        // kill one node
        m_cluster.killSingleHost(1);

        // drop stream
        ClientResponse response = client.callProcedure("@AdHoc", "DROP STREAM t_1");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        // rejoin node back
        m_cluster.rejoinOne(1);
        client.drain();

        client = getClient(m_cluster);
        TestExportBaseSocketExport.waitForStreamedTargetAllocatedMemoryZero(client);
        assertEquals(3, m_cluster.getLiveNodeCount());
    }

    @Test
    public void testExportRejoinOldGenerationStream_ENG_16239() throws Exception
    {
        Client client = getClient(m_cluster);
        // Generate PBD files
        Object[] data = new Object[3];
        Arrays.fill(data, 1);
        int pkeyStart = 0;
        insertToStream("t_1", pkeyStart, 1000, client, data);

        // Write some data to PBD then kill one node
        client.drain();
        client.callProcedure("@Quiesce");
        m_cluster.killSingleHost(0);

        // drop stream
        ClientResponse response = client.callProcedure("@AdHoc", "DROP STREAM t_1");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("@AdHoc", T1_SCHEMA);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        pkeyStart = 1000;
        insertToStream("t_1", pkeyStart, 100, client, data);
        client.drain();
        // rejoin node back
        m_cluster.rejoinOne(0);

        client = getClient(m_cluster);
        client.drain();
        client.callProcedure("@Quiesce");
        TestExportBaseSocketExport.waitForStreamedTargetAllocatedMemoryZero(client);
        // make sure no partition has more than active stream
        VoltTable stats = client.callProcedure("@Statistics", "export", 0).getResults()[0];
        Map<String, Integer> masterCounters = Maps.newHashMap();
        while (stats.advanceRow()) {
            String target = stats.getString("TARGET");
            String ttable = stats.getString("SOURCE");
            String isMaster = stats.getString("ACTIVE");
            Long pid = stats.getLong("PARTITION_ID");
            String key = "TARGET: " + target + " SOURCE: " + ttable + " PARTITION_ID: " + pid + " ACTIVE: " + isMaster;
            Integer count = masterCounters.get(key);
            if (count == null) {
                masterCounters.put(key, 1);
            } else {
                masterCounters.put(key, count + 1);
            }
        }
        for (Entry<String, Integer> e : masterCounters.entrySet()) {
            if (e.getValue() > 1) {
                assertEquals("Stream (" + e.getKey() + ") has more than one master", 1, (int)e.getValue());
            }
        }
    }

    @Test
    @Ignore
    public void testExportRecoverWithEmptySnapshot() throws Exception
    {
        Client client = getClient(m_cluster);
        ClientResponse response = client.callProcedure("@SnapshotSave",
                                                        "/tmp/" + System.getProperty("user.name"),
                                                        "testnonce",
                                                        (byte) 1);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("@AdHoc", "DROP STREAM t_1");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        response = client.callProcedure("@AdHoc", T1_SCHEMA);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        // Generate PBD files
        Object[] data = new Object[3];
        Arrays.fill(data, 1);
        int pkeyStart = 0;
        insertToStream("t_1", pkeyStart, 100, client, data);
        // Write some data to PBD then kill one node
        client.drain();
        client.callProcedure("@Quiesce");
        m_cluster.shutDown();

        // drop stream
        m_cluster.startUp(false);
        client = getClient(m_cluster);
        client.drain();
        client.callProcedure("@Quiesce");
        TestExportBaseSocketExport.waitForStreamedTargetAllocatedMemoryZero(client);
    }
}
