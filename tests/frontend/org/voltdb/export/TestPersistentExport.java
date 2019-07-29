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

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.TestExportBaseSocketExport.ServerListener;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

public class TestPersistentExport extends ExportLocalClusterBase {
    private LocalCluster m_cluster;

    private static int KFACTOR = 1;
    private static final String SCHEMA =
            "CREATE TABLE T1 EXPORT TO TARGET FOO1 ON INSERT, DELETE (a integer not null, b integer not nulL);" +
            " PARTITION table T1 ON COLUMN a;" +
            "CREATE TABLE T3 EXPORT TO TARGET FOO3 ON UPDATE (a integer not null, b integer not nulL);";
    private static List<String> streamNames = new ArrayList<>();

    @Before
    public void setUp() throws Exception {
        streamNames.addAll(Arrays.asList("T1", "T2"));
    }

    @After
    public void tearDown() throws Exception {
        if (m_cluster != null) {
            System.out.println("Shutting down client and server");
            for (Entry<String, ServerListener> entry : m_serverSockets.entrySet()) {
                ServerListener serverSocket = entry.getValue();
                if (serverSocket != null) {
                    serverSocket.closeClient();
                    serverSocket.close();
                }
            }
            m_cluster.shutDown();
            m_cluster = null;
        }
    }

    @Test
    public void testInsertDeleteUpdate() throws Exception {
        if (!MiscUtils.isPro()) {
            return;
        }
        resetDir();
        VoltFile.resetSubrootForThisProcess();

        VoltProjectBuilder builder = null;
        builder = new VoltProjectBuilder();
        builder.addLiteralSchema(SCHEMA);
        builder.setUseDDLSchema(true);
        builder.setPartitionDetectionEnabled(true);
        builder.setDeadHostTimeout(30);
        builder.addExport(true /* enabled */,
                         ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                         createSocketExportProperties("T1", false /* is replicated stream? */),
                         "FOO1");
        builder.addExport(true /* enabled */,
                ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                createSocketExportProperties("T3", false /* is replicated stream? */),
                "FOO3");

        // Start socket exporter client
        startListener();

        m_cluster = new LocalCluster("TestPersistentExport.jar", 4, 3, KFACTOR, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setNewCli(true);
        m_cluster.setHasLocalServer(false);
        m_cluster.overrideAnyRequestForValgrind();
        boolean success = m_cluster.compile(builder);
        assertTrue(success);
        m_cluster.startUp(true);
        m_verifier = new ExportTestExpectedData(m_serverSockets, false /*is replicated stream? */, true, KFACTOR + 1);
        m_verifier.m_verifySequenceNumber = false;
        m_verifier.m_verbose = false;

        Client client = getClient(m_cluster);

        //add data to stream table
        Object[] data = new Object[3];
        Arrays.fill(data, 1);
        insertToStream("T1", 0, 100, client, data);
        client.callProcedure("@AdHoc", "delete from T1 where a < 10000;");
        client.drain();
        TestExportBaseSocketExport.waitForExportAllRowsDelivered(client, streamNames);
        checkTupleCount(client, "T1", 200, false);
        m_verifier.verifyRows();

        // test update on replicated table
        insertToStream("T3", 0, 100, client, data);
        client.callProcedure("@AdHoc", "update T3 set b = 100 where a < 10000;");
        client.drain();
        TestExportBaseSocketExport.waitForExportAllRowsDelivered(client, streamNames);
        checkTupleCount(client, "T3", 200, true);

        // Change trigger to update_new
        client.callProcedure("@AdHoc", "ALTER TABLE T3 ALTER EXPORT TO TARGET FOO3 ON UPDATE_NEW,DELETE;");
        client.callProcedure("@AdHoc", "update T3 set b = 200 where a < 10000;");
        client.drain();
        TestExportBaseSocketExport.waitForExportAllRowsDelivered(client, streamNames);
        checkTupleCount(client, "T3", 300, true);

        client.callProcedure("@AdHoc", "delete from T3 where a < 10000;");
        client.drain();
        TestExportBaseSocketExport.waitForExportAllRowsDelivered(client, streamNames);
        checkTupleCount(client, "T3", 400, true);

        //test alter table add column
        client.callProcedure("@AdHoc", "ALTER TABLE T3 ADD COLUMN tweaked SMALLINT DEFAULT 0;");
        client.callProcedure("@AdHoc", "ALTER TABLE T3 ALTER EXPORT TO TARGET FOO3 ON INSERT;");
        insertToStreamWithNewColumn("T3", 600, 100, client, data);
        client.drain();
        TestExportBaseSocketExport.waitForExportAllRowsDelivered(client, streamNames);
        checkTupleCount(client, "T3", 500, true);
    }

    private static void checkTupleCount(Client client, String tableName, long expectedCount, boolean replicated){

        //allow time to get the stats
        final long maxSleep = TimeUnit.MINUTES.toMillis(2);
        boolean success = false;
        long start = System.currentTimeMillis();
        while (!success) {
            try {
                VoltTable vt = client.callProcedure("@Statistics", "EXPORT").getResults()[0];
                long count = 0;
                while (vt.advanceRow()) {
                    if (tableName.equalsIgnoreCase(vt.getString("SOURCE"))
                            && "TRUE".equalsIgnoreCase(vt.getString("ACTIVE"))) {
                        if (replicated) {
                            if (0 == vt.getLong("PARTITION_ID")) {
                                count = vt.getLong("TUPLE_COUNT");
                                break;
                            }
                        } else {
                            count +=vt.getLong("TUPLE_COUNT");
                        }
                    }
                }
                if (count == expectedCount) {
                    return;
                }
                if (maxSleep < (System.currentTimeMillis() - start)) {
                    break;
                }
                try { Thread.sleep(5000); } catch (Exception ignored) { }

            } catch (Exception e) {
            }
        }
        assert(false);
    }
}
