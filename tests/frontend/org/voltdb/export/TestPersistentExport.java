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

import java.util.Arrays;
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
import org.voltdb.utils.VoltFile;

public class TestPersistentExport extends ExportLocalClusterBase {
    private LocalCluster m_cluster;

    private static int KFACTOR = 1;
    private static final String SCHEMA =
            "CREATE TABLE T1 (a integer not null, b integer not nulL) EXPORT TO TARGET FOO1 ON(INSERT, DELETE);" +
            " PARTITION table T1 ON COLUMN a;" +
            "CREATE TABLE T3 (a integer not null, b integer not nulL) EXPORT TO TARGET FOO3 ON(UPDATE);";

    @Before
    public void setUp() throws Exception
    {
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
    public void testInsertDeleteUpdate() throws Exception {
        Client client = getClient(m_cluster);

        //add data to stream table
        Object[] data = new Object[3];
        Arrays.fill(data, 1);
        insertToStream("T1", 0, 100, client, data);
        client.callProcedure("@AdHoc", "delete from T1 where a < 10000;");
        client.drain();
        TestExportBaseSocketExport.waitForStreamedTargetAllocatedMemoryZero(client);
        checkTupleCount(client, "T1", 200, false);
        m_verifier.verifyRows();

        // On replicated table
        insertToStream("T3", 0, 100, client, data);
        client.callProcedure("@AdHoc", "update T3 set b = 100 where a < 10000;");
        client.drain();
        TestExportBaseSocketExport.waitForStreamedTargetAllocatedMemoryZero(client);
        checkTupleCount(client, "T3", 200, true);

        // Change trigger to update_new
        client.callProcedure("@AdHoc", "ALTER TABLE T3 EXPORT TO TARGET FOO3 ON (UPDATE_NEW)");
        client.callProcedure("@AdHoc", "update T3 set b = 200 where a < 10000;");
        client.drain();
        TestExportBaseSocketExport.waitForStreamedTargetAllocatedMemoryZero(client);
        checkTupleCount(client, "T3", 300, true);
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
                vt.resetRowPosition();
                while (vt.advanceRow()) {
                    if (tableName.equalsIgnoreCase(vt.getString("SOURCE"))
                            && "TRUE".equalsIgnoreCase(vt.getString("ACTIVE"))) {
                        count +=vt.getLong("TUPLE_COUNT");
                        if (replicated && count > 0) {
                            assert(count == expectedCount);
                            return;
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
