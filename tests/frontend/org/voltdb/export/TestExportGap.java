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
package org.voltdb.export;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.compiler.deploymentfile.ServerExportEnum;
import org.voltdb.export.TestExportBaseSocketExport.ServerListener;
import org.voltdb.regressionsuites.LocalCluster;

/**
 *
 */
public class TestExportGap extends ExportLocalClusterBase {

    private static final String S1_SCHEMA =
            "CREATE STREAM s1 "
            + "PARTITION ON COLUMN a "
            + "EXPORT TO TARGET s1_target ("
            + "     a INTEGER NOT NULL, "
            + "     b INTEGER NOT NULL"
            + ");";

    private VoltProjectBuilder m_builder;
    private LocalCluster m_cluster;
    private Client m_client;
    private int m_count = 10;
    private int m_batch = 100;

    @Rule
    public final TestName m_name = new TestName();

    @Before
    public void setup() throws Exception {
        m_builder = new VoltProjectBuilder();
        m_builder.addLiteralSchema(S1_SCHEMA);
        m_builder.setUseDDLSchema(true);

        m_builder.addExport(false /* disabled */,
                ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                createSocketExportProperties("s1", false /* is replicated stream? */),
                "s1_target");
        m_builder.setFlushIntervals(250, 1000, 250);

        int sph = 4;
        int hostCount = 2;
        int kfactor = 1;
        m_cluster = new LocalCluster("testexport.jar", sph, hostCount, kfactor, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setHasLocalServer(false);
        m_cluster.overrideAnyRequestForValgrind();
        assertTrue(m_cluster.compile(m_builder));

        // Start cluster and socket listener
        m_cluster.startUp(true);
        startListener();

        /*
         * Create a verifier but do not require exact verification because
         * the verifier sockets may receive rows out of order, due to the fact that
         * SocketExporter acknowledges rows when written on the sending socket,
         * not when received on the destination host.
         */
        m_verifier = new ExportTestExpectedData(m_serverSockets,
                false  /*is replicated stream? */,
                false, /* not exact verification */
                kfactor + 1);
    }

    @Test(timeout = 90_000)
    public void runTest() throws Exception {
        String stream = "s1";
        int rowCount = 0;

        // MUST CALL getClient because verifier requires
        // the hashinator to be initialized
        m_client = getClient(m_cluster);

        //add data to stream table
        Object[] data = new Object[3];
        Arrays.fill(data, 1);

        for (int i = 0; i < m_count; i++) {
            insertToStream(stream, rowCount, m_batch, m_client, data);
            rowCount += m_batch;
        }
        m_client.drain();

        // Kill host 0 and reopen client
        m_cluster.killSingleHost(0);
        Thread.sleep(500);
        m_client = getClient(m_cluster);

        // Insert more data into surviving host
        for (int i = 0; i < m_count; i++) {
            insertToStream(stream, rowCount, m_batch, m_client, data);
            rowCount += m_batch;
        }
        m_client.drain();

        // Recover host 0 and reopen client
        m_cluster.recoverOne(0, null);
        Thread.sleep(500);
        m_client = getClient(m_cluster);

        // Insert more data into both hosts
        for (int i = 0; i < m_count; i++) {
            insertToStream(stream, rowCount, m_batch, m_client, data);
            rowCount += m_batch;
        }
        m_client.drain();

        // Kill host 1 and reopen client
        m_cluster.killSingleHost(1);
        Thread.sleep(500);
        m_client = getClient(m_cluster);

        // Insert more data into surviving host
        for (int i = 0; i < m_count; i++) {
            insertToStream(stream, rowCount, m_batch, m_client, data);
            rowCount += m_batch;
        }
        m_client.drain();

        // Recover host 1 and reopen client
        m_cluster.recoverOne(1, null);
        Thread.sleep(500);
        m_client = getClient(m_cluster);

        // Insert more data into both hosts
        for (int i = 0; i < m_count; i++) {
            insertToStream(stream, rowCount, m_batch, m_client, data);
            rowCount += m_batch;
        }
        m_client.drain();

        // Now enable the exports
        m_builder.clearExports();
        m_builder.addExport(true /* enabled */,
                ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                createSocketExportProperties(stream, false /* is replicated stream? */),
                "s1_target");
        m_cluster.updateCatalog(m_builder);

        // Wait for exports to drain
        m_verifier.waitForTuplesAndVerify(m_client);
    }

    @After
    public void tearDown() throws Exception {
        for (Entry<String, ServerListener> entry : m_serverSockets.entrySet()) {
            ServerListener serverSocket = entry.getValue();
            if (serverSocket != null) {
                serverSocket.closeClient();
                serverSocket.close();
            }
        }

        if (m_cluster != null) {
            try {
                m_cluster.shutDown();
            } catch(Exception ex) {}
        }

        if (m_client != null) {
            try {
                m_client.close();
            } catch(Exception ex) {}
        }
    }

}
