/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import java.util.List;
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

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Lists;

/**
 * Test handling of dangling data sources
 */
public class TestExportDanglingSources extends ExportLocalClusterBase {
    private static final int SPH = 4;
    private static final int HOST_COUNT = 5;
    private static final int KFACTOR = 1;

    private static final int LOOP_COUNT = 10;
    private static final int ROW_BATCH = 10;

    private static final String STREAM_NAME = "stream_1";
    private static final String TARGET_NAME = "target_1";
    private static final String SCHEMA =
            "CREATE STREAM " + STREAM_NAME + " "
            + "PARTITION ON COLUMN a "
            + "EXPORT TO TARGET " + TARGET_NAME + " ("
            + "     a INTEGER NOT NULL, "
            + "     b INTEGER NOT NULL"
            + ");";

    private VoltProjectBuilder m_builder;
    private LocalCluster m_cluster;
    private Client m_client;
    private String m_logPattern = "Drained source for";

    @Rule
    public final TestName m_name = new TestName();

    @Before
    public void setUp() throws Exception {
        m_builder = new VoltProjectBuilder();
        m_builder.addLiteralSchema(SCHEMA);
        m_builder.addExport(false /* initially disabled */,
                ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                createSocketExportProperties(STREAM_NAME, false /* is replicated stream? */),
                TARGET_NAME);
        m_builder.setUseDDLSchema(true);

        m_cluster = new LocalCluster("testexport.jar", SPH, HOST_COUNT, KFACTOR, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setHasLocalServer(false);
        m_cluster.overrideAnyRequestForValgrind();
        assertTrue(m_cluster.compile(m_builder));

        // Set up log message pattern
        List<String> patterns = Lists.newArrayList();
        patterns.add(m_logPattern);
        m_cluster.setLogSearchPatterns(patterns);

        // Start cluster and socket listener
        m_cluster.startUp(true);
        startListener();

        // Create an exact verifier
        m_verifier = new ExportTestExpectedData(m_serverSockets,
                false  /*is replicated stream? */,
                true, /* exact verification */
                KFACTOR + 1);
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

    @Test(timeout = 60_000)
    public void testExportDanglingSources() throws Exception {

        int rowCount = 0;

        // MUST CALL getClient because verifier requires
        // the hashinator to be initialized
        m_client = getClient(m_cluster);

        //add data to all stream tables
        Object[] data = new Object[] {1, 1, 1};

        for (int i = 0; i < LOOP_COUNT; i++) {
            insertToStream(STREAM_NAME, rowCount, ROW_BATCH, m_client, data);
            rowCount += ROW_BATCH;
        }
        m_client.drain();

        // Kill 2 hosts, hopefully from different partition groups
        // Host 0 expected to be in bigger partition group
        m_cluster.killSingleHost(0);
        // Host 1 expected to be in smaller partition group
        m_cluster.killSingleHost(1);
        Thread.sleep(500);

        // Insert rows with hosts down
        m_client = getClient(m_cluster);
        for (int i = 0; i < LOOP_COUNT; i++) {
            insertToStream(STREAM_NAME, rowCount, ROW_BATCH, m_client, data);
            rowCount += ROW_BATCH;
        }
        m_client.drain();

        // Rejoin (re-init) host 1, should join larger partition group
        m_cluster.rejoinOne(1);
        Thread.sleep(500);

        // Recover the other
        m_cluster.recoverOne(0,  null, "");
        Thread.sleep(500);

        // Insert rows with hosts up
        m_client = getClient(m_cluster);
        for (int i = 0; i < LOOP_COUNT; i++) {
            insertToStream(STREAM_NAME, rowCount, ROW_BATCH, m_client, data);
            rowCount += ROW_BATCH;
        }
        m_client.drain();

        // Enable export
        m_builder.addExport(true /* enable */,
                ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                createSocketExportProperties(STREAM_NAME, false /* is replicated stream? */),
                TARGET_NAME);
        m_cluster.updateCatalog(m_builder);

        // Wait for export to drain
        List<String> list = ImmutableList.of(STREAM_NAME);
        TestExportBaseSocketExport.waitForExportAllRowsDelivered(m_client, list);
        m_verifier.verifyRows();

        // Verify at least one host had to drain a data source
        assertTrue(m_cluster.anyHostHasLogMessage(m_logPattern));
    }

}
