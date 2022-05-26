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

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
import org.voltdb.utils.MiscUtils;

/**
 * @author rdykiel
 *
 */
public class TestMultiStreamPolling extends ExportLocalClusterBase {

    private static final int SPH = 4;
    private static final int HOST_COUNT = 2;
    private static final int KFACTOR = 1;

    private static final int STREAM_COUNT_COMMUNITY = 2;
    private static final int STREAM_COUNT_PRO = 10;
    private static final int LOOP_COUNT = 100;
    private static final int ROW_BATCH = 100;

    private static final String STREAM_TEMPLATE = "stream_%s";
    private static final String TARGET_TEMPLATE = "target_%s";
    private static final String SCHEMA_TEMPLATE =
            "CREATE STREAM %s "
            + "PARTITION ON COLUMN a "
            + "EXPORT TO TARGET %s ("
            + "     a INTEGER NOT NULL, "
            + "     b INTEGER NOT NULL"
            + ");";

    private static final String ONE_TARGET_NAME = "one_target";

    private VoltProjectBuilder m_builder;
    private LocalCluster m_cluster;
    private Client m_client;
    private int m_streamCount;

    @Rule
    public final TestName m_name = new TestName();

    @Before
    public void setUp() throws Exception {
        m_streamCount = MiscUtils.isPro() ? STREAM_COUNT_PRO : STREAM_COUNT_COMMUNITY;
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

    /**
     * Setup multiple streams, each with its own export target
     *
     * @param streamCount
     * @throws IOException
     */
    private void setupMultipleParallelStreams(int streamCount) throws IOException {

        m_builder = new VoltProjectBuilder();

        for (int i = 0; i < streamCount; i++) {
            String streamName = String.format(STREAM_TEMPLATE, i);
            String targetName = String.format(TARGET_TEMPLATE, i);

            m_builder.addLiteralSchema(String.format(SCHEMA_TEMPLATE, streamName, targetName));
            m_builder.addExport(true /* enabled */,
                    ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                    createSocketExportProperties(streamName, false /* is replicated stream? */),
                    targetName);
        }
        m_builder.setUseDDLSchema(true);

        m_cluster = new LocalCluster("testexport.jar", SPH, HOST_COUNT, KFACTOR, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setHasLocalServer(false);
        m_cluster.overrideAnyRequestForValgrind();
        assertTrue(m_cluster.compile(m_builder));

        // Start cluster and socket listener
        m_cluster.startUp(true);
        startListener();

        // Create an exact verifier
        m_verifier = new ExportTestExpectedData(m_serverSockets,
                false  /*is replicated stream? */,
                true, /* exact verification */
                KFACTOR + 1);
    }

    /**
     * Setup multiple streams exporting to 1 target, return stream name to check
     *
     * @param streamCount
     * @param targetName
     * @return the stream name to check
     * @throws IOException
     */
    private String setupMultipleStreamsToOneTarget(int streamCount, String targetName) throws IOException {

        String ret = null;
        m_builder = new VoltProjectBuilder();

        // Create multiple streams exporting to one target
        for (int i = 0; i < streamCount; i++) {
            String streamName = String.format(STREAM_TEMPLATE, i);

            if (i == 1) {
                // Create just one export, use just one stream for verification mapping
                m_builder.addExport(true /* enabled */,
                        ServerExportEnum.CUSTOM, "org.voltdb.exportclient.SocketExporter",
                        createSocketExportProperties(streamName, false /* is replicated stream? */),
                        targetName);
                ret = streamName;
            }
            m_builder.addLiteralSchema(String.format(SCHEMA_TEMPLATE, streamName, targetName));
        }

        m_builder.setUseDDLSchema(true);

        m_cluster = new LocalCluster("testexport.jar", SPH, HOST_COUNT, KFACTOR, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setHasLocalServer(false);
        m_cluster.overrideAnyRequestForValgrind();
        assertTrue(m_cluster.compile(m_builder));

        // Start cluster and socket listener
        m_cluster.startUp(true);
        startListener();

        // Create an exact verifier but don't attempt to verify the sequence numbers since
        // all streams go thru the same export
        m_verifier = new ExportTestExpectedData(m_serverSockets,
                false  /*is replicated stream? */,
                true, /* exact verification */
                KFACTOR + 1);
        m_verifier.m_verifySequenceNumber = false;
        return ret;
    }

    @Test(timeout = 600_000)
    public void testPollMultipleParallelStreams() throws Exception {

        setupMultipleParallelStreams(m_streamCount);
        int rowCount = 0;

        // MUST CALL getClient because verifier requires
        // the hashinator to be initialized
        m_client = getClient(m_cluster);

        //add data to all stream tables
        Object[] data = new Object[3];
        Arrays.fill(data, 1);

        for (int i = 0; i < LOOP_COUNT; i++) {
            for (int j = 0; j < m_streamCount; j++) {
                String streamName = String.format(STREAM_TEMPLATE, j);
                insertToStream(streamName, rowCount, ROW_BATCH, m_client, data);
            }
            rowCount += ROW_BATCH;
        }

        // Wait for exports to drain
        m_verifier.waitForTuplesAndVerify(m_client);
    }

    @Test(timeout = 600_000)
    public void testPollMultipleStreamsToOneTarget() throws Exception {

        String checkStream = setupMultipleStreamsToOneTarget(m_streamCount, ONE_TARGET_NAME);
        int rowCount = 0;

        // MUST CALL getClient because verifier requires
        // the hashinator to be initialized
        m_client = getClient(m_cluster);

        //add data to all stream tables
        Object[] data = new Object[3];
        Arrays.fill(data, 1);

        // Insert rows on different streams but since they all go thru one export
        // track them thru the checkStream
        Map<String, Long> counts = new HashMap<>();
        for (int i = 0; i < LOOP_COUNT; i++) {
            for (int j = 0; j < m_streamCount; j++) {
                String streamName = String.format(STREAM_TEMPLATE, j);
                for (int k = rowCount; k < rowCount + ROW_BATCH; k++) {
                    data[1] = k; // Pkey column
                    m_verifier.addRow(m_client, checkStream, k, data);
                    m_client.callProcedure("@AdHoc", "insert into "+ streamName + " values(" + k + ", 1)");
                }
                rowCount += ROW_BATCH;
                counts.compute(streamName, (n, c) -> (c == null ? 0 : c.longValue()) + ROW_BATCH);
            }
        }

        // Wait for exports to drain
        // Cannot use wait on verifier because verifier is tracking the targets not the streams
        TestExportBaseSocketExport.waitForExportRowsToBeDelivered(m_client, counts);
        m_verifier.verifyRows();
    }
}
