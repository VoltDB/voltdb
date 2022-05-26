/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

public class TestCatalogUpdateDuringSnapshot extends JUnit4LocalClusterTest {

    private static final String CREATE_TABLE = "CREATE TABLE A (ID BIGINT NOT NULL, VALUE VARCHAR);";
    private static final String DROP_TABLE = "DROP TABLE A IF EXISTS;";
    private static final String PARTITION_TABLE = "PARTITION TABLE A ON COLUMN ID;";
    private static final String CREATE_VIEW = "CREATE VIEW AV (VALUE, VALUE_COUNT) AS SELECT VALUE, COUNT(*) FROM A GROUP BY VALUE;";
    private static final String DROP_VIEW = "DROP VIEW AV;";

    @Rule
    public final TestName m_name = new TestName();

    @ClassRule
    public static final TemporaryFolder m_folder = new TemporaryFolder();

    @Rule
    public final TestWatcher m_cleanUpOnError = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            try {
                cleanup();
            } catch (Exception e1) {
                e1.printStackTrace();
            }
        };
    };

    private static LocalCluster m_cluster;
    private static Client m_client;

    @Before
    public void start() throws IOException {
        if (m_cluster == null) {
            m_cluster = new LocalCluster(m_name.getMethodName() + ".jar", 6, 2, 1, BackendTarget.NATIVE_EE_JNI);
            VoltProjectBuilder project = new VoltProjectBuilder();
            project.setUseDDLSchema(true);
            m_cluster.setCallingMethodName(m_name.getMethodName());
            m_cluster.setHasLocalServer(false);
            assertTrue(m_cluster.compile(project));
            m_cluster.startCluster();
            m_client = m_cluster.createClient(new ClientConfig());
        }
    }

    @AfterClass
    public static void cleanup() throws Exception {
        try {
            if (m_client != null) {
                m_client.close();
            }
        } finally {
            try {
                if (m_cluster != null) {
                    m_cluster.shutDown();
                }
            } finally {
                m_cluster = null;
                m_client = null;
            }
        }
    }

    @Test
    public void replicatedTableCreate() throws Exception {
        performTest(CREATE_TABLE, DROP_TABLE);
    }

    @Test
    public void partitionedTableCreate() throws Exception {
        performTest(CREATE_TABLE + PARTITION_TABLE, DROP_TABLE);
    }

    @Test
    public void partitionTable() throws Exception {
        performTest(CREATE_TABLE, PARTITION_TABLE, DROP_TABLE);
    }

    @Test
    public void dropReplicatedTable() throws Exception {
        performTest(CREATE_TABLE, DROP_TABLE, null);
    }

    @Test
    public void dropPartitionedTable() throws Exception {
        performTest(CREATE_TABLE + PARTITION_TABLE, DROP_TABLE, null);
    }

    @Test
    public void createReplicatedView() throws Exception {
        performTest(CREATE_TABLE, CREATE_VIEW, DROP_VIEW + DROP_TABLE);
    }

    @Test
    public void createPartitionedView() throws Exception {
        performTest(CREATE_TABLE + PARTITION_TABLE, CREATE_VIEW, DROP_VIEW + DROP_TABLE);
    }

    @Test
    public void dropReplicatedView() throws Exception {
        performTest(CREATE_TABLE + CREATE_VIEW, DROP_VIEW, DROP_TABLE);
    }

    @Test
    public void dropPartitionedView() throws Exception {
        performTest(CREATE_TABLE + PARTITION_TABLE + CREATE_VIEW, DROP_VIEW, DROP_TABLE);
    }

    private void performTest(String duringSnapshot, String cleanupDdl) throws Exception {
        performTest(null, duringSnapshot, cleanupDdl);
    }

    private void performTest(String setupDdl, String duringSnapshot, String cleanupDdl) throws Exception {
        if (setupDdl != null) {
            performDdlProcedure(setupDdl);
        }

        performSnapsthot();
        performDdlProcedure(duringSnapshot);
        waitForSnapshotToComplete();

        if (cleanupDdl != null) {
            performDdlProcedure(cleanupDdl);
        }
    }

    private void performSnapsthot() throws NoConnectionsException, IOException, ProcCallException {
        m_client.callProcedure("@SnapshotSave", m_folder.newFolder().getAbsolutePath(), m_name.getMethodName(), 0);
    }

    private void performDdlProcedure(String procedure) throws NoConnectionsException, IOException, ProcCallException {
        m_client.callProcedure("@AdHoc", procedure);
    }

    private void waitForSnapshotToComplete() throws Exception {
        int attempt = 0;
        do {
            VoltTable table = m_client.callProcedure("@Statistics", "SnapshotStatus", 0).getResults()[0];
            boolean completed = true;
            while (table.advanceRow()) {
                if (table.getLong("END_TIME") == 0) {
                    completed = false;
                    break;
                }
            }
            if (completed) {
                return;
            }
            if (++attempt > 240) {
                fail("Snapshot failed to complete in time: \n" + table);
            }
            Thread.sleep(250);
        } while (true);
    }
}
