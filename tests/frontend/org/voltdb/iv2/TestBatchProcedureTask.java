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
package org.voltdb.iv2;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.voltdb.AuthSystem;
import org.voltdb.BackendTarget;
import org.voltdb.ClientInterface;
import org.voltdb.ClientResponseImpl;
import org.voltdb.SQLStmt;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.test.utils.RandomTestRule;

import com.google_voltpatches.common.collect.ComparisonChain;

public class TestBatchProcedureTask extends JUnit4LocalClusterTest {
    private static final VoltTable s_tableTemplate = new VoltTable(new VoltTable.ColumnInfo("key", VoltType.BIGINT),
            new VoltTable.ColumnInfo("value1", VoltType.INTEGER), new VoltTable.ColumnInfo("value2", VoltType.STRING));
    private static final String s_tableName = TestBatchProcedureTask.class.getSimpleName();
    private static final String s_tableDefaultProc = s_tableName.toUpperCase() + ".insert";

    private static LocalCluster s_cluster;
    private static Client s_client;

    private ClientInterface m_ci;
    private AuthSystem.AuthUser m_user;

    @Rule
    public final RandomTestRule m_random = new RandomTestRule();

    @Rule
    public final TestWatcher m_cleanUpOnError = new TestWatcher() {
        @Override
        protected void failed(Throwable e, Description description) {
            if (s_cluster != null) {
                try {
                    if (s_client != null) {
                        s_client.close();
                    }
                    s_cluster.shutDown();
                } catch (InterruptedException e1) {
                    throw new RuntimeException(e1);
                }
                s_cluster = null;
            }
        };
    };

    @Before
    public void setup() throws IOException, ProcCallException {
        if (s_cluster == null) {
            s_cluster = new LocalCluster("test.jar", 4, 1, 0, BackendTarget.NATIVE_EE_JNI);
            s_cluster.setHasLocalServer(true);
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.setUseDDLSchema(true);
            assertTrue(s_cluster.compile(builder));
            s_cluster.startCluster();

            s_client = s_cluster.createClient();
        }

        m_ci = VoltDB.instance().getClientInterface();
        m_user = VoltDB.instance().getCatalogContext().authSystem.getInternalAdminUser();

        s_client.callProcedure("@AdHoc",
                MessageFormat.format(
                        "CREATE TABLE {0} (key BIGINT NOT NULL, value1 INTEGER, value2 VARCHAR(4096));\n"
                                + "PARTITION TABLE {0} ON COLUMN key;\n"
                                + "CREATE PROCEDURE FROM CLASS {1};",
                        s_tableName, MpInsertion.class.getName()));
    }

    @After
    public void tearDown() throws NoConnectionsException, IOException, ProcCallException {
        s_client.callProcedure("@AdHoc", MessageFormat.format("DROP PROCEDURE {0};\n"
                + "DROP TABLE {1} IF EXISTS", MpInsertion.class.getName(), s_tableName));
    }

    @AfterClass
    public static void tearDownCluster() throws InterruptedException {
        if (s_client != null) {
            s_client.close();
        }
        if (s_cluster != null) {
            s_cluster.shutDown();
        }
    }

    @Test
    public void basicSpBatchExecution() throws Exception {
        VoltTable params = s_tableTemplate.clone(16 * 1024 * 1024);
        Long keyValue = m_random.nextLong();
        NavigableMap<Object, Object[]> rows = new TreeMap<>();
        for (int i = 0; i < 100; ++i) {
            Object[] row = { keyValue, m_random.nextValue(VoltType.INTEGER, 0), m_random.nextValue(VoltType.STRING) };
            rows.put(row[1], row);
            params.addRow(row);
        }

        ClientResponseImpl response = callBatchProcedure(s_tableDefaultProc, keyValue, params);

        assertEquals(response.toJSONString(), ClientResponse.SUCCESS, response.getStatus());
        assertEquals(rows.size(), response.getResults()[0].asScalarLong());

        VoltTable result = s_client.callProcedure("@AdHoc", "SELECT * FROM " + s_tableName + " ORDER BY value1")
                .getResults()[0];
        Iterator<Object[]> iter = rows.values().iterator();
        while (result.advanceRow()) {
            assertArrayEquals(iter.next(), result.getRowObjects());
        }

        assertFalse(iter.hasNext());
    }

    @Test
    public void mispartitionedParameterInSpBatch() throws Exception {
        VoltTable params = s_tableTemplate.clone(16 * 1024 * 1024);
        Long keyValue = m_random.nextLong();
        for (int i = 0; i < 100; ++i) {
            params.addRow(keyValue, m_random.nextValue(VoltType.INTEGER), m_random.nextValue(VoltType.STRING));
        }

        int currentPartition = TheHashinator.getPartitionForParameter(VoltType.BIGINT, keyValue);
        long mispartitionedKey;
        for (mispartitionedKey = 0;; ++mispartitionedKey) {
            if (TheHashinator.getPartitionForParameter(VoltType.BIGINT, mispartitionedKey) != currentPartition) {
                params.addRow(mispartitionedKey, m_random.nextValue(VoltType.INTEGER),
                        m_random.nextValue(VoltType.STRING));
                break;
            }
        }

        ClientResponseImpl response = callBatchProcedure(s_tableDefaultProc, keyValue, params);
        assertEquals(response.toJSONString(), ClientResponse.GRACEFUL_FAILURE, response.getStatus());
        assertEquals(response.toJSONString(), ClientResponse.TXN_MISPARTITIONED, response.getAppStatus());
        assertEquals(0,
                s_client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + s_tableName).getResults()[0].asScalarLong());
    }

    @Test
    public void badRowAbortsSpTransaction() throws Exception {
        VoltTable params = s_tableTemplate.clone(16 * 1024 * 1024);
        Long keyValue = m_random.nextLong();
        for (int i = 0; i < 100; ++i) {
            params.addRow(keyValue, m_random.nextValue(VoltType.INTEGER), m_random.nextValue(VoltType.STRING));
        }

        params.addRow(null, m_random.nextValue(VoltType.INTEGER), m_random.nextValue(VoltType.STRING));

        ClientResponseImpl response = callBatchProcedure(s_tableDefaultProc, keyValue, params);
        assertEquals(response.toJSONString(), ClientResponse.GRACEFUL_FAILURE, response.getStatus());
        assertEquals(0,
                s_client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + s_tableName).getResults()[0].asScalarLong());
    }

    @Test
    public void basicMpBatchExecution() throws Exception {
        VoltTable params = s_tableTemplate.clone(16 * 1024 * 1024);
        Long keyValue = m_random.nextLong();
        NavigableMap<SortKey, Object[]> rows = new TreeMap<>();
        for (int i = 0; i < 50; ++i) {
            Object[] row = { keyValue, m_random.nextValue(VoltType.INTEGER, 0), m_random.nextValue(VoltType.STRING) };
            params.addRow(row);
            for (int j = 0; j < 10; ++j) {
                long newKey = keyValue + (long) Math.pow(2, j);
                row[0] = newKey;
                rows.put(new SortKey(row[1], newKey), row.clone());
            }
        }

        ClientResponseImpl response = callBatchProcedure(MpInsertion.NAME, keyValue, params);

        assertEquals(response.toJSONString(), ClientResponse.SUCCESS, response.getStatus());
        assertEquals(rows.size(), response.getResults()[0].asScalarLong());

        VoltTable result = s_client.callProcedure("@AdHoc", "SELECT * FROM " + s_tableName + " ORDER BY value1, key")
                .getResults()[0];
        Iterator<Object[]> iter = rows.values().iterator();
        while (result.advanceRow()) {
            assertArrayEquals("Row " + result.getActiveRowIndex(), iter.next(), result.getRowObjects());
        }

        assertFalse(iter.hasNext());
    }

    @Test
    public void badRowAbortsMpTransaction() throws Exception {
        VoltTable params = s_tableTemplate.clone(16 * 1024 * 1024);
        Long keyValue = m_random.nextLong();
        for (int i = 0; i < 100; ++i) {
            params.addRow(keyValue, m_random.nextValue(VoltType.INTEGER), m_random.nextValue(VoltType.STRING));
        }

        params.addRow(null, m_random.nextValue(VoltType.INTEGER), m_random.nextValue(VoltType.STRING));

        ClientResponseImpl response = callBatchProcedure(MpInsertion.NAME, keyValue, params);
        assertEquals(response.toJSONString(), ClientResponse.GRACEFUL_FAILURE, response.getStatus());
        assertEquals(0,
                s_client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM " + s_tableName).getResults()[0].asScalarLong());
    }

    private ClientResponseImpl callBatchProcedure(String procName, Long partitionParameter, VoltTable params)
            throws InterruptedException, ExecutionException {
        Procedure procedure = m_ci.getProcedureFromName(procName);

        StoredProcedureInvocation spi = new StoredProcedureInvocation();
        spi.setProcName(procName);
        spi.setBatchCall(params);
        if (procedure.getSinglepartition()) {
            spi.setPartitionDestination(TheHashinator.getPartitionForParameter(VoltType.BIGINT, partitionParameter));
        }

        CompletableFuture<ClientResponse> future = new CompletableFuture<>();

        assertTrue(m_ci.getInternalConnectionHandler().callProcedure("", m_user, false, spi, procedure,
                future::complete, false, null));

        return (ClientResponseImpl) future.get();
    }

    public static class MpInsertion extends VoltProcedure {
        static final String NAME = MpInsertion.class.getName()
                .substring(MpInsertion.class.getName().lastIndexOf('.') + 1);
        private static final SQLStmt s_insert = new SQLStmt("INSERT INTO " + s_tableName + " VALUES (?, ?, ?);");

        public long run(Long key, Integer value1, String value2) {
            for (int i = 0; i < 5; ++i) {
                voltQueueSQL(s_insert, key == null ? null : key + (long) Math.pow(2, i), value1, value2);
            }
            voltExecuteSQL();

            for (int i = 5; i < 10; ++i) {
                voltQueueSQL(s_insert, key == null ? null : key + (long) Math.pow(2, i), value1, value2);
            }
            voltExecuteSQL(true);
            return 10;
        }
    }

    private static class SortKey implements Comparable<SortKey>{
        private final int m_value1;
        private final long m_value2;

        SortKey(Object value1, long value2) {
            m_value1 = (int) value1;
            m_value2 = value2;
        }

        @Override
        public int compareTo(SortKey o) {
            return ComparisonChain.start().compare(m_value1, o.m_value1).compare(m_value2, o.m_value2).result();
        }

        @Override
        public String toString() {
            return "SortKey [m_value1=" + m_value1 + ", m_value2=" + m_value2 + "]";
        }
    }
}
