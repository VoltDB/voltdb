/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
 * terms and conditions:
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
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.voltdb.regressionsuites;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.URL;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestWindowFunctionsWithIndexes extends RegressionSuite {
    private final static boolean IS_ENABLED = true;
    private final static boolean ISNOT_ENABLED = false;

    public TestWindowFunctionsWithIndexes(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }
    static private void setupSchema(VoltProjectBuilder project) throws IOException {
        URL url = TestWindowFunctionsWithIndexes.class.getResource("testwindowfunctionswithindexes-ddl.sql");
        project.addSchema(url);
        project.setUseDDLSchema(true);
    }

    /**
     * Execute a sql query and return the result.
     *
     * @param client
     * @param SQL
     * @return
     * @throws IOException
     * @throws NoConnectionsException
     * @throws ProcCallException
     */
    private void validateQuery(Client client,
                               String SQL,
                               String plainTable,
                               String indexedTable) throws IOException, NoConnectionsException, ProcCallException {
        ClientResponse cr;
        VoltTable plainResults;
        VoltTable indexedResults;
        String pSQL = String.format(SQL, plainTable);
        String iSQL = String.format(SQL, indexedTable);
        cr = client.callProcedure("@AdHoc", pSQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        plainResults = cr.getResults()[0];
        cr = client.callProcedure("@AdHoc", iSQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        indexedResults = cr.getResults()[0];
        assertTablesAreEqual("Expected equal tables", plainResults, indexedResults);
    }

    private void validateQuery(Client client, String string, Object[][] o4) throws Exception {
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", string);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        assertApproximateContentOfTable(o4, vt, 1.0e-4);
    }
    static private Object [][] m_O4;
    static private Object [][] m_P_DECIMAL;
    static private Object [][] m_P_DECIMAL_OUTPUT;
    /**
     * Initialize a table with some values.
     *
     * @param client
     * @param tableName
     * @param tableContents
     * @throws Exception
     */
    private void initTable(Client     client,
                           String     tableName,
                           Object     tableContents[][]) throws Exception {
        String insertCmd = tableName + ".insert";
        Object tmp[][] = shuffleArray(tableContents);
        for (int idx = 0; idx < tableContents.length; idx += 1) {
            client.callProcedure(insertCmd, tableContents[idx]);
        }
    }


    private void initTables(Client client) throws Exception {
        final int NROWS = 64;
        Integer table[][] = new Integer[NROWS][];
        for (int idx = 0; idx < NROWS; idx += 1) {
            table[idx] = new Integer[3];
            table[idx][0] = 100 + idx;
            table[idx][1] = 200 + idx;
            table[idx][2] = 300 + idx;
        }

        Integer output[][];
        output = shuffleArray(table);
        initTable(client,
                  "vanilla",
                  output);
        initTable(client,
                  "vanilla_idx",
                  output);
        output = shuffleArray(table);
        initTable(client,
                  "vanilla_pa",
                  output);
        initTable(client,
                  "vanilla_pa_idx",
                  output);
        output = shuffleArray(table);
        initTable(client,
                  "vanilla_pb",
                  output);
        initTable(client,
                  "vanilla_pb_idx",
                  output);
        // Initialize O4, which has a different
        // schema.
        m_O4 = new Object[NROWS][];
        for (int idx = 0; idx < NROWS; idx += 1) {
            m_O4[idx] = new Object[2];
            m_O4[idx][0] = Long.valueOf(idx);
            m_O4[idx][1] = Long.valueOf(idx+100);
        }
        initTable(client,
                  "O4",
                  m_O4);

        // Initialize P_DECIMAL.
        m_P_DECIMAL_OUTPUT = new Object[][] {
            {0, new BigDecimal(43.7466723728), null, 0.0167254440373, 8, 1},
            {1, new BigDecimal(43.7466723728), null, 0.655978114997, 7, 2},
            {2, new BigDecimal(43.7466723728), new BigDecimal(74.1274731896), 0.503481924952, 6, 3},
            {3, new BigDecimal(43.7466723728), new BigDecimal(74.1274731896), 0.970442363316, 5, 4},
            {4, new BigDecimal(5.36338386576), null, 0.606293209075, 4, 5},
            {5, new BigDecimal(5.36338386576), null, 0.719625466141, 3, 6},
            {6, new BigDecimal(5.36338386576), new BigDecimal(59.9097237768), 0.175559752822, 2, 7},
            {7, new BigDecimal(5.36338386576), new BigDecimal(59.9097237768), 0.405504260609, 1, 8}
        };
        m_P_DECIMAL = new Object[][] {
            {0, new BigDecimal(43.7466723728), null, 0.0167254440373},
            {1, new BigDecimal(43.7466723728), null, 0.655978114997},
            {2, new BigDecimal(43.7466723728), new BigDecimal(74.1274731896), 0.503481924952},
            {3, new BigDecimal(43.7466723728), new BigDecimal(74.1274731896), 0.970442363316},
            {4, new BigDecimal(5.36338386576), null, 0.606293209075},
            {5, new BigDecimal(5.36338386576), null, 0.719625466141},
            {6, new BigDecimal(5.36338386576), new BigDecimal(59.9097237768), 0.175559752822},
            {7, new BigDecimal(5.36338386576), new BigDecimal(59.9097237768), 0.405504260609}
        };
        client.callProcedure("P_DECIMAL.insert", m_P_DECIMAL[0]);
        client.callProcedure("P_DECIMAL.insert", m_P_DECIMAL[1]);
        client.callProcedure("P_DECIMAL.insert", m_P_DECIMAL[2]);
        client.callProcedure("P_DECIMAL.insert", m_P_DECIMAL[3]);
        client.callProcedure("P_DECIMAL.insert", m_P_DECIMAL[4]);
        client.callProcedure("P_DECIMAL.insert", m_P_DECIMAL[5]);
        client.callProcedure("P_DECIMAL.insert", m_P_DECIMAL[6]);
        client.callProcedure("P_DECIMAL.insert", m_P_DECIMAL[7]);
    }

    public void testAll() throws Exception {
        Client client = getClient();
        // Since we are only doing queries here, we
        // don't need to truncate and reload the tables
        // between tests.
        initTables(client);
        //  1: No SLOB, No WF, SP Query, noindex
        //     Expect SeqScan
        //     select * from vanilla;
        // Note: This works, but only because the EE preserves
        //       the order of insertion, and we insert into
        //       vanilla and vanilla_idx in the same order.
        //       This is really not testing much, so it's
        //       disabled.
        if (ISNOT_ENABLED) {
            validateQuery(client,
                          "select * from %s",
                          "vanilla",
                          "vanilla_idx");
        }
        //  2: No SLOB, No WF, MP Query, noindex
        //     Expect RECV -> SEND -> SeqScan
        //.    select * from vanilla_pa;
        // This is not enabled because there is no way the
        // indexed and nonindexed scan will be ordered the
        // same way.
        if (ISNOT_ENABLED) {
            validateQuery(client,
                          "select * from %s", "vanilla_pa", "vanilla_pa_idx");
        }

        //  3: No SLOB, No WF, SP Query, index(NONEIndex)
        //     Expect IndxScan
        //     select * from vanilla_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1",
                          "vanilla",
                          "vanilla_idx");
        }

        // -- Force us to use the index on column vanilla_pb_idx.a
        // -- which in this case is not the partition column.
        //  4: No SLOB, No WF, MP Query, index(NONEIndex)
        //     Expect RECV -> SEND -> IndxScan
        //     select * from vanilla_pb_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1", "vanilla", "vanilla_pb_idx");
        }
        //  5: No SLOB, One WF, SP Query, noindex
        //     Expect WinFun -> OrderBy -> SeqScan
        //     select a, b, max(b) over ( partition by a ) from vanilla;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s;",
                          "vanilla",
                          "vanilla_idx");
        }
        //  6: No SLOB, One WF, MP Query, noindex
        //     Expect WinFun -> OrderBy -> RECV -> SEND -> SeqScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_pa;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s;",
                          "vanilla",
                          "vanilla_pa_idx");
        }
        //  7: No SLOB, one WF, SP Query, index (Can order the WF)
        //     Expect WinFun -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1;",
                          "vanilla",
                          "vanilla_idx");
        }
        //  7a: No SLOB, one WF, SP Query, index (Only to order the WF)
        //     Expect WinFun -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_idx;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s;",
                          "vanilla",
                          "vanilla_idx");
        }
        //  8: No SLOB, one WF, MP Query, index (Can order the WF)
        //     Expect WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        //  8a: No SLOB, one WF, MP Query, index (Only to order the WF)
        //     Expect WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_pb_idx;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        //  9: No SLOB, one WF, SP Query, index (not for the WF)
        //     Expect WinFun -> OrderBy -> IndxScan
        //     select a, b, max(b) over ( partition by b ) from vanilla_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s where a = 1;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 10: No SLOB, one WF, MP Query, index (not for the WF)
        //     Expect WinFun -> OrderBy -> RECV -> SEND -> IndxScan
        //     select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s where a = 1;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // 11: SLOB, No WF, SP Query, noindex
        //     Expect OrderBy(SLOB) -> SeqScan
        //     select * from vanilla order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 12: SLOB, No WF, MP Query, noindex
        //     Expect OrderBy(SLOB) -> RECV -> SEND -> SeqScan
        //     select * from vanilla_pa order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s order by b;",
                          "vanilla_pa",
                          "vanilla_pa_idx");
        }
        // 13: SLOB, No WF, SP Query, index (Can order the SLOB)
        //     Expect PlanNodeType.INDEXSCAN
        //     select * from vanilla_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1 order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 13a: SLOB, No WF, SP Query, index (only to order the SLOB)
        //     Expect PlanNodeType.INDEXSCAN
        //     select * from vanilla_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 14: SLOB, No WF, MP Query, index (Can order the SLOB)
        //     Expect MrgRecv(SLOB) -> SEND -> IndxScan
        //     select * from vanilla_pb_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s order by a;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // 14a: SLOB, No WF, MP Query, index (Only to order the SLOB)
        //     Expect MrgRecv(SLOB) -> SEND -> IndxScan
        //     select * from vanilla_pb_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1 order by a;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // 15: SLOB, No WF, SP Query, index (Cannot order the SLOB)
        //     Expect OrderBy(SLOB) -> IndxScan
        //     select * from vanilla_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1 order by b;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 16: SLOB, No WF, MP Query, index (Cannot order the SLOB)
        //     Expect OrderBy(SLOB) -> RECV -> SEND -> IndxScan
        //     select * from vanilla_pb_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1 order by b;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // 17: SLOB, One WF, SP Query, index (Cannot order SLOB or WF)
        //     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> IndxScan
        //     select a, b, max(b) over (partition by b) from vanilla_idx where a = 1 order by c;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over (partition by b) from %s where a = 1 order by c;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 18: SLOB, One WF, MP Query, index (Cannot order SLOB or WF)
        //     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> RECV -> SEND -> IndxScan
        //     select a, b, max(b) over ( partition by c ) from vanilla_pb_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by c ) from %s where a = 1 order by b;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // 19: SLOB, one WF, SP Query, index (Can order the WF, Cannot order the SLOB)
        //     Expect OrderBy(SLOB) -> WinFun -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1 order by b;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 19a: SLOB, one WF, SP Query, index (Only to order the WF, not SLOB)
        //     Expect OrderBy(SLOB) -> WinFun -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_idx order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s order by b;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 20: SLOB, one WF, MP Query, index (Can order the WF, not SLOB)
        //     Expect OrderBy(SLOB) -> WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1 order by b;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // 20a: SLOB, one WF, MP Query, index (Can order the WF, not SLOB)
        //     Expect OrderBy(SLOB) -> WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_pb_idx order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s order by b;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // 21: SLOB, one WF, SP Query, index (Can order the SLOB, not WF)
        //     The index is not usable for the SLOB, since the WF invalidates the order.
        //     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> IndxScan
        //     select a, b, max(b) over ( partition by b ) from vanilla_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s where a = 1 order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 21a: SLOB, one WF, SP Query, index (Can order the SLOB, not WF)
        //     The index is unusable for the SLOB, since the WF invalidates the order.
        //     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> SeqScan
        //     select a, b, max(b) over ( partition by b ) from vanilla_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 22: SLOB, one WF, MP Query, index (Can order the SLOB, not WF)
        //     The index is unusable by the SLOB since the WF invalidates it.
        //     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> RECV -> SEND -> IndxScan
        //     select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s where a = 1 order by a;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // 22a: SLOB, one WF, MP Query, index (Can order the SLOB, not WF)
        //     The index is unusable by the SLOB since the WF invalidates it.
        //     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> RECV -> SEND -> SeqScan
        //     select a, b, max(b) over ( partition by b ) from vanilla_pb_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s order by a;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // 23: SLOB, one WF, SP Query, index (Can order the WF and SLOB both)
        //     Expect WinFun -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1 order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 23a: SLOB, one WF, SP Query, index (Can order the WF and SLOB both)
        //     Expect WinFun -> IndxScan
        //     select a, b, max(b) over ( partition by a ) from vanilla_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select max(b) over ( partition by a ) from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // 24: SLOB, one WF, MP Query, index (For the WF and SLOB both)
        //     Expect WinFun -> MrgRecv(SLOB or WF) -> SEND -> IndxScan
        //     select max(b) over ( partition by a ) from vanilla_pb_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select max(b) over ( partition by a ) from %s where a = 1 order by a;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }

        // This is one of the queries from the regression test.
        // It is here because it tests that the window function
        // and order by function have the same expressions but
        // different sort directions.
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, rank() over (order by a desc) from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
            validateQuery(client,
                          "select a, rank() over (order by a) from %s order by a desc;",
                          "vanilla",
                          "vanilla_idx");

            // These are like the last one, but the window function
            // and order by have the same orders.
            validateQuery(client,
                          "select a, rank() over (order by a) from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
            validateQuery(client,
                          "select a, rank() over (order by a desc) from %s order by a desc;",
                          "vanilla",
                          "vanilla_idx");
        }
        if (IS_ENABLED) {
            // Test that similar indexes don't cause
            // problems.  There is an index on CTR + 100,
            // but no index on CTR + 200.  We represent these
            // in a very similar way in the planner, and we
            // want to test that we can choose the right one
            // here.
            validateQuery(client, "select * from O4 where CTR + 100 < 1000 order by id", m_O4);
            validateQuery(client, "select * from O4 where CTR + 200 < 1000 order by id", m_O4);
        }

        if (IS_ENABLED) {
            String SQL = "SELECT *, RANK() OVER ( ORDER BY ID ) FUNC FROM (SELECT *, RANK() OVER ( ORDER BY ID DESC ) SUBFUNC FROM P_DECIMAL W12) SUB";
            validateQuery(client, SQL, m_P_DECIMAL_OUTPUT);
        }

    }



    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestWindowFunctionsWithIndexes.class);
        boolean success = false;

        VoltProjectBuilder project;
        try {
            project = new VoltProjectBuilder();
            config = new LocalCluster("test-window-function-with-indexes.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);

            project = new VoltProjectBuilder();
            config = new LocalCluster("test-window-functions-with-indexes.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);
        }
        catch (IOException excp) {
            fail();
        }

        return builder;
    }

}
