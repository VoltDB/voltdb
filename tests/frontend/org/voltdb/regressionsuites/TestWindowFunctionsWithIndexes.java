/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
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
 */package org.voltdb.regressionsuites;

import java.io.IOException;
import java.net.URL;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
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
        // Object tmp[][] = shuffleArray(tableContents);
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
    }

    public void testAll() throws Exception {
        Client client = getClient();
        // Since we are only doing queries here, we
        // don't need to truncate and reload the tables
        // between tests.
        initTables(client);
        // echo  1: No SLOB, No WF, SP Query, noindex
        // echo     Expect SeqScan
        // explain select * from vanilla;
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
        // echo  2: No SLOB, No WF, MP Query, noindex
        // echo     Expect RECV -> SEND -> SeqScan
        // This is not enabled because there is no way the
        // indexed and nonindexed scan will be ordered the
        // same way.
        if (ISNOT_ENABLED) {
            validateQuery(client,
                          "select * from %s", "vanilla_pa", "vanilla_pa_idx");
        }

        // echo  3: No SLOB, No WF, SP Query, index(NONEIndex)
        // echo     Expect IndxScan
        // explain select * from vanilla_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1",
                          "vanilla",
                          "vanilla_idx");
        }

        // -- Force us to use the index on column vanilla_pb_idx.a
        // -- which in this case is not the partition column.
        // echo  4: No SLOB, No WF, MP Query, index(NONEIndex)
        // echo     Expect RECV -> SEND -> IndxScan
        // explain select * from vanilla_pb_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1", "vanilla", "vanilla_pb_idx");
        }
        // echo  5: No SLOB, One WF, SP Query, noindex
        // echo     Expect WinFun -> OrderBy -> SeqScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo  6: No SLOB, One WF, MP Query, noindex
        // echo     Expect WinFun -> OrderBy -> RECV -> SEND -> SeqScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_pa;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s;",
                          "vanilla",
                          "vanilla_pa_idx");
        }
        // echo  7: No SLOB, one WF, SP Query, index (Can order the WF)
        // echo     Expect WinFun -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo  7a: No SLOB, one WF, SP Query, index (Only to order the WF)
        // echo     Expect WinFun -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_idx;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo  8: No SLOB, one WF, MP Query, index (Can order the WF)
        // echo     Expect WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo  8a: No SLOB, one WF, MP Query, index (Only to order the WF)
        // echo     Expect WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_pb_idx;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo  9: No SLOB, one WF, SP Query, index (not for the WF)
        // echo     Expect WinFun -> OrderBy -> IndxScan
        // explain select a, b, max(b) over ( partition by b ) from vanilla_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s where a = 1;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 10: No SLOB, one WF, MP Query, index (not for the WF)
        // echo     Expect WinFun -> OrderBy -> RECV -> SEND -> IndxScan
        // explain select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a = 1;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s where a = 1;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo 11: SLOB, No WF, SP Query, noindex
        // echo     Expect OrderBy(SLOB) -> SeqScan
        // explain select * from vanilla order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 12: SLOB, No WF, MP Query, noindex
        // echo     Expect OrderBy(SLOB) -> RECV -> SEND -> SeqScan
        // explain select * from vanilla_pa order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s order by b;",
                          "vanilla_pa",
                          "vanilla_pa_idx");
        }
        // echo 13: SLOB, No WF, SP Query, index (Can order the SLOB)
        // echo     Expect PlanNodeType.INDEXSCAN
        // explain explain select * from vanilla_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1 order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 13a: SLOB, No WF, SP Query, index (only to order the SLOB)
        // echo     Expect PlanNodeType.INDEXSCAN
        // explain explain select * from vanilla_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 14: SLOB, No WF, MP Query, index (Can order the SLOB)
        // echo     Expect MrgRecv(SLOB) -> SEND -> IndxScan
        // explain select * from vanilla_pb_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s order by a;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo 14a: SLOB, No WF, MP Query, index (Only to order the SLOB)
        // echo     Expect MrgRecv(SLOB) -> SEND -> IndxScan
        // explain select * from vanilla_pb_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1 order by a;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo 15: SLOB, No WF, SP Query, index (Cannot order the SLOB)
        // echo     Expect OrderBy(SLOB) -> IndxScan
        // explain select * from vanilla_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1 order by b;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 16: SLOB, No WF, MP Query, index (Cannot order the SLOB)
        // echo     Expect OrderBy(SLOB) -> RECV -> SEND -> IndxScan
        // explain select * from vanilla_pb_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select * from %s where a = 1 order by b;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo 17: SLOB, One WF, SP Query, index (Cannot order SLOB or WF)
        // echo     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> IndxScan
        // explain select a, b, max(b) over (partition by b) from vanilla_idx where a = 1 order by c;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over (partition by b) from %s where a = 1 order by c;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 18: SLOB, One WF, MP Query, index (Cannot order SLOB or WF)
        // echo     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> RECV -> SEND -> IndxScan
        // explain select a, b, max(b) over ( partition by c ) from vanilla_pb_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by c ) from %s where a = 1 order by b;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo 19: SLOB, one WF, SP Query, index (Can order the WF, Cannot order the SLOB)
        // echo     Expect OrderBy(SLOB) -> WinFun -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1 order by b;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 19a: SLOB, one WF, SP Query, index (Only to order the WF, not SLOB)
        // echo     Expect OrderBy(SLOB) -> WinFun -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_idx order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s order by b;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 20: SLOB, one WF, MP Query, index (Can order the WF, not SLOB)
        // echo     Expect OrderBy(SLOB) -> WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_pb_idx where a = 1 order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1 order by b;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo 20a: SLOB, one WF, MP Query, index (Can order the WF, not SLOB)
        // echo     Expect OrderBy(SLOB) -> WinFun -> MrgRecv(WF) -> SEND -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_pb_idx order by b;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s order by b;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo 21: SLOB, one WF, SP Query, index (Can order the SLOB, not WF)
        // echo     The index is not usable for the SLOB, since the WF invalidates the order.
        // echo     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> IndxScan
        // explain select a, b, max(b) over ( partition by b ) from vanilla_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s where a = 1 order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 21a: SLOB, one WF, SP Query, index (Can order the SLOB, not WF)
        // echo     The index is unusable for the SLOB, since the WF invalidates the order.
        // echo     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> SeqScan
        // explain select a, b, max(b) over ( partition by b ) from vanilla_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 22: SLOB, one WF, MP Query, index (Can order the SLOB, not WF)
        // echo     The index is unusable by the SLOB since the WF invalidates it.
        // echo     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> RECV -> SEND -> IndxScan
        // explain select a, b, max(b) over ( partition by b ) from vanilla_pb_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s where a = 1 order by a;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo 22a: SLOB, one WF, MP Query, index (Can order the SLOB, not WF)
        // echo     The index is unusable by the SLOB since the WF invalidates it.
        // echo     Expect OrderBy(SLOB) -> WinFun -> OrderBy(WF) -> RECV -> SEND -> SeqScan
        // explain select a, b, max(b) over ( partition by b ) from vanilla_pb_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by b ) from %s order by a;",
                          "vanilla_pb",
                          "vanilla_pb_idx");
        }
        // echo 23: SLOB, one WF, SP Query, index (Can order the WF and SLOB both)
        // echo     Expect WinFun -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_idx where a = 1 order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select a, b, max(b) over ( partition by a ) from %s where a = 1 order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 23a: SLOB, one WF, SP Query, index (Can order the WF and SLOB both)
        // echo     Expect WinFun -> IndxScan
        // explain select a, b, max(b) over ( partition by a ) from vanilla_idx order by a;
        if (IS_ENABLED) {
            validateQuery(client,
                          "select max(b) over ( partition by a ) from %s order by a;",
                          "vanilla",
                          "vanilla_idx");
        }
        // echo 24: SLOB, one WF, MP Query, index (For the WF and SLOB both)
        // echo     Expect WinFun -> MrgRecv(SLOB or WF) -> SEND -> IndxScan
        // explain select max(b) over ( partition by a ) from vanilla_pb_idx where a = 1 order by a;
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
