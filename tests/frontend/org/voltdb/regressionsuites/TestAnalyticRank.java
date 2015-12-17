/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 */
package org.voltdb.regressionsuites;

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestAnalyticRank extends RegressionSuite {

    private void initUniqueTable(Client client) throws NoConnectionsException, IOException, ProcCallException {
        client.callProcedure("tu.insert", 10, 2);
        client.callProcedure("tu.insert", 20, 1);
        client.callProcedure("tu.insert", 30, 1);
        client.callProcedure("tu.insert", 40, 3);
        client.callProcedure("tu.insert", 50, 1);
    }

    private void initUniqueTableExtra(Client client, boolean append)
            throws NoConnectionsException, IOException, ProcCallException {
        if (! append) {
            initUniqueTable(client);
        }

        // extra data
        client.callProcedure("tu.insert", 60, 2);
        client.callProcedure("tu.insert", 70, 3);
        client.callProcedure("tu.insert", 80, 2);
    }

    public void testNonSupportedCase() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        initUniqueTable(client);

        verifyAdHocFails(client, "RANK clause without using partial index matching table where clause is not allowed",
                "select a, rank() over (order by a) from tu where a > 20 order by a;");

        vt = client.callProcedure("@AdHoc", "select a, rank() over (order by a) from tu where a > 30 order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{40, 1}, {50, 2}});
    }

    public void testPercentRank() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        initUniqueTable(client);

        vt = client.callProcedure("@Explain", "select a from tu where percent_rank() over (order by a) = 0.5;").getResults()[0];
        assertTrue(vt.toString().contains("Rank SCAN"));

        vt = client.callProcedure("@AdHoc", "select a from tu where percent_rank() over (order by a) = 0.5;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{20});

        vt = client.callProcedure("@AdHoc", "select a from tu where percent_rank() over (order by a) = 0.61;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{30});

        vt = client.callProcedure("@AdHoc", "select a from tu where percent_rank() over (order by a) = 0.8;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{40});

        vt = client.callProcedure("@AdHoc", "select a from tu where percent_rank() over (order by a) = 0.9;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{40});

        vt = client.callProcedure("@AdHoc", "select sum(a) from tu where percent_rank() over (order by a) >= 0.4;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{140});

        vt = client.callProcedure("@AdHoc", "select sum(a) from tu where percent_rank() over (order by a) >= 0.7;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{120});

        //
        // partition by
        //

        // no rank scan support yet
        vt = client.callProcedure("@Explain", "select a from tu where percent_rank() over (partition by b order by a) > 0.1 order by abs(a);").getResults()[0];
        assertTrue(vt.toString().contains("SEQUENTIAL SCAN"));

        vt = client.callProcedure("@AdHoc", "select a from tu where percent_rank() over (partition by b order by a) > 0.1 order by abs(a);").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{10, 20, 30, 40, 50});

        vt = client.callProcedure("@Explain", "select a from tu where percent_rank() over (partition by b order by a) > 0.5 order by abs(a);").getResults()[0];
        System.err.println(vt);
        assertTrue(vt.toString().contains("SEQUENTIAL SCAN"));

        vt = client.callProcedure("@AdHoc", "select a from tu where percent_rank() over (partition by b order by a) > 0.5 order by abs(a);").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{10, 30, 40, 50});
    }

    public void testRank_UNIQUE() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        initUniqueTable(client);

        vt = client.callProcedure("@AdHoc", "select a, rank() over (order by a) from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 1}, {20, 2}, {30, 3}, {40, 4}, {50, 5}});

        // decending
        vt = client.callProcedure("@AdHoc", "select a, rank() over (order by a desc) from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 5}, {20, 4}, {30, 3}, {40, 2}, {50, 1}});

        // where clause
        vt = client.callProcedure("@AdHoc", "select a from tu "
                + "where rank() over (order by a) <= 3 order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10}, {20}, {30}});

        vt = client.callProcedure("@AdHoc", "select a from tu "
                + "where rank() over (order by a) >= 2 and "
                + "rank() over (order by a) < 4 order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{20}, {30}});

        vt = client.callProcedure("@AdHoc", "select * from tu "
                + "where rank() over (order by a) = 3 order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{30, 1}});

        //
        // PARTITION BY
        //
        initUniqueTableExtra(client, true);

        vt = client.callProcedure("@AdHoc", "select a, rank() over (partition by b order by a) from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 1}, {20, 1}, {30, 2}, {40, 1}, {50, 3}, {60, 2}, {70, 2}, {80, 3}});

        vt = client.callProcedure("@AdHoc", "select a, rank() over (partition by b order by a desc) from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 3}, {20, 3}, {30, 2}, {40, 2}, {50, 1}, {60, 2}, {70, 1}, {80, 1}});

    }

    public void testRankScan_UNIQUE_EQ() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        initUniqueTable(client);

        vt = client.callProcedure("@Explain", "select a from tu where rank() over (order by a) = 2;").getResults()[0];
        assertTrue(vt.toString().contains("Rank SCAN"));

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) = 2;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{20});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) = 5;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{50});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) = 10;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});

        // aggregates
        vt = client.callProcedure("@AdHoc", "select sum(a) from tu where rank() over (order by a) = 3;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{30});
    }

    public void testRankScan_UNIQUE_GT_GTE() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        initUniqueTable(client);

        // TODO(xin): NOT TESTED against KEY exception
        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) > -2 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{10, 20, 30, 40, 50});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) >= 3 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{30, 40, 50});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) > 3 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{40, 50});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) > 3 and a + 10 != 50;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{50});

        // aggregates
        vt = client.callProcedure("@Explain", "select sum(a) from tu where rank() over (order by a) >= 3;").getResults()[0];
        assertTrue(vt.toString().contains("Rank SCAN"));

        vt = client.callProcedure("@AdHoc", "select sum(a) from tu where rank() over (order by a) >= 3;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{120});

        vt = client.callProcedure("@AdHoc", "select b, sum(a) from tu where rank() over (order by a) >= 2 "
                + " group by b order by b;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{1, 100}, {3, 40}});
    }

    public void testRankScan_UNIQUE_LT_LTE() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        initUniqueTable(client);

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) < 3 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{10, 20});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) <= 3 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{10, 20, 30});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) < 1 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) < 10 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{10, 20, 30, 40, 50});
    }


    public void testRankScan_UNIQUE_ALL() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        initUniqueTable(client);

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) < 4 "
                + " and rank() over (order by a) > 1 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{20, 30});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) > 1 "
                + " and rank() over (order by a) < 4 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{20, 30});

        vt = client.callProcedure("@AdHoc", "select a from tu where rank() over (order by a) > 1 "
                + " and rank() over (order by a) < 4 and a + 10 != 30 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{30});


        vt = client.callProcedure("@AdHoc", "select sum(a) from tu where rank() over (order by a) > 1 "
                + " and rank() over (order by a) < 10 order by a;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{140});

        vt = client.callProcedure("@AdHoc", "select b, sum(a) from tu where rank() over (order by a) > 0 "
                + " and rank() over (order by a) < 10 group by b order by b;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{1, 100}, {2, 10}, {3, 40}});
    }

    //
    // NON-UNIQUE RANK SCAN TEST
    //
    public void testRank_NON_UNIQUE() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        client.callProcedure("tm.insert", 10, 1);
        client.callProcedure("tm.insert", 10, 1);
        client.callProcedure("tm.insert", 10, 2);
        client.callProcedure("tm.insert", 20, 1);
        client.callProcedure("tm.insert", 30, 3);
        client.callProcedure("tm.insert", 30, 1);
        client.callProcedure("tm.insert", 40, 2);
        client.callProcedure("tm.insert", 40, 3);
        client.callProcedure("tm.insert", 50, 2);

        vt = client.callProcedure("@AdHoc", "select a, rank() over (order by a) from tm order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 1},{10, 1},{10, 1},
                {20, 4}, {30, 5}, {30, 5}, {40, 7}, {40, 7}, {50, 9}});

        vt = client.callProcedure("@AdHoc", "select a, rank() over (order by a desc) from tm order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 7},{10, 7},{10, 7},
                {20, 6}, {30, 4}, {30, 4}, {40, 2}, {40, 2}, {50, 1}});

        //
        // PARTITION BY
        //
        vt = client.callProcedure("@AdHoc", "select b, a, rank() over (partition by b order by a) from tm order by b, a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{1, 10, 1},{1, 10, 1}, {1, 20, 3}, {1, 30, 4}, {2, 10, 1},
                {2, 40, 2}, {2, 50, 3}, {3, 30, 1}, {3, 40, 2}});

        vt = client.callProcedure("@AdHoc", "select b, a, rank() over (partition by b order by a desc) from tm order by b, a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{1, 10, 3},{1, 10, 3}, {1, 20, 2}, {1, 30, 1}, {2, 10, 3},
                {2, 40, 2}, {2, 50, 1}, {3, 30, 2}, {3, 40, 1}});
    }


    public void testNoRankScan_NON_UNIQUE() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("STARTING xin......");
        Client client = getClient();
        VoltTable vt = null;

        client.callProcedure("tm.insert", 10, 1);
        client.callProcedure("tm.insert", 10, 1);
        client.callProcedure("tm.insert", 10, 2);
        client.callProcedure("tm.insert", 20, 1);
        client.callProcedure("tm.insert", 30, 3);
        client.callProcedure("tm.insert", 30, 1);
        client.callProcedure("tm.insert", 40, 2);
        client.callProcedure("tm.insert", 40, 3);
        client.callProcedure("tm.insert", 50, 2);

        // SEQ SCAN

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 1;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{10, 10, 10});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 2;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 3;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 4;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{20});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 5;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{30, 30});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 6;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 7;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{40, 40});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 8;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 9;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{50});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 10;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = -1;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{});


        // INDEX SCAN
        vt = client.callProcedure("@Explain", "select a from tm where rank() over (order by a) = 4 and a >= 20 and a < 40;").getResults()[0];
        assertTrue(vt.toString().contains("INDEX SCAN"));

        vt = client.callProcedure("@AdHoc", "select a from tm where rank() over (order by a) = 4 and a >= 20 and a < 40;").getResults()[0];
        validateTableOfScalarLongs(vt, new long[]{20});
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestAnalyticRank(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
                new MultiConfigSuiteBuilder(TestAnalyticRank.class);
        boolean success;
        VoltProjectBuilder project = new VoltProjectBuilder();

        final String literalSchema =
                "create table tu (a integer, b integer);" +
                        "create unique index idx1 on tu (a);" +
                        "create unique index idx2 on tu (b, a);" +
                        "create unique index idx3 on tu (a) where a > 30;" +

            "create table tm (a integer, b integer);" +
            "create index tm_idx1 on tm (a);" +
            "create index tm_idx2 on tm (b, a);" +

            "create table pu (a integer, b integer);" +
            "create index pu_idx1 on pu (a);" +
            "create index pu_idx2 on pu (b, a);" +

            ""
            ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        //        project.addStmtProcedure("TRIM_ANY", "select id, TRIM(LEADING ? FROM var16) from r1 where id = ?");

        // CONFIG #1: Local Site/Partition running on JNI backend
        config = new LocalCluster("fixedsql-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        // CONFIG #2: HSQL -- disabled, the functions being tested are not HSQL compatible
        //        config = new LocalCluster("fixedsql-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        //        success = config.compile(project);
        //        assertTrue(success);
        //        builder.addServerConfig(config);

        // no clustering tests for functions

        return builder;
    }
}
