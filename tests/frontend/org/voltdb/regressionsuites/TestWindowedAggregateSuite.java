/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestWindowedAggregateSuite extends RegressionSuite {

    public TestWindowedAggregateSuite(String name) {
        super(name);
        // TODO Auto-generated constructor stub
    }

    static private void setupSchema(VoltProjectBuilder project) throws IOException {
        String literalSchema =
                "CREATE TABLE T (\n"
                + "  A INTEGER,"
                + "  B INTEGER,"
                + "  C INTEGER"
                + ");\n"

                +"CREATE TABLE T_STRING ("
                + "  A INTEGER,"
                + "  B INTEGER,"
                + "  C VARCHAR"
                + ");"

                + "create table tu (a integer, b integer);"
                + "create unique index idx1 on tu (a);"
                + "create unique index idx2 on tu (b, a);"
                + "create unique index idx3 on tu (a) where a > 30;"

                + "create table tm (a integer, b integer);"
                + "create index tm_idx1 on tm (a);"
                + "create index tm_idx2 on tm (b, a);"

                + "create table pu (a integer, b integer);"
                + "create index pu_idx1 on pu (a);"
                + "create index pu_idx2 on pu (b, a);"
                ;
        project.addLiteralSchema(literalSchema);
        project.setUseDDLSchema(true);
    }

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

        //
        // PARTITION BY
        //
        initUniqueTableExtra(client, true);

        vt = client.callProcedure("@AdHoc", "select a, rank() over (partition by b order by a) from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 1}, {20, 1}, {30, 2}, {40, 1}, {50, 3}, {60, 2}, {70, 2}, {80, 3}});

        vt = client.callProcedure("@AdHoc", "select a, rank() over (partition by b order by a desc) from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 3}, {20, 3}, {30, 2}, {40, 2}, {50, 1}, {60, 2}, {70, 1}, {80, 1}});

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


    public void testRankWithString() throws Exception {
        // Save the guard and restore it after.
        Client client = getClient();

        long expected[][] = new long[][] {
                {  1L,  1L,  101L, 1L },
                {  1L,  1L,  102L, 1L },
                {  1L,  2L,  201L, 3L },
                {  1L,  2L,  202L, 3L },
                {  1L,  3L,  203L, 5L },
                {  2L,  1L, 1101L, 1L },
                {  2L,  1L, 1102L, 1L },
                {  2L,  2L, 1201L, 3L },
                {  2L,  2L, 1202L, 3L },
                {  2L,  3L, 1203L, 5L },
                { 20L,  1L, 2101L, 1L },
                { 20L,  1L, 2102L, 1L },
                { 20L,  2L, 2201L, 3L },
                { 20L,  2L, 2202L, 3L },
                { 20L,  3L, 2203L, 5L },
        };
        long input[][] = expected.clone();
        shuffleArrayOfLongs(input);
        ClientResponse cr;
        VoltTable vt;
        for (long [] row : input) {
            cr = client.callProcedure("T_STRING.insert", row[0], row[1], Long.toString(row[2], 10));
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        String sql = "select A, B, C, rank() over (partition by A order by B) as R from T_STRING ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(expected.length, vt.getRowCount());
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            assertEquals(expected[rowIdx][0], vt.getLong(0));
            assertEquals(expected[rowIdx][1], vt.getLong(1));
            assertEquals(Long.toString(expected[rowIdx][2], 10), vt.getString(2));
            assertEquals(expected[rowIdx][3], vt.getLong(3));
        }
    }

    public void testRank() throws Exception {
        // Save the guard and restore it after.
        Client client = getClient();

        long expected[][] = new long[][] {
                {  1L,  1L,  101L, 1L },
                {  1L,  1L,  102L, 1L },
                {  1L,  2L,  201L, 3L },
                {  1L,  2L,  202L, 3L },
                {  1L,  3L,  203L, 5L },
                {  2L,  1L, 1101L, 1L },
                {  2L,  1L, 1102L, 1L },
                {  2L,  2L, 1201L, 3L },
                {  2L,  2L, 1202L, 3L },
                {  2L,  3L, 1203L, 5L },
                { 20L,  1L, 2101L, 1L },
                { 20L,  1L, 2102L, 1L },
                { 20L,  2L, 2201L, 3L },
                { 20L,  2L, 2202L, 3L },
                { 20L,  3L, 2203L, 5L },
        };
        long input[][] = expected.clone();
        shuffleArrayOfLongs(input);
        ClientResponse cr;
        VoltTable vt;
        for (long [] row : input) {
            cr = client.callProcedure("T.insert", row[0], row[1], row[2]);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        String sql = "select A, B, C, rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        validateTableOfLongs(vt, expected);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestWindowedAggregateSuite.class);
        boolean success = false;


        try {
            VoltProjectBuilder project = new VoltProjectBuilder();
            config = new LocalCluster("test-windowed-rank.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
        }
        catch (IOException excp) {
            fail();
        }

        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
