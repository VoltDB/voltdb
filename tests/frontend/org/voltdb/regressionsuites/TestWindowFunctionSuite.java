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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;

public class TestWindowFunctionSuite extends RegressionSuite {

    public TestWindowFunctionSuite(String name) {
        super(name);
    }

    static private void setupSchema(VoltProjectBuilder project) throws IOException {
        String literalSchema =
                "CREATE TABLE T (\n"
                + "  A INTEGER,"
                + "  B INTEGER,"
                + "  C INTEGER"
                + ");\n"

                +"CREATE TABLE T_STRING ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR NOT NULL"
                + ");"

                +"CREATE TABLE T_STRING_A ("
                + "  A VARCHAR NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C INTEGER NOT NULL"
                + ");"
                + "PARTITION TABLE T_STRING_A ON COLUMN A;"

                + "CREATE TABLE T_TIMESTAMP ("
                + "  A INTEGER,"
                + "  B INTEGER,"
                + "  C TIMESTAMP"
                + ");"

                + "CREATE TABLE T_4COL (\n"
                + "  A INTEGER,"
                + "  AA INTEGER,"
                + "  B INTEGER,"
                + "  C INTEGER"
                + ");\n"

                + "CREATE TABLE T_PA (\n"
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C INTEGER NOT NULL"
                + ");\n"
                + "PARTITION TABLE T_PA ON COLUMN A;"

                + "CREATE TABLE T_PAA (\n"
                + "  A INTEGER NOT NULL,"
                + "  AA INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C INTEGER NOT NULL"
                + ");\n"
                + "PARTITION TABLE T_PAA ON COLUMN AA;"

                + "CREATE TABLE T_PB (\n"
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C INTEGER NOT NULL"
                + ");\n"
                + "PARTITION TABLE T_PB ON COLUMN B;"

                + "CREATE TABLE T_PC (\n"
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C INTEGER NOT NULL"
                + ");\n"
                + "PARTITION TABLE T_PC ON COLUMN C;"

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

                + "CREATE TABLE P1_ENG_10972 ("
                + "     ID INTEGER NOT NULL, "
                + "     VCHAR VARCHAR(300), "
                + "     NUM INTEGER, "
                + "     RATIO FLOAT, "
                + "     PRIMARY KEY (ID) "
                + "   ); "
                + "PARTITION TABLE P1_ENG_10972 ON COLUMN ID; "

                + "CREATE TABLE P2 ("
                + "  ID INTEGER NOT NULL,"
                + "  TINY TINYINT NOT NULL,"
                + "  SMALL SMALLINT,"
                + "  BIG BIGINT NOT NULL,"
                + "  PRIMARY KEY (ID, TINY)"
                + ");"
                + "PARTITION TABLE P2 ON COLUMN TINY;"

                + "CREATE TABLE P1_ENG_11029 ("
                + "        ID INTEGER NOT NULL,"
                + "        TINY TINYINT NOT NULL,"
                + "        SMALL SMALLINT NOT NULL,"
                + "        BIG BIGINT NOT NULL,"
                + "        PRIMARY KEY (ID)"
                + ");"
                + "PARTITION TABLE P1_ENG_11029 ON COLUMN ID;"
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
    private static final boolean IS_ENABLED = true;
    private static final boolean ISNOT_ENABLED = false;

    public void testAll() throws Exception {
        Client client = getClient();
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRank_UNIQUE();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRank_NON_UNIQUE();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRankWithString();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRankWithTimestamp();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRankPartitionedTable();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRank();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRowNumber();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRankMultPartitionBys();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRankWithEmptyTable();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestRankOrderbyExpressions();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestSubqueryWindowFunctionExpressions();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestExplainPlan();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestEng10972();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestEng11029();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestCount();
        }
    }

    private void subtestRank_UNIQUE() throws NoConnectionsException, IOException, ProcCallException {
        Client client = getClient();
        VoltTable vt = null;

        initUniqueTable(client);

        vt = client.callProcedure("@AdHoc", "select a, rank() over (order by a) from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 1}, {20, 2}, {30, 3}, {40, 4}, {50, 5}});

        // descending
        vt = client.callProcedure("@AdHoc", "select a, rank() over (order by a desc) from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 5}, {20, 4}, {30, 3}, {40, 2}, {50, 1}});

        //
        // PARTITION BY
        //
        initUniqueTableExtra(client, true);

        vt = client.callProcedure("@AdHoc", "select a, rank() over (partition by b order by a) as r from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 1}, {20, 1}, {30, 2}, {40, 1}, {50, 3}, {60, 2}, {70, 2}, {80, 3}});

        vt = client.callProcedure("@AdHoc", "select a, rank() over (partition by b order by a desc) from tu order by a;").getResults()[0];
        validateTableOfLongs(vt, new long[][]{{10, 3}, {20, 3}, {30, 2}, {40, 2}, {50, 1}, {60, 2}, {70, 1}, {80, 1}});

    }

    //
    // NON-UNIQUE RANK SCAN TEST
    //
    private void subtestRank_NON_UNIQUE() throws NoConnectionsException, IOException, ProcCallException {
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


    // rank1 is the rank for partition by A, order by B
    // rank2 is the rank for partition by A, AA, order by B
    private Long expected[][] = new Long[][] {
        // A     AA   B     C    rank1   rank2   rank3   row_number1  row_number2
        //--------------------------------------
        {  1L,  301L, 1L,  101L, 1L,      1L,      1L,   1L,          1L},
        {  1L,  301L, 1L,  102L, 1L,      1L,      1L,   2L,          2L},
        //======================================
        {  1L,  302L, 2L,  201L, 3L,      1L,      2L,   3L,          3L},
        {  1L,  302L, 2L,  202L, 3L,      1L,      2L,   4L,          4L},
        //======================================
        {  1L,  302L, 3L,  203L, 5L,      3L,      3L,   5L,          5L},
        //--------------------------------------
        {  2L,  303L, 1L, 1101L, 1L,      1L,      1L,   1L,          6L},
        {  2L,  303L, 1L, 1102L, 1L,      1L,      1L,   2L,          7L},
        //======================================
        {  2L,  303L, 2L, 1201L, 3L,      3L,      2L,   3L,          8L},
        {  2L,  304L, 2L, 1202L, 3L,      1L,      2L,   4L,          9L},
        //======================================
        {  2L,  304L, 3L, 1203L, 5L,      2L,      3L,   5L,          10L},
        //--------------------------------------
        { 20L,  305L, 1L, 2101L, 1L,      1L,      1L,   1L,          11L},
        { 20L,  305L, 1L, 2102L, 1L,      1L,      1L,   2L,          12L},
        //======================================
        { 20L,  305L, 2L, 2201L, 3L,      3L,      2L,   3L,          13L},
        { 20L,  306L, 2L, 2202L, 3L,      1L,      2L,   4L,          14L},
        //======================================
        { 20L,  306L, 3L, 2203L, 5L,      2L,      3L,   5L,          15L},
        //--------------------------------------
    };

    // Names for the column indices.
    final int colA          = 0;
    final int colAA         = 1;
    final int colB          = 2;
    final int colC          = 3;
    final int colR_A        = 4;
    final int colR_AA       = 5;
    final int colR_dense    = 6;
    final int colR_row1     = 7;
    final int colR_row2     = 8;

    private void subtestRankWithString() throws Exception {
        Client client = getClient();

        Long input[][] = expected.clone();
        shuffleArray(input);
        ClientResponse cr;
        VoltTable vt;
        for (Long [] row : input) {
            cr = client.callProcedure("T_STRING.insert", row[colA], row[colB], Long.toString(row[colC], 10));
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("T_STRING_A.insert", Long.toString(row[colA], 10), row[colB], row[colC]);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        String sql;
        // Test string values
        sql = "select A, B, C, rank() over (partition by A order by B) as R from T_STRING ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(expected.length, vt.getRowCount());
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            assertEquals(expected[rowIdx][colA], Long.valueOf(vt.getLong(0)));
            assertEquals(expected[rowIdx][colB], Long.valueOf(vt.getLong(1)));
            assertEquals(Long.toString(expected[rowIdx][colC], 10), vt.getString(2));
            assertEquals(expected[rowIdx][colR_A], Long.valueOf(vt.getLong(3)));
        }
        // Test with partition by over a string column
        sql = "select A, B, C, rank() over (partition by A order by B) as R from T_STRING_A ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(expected.length, vt.getRowCount());
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            assertEquals(Long.toString(expected[rowIdx][colA], 10), vt.getString(0));
            assertEquals(expected[rowIdx][colB], Long.valueOf(vt.getLong(1)));
            assertEquals(expected[rowIdx][colC], Long.valueOf(vt.getLong(2)));
            assertEquals(expected[rowIdx][colR_A], Long.valueOf(vt.getLong(3)));
        }
    }


    private void subtestRankWithTimestamp() throws Exception {
        Client client = getClient();

        long baseTime = TimestampType.millisFromJDBCformat("1953-06-10 00:00:00");
        Long input[][] = expected.clone();
        shuffleArray(input);
        ClientResponse cr;
        VoltTable vt;
        for (Long [] row : input) {
            cr = client.callProcedure("T_TIMESTAMP.insert", row[colA], row[colB], new TimestampType(baseTime + row[colB]*1000));
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        String sql = "select A, B, C, rank() over (partition by A order by C) as R from T_TIMESTAMP ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertEquals(expected.length, vt.getRowCount());
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            String msg = String.format("Row %d:", rowIdx);
            assertEquals(msg, expected[rowIdx][colA], Long.valueOf(vt.getLong(0)));
            assertEquals(msg, expected[rowIdx][colB], Long.valueOf(vt.getLong(1)));
            assertEquals(msg, baseTime + expected[rowIdx][colB]*1000, vt.getTimestampAsLong(2));
            assertEquals(msg, expected[rowIdx][colR_A], Long.valueOf(vt.getLong(3)));
        }
    }

    private void subtestRankPartitionedTable() throws Exception {
        Client client = getClient();

        Long input[][] = expected.clone();
        shuffleArray(input);
        ClientResponse cr;
        VoltTable vt;
        for (Long [] row : input) {
            cr = client.callProcedure("T_PA.insert", row[colA], row[colB], row[colC]);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("T_PB.insert", row[colA], row[colB], row[colC]);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("T_PC.insert", row[colA], row[colB], row[colC]);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            cr = client.callProcedure("T_PAA.insert", row[colA], row[colAA], row[colB], row[colC]);
        }
        String sql;
        // Test rank with partition by over a partitioned column.
        sql = "select A, B, C, rank() over (partition by A order by B) as R from T_PA ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            String msg = String.format("Row %d:", rowIdx);
            assertEquals(msg, expected[rowIdx][colA],    Long.valueOf(vt.getLong(0)));
            assertEquals(msg, expected[rowIdx][colB],    Long.valueOf(vt.getLong(1)));
            assertEquals(msg, expected[rowIdx][colC],    Long.valueOf(vt.getLong(2)));
            assertEquals(msg, expected[rowIdx][colR_A],  Long.valueOf(vt.getLong(3)));
        }
        // Test rank with ordered window over a partitioned column, and
        // partition not over a partitioned column.
        sql = "select A, B, C, rank() over (partition by A order by B) as R from T_PB ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            String msg = String.format("Row %d:", rowIdx);
            assertEquals(msg, expected[rowIdx][colA],    Long.valueOf(vt.getLong(0)));
            assertEquals(msg, expected[rowIdx][colB],    Long.valueOf(vt.getLong(1)));
            assertEquals(msg, expected[rowIdx][colC],    Long.valueOf(vt.getLong(2)));
            assertEquals(msg, expected[rowIdx][colR_A],  Long.valueOf(vt.getLong(3)));
        }
        // Select rank with neither partition nor rank over partioned
        // columns, but with a partitioned table.
        sql = "select A, B, C, rank() over (partition by A order by B) as R from T_PC ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            String msg = String.format("Row %d:", rowIdx);
            assertEquals(msg, expected[rowIdx][colA],    Long.valueOf(vt.getLong(0)));
            assertEquals(msg, expected[rowIdx][colB],    Long.valueOf(vt.getLong(1)));
            assertEquals(msg, expected[rowIdx][colC],    Long.valueOf(vt.getLong(2)));
            assertEquals(msg, expected[rowIdx][colR_A],  Long.valueOf(vt.getLong(3)));
        }
        // Check rank with windowed partition on two columns, one partitioned and
        // one not partitioned, but ordered by a non-partitioned column.
        sql = "select A, B, C, rank() over (partition by A, AA order by B) as R from T_PAA ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            String msg = String.format("Row %d:", rowIdx);
            assertEquals(msg, expected[rowIdx][colA],    Long.valueOf(vt.getLong(0)));
            assertEquals(msg, expected[rowIdx][colB],    Long.valueOf(vt.getLong(1)));
            assertEquals(msg, expected[rowIdx][colC],    Long.valueOf(vt.getLong(2)));
            assertEquals(msg, expected[rowIdx][colR_AA], Long.valueOf(vt.getLong(3)));
        }
        // Check the previous case, but with the partition by order reversed.
        sql = "select A, B, C, rank() over (partition by AA, A order by B) as R from T_PAA ORDER BY A, AA, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            String msg = String.format("Row %d:", rowIdx);
            assertEquals(msg, expected[rowIdx][colA],    Long.valueOf(vt.getLong(0)));
            assertEquals(msg, expected[rowIdx][colB],    Long.valueOf(vt.getLong(1)));
            assertEquals(msg, expected[rowIdx][colC],    Long.valueOf(vt.getLong(2)));
            assertEquals(msg, expected[rowIdx][colR_AA], Long.valueOf(vt.getLong(3)));
        }

    }

    public void validateRankFunction(String sql, int expectedCol) throws Exception {
        Client client = getClient();

        Long input[][] = expected.clone();
        shuffleArray(input);
        ClientResponse cr;
        VoltTable vt;

        cr = client.callProcedure("@AdHoc", "TRUNCATE TABLE T");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        for (Long [] row : input) {
            cr = client.callProcedure("T.insert", row[colA], row[colB], row[colC]);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            String msg = String.format("Row %d:", rowIdx);
            assertEquals(msg, expected[rowIdx][colA],       Long.valueOf(vt.getLong(0)));
            assertEquals(msg, expected[rowIdx][colB],       Long.valueOf(vt.getLong(1)));
            assertEquals(msg, expected[rowIdx][colC],       Long.valueOf(vt.getLong(2)));
            assertEquals(msg, expected[rowIdx][expectedCol],Long.valueOf(vt.getLong(3)));
        }
    }

    private void subtestRank() throws Exception {
        validateRankFunction("select A, B, C, dense_rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;",
                             colR_dense);
        validateRankFunction("select A, B, C, rank() over (partition by A order by B) as R from T ORDER BY A, B, C, R;",
                              colR_A);
    }

    private void subtestRowNumber() throws Exception {
        // partition by
        validateRankFunction("select A, B, C, row_number() over (partition by A order by B, C) as R from T order by A;",
                colR_row1);
        // no partition by
        validateRankFunction("select A, B, C, row_number() over (ORDER BY A, B, C) as R from T;",
                colR_row2);
    }

    private void subtestRankMultPartitionBys() throws Exception {
        Client client = getClient();

        Long input[][] = expected.clone();
        shuffleArray(input);
        ClientResponse cr;
        VoltTable vt;
        for (Long [] row : input) {
            cr = client.callProcedure("T.insert", row[0], row[1], row[2]);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        String sql = "select A, B, C, rank() over (partition by A, AA order by B) as R from T_4COL ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            String msg = String.format("Row %d:", rowIdx);
            assertEquals(msg, expected[rowIdx][colA],    Long.valueOf(vt.getLong(0)));
            assertEquals(msg, expected[rowIdx][colB],    Long.valueOf(vt.getLong(1)));
            assertEquals(msg, expected[rowIdx][colC],    Long.valueOf(vt.getLong(2)));
            assertEquals(msg, expected[rowIdx][colR_AA], Long.valueOf(vt.getLong(3)));
        }
    }

    private void subtestRankWithEmptyTable() throws Exception {
        Client client = getClient();

        ClientResponse cr;
        // Don't insert nothing.  Or, rather, do insert nothing.
        String sql = "select A, B, C, rank() over (partition by A*A*A, A*A order by B*B) as R from T ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // That's it.  If the EE does not crash we are happy.
    }
    private void subtestRankOrderbyExpressions() throws Exception {
        Client client = getClient();

        Long input[][] = expected.clone();
        shuffleArray(input);
        ClientResponse cr;
        VoltTable vt;
        for (Long [] row : input) {
            cr = client.callProcedure("T.insert", row[colA], row[colB], row[colC]);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
        String sql = "select A, B, C, rank() over (partition by A*A*A, A*A order by B*B) as R from T ORDER BY A, B, C, R;";
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        for (int rowIdx = 0; vt.advanceRow(); rowIdx += 1) {
            String msg = String.format("Row %d:", rowIdx);
            assertEquals(msg, expected[rowIdx][colA],    Long.valueOf(vt.getLong(0)));
            assertEquals(msg, expected[rowIdx][colB],    Long.valueOf(vt.getLong(1)));
            assertEquals(msg, expected[rowIdx][colC],    Long.valueOf(vt.getLong(2)));
            assertEquals(msg, expected[rowIdx][colR_A],  Long.valueOf(vt.getLong(3)));
        }
    }

    /**
     * Validate that we get the same answer if we calculate a rank expression
     * in a subquery or in an outer query.  Try with queries whose partition
     * by list contains partition columns of their tables and those whose
     * partition by list do not.
     *
     * @throws Exception
     */
    private void subtestSubqueryWindowFunctionExpressions() throws Exception {
        Client client = getClient();

        client.callProcedure("P2.insert", 0, 2, null, -67);
        client.callProcedure("P2.insert", 1, 2, null, 39);
        client.callProcedure("P2.insert", 2, 2, 106, -89);
        client.callProcedure("P2.insert", 3, 2, 106, 123);
        client.callProcedure("P2.insert", 4, 5, -100, -92);
        client.callProcedure("P2.insert", 5, 5, -100, -52);
        client.callProcedure("P2.insert", 6, 5, 119, -110);
        client.callProcedure("P2.insert", 7, 5, 119, 102);

        String sql;

        sql = "SELECT *, RANK() OVER (PARTITION BY SMALL ORDER BY BIG ) SRANK "
                + "FROM ( SELECT *, RANK() OVER (PARTITION BY SMALL ORDER BY BIG ) SUBRANK FROM P2 W09) SUB "
                + "ORDER BY ID, TINY, SMALL, BIG, SRANK;"
                ;
        validateSubqueryWithWindowedAggregate(client, sql);

        sql = "SELECT *, RANK() OVER (PARTITION BY SMALL ORDER BY BIG ) SRANK "
               + "FROM (SELECT *, RANK() OVER (PARTITION BY SMALL ORDER BY BIG ) SUBRANK FROM P2 W09) SUB "
               + "ORDER BY ID, TINY, SMALL, BIG, SRANK;"
               ;
        validateSubqueryWithWindowedAggregate(client, sql);
    }

    private void validateSubqueryWithWindowedAggregate(Client client, String sql)
            throws IOException, NoConnectionsException, ProcCallException {
        ClientResponse cr;
        VoltTable vt;
        cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        int nc = vt.getColumnCount();
        while (vt.advanceRow()) {
            assertEquals(vt.getLong(nc-2), vt.getLong(nc-1));
        }
    }

    /*
     * This test just makes sure that we can execute the @Explain
     * sysproc on a windowed aggregate.  At one time this failed due to
     * an NPE.  When deserializing a JSON string the resulting plan is not
     * the same as the original plan.  We don't serialize sort orders
     * for windowed aggregates.
     */
    private void subtestExplainPlan() throws Exception {
        Client client = getClient();
        String sql = "select rank() over ( partition by A, B order by C ) from T;";

        ClientResponse cr;
        try {
            cr = client.callProcedure("@Explain", sql);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            // We just care that the explain has succeeded and not
            // caused an NPE.  The results here are not used, but they
            // are useful for diagnosing errors.  So we leave them
            // here.
            VoltTable vt = cr.getResults()[0];
            assertTrue(true);
        } catch (Exception ex) {
            fail("Exception on @Explain of windowed expression");
        }
    }

    private void subtestEng10972() throws Exception {
        // reproducer for ENG-10972 and ENG-10973, found by sqlcoverage
        Client client = getClient();
        VoltTable vt;

        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_10972 VALUES (0, 'BS', NULL, 2.0);");
        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_10972 VALUES (1, 'DS', NULL, 2.0);");
        vt = client.callProcedure("@AdHoc",
                "SELECT RANK() OVER (PARTITION BY ID ORDER BY ABS(NUM) ) SRANK "
                + "FROM P1_ENG_10972;").getResults()[0];
        assertContentOfTable(new Object[][] {
            {1},
            {1}}, vt);

        client.callProcedure("@AdHoc", "truncate table P1_ENG_10972");

        client.callProcedure("@AdHoc", "INSERT INTO P1_ENG_10972 VALUES (0, 'BS', NULL, 2.0);");

        client.callProcedure("@AdHoc", "SELECT ID, VCHAR, NUM, RATIO, RANK() OVER (PARTITION BY ID ORDER BY ABS(NUM) ) SRANK FROM P1_ENG_10972;");
        vt = client.callProcedure("@AdHoc",
                "SELECT RATIO, RANK() OVER (PARTITION BY ID ORDER BY ABS(NUM) ) SRANK "
                 + "FROM P1_ENG_10972 "
                 + "ORDER BY RATIO, SRANK;").getResults()[0];
        assertContentOfTable(new Object[][] {
            {2.0, 1}}, vt);
    }

    private void subtestEng11029() throws Exception {
        // Regression test for ENG-11029
        Client client = getClient();

        //        CREATE TABLE P1_ENG_11029 (
        //                ID INTEGER NOT NULL,
        //                TINY TINYINT NOT NULL,
        //                SMALL SMALLINT NOT NULL,
        //                BIG BIGINT NOT NULL,
        //                PRIMARY KEY (ID)
        //        );
        //
        //        PARTITION TABLE P1_ENG_11029 ON COLUMN ID;

        client.callProcedure("P1_ENG_11029.Insert", 0, 1, 10, 100);
        client.callProcedure("P1_ENG_11029.Insert", 1, 1, 10, 101);
        client.callProcedure("P1_ENG_11029.Insert", 2, 2, 12, 102);
        client.callProcedure("P1_ENG_11029.Insert", 3, 2, 12, 103);

        VoltTable vt;
        vt = client.callProcedure("@AdHoc",
                "SELECT "
                + "  BIG, "
                + "  RANK() OVER (PARTITION BY SMALL ORDER BY BIG ) SRANK, "
                + "  SMALL "
                + "FROM P1_ENG_11029 "
                + "ORDER BY BIG, SRANK, SMALL;").getResults()[0];
        assertContentOfTable(new Object [][] {
            {100, 1, 10},
            {101, 2, 10},
            {102, 1, 12},
            {103, 2, 12}
        }, vt);

        vt = client.callProcedure("@AdHoc",
                "SELECT "
                + "  TINY, "
                + "  SMALL, "
                + "  RANK() OVER (PARTITION BY SMALL ORDER BY TINY ) SRANK "
                + "FROM P1_ENG_11029 "
                + "ORDER BY TINY, SMALL, SRANK;").getResults()[0];
        assertContentOfTable(new Object [][] {
            {1, 10, 1},
            {1, 10, 1},
            {2, 12, 1},
            {2, 12, 1}
        }, vt);

        vt = client.callProcedure("@AdHoc",
                "SELECT "
                + "  BIG, "
                + "  RANK() OVER (PARTITION BY TINY ORDER BY SMALL) SRANK "
                + "FROM P1_ENG_11029 "
                + "ORDER BY BIG, SRANK;").getResults()[0];
        assertContentOfTable(new Object [][] {
            {100, 1},
            {101, 1},
            {102, 1},
            {103, 1}
        }, vt);
    }

    private void validateCount(Client client, String sql, long expected[][]) throws Exception {
        ClientResponse cr = client.callProcedure("@AdHoc", sql);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        validateTableOfLongs(vt, expected);
    }

    private void subtestCount() throws Exception {
        Client client = getClient();
        initUniqueTable(client);

        // Try some simple cases with partition by and order by.
        validateCount(client,
                      "select a, count(*) over (partition by a) from tu order by a",
                      new long[][]{{10, 1}, {20, 1}, {30, 1}, {40, 1}, {50, 1}});
        validateCount(client,
                      "select a, count(*) over (order by a) from tu order by a",
                      new long[][]{{10, 1}, {20, 2}, {30, 3}, {40, 4}, {50, 5}});
        validateCount(client,
                      "select a, count(*) over () from tu order by a",
                      new long[][]{{10, 5}, {20, 5}, {30, 5}, {40, 5}, {50, 5}});

        // Try some simple cases with partition by and order by
        // on tables that have duplicate order keys.
        client.callProcedure("tm.insert", 10, 1);
        client.callProcedure("tm.insert", 10, 1);
        client.callProcedure("tm.insert", 10, 2);
        client.callProcedure("tm.insert", 20, 1);
        client.callProcedure("tm.insert", 30, 3);
        client.callProcedure("tm.insert", 30, 1);
        client.callProcedure("tm.insert", 40, 2);
        client.callProcedure("tm.insert", 40, 3);
        client.callProcedure("tm.insert", 50, 2);
        validateCount(client,
                      "select a, count(*) over (partition by a) from tm order by a",
                      new long[][]{{10, 3}, {10, 3}, {10, 3},
                                   {20, 1},
                                   {30, 2}, {30, 2},
                                   {40, 2}, {40, 2},
                                   {50, 1}});

        validateCount(client,
                      "select a, count(*) over (partition by a order by b) from tm order by a",
                      new long[][]{{10, 2}, {10, 2}, {10, 3},
                                   {20, 1},
                                   {30, 1}, {30, 2},
                                   {40, 1}, {40, 2},
                                   {50, 1}});
        validateCount(client,
                      "select a, count(*) over (order by a) from tm order by a",
                      new long[][]{{10, 3}, {10, 3}, {10, 3},
                                   {20, 4},
                                   {30, 6}, {30, 6},
                                   {40, 8}, {40, 8},
                                   {50, 9}});
        validateCount(client,
                      "select a, count(*) over () from tm order by a",
                      new long[][]{{10, 9}, {10, 9}, {10, 9},
                                   {20, 9},
                                   {30, 9}, {30, 9},
                                   {40, 9}, {40, 9},
                                   {50, 9}});

        client.callProcedure("@AdHoc", "truncate table tm");
        client.callProcedure("@AdHoc", "truncate table tu");

        // Try some cases with nulls.  These answers are not
        // the same as the answers we would see with postsgresql,
        // since we sort nulls first and PG sorts them last.
        client.callProcedure("t.insert", 1,    1, 100);
        client.callProcedure("t.insert", 1,    2, 101);
        client.callProcedure("t.insert", 1,    3, 102);
        client.callProcedure("t.insert", 1, null, 103);
        client.callProcedure("t.insert", 1, null, 104);
        client.callProcedure("t.insert", 1, null, 105);
        client.callProcedure("t.insert", 2,    1, 200);
        client.callProcedure("t.insert", 2,    2, 201);
        client.callProcedure("t.insert", 2,    3, 202);
        client.callProcedure("t.insert", 2,    4, 203);
        client.callProcedure("t.insert", 2,    5, 204);
        client.callProcedure("t.insert", 2,    4, 205);

        validateCount(client,
                      "select a, c, count(*) over (partition by a order by b) from t order by a, c",
                      // Note: The nulls sort first.  So, since this is
                      //       order by b, the rows with b sort together,
                      //       making their count all 3.  The rest count
                      //       up from there.  This is not the same as
                      //       postgresql, where the nulls sort after.
                      new long[][]{{1, 100, 4},
                                   {1, 101, 5},
                                   {1, 102, 6},
                                   {1, 103, 3},
                                   {1, 104, 3},
                                   {1, 105, 3},
                                   {2, 200, 1},
                                   {2, 201, 2},
                                   {2, 202, 3},
                                   {2, 203, 5},
                                   {2, 204, 6},
                                   {2, 205, 5}});
        validateCount(client,
                      "select a, c, count(b) over (partition by a order by b) from t order by a, c",
                      // Note: Here the argument expression b in count(b) is
                      //       null for c == 103, 104, 105.  Since these sort
                      //       rows sort first, but the rows don't count,
                      //       we get counts of 0.
                      new long[][]{{1, 100, 1},
                                   {1, 101, 2},
                                   {1, 102, 3},
                                   {1, 103, 0},
                                   {1, 104, 0},
                                   {1, 105, 0},
                                   {2, 200, 1},
                                   {2, 201, 2},
                                   {2, 202, 3},
                                   {2, 203, 5},
                                   {2, 204, 6},
                                   {2, 205, 5}});
        validateCount(client,
                      "select a, c, count(b) over (partition by a order by b desc) from t order by a, c",
                      // Note: Here the argument expression b in count(b) is
                      //       null for c == 103, 104, 105.  Since these sort
                      //       rows sort last in descending order, but the rows
                      //       don't count, we get counts of 0.
                      new long[][]{{1, 100, 3},
                                   {1, 101, 2},
                                   {1, 102, 1},
                                   {1, 103, 3},
                                   {1, 104, 3},
                                   {1, 105, 3},
                                   {2, 200, 6},
                                   {2, 201, 5},
                                   {2, 202, 4},
                                   {2, 203, 3},
                                   {2, 204, 1},
                                   {2, 205, 3}});

        // Test that COUNT(E) works if E is a timestamp.
        long baseTime = TimestampType.millisFromJDBCformat("1953-06-10 00:00:00");
        TimestampType baseTimestamp = new TimestampType(baseTime);
        client.callProcedure("T_TIMESTAMP.insert", 100, 100, baseTimestamp);
        client.callProcedure("T_TIMESTAMP.insert", 100, 100, null);
        client.callProcedure("T_TIMESTAMP.insert", 100, 100, baseTimestamp);
        validateCount(client,
                      "select count(c) over() from T_TIMESTAMP",
                      new long[][]{{2}, {2}, {2}});
        // Try some things on an empty table.
        // We expect to get no answers anywhere.
        client.callProcedure("@AdHoc", "TRUNCATE TABLE T;");
        validateCount(client,
                      "select a, c, count(b) over (partition by a order by b desc) from t order by a, c",
                      new long[][]{});
        validateCount(client,
                      "select a, c, count(b) over (partition by a) from t order by a, c",
                      new long[][]{});
        validateCount(client,
                      "select a, c, count(b) over (order by b desc) from t order by a, c",
                      new long[][]{});
        validateCount(client,
                      "select a, c, count(b) over () from t order by a, c",
                      new long[][]{});
        validateCount(client,
                      "select a, c, count(*) over (partition by a order by b desc) from t order by a, c",
                      new long[][]{});
        validateCount(client,
                      "select a, c, count(*) over (partition by a) from t order by a, c",
                      new long[][]{});
        validateCount(client,
                      "select a, c, count(*) over (order by b desc) from t order by a, c",
                      new long[][]{});
        validateCount(client,
                      "select a, c, count(*) over () from t order by a, c",
                      new long[][]{});

    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestWindowFunctionSuite.class);
        boolean success = false;

        VoltProjectBuilder project;
        try {
            project = new VoltProjectBuilder();
            config = new LocalCluster("test-windowed-rank.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);

            project = new VoltProjectBuilder();
            config = new LocalCluster("test-windowed-rank.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
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
