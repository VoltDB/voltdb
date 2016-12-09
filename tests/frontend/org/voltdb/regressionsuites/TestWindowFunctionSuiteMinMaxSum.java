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
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestWindowFunctionSuiteMinMaxSum extends RegressionSuite {
    public TestWindowFunctionSuiteMinMaxSum(String name) {
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

    private void initTable(Client client) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", "truncate table t");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  1,  2,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  1,  3,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure("t.insert",  2,  1,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  2,  2,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  2,  3,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void initTableWithSomeNulls(Client client) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", "truncate table t");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  1,  2,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  1,  3,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure("t.insert",  2,  1,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  2,  2,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  2,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void initTableWithAllNulls(Client client) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", "truncate table t");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure("t.insert",  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure("t.insert",  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    public void testMin() throws Exception {
        Client client = getClient();
        long expected[] [] = new long[][] {
            {  1L,  1L,    0L},
            {  1L,  1L,    0L},
            {  1L,  1L,    0L},
            {  1L,  1L,    0L},
            {  1L,  1L,    0L},
            //======================================
            {  1L,  2L,    0L},
            {  1L,  2L,    0L},
            {  1L,  2L,    0L},
            {  1L,  2L,    0L},
            {  1L,  2L,    0L},
            //======================================
            {  1L,  3L,    0L},
            {  1L,  3L,    0L},
            {  1L,  3L,    0L},
            {  1L,  3L,    0L},
            {  1L,  3L,    0L},
            //--------------------------------------
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            //======================================
            {  2L,  2L,    0L},
            {  2L,  2L,    0L},
            {  2L,  2L,    0L},
            {  2L,  2L,    0L},
            {  2L,  2L,    0L},
            //======================================
            {  2L,  3L,    0L},
            {  2L,  3L,    0L},
            {  2L,  3L,    0L},
            {  2L,  3L,    0L},
            {  2L,  3L,    0L}
        };
        initTable(client);
        ClientResponse cr;
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(abs(5-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        // Find the min at the middle of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(abs(3-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        // Find the min at the beginning of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(abs(1-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        long expectedWithSomeNulls[] [] = new long[][] {
            {  1L,  1L,    1L},
            {  1L,  1L,    1L},
            {  1L,  1L,    1L},
            {  1L,  1L,    1L},
            {  1L,  1L,    1L},
            //======================================
            {  1L,  2L,    1L},
            {  1L,  2L,    1L},
            {  1L,  2L,    1L},
            {  1L,  2L,    1L},
            {  1L,  2L,    1L},
            //======================================
            {  1L,  3L,    1L},
            {  1L,  3L,    1L},
            {  1L,  3L,    1L},
            {  1L,  3L,    1L},
            {  1L,  3L,    1L},
            //--------------------------------------
            {  2L,  1L,    1L},
            {  2L,  1L,    1L},
            {  2L,  1L,    1L},
            {  2L,  1L,    1L},
            {  2L,  1L,    1L},
            //======================================
            {  2L,  2L,    1L},
            {  2L,  2L,    1L},
            {  2L,  2L,    1L},
            {  2L,  2L,    1L},
            {  2L,  2L,    1L},
            //======================================
            {  2L,  3L,    1L},
            {  2L,  3L,    1L},
            {  2L,  3L,    1L},
            {  2L,  3L,    1L},
            {  2L,  3L,    1L}
        };
        initTableWithSomeNulls(client);
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithSomeNulls);

        long expectedWithAllNulls[] [] = new long[][] {
            {  1L,  1L,    Long.MIN_VALUE},
            {  1L,  1L,    Long.MIN_VALUE},
            {  1L,  1L,    Long.MIN_VALUE},
            {  1L,  1L,    Long.MIN_VALUE},
            {  1L,  1L,    Long.MIN_VALUE},
            //======================================
            {  1L,  2L,    Long.MIN_VALUE},
            {  1L,  2L,    Long.MIN_VALUE},
            {  1L,  2L,    Long.MIN_VALUE},
            {  1L,  2L,    Long.MIN_VALUE},
            {  1L,  2L,    Long.MIN_VALUE},
            //======================================
            {  1L,  3L,    Long.MIN_VALUE},
            {  1L,  3L,    Long.MIN_VALUE},
            {  1L,  3L,    Long.MIN_VALUE},
            {  1L,  3L,    Long.MIN_VALUE},
            {  1L,  3L,    Long.MIN_VALUE},
            //--------------------------------------
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            //======================================
            {  2L,  2L,    Long.MIN_VALUE},
            {  2L,  2L,    Long.MIN_VALUE},
            {  2L,  2L,    Long.MIN_VALUE},
            {  2L,  2L,    Long.MIN_VALUE},
            {  2L,  2L,    Long.MIN_VALUE},
            //======================================
            {  2L,  3L,    Long.MIN_VALUE},
            {  2L,  3L,    Long.MIN_VALUE},
            {  2L,  3L,    Long.MIN_VALUE},
            {  2L,  3L,    Long.MIN_VALUE},
            {  2L,  3L,    Long.MIN_VALUE}
        };
        initTableWithAllNulls(client);
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithAllNulls);
    }

    public void testMax() throws Exception {
        Client client = getClient();
        long expected[] [] = new long[][] {
            {  1L,  1L,    0L},
            {  1L,  1L,    0L},
            {  1L,  1L,    0L},
            {  1L,  1L,    0L},
            {  1L,  1L,    0L},
            //======================================
            {  1L,  2L,    0L},
            {  1L,  2L,    0L},
            {  1L,  2L,    0L},
            {  1L,  2L,    0L},
            {  1L,  2L,    0L},
            //======================================
            {  1L,  3L,    0L},
            {  1L,  3L,    0L},
            {  1L,  3L,    0L},
            {  1L,  3L,    0L},
            {  1L,  3L,    0L},
            //--------------------------------------
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            //======================================
            {  2L,  2L,    0L},
            {  2L,  2L,    0L},
            {  2L,  2L,    0L},
            {  2L,  2L,    0L},
            {  2L,  2L,    0L},
            //======================================
            {  2L,  3L,    0L},
            {  2L,  3L,    0L},
            {  2L,  3L,    0L},
            {  2L,  3L,    0L},
            {  2L,  3L,    0L}
        };
        initTable(client);
        ClientResponse cr;
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(-1*abs(5-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        // Find the min at the middle of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(-1*abs(3-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        // Find the min at the beginning of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(-1*abs(1-C)) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        long expectedWithSomeNulls[] [] = new long[][] {
            {  1L,  1L,    1L},
            {  1L,  1L,    1L},
            {  1L,  1L,    1L},
            {  1L,  1L,    1L},
            {  1L,  1L,    1L},
            //======================================
            {  1L,  2L,    1L},
            {  1L,  2L,    1L},
            {  1L,  2L,    1L},
            {  1L,  2L,    1L},
            {  1L,  2L,    1L},
            //======================================
            {  1L,  3L,    1L},
            {  1L,  3L,    1L},
            {  1L,  3L,    1L},
            {  1L,  3L,    1L},
            {  1L,  3L,    1L},
            //--------------------------------------
            {  2L,  1L,    1L},
            {  2L,  1L,    1L},
            {  2L,  1L,    1L},
            {  2L,  1L,    1L},
            {  2L,  1L,    1L},
            //======================================
            {  2L,  2L,    1L},
            {  2L,  2L,    1L},
            {  2L,  2L,    1L},
            {  2L,  2L,    1L},
            {  2L,  2L,    1L},
            //======================================
            {  2L,  3L,    1L},
            {  2L,  3L,    1L},
            {  2L,  3L,    1L},
            {  2L,  3L,    1L},
            {  2L,  3L,    1L}
        };
        initTableWithSomeNulls(client);
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithSomeNulls);

        long expectedWithAllNulls[] [] = new long[][] {
            {  1L,  1L,    Long.MIN_VALUE},
            {  1L,  1L,    Long.MIN_VALUE},
            {  1L,  1L,    Long.MIN_VALUE},
            {  1L,  1L,    Long.MIN_VALUE},
            {  1L,  1L,    Long.MIN_VALUE},
            //======================================
            {  1L,  2L,    Long.MIN_VALUE},
            {  1L,  2L,    Long.MIN_VALUE},
            {  1L,  2L,    Long.MIN_VALUE},
            {  1L,  2L,    Long.MIN_VALUE},
            {  1L,  2L,    Long.MIN_VALUE},
            //======================================
            {  1L,  3L,    Long.MIN_VALUE},
            {  1L,  3L,    Long.MIN_VALUE},
            {  1L,  3L,    Long.MIN_VALUE},
            {  1L,  3L,    Long.MIN_VALUE},
            {  1L,  3L,    Long.MIN_VALUE},
            //--------------------------------------
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            //======================================
            {  2L,  2L,    Long.MIN_VALUE},
            {  2L,  2L,    Long.MIN_VALUE},
            {  2L,  2L,    Long.MIN_VALUE},
            {  2L,  2L,    Long.MIN_VALUE},
            {  2L,  2L,    Long.MIN_VALUE},
            //======================================
            {  2L,  3L,    Long.MIN_VALUE},
            {  2L,  3L,    Long.MIN_VALUE},
            {  2L,  3L,    Long.MIN_VALUE},
            {  2L,  3L,    Long.MIN_VALUE},
            {  2L,  3L,    Long.MIN_VALUE}
        };
        initTableWithAllNulls(client);
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithAllNulls);
    }

    public void testSum() throws Exception {
        Client client = getClient();
        long expected[] [] = new long[][] {
            {  1L,  1L,   20L},
            {  1L,  1L,   20L},
            {  1L,  1L,   20L},
            {  1L,  1L,   20L},
            {  1L,  1L,   20L},
            //======================================
            {  1L,  2L,   45L},
            {  1L,  2L,   45L},
            {  1L,  2L,   45L},
            {  1L,  2L,   45L},
            {  1L,  2L,   45L},
            //======================================
            {  1L,  3L,   75L},
            {  1L,  3L,   75L},
            {  1L,  3L,   75L},
            {  1L,  3L,   75L},
            {  1L,  3L,   75L},
            //--------------------------------------
            {  2L,  1L,   20L},
            {  2L,  1L,   20L},
            {  2L,  1L,   20L},
            {  2L,  1L,   20L},
            {  2L,  1L,   20L},
            //======================================
            {  2L,  2L,   45L},
            {  2L,  2L,   45L},
            {  2L,  2L,   45L},
            {  2L,  2L,   45L},
            {  2L,  2L,   45L},
            //======================================
            {  2L,  3L,   75L},
            {  2L,  3L,   75L},
            {  2L,  3L,   75L},
            {  2L,  3L,   75L},
            {  2L,  3L,   75L}
        };
        initTable(client);
        ClientResponse cr;
        // Find the sum.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        long expectedWithSomeNulls[] [] = new long[][] {
            {  1L,  1L,   20L},
            {  1L,  1L,   20L},
            {  1L,  1L,   20L},
            {  1L,  1L,   20L},
            {  1L,  1L,   20L},
            //======================================
            {  1L,  2L,   23L},
            {  1L,  2L,   23L},
            {  1L,  2L,   23L},
            {  1L,  2L,   23L},
            {  1L,  2L,   23L},
            //======================================
            {  1L,  3L,   53L},
            {  1L,  3L,   53L},
            {  1L,  3L,   53L},
            {  1L,  3L,   53L},
            {  1L,  3L,   53L},
            //--------------------------------------
            {  2L,  1L,   20L},
            {  2L,  1L,   20L},
            {  2L,  1L,   20L},
            {  2L,  1L,   20L},
            {  2L,  1L,   20L},
            //======================================
            {  2L,  2L,   45L},
            {  2L,  2L,   45L},
            {  2L,  2L,   45L},
            {  2L,  2L,   45L},
            {  2L,  2L,   45L},
            //======================================
            {  2L,  3L,   53L},
            {  2L,  3L,   53L},
            {  2L,  3L,   53L},
            {  2L,  3L,   53L},
            {  2L,  3L,   53L}
        };

        initTableWithSomeNulls(client);
        // Find the sum.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithSomeNulls);

        long expectedWithAllNulls[] [] = new long[][] {
            {  1L,  1L, 0},
            {  1L,  1L, 0},
            {  1L,  1L, 0},
            {  1L,  1L, 0},
            {  1L,  1L, 0},
            //======================================
            {  1L,  2L, 0},
            {  1L,  2L, 0},
            {  1L,  2L, 0},
            {  1L,  2L, 0},
            {  1L,  2L, 0},
            //======================================
            {  1L,  3L, 0},
            {  1L,  3L, 0},
            {  1L,  3L, 0},
            {  1L,  3L, 0},
            {  1L,  3L, 0},
            //--------------------------------------
            {  2L,  1L, 0},
            {  2L,  1L, 0},
            {  2L,  1L, 0},
            {  2L,  1L, 0},
            {  2L,  1L, 0},
            //======================================
            {  2L,  2L, 0},
            {  2L,  2L, 0},
            {  2L,  2L, 0},
            {  2L,  2L, 0},
            {  2L,  2L, 0},
            //======================================
            {  2L,  3L, 0},
            {  2L,  3L, 0},
            {  2L,  3L, 0},
            {  2L,  3L, 0},
            {  2L,  3L, 0}
        };
        initTableWithAllNulls(client);
        // Find the sum.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithAllNulls);
    }
    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestWindowFunctionSuiteMinMaxSum.class);
        boolean success = false;

        VoltProjectBuilder project;
        try {
            project = new VoltProjectBuilder();
            config = new LocalCluster("test-windowed-rank.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);
            /*
            project = new VoltProjectBuilder();
            config = new LocalCluster("test-windowed-rank.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);
            */
        }
        catch (IOException excp) {
            fail();
        }

        return builder;
    }

}
