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

                +"CREATE TABLE T_STRING_NULL ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR "
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

    /*
     * Make a string whose length is size * 8.  This is
     * a constant.
     */
    private String makeLongString(int size) {
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < size; idx += 1) {
            sb.append("abcdefgh");
        }
        return sb.toString();
    }

    private void initTable(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;
        String insertProcName = tableName + ".insert";
        cr = client.callProcedure("@AdHoc", "truncate table " + tableName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("t.insert",  1,  1,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  2,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  3,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure(insertProcName,  2,  1,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  2,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  3,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void initTableWithSomeNulls(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;
        String insertProcName = tableName + ".insert";
        cr = client.callProcedure("@AdHoc", "truncate table " + tableName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  2,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  3,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  2,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    4);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  3,    1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    2);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    3);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    4);;
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    5);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void initTableWithAllNulls(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;

        String insertProcName = tableName + ".insert";
        cr = client.callProcedure("@AdHoc", "truncate table " + tableName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure(insertProcName,  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3, null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void initStringTable(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;

        String insertProcName = tableName + ".insert";
        cr = client.callProcedure("@AdHoc", "truncate table t_string_null");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void initStringTableSomeNulls(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;

        String insertProcName = tableName + ".insert";
        cr = client.callProcedure("@AdHoc", "truncate table t_string_null");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(5));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(4));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(3));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(2));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(1));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }
    private void initStringTableAllNulls(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;

        String insertProcName = tableName + ".insert";
        cr = client.callProcedure("@AdHoc", "truncate table t_string_null");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    null);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void validateStringTable(VoltTable voltTable, Object[][] expect) {
        assertEquals(expect.length, voltTable.getRowCount());
        for (int rowIdx = 0; rowIdx < expect.length; rowIdx += 1) {
            Object expectRow[] = expect[rowIdx];
            assertEquals(expectRow.length, voltTable.getColumnCount());
            assertTrue(voltTable.advanceRow());
            for (int colIdx = 0; colIdx < expectRow.length; colIdx += 1) {
                Object expVal = expectRow[colIdx];
                Object actVal;
                if (colIdx == 2) {
                    actVal = voltTable.getString(colIdx);
                } else {
                    actVal = voltTable.getLong(colIdx);
                }
                if (voltTable.wasNull()) {
                    assertNull(expVal);
                } else {
                    assertEquals(expVal, actVal);
                }
            }
        }
    }

    public void testMin() throws Exception {
        Client client = getClient();
        ClientResponse cr;

        //
        // First, test with no nulls.  Look for the min at the
        // beginning of the order by peer group, the middle and the
        // end.
        //
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
        initTable(client, "t");
        //
        // Test the replicated table, t.
        //
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

        //
        // Test the partitioned table t_pc.
        //
        initTable(client, "t_pc");
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(abs(5-C)) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        // Find the min at the middle of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(abs(3-C)) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        // Find the min at the beginning of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(abs(1-C)) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);
        //
        // Now test with some nulls.
        //
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
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
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

        //
        // Test the replicated table, T.
        //
        initTableWithSomeNulls(client, "t");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithSomeNulls);

        //
        // Test with a partitioned table.
        //
        initTableWithSomeNulls(client, "t_pc");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithSomeNulls);
        //
        // Now test with all nulls.  First, the replicated table.
        //
        initTableWithAllNulls(client, "t");

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
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithAllNulls);

        //
        // Now test the partitioned table.
        //
        initTableWithAllNulls(client, "t_pc");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithAllNulls);

        //
        // Strings are not partitionable, so we don't test
        // partitioned tables below here.
        //
        Object expectStringTable[][] = new Object[][] {
            {  1L,  1L,    makeLongString(1)},
            {  1L,  1L,    makeLongString(1)},
            {  1L,  1L,    makeLongString(1)},
            {  1L,  1L,    makeLongString(1)},
            {  1L,  1L,    makeLongString(1)},
            //======================================
            {  1L,  2L,    makeLongString(1)},
            {  1L,  2L,    makeLongString(1)},
            {  1L,  2L,    makeLongString(1)},
            {  1L,  2L,    makeLongString(1)},
            {  1L,  2L,    makeLongString(1)},
            //======================================
            {  1L,  3L,    makeLongString(1)},
            {  1L,  3L,    makeLongString(1)},
            {  1L,  3L,    makeLongString(1)},
            {  1L,  3L,    makeLongString(1)},
            {  1L,  3L,    makeLongString(1)},
            //--------------------------------------
            {  2L,  1L,    makeLongString(1)},
            {  2L,  1L,    makeLongString(1)},
            {  2L,  1L,    makeLongString(1)},
            {  2L,  1L,    makeLongString(1)},
            {  2L,  1L,    makeLongString(1)},
            //======================================
            {  2L,  2L,    makeLongString(1)},
            {  2L,  2L,    makeLongString(1)},
            {  2L,  2L,    makeLongString(1)},
            {  2L,  2L,    makeLongString(1)},
            {  2L,  2L,    makeLongString(1)},
            //======================================
            {  2L,  3L,    makeLongString(1)},
            {  2L,  3L,    makeLongString(1)},
            {  2L,  3L,    makeLongString(1)},
            {  2L,  3L,    makeLongString(1)},
            {  2L,  3L,    makeLongString(1)}
        };
        initStringTable(client, "t");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T_STRING_NULL ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateStringTable(cr.getResults()[0], expectStringTable);


        Object expectStringTableSomeNull[][] = new Object[][] {
            {  1L,  1L,    makeLongString(1)},
            {  1L,  1L,    makeLongString(1)},
            {  1L,  1L,    makeLongString(1)},
            {  1L,  1L,    makeLongString(1)},
            {  1L,  1L,    makeLongString(1)},
            //======================================
            {  1L,  2L,    makeLongString(1)},
            {  1L,  2L,    makeLongString(1)},
            {  1L,  2L,    makeLongString(1)},
            {  1L,  2L,    makeLongString(1)},
            {  1L,  2L,    makeLongString(1)},
            //======================================
            {  1L,  3L,    makeLongString(1)},
            {  1L,  3L,    makeLongString(1)},
            {  1L,  3L,    makeLongString(1)},
            {  1L,  3L,    makeLongString(1)},
            {  1L,  3L,    makeLongString(1)},
            //--------------------------------------
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            //======================================
            {  2L,  2L,    makeLongString(1)},
            {  2L,  2L,    makeLongString(1)},
            {  2L,  2L,    makeLongString(1)},
            {  2L,  2L,    makeLongString(1)},
            {  2L,  2L,    makeLongString(1)},
            //======================================
            {  2L,  3L,    makeLongString(1)},
            {  2L,  3L,    makeLongString(1)},
            {  2L,  3L,    makeLongString(1)},
            {  2L,  3L,    makeLongString(1)},
            {  2L,  3L,    makeLongString(1)}
        };
        initStringTableSomeNulls(client, "t_string_null");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T_STRING_NULL ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        validateStringTable(cr.getResults()[0], expectStringTableSomeNull);

        Object expectStringTableAllNull[][] = new Object[][] {
            {  1L,  1L,    null},
            {  1L,  1L,    null},
            {  1L,  1L,    null},
            {  1L,  1L,    null},
            {  1L,  1L,    null},
            //======================================
            {  1L,  2L,    null},
            {  1L,  2L,    null},
            {  1L,  2L,    null},
            {  1L,  2L,    null},
            {  1L,  2L,    null},
            //======================================
            {  1L,  3L,    null},
            {  1L,  3L,    null},
            {  1L,  3L,    null},
            {  1L,  3L,    null},
            {  1L,  3L,    null},
            //--------------------------------------
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            //======================================
            {  2L,  2L,    null},
            {  2L,  2L,    null},
            {  2L,  2L,    null},
            {  2L,  2L,    null},
            {  2L,  2L,    null},
            //======================================
            {  2L,  3L,    null},
            {  2L,  3L,    null},
            {  2L,  3L,    null},
            {  2L,  3L,    null},
            {  2L,  3L,    null},
        };

        initStringTableAllNulls(client, "t_string_null");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, min(C) over (partition by A order by B) as R from T_STRING_NULL ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateStringTable(cr.getResults()[0], expectStringTableAllNull);

    }
    public void testMax() throws Exception {
        Client client = getClient();
        ClientResponse cr;

        //
        // Test with no nulls.  Look for the max at the
        // beginning, end and middle of the order by peer
        // group.
        //
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
        //
        // Test the replicated table.
        //
        initTable(client, "t");
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

        //
        // Test the partitioned table.
        //
        initTable(client, "t_pc");
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(-1*abs(5-C)) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        // Find the min at the middle of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(-1*abs(3-C)) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        // Find the min at the beginning of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(-1*abs(1-C)) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);
        //
        // Test with some nulls.
        //
        long expectedWithSomeNulls[] [] = new long[][] {
            {  1L,  1L,    5L},
            {  1L,  1L,    5L},
            {  1L,  1L,    5L},
            {  1L,  1L,    5L},
            {  1L,  1L,    5L},
            //======================================
            {  1L,  2L,    5L},
            {  1L,  2L,    5L},
            {  1L,  2L,    5L},
            {  1L,  2L,    5L},
            {  1L,  2L,    5L},
            //======================================
            {  1L,  3L,    5L},
            {  1L,  3L,    5L},
            {  1L,  3L,    5L},
            {  1L,  3L,    5L},
            {  1L,  3L,    5L},
            //--------------------------------------
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            {  2L,  1L,    Long.MIN_VALUE},
            //======================================
            {  2L,  2L,    5L},
            {  2L,  2L,    5L},
            {  2L,  2L,    5L},
            {  2L,  2L,    5L},
            {  2L,  2L,    5L},
            //======================================
            {  2L,  3L,    5L},
            {  2L,  3L,    5L},
            {  2L,  3L,    5L},
            {  2L,  3L,    5L},
            {  2L,  3L,    5L}
        };

        //
        // Test the replicated table.
        //
        initTableWithSomeNulls(client, "t");
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithSomeNulls);

        //
        // Test a partitioned table.
        //
        initTableWithSomeNulls(client, "t_pc");
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(C) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithSomeNulls);

        //
        // Test with all nulls.
        //
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
        //
        // Test a replicated table.
        //
        initTableWithAllNulls(client, "t");
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithAllNulls);

        //
        // Test a partitioned table.
        //
        initTableWithAllNulls(client, "t_pc");
        // Find the min at the end of the peer group.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(C) over (partition by A order by B) as R from T_pc ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithAllNulls);

        // Since strings are not a partitionable type,
        // we don't test partition tables below.

        //
        // Test strings with no nulls.
        //
        Object expectStringTable[][] = new Object[][] {
            {  1L,  1L,    makeLongString(5)},
            {  1L,  1L,    makeLongString(5)},
            {  1L,  1L,    makeLongString(5)},
            {  1L,  1L,    makeLongString(5)},
            {  1L,  1L,    makeLongString(5)},
            //======================================
            {  1L,  2L,    makeLongString(5)},
            {  1L,  2L,    makeLongString(5)},
            {  1L,  2L,    makeLongString(5)},
            {  1L,  2L,    makeLongString(5)},
            {  1L,  2L,    makeLongString(5)},
            //======================================
            {  1L,  3L,    makeLongString(5)},
            {  1L,  3L,    makeLongString(5)},
            {  1L,  3L,    makeLongString(5)},
            {  1L,  3L,    makeLongString(5)},
            {  1L,  3L,    makeLongString(5)},
            //--------------------------------------
            {  2L,  1L,    makeLongString(5)},
            {  2L,  1L,    makeLongString(5)},
            {  2L,  1L,    makeLongString(5)},
            {  2L,  1L,    makeLongString(5)},
            {  2L,  1L,    makeLongString(5)},
            //======================================
            {  2L,  2L,    makeLongString(5)},
            {  2L,  2L,    makeLongString(5)},
            {  2L,  2L,    makeLongString(5)},
            {  2L,  2L,    makeLongString(5)},
            {  2L,  2L,    makeLongString(5)},
            //======================================
            {  2L,  3L,    makeLongString(5)},
            {  2L,  3L,    makeLongString(5)},
            {  2L,  3L,    makeLongString(5)},
            {  2L,  3L,    makeLongString(5)},
            {  2L,  3L,    makeLongString(5)}
        };
        initStringTable(client, "t_string_null");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(C) over (partition by A order by B) as R from T_STRING_NULL ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateStringTable(cr.getResults()[0], expectStringTable);

        //
        // Test strings with some nulls.
        //
        Object expectStringTableSomeNull[][] = new Object[][] {
            {  1L,  1L,    makeLongString(5)},
            {  1L,  1L,    makeLongString(5)},
            {  1L,  1L,    makeLongString(5)},
            {  1L,  1L,    makeLongString(5)},
            {  1L,  1L,    makeLongString(5)},
            //======================================
            {  1L,  2L,    makeLongString(5)},
            {  1L,  2L,    makeLongString(5)},
            {  1L,  2L,    makeLongString(5)},
            {  1L,  2L,    makeLongString(5)},
            {  1L,  2L,    makeLongString(5)},
            //======================================
            {  1L,  3L,    makeLongString(5)},
            {  1L,  3L,    makeLongString(5)},
            {  1L,  3L,    makeLongString(5)},
            {  1L,  3L,    makeLongString(5)},
            {  1L,  3L,    makeLongString(5)},
            //--------------------------------------
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            //======================================
            {  2L,  2L,    makeLongString(5)},
            {  2L,  2L,    makeLongString(5)},
            {  2L,  2L,    makeLongString(5)},
            {  2L,  2L,    makeLongString(5)},
            {  2L,  2L,    makeLongString(5)},
            //======================================
            {  2L,  3L,    makeLongString(5)},
            {  2L,  3L,    makeLongString(5)},
            {  2L,  3L,    makeLongString(5)},
            {  2L,  3L,    makeLongString(5)},
            {  2L,  3L,    makeLongString(5)}
        };
        initStringTableSomeNulls(client, "t_string_null");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(C) over (partition by A order by B) as R from T_STRING_NULL ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateStringTable(cr.getResults()[0], expectStringTableSomeNull);

        //
        // Test strings with all nulls.
        //
        Object expectStringTableAllNull[][] = new Object[][] {
            {  1L,  1L,    null},
            {  1L,  1L,    null},
            {  1L,  1L,    null},
            {  1L,  1L,    null},
            {  1L,  1L,    null},
            //======================================
            {  1L,  2L,    null},
            {  1L,  2L,    null},
            {  1L,  2L,    null},
            {  1L,  2L,    null},
            {  1L,  2L,    null},
            //======================================
            {  1L,  3L,    null},
            {  1L,  3L,    null},
            {  1L,  3L,    null},
            {  1L,  3L,    null},
            {  1L,  3L,    null},
            //--------------------------------------
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            //======================================
            {  2L,  2L,    null},
            {  2L,  2L,    null},
            {  2L,  2L,    null},
            {  2L,  2L,    null},
            {  2L,  2L,    null},
            //======================================
            {  2L,  3L,    null},
            {  2L,  3L,    null},
            {  2L,  3L,    null},
            {  2L,  3L,    null},
            {  2L,  3L,    null}
        };
        initStringTableAllNulls(client, "t_string_null");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, max(C) over (partition by A order by B) as R from T_STRING_NULL ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateStringTable(cr.getResults()[0], expectStringTableAllNull);
    }

    public void testSum() throws Exception {
        Client client = getClient();
        ClientResponse cr;

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

        initTable(client, "t");
        // Find the sum.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);

        initTable(client, "t_pc");
        // Find the sum.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
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
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            {  2L,  1L,    0L},
            //======================================
            {  2L,  2L,   25L},
            {  2L,  2L,   25L},
            {  2L,  2L,   25L},
            {  2L,  2L,   25L},
            {  2L,  2L,   25L},
            //======================================
            {  2L,  3L,   55L},
            {  2L,  3L,   55L},
            {  2L,  3L,   55L},
            {  2L,  3L,   55L},
            {  2L,  3L,   55L}
        };

        initTableWithSomeNulls(client, "t");
        // Find the sum.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithSomeNulls);

        initTableWithSomeNulls(client, "t_pc");
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
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
        initTableWithAllNulls(client, "t");
        // Find the sum.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T ORDER BY A, B, R;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expectedWithAllNulls);

        initTableWithAllNulls(client, "t_pc");
        // Find the sum.
        cr = client.callProcedure("@AdHoc",
                                  "select A, B, sum(B+C) over (partition by A order by B) as R from T_PC ORDER BY A, B, R;");
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
