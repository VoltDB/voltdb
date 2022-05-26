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

                // Nothing nullable, no partitions.
                +"CREATE TABLE T_STRING ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR(128) NOT NULL"
                + ");"

                // Nothing nullable, no partitions.
                +"CREATE TABLE T_STRING_INLINE ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR(60 bytes) NOT NULL"
                + ");"

                // C nullable, no partitions.
                +"CREATE TABLE T_STRING_C_NULL ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR(128)"
                + ");"

                // C nullable, no partitions.
                +"CREATE TABLE T_STRING_INLINE_C_NULL ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR(63 bytes)"
                + ");"

                // C nullable, partition on B.
                +"CREATE TABLE T_STRING_C_NULL_PB ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR(128) "
                + ");"
                + "PARTITION TABLE T_STRING_C_NULL_PB ON COLUMN B;"

                // C nullable, partition on B.
                +"CREATE TABLE T_STRING_INLINE_C_NULL_PB ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR(128) "
                + ");"
                + "PARTITION TABLE T_STRING_INLINE_C_NULL_PB ON COLUMN B;"

                // Nothing nullable, partition on C
                +"CREATE TABLE T_STRING_PC ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR(128) NOT NULL"
                + ");"
                + "PARTITION TABLE T_STRING_PC ON COLUMN C;"

                // Nothing nullable, partition on C
                +"CREATE TABLE T_STRING_INLINE_PC ("
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C VARCHAR(63 bytes) NOT NULL"
                + ");"
                + "PARTITION TABLE T_STRING_INLINE_PC ON COLUMN C;"

                + "CREATE TABLE T_PA (\n"
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C INTEGER NOT NULL"
                + ");\n"
                + "PARTITION TABLE T_PA ON COLUMN A;"

                + "CREATE TABLE T_PB (\n"
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C INTEGER"
                + ");\n"
                + "PARTITION TABLE T_PB ON COLUMN B;"

                + "CREATE TABLE T_PC (\n"
                + "  A INTEGER NOT NULL,"
                + "  B INTEGER NOT NULL,"
                + "  C INTEGER NOT NULL"
                + ");\n"
                + "PARTITION TABLE T_PC ON COLUMN C;"

                ;
        project.addLiteralSchema(literalSchema);
        project.setUseDDLSchema(true);
    }

    private static String LONG_REPL = "abcdefghij0123456789";
    private static String SHORT_REPL = "abcdefgh";
    /*
     * Make a string whose length is size * 20.  This is
     * a constant.  These can be used to test out-of-line strings.
     */
    private String makeLongString(int size, String repl) {
        StringBuffer sb = new StringBuffer();
        for (int idx = 0; idx < size; idx += 1) {
            sb.append(repl);
        }
        return sb.toString();
    }

    private void initTable(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
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

    private void initStringTable(Client client, String tableName, String repl) throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;

        String insertProcName = tableName + ".insert";
        cr = client.callProcedure("@AdHoc", "truncate table " + tableName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(5, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  2,    makeLongString(5, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(5, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //--------------------------------------
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  1,    makeLongString(5, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(5, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(5, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void initStringTableSomeNulls(Client client, String tableName, String repl)
                throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;

        String insertProcName = tableName + ".insert";
        cr = client.callProcedure("@AdHoc", "truncate table " + tableName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  1,    makeLongString(5, repl));
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
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  1,  3,    makeLongString(5, repl));
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
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(5, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  2,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //======================================
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(5, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(4, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(3, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(2, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure(insertProcName,  2,  3,    makeLongString(1, repl));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    private void initStringTableAllNulls(Client client, String tableName)
                throws NoConnectionsException, IOException, ProcCallException {
        ClientResponse cr;

        String insertProcName = tableName + ".insert";
        cr = client.callProcedure("@AdHoc", "truncate table " + tableName);
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


    private void testNoNulls(Client client,
                             String tableName,
                             String functionStrings[],
                             long[][] expected) throws Exception {
        ClientResponse cr;
        initTable(client, tableName);
        for (String functionString : functionStrings) {
            cr = client.callProcedure("@AdHoc",
                    String.format("select A, "
                                  + "     B, "
                                  + "     %s over (partition by A order by B) as R "
                                  + "from %s ORDER BY A, B, R;",
                                  functionString,
                                  tableName));
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            validateTableOfLongs(cr.getResults()[0], expected);
        }
    }

    private void testSomeNulls(Client client,
                               String tableName,
                               String functionString,
                               long[][] expected) throws Exception {
        ClientResponse cr;
        initTableWithSomeNulls(client, tableName);
        cr = client.callProcedure("@AdHoc",
                                  String.format("select A, "
                                                + "     B, "
                                                + "     %s over (partition by A order by B) as R "
                                                + "from %s ORDER BY A, B, R;",
                                                functionString,
                                                tableName));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);
    }

    private void testAllNulls(Client client,
                              String tableName,
                              String functionString,
                              long[][] expected) throws Exception {
        ClientResponse cr;
        initTableWithAllNulls(client, tableName);
        cr = client.callProcedure("@AdHoc",
                                  String.format("select A, "
                                                + "     B, "
                                                + "     %s over (partition by A order by B) as R "
                                                + " from %s ORDER BY A, B, R;",
                                                functionString,
                                                tableName));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(cr.getResults()[0], expected);
    }

    private void testStringsNoNulls(Client client,
                                    String tableName,
                                    String functionString,
                                    String repl,
                                    Object[][] expected) throws Exception {
        ClientResponse cr;
        initStringTable(client, tableName, repl);
        cr = client.callProcedure("@AdHoc",
                                  String.format("select A, "
                                                + "     B, "
                                                + "     %s over (partition by A order by B) as R "
                                                + "from %s ORDER BY A, B, R;",
                                                functionString,
                                                tableName));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateStringTable(cr.getResults()[0], expected);
    }

    private void testStringsSomeNulls(Client client,
                                      String tableName,
                                      String functionString,
                                      String repl,
                                      Object[][] expected) throws Exception {
        ClientResponse cr;
        initStringTableSomeNulls(client, tableName, repl);
        cr = client.callProcedure("@AdHoc",
                                  String.format("select A, "
                                                + "     B, "
                                                + "     %s over (partition by A order by B) as R "
                                                + "from %s ORDER BY A, B, R;",
                                                functionString,
                                                tableName));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateStringTable(cr.getResults()[0], expected);
    }

    private void testStringsAllNulls(Client client,
                                     String tableName,
                                     String functionString,
                                     Object[][] expected) throws Exception {
        ClientResponse cr;
        initStringTableAllNulls(client, tableName);
        cr = client.callProcedure("@AdHoc",
                                  String.format("select A, "
                                                + "     B, "
                                                + "     %s over (partition by A order by B) as R "
                                                + "from %s ORDER BY A, B, R;",
                                                functionString,
                                                tableName));
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateStringTable(cr.getResults()[0], expected);
    }

    private static final boolean IS_ENABLED = true;
    private static final boolean ISNOT_ENABLED = false;

    public void testAll() throws Exception {
        Client client = getClient();
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestMin();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestMax();
        }
        if (IS_ENABLED) {
            truncateAllTables(client);
            subtestSum();
        }
    }

    private void subtestMin() throws Exception {
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
        //
        // Test the replicated table, t.
        //
        testNoNulls(client, "t",
                    new String []{
                            // Test with the min at the end of the peer group.
                            "min(abs(5-C))",
                            // Test with the min at the middle of the peer group.
                            "min(abs(3-C))",
                            // Test with the min at the start of the peer group.
                            "min(abs(1-C))"
                    },
                    expected);
        //
        // Test the partitioned table t_pc.  This is
        // partitioned on column C, and C is the column
        // we are querying.
        //
        testNoNulls(client, "t_pc",
                    new String []{
                            // Test with the min at the end of the peer group.
                            "min(abs(5-C))",
                            // Test with the min at the middle of the peer group.
                            "min(abs(3-C))",
                            // Test with the min at the start of the peer group.
                            "min(abs(1-C))"
                    },
                    expected);


        //
        // Test the partitioned table t_pb.  This is
        // partitioned on column B but C is the column
        // we are querying.
        //
        testNoNulls(client,
                    "t_pb",
                    new String[] {
                            // Test with the min at the end of the peer group.
                            "min(abs(5-C))",
                            // Test with the min at the middle of the peer group.
                            "min(abs(3-C))",
                            // Test with the min at the start of the peer group.
                            "min(abs(1-C))"
                    },
                    expected);


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
        testSomeNulls(client,
                      "t",
                      "min(C)",
                      expectedWithSomeNulls);

        //
        // Can't test with nulls in a partition column because
        // that is nonsense.  But we can test with nulls in a
        // non-partitioned column.  Here B is the partition
        // column but C is the column with nulls.
        //
        testSomeNulls(client,
                      "t_pb",
                      "min(C)",
                      expectedWithSomeNulls);

        //
        // Now test with all nulls.  First, the replicated table.
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
        // Test the replicated table t.
        //
        testAllNulls(client,
                     "t",
                     "min(C)",
                     expectedWithAllNulls);
        //
        // Again, can't test with nulls in a partition column
        // because that is nonsense.  But we can test with nulls in
        // a non-partitioned column.
        //
        testAllNulls(client,
                     "t_pb",
                     "min(C)",
                     expectedWithAllNulls);

        ////////////////////////////////////////////////////////////////////
        //
        // Test with strings.
        //
        ////////////////////////////////////////////////////////////////////
        //
        // Test with partitioned string columns.
        //
        Object expectLongStringTable[][] = new Object[][] {
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            //======================================
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            //======================================
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            //--------------------------------------
            {  2L,  1L,    makeLongString(1, LONG_REPL)},
            {  2L,  1L,    makeLongString(1, LONG_REPL)},
            {  2L,  1L,    makeLongString(1, LONG_REPL)},
            {  2L,  1L,    makeLongString(1, LONG_REPL)},
            {  2L,  1L,    makeLongString(1, LONG_REPL)},
            //======================================
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            //======================================
            {  2L,  3L,    makeLongString(1, LONG_REPL)},
            {  2L,  3L,    makeLongString(1, LONG_REPL)},
            {  2L,  3L,    makeLongString(1, LONG_REPL)},
            {  2L,  3L,    makeLongString(1, LONG_REPL)},
            {  2L,  3L,    makeLongString(1, LONG_REPL)}
        };

        Object expectShortStringTable[][] = new Object[][] {
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            //======================================
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            //======================================
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            //--------------------------------------
            {  2L,  1L,    makeLongString(1, SHORT_REPL)},
            {  2L,  1L,    makeLongString(1, SHORT_REPL)},
            {  2L,  1L,    makeLongString(1, SHORT_REPL)},
            {  2L,  1L,    makeLongString(1, SHORT_REPL)},
            {  2L,  1L,    makeLongString(1, SHORT_REPL)},
            //======================================
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            //======================================
            {  2L,  3L,    makeLongString(1, SHORT_REPL)},
            {  2L,  3L,    makeLongString(1, SHORT_REPL)},
            {  2L,  3L,    makeLongString(1, SHORT_REPL)},
            {  2L,  3L,    makeLongString(1, SHORT_REPL)},
            {  2L,  3L,    makeLongString(1, SHORT_REPL)}
        };

        //
        // Test the replicated table t_string_c_null,
        // which is not partitioned and has C nullable string.
        //
        testStringsNoNulls(client,
                           "t_string",
                           "min(C)",
                           LONG_REPL,
                           expectLongStringTable);
        testStringsNoNulls(client,
                           "t_string_inline",
                           "min(C)",
                           SHORT_REPL,
                           expectShortStringTable);
        //
        // Test the partitioned table t_string_pc, which
        // is partitioned on the string column C.  Nothing
        // can be null here.
        //
        testStringsNoNulls(client,
                           "t_string_pc",
                           "min(C)",
                           LONG_REPL,
                           expectLongStringTable);
        testStringsNoNulls(client,
                           "t_string_inline_pc",
                           "min(C)",
                           SHORT_REPL,
                           expectShortStringTable);

        Object expectLongStringTableSomeNull[][] = new Object[][] {
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            {  1L,  1L,    makeLongString(1, LONG_REPL)},
            //======================================
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            {  1L,  2L,    makeLongString(1, LONG_REPL)},
            //======================================
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            {  1L,  3L,    makeLongString(1, LONG_REPL)},
            //--------------------------------------
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            //======================================
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            {  2L,  2L,    makeLongString(1, LONG_REPL)},
            //======================================
            {  2L,  3L,    makeLongString(1, LONG_REPL)},
            {  2L,  3L,    makeLongString(1, LONG_REPL)},
            {  2L,  3L,    makeLongString(1, LONG_REPL)},
            {  2L,  3L,    makeLongString(1, LONG_REPL)},
            {  2L,  3L,    makeLongString(1, LONG_REPL)}
        };

        Object expectShortStringTableSomeNull[][] = new Object[][] {
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            {  1L,  1L,    makeLongString(1, SHORT_REPL)},
            //======================================
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            {  1L,  2L,    makeLongString(1, SHORT_REPL)},
            //======================================
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            {  1L,  3L,    makeLongString(1, SHORT_REPL)},
            //--------------------------------------
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            //======================================
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            {  2L,  2L,    makeLongString(1, SHORT_REPL)},
            //======================================
            {  2L,  3L,    makeLongString(1, SHORT_REPL)},
            {  2L,  3L,    makeLongString(1, SHORT_REPL)},
            {  2L,  3L,    makeLongString(1, SHORT_REPL)},
            {  2L,  3L,    makeLongString(1, SHORT_REPL)},
            {  2L,  3L,    makeLongString(1, SHORT_REPL)}
        };
        // Test the replicated table t_string.
        testStringsSomeNulls(client,
                             "t_string_c_null",
                             "min(C)",
                             LONG_REPL,
                             expectLongStringTableSomeNull);
        testStringsSomeNulls(client,
                             "t_string_c_null",
                             "min(C)",
                             LONG_REPL,
                             expectLongStringTableSomeNull);
        //
        // We can't put nulls in the partition column, C,
        // but we can partition on the integer column B and
        // put nulls in the non-partitioned string column, C.
        //
        testStringsSomeNulls(client,
                             "t_string_c_null_pb",
                             "min(C)",
                             LONG_REPL,
                             expectLongStringTableSomeNull);
        testStringsSomeNulls(client,
                             "t_string_c_null_pb",
                             "min(C)",
                             SHORT_REPL,
                             expectShortStringTableSomeNull);

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

        // Test the replicated table t_string.
        testStringsAllNulls(client,
                            "t_string_c_null",
                            "min(C)",
                            expectStringTableAllNull);
        //
        // We can't put nulls in the partition column, C,
        // but we can partition on the integer column B and
        // put nulls in the non-partitioned string column, C.
        //
        testStringsAllNulls(client,
                            "t_string_c_null_pb",
                            "min(C)",
                            expectStringTableAllNull);
    }

    private void subtestMax() throws Exception {
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
        testNoNulls(client,
                    "t",
                    new String[] {
                            // Find the max at the end of the peer group.
                            "max(-1*abs(5-C))",
                            // Find the max at the middle of the peer group.
                            "max(-1*abs(3-C))",
                            // Find the max at the start of the peer group.
                            "max(-1*abs(1-C))"
                    },
                    expected);

        //
        // Test a partitioned table.
        //
        testNoNulls(client,
                    "t_pc",
                    new String[] {
                            // Find the max at the end of the peer group.
                            "max(-1*abs(5-C))",
                            // Find the max at the middle of the peer group.
                            "max(-1*abs(3-C))",
                            // Find the max at the start of the peer group.
                            "max(-1*abs(1-C))"
                    },
                    expected);

        //
        // Test a table partitioned on C but querying B.
        //
        testNoNulls(client,
                    "t_pb",
                    new String[] {
                            // Find the max at the end of the peer group.
                            "max(-1*abs(5-C))",
                            // Find the max at the middle of the peer group.
                            "max(-1*abs(3-C))",
                            // Find the max at the start of the peer group.
                            "max(-1*abs(1-C))"
                    },
                    expected);
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
        testSomeNulls(client,
                      "t",
                      "max(C)",
                      expectedWithSomeNulls);

        //
        // Test a partitioned table.  We can't put
        // nulls in the partition column, but we can
        // put nulls in another column.
        //
        testSomeNulls(client,
                      "t_pb",
                      "max(C)",
                      expectedWithSomeNulls);

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
        testAllNulls(client,
                     "t",
                     "max(C)",
                     expectedWithAllNulls);

        //
        // Test a partitioned table.  Put the nulls
        // in another, non-partitioned column.
        //
        testAllNulls(client,
                     "t_pb",
                     "max(C)",
                     expectedWithAllNulls);
        ////////////////////////////////////////////////////////////////////
        //
        // Test with strings.
        //
        ////////////////////////////////////////////////////////////////////
        //
        // Test strings with no nulls.
        //
        Object expectLongStringTable[][] = new Object[][] {
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            //======================================
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            //======================================
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            //--------------------------------------
            {  2L,  1L,    makeLongString(5, LONG_REPL)},
            {  2L,  1L,    makeLongString(5, LONG_REPL)},
            {  2L,  1L,    makeLongString(5, LONG_REPL)},
            {  2L,  1L,    makeLongString(5, LONG_REPL)},
            {  2L,  1L,    makeLongString(5, LONG_REPL)},
            //======================================
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            //======================================
            {  2L,  3L,    makeLongString(5, LONG_REPL)},
            {  2L,  3L,    makeLongString(5, LONG_REPL)},
            {  2L,  3L,    makeLongString(5, LONG_REPL)},
            {  2L,  3L,    makeLongString(5, LONG_REPL)},
            {  2L,  3L,    makeLongString(5, LONG_REPL)}
        };

        Object expectShortStringTable[][] = new Object[][] {
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            //======================================
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            //======================================
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            //--------------------------------------
            {  2L,  1L,    makeLongString(5, SHORT_REPL)},
            {  2L,  1L,    makeLongString(5, SHORT_REPL)},
            {  2L,  1L,    makeLongString(5, SHORT_REPL)},
            {  2L,  1L,    makeLongString(5, SHORT_REPL)},
            {  2L,  1L,    makeLongString(5, SHORT_REPL)},
            //======================================
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            //======================================
            {  2L,  3L,    makeLongString(5, SHORT_REPL)},
            {  2L,  3L,    makeLongString(5, SHORT_REPL)},
            {  2L,  3L,    makeLongString(5, SHORT_REPL)},
            {  2L,  3L,    makeLongString(5, SHORT_REPL)},
            {  2L,  3L,    makeLongString(5, SHORT_REPL)}
        };
        testStringsNoNulls(client,
                           "t_string",
                           "max(C)",
                           LONG_REPL,
                           expectLongStringTable);

        testStringsNoNulls(client,
                           "t_string_inline",
                           "max(C)",
                           SHORT_REPL,
                           expectShortStringTable);

        testStringsNoNulls(client,
                           "t_string_pc",
                           "max(C)",
                           LONG_REPL,
                           expectLongStringTable);


        testStringsNoNulls(client,
                           "t_string_inline_pc",
                           "max(C)",
                           SHORT_REPL,
                           expectShortStringTable);


        testStringsNoNulls(client,
                           "t_string_c_null_pb",
                           "max(C)",
                           LONG_REPL,
                           expectLongStringTable);

        testStringsNoNulls(client,
                           "t_string_inline_c_null_pb",
                           "max(C)",
                           SHORT_REPL,
                           expectShortStringTable);
        //
        // Test strings with some nulls.
        //
        Object expectLongStringTableSomeNull[][] = new Object[][] {
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            {  1L,  1L,    makeLongString(5, LONG_REPL)},
            //======================================
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            {  1L,  2L,    makeLongString(5, LONG_REPL)},
            //======================================
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            {  1L,  3L,    makeLongString(5, LONG_REPL)},
            //--------------------------------------
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            //======================================
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            {  2L,  2L,    makeLongString(5, LONG_REPL)},
            //======================================
            {  2L,  3L,    makeLongString(5, LONG_REPL)},
            {  2L,  3L,    makeLongString(5, LONG_REPL)},
            {  2L,  3L,    makeLongString(5, LONG_REPL)},
            {  2L,  3L,    makeLongString(5, LONG_REPL)},
            {  2L,  3L,    makeLongString(5, LONG_REPL)}
        };

        Object expectShortStringTableSomeNull[][] = new Object[][] {
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            {  1L,  1L,    makeLongString(5, SHORT_REPL)},
            //======================================
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            {  1L,  2L,    makeLongString(5, SHORT_REPL)},
            //======================================
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            {  1L,  3L,    makeLongString(5, SHORT_REPL)},
            //--------------------------------------
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            {  2L,  1L,    null},
            //======================================
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            {  2L,  2L,    makeLongString(5, SHORT_REPL)},
            //======================================
            {  2L,  3L,    makeLongString(5, SHORT_REPL)},
            {  2L,  3L,    makeLongString(5, SHORT_REPL)},
            {  2L,  3L,    makeLongString(5, SHORT_REPL)},
            {  2L,  3L,    makeLongString(5, SHORT_REPL)},
            {  2L,  3L,    makeLongString(5, SHORT_REPL)}
        };
        testStringsSomeNulls(client,
                             "t_string_c_null",
                             "max(C)",
                             LONG_REPL,
                             expectLongStringTableSomeNull);
        testStringsSomeNulls(client,
                             "t_string_inline_c_null",
                             "max(C)",
                             SHORT_REPL,
                             expectShortStringTableSomeNull);
        testStringsSomeNulls(client,
                             "t_string_c_null_pb",
                             "max(C)",
                             LONG_REPL,
                             expectLongStringTableSomeNull);
        testStringsSomeNulls(client,
                             "t_string_c_null_pb",
                             "max(C)",
                             SHORT_REPL,
                             expectShortStringTableSomeNull);
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
        testStringsAllNulls(client,
                            "t_string_c_null",
                            "max(C)",
                            expectStringTableAllNull);
        testStringsAllNulls(client,
                             "t_string_c_null_pb",
                             "max(C)",
                             expectStringTableAllNull);
    }

    private void subtestSum() throws Exception {
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

        //////////////////////////////////////////////////////////////////
        //
        // Integers.
        //
        //////////////////////////////////////////////////////////////////
        //
        // Test a replicated table.
        //
        testNoNulls(client,
                    "t",
                    new String[] {"sum(B+C)"},
                    expected);
        //
        // Test a table partitioned on c, query on c.
        //
        testNoNulls(client,
                    "t_pc",
                    new String[] {"sum(B+C)"},
                    expected);

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
            {  2L,  1L,   Long.MIN_VALUE},
            {  2L,  1L,   Long.MIN_VALUE},
            {  2L,  1L,   Long.MIN_VALUE},
            {  2L,  1L,   Long.MIN_VALUE},
            {  2L,  1L,   Long.MIN_VALUE},
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

        testSomeNulls(client,
                      "t",
                      "sum(B+C)",
                      expectedWithSomeNulls);
        testSomeNulls(client,
                      "t_pb",
                      "sum(B+C)",
                      expectedWithSomeNulls);

        long expectedWithAllNulls[] [] = new long[][] {
            {  1L,  1L, Long.MIN_VALUE},
            {  1L,  1L, Long.MIN_VALUE},
            {  1L,  1L, Long.MIN_VALUE},
            {  1L,  1L, Long.MIN_VALUE},
            {  1L,  1L, Long.MIN_VALUE},
            //======================================
            {  1L,  2L, Long.MIN_VALUE},
            {  1L,  2L, Long.MIN_VALUE},
            {  1L,  2L, Long.MIN_VALUE},
            {  1L,  2L, Long.MIN_VALUE},
            {  1L,  2L, Long.MIN_VALUE},
            //======================================
            {  1L,  3L, Long.MIN_VALUE},
            {  1L,  3L, Long.MIN_VALUE},
            {  1L,  3L, Long.MIN_VALUE},
            {  1L,  3L, Long.MIN_VALUE},
            {  1L,  3L, Long.MIN_VALUE},
            //--------------------------------------
            {  2L,  1L, Long.MIN_VALUE},
            {  2L,  1L, Long.MIN_VALUE},
            {  2L,  1L, Long.MIN_VALUE},
            {  2L,  1L, Long.MIN_VALUE},
            {  2L,  1L, Long.MIN_VALUE},
            //======================================
            {  2L,  2L, Long.MIN_VALUE},
            {  2L,  2L, Long.MIN_VALUE},
            {  2L,  2L, Long.MIN_VALUE},
            {  2L,  2L, Long.MIN_VALUE},
            {  2L,  2L, Long.MIN_VALUE},
            //======================================
            {  2L,  3L, Long.MIN_VALUE},
            {  2L,  3L, Long.MIN_VALUE},
            {  2L,  3L, Long.MIN_VALUE},
            {  2L,  3L, Long.MIN_VALUE},
            {  2L,  3L, Long.MIN_VALUE}
        };
        testAllNulls(client,
                     "t",
                     "sum(B+C)",
                     expectedWithAllNulls);
        testAllNulls(client,
                     "t_pb",
                     "sum(B+C)",
                     expectedWithAllNulls);
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
