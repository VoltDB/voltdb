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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestCommonTableExpressionsSuite extends RegressionSuite {

    public TestCommonTableExpressionsSuite(String name) {
        super(name);
    }

    static private void setupSchema(VoltProjectBuilder project) throws IOException {
        String literalSchema =
                "CREATE TABLE CTE_DATA ( "
                + "  ID   BIGINT PRIMARY KEY NOT NULL, "
                + "  NAME VARCHAR(1024), "
                + "  R    BIGINT, "
                + "  L    BIGINT);"
                + "CREATE TABLE RT ( "
                + "  ID   BIGINT PRIMARY KEY NOT NULL, "
                + "  NAME VARCHAR(1024), "
                + "  R    BIGINT, "
                + "  L    BIGINT);";

        project.addLiteralSchema(literalSchema);
        project.setUseDDLSchema(true);
    }

    private void initData(Client client) throws Exception {
        String procName = "CTE_DATA.insert";
        client.callProcedure(procName, 1111, "1111",    -1,     -1);
        client.callProcedure(procName, 1112, "1112",    -1,     -1);
        client.callProcedure(procName, 1121, "1121",    -1,     -1);
        client.callProcedure(procName, 1122, "1122",    -1,     -1);
        client.callProcedure(procName, 1211, "1211",    -1,     -1);
        client.callProcedure(procName, 1212, "1212",    -1,     -1);
        client.callProcedure(procName, 1221, "1221",    -1,     -1);
        client.callProcedure(procName, 1222, "1221",    -1,     -1);
        client.callProcedure(procName,  111,  "111",  1111,  1112);
        client.callProcedure(procName,  112,  "112",  1121,  1122);
        client.callProcedure(procName,  121,  "121",  1211,  1212);
        client.callProcedure(procName,  122,  "122",  1221,  1222);
        client.callProcedure(procName,   11,   "11",   111,   112);
        client.callProcedure(procName,   12,   "12",   121,   122);
        client.callProcedure(procName,    1,    "1",    11,    12);
    }

    public void testCTE() throws Exception {
        Client client = getClient();
        initData(client);

        String SQL =
                "with recursive rt(ID, NAME, L, R) as ("
                + "    select * from cte_data where id = 1 "
                + "        union all "
                + "    select cte_data.* from cte_data join rt on cte_data.id = rt.l "
                + ") "
                + "select * from rt order by rt.id";
        ClientResponse cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        Object[][] expectedTable = new Object[][] {
            {1, "1", 11, 12},
            {11, "11", 111, 112},
            {111, "111", 1111, 1112},
            {1111, "1111", -1, -1}
        };
        assertContentOfTable(expectedTable, vt);
    }

    public void testSimpleCTE() throws Exception {
        Client client = getClient();
        String SQL =
                "with recursive rt(ID, NAME, L, R) as ("
                + "    select * from cte_data where id = 1 "
                + "        union all "
                + "    select cte_data.* from cte_data join rt on cte_data.id = rt.l "
                + ") "
                + "select * from rt order by rt.id";
        String SQL1 =
                "select cte_data.* from cte_data join rt on cte_data.id = rt.l;";
        ClientResponse cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        assertFalse(vt.advanceRow());
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestCommonTableExpressionsSuite.class);
        boolean success = false;

        VoltProjectBuilder project;
        try {
            project = new VoltProjectBuilder();
            config = new LocalCluster("test-cte.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            setupSchema(project);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);

            project = new VoltProjectBuilder();
            config = new LocalCluster("test-cte.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
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
