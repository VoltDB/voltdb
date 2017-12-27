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
                + "  L    BIGINT); "
                + "CREATE TABLE EMPLOYEES ( "
                + "  PART_KEY BIGINT NOT NULL, "
                + "  LAST_NAME VARCHAR(2048) NOT NULL, "
                + "  EMP_ID INTEGER NOT NULL, "
                + "  MANAGER_ID INTEGER "
                + "); "
                + "CREATE TABLE R_EMPLOYEES ( "
                + "  LAST_NAME VARCHAR(2048) NOT NULL, "
                + "  EMP_ID INTEGER NOT NULL, "
                + "  MANAGER_ID INTEGER "
                + "); "
                + "PARTITION TABLE EMPLOYEES ON COLUMN PART_KEY; "
                + "CREATE PROCEDURE EETestQuery AS "
                + "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
                + "  FROM EMPLOYEES "
                + "  WHERE PART_KEY = ? AND MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
                + "  FROM EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + ") "
                + "SELECT * FROM EMP_PATH; "
                + "PARTITION PROCEDURE EETestQuery ON TABLE EMPLOYEES COLUMN PART_KEY PARAMETER 0; "
                + "\n"
                + "CREATE PROCEDURE NonRecursiveCteMp AS "
                + "WITH BASE_EMP AS ( "
                + "  SELECT EMP_ID, LAST_NAME, MANAGER_ID "
                + "  FROM R_EMPLOYEES "
                + "  WHERE LAST_NAME = 'Errazuriz' "
                + ")"
                + "SELECT CTE.LAST_NAME, E.LAST_NAME "
                + "FROM BASE_EMP AS CTE INNER JOIN R_EMPLOYEES AS E ON CTE.EMP_ID = E.MANAGER_ID "
                + "ORDER BY 1, 2;"
                ;
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

    private final void inRow(Client client, String name, Integer id, Integer manid) throws Exception {
        client.callProcedure("EMPLOYEES.insert", 0, name, id, manid);
    }

    private final void inReplicatedRow(Client client, String name, Integer id, Integer manid) throws Exception {
        client.callProcedure("R_EMPLOYEES.insert", name, id, manid);
    }

    public void testEmployeesExample() throws Exception {
        Object[][] expectedTable = new Object[][] {
            {"King",      100, null,        1, "King"},
            {"Cambrault", 148, 100,         2, "King/Cambrault"},
            {"De Haan",   102, 100,         2, "King/De Haan"},
            {"Errazuriz", 147, 100,         2, "King/Errazuriz"},
            {"Bates",     172, 148,         3, "King/Cambrault/Bates"},
            {"Bloom",     169, 148,         3, "King/Cambrault/Bloom"},
            {"Fox",       170, 148,         3, "King/Cambrault/Fox"},
            {"Kumar",     173, 148,         3, "King/Cambrault/Kumar"},
            {"Ozer",      168, 148,         3, "King/Cambrault/Ozer"},
            {"Smith",     171, 148,         3, "King/Cambrault/Smith"},
            {"Hunold",    103, 102,         3, "King/De Haan/Hunold"},
            {"Ande",      166, 147,         3, "King/Errazuriz/Ande"},
            {"Banda",     167, 147,         3, "King/Errazuriz/Banda"},
            {"Austin",    105, 103,         4, "King/De Haan/Hunold/Austin"},
            {"Ernst",     104, 103,         4, "King/De Haan/Hunold/Ernst"},
            {"Lorentz",   107, 103,         4, "King/De Haan/Hunold/Lorentz"},
            {"Pataballa", 106, 103,         4, "King/De Haan/Hunold/Pataballa"}
        };
        Client client = getClient();
        inRow(client, "King",      100, null);
        inRow(client, "Cambrault", 148, 100);
        inRow(client, "Bates",     172, 148);
        inRow(client, "Bloom",     169, 148);
        inRow(client, "Fox",       170, 148);
        inRow(client, "Kumar",     173, 148);
        inRow(client, "Ozer",      168, 148);
        inRow(client, "Smith",     171, 148);
        inRow(client, "De Haan",   102, 100);
        inRow(client, "Hunold",    103, 102);
        inRow(client, "Austin",    105, 103);
        inRow(client, "Ernst",     104, 103);
        inRow(client, "Lorentz",   107, 103);
        inRow(client, "Pataballa", 106, 103);
        inRow(client, "Errazuriz", 147, 100);
        inRow(client, "Ande",      166, 147);
        inRow(client, "Banda",     167, 147);
        ClientResponse cr = client.callProcedure("EETestQuery", 0);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        assertContentOfTable(expectedTable, vt);
    }

    public void testAdHocCte() throws Exception {
        Object[][] expectedTable = new Object[][] {
            {"King",      100, null,        1, "King"},
            {"Cambrault", 148, 100,         2, "King/Cambrault"},
            {"De Haan",   102, 100,         2, "King/De Haan"},
            {"Errazuriz", 147, 100,         2, "King/Errazuriz"},
            {"Bates",     172, 148,         3, "King/Cambrault/Bates"},
            {"Bloom",     169, 148,         3, "King/Cambrault/Bloom"},
            {"Fox",       170, 148,         3, "King/Cambrault/Fox"},
            {"Kumar",     173, 148,         3, "King/Cambrault/Kumar"},
            {"Ozer",      168, 148,         3, "King/Cambrault/Ozer"},
            {"Smith",     171, 148,         3, "King/Cambrault/Smith"},
            {"Hunold",    103, 102,         3, "King/De Haan/Hunold"},
            {"Ande",      166, 147,         3, "King/Errazuriz/Ande"},
            {"Banda",     167, 147,         3, "King/Errazuriz/Banda"},
            {"Austin",    105, 103,         4, "King/De Haan/Hunold/Austin"},
            {"Ernst",     104, 103,         4, "King/De Haan/Hunold/Ernst"},
            {"Lorentz",   107, 103,         4, "King/De Haan/Hunold/Lorentz"},
            {"Pataballa", 106, 103,         4, "King/De Haan/Hunold/Pataballa"}
        };

        Client client = getClient();
        inReplicatedRow(client, "King",      100, null);
        inReplicatedRow(client, "Cambrault", 148, 100);
        inReplicatedRow(client, "Bates",     172, 148);
        inReplicatedRow(client, "Bloom",     169, 148);
        inReplicatedRow(client, "Fox",       170, 148);
        inReplicatedRow(client, "Kumar",     173, 148);
        inReplicatedRow(client, "Ozer",      168, 148);
        inReplicatedRow(client, "Smith",     171, 148);
        inReplicatedRow(client, "De Haan",   102, 100);
        inReplicatedRow(client, "Hunold",    103, 102);
        inReplicatedRow(client, "Austin",    105, 103);
        inReplicatedRow(client, "Ernst",     104, 103);
        inReplicatedRow(client, "Lorentz",   107, 103);
        inReplicatedRow(client, "Pataballa", 106, 103);
        inReplicatedRow(client, "Errazuriz", 147, 100);
        inReplicatedRow(client, "Ande",      166, 147);
        inReplicatedRow(client, "Banda",     167, 147);

        String cteQuery = "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
                + "  FROM R_EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + ") "
                + "SELECT * FROM EMP_PATH; ";

        ClientResponse cr = client.callProcedure("@AdHoc", cteQuery);
        assertEquals(cr.getStatusString(), ClientResponse.SUCCESS, cr.getStatus());
        assertContentOfTable(expectedTable, cr.getResults()[0]);
    }

    public void testNonRecursiveCte() throws Exception {
        Client client = getClient();

        inReplicatedRow(client, "King",      100, null);
        inReplicatedRow(client, "Cambrault", 148, 100);
        inReplicatedRow(client, "Bates",     172, 148);
        inReplicatedRow(client, "Bloom",     169, 148);
        inReplicatedRow(client, "Fox",       170, 148);
        inReplicatedRow(client, "Kumar",     173, 148);
        inReplicatedRow(client, "Ozer",      168, 148);
        inReplicatedRow(client, "Smith",     171, 148);
        inReplicatedRow(client, "De Haan",   102, 100);
        inReplicatedRow(client, "Hunold",    103, 102);
        inReplicatedRow(client, "Austin",    105, 103);
        inReplicatedRow(client, "Ernst",     104, 103);
        inReplicatedRow(client, "Lorentz",   107, 103);
        inReplicatedRow(client, "Pataballa", 106, 103);
        inReplicatedRow(client, "Errazuriz", 147, 100);
        inReplicatedRow(client, "Ande",      166, 147);
        inReplicatedRow(client, "Banda",     167, 147);

        Object[][] expectedTable = new Object[][]
                {{"Errazuriz", "Ande"},
                 {"Errazuriz", "Banda"}};

        ClientResponse cr = client.callProcedure("NonRecursiveCteMp");
        assertEquals(cr.getStatusString(), ClientResponse.SUCCESS, cr.getStatus());
        assertContentOfTable(expectedTable, cr.getResults()[0]);
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
            ((LocalCluster)config).m_nextIPCPort = 10000;
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
