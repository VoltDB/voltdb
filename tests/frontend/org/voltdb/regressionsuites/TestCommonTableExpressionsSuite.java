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
                +"\n"
                + "CREATE PROCEDURE EmployeeTreeSP AS "
                + "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
                + "  FROM EMPLOYEES "
                + "  WHERE PART_KEY = ? AND MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
                + "  FROM EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + ") "
                + "SELECT * FROM EMP_PATH; "
                + "PARTITION PROCEDURE EmployeeTreeSP ON TABLE EMPLOYEES COLUMN PART_KEY PARAMETER 0; "
                + "\n"
                + "CREATE PROCEDURE EmployeeTreeMP AS "
                + "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
                + "  FROM R_EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + ") "
                + "SELECT * FROM EMP_PATH; "
                + "\n"
                + "CREATE PROCEDURE EmployeeOneLevelSP AS "
                + "WITH BASE_EMP AS ( "
                + "  SELECT EMP_ID, LAST_NAME, MANAGER_ID "
                + "  FROM EMPLOYEES "
                + "  WHERE PART_KEY = ? AND LAST_NAME = 'Errazuriz' "
                + ")"
                + "SELECT CTE.LAST_NAME, E.LAST_NAME "
                + "FROM BASE_EMP AS CTE INNER JOIN EMPLOYEES AS E ON CTE.EMP_ID = E.MANAGER_ID "
                + "WHERE E.PART_KEY = ? "
                + "ORDER BY 1, 2; "
                + "PARTITION PROCEDURE EmployeeOneLevelSP ON TABLE EMPLOYEES COLUMN PART_KEY PARAMETER 0; "
                + "\n"
                + "CREATE PROCEDURE EmployeeOneLevelMP AS "
                + "WITH BASE_EMP AS ( "
                + "  SELECT EMP_ID, LAST_NAME, MANAGER_ID "
                + "  FROM R_EMPLOYEES "
                + "  WHERE LAST_NAME = 'Errazuriz' "
                + ")"
                + "SELECT CTE.LAST_NAME, E.LAST_NAME "
                + "FROM BASE_EMP AS CTE INNER JOIN R_EMPLOYEES AS E ON CTE.EMP_ID = E.MANAGER_ID "
                + "ORDER BY 1, 2; "
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
        ClientResponse cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        assertFalse(vt.advanceRow());
    }

    private static void insertEmployees(Client client, String tableName) throws Exception {
        Object employees[][] = new Object[][] {
            {"King",      100, null},
            {"Cambrault", 148, 100},
            {"Bates",     172, 148},
            {"Bloom",     169, 148},
            {"Fox",       170, 148},
            {"Kumar",     173, 148},
            {"Ozer",      168, 148},
            {"Smith",     171, 148},
            {"De Haan",   102, 100},
            {"Hunold",    103, 102},
            {"Austin",    105, 103},
            {"Ernst",     104, 103},
            {"Lorentz",   107, 103},
            {"Pataballa", 106, 103},
            {"Errazuriz", 147, 100},
            {"Ande",      166, 147},
            {"Banda",     167, 147}
        };

        boolean isReplicated = tableName.startsWith("R_");
        for (Object[] employee : employees) {
            ClientResponse cr;
            if (isReplicated) {
                cr = client.callProcedure(tableName + ".insert", employee[0], employee[1], employee[2]);
            }
            else {
                cr = client.callProcedure(tableName + ".insert", 0, employee[0], employee[1], employee[2]);
            }

            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        }
    }

    public void testEmployeesRecursive() throws Exception {
        final Object[][] EMPLOYEES_EXPECTED_RECURSIVE_RESULT = new Object[][] {
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
        insertEmployees(client, "EMPLOYEES");
        insertEmployees(client, "R_EMPLOYEES");

        ClientResponse cr;
        VoltTable vt;

        cr = client.callProcedure("EmployeeTreeSP", 0);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(EMPLOYEES_EXPECTED_RECURSIVE_RESULT, vt);

        // With the wrong partition key, should produce zero rows
        cr = client.callProcedure("EmployeeTreeSP", 1);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(new Object[][] {}, vt);

        // Now try the MP version that uses replicated tables
        cr = client.callProcedure("EmployeeTreeMP");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(EMPLOYEES_EXPECTED_RECURSIVE_RESULT, vt);

        // Try the same queries as above using @AdHoc.  Partitioning should be inferred in this case.

        String replicatedQuery = "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
                + "  FROM R_EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + ") "
                + "SELECT * FROM EMP_PATH ORDER BY LEVEL, PATH; ";

        // Ad hoc query that uses the replicated table R_EMPLOYEES
        cr = client.callProcedure("@AdHoc", replicatedQuery);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(EMPLOYEES_EXPECTED_RECURSIVE_RESULT, vt);

        // This produces a wrong answer and should have a guard.
        //        String partitionedQuery = "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
        //                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
        //                + "  FROM EMPLOYEES "
        //                + "  WHERE PART_KEY = 0 AND MANAGER_ID IS NULL "
        //                + "UNION ALL "
        //                + "  SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
        //                + "  FROM EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
        //                + ") "
        //                + "SELECT * FROM EMP_PATH ORDER BY LEVEL, PATH; ";
        //
        //        cr = client.callProcedure("@AdHoc", partitionedQuery);
        //        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        //        vt = cr.getResults()[0];
        //        assertContentOfTable(EMPLOYEES_EXPECTED_RECURSIVE_RESULT, vt);
    }

    public void testEmployeesNonRecursive() throws Exception {
        Client client = getClient();
        insertEmployees(client, "EMPLOYEES");
        insertEmployees(client, "R_EMPLOYEES");

        Object[][] expectedTable = new Object[][]
                {{"Errazuriz", "Ande"},
                 {"Errazuriz", "Banda"}};

        ClientResponse cr;
        VoltTable vt;

        cr = client.callProcedure("EmployeeOneLevelSP", 0, 0);
        assertEquals(cr.getStatusString(), ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(expectedTable, vt);

        // non-existent part_key produces empty result
        cr = client.callProcedure("EmployeeOneLevelSP", 1, 1);
        assertEquals(cr.getStatusString(), ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(new Object[][] {}, vt);

        cr = client.callProcedure("EmployeeOneLevelMP");
        assertEquals(cr.getStatusString(), ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(expectedTable, vt);

        // Try the same queries in ad hoc form
        String partitionedQuery = "WITH BASE_EMP AS ( "
                + "  SELECT EMP_ID, LAST_NAME, MANAGER_ID "
                + "  FROM EMPLOYEES "
                + "  WHERE PART_KEY = 0 AND LAST_NAME = 'Errazuriz' "
                + ")"
                + "SELECT CTE.LAST_NAME, E.LAST_NAME "
                + "FROM BASE_EMP AS CTE INNER JOIN EMPLOYEES AS E ON CTE.EMP_ID = E.MANAGER_ID "
                + "WHERE E.PART_KEY = 0 "
                + "ORDER BY 1, 2; ";
        cr = client.callProcedure("@AdHoc", partitionedQuery);
        assertEquals(cr.getStatusString(), ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(expectedTable, vt);

        String replicatedQuery = "WITH BASE_EMP AS ( "
                + "  SELECT EMP_ID, LAST_NAME, MANAGER_ID "
                + "  FROM R_EMPLOYEES "
                + "  WHERE LAST_NAME = 'Errazuriz' "
                + ")"
                + "SELECT CTE.LAST_NAME, E.LAST_NAME "
                + "FROM BASE_EMP AS CTE INNER JOIN R_EMPLOYEES AS E ON CTE.EMP_ID = E.MANAGER_ID "
                + "ORDER BY 1, 2; ";
        cr = client.callProcedure("@AdHoc", replicatedQuery);
        assertEquals(cr.getStatusString(), ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(expectedTable, vt);
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
