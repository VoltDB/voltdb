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
import org.voltdb.compiler.VoltProjectBuilder;

public class TestCommonTableExpressionsSuite extends RegressionSuite {

    public TestCommonTableExpressionsSuite(String name) {
        super(name);
    }

    static private void setupSchema(VoltProjectBuilder project) throws IOException {
        String literalSchema =
                ""
                + "CREATE TABLE CTE_DATA ( "
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
                + "SELECT * FROM EMP_PATH ORDER BY LEVEL, PATH; "
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
                + "SELECT * FROM EMP_PATH ORDER BY LEVEL, PATH; "
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
                + "\n\n"
                + "CREATE TABLE R2 ( "
                + "  ID      INTEGER NOT NULL, "
                + "  TINY    TINYINT, "
                + "  SMALL   SMALLINT, "
                + "  INT     INTEGER, "
                + "  BIG     BIGINT, "
                + "  NUM     FLOAT, "
                + "  DEC     DECIMAL, "
                + "  VCHAR_INLINE     VARCHAR(14), "
                + "  VCHAR_INLINE_MAX VARCHAR(63 BYTES), "
                + "  VCHAR            VARCHAR(64 BYTES), "
                + "  VCHAR_JSON       VARCHAR(1000), "
                + "  TIME    TIMESTAMP, "
                + "  VARBIN  VARBINARY(100), "
                + "  POINT   GEOGRAPHY_POINT, "
                + "  POLYGON GEOGRAPHY, "
                + "  IPV4    VARCHAR(15), "
                + "  IPV6    VARCHAR(60), "
                + "  VBIPV4  VARBINARY(4), "
                + "  VBIPV6  VARBINARY(16), "
                + "  PRIMARY KEY (ID) "
                + "); "
                + "CREATE INDEX IDX_R2_TINY ON R2 (TINY); "
                + "CREATE INDEX IDX_R2_BIG  ON R2 (BIG); "
                + "CREATE INDEX IDX_R2_DEC  ON R2 (DEC); "
                + "CREATE INDEX IDX_R2_VIM  ON R2 (VCHAR_INLINE_MAX); "
                + "CREATE INDEX IDX_R2_TIME ON R2 (TIME); "
                + "CREATE INDEX IDX_R2_VBIN ON R2 (VARBIN); "
                + "CREATE INDEX IDX_R2_POLY ON R2 (POLYGON);"
                + ""
                + "CREATE TABLE ENG13540_ONE_ROW ("
                + "  ID         BIGINT "
                + ");"
                + ""
                + "CREATE TABLE ENG13540_CTE_TABLE ("
                + "  ID         BIGINT, "
                + "  NAME       VARCHAR, "
                + "  LEFT_RENT  BIGINT, "
                + "  RIGHT_RENT BIGINT "
                + "); "
                + "CREATE TABLE R4 ( "
                + "  ID      INTEGER  DEFAULT 0, "
                + "  TINY    TINYINT  DEFAULT 0, "
                + "  SMALL   SMALLINT DEFAULT 0, "
                + "  INT     INTEGER  DEFAULT 0, "
                + "  BIG     BIGINT   DEFAULT 0, "
                + "  NUM     FLOAT    DEFAULT 0, "
                + "  DEC     DECIMAL  DEFAULT 0, "
                + "  VCHAR_INLINE      VARCHAR(14)       DEFAULT '0', "
                + "  VCHAR_INLINE_MAX  VARCHAR(63 BYTES) DEFAULT '0', "
                + "  VCHAR_OUTLINE_MIN VARCHAR(64 BYTES) DEFAULT '0' NOT NULL, "
                + "  VCHAR             VARCHAR           DEFAULT '0', "
                + "  VCHAR_JSON        VARCHAR(1000)     DEFAULT '0', "
                + "  TIME    TIMESTAMP       DEFAULT '2013-10-30 23:22:29', "
                + "  VARBIN  VARBINARY(100)  DEFAULT x'00', "
                + "  POINT   GEOGRAPHY_POINT, "
                + "  POLYGON GEOGRAPHY, "
                + "  IPV4    VARCHAR(15), "
                + "  IPV6    VARCHAR(60), "
                + "  VBIPV4  VARBINARY(4), "
                + "  VBIPV6  VARBINARY(16), "
                + "  PRIMARY KEY (ID, VCHAR_OUTLINE_MIN) "
                + "  ); "
                + "CREATE UNIQUE INDEX IDX_R4_TV  ON R4 (TINY, VCHAR); "
                + "CREATE UNIQUE INDEX IDX_R4_VSI ON R4 (VCHAR, SMALL, INT); "
                + "CREATE VIEW VR4 (VCHAR, BIG, "
                + "  ID, TINY, SMALL, INT, NUM, DEC, "
                + "  VCHAR_INLINE, VCHAR_INLINE_MAX, VCHAR_OUTLINE_MIN, VCHAR_JSON, TIME "
                + "  , VARBIN, POINT, POLYGON "
                + "  , IPV4, IPV6, VBIPV4, VBIPV6 "
                + ") AS "
                + "SELECT VCHAR, BIG, "
                + "  COUNT(*), SUM(TINY), MAX(SMALL), COUNT(VCHAR_INLINE_MAX), MAX(NUM), MIN(DEC), "
                + "  MAX(VCHAR_INLINE), MIN(VCHAR_INLINE_MAX), MAX(VCHAR_OUTLINE_MIN), MIN(VCHAR_JSON), MAX(TIME) "
                + "  , MIN(VARBIN), MAX(POINT), MIN(POLYGON) "
                + "  , MAX(IPV4), MIN(IPV6), MAX(VBIPV4), MIN(VBIPV6) "
                + "FROM R4 WHERE VCHAR_INLINE < 'N' "
                + "GROUP BY VCHAR, BIG; "
                + "CREATE INDEX IDX_VR4_VB  ON VR4 (VCHAR, BIG)             WHERE BIG >= 0; "
                + "CREATE INDEX IDX_VR4_IDV ON VR4 (INT, DEC, VCHAR_INLINE) WHERE VCHAR_INLINE < 'a'; "
                + "CREATE INDEX IDX_VR4_VMB ON VR4 (VCHAR_INLINE_MAX, BIG)  WHERE BIG >= 0 AND VCHAR_INLINE_MAX IS NOT NULL;"
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

    public void insertOneRow(Client client) throws Exception {
        String procName = "ENG13540_ONE_ROW.insert";
        client.callProcedure(procName, (Long)null);
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
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        Object[][] expectedTable = new Object[][] {
            {1, "1", 11, 12},
            {11, "11", 111, 112},
            {111, "111", 1111, 1112},
            {1111, "1111", -1, -1}
        };
        assertContentOfTable(expectedTable, vt);

        // Test with a subquery in the base case.  This
        // should get exactly the same answer as before.
        SQL =
                "with recursive rt(ID, NAME, L, R) as ("
                + "    ( select * from cte_data where id = 1 ) "
                + "        union all "
                + "    ( select cte_data.* from cte_data join rt on cte_data.id = rt.l ) "
                + ") "
                + "select * from rt order by rt.id";

        cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
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

    public void testEng13540Crash() throws Exception {
        Client client = getClient();
        insertOneRow(client);
        String SQL;
        // Potentially bad string.
        SQL =   "WITH RECURSIVE CTE(ID, NAME, LEFT_RENT, RIGHT_RENT) AS ( "
              + "  SELECT -1, CAST(NULL AS VARCHAR), -1, -1 FROM ENG13540_ONE_ROW "
              + "  UNION ALL "
              + "    SELECT L.ID, L.NAME, L.LEFT_RENT, L.RIGHT_RENT FROM ENG13540_CTE_TABLE L JOIN CTE R ON L.ID = R.LEFT_RENT ) "
              + "  SELECT ID FROM CTE;"
              ;
        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // This ought to work as well.
        SQL =   "WITH RECURSIVE CTE(ID, NAME, LEFT_RENT, RIGHT_RENT) AS ( "
              + "  SELECT CAST(-1 AS BIGINT), CAST(NULL AS VARCHAR), CAST(-1 AS BIGINT), CAST(-1 AS BIGINT) FROM ENG13540_ONE_ROW "
              + "UNION ALL "
              + "  SELECT L.ID, L.NAME, L.LEFT_RENT, L.RIGHT_RENT FROM ENG13540_CTE_TABLE L JOIN CTE R ON L.ID = R.LEFT_RENT )"
              + "SELECT ID FROM CTE;"
              ;
        cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // The answer is not important.  All that is
        // important is that the query runs to completion
        // and does not crash.
    }

    public void testEng13534Crash() throws Exception {
        Client client = getClient();
        insertEmployees(client, "R_EMPLOYEES");
        ClientResponse cr;
        String SQL;
        SQL =   "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
             +  "    SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME FROM R_EMPLOYEES WHERE MANAGER_ID = 0 "
             +  "  UNION ALL "
             +  "    SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME FROM R_EMPLOYEES E JOIN EMP_PATH EP ON E.MANAGER_ID = EP.EMP_ID "
             +  ") "
             +  "SELECT * FROM EMP_PATH;"
             ;
        cr = client.callProcedure("@AdHoc", SQL);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        // Do we care about the answer here?
        // I don't really think so.  I think all that is
        // important is that the query runs to completion
        // and does not crash.
    }

    public void testEng13500Crash() throws Exception {
        Client client = getClient();

        assertSuccessfulDML(client,
                "insert into r2 values (0, "
                + "100, 101, 102, 103, "
                + "104.0, 105.0, "
                + "'foo', 'bar', 'baz', 'json', "
                + "null, "
                + "x'aabbccddeeff', "
                + "null, null, "
                + "'100.100.100.100', 'asdf', "
                + "x'00aa', x'bbcc');");

        // In this bug we weren't resolving the column indices
        // for the child of the common table plan node, that is,
        // the base query.
        String query = "WITH RECURSIVE rcte(RCTE_C1) AS ( "
                + "SELECT * FROM (SELECT MAX(VCHAR) FROM R2 WHERE IPV4 != 'dogs') AS DTBL "
                + "UNION ALL "
                + "SELECT 'x.' || RCTE_C1 "
                + "FROM rcte "
                + "WHERE CHAR_LENGTH(RCTE_C1) < 10  "
                + ") "
                + "SELECT * FROM rcte ORDER BY 1;";

        ClientResponse cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        assertContentOfTable(new Object[][] {
            {"baz"},
            {"x.baz"},
            {"x.x.baz"},
            {"x.x.x.baz"},
            {"x.x.x.x.baz"}},
                vt);
    }


    public void testRuntimeErrors() throws Exception {
        Client client = getClient();

        insertEmployees(client, "R_EMPLOYEES");

        // Test runtime errors that occur during evaluation of common tables.
        // When run in memcheck mode, there should be no leaks.

        String query = "WITH RECURSIVE RCTE (N) AS ( "
                + "  SELECT 3 AS N "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  SELECT 100 / (N - 1) FROM RCTE "
                + ") "
                + "SELECT * FROM RCTE; ";

        verifyStmtFails(client, query, "Attempted to divide 100 by 0");

        // Try another with a runtime error in the base query.
        query = "WITH RECURSIVE RCTE (N) AS ( "
                + "  SELECT 100 / CHAR_LENGTH('') AS N "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  SELECT N FROM RCTE "
                + ") "
                + "SELECT * FROM RCTE; ";
        verifyStmtFails(client, query, "Attempted to divide 100 by 0");

        // Non-recursive query
        query = "WITH RCTE (N) AS ( "
                + "  SELECT 100 / CHAR_LENGTH('') AS N "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + ") "
                + "SELECT * FROM RCTE; ";
        verifyStmtFails(client, query, "Attempted to divide 100 by 0");
    }

    public void testNonRecursiveSetOps() throws Exception {
        Client client = getClient();

        insertEmployees(client, "R_EMPLOYEES");

        // Check that UNION ALL behaves in the traditional way
        String query = "WITH RCTE (LAST_NAME) AS ( "
                + "  SELECT LAST_NAME "
                + "  FROM R_EMPLOYEES  "
                + "  WHERE LAST_NAME IN ('Bloom', 'Fox') "
                + "UNION ALL "
                + "  SELECT LAST_NAME "
                + "  FROM R_EMPLOYEES  "
                + "  WHERE EMP_ID IN (170, 173) "
                + ") "
                + "SELECT * FROM RCTE ORDER BY LAST_NAME; ";
        ClientResponse cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        assertContentOfTable(new Object[][] {
            {"Bloom"}, {"Fox"}, {"Fox"}, {"Kumar"}},
                vt);

        // UNION with no ALL---dupes removed
        query = "WITH RCTE (LAST_NAME) AS ( "
                + "  SELECT LAST_NAME "
                + "  FROM R_EMPLOYEES  "
                + "  WHERE LAST_NAME IN ('Bloom', 'Fox') "
                + "UNION "
                + "  SELECT LAST_NAME "
                + "  FROM R_EMPLOYEES  "
                + "  WHERE EMP_ID IN (170, 173) "
                + ") "
                + "SELECT * FROM RCTE ORDER BY LAST_NAME; ";
        cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(new Object[][] {
            {"Bloom"}, {"Fox"}, {"Kumar"}},
                vt);

        // INTERSECT
        query = "WITH RCTE (LAST_NAME) AS ( "
                + "  SELECT LAST_NAME "
                + "  FROM R_EMPLOYEES  "
                + "  WHERE LAST_NAME IN ('Bloom', 'Fox') "
                + "INTERSECT "
                + "  SELECT LAST_NAME "
                + "  FROM R_EMPLOYEES  "
                + "  WHERE EMP_ID IN (170, 173) "
                + ") "
                + "SELECT * FROM RCTE ORDER BY LAST_NAME; ";
        cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        vt = cr.getResults()[0];
        assertContentOfTable(new Object[][] {{"Fox"}}, vt);
    }

    public void testGroupByAndOrderBy() throws Exception {
        Client client = getClient();
        String query;
        ClientResponse cr;

        insertEmployees(client, "R_EMPLOYEES");

        // Non-recursive with OB clause
        query = "WITH THE_CTE AS ( "
                + "  SELECT MANAGER_ID, LAST_NAME "
                + "  FROM R_EMPLOYEES "
                + "  ORDER BY MANAGER_ID DESC, LAST_NAME "
                + "  LIMIT 2 "
                + ")"
                + "SELECT * FROM THE_CTE ORDER BY MANAGER_ID, LAST_NAME";
        cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertContentOfTable(new Object[][] {{148, "Bates"}, {148, "Bloom"}}, cr.getResults()[0]);

        // Similar to ENG-13530
        query = "WITH RECURSIVE RCTE (N) AS ( "
                + "SELECT COUNT(*) AS N "
                + "FROM R_EMPLOYEES "
                + "WHERE LAST_NAME LIKE 'B%' " // three rows
                + "UNION ALL "
                + "SELECT RCTE.N - 1 "
                + "FROM RCTE "
                + "WHERE RCTE.N >= 0 "
                + ") "
                + "SELECT * FROM RCTE ORDER BY N DESC";
        cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertContentOfTable(new Object[][] {{3}, {2}, {1}, {0}, {-1}}, cr.getResults()[0]);

        // GB clause in base query.
        // Compute the number of managed employees for each manager
        query = "WITH RECURSIVE EMP_PATH(EMP_ID, LAST_NAME, LEVEL, EMP_CNT) AS ( "
                + ""
                + "  SELECT "
                + "         RE1.EMP_ID, "
                + "         RE1.LAST_NAME, "
                + "         1 AS LEVEL, "
                + "         COUNT(*) AS EMP_CNT "
                + "  FROM R_EMPLOYEES AS RE1 INNER JOIN R_EMPLOYEES AS RE2 "
                + "         ON RE1.EMP_ID = RE2.MANAGER_ID "
                + "  WHERE RE1.MANAGER_ID IS NULL "
                + "  GROUP BY RE1.EMP_ID, RE1.LAST_NAME, LEVEL "
                + ""
                + "UNION ALL "
                + ""
                + "  SELECT E.EMP_ID, E.LAST_NAME, EP.LEVEL + 1 AS LEVEL, COUNT(*) AS EMP_CNT "
                + "  FROM R_EMPLOYEES AS E INNER JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + "    INNER JOIN R_EMPLOYEES AS E2 ON E2.MANAGER_ID = E.EMP_ID "
                + "  GROUP BY E.EMP_ID, E.LAST_NAME, EP.LEVEL + 1 "
                + ""
                + ") "
                + "SELECT LAST_NAME, EMP_CNT FROM EMP_PATH ORDER BY EMP_CNT DESC; ";
        cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertContentOfTable(new Object[][] {
            {"Cambrault", 6},
            {"Hunold", 4},
            {"King", 3},
            {"Errazuriz", 2},
            {"De Haan", 1}
        }, cr.getResults()[0]);

        // Recursive CTE with GB clause
        // For each employee, show level and number of managers
        // (Might be more interesting for a DAG instead of a tree)
        // Limit to 5 rows
        query = "WITH RECURSIVE EMP_PATH(EMP_ID, LAST_NAME, LEVEL, MGR_CNT) AS ( "
                + "  SELECT EMP_ID, LAST_NAME, 1 AS LEVEL, 0 AS MGR_CNT "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  SELECT E.EMP_ID, E.LAST_NAME, EP.LEVEL + 1 AS LEVEL, COUNT(*) AS MGR_CNT "
                + "  FROM R_EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + "  GROUP BY E.EMP_ID, E.LAST_NAME, EP.LEVEL + 1 "
                + ") "
                + "SELECT LAST_NAME, MGR_CNT FROM EMP_PATH ORDER BY LEVEL, LAST_NAME LIMIT 5; ";
        cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertContentOfTable(new Object[][] {
            {"King", 0},
            {"Cambrault", 1},
            {"De Haan", 1},
            {"Errazuriz", 1},
            {"Ande", 1}
        }, cr.getResults()[0]);

        // Recursive statement that has order by clause and limit (parentheses required)
        // Just traverses the first child of each level.
        query = "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  (SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
                + "  FROM R_EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + "  ORDER BY E.EMP_ID LIMIT 1) "
                + ") "
                + "SELECT PATH FROM EMP_PATH ORDER BY LEVEL; ";
        cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertContentOfTable(new Object[][] {
            {"King"},
            {"King/De Haan"},
            {"King/De Haan/Hunold"},
            {"King/De Haan/Hunold/Ernst"}
        }, cr.getResults()[0]);

        // A recursive statement with both GB and OB/LIMIT clauses
        // Shows manager count for each employee by level, only
        // traversing first child of each level.
        query = "WITH RECURSIVE EMP_PATH(EMP_ID, LAST_NAME, LEVEL, MGR_CNT) AS ( "
                + "  SELECT EMP_ID, LAST_NAME, 1 AS LEVEL, 0 AS MGR_CNT "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL ( "
                + "  SELECT E.EMP_ID, E.LAST_NAME, EP.LEVEL + 1 AS LEVEL, COUNT(*) AS MGR_CNT "
                + "  FROM R_EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + "  GROUP BY E.EMP_ID, E.LAST_NAME, EP.LEVEL + 1 "
                + "  ORDER BY E.EMP_ID LIMIT 1 "
                + ") "
                + ") "
                + "SELECT LAST_NAME, MGR_CNT FROM EMP_PATH ORDER BY LEVEL, LAST_NAME; ";
        cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertContentOfTable(new Object[][] {
            {"King", 0},
            {"De Haan", 1},
            {"Hunold", 1},
            {"Ernst", 1}
        }, cr.getResults()[0]);
    }

    public void testMultipartitionPlans() throws Exception {
        // The CTE should only reference replicated tables,
        // but show that we can access the CTE from the collector fragment,
        // coordinator fragment, or both.
        Client client = getClient();

        insertEmployees(client, "EMPLOYEES");
        insertEmployees(client, "R_EMPLOYEES");

        // Add another couple of employees in the partitioned table, on a different partition.
        assertSuccessfulDML(client, "insert into employees values (1, 'Scott', 700, null)");
        assertSuccessfulDML(client, "insert into employees values (1, 'Schrute', 701, 700)");

        assertSuccessfulDML(client, "insert into r_employees values ('Scott', 700, null)");
        assertSuccessfulDML(client, "insert into r_employees values ('Schrute', 701, 700)");

        String query;
        ClientResponse cr;

        // This query must reference the common table from the coordinator fragment,
        // rather than the collector fragment.
        query = "with the_cte as ( "
                + "select * from r_employees "
                + "where last_name in ('King', 'Scott', 'Cambrault')"
                + ") "
                + "select the_cte.last_name, dtbl.cnt the_cnt from ( "
                //       A count of employees directly reporting to each manager
                + "      select e1.manager_id, count(*) as cnt "
                + "      from employees e1 "
                + "      group by e1.manager_id) as dtbl "
                + "    inner join the_cte on the_cte.emp_id = dtbl.manager_id "
                + "order by the_cnt desc";
        cr = client.callProcedure("@AdHoc", query);
        assertContentOfTable(new Object[][] {{"Cambrault", 6}, {"King", 3}, {"Scott", 1}}, cr.getResults()[0]);

        // Non-recursive CTE referenced in collector fragment of main query
        query = "with the_cte as ( "
                + "select * from r_employees "
                + "where last_name in ('King', 'Scott', 'Cambrault')"
                + ") "
                + "select the_cte.emp_id, e.last_name "
                + "from employees as e inner join the_cte "
                + "  on e.emp_id = the_cte.emp_id "
                + "order by the_cte.emp_id";
        cr = client.callProcedure("@AdHoc", query);
        assertContentOfTable(new Object[][] {{100, "King"}, {148, "Cambrault"}, {700, "Scott"}}, cr.getResults()[0]);

        // A plan that requires access to the common table on both collector and coordinator.
        query = "with the_cte as ( "
                + "select last_name, emp_id from r_employees "
                + ") "
                + "select the_cte.emp_id, dtbl.cnt the_cnt from ( "
                //       A count of employees directly reporting to each manager
                + "      select re.last_name as last_name, count(*) as cnt "
                + "      from employees e inner join the_cte re "
                + "             on e.manager_id = re.emp_id "
                + "      group by re.last_name) as dtbl"
                + "    inner join the_cte on the_cte.last_name = dtbl.last_name "
                + "order by the_cnt asc, the_cte.emp_id limit 3";
        cr = client.callProcedure("@AdHoc", query);
        assertContentOfTable(new Object[][] {{102, 1}, {700, 1}, {147, 2}}, cr.getResults()[0]);

        // Recursive examples
        // CTE referenced by collector fragment
        query = "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  (SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
                + "  FROM R_EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + "  ORDER BY E.EMP_ID LIMIT 1) "
                + ") "
                + "SELECT E.LAST_NAME, PATH "
                + "FROM EMP_PATH AS EP INNER JOIN EMPLOYEES AS E "
                + "  ON EP.EMP_ID = E.EMP_ID "
                + "ORDER BY PATH";
        cr = client.callProcedure("@AdHoc", query);
        assertContentOfTable(new Object[][] {
            {"King", "King"},
            {"De Haan", "King/De Haan"},
            {"Hunold", "King/De Haan/Hunold"},
            {"Ernst", "King/De Haan/Hunold/Ernst"},
            {"Scott", "Scott"}
        }, cr.getResults()[0]);

        // CTE referenced by coordinator fragment
        query = "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  (SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
                + "  FROM R_EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + "  ORDER BY E.EMP_ID LIMIT 1) "
                + ") "
                + "SELECT DTBL.LAST_NAME, DTBL.EMP_COUNT, EP.PATH "
                //       Count of employees managed by each manager
                + "FROM (SELECT E.LAST_NAME, COUNT(*) EMP_COUNT"
                + "      FROM EMPLOYEES AS E INNER JOIN R_EMPLOYEES AS RE "
                + "        ON E.EMP_ID = RE.MANAGER_ID "
                + "      GROUP BY E.LAST_NAME) AS DTBL "
                + "    INNER JOIN EMP_PATH AS EP "
                + "    ON EP.LAST_NAME = DTBL.LAST_NAME "
                + "ORDER BY EP.PATH";
        cr = client.callProcedure("@AdHoc", query);
        assertContentOfTable(new Object[][] {
            {"King", 3, "King"},
            {"De Haan", 1, "King/De Haan"},
            {"Hunold", 4, "King/De Haan/Hunold"},
            {"Scott", 1, "Scott"}
        }, cr.getResults()[0]);

        // CTE referenced by both coordinator and collector fragment
        query = "WITH RECURSIVE EMP_PATH(LAST_NAME, EMP_ID, MANAGER_ID, LEVEL, PATH) AS ( "
                + "  SELECT LAST_NAME, EMP_ID, MANAGER_ID, 1, LAST_NAME "
                + "  FROM R_EMPLOYEES "
                + "  WHERE MANAGER_ID IS NULL "
                + "UNION ALL "
                + "  (SELECT E.LAST_NAME, E.EMP_ID, E.MANAGER_ID, EP.LEVEL+1, EP.PATH || '/' || E.LAST_NAME "
                + "  FROM R_EMPLOYEES AS E JOIN EMP_PATH AS EP ON E.MANAGER_ID = EP.EMP_ID "
                + ") ) "
                + "SELECT DTBL.LAST_NAME, DTBL.EMP_COUNT, EP.PATH "
                //       Count of employees managed by each manager
                + "FROM (SELECT E.LAST_NAME, COUNT(*) EMP_COUNT"
                + "      FROM EMPLOYEES AS E INNER JOIN EMP_PATH AS EP "
                + "        ON E.EMP_ID = EP.MANAGER_ID "
                + "      GROUP BY E.LAST_NAME) AS DTBL "
                + "    INNER JOIN EMP_PATH AS EP "
                + "    ON EP.LAST_NAME = DTBL.LAST_NAME "
                + "ORDER BY EP.PATH";
        cr = client.callProcedure("@AdHoc", query);
        assertContentOfTable(new Object[][] {
            {"King", 3, "King"},
            {"Cambrault", 6, "King/Cambrault"},
            {"De Haan", 1, "King/De Haan"},
            {"Hunold", 4, "King/De Haan/Hunold"},
            {"Errazuriz", 2, "King/Errazuriz"},
            {"Scott", 1, "Scott"}
        }, cr.getResults()[0]);
    }

    public void testGrammarGeneratorQueries() throws Exception {
        Client client = getClient();
        client.callProcedure("@AdHoc", "insert into R4 (id, tiny, small, int, big, num, dec, "
                + "vchar_inline, vchar_inline_max, vchar_outline_min, vchar, vchar_json) "
                + "values ("
                + "0, " // id
                + "1, " // tiny
                + "2, " // small
                + "3, " // int
                + "4, " // big
                + "5.0, " // num
                + "6.0, " // dec
                + "'M', " // VCHAR_INLINE
                + "'bar', "  // VCHAR_INLINE_MAX
                + "'baz', "  // VCHAR_OUTLINE_MIN
                + "'aaa', "  // VCHAR
                + "'{}'"
                + ")");  // VCHAR_JSON

        String query = "WITH RECURSIVE rcte ("
                + "  RCTE_C1, RCTE_C2, RCTE_C3, "
                + "  RCTE_C4, RCTE_C5, RCTE_C6, "
                + "  RCTE_C7, RCTE_C8, RCTE_C9, "
                + "  N) "
                + "AS ("
                + "  SELECT VCHAR_INLINE_MAX, VCHAR, VCHAR, "
                + "    CONCAT(SUBSTRING('U', -730), VCHAR_INLINE_MAX), VCHAR, VCHAR, "
                + "    VCHAR, '6', VCHAR, "
                + "    10 AS N  "
                + "  FROM VR4  AS TA1   "
                + "UNION ALL "
                + "  SELECT RCTE_C1, RCTE_C2, RCTE_C3, "
                + "    RCTE_C4, RCTE_C5, RCTE_C6 || '', "
                + "    RCTE_C7, RCTE_C8, RCTE_C9,"
                + "    N - 1  "
                + "  FROM rcte "
                + "  WHERE RCTE_C6 < 'xWYC' AND N > 0"
                + ") "
                + "SELECT MIN(N) FROM rcte;";
        ClientResponse cr = client.callProcedure("@AdHoc", query);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        assertContentOfTable(new Object[][] {{0}}, cr.getResults()[0]);
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
