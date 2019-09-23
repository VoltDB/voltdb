/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.regressionsuites;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.sysprocs.AdHocNTBase;

public class TestCalciteJoinsSuite extends RegressionSuite {

    public TestCalciteJoinsSuite(String name) {
        super(name);
    }

    private static final long NULL_VALUE = Long.MIN_VALUE;

    private boolean useCalcite = AdHocNTBase.USING_CALCITE;

    private static final String[] TABLES = new String[] { "R1", "R2", "R3", "R4", "R5", "R6" };

    @Test
    public void testMergeJoins() throws Exception {
        Client client = getClient();
        try {
            subtestEmptyTablesMergeJoin(client);
        } finally {
            truncateTables(client, TABLES);
        }
        try {
            subtestNonEmptyTablesMergeJoin(client);
        } finally {
            truncateTables(client, TABLES);
        }
        try {
            subtestCompoundIndexMergeJoin(client);
        } finally {
            truncateTables(client, TABLES);
        }
    }

    @Test
    public void testFullMergeJoins() throws Exception {
        Client client = getClient();
        try {
            subtestUsingFullJoin(client);
        } finally {
            truncateTables(client, TABLES);
        }
        try {
            subtestTwoReplicatedTableFullNLJoin(client);
        }finally {
            truncateTables(client, TABLES);
        }
        try {
            subtestTwoReplicatedTableFullNLJoin(client);
        }finally {
            truncateTables(client, TABLES);
        }

    }

    @Test
    public void testNLIJs() throws Exception {
        Client client = getClient();
        try {
            subtestEmptyTablesNLIJ(client);
        } finally {
            truncateTables(client, TABLES);
        }
        try {
            subtestNonEmptyTablesNLIJJoin(client);
        } finally {
            truncateTables(client, TABLES);
        }
    }

    private void subtestEmptyTablesMergeJoin(Client client) throws Exception {
        String query = "SELECT * FROM R3 INNER JOIN R4 ON R3.A = R4.G ;";

        // Both table are empty
        if (useCalcite) {
            checkQueryPlan(client, query,"MERGE INNER JOIN");
        }
        validateRowCount(client, query, 0);

        // One table is empty
        client.callProcedure("R3.INSERT", 1, 1);
        validateRowCount(client, query, 0);
    }

    private void subtestNonEmptyTablesMergeJoin(Client client) throws Exception {
        String query = "SELECT R3.A, R3.C, R4.A, R4.G FROM R3 INNER JOIN R4 " +
              "ON R3.A = R4.G ORDER BY 1,2,3,4;";
        client.callProcedure("R3.INSERT", 1, 1); // No matches
        client.callProcedure("R3.INSERT", 2, 2); // 3 matches
        client.callProcedure("R3.INSERT", 2, 3); // 3 matches
        client.callProcedure("R3.INSERT", 3, 4); // No matches
        client.callProcedure("R3.INSERT", 4, 5); // 1 match
        client.callProcedure("R3.INSERT", 4, 6); // 1 match
        client.callProcedure("R3.INSERT", 4, 7); // 1 match

        client.callProcedure("R4.INSERT", 1, 10);
        client.callProcedure("R4.INSERT", 2, 2);
        client.callProcedure("R4.INSERT", 3, 2);
        client.callProcedure("R4.INSERT", 4, 2);
        client.callProcedure("R4.INSERT", 5, 4);
        client.callProcedure("R4.INSERT", 6, 6);

        if (useCalcite) {
            checkQueryPlan(client, query,"MERGE INNER JOIN");
        }

        validateTableOfLongs(client, query, new long[][]{
            {2,2,2,2},
            {2,2,3,2},
            {2,2,4,2},
            {2,3,2,2},
            {2,3,3,2},
            {2,3,4,2},
            {4,5,5,4},
            {4,6,5,4},
            {4,7,5,4}
            });
    }

    private void subtestCompoundIndexMergeJoin(Client client) throws Exception {
        String query = "SELECT R6.A, R6.G, R5.SI, R5.BI FROM R6 INNER JOIN R5 " +
              "ON R6.G = R5.SI AND R6.A = R5.BI ORDER BY 1,2,3,4;";

        client.callProcedure("@AdHoc", "insert into R6 values(?, ?, ?)", 5, "1", 1);
        client.callProcedure("@AdHoc", "insert into R6 values(?, ?, ?)", 4, "2", 0);
        client.callProcedure("@AdHoc", "insert into R6 values(?, ?, ?)", 4, "3", 1);
        client.callProcedure("@AdHoc", "insert into R6 values(?, ?, ?)", 5, "4", 2);
        client.callProcedure("@AdHoc", "insert into R6 values(?, ?, ?)", 5, "5", 3);
        client.callProcedure("@AdHoc", "insert into R6 values(?, ?, ?)", 10, "6", 8);

        client.callProcedure("@AdHoc", "insert into R5 values(?, ?, ?)", 1, "1", 5);
        client.callProcedure("@AdHoc", "insert into R5 values(?, ?, ?)", 3, "2", 5);
        client.callProcedure("@AdHoc", "insert into R5 values(?, ?, ?)", 4, "3", 1);
        client.callProcedure("@AdHoc", "insert into R5 values(?, ?, ?)", 8, "4", 10);

        if (useCalcite) {
            checkQueryPlan(client, query,"MERGE INNER JOIN");
        }

        validateTableOfLongs(client, query, new long[][]{
            {5,1,1,5},
            {5,3,3,5},
            {10,8,8,10}
        });
    }

    private void subtestEmptyTablesNLIJ(Client client) throws Exception {
        String query = "SELECT R3.C, R4.G FROM R3 INNER JOIN R4 ON R3.C = R4.G ;";

        // Both table are empty
        checkQueryPlan(client, query,"NESTLOOP INDEX INNER JOIN");
        validateRowCount(client, query, 0);

        // One table is empty
        client.callProcedure("R3.INSERT", 1, 1);
        validateRowCount(client, query, 0);
    }

    private void subtestNonEmptyTablesNLIJJoin(Client client) throws Exception {
        String query = "SELECT R3.A, R3.C, R4.A, R4.G FROM R3 INNER JOIN R4 " +
              "ON R3.A = R4.G ORDER BY 1,2,3,4;";
        client.callProcedure("R3.INSERT", 1, 1); // No matches
        client.callProcedure("R3.INSERT", 2, 2); // 3 matches
        client.callProcedure("R3.INSERT", 2, 3); // 3 matches
        client.callProcedure("R3.INSERT", 3, 4); // No matches
        client.callProcedure("R3.INSERT", 4, 5); // 1 match
        client.callProcedure("R3.INSERT", 4, 6); // 1 match
        client.callProcedure("R3.INSERT", 4, 7); // 1 match

        client.callProcedure("R4.INSERT", 1, 10);
        client.callProcedure("R4.INSERT", 2, 2);
        client.callProcedure("R4.INSERT", 3, 2);
        client.callProcedure("R4.INSERT", 4, 2);
        client.callProcedure("R4.INSERT", 5, 4);
        client.callProcedure("R4.INSERT", 6, 6);

        // Calcite combines JOIN and WHERE expressions into a single one that is not an equivalence one.
        // NLIJ instead of a possible MJ
        query = "SELECT R3.A, R3.C, R4.A, R4.G FROM R3 INNER JOIN R4 " +
                "ON R3.A = R4.G where R3.C > R4.A order by 1, 2, 3, 4;";

        checkQueryPlan(client, query,"NESTLOOP INDEX INNER JOIN");

        validateTableOfLongs(client, query, new long[][]{
            {2,3,2,2},
            {4,6,5,4},
            {4,7,5,4}
        });
    }

    private void subtestUsingFullJoin(Client client) throws Exception {
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(null, '1', 1);");
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(2, '2', 1);");
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(1, '1', 2);");
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(3, '3', 3);");
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(4, '4', 4);");

        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(3, 1);");
        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(8, 3);");
        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(8, 5);");

        //String query = "SELECT MAX(R6.A), G FROM R6 FULL JOIN R4 USING (G) WHERE G > 0 GROUP BY G ORDER BY G";
        String query = "SELECT MAX(R6.A), G FROM R6 FULL JOIN R4 USING (G) GROUP BY G ORDER BY G";
        if (useCalcite) {
            checkQueryPlan(client, query,"MERGE FULL JOIN");
        }

        validateTableOfLongs(client, query,
                new long[][]{ {2, 1}, {1, 2}, {3, 3}, {4, 4}, {NULL_VALUE, 5} });
    }

    private void subtestTwoReplicatedTableFullNLJoin(Client client) throws Exception {
        // case: two empty tables
        String query = "SELECT R4.A, R4.G, R6.A, R6.G FROM R4 FULL JOIN R6 ON R4.G = R6.G AND R4.A =" +
                " R6.A ORDER BY 1, 2, 3, 4";
        if (useCalcite) {
            checkQueryPlan(client, query,"MERGE FULL JOIN");
        }
        validateTableOfLongs(client, query,
                new long[][]{});

        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(null, '1', 1)");
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(2, '2', 1)");
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(1, '1', 2)");
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(4, '4', 2)");
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(3, '3', 3)");
        client.callProcedure("@AdHoc", "INSERT INTO R6 VALUES(4, '4', 4)");
        // Delete one row to have non-active tuples in the table
        client.callProcedure("@AdHoc", "DELETE FROM R6 WHERE A = 4 AND G = 2;");

        // case: Left table is empty
        validateTableOfLongs(client, query,
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, NULL_VALUE, 1},
                        {NULL_VALUE, NULL_VALUE, 1, 2},
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                });

        // case: Right table is empty
        // the equality expression left and right subexpressions have outer and inner TVEs
        query = "SELECT R4.A, R4.G, R6.A, R6.G FROM R6 FULL JOIN R4 ON R6.G = R4.G AND R4.A =" +
                " R6.A ORDER BY 1, 2, 3, 4";
        if (useCalcite) {
            checkQueryPlan(client, query,"MERGE FULL JOIN");
        }
        validateTableOfLongs(client, query,
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, NULL_VALUE, 1},
                        {NULL_VALUE, NULL_VALUE, 1, 2},
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                });

        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(1, 1)");
        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(1, 2)");
        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(2, 2)");
        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(3, 3)");
        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(4, 4)");
        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(5, 5)");
        client.callProcedure("@AdHoc", "INSERT INTO R4 VALUES(null, 5)");
        // Delete one row to have non-active tuples in the table
        client.callProcedure("@AdHoc", "DELETE FROM R4 WHERE A = 4 AND G = 4;");

        // case 1: equality join on two columns
        validateTableOfLongs(client, query,
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, NULL_VALUE, 1},
                        {NULL_VALUE, NULL_VALUE, 2, 1},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {NULL_VALUE, 5, NULL_VALUE, NULL_VALUE},
                        {1, 1, NULL_VALUE, NULL_VALUE},
                        {1, 2, 1, 2},
                        {2, 2, NULL_VALUE, NULL_VALUE},
                        {3, 3, 3, 3},
                        {5, 5, NULL_VALUE, NULL_VALUE}
                });

        // case 2: equality join on two columns plus outer join expression
        // an extra R1.C = 1 expression disables MJ
        validateTableOfLongs(client,
                "SELECT R4.G, R4.A, R6.G, R6.A FROM R4 FULL JOIN R6 ON R4.G = R6.G AND R6.A = R4.A "
                + "AND R6.STR = '1' ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 1, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 1, 2},
                        {NULL_VALUE, NULL_VALUE, 3, 3},
                        {NULL_VALUE, NULL_VALUE, 4, 4},
                        {1, 1, NULL_VALUE, NULL_VALUE},
                        {2, 1, 2, 1},
                        {2, 2, NULL_VALUE, NULL_VALUE},
                        {3, 3, NULL_VALUE, NULL_VALUE},
                        {5, NULL_VALUE, NULL_VALUE, NULL_VALUE},
                        {5, 5, NULL_VALUE, NULL_VALUE}
                });

        // case 5: equality join on single column
        validateTableOfLongs(client,
                "SELECT R6.G, R6.A, R4.G, R4.A FROM R4 FULL JOIN R6 ON R6.G = R4.G ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {NULL_VALUE, NULL_VALUE, 5, 5},
                        {1, NULL_VALUE, 1, 1},
                        {1, 2, 1, 1},
                        {2, 1, 2, 1},
                        {2, 1, 2, 2},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });

        // case 6: equality join on single column and WHERE inner expression
        // NLIJ instead

        if (!isHSQL()) {
            // HSQL returns incorrect results
            validateTableOfLongs(client,
                "SELECT R6.G, R6.A, R4.G, R4.A FROM R4 FULL JOIN R6 ON R4.G = R6.G "
                + "WHERE R4.A = 3 OR R4.A IS NULL ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });
        }

        if (!isHSQL()) {
            // HSQL incorrectly returns
            //   NULL,NULL,1,1
            //   NULL,NULL,2,1
            //   NULL,NULL,2,2
            //   NULL,NULL,5,NULL
            //   NULL,NULL,5,5
            //   3,3,3,3

            // case 7: equality join on single column and WHERE outer expression
            // NLIJ
            validateTableOfLongs(client,
                "SELECT R6.G, R6.A, R4.G, R4.A FROM R4 FULL JOIN R6 ON R4.G = R6.G "
                + "WHERE R6.G = 3 OR R6.G IS NULL ORDER BY 1, 2, 3, 4",
                new long[][]{
                    {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                    {NULL_VALUE, NULL_VALUE, 5, 5},
                    {3, 3, 3, 3}
                });
        }

        // case 8: equality join on single column and WHERE inner-outer expression

        validateTableOfLongs(client,
                "SELECT R6.G, R6.A, R4.G, R4.A FROM R6 FULL JOIN R4 ON R6.G = R4.G WHERE R6.G = 3 OR R4.A IS NULL ORDER BY 1, 2, 3, 4",
                new long[][]{
                        {NULL_VALUE, NULL_VALUE, 5, NULL_VALUE},
                        {3, 3, 3, 3},
                        {4, 4, NULL_VALUE, NULL_VALUE}
                });
    }

    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestCalciteJoinsSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(TestJoinsSuite.class.getResource("testjoins-ddl.sql"));

        LocalCluster config;

        config = new LocalCluster("testjoin-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testjoin-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        // HSQLDB
        config = new LocalCluster("testjoin-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        assertTrue(config.compile(project));
        builder.addServerConfig(config);

        return builder;
    }
}