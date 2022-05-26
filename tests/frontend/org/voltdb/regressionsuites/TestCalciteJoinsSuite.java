/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
        try {
            subtestThreeWayMergeJoin(client);
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

        // Both WHERE conditions are pushed down to children
        query = "SELECT R3.A, R3.C, R4.A, R4.G FROM R3 INNER JOIN R4 " +
                "ON R3.A = R4.G WHERE R4.A = 5 and R3.A = 4 ORDER BY 1,2,3,4;";
        validateTableOfLongs(client, query, new long[][]{
            {4,5,5,4},
            {4,6,5,4},
            {4,7,5,4}
            });

        // The WHERE condition stays at the join level as a post-predicate
        query = "SELECT R3.A, R3.C, R4.A, R4.G FROM R3 INNER JOIN R4 " +
                "ON R3.A = R4.G WHERE R3.C + R4.A = 12 ORDER BY 1,2,3,4;";
        validateTableOfLongs(client, query, new long[][]{
            {4,7,5,4}
            });

    }

    private void subtestThreeWayMergeJoin(Client client) throws Exception {

        client.callProcedure("R3.INSERT", 1, 13);
        client.callProcedure("R3.INSERT", 2, 23);
        client.callProcedure("R3.INSERT", 2, 23);
        client.callProcedure("R3.INSERT", 3, 43);
        client.callProcedure("R3.INSERT", 4, 53);

        client.callProcedure("@AdHoc", "insert into r7 values(11, 17)");
        client.callProcedure("@AdHoc", "insert into r7 values(2, 77)");
        client.callProcedure("@AdHoc", "insert into r7 values(4, 57)");

        client.callProcedure("@AdHoc", "insert into r8 values(2, 78)");
        client.callProcedure("@AdHoc", "insert into r8 values(4, 58)");
        client.callProcedure("@AdHoc", "insert into r8 values(7, 58)");

        String query = "SELECT R3.A, R3.C, R7.A, R7.C, R8.A, R8.C "
                + "FROM R3 JOIN R7 ON R3.A = R7.A "
                + " JOIN R8 ON R7.A = R8.A ORDER BY 1,2,3,4,5,6;";
          if (useCalcite) {
            checkQueryPlan(client, query,"MERGE INNER JOIN");
        }

        validateTableOfLongs(client, query, new long[][]{
            {2,  23,  2, 77,  2,  78},
            {2,  23,  2, 77,  2,  78},
            {4,  53,  4, 57,  4,  58}
            });

        query = "SELECT R3.A, R3.C, R7.A, R7.C, R8.A, R8.C "
                + "FROM R3 JOIN R7 ON R3.A = R7.A "
                + " JOIN R8 ON R7.A = R8.A WHERE R3.C = 53 ORDER BY 1,2,3,4,5,6;";
        validateTableOfLongs(client, query, new long[][]{
            {4,  53,  4, 57,  4,  58}
            });

        query = "SELECT R3.A, R3.C, R7.A, R7.C, R8.A, R8.C "
                + "FROM R3 JOIN R7 ON R3.A = R7.A "
                + " JOIN R8 ON R7.A = R8.A WHERE R7.C + 1 = R8.C ORDER BY 1,2,3,4,5,6;";
        validateTableOfLongs(client, query, new long[][]{
            {2,  23,  2, 77,  2,  78},
            {2,  23,  2, 77,  2,  78},
            {4,  53,  4, 57,  4,  58}
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
                new long[][]{
            {2, 1},
            {1, 2},
            {3, 3},
            {4, 4},
            {NULL_VALUE, 5} });

        // WHERE Expression with ambiguous USING column
        query = "SELECT MAX(R6.A), G FROM R6 FULL JOIN R4 USING (G) WHERE G > 3 GROUP BY G ORDER BY G";
        if (useCalcite) {
            checkQueryPlan(client, query,"MERGE FULL JOIN");
        }
        validateTableOfLongs(client, query,
                new long[][]{
            {4, 4},
            {NULL_VALUE, 5} });

        query = "SELECT MAX(R6.A), R6.G FROM R6 FULL JOIN R4 on R6.G = R4.G GROUP BY R6.G ORDER BY R6.G";
        if (useCalcite) {
            checkQueryPlan(client, query,"MERGE FULL JOIN");
        }

        validateTableOfLongs(client, query,
                new long[][]{
            {NULL_VALUE, NULL_VALUE},
            {2, 1},
            {1, 2},
            {3, 3},
            {4, 4} });

        query = "SELECT MAX(R6.A), R6.G FROM R6 FULL JOIN R4 on R6.G = R4.G WHERE R6.G IS NULL GROUP BY R6.G ORDER BY R6.G";

        validateTableOfLongs(client, query,
                new long[][]{
            {NULL_VALUE, NULL_VALUE} });

        query = "SELECT MAX(R6.A), R6.G FROM R6 FULL JOIN R4 on R6.G = R4.G "
                + "WHERE R6.G IS NULL OR R4.G = 3 GROUP BY R6.G ORDER BY R6.G";

        validateTableOfLongs(client, query,
                new long[][]{
            {NULL_VALUE, NULL_VALUE},
            {3, 3} });

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
