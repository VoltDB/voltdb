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

public class TestCalciteJoinsSuite extends RegressionSuite {

    public TestCalciteJoinsSuite(String name) {
        super(name);
    }

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
        // TODO Calcite read uncomment
        //checkQueryPlan(client, query,"MERGE INNER JOIN");
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

        // TODO Calcite ready uncomment
        //checkQueryPlan(client, query,"MERGE INNER JOIN");

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

        // TODO Calcite ready uncomment
        //checkQueryPlan(client, query,"MERGE INNER JOIN");

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