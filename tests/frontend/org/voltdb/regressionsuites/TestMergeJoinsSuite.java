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

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestMergeJoinsSuite extends RegressionSuite {

    public TestMergeJoinsSuite(String name) {
        super(name);
    }

    private static final String[] TABLES =
            new String[] { "R1", "R2", "R3", "R4", "R5" };


    public void testMergeJoins() throws Exception {
        Client client = getClient();
        truncateTables(client, TABLES);
        subtestEmptyTablesMergeJoin(client);
        truncateTables(client, TABLES);
        subtestNonEmptyTablesMergeJoin(client);
        truncateTables(client, TABLES);
        subtestCompoundIndexMergeJoin(client);
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

        // TODO Calcite read uncomment
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

        // Calcite combines JOIN and WHERE expressions into a single one that is not an equivalence one.
        query = "SELECT R3.A, R3.C, R4.A, R4.G FROM R3 INNER JOIN R4 " +
                "ON R3.A = R4.G where R3.C > R4.A;";

        // TODO Calcite read uncomment
        //checkQueryPlan(client, query,"MERGE INNER JOIN");

        validateTableOfLongs(client, query, new long[][]{
            {2,3,2,2},
            {4,6,5,4},
            {4,7,5,4}
        });
    }

    private void subtestCompoundIndexMergeJoin(Client client) throws Exception {
        String query = "SELECT R4.A, R4.G, R5.SI, R5.BI FROM R4 INNER JOIN R5 " +
              "ON R4.G = R5.SI AND R4.A = R5.BI ORDER BY 1,2,3,4;";

        client.callProcedure("R4.INSERT", 5, 1);
        client.callProcedure("R4.INSERT", 4, 0);
        client.callProcedure("R4.INSERT", 4, 1);
        client.callProcedure("R4.INSERT", 5, 2);
        client.callProcedure("R4.INSERT", 5, 3);
        client.callProcedure("R4.INSERT", 10, 8);

        client.callProcedure("@AdHoc", "insert into R5 values(?, ?)", 1, 5);
        client.callProcedure("@AdHoc", "insert into R5 values(?, ?)", 3, 5);
        client.callProcedure("@AdHoc", "insert into R5 values(?, ?)", 4, 1);
        client.callProcedure("@AdHoc", "insert into R5 values(?, ?)", 8, 10);

        // TODO Calcite read uncomment
        //checkQueryPlan(client, query,"MERGE INNER JOIN");

        validateTableOfLongs(client, query, new long[][]{
            {5,1,1,5},
            {5,3,3,5},
            {10,8,8,10}
        });
    }

    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestMergeJoinsSuite.class);
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