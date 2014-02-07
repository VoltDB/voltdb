/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.io.IOException;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestSubQueriesSuite extends RegressionSuite {
    public TestSubQueriesSuite(String name) {
        super(name);
    }

    private void clearSeqTables(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("@AdHoc", "DELETE FROM R1;");
        client.callProcedure("@AdHoc", "DELETE FROM R2;");
        client.callProcedure("@AdHoc", "DELETE FROM P1;");
    }

    public void testSimpleSubQueries()
            throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        clearSeqTables(client);
        subtestSingleSubQuery(client);
        clearSeqTables(client);
        subtestNestedSubQueries(client);
        clearSeqTables(client);
        subtestTwoSubQueries(client);
    }

    /**
     * Simple sub-query
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestSingleSubQuery(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", -1, -1, -1); // eliminated
        client.callProcedure("InsertR1", 1, 1, 1); //
        client.callProcedure("InsertR1", 2, 2, 2); //
        client.callProcedure("InsertR1", 3, 3, 3); //
        VoltTable result = client.callProcedure("@AdHoc", "select A, C FROM (SELECT A, C FROM R1) TEMP WHERE TEMP.A > 0;")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(3, result.getRowCount());
        result = client.callProcedure("@AdHoc", "select A1, C1 FROM (SELECT A A1, C C1 FROM R1) TEMP WHERE TEMP.A1 > 0;")
                .getResults()[0];
        System.out.println(result.toString());
        assertEquals(3, result.getRowCount());
    }

    /**
     * SELECT FROM SELECT FROM SELECT
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestNestedSubQueries(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", -1, -1, -1); // eliminated
        client.callProcedure("InsertR1", 1, 1, 1); // eliminated
        client.callProcedure("InsertR1", 2, 2, 2); // eliminated
        client.callProcedure("InsertR1", 3, 3, 3); //
        VoltTable result = client.callProcedure("@AdHoc",
                "select A2 FROM (SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1) TEMP1 WHERE TEMP1.A1 > 0) TEMP2 WHERE TEMP2.A2 = 3")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(1, result.getRowCount());
    }

    /**
     * Join two sub queries
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    private void subtestTwoSubQueries(Client client)
            throws NoConnectionsException, IOException, ProcCallException
    {
        client.callProcedure("InsertR1", -1, -1, -1); // eliminated
        client.callProcedure("InsertR1", 1, 1, 1); // eliminated
        client.callProcedure("InsertR1", 2, 2, 2); //
        client.callProcedure("InsertR1", 3, 3, 3); //
        client.callProcedure("InsertR2", 2, 2); //
        client.callProcedure("InsertR2", 3, 3); //
        client.callProcedure("InsertR2", 4, 4); //eliminated
        VoltTable result = client.callProcedure("@AdHoc",
                "select A, C FROM (SELECT A FROM R1) TEMP1, (SELECT C FROM R2) TEMP2 WHERE A = C")
                                 .getResults()[0];
        System.out.println(result.toString());
        assertEquals(2, result.getRowCount());
    }

    static public junit.framework.Test suite()
    {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSubQueriesSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestJoinsSuite.class.getResource("testsubqueries-ddl.sql"));
        project.addStmtProcedure("InsertR1", "INSERT INTO R1 VALUES(?, ?, ?);");
        project.addStmtProcedure("InsertR2", "INSERT INTO R2 VALUES(?, ?);");
        project.addStmtProcedure("InsertR3", "INSERT INTO R3 VALUES(?, ?);");
        project.addStmtProcedure("InsertP1", "INSERT INTO P1 VALUES(?, ?);");
        project.addStmtProcedure("InsertP2", "INSERT INTO P2 VALUES(?, ?);");
        project.addStmtProcedure("InsertP3", "INSERT INTO P3 VALUES(?, ?);");
        /*
        config = new LocalCluster("testunion-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);
        */
        // Cluster
        config = new LocalCluster("testunion-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // HSQLDB
        config = new LocalCluster("testunion-cluster.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);
        return builder;
    }

}
