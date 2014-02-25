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

    private void loadData(Client client) throws NoConnectionsException, IOException, ProcCallException   {
        client.callProcedure("@AdHoc", "Truncate table R1");
        client.callProcedure("@AdHoc", "Truncate table R2");
        client.callProcedure("@AdHoc", "Truncate table P1");

        // Data for R1
        client.callProcedure("R1.insert", -1, -1, -1);
        client.callProcedure("R1.insert",  1,  1,  1);
        client.callProcedure("R1.insert",  2,  2,  2);
        client.callProcedure("R1.insert",  3,  3,  3);

        // Data for R2
        client.callProcedure("R2.insert", 2, 2);
        client.callProcedure("R2.insert", 3, 3);
        client.callProcedure("R2.insert", 4, 4);

        // Data for P1

    }

    /**
     * Simple sub-query
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testSingleSubQuery() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(client);
        VoltTable vt;

        vt = client.callProcedure("@AdHoc", "select A, C FROM (SELECT A, C FROM R1) T1 WHERE T1.A > 2;").getResults()[0];
        System.out.println(vt);
        validateTableOfLongs(vt, new long[][] { {3, 3}});
    }

    /**
     * SELECT FROM SELECT FROM SELECT
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testNestedSubQueries() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(client);
        VoltTable vt;

        vt = client.callProcedure("@AdHoc",
                "select A FROM (SELECT A FROM R1 WHERE A > 0) T2 " +
                "order by A").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{1}, {2}, {3}});

        vt = client.callProcedure("@AdHoc",
                "select A2 FROM (SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1) T1 WHERE T1.A1 > 0) T2 " +
                "WHERE T2.A2 >= 3").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{3}});

        vt = client.callProcedure("@AdHoc",
                "select A2+1 FROM (SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1) T1 WHERE T1.A1 > 0) T2 " +
                "WHERE T2.A2 >= 3").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{4}});

        vt = client.callProcedure("@AdHoc",
                "select A2+1 AS A3 FROM (SELECT A1 AS A2 FROM (SELECT A AS A1 FROM R1) T1 WHERE T1.A1 > 0) T2 " +
                "WHERE T2.A2 >= 2 Order by A3").getResults()[0];
        validateTableOfLongs(vt, new long[][] {{3}, {4}});


        // Test group by queries, order by, limit, offset

    }

    /**
     * Join two sub queries
     * @throws NoConnectionsException
     * @throws IOException
     * @throws ProcCallException
     */
    public void testTwoSubQueries() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        loadData(client);

        VoltTable vt;
        vt = client.callProcedure("@AdHoc",
                "select A, C FROM (SELECT A FROM R1) T1, (SELECT C FROM R2) T2 WHERE T1.A = T2.C ORDER BY A").getResults()[0];
        System.out.println(vt.toString());
        validateTableOfLongs(vt, new long[][] {{2, 2}, {3, 3}});
    }

    static public junit.framework.Test suite()
    {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestSubQueriesSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestSubQueriesSuite.class.getResource("testsubqueries-ddl.sql"));

        config = new LocalCluster("testunion-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // HSQLDB
        config = new LocalCluster("testunion-cluster.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testunion-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }

}
