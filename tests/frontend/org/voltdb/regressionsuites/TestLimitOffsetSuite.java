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
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestLimitOffsetSuite extends RegressionSuite {
    public TestLimitOffsetSuite(String name)
    {
        super(name);
    }

    private static void load(Client client)
    throws NoConnectionsException, IOException, InterruptedException
    {
        for (int i = 0; i < 10; i++) {
            SyncCallback cb = new SyncCallback();
            client.callProcedure(cb, "InsertA", i, i);
            cb.waitForResponse();
            assertEquals(1, cb.getResponse().getResults()[0].asScalarLong());
        }

        for (int i = 0; i < 10; i++) {
            SyncCallback cb = new SyncCallback();
            client.callProcedure(cb, "InsertB", i, i);
            cb.waitForResponse();
            assertEquals(1, cb.getResponse().getResults()[0].asScalarLong());
        }
    }

    private static void doLimitOffsetAndCheck(Client client, String proc)
    throws IOException, InterruptedException, ProcCallException
    {
        ClientResponse resp = client.callProcedure(proc, 4, 0);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        VoltTable[] results = resp.getResults();
        assertEquals(1, results.length);
        VoltTable vt = results[0];
        int i = 0;
        while (vt.advanceRow()) {
            assertEquals(i++, vt.getLong(1));
        }
        assertEquals(4, i);

        resp = client.callProcedure(proc, 3, 1);
        assertEquals(ClientResponse.SUCCESS, resp.getStatus());
        results = resp.getResults();
        assertEquals(1, results.length);
        vt = results[0];
        i = 1;
        while (vt.advanceRow()) {
            assertEquals(i++, vt.getLong(1));
        }
        assertEquals(4, i);
    }

    public void testBasicLimitOffsets() throws IOException, ProcCallException, InterruptedException
    {
        Client client = this.getClient();
        load(client);
        doTestMultiPartInlineLimit(client);
        doTestMultiPartLimit(client);
        doTestReplicatedInlineLimit(client);
        doTestReplicatedLimit(client);
        doTestJoinAndLimitOffset(client);
    }

    private static void doTestMultiPartInlineLimit(Client client) throws IOException, InterruptedException, ProcCallException
    {
        doLimitOffsetAndCheck(client, "LimitAPKEY");
    }

    private static void doTestMultiPartLimit(Client client) throws IOException, InterruptedException, ProcCallException
    {
        doLimitOffsetAndCheck(client, "LimitAI");
    }

    private static void doTestReplicatedInlineLimit(Client client) throws IOException, InterruptedException, ProcCallException
    {
        doLimitOffsetAndCheck(client, "LimitBPKEY");
    }

    private static void doTestReplicatedLimit(Client client) throws IOException, InterruptedException, ProcCallException
    {
        doLimitOffsetAndCheck(client, "LimitBI");
    }

    public static void doTestJoinAndLimitOffset(Client client) throws IOException, ProcCallException, InterruptedException
    {
        int limits[] = new int[] { 1, 2, 5, 10, 12, 25 };
        int offsets[] = new int[] { 0, 1, 2, 5, 10, 12, 25 };
        String selecteds[] = new String[] { "*", "A.PKEY" };
        String joinops[] = new String[] { ",", "LEFT JOIN", "RIGHT JOIN" };
        String conditions[] = new String[] { " A.PKEY < B.PKEY ", " A.PKEY = B.PKEY ", " A.I = B.I " };
        client.callProcedure("InsertA", -1, 0);
        for (String joinop : joinops) {
            String onwhere = "ON";
            if (joinop.equals(",")) {
                onwhere = "WHERE";
            }
            for (String selected : selecteds) {
                for (int limit : limits) {
                    for (int offset : offsets) {
                        for (String condition : conditions) {
                            String query;
                            VoltTable result;
                            query = "SELECT COUNT(*) FROM A " + joinop + " B " +
                                    onwhere + condition +
                                    ";";
                            result = client.callProcedure("@AdHoc", query).getResults()[0];
                            long found = result.asScalarLong();
                            query = "SELECT " + selected +
                                    " FROM A " + joinop + " B " +
                                    onwhere + condition +
                                    " ORDER BY A.PKEY, B.PKEY " +
                                    "LIMIT " + limit +
                                    ((offset == 0) ? "" : " OFFSET " + offset) +
                                    ";";
                            result = client.callProcedure("@AdHoc", query).getResults()[0];
                            assertEquals(Math.max(0, Math.min(limit, found-offset)), result.getRowCount());
                        }
                    }
                }
            }
        }
    }

    public void testDistinctLimitOffset() throws NoConnectionsException, IOException, ProcCallException
    {
        Client client = getClient();
        client.callProcedure("InsertA", 0, 1);
        client.callProcedure("InsertA", 1, 1);
        client.callProcedure("InsertA", 2, 2);
        VoltTable result = null;

        result = client.callProcedure("@AdHoc", "SELECT DISTINCT I FROM A LIMIT 1 OFFSET 1;").getResults()[0];
        assertEquals(1, result.getRowCount());

        result = client.callProcedure("@AdHoc", "SELECT DISTINCT I FROM A LIMIT 0 OFFSET 1;").getResults()[0];
        assertEquals(0, result.getRowCount());
    }

    public void testENG3487() throws IOException, ProcCallException
    {
        Client client = this.getClient();

        client.callProcedure("A.insert", 1, 1);
        client.callProcedure("A.insert", 2, 1);
        client.callProcedure("A.insert", 3, 1);
        client.callProcedure("A.insert", 4, 4);
        client.callProcedure("A.insert", 5, 4);
        client.callProcedure("A.insert", 6, 9);


        VoltTable result = client.callProcedure("@AdHoc", "select I, count(*) as tag from A group by I order by tag, I limit 1")
                .getResults()[0];

        assertEquals(1, result.getRowCount());
        assertTrue(result.advanceRow());
        //System.err.println("Result:\n" + result);
        assertEquals(9, result.getLong(0));
        assertEquals(1, result.getLong(1));

    }

    public void testENG1808() throws IOException, ProcCallException
    {
        Client client = this.getClient();

        client.callProcedure("A.insert", 1, 1);

        VoltTable result = client.callProcedure("@AdHoc", "select I from A limit 0").getResults()[0];

        assertEquals(0, result.getRowCount());
    }

    public void testENG5156() throws IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable result = null;

        String insertProc = "SCORE.insert";
        client.callProcedure(insertProc,  1, "b", 1, 1378827221795L, 1, 1);
        client.callProcedure(insertProc,  2, "b", 2, 1378827221795L, 2, 2);

        result = client.callProcedure("@ExplainProc", "GetTopScores").getResults()[0];
        // using the "IDX_SCORE_VALUE_USER" index for sort order only.
        assertTrue(result.toString().contains("IDX_SCORE_VALUE_USER"));
        assertTrue(result.toString().contains("inline LIMIT with parameter"));

        result = client.callProcedure("GetTopScores", 1378827221793L, 1378827421793L, 1).getResults()[0];
        validateTableOfLongs(result, new long[][] {{2,2}});

        // Test AdHoc.
        result = client.callProcedure("@Explain",
                "SELECT user_id, score_value FROM score " +
                "WHERE score_date > 1378827221793 AND score_date <= 1378827421793 " +
                "ORDER BY score_value DESC, user_id DESC LIMIT 1; ").getResults()[0];
        assertTrue(result.toString().contains("IDX_SCORE_VALUE_USER"));
        assertTrue(result.toString().contains("inline LIMIT with parameter"));

        result = client.callProcedure("@AdHoc",
                "SELECT user_id, score_value FROM score " +
                "WHERE score_date > 1378827221793 AND score_date <= 1378827421793 " +
                "ORDER BY score_value DESC, user_id DESC LIMIT 1; ").getResults()[0];
        validateTableOfLongs(result, new long[][] {{2,2}});
    }

    public void testENG6485() throws IOException, ProcCallException {
        Client client = this.getClient();
        VoltTable result = null;

        String insertProc = "C.insert";
        client.callProcedure(insertProc, 1, 1, "foo");
        client.callProcedure(insertProc, 2, 1, "foo");
        client.callProcedure(insertProc, 3, 1, "foo");
        client.callProcedure(insertProc, 4, 1, "bar");
        client.callProcedure(insertProc, 5, 1, "bar");
        client.callProcedure(insertProc, 7, 1, "woof");
        client.callProcedure(insertProc, 8, 1, "woof");
        client.callProcedure(insertProc, 9, 1, "foo");
        client.callProcedure(insertProc, 10, 1, "foo");
        client.callProcedure(insertProc, 11, 2, "foo");
        client.callProcedure(insertProc, 12, 2, "foo");
        client.callProcedure(insertProc, 13, 2, "woof");
        client.callProcedure(insertProc, 14, 2, "woof");
        client.callProcedure(insertProc, 15, 2, "woof");
        client.callProcedure(insertProc, 16, 2, "bar");
        client.callProcedure(insertProc, 17, 2, "bar");
        client.callProcedure(insertProc, 18, 2, "foo");
        client.callProcedure(insertProc, 19, 2, "foo");

        result = client.callProcedure("@AdHoc", "SELECT COUNT(*) FROM C;").getResults()[0];
        validateTableOfScalarLongs(result, new long[] {18});

        result = client.callProcedure("@AdHoc", "SELECT name, count(id) FROM C GROUP BY name limit 1").getResults()[0];
        if (result.advanceRow()) {
            String name = result.getString(0);
            long count = result.getLong(1);
            switch (name){
            case "foo":
                assertEquals(9, count);
                break;
            case "bar":
                assertEquals(4, count);
                break;
            case "woof":
                assertEquals(5, count);
                break;
            }
        }
        else {
            fail("cannot get data from table c");
        }
    }

    static public junit.framework.Test suite()
    {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestLimitOffsetSuite.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        project.addSchema(TestLimitOffsetSuite.class.getResource("testlimitoffset-ddl.sql"));
        project.addPartitionInfo("A", "PKEY");

        project.addStmtProcedure("InsertA", "INSERT INTO A VALUES(?, ?);");
        project.addStmtProcedure("InsertB", "INSERT INTO B VALUES(?, ?);");
        project.addStmtProcedure("LimitAPKEY", "SELECT * FROM A ORDER BY PKEY LIMIT ? OFFSET ?;");
        project.addStmtProcedure("LimitBPKEY", "SELECT * FROM B ORDER BY PKEY LIMIT ? OFFSET ?;");
        project.addStmtProcedure("LimitAI", "SELECT * FROM A ORDER BY I LIMIT ? OFFSET ?;");
        project.addStmtProcedure("LimitBI", "SELECT * FROM B ORDER BY I LIMIT ? OFFSET ?;");

        // local
        config = new LocalCluster("testlimitoffset-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("testlimitoffset-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // HSQL for baseline
        config = new LocalCluster("testlimitoffset-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);
        return builder;
    }
}
