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

    private static void load(Client client) throws IOException, InterruptedException {
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

    private static void doLimitOffsetAndCheck(Client client, String proc) throws IOException, ProcCallException {
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

    /** Check the result of a query that has only an OFFSET and no LIMIT clause.
     * This is done by executing the query with and without the offset clause,
     * and then skipping past the offset rows in the expected table here
     * on the client side. */
    private static void doOffsetAndCheck(Client client, String stmt) throws IOException, ProcCallException {
        String stmtNoOffset = stmt.substring(0, stmt.indexOf("OFFSET"));
        VoltTable expectedTable = client.callProcedure("@AdHoc", stmtNoOffset).getResults()[0];
        int rowCountBeforeOffset = expectedTable.getRowCount();

        int[] offsets = {0, 1, 5, 10, 11, 15};
        for (int offset : offsets) {
            VoltTable actualTable = client.callProcedure("@AdHoc", stmt, offset).getResults()[0];
            int expectedRowCount = Math.max(rowCountBeforeOffset - offset, 0);
            assertEquals("Actual table has wrong number of rows: ",
                    expectedRowCount, actualTable.getRowCount());
            if (actualTable.getRowCount() == 0)
                continue;

            // non-empty result.
            // Advance expected table past offset
            // then compare what's left.
            actualTable.resetRowPosition();
            for (int i = 0; i < offset; ++i)
                expectedTable.advanceRow();

            while (actualTable.advanceRow() && expectedTable.advanceRow()) {
                assertEquals(expectedTable.getLong(0), actualTable.getLong(0));
                assertEquals(expectedTable.getLong(1), actualTable.getLong(1));
            }
        }
    }

    public void testBasicLimitOffsets() throws IOException, ProcCallException, InterruptedException
    {
        Client client = this.getClient();
        load(client);

        String[] procedureNames = {
                "LimitAPKEY",
                "LimitAI",
                "LimitBPKEY",
                "LimitBI"
        };
        for (String procedureName : procedureNames) {
            doLimitOffsetAndCheck(client, procedureName);
        }

        String[] offsetOnlyStmts = {
                "SELECT * FROM A ORDER BY PKEY OFFSET ?;",
                "SELECT * FROM B ORDER BY PKEY OFFSET ?;",
                "SELECT * FROM A ORDER BY I OFFSET ?;",
                "SELECT * FROM B ORDER BY I OFFSET ?;"
        };
        for (String stmt : offsetOnlyStmts) {
            doOffsetAndCheck(client, stmt);
        }

        doTestJoinAndLimitOffset(client);
    }

    public static void doTestJoinAndLimitOffset(Client client) throws IOException, ProcCallException {
        int limits[] = new int[] { 1, 2, 5, 10, 12, 25, Integer.MAX_VALUE };
        int offsets[] = new int[] { 0, 1, 2, 5, 10, 12, 25 };
        String selecteds[] = new String[] { "*", "A.PKEY" };
        String joinops[] = new String[] { ",", "LEFT JOIN", "RIGHT JOIN", " FULL JOIN" };
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
                                    ((limit == Integer.MAX_VALUE) ? "" : "LIMIT " + limit) +
                                    ((offset == 0) ? "" : " OFFSET " + offset) +
                                    ";";
                            result = client.callProcedure("@AdHoc", query).getResults()[0];
                            long expectedRowCount = Math.max(0, Math.min(limit, found-offset));
                            assertEquals("Statement \"" + query + "\" produced wrong number of rows: ",
                                    expectedRowCount, result.getRowCount());
                        }
                    }
                }
            }
        }
    }

    public void testDistinctLimitOffset() throws IOException, ProcCallException {
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
    public void testSubqueryLimit() throws Exception {
        Client client = getClient();

        ClientResponse cr;
        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (8, 'nSAFoccWXxEGXR', -3364, 7.76005886643784892343e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (9, 'nSAFoccWXxEGXR', -3364, 8.65086522017155634678e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (10, 'nSAFoccWXxEGXR', 11411, 3.49977104648325210157e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (11, 'nSAFoccWXxEGXR', 11411, 4.96260220021031761561e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (12, 'ebWfhdmIZfYhRC', NULL, 3.94021683247165688257e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (13, 'ebWfhdmIZfYhRC', NULL, 2.97950296374613898820e-02);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (14, 'ebWfhdmIZfYhRC', 23926, 8.56241324965489991605e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (15, 'ebWfhdmIZfYhRC', 23926, 3.61291695704730075889e-01); ");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        String selectStmt = "select NUM from R1 where NUM in (select NUM from R1 where NUM <> 12 order by NUM limit 4) ORDER BY NUM;";
        VoltTable tbl;
        cr = client.callProcedure("@AdHoc", selectStmt);
        tbl = cr.getResults()[0];
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        validateTableOfLongs(tbl, new long[][]{{-3364L}, {-3364}, {11411}, {11411}});
    }

    public void testLimitZeroWithOrderBy() throws Exception {
        Client client = getClient();
        ClientResponse cr;
        VoltTable vt;

        // Check that limit 0 on a table with no indices or partitions
        // works as expected, and that it doesn't break other limits.
        cr = client.callProcedure("PLAINJANE.insert", 100, 101, 102);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("PLAINJANE.insert", 200, 201, 202);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("PLAINJANE.insert", 300, 301, 302);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("PLAINJANE.insert", 400, 401, 402);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        cr = client.callProcedure("PLAINJANE.insert", 500, 501, 502);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        for (int idx = 0; idx < 5; idx += 1) {
            String sql = "SELECT * FROM PLAINJANE ORDER BY ID LIMIT " + idx + ";";
            cr = client.callProcedure("@AdHoc", sql);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            assertEquals(idx, vt.getRowCount());
        }

        // Check the same thing using a table with an index.  This is
        // important since the plan may be different.  The order by
        // node of the plan may be avoided by scanning the index.
        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (8, 'nSAFoccWXxEGXR', -3364, 7.76005886643784892343e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (9, 'nSAFoccWXxEGXR', -3364, 8.65086522017155634678e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (10, 'nSAFoccWXxEGXR', 11411, 3.49977104648325210157e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (11, 'nSAFoccWXxEGXR', 11411, 4.96260220021031761561e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (12, 'ebWfhdmIZfYhRC', NULL, 3.94021683247165688257e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (13, 'ebWfhdmIZfYhRC', NULL, 2.97950296374613898820e-02);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (14, 'ebWfhdmIZfYhRC', 23926, 8.56241324965489991605e-01);");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc", "INSERT INTO R1 VALUES (15, 'ebWfhdmIZfYhRC', 23926, 3.61291695704730075889e-01); ");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        for (int idx = 0; idx < 5; idx += 1) {
            String sql = "SELECT * FROM R1 ORDER BY ID LIMIT " + idx + ";";
            cr = client.callProcedure("@AdHoc", sql);
            assertEquals(ClientResponse.SUCCESS, cr.getStatus());
            vt = cr.getResults()[0];
            assertEquals(idx, vt.getRowCount());
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
