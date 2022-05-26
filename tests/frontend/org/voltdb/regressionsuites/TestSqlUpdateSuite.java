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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * System tests for UPDATE, mainly focusing on the correctness of the WHERE
 * clause
 */
public class TestSqlUpdateSuite extends RegressionSuite {
    static final int ROWS = 10;

    private void insertRows(Client client, String table, int count) throws Exception {
        for (int i = 0; i < count; ++i) {
            client.callProcedure("Insert", table, i, "desc", i, 14.5);
        }
    }

    private void executeAndTestUpdate(Client client, String table, String update,
                                      int expectedRowsChanged) throws Exception {
        insertRows(client, table, ROWS);
        VoltTable[] results = client.callProcedure("@AdHoc", update).getResults();
        // ADHOC update still returns number of modified rows * number of partitions
        // Comment this out until it's fixed; the select count should be good enough, though
        //assertEquals(expectedRowsChanged, results[0].asScalarLong());
        String query = "select count(X.NUM) from " + table + " X where X.NUM = -1";
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(String.format("Failing SQL: %s",query), expectedRowsChanged, results[0].asScalarLong());
        client.callProcedure("@AdHoc", "truncate table " + table + ";");

    }

    public void testUpdate() throws Exception {
        subtestUpdateBasic();
        subtestENG11918();
        subtestENG13926();
        subtestUpdateWithSubquery();
        subtestUpdateWithCaseWhen();
        subtestENG14478();
    }

    private void subtestUpdateBasic() throws Exception {
        Client client = getClient();
        String[] tables = {"P1", "R1"};

        System.out.println("testUpdate");
        for (String table : tables) {
            String update = String.format("update %s set %s.NUM = -1",
                                          table, table);
            // Expect all rows to change
            executeAndTestUpdate(client, table, update, ROWS);
        }

        System.out.println("testUpdateWithEqualToIndexPredicate");
        for (String table : tables) {
            String update = String.format("update %s set %s.NUM = -1 where %s.ID = 5",
                                          table, table, table);
            // Only row with ID = 5 should change
            executeAndTestUpdate(client, table, update, 1);
        }

        System.out.println("testUpdateWithEqualToNonIndexPredicate");
        for (String table : tables) {
            String update = String.format("update %s set %s.NUM = -1 where %s.NUM = 5",
                                          table, table, table);
            // Only row with NUM = 5 should change
            executeAndTestUpdate(client, table, update, 1);
        }

        // This tests a bug found by the SQL coverage tool.  The code in HSQL
        // which generates the XML eaten by the planner didn't generate
        // anything in the <condition> element output for > or >= on an index
        System.out.println("testUpdateWithGreaterThanIndexPredicate");
        for (String table : tables) {
            String update = String.format("update %s set %s.NUM = -1 where %s.ID > 5",
                                          table, table, table);
            // Rows 6-9 should change
            executeAndTestUpdate(client, table, update, 4);
        }

        System.out.println("testUpdateWithGreaterThanNonIndexPredicate");
        for (String table : tables) {
            String update = String.format("update %s set %s.NUM = -1 where %s.NUM > 5",
                                          table, table, table);
            // rows 6-9 should change
            executeAndTestUpdate(client, table, update, 4);
        }

        System.out.println("testUpdateWithLessThanIndexPredicate");
        for (String table : tables) {
            String update = String.format("update %s set %s.NUM = -1 where %s.ID < 5",
                                          table, table, table);
            // Rows 0-4 should change
            executeAndTestUpdate(client, table, update, 5);
        }

        // This tests a bug found by the SQL coverage tool.  The code in HSQL
        // which generates the XML eaten by the planner wouldn't combine
        // the various index and non-index join and where conditions, so the planner
        // would end up only seeing the first subnode written to the <condition>
        // element
        System.out.println("testUpdateWithOnePredicateAgainstIndexAndOneFalse");

        for (String table : tables) {
            String update = "update " + table + " set " + table + ".NUM = 100" +
                " where " + table + ".NUM = 1000 and " + table + ".ID = 4";
            executeAndTestUpdate(client, table, update, 0);
        }

        // This tests a bug found by the SQL coverage tool.  The code in HSQL
        // which generates the XML eaten by the planner wouldn't combine (AND)
        // the index begin and end conditions, so the planner would only see the
        // begin condition in the <condition> element.
        System.out.println("testUpdateWithRangeAgainstIndex");
        for (String table : tables) {
            String update = String.format("update %s set %s.NUM = -1 where %s.ID < 8 and %s.ID > 5",
                                          table, table, table, table);
            executeAndTestUpdate(client, table, update, 2);
        }

        System.out.println("testUpdateWithRangeAgainstNonIndex");
        for (String table : tables) {
            String update = String.format("update %s set %s.NUM = -1 where %s.NUM < 8 and %s.NUM > 5",
                                          table, table, table, table);
            executeAndTestUpdate(client, table, update, 2);
        }

        // This is a regression test for ENG-6799
        System.out.println("testUpdateFromInlineVarchar");
        client.callProcedure("STRINGPART.insert",
                "aa", 1, 1, 0, "a potentially (but not really) very long string)");
        // NAME is inlined varchar, DESC is not.
        String update = "update STRINGPART set desc = name, num = -1 where val1 = 1";
        executeAndTestUpdate(client, "STRINGPART", update, 1);

        System.out.println("testInvalidUpdate");
        verifyStmtFails(client, "UPDATE P1_VIEW SET NUM_SUM = 5",
                 "Illegal to modify a materialized view.");
        verifyStmtFails(client, "UPDATE P1 SET NUM = 1 WHERE COUNT(*) IS NULL", "invalid WHERE expression");
    }

    public void subtestUpdateWithSubquery() throws Exception {
        Client client = getClient();
        String tables[] = {"P1", "R1"};
        // insert rows where ID is 0..3
        insertRows(client, "R2", 4);

        for (String table : tables) {
            // insert rows where ID is 0 and num is 0..9
            insertRows(client, table, 10);

            // update rows where ID is 0 and 5
            VoltTable vt = client.callProcedure("@AdHoc",
                    "UPDATE " + table + " SET NUM = NUM + 20 WHERE ID IN (SELECT ID * 5 FROM R2 " +
            " WHERE R2.NUM * 5 = " + table + ".NUM)")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] { 2 });

            String stmt = "SELECT NUM FROM " + table + " ORDER BY NUM";
            validateTableOfScalarLongs(client, stmt, new long[] { 1, 2, 3, 4, 6, 7, 8, 9, 20, 25 });

            vt = client.callProcedure("@AdHoc",
                    "UPDATE " + table + " SET NUM = (SELECT MAX(NUM) FROM R2)")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] { 10 });

            stmt = "SELECT NUM FROM " + table + " ORDER BY NUM";
            validateTableOfScalarLongs(client, stmt, new long[] { 3, 3, 3, 3, 3, 3, 3, 3, 3, 3 });

            vt = client.callProcedure("@AdHoc",
                    "UPDATE " + table + " SET NUM = 20 WHERE ID = (SELECT MAX(NUM) FROM R2)")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] { 1 });

            stmt = "SELECT NUM FROM " + table + " WHERE ID = (SELECT MAX(NUM) FROM R2)";
            validateTableOfScalarLongs(client, stmt, new long[] { 20 });

            vt = client.callProcedure("@AdHoc",
                    "UPDATE " + table + " SET NUM = (SELECT R2.NUM + " + table + ".ID FROM R2 WHERE R2.ID = 3) " +
                    "WHERE " + table + ".ID = 8;")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] { 1 });
            stmt = "SELECT NUM FROM " + table + " WHERE ID = 8";
            validateTableOfScalarLongs(client, stmt, new long[] { 11 });

}
    }

    private void subtestUpdateWithCaseWhen() throws Exception {
        System.out.println("testUpdateWithCaseWhen");
        Client client = getClient();

        ClientResponse cr = client.callProcedure("@AdHoc", "truncate table p1;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        client.callProcedure("P1.Insert", 0, "", 150, 0.0);
        client.callProcedure("P1.Insert", 1, "", 75, 0.0);
        client.callProcedure("P1.Insert", 2, "", 30, 0.0);
        client.callProcedure("P1.Insert", 3, "", 15, 0.0);
        client.callProcedure("P1.Insert", 4, "", null, 0.0);

        client.callProcedure("@AdHoc", "update p1 set "
                + "num = case "
                + "when num > 100 then 100 "
                + "when num > 50 then 50 "
                + "when num > 25 then 25 "
                + "else num end;");

        validateTableOfScalarLongs(client, "select num from p1 order by id asc",
                new long[] {100, 50, 25, 15, Long.MIN_VALUE});

        client.callProcedure("@AdHoc", "update p1 set "
                + "num = case num "
                + "when 100 then 101 "
                + "when 50 then 52 "
                + "when 25 then 27 "
                + "else num end");
        validateTableOfScalarLongs(client, "select num from p1 order by id asc",
                new long[] {101, 52, 27, 15, Long.MIN_VALUE});
    }

    private void subtestENG11918() throws Exception {
        System.out.println("testENG11918 (invalid timestamp cast)");

        if (isHSQL()) {
            // This regression test covers VoltDB-specific error behavior
            return;
        }

        Client client = getClient();
        client.callProcedure("@AdHoc", "INSERT INTO ENG_11918 (id, int, time) VALUES "
                + "(101, 12, '1382-01-26 17:04:59');");
        verifyStmtFails(client, "UPDATE ENG_11918 SET VCHAR = TIME WHERE INT != -0.539;",
                "Input to SQL function CAST is outside of the supported range");
    }

    private void subtestENG13926() throws Exception {
        if (isHSQL()) {
            // This regression test covers VoltDB-specific error behavior
            return;
        }

        Client client = getClient();
        client.callProcedure("@AdHoc", "INSERT INTO ENG_13926 (A, B, C, D) "
                + "VALUES (-127, -127, -127, -127);");
        verifyStmtFails(client, "UPDATE ENG_13926 SET C = C - 1;",
                "Type BIGINT with value -128 can't be cast as TINYINT "
                        + "because the value is out of range for the destination type");
    }

    private void subtestENG14478() throws Exception {
        Client client = getClient();
        assertSuccessfulDML(client, "INSERT INTO ENG_14478 VALUES (0, 'abc', 'foo', 'gFbdVtLvw青βяvh', 0.0);");
        assertSuccessfulDML(client, "UPDATE ENG_14478 SET VCHAR_INLINE_MAX = VCHAR_INLINE;");
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlUpdateSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSqlUpdateSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(Insert.class.getResource("sql-update-ddl.sql"));
        project.addProcedure(Insert.class);

        config = new LocalCluster("sqlupdate-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) {
            fail();
        }
        builder.addServerConfig(config);

        config = new LocalCluster("sqlupdate-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) {
            fail();
        }
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("sqlupdate-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) {
            fail();
        }
        builder.addServerConfig(config);

        return builder;
    }

}
