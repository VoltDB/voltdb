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

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

import java.util.Arrays;

/**
 * System tests for DELETE
 * This is mostly cloned and modified from TestSqlUpdateSuite
 */

public class TestSqlDeleteSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    static final int ROWS = 10;

    private static void insertRows(Client client, String tableName, int numRows) throws Exception {
        for (int i = 0; i < numRows; ++i) {
            client.callProcedure("Insert", tableName, i, "desc", i, 14.5);
        }

    }

    private void executeAndTestDelete(String tableName, String deleteStmt, int numExpectedRowsChanged)
            throws Exception {

        Client client = getClient();

        insertRows(client, tableName, ROWS);

        VoltTable[] results = client.callProcedure("@AdHoc", deleteStmt).getResults();
        assertEquals(numExpectedRowsChanged, results[0].asScalarLong());

        int indexOfWhereClause = deleteStmt.toLowerCase().indexOf("where");
        String deleteWhereClause = "";
        if (indexOfWhereClause != -1) {
            deleteWhereClause = deleteStmt.substring(indexOfWhereClause);
        }
        else {
            deleteWhereClause = "";
        }

        String query = String.format("select count(*) from %s %s",
                                     tableName, deleteWhereClause);
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(0, results[0].asScalarLong());
    }

    public void testDelete()
    throws Exception
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = String.format("delete from %s",
                                          table, table);
            // Expect all rows to be deleted
            executeAndTestDelete(table, delete, ROWS);
        }
    }

    public void testDeleteWithEqualToIndexPredicate()
    throws Exception
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = String.format("delete from %s where %s.ID = 5",
                                          table, table);
            // Only row with ID = 5 should be deleted
            executeAndTestDelete(table, delete, 1);
        }
    }

    public void testDeleteWithEqualToNonIndexPredicate()
    throws Exception
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = String.format("delete from %s where %s.NUM = 5",
                                          table, table);
            // Only row with NUM = 5 should be deleted
            executeAndTestDelete(table, delete, 1);
        }
    }

    // This tests a bug found by the SQL coverage tool.  The code in HSQL
    // which generates the XML eaten by the planner didn't generate
    // anything in the <condition> element output for > or >= on an index
    public void testDeleteWithGreaterThanIndexPredicate()
    throws Exception
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = String.format("delete from %s where %s.ID > 5",
                                          table, table);
            // Rows 6-9 should be deleted
            executeAndTestDelete(table, delete, 4);
        }
    }

    public void testDeleteWithGreaterThanNonIndexPredicate()
    throws Exception
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = String.format("delete from %s where %s.NUM > 5",
                                          table, table);
            // rows 6-9 should be deleted
            executeAndTestDelete(table, delete, 4);
        }
    }

    public void testDeleteWithLessThanIndexPredicate()
    throws Exception
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = String.format("delete from %s where %s.ID < 5",
                                          table, table);
            // Rows 0-4 should be deleted
            executeAndTestDelete(table, delete, 5);
        }
    }

    // This tests a bug found by the SQL coverage tool.  The code in HSQL
    // which generates the XML eaten by the planner wouldn't combine
    // the various index and non-index join and where conditions, so the planner
    // would end up only seeing the first subnode written to the <condition>
    // element
    public void testDeleteWithOnePredicateAgainstIndexAndOneFalse()
    throws Exception
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = "delete from " + table +
                    " where " + table + ".NUM = 1000 and " + table + ".ID = 4";
            executeAndTestDelete(table, delete, 0);
        }
    }

    // This tests a bug found by the SQL coverage tool.  The code in HSQL
    // which generates the XML eaten by the planner wouldn't combine (AND)
    // the index begin and end conditions, so the planner would only see the
    // begin condition in the <condition> element.
    public void testDeleteWithRangeAgainstIndex()
    throws Exception
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = String.format("delete from %s where %s.ID < 8 and %s.ID > 5",
                                          table, table, table);
            executeAndTestDelete(table, delete, 2);
        }
    }

    public void testDeleteWithRangeAgainstNonIndex()
    throws Exception
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = String.format("delete from %s where %s.NUM < 8 and %s.NUM > 5",
                                          table, table, table);
            executeAndTestDelete(table, delete, 2);
        }
    }

    // Test replicated case with no where clause
    public void testDeleteWithOrderBy() throws Exception {
        if (isHSQL()) {
            return;
        }

        Client client = getClient();
        String[] stmtTemplates = {
                "DELETE FROM %s ORDER BY NUM ASC LIMIT 1",
                "DELETE FROM %s ORDER BY NUM DESC LIMIT 2",
                "DELETE FROM %s ORDER BY NUM LIMIT 3"};
        // Table starts with 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        long[][] expectedResults = {
                new long[] {1, 2, 3, 4, 5, 6, 7, 8, 9},
                new long[] {1, 2, 3, 4, 5, 6, 7},
                new long[] {4, 5, 6, 7}};

        insertRows(client, "P1", 10);
        insertRows(client, "R1", 10);

        VoltTable vt;
        for (int i = 0; i < stmtTemplates.length; ++i) {

            // Should succeed on replicated table
            String replStmt = String.format(stmtTemplates[i], "R1");
            int len = replStmt.length();
            long expectedRows = Long.valueOf(replStmt.substring(len - 1, len));
            vt = client.callProcedure("@AdHoc", replStmt).getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {expectedRows});

            vt = client.callProcedure("@AdHoc", "SELECT NUM FROM R1 ORDER BY NUM ASC").getResults()[0];
            validateTableOfScalarLongs(vt, expectedResults[i]);

            // In the partitioned case, we expect to get an error
            String partStmt = String.format(stmtTemplates[i], "P1");
            verifyStmtFails(client, partStmt, "Only single-partition DELETE statements "
                    + "may contain ORDER BY with LIMIT and/or OFFSET clauses.");
        }
    }

    private static void insertMoreRows(Client client, String tableName, int tens, int ones)
            throws Exception {
        for (int i = 0; i < tens; ++i) {
            for (int j = 0; j < ones; ++j) {
                client.callProcedure("Insert", tableName, i * 10, "desc", i * 10 + j, 14.5);
            }
        }
    }

    public void testDeleteWithWhereAndOrderBy() throws Exception {
        if (isHSQL()) {
            return;
        }

        // These queries can all be inferred single-partition for P1.
        Client client = getClient();
        String[] stmtTemplates = {
                "DELETE FROM %s WHERE ID = %d ORDER BY NUM ASC LIMIT 1",
                "DELETE FROM %s WHERE ID = %d AND DESC LIKE 'de%%' ORDER BY NUM DESC LIMIT 2",
                "DELETE FROM %s WHERE NUM = ID and ID = %d ORDER BY NUM LIMIT 3"
                };
        // Table starts with 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        long[][] expectedResults = {
                new long[] {1, 2},
                new long[] {10},
                new long[] {}
                //new long[] {4, 5, 6, 7}
                };
        long[] expectedCountStar = {
                29,
                27,
                24
                };

        VoltTable vt;
        for (String table : Arrays.asList("P3", "R3")) {
            insertMoreRows(client, table, 10, 3);
            for (int i = 0; i < stmtTemplates.length; ++i) {
                long key = i * 10;

                vt = client.callProcedure("@AdHoc", "select * from " + table).getResults()[0];
                System.out.println(vt);

                String stmt = String.format(stmtTemplates[i], table, key);
                int len = stmt.length();
                long expectedModCount = Long.valueOf(stmt.substring(len - 1, len));
                vt = client.callProcedure("@AdHoc", stmt).getResults()[0];
                validateTableOfScalarLongs(vt, new long[] {expectedModCount});

                // verify the rows that are left are what we expect
                vt = client.callProcedure("@AdHoc",
                        "select num from " + table + " where id = " + key + " order by num asc" ).getResults()[0];
                validateTableOfScalarLongs(vt, expectedResults[i]);

                // Total row count
                vt = client.callProcedure("@AdHoc",
                        "select count(*) from " + table ).getResults()[0];
                validateTableOfScalarLongs(vt, new long[] {expectedCountStar[i]});

            }
        }
    }

    public void testDeleteWithOrderByNegative() throws Exception {
        if (isHSQL()) {
            return;
        }

        Client client = getClient();

        // ORDER BY must have a LIMIT or OFFSET.
        // LIMIT or OFFSET may not appear by themselves.

        verifyStmtFails(client, "DELETE FROM R1 ORDER BY NUM ASC",
                "DELETE statement with ORDER BY but no LIMIT or OFFSET is not allowed.");
        verifyStmtFails(client, "DELETE FROM R1 LIMIT 1",
                "DELETE statement with LIMIT or OFFSET but no ORDER BY would produce non-deterministic results.");

        // This fails in a different way due to a bug in HSQL.  OFFSET with no LIMIT confuses HSQL.
        verifyStmtFails(client, "DELETE FROM R1 OFFSET 1",
                "PlanningErrorException");

        verifyStmtFails(client, "DELETE FROM R1 LIMIT 1 OFFSET 1",
                "DELETE statement with LIMIT or OFFSET but no ORDER BY would produce non-deterministic results.");
        verifyStmtFails(client, "DELETE FROM R1 OFFSET 1 LIMIT 1",
                "DELETE statement with LIMIT or OFFSET but no ORDER BY would produce non-deterministic results.");

        verifyStmtFails(client, "DELETE FROM P1_VIEW ORDER BY ID ASC LIMIT 1",
                "INSERT, UPDATE, or DELETE not permitted for view");
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlDeleteSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSqlDeleteSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addSchema(Insert.class.getResource("sql-update-ddl.sql"));
        project.addProcedures(PROCEDURES);

        config = new LocalCluster("sqldelete-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqldelete-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("sqldelete-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }

}
