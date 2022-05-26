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

import java.util.Arrays;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.delete.DeleteOrderByLimit;
import org.voltdb_testprocs.regressionsuites.delete.DeleteOrderByLimitOffset;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * System tests for DELETE
 * This is mostly cloned and modified from TestSqlUpdateSuite
 */

public class TestSqlDeleteSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] MP_PROCEDURES = {
        DeleteOrderByLimit.class,
        DeleteOrderByLimitOffset.class
        };

    static final int ROWS = 10;

    private static void insertOneRow(Client client, String tableName,
            long id, String desc, long num, double ratio) throws Exception {
        VoltTable vt = client.callProcedure("@AdHoc",
                "insert into " + tableName + " values ("
                + id + ", '" + desc + "', " + num + ", " + ratio + ")")
                .getResults()[0];
        vt.advanceRow();
        assertEquals(vt.getLong(0), 1);
    }

    private static void insertRows(Client client, String tableName, int numRows)
            throws Exception {
        for (int i = 0; i < numRows; ++i) {
            insertOneRow(client, tableName, i, "desc", i, 14.5);
        }

    }

    private void executeAndTestDelete(String tableName, String deleteStmt, int numExpectedRowsChanged)
            throws Exception {

        Client client = getClient();

        insertRows(client, tableName, ROWS);

        VoltTable[] results = client.callProcedure("@AdHoc", deleteStmt).getResults();
        assertEquals(numExpectedRowsChanged, results[0].asScalarLong());

        int indexOfWhereClause = deleteStmt.toLowerCase().indexOf("where");
        String deleteWhereClause;
        if (indexOfWhereClause != -1) {
            deleteWhereClause = deleteStmt.substring(indexOfWhereClause);
        }
        else {
            deleteWhereClause = "";
        }

        // Get the full table reference, including alias if one exists. This is needed
        // because the where clause can contain aliases.
        int indexOfTableRef = deleteStmt.toLowerCase().indexOf(tableName.toLowerCase());
        String deleteTableRef;
        if (indexOfTableRef != -1) {
            if (indexOfWhereClause != -1) {
                deleteTableRef = deleteStmt.substring(indexOfTableRef, indexOfWhereClause - 1);
            }
            else {
                deleteTableRef = deleteStmt.substring(indexOfTableRef);
            }
        }
        else {
            deleteTableRef = "";
        }

        String query = String.format("select count(*) from %s %s",
                                      deleteTableRef, deleteWhereClause);
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(0, results[0].asScalarLong());
    }

    private static void insertMoreRows(Client client, String tableName,
            int tens, int ones) throws Exception {
        for (int i = 0; i < tens; ++i) {
            for (int j = 0; j < ones; ++j) {
                insertOneRow(client, tableName,
                        i * 10, "desc", i * 10 + j, 14.5);
            }
        }
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
        verifyStmtFails(getClient(), "DELETE FROM P1 WHERE COUNT(*) = 1", "invalid WHERE expression");
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
            String delete =
                    String.format("delete from %s where %s.ID < 8 and %s.ID > 5",
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
            String delete =
                    String.format("delete from %s where %s.NUM < 8 and %s.NUM > 5",
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
        String[] stmtTemplates = { "DELETE FROM %s ORDER BY NUM ASC LIMIT 1",
                "DELETE FROM %s ORDER BY NUM DESC LIMIT 2",
                "DELETE FROM %s ORDER BY NUM LIMIT 3",
                "DELETE FROM %s ORDER BY NUM OFFSET 2",
                "DELETE FROM %s ORDER BY NUM OFFSET 0",
                };
        // Table starts with 0, 1, 2, 3, 4, 5, 6, 7, 8, 9
        long[][] expectedResults = {
                { 1, 2, 3, 4, 5, 6, 7, 8, 9 },
                { 1, 2, 3, 4, 5, 6, 7 },
                { 4, 5, 6, 7 },
                { 4, 5 },
                {}
                };

        insertRows(client, "P3", 10);
        insertRows(client, "R3", 10);

        for (int i = 0; i < stmtTemplates.length; ++i) {

            long numRowsBefore = client.callProcedure("@AdHoc", "select count(*) from R3")
                    .getResults()[0].asScalarLong();

            // Should succeed on replicated table
            String replStmt = String.format(stmtTemplates[i], "R3");
            long expectedRows = numRowsBefore - expectedResults[i].length;
            validateTableOfScalarLongs(client , replStmt, new long[] { expectedRows });

            validateTableOfScalarLongs(client, "SELECT NUM FROM R3 ORDER BY NUM ASC",
                    expectedResults[i]);

            // In the partitioned case, we expect to get an error
            String partStmt = String.format(stmtTemplates[i], "P3");
            verifyStmtFails(
                    client,
                    partStmt,
                    "DELETE statements affecting partitioned tables must "
                            + "be able to execute on one partition "
                            + "when ORDER BY and LIMIT or OFFSET clauses "
                            + "are present.");
        }
    }

    public void testDeleteWithWhereAndOrderBy() throws Exception {
        if (isHSQL()) {
            return;
        }

        Client client = getClient();
        for (String table : Arrays.asList("P3", "R3")) {
            insertMoreRows(client, table, 10, 3);
            // table now contains rows like this
            // ID  NUM  [other columns omitted]
            // 0   0
            // 0   1
            // 0   2
            // 10  10
            // 10  11
            // 10  12
            // ...
            // 90  90
            // 90  91
            // 90  92

            String countStmt = "select count(*) from " + table;

            // This statement avoids sorting by using index on NUM
            String stmt = "DELETE FROM " + table + " WHERE ID = 0 ORDER BY NUM ASC LIMIT 1";
            validateTableOfScalarLongs(client, stmt, new long[] {1});

            // verify the rows that are left are what we expect
            validateTableOfScalarLongs(client,
                    "select num from " + table + " where id = 0 order by num asc",
                    new long[] {1, 2});

            // Total row count-- make sure we didn't delete any other rows
            validateTableOfScalarLongs(client, countStmt, new long[] {29});

            /// ---------------------------------------------------------------------------------
            // Delete rows where num is 12, 11
            stmt = "DELETE FROM " + table
                    + " WHERE DESC LIKE 'de%' AND ID = 10 ORDER BY NUM DESC LIMIT 2";
            validateTableOfScalarLongs(client, stmt, new long[] { 2 });

            // verify the rows that are left are what we expect
            stmt = "select num from " + table + " where id = 10 order by num asc";
            validateTableOfScalarLongs(client, stmt, new long[] { 10 });

            // Total row count-- make sure we didn't delete any other rows
            stmt = "select count(*) from " + table;
            validateTableOfScalarLongs(client, stmt, new long[] { 27 });

            /// ---------------------------------------------------------------------------------
            // Delete rows where num is 22
            stmt = "DELETE FROM " + table
                    + " WHERE ID = 20 ORDER BY NUM LIMIT 10 OFFSET 2";
            validateTableOfScalarLongs(client, stmt, new long[] { 1 });

            // verify the rows that are left are what we expect
            stmt ="select num from " + table + " where id = 20 order by num asc";
            validateTableOfScalarLongs(client, stmt, new long[] { 20, 21 });

            // Total row count-- make sure we didn't delete any other rows
            validateTableOfScalarLongs(client, countStmt, new long[] { 26 });

            /// ---------------------------------------------------------------------------------
            // Delete rows where num is 31
            stmt = "DELETE FROM " + table
                    + " WHERE ID = 30 ORDER BY NUM LIMIT 1 OFFSET 1";
            validateTableOfScalarLongs(client, stmt, new long[] { 1 });

            // verify the rows that are left are what we expect
            stmt = "select num from " + table + " where id = 30 order by num asc";
            validateTableOfScalarLongs(client, stmt, new long[] { 30, 32 });

            // Total row count-- make sure we didn't delete any other rows
            validateTableOfScalarLongs(client, countStmt, new long[] { 25 });

            /// ---------------------------------------------------------------------------------
            // index used to evaluate predicate can also be used for order by
            stmt = "DELETE FROM " + table
                    + " WHERE ID = 40 AND NUM = 41 ORDER BY NUM LIMIT 1";
            validateTableOfScalarLongs(client, stmt, new long[] { 1 });

            // verify the rows that are left are what we expect
            stmt = "select num from " + table + " where id = 40 order by num asc";
            validateTableOfScalarLongs(client, stmt, new long[] { 40, 42 });

            // Total row count-- make sure we didn't delete any other rows
            validateTableOfScalarLongs(client, countStmt, new long[] { 24 });

            /// ---------------------------------------------------------------------------------
            // Indexes can't be used for either ORDER BY or WHERE
            stmt = "DELETE FROM " + table
                    + " WHERE ID = 50 AND RATIO > 0 ORDER BY DESC, NUM LIMIT 1";
            validateTableOfScalarLongs(client, stmt, new long[] { 1 });

            // verify the rows that are left are what we expect
            stmt = "select num from " + table + " where id = 50 order by num asc";
            validateTableOfScalarLongs(client, stmt, new long[] { 51, 52 });

            // Total row count-- make sure we didn't delete any other rows
            validateTableOfScalarLongs(client, countStmt, new long[] { 23 });
        }
    }

    public void testDeleteWithOrderByDeterminism() throws Exception {
        if (isHSQL()) {
            return;
        }

        Client client = getClient();

        // ORDER BY must have a LIMIT or OFFSET.
        // LIMIT or OFFSET may not appear by themselves.

        verifyStmtFails(client, "DELETE FROM R1 ORDER BY NUM ASC",
                "DELETE statement with ORDER BY but no LIMIT or OFFSET is not allowed.");
        verifyStmtFails(
                client,
                "DELETE FROM R1 LIMIT 1",
                "DELETE statement with LIMIT or OFFSET but no ORDER BY would produce non-deterministic results.");

        // This fails in a different way due to a bug in HSQL. OFFSET with no
        // LIMIT confuses HSQL.
        verifyStmtFails(client, "DELETE FROM R1 OFFSET 1",
                "SQL error while compiling query");

        verifyStmtFails(
                client,
                "DELETE FROM R1 LIMIT 1 OFFSET 1",
                "DELETE statement with LIMIT or OFFSET but no ORDER BY would produce non-deterministic results.");
        verifyStmtFails(
                client,
                "DELETE FROM R1 OFFSET 1 LIMIT 1",
                "DELETE statement with LIMIT or OFFSET but no ORDER BY would produce non-deterministic results.");

        verifyStmtFails(client, "DELETE FROM P1_VIEW ORDER BY ID ASC LIMIT 1",
                "DELETE with ORDER BY, LIMIT or OFFSET is currently unsupported on views");

        // Check failure for partitioned table where where clause cannot infer
        // partitioning
        verifyStmtFails(
                client,
                "DELETE FROM P1 WHERE ID < 50 ORDER BY NUM DESC LIMIT 1",
                "DELETE statements affecting partitioned tables must "
                        + "be able to execute on one partition "
                        + "when ORDER BY and LIMIT or OFFSET clauses "
                        + "are present.");

        // Non-deterministic ordering should fail!
        // RATIO is not unique.
        verifyStmtFails(client,
                "DELETE FROM P1 WHERE ID = 1 ORDER BY RATIO LIMIT 1",
                "statement manipulates data in a non-deterministic way");

        // Table P4 has a two-column unique constraint on RATIO and NUM
        // Ordering by only one column in a two column unique constraint should
        // fail, but both should work.
        verifyStmtFails(client,
                "DELETE FROM P4 WHERE ID = 1 ORDER BY RATIO LIMIT 1",
                "statement manipulates data in a non-deterministic way");
        verifyStmtFails(client,
                "DELETE FROM P4 WHERE ID = 1 ORDER BY NUM LIMIT 1",
                "statement manipulates data in a non-deterministic way");


        insertMoreRows(client, "P4", 1, 12);
        String stmt = "DELETE FROM P4 WHERE ID = 0 ORDER BY RATIO, NUM LIMIT 9";
        validateTableOfScalarLongs(client, stmt, new long[] { 9 });
        validateTableOfScalarLongs(client, "select num from P4 order by num",
                new long[] {9, 10, 11});

        // Ordering by all columns should be ok.
        // P5 has no unique or primary key constraints
        insertMoreRows(client, "P5", 1, 15);
        stmt = "DELETE FROM P5 WHERE ID = 0 ORDER BY NUM, DESC, ID, RATIO LIMIT 13";
        validateTableOfScalarLongs(client, stmt, new long[] { 13 });
        validateTableOfScalarLongs(client, "select num from P5 order by num",
                new long[] {13, 14});
    }

    public void testDeleteLimitParam() throws Exception {
        if (isHSQL()) {
            return;
        }

        Client client = getClient();

        // insert rows where ID is 0..19
        insertRows(client, "R1", 20);

        VoltTable vt;

        // delete the first 10 rows, ordered by ID
        vt = client.callProcedure("DeleteOrderByLimit", 10)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] { 10 });

        String stmt = "select id from R1 order by id asc";
        validateTableOfScalarLongs(client, stmt, new long[] { 10, 11, 12, 13, 14, 15, 16, 17, 18, 19 });
    }

    public void testDeleteLimitOffsetParam() throws Exception {
        if (isHSQL()) {
            return;
        }

        Client client = getClient();

        // insert rows where ID is 0..9
        insertRows(client, "R1", 10);

        VoltTable vt;

        // delete 5 rows, skipping the first three
        vt = client.callProcedure("DeleteOrderByLimitOffset", 5, 3)
                .getResults()[0];
        validateTableOfScalarLongs(vt, new long[] { 5 });

        String stmt = "select id from R1 order by id asc";
        validateTableOfScalarLongs(client, stmt, new long[] { 0, 1, 2, 8, 9 });
    }

    public void testDeleteOffsetParam() throws Exception {
        if (isHSQL()) {
            return;
        }

        Client client = getClient();
        String tables[] = {"P3", "R3"};

        for (String table : tables) {
            // insert rows where ID is 0 and num is 0..9
            insertMoreRows(client, table, 1, 10);

            // delete the last 2 rows where ID = 0, ordered by NUM
            VoltTable vt = client.callProcedure("@AdHoc",
                    "DELETE FROM " + table + " WHERE ID = 0 ORDER BY NUM OFFSET 8")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] { 2 });

            validateTableOfScalarLongs(client,
                    "select num from " + table + " order by num asc",
                    new long[] {0, 1, 2, 3, 4, 5, 6, 7});

            // Offset by 8 rows, but there are only 8 rows, so should delete nothing
            vt = client.callProcedure("@AdHoc",
                    "DELETE FROM " + table + " WHERE ID = 0 ORDER BY NUM OFFSET 8")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] { 0 });

            validateTableOfScalarLongs(client,
                    "select num from " + table + " order by num asc",
                    new long[] {0, 1, 2, 3, 4, 5, 6, 7});

            // offset with a where clause, and also some parameters.
            // This should delete rows where num is 4 and 5.
            String stmt = "delete from " + table + " where id = 0 and num between ? and ? order by num offset ?";
            vt = client.callProcedure("@AdHoc", stmt, 2, 5, 2)
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] {2});

            validateTableOfScalarLongs(client,
                    "select num from " + table + " order by num asc",
                    new long[] {0, 1, 2, 3, 6, 7});

            // Offset by 0 rows: should delete all rows.
            vt = client.callProcedure("@AdHoc",
                    "DELETE FROM " + table + " WHERE ID = 0 ORDER BY NUM OFFSET 0")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] { 6 });

            validateTableOfScalarLongs(client,
                    "select num from " + table + " order by num asc",
                    new long[] {});
        }
    }

    public void testDeleteWithExpresionSubquery()  throws Exception {
        Client client = getClient();
        String tables[] = {"P3", "R3"};
        // insert rows where ID is 0..3
        insertRows(client, "R1", 4);

        for (String table : tables) {
            // insert rows where ID is 0 and num is 0..9
            insertRows(client, table, 10);

            // delete rows where ID is IN 0..3
            VoltTable vt = client.callProcedure("@AdHoc",
                    "DELETE FROM " + table + " WHERE ID IN (SELECT NUM FROM R1)")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] { 4 });

            String stmt = "SELECT ID FROM " + table + " ORDER BY ID";
            validateTableOfScalarLongs(client, stmt, new long[] { 4, 5, 6, 7, 8, 9 });

            // delete rows where NUM is 4 and 5
            vt = client.callProcedure("@AdHoc",
                    "DELETE FROM " + table + " WHERE NUM IN (SELECT NUM + 2 FROM R1 WHERE R1.ID + 2 = " +
                    table + ".ID)")
                    .getResults()[0];
            validateTableOfScalarLongs(vt, new long[] { 2 });
            stmt = "SELECT NUM FROM " + table + " ORDER BY NUM";
            validateTableOfScalarLongs(client, stmt, new long[] { 6, 7, 8, 9 });
        }

    }

    // Test cases where the usages of table alias in delete statement are valid
    public void testDeleteTableAliasValid() throws Exception {
        Client client = getClient();

        // Aliasing in FROM without AS, refer to a column in WHERE without the alias
        String[] tables = {"P1", "R1"};
        String alias = "ALIAS";
        for (String table : tables)
        {
            String delete =
            String.format("DELETE FROM %s %s WHERE ID >= 0",
            table, alias);
            executeAndTestDelete(table, delete, ROWS);
        }

        // Aliasing in FROM with AS, refer to a column in WHERE with the alias
        for (String table : tables)
        {
            String delete =
            String.format("DELETE FROM %s AS %s WHERE %s.ID >= 0",
            table, alias, alias);
            executeAndTestDelete(table, delete, ROWS);
        }

        // Aliasing in FROM without AS, refer to a column in WHERE without the alias
        for (String table : tables)
        {
            String delete =
            String.format("DELETE FROM %s %s WHERE ID >= 0",
            table, alias);
            executeAndTestDelete(table, delete, ROWS);
        }

        // Aliasing in FROM with AS, refer to a column in WHERE with the alias
        for (String table : tables)
        {
            String delete =
            String.format("DELETE FROM %s AS %s WHERE %s.ID >= 0",
            table, alias, alias);
            executeAndTestDelete(table, delete, ROWS);
        }
    }

    // Test cases where the usages of table alias in delete statement are invalid
    public void testDeleteTableAliasInvalid() throws Exception {
        Client client = getClient();

        // Aliasing in FROM with AS, refer to a column in WHERE with the original table
        verifyStmtFails(client, "DELETE FROM P1 AS P WHERE P1.ID < 8 AND P1.ID > 5",
        "object not found: P1.ID");

        // Aliasing in FROM without AS, refer to a column in WHERE with the original table
        verifyStmtFails(client, "DELETE FROM P1 P WHERE P1.ID < 8 AND P1.ID > 5",
        "object not found: P1.ID");

        // Using LIMIT, a reserved keyword, as a table alias
        verifyStmtFails(client, "DELETE FROM P1 AS LIMIT WHERE P1.ID < 8 and P1.ID > 5",
        "unexpected token: LIMIT");
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
        project.addMultiPartitionProcedures(MP_PROCEDURES);

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
