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
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.fixedsql.Insert;

/**
 * System tests for DELETE
 * This is mostly cloned and modified from TestSqlUpdateSuite
 */

public class TestSqlDeleteSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    static final int ROWS = 10;

    private void executeAndTestDelete(String tableName, String deleteStmt, int numExpectedRowsChanged)
            throws IOException, ProcCallException {

        Client client = getClient();

        for (int i = 0; i < ROWS; ++i) {
            client.callProcedure("Insert", tableName, i, "desc", i, 14.5);
        }

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
    throws IOException, ProcCallException
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
    throws IOException, ProcCallException
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
    throws IOException, ProcCallException
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
    throws IOException, ProcCallException
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
    throws IOException, ProcCallException
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
    throws IOException, ProcCallException
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
    throws IOException, ProcCallException
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
    throws IOException, ProcCallException
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
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String delete = String.format("delete from %s where %s.NUM < 8 and %s.NUM > 5",
                                          table, table, table);
            executeAndTestDelete(table, delete, 2);
        }
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
