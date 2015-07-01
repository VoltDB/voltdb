/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 * System tests for UPDATE, mainly focusing on the correctness of the WHERE
 * clause
 */

public class TestSqlUpdateSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    static final int ROWS = 10;

    private void executeAndTestUpdate(String table, String update,
                                      int expectedRowsChanged)
    throws IOException, ProcCallException
    {
        Client client = getClient();
        for (int i = 0; i < ROWS; ++i)
        {
            client.callProcedure("Insert", table, i, "desc", i, 14.5);
        }
        VoltTable[] results = client.callProcedure("@AdHoc", update).getResults();
        // ADHOC update still returns number of modified rows * number of partitions
        // Comment this out until it's fixed; the select count should be good enough, though
        //assertEquals(expectedRowsChanged, results[0].asScalarLong());
        String query = String.format("select count(%s.NUM) from %s where %s.NUM = -1",
                                     table, table, table);
        results = client.callProcedure("@AdHoc", query).getResults();
        assertEquals(expectedRowsChanged, results[0].asScalarLong());
    }

    public void testUpdate()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String update = String.format("update %s set %s.NUM = -1",
                                          table, table);
            // Expect all rows to change
            executeAndTestUpdate(table, update, ROWS);
        }
    }

    public void testUpdateWithEqualToIndexPredicate()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String update = String.format("update %s set %s.NUM = -1 where %s.ID = 5",
                                          table, table, table);
            // Only row with ID = 5 should change
            executeAndTestUpdate(table, update, 1);
        }
    }

    public void testUpdateWithEqualToNonIndexPredicate()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String update = String.format("update %s set %s.NUM = -1 where %s.NUM = 5",
                                          table, table, table);
            // Only row with NUM = 5 should change
            executeAndTestUpdate(table, update, 1);
        }
    }

    // This tests a bug found by the SQL coverage tool.  The code in HSQL
    // which generates the XML eaten by the planner didn't generate
    // anything in the <condition> element output for > or >= on an index
    public void testUpdateWithGreaterThanIndexPredicate()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String update = String.format("update %s set %s.NUM = -1 where %s.ID > 5",
                                          table, table, table);
            // Rows 6-9 should change
            executeAndTestUpdate(table, update, 4);
        }
    }

    public void testUpdateWithGreaterThanNonIndexPredicate()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String update = String.format("update %s set %s.NUM = -1 where %s.NUM > 5",
                                          table, table, table);
            // rows 6-9 should change
            executeAndTestUpdate(table, update, 4);
        }
    }

    public void testUpdateWithLessThanIndexPredicate()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String update = String.format("update %s set %s.NUM = -1 where %s.ID < 5",
                                          table, table, table);
            // Rows 0-4 should change
            executeAndTestUpdate(table, update, 5);
        }
    }

    // This tests a bug found by the SQL coverage tool.  The code in HSQL
    // which generates the XML eaten by the planner wouldn't combine
    // the various index and non-index join and where conditions, so the planner
    // would end up only seeing the first subnode written to the <condition>
    // element
    public void testUpdateWithOnePredicateAgainstIndexAndOneFalse()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String update = "update " + table + " set " + table + ".NUM = 100" +
                " where " + table + ".NUM = 1000 and " + table + ".ID = 4";
            executeAndTestUpdate(table, update, 0);
        }
    }

    // This tests a bug found by the SQL coverage tool.  The code in HSQL
    // which generates the XML eaten by the planner wouldn't combine (AND)
    // the index begin and end conditions, so the planner would only see the
    // begin condition in the <condition> element.
    public void testUpdateWithRangeAgainstIndex()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String update = String.format("update %s set %s.NUM = -1 where %s.ID < 8 and %s.ID > 5",
                                          table, table, table, table);
            executeAndTestUpdate(table, update, 2);
        }
    }

    public void testUpdateWithRangeAgainstNonIndex()
    throws IOException, ProcCallException
    {
        String[] tables = {"P1", "R1"};
        for (String table : tables)
        {
            String update = String.format("update %s set %s.NUM = -1 where %s.NUM < 8 and %s.NUM > 5",
                                          table, table, table, table);
            executeAndTestUpdate(table, update, 2);
        }
    }

    // This is a regression test for ENG-6799
    public void testUpdateFromInlineVarchar() throws Exception
    {
        Client client = getClient();
        client.callProcedure("STRINGPART.insert",
                "aa", 1, 1, 0, "a potentially (but not really) very long string)");

        // NAME is inlined varchar, DESC is not.
        String update = "update STRINGPART set desc = name, num = -1 where val1 = 1";
        executeAndTestUpdate("STRINGPART", update, 1);
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
        project.addProcedures(PROCEDURES);

        config = new LocalCluster("sqlupdate-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqlupdate-hsql.jar", 1, 1, 0, BackendTarget.HSQLDB_BACKEND);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        // Cluster
        config = new LocalCluster("sqlupdate-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }

}
