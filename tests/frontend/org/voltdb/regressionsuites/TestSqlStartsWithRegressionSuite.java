/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import org.voltdb_testprocs.regressionsuites.aggregates.Insert;

import junit.framework.TestCase;

final class TestStartsWithQueries extends TestCase {

    static class StartsWithTest
    {
        String pattern;
        int matches;
        boolean crashes;
        boolean addNot = false;

        public StartsWithTest(String pattern, int matches) {
            this.pattern = pattern;
            this.matches = matches;
            this.crashes = false;
        }

        public StartsWithTest(String pattern, int matches, boolean crashes, boolean addNot) {
            this.pattern = pattern;
            this.matches = matches;
            this.crashes = crashes;
            this.addNot  = addNot;
        }

        public String getClause() {
            String not = (this.addNot ? "NOT " : "");
            String clause = String.format("%sSTARTS WITH '%s'", not, this.pattern);
            return clause;
        }
    }

    static class NotStartsWithTest extends StartsWithTest {
        public NotStartsWithTest(String pattern, int matches) {
            super(pattern, matches, false, true);
        }
    }

    static class UnsupportedStartsWithTest extends StartsWithTest {
        public UnsupportedStartsWithTest(String pattern, int matches) {
            super(pattern, matches, true, false);
        }
    }

    static class StartsWithTestData {
        public final String val;
        public final String pat;
        public StartsWithTestData(String val, String pat) {
            this.val = val;
            this.pat = pat;
        }
    }

    public static final String schema =
            "create table STRINGS (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PAT varchar(32) default null," +
            "PRIMARY KEY(ID));";

    static final StartsWithTestData[] rowData = {
            new StartsWithTestData("aaaaaaa", "aaa"),
            new StartsWithTestData("abcccc%", "abc"),
            new StartsWithTestData("abcdefg", "abcdefg"),
            new StartsWithTestData("âxxxéyy", "âxxx"),
            new StartsWithTestData("â🀲x一xxéyyԱ", "â🀲x一"),
            new StartsWithTestData("â🀲x", "â🀲"),
        };

    static final StartsWithTest[] tests = new StartsWithTest[] {
            // Patterns that pass (currently supported)
            new StartsWithTest("aaa", 1),
            new StartsWithTest("abc", 2),
            new StartsWithTest("AbC", 0),
            new StartsWithTest("zzz", 0),
            new StartsWithTest("", rowData.length),
            new StartsWithTest("a", 3),
            new StartsWithTest("âxxx", 1),
            new StartsWithTest("aaaaaaa", 1),
            new StartsWithTest("abcdef", 1),
            new StartsWithTest("abcdef_", 0),
            new StartsWithTest("ab_d_fg", 0),
            new StartsWithTest("â🀲x", 2),
            new StartsWithTest("â🀲x一xxéyyԱ", 1),
            new StartsWithTest("â🀲x一xéyyԱ", 0),
            new NotStartsWithTest("aaa", rowData.length - 1),
    };

    public static class StartsWithSuite {

        public StartsWithSuite(Client client) {
            try {
                loadForTests(client);
            } catch (IOException | ProcCallException e) {
                e.printStackTrace();
            }
        }

        public void doTests(Client client, boolean forHSQLcomparison) throws IOException, NoConnectionsException, ProcCallException {
            if (forHSQLcomparison == false) {
                doViaStoredProc(client);
            }
            doViaAdHoc(client, forHSQLcomparison);
        }

        private void loadForTests(Client client) throws IOException, NoConnectionsException, ProcCallException {
            int id = 0;
            for (StartsWithTestData data : rowData) {
                id++;
                String query = String.format("insert into strings values (%d,'%s','%s');", id, data.val, data.pat);
                VoltTable modCount = client.callProcedure("@AdHoc", query).getResults()[0];
                assertEquals("Bad insert row count:", 1, modCount.getRowCount());
                assertEquals("Bad insert modification count:", 1, modCount.asScalarLong());
            }
        }

        protected void doViaStoredProc(Client client) throws IOException, NoConnectionsException {
            // Tests based on the above StartsWith test
            for (StartsWithTest test : tests) {
                doTestViaStoredProc(client, test);
            }
        }

        private void doViaAdHoc(Client client, boolean forHSQLcomparison) throws IOException, NoConnectionsException, ProcCallException {
            // Test parameter values used as in starts with expression
            for (StartsWithTest test : tests) {
                doTestViaAdHoc(client, test);
            }

            // Tests using PAT column as the pattern for matching
            {
                String query = "select * from strings where val starts with pat";
                VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
                assertEquals(String.format("STARTS WITH column test: bad row count:"),
                            rowData.length, result.getRowCount());
            }
        }

        private void doTestViaStoredProc(Client client, StartsWithTest test) throws IOException, NoConnectionsException {
            String procName = null;
            if (test.getClass() == StartsWithTest.class) {
                procName = "SelectStartsWith";
            } else if (test instanceof NotStartsWithTest) {
                procName = "NotStartsWith";
            }
            if (test.getClass() == StartsWithTest.class) {
                System.out.printf("SelectStartsWith pattern \"%s\"\n", test.pattern);
                try {
                    VoltTable result = client.callProcedure(procName, test.pattern).getResults()[0];
                    assertEquals(String.format("\"%s\": bad row count:", test.pattern),
                                 test.matches, result.getRowCount());
                    System.out.println(result.toString());
                    assertFalse(String.format("Expected to crash on \"%s\", but didn't", test.pattern), test.crashes);
                } catch (ProcCallException e) {
                    System.out.printf("STARTS WITH pattern \"%s\" failed\n", test.pattern);
                    System.out.println(e.toString());
                    assertTrue("This failure was unexpected", test.crashes);
                    System.out.println("(This failure was expected)");
                }
            }
        }

        private void doTestViaAdHoc(Client client, StartsWithTest test) throws IOException, NoConnectionsException {
            String clause = test.getClause();
            String query = String.format("select * from strings where val %s", clause);
            System.out.printf("STARTS WITH clause \"%s\"\n", clause);
            try {
                VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
                assertEquals(String.format("\"%s\": bad row count:", clause),
                        test.matches, result.getRowCount());
                System.out.println(result.toString());
                assertFalse(String.format("Expected to crash on \"%s\", but didn't", clause), test.crashes);
            } catch (ProcCallException e) {
                System.out.printf("STARTS WITH clause \"%s\" failed\n", clause);
                System.out.println(e.toString());
                assertTrue("This failure was unexpected", test.crashes);
                System.out.println("(This failure was expected)");
            }
        }
    }

}

/**
 * System tests for basic STARTS WITH functionality
 */
public class TestSqlStartsWithRegressionSuite extends RegressionSuite {

    /** Procedures used by this suite */
    static final Class<?>[] PROCEDURES = { Insert.class };

    static final int ROWS = 10;

    public void testStartsWith() throws IOException, ProcCallException
    {
        Client client = getClient();
        TestStartsWithQueries.StartsWithSuite tests = new TestStartsWithQueries.StartsWithSuite(client);
        tests.doTests(client, true);
        tests.doTests(client, false);
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestSqlStartsWithRegressionSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSqlStartsWithRegressionSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        try {
            project.addLiteralSchema(TestStartsWithQueries.schema);
        } catch (IOException e) {
            e.printStackTrace();
        }
        project.addPartitionInfo("STRINGS", "ID");
        project.addStmtProcedure("Insert", "insert into strings values (?, ?, ?);");
        project.addStmtProcedure("SelectStartsWith", "select * from strings where val starts with ?;");
        project.addStmtProcedure("NotStartsWith", "select * from strings where val not starts with ?;");

        config = new LocalCluster("sqllike-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqllike-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        config = new LocalCluster("sqllike-twosites.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        if (!config.compile(project)) fail();
        builder.addServerConfig(config);

        return builder;
    }
}
