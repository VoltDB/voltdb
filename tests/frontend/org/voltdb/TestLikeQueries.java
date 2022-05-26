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

package org.voltdb;

import java.io.IOException;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

import junit.framework.TestCase;

public class TestLikeQueries extends TestCase {

    static class LikeTest {
        String pattern;
        int matches;
        boolean crashes;
        boolean addNot = false;
        String escape  = null;

        LikeTest(String pattern, int matches) {
            this.pattern = pattern;
            this.matches = matches;
            this.crashes = false;
        }

        LikeTest(String pattern, int matches, boolean crashes, boolean addNot, String escape) {
            this.pattern = pattern;
            this.matches = matches;
            this.crashes = crashes;
            this.addNot  = addNot;
            this.escape  = escape;
        }

        public String getClause() {
            String not = (this.addNot ? "NOT " : "");
            String escape = (this.escape != null ? String.format(" ESCAPE '%s'", this.escape) : "");
            return String.format("%sLIKE '%s'%s", not, this.pattern, escape);
        }
    }

    static class NotLikeTest extends LikeTest {
        NotLikeTest(String pattern, int matches) {
            super(pattern, matches, false, true, null);
        }
    }

    static class EscapeLikeTest extends LikeTest {
        EscapeLikeTest(String pattern, int matches, String escape) {
            super(pattern, matches, false, false, escape);
        }
    }

    static class UnsupportedEscapeLikeTest extends LikeTest {
        UnsupportedEscapeLikeTest(String pattern, int matches, String escape) {
            super(pattern, matches, true, false, escape);
        }
    }

    static class LikeTestData {
        public final String val;
        final String pat;
        LikeTestData(String val, String pat) {
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

    static final LikeTestData[] rowData = {
            new LikeTestData("aaaaaaa", "aaa%"),
            new LikeTestData("abcccc%", "abc%"),
            new LikeTestData("abcdefg", "abcdefg"),
            new LikeTestData("âxxxéyy", "âxxx%"),
            new LikeTestData("â🀲x一xxéyyԱ", "â🀲x一%"),
            new LikeTestData("â🀲x", "â🀲%"),
            new LikeTestData("ENG-14485", "ENG-14485%%"),
        };

    static final LikeTest[] tests = new LikeTest[] {
            // Patterns that pass (currently supported)
            new LikeTest("aaa%", 1),
            new LikeTest("abc%", 2),
            new LikeTest("AbC%", 0),
            new LikeTest("zzz%", 0),
            new LikeTest("%", rowData.length),
            new LikeTest("_%", rowData.length),
            new LikeTest("a%", 3),
            new LikeTest("âxxx%", 1),
            new LikeTest("aaaaaaa", 1),
            new LikeTest("aaa", 0),
            new LikeTest("abcdef_", 1),
            new LikeTest("ab_d_fg", 1),
            new LikeTest("%defg", 1),
            new LikeTest("%de%", 1),
            new LikeTest("â🀲x", 1),
            new LikeTest("â🀲x一xxéyyԱ", 1),
            new LikeTest("â🀲x_xxéyyԱ", 1),
            new LikeTest("â🀲x一xxéyy_", 1),
            new LikeTest("â🀲x一xéyyԱ", 0),
            // ENG-14485 handle two or more consecutive '%' characters
            new LikeTest("ENG-14485%%%", 1),
            new LikeTest("%%ENG-14485", 1),
            new LikeTest("EN%%G-14485", 1),
            new LikeTest("ENG-144__%%", 1),
            new LikeTest("%%%", rowData.length),

            new NotLikeTest("aaa%", rowData.length - 1),
            new EscapeLikeTest("ââ🀲x一xxéyyԱ", 1, "â"),
            new EscapeLikeTest("abccccâ%", 1, "â"),
            new EscapeLikeTest("abcccc|%", 1, "|"),
            new EscapeLikeTest("abc%", 2, "|"),
            new EscapeLikeTest("aaa", 0, "|"),
            new EscapeLikeTest("abccccccccc%", 1, "c"), // User can choose to confuse himself
    };

    static final LikeTest[] hsqlDiscrepencies = new LikeTest[] {
            // Patterns that fail on hsql (unsupported until someone fixes unicode handling).
            // We don't bother to run these in the head-to-head regression suite
            new LikeTest("â_x一xxéyyԱ", 1),
            new UnsupportedEscapeLikeTest("abcd!%%", 0, "!"),
    };

    public static class LikeSuite {
        public void doTests(Client client, boolean forHSQLcomparison) throws IOException, ProcCallException {
            loadForTests(client);
            if (! forHSQLcomparison) {
                doViaStoredProc(client);
            }
            doViaAdHoc(client, forHSQLcomparison);
        }

        private void loadForTests(Client client) throws IOException, ProcCallException {
            int id = 0;
            for (LikeTestData data : rowData) {
                id++;
                String query = String.format("insert into strings values (%d,'%s','%s');", id, data.val, data.pat);
                VoltTable modCount = client.callProcedure("@AdHoc", query).getResults()[0];
                assertEquals("Bad insert row count:", 1, modCount.getRowCount());
                assertEquals("Bad insert modification count:", 1, modCount.asScalarLong());
            }
        }

        private void doViaStoredProc(Client client) throws IOException {
            // Tests based on LikeTest list
            for (LikeTest test : tests) {
                doTestViaStoredProc(client, test);
            }
            for (LikeTest nonHsqlTest : hsqlDiscrepencies) {
                doTestViaStoredProc(client, nonHsqlTest);
            }
        }

        private void doViaAdHoc(Client client, boolean forHSQLcomparison) throws IOException, ProcCallException {
            // Test parameter values used as like expression
            for (LikeTest test : tests) {
                doTestViaAdHoc(client, test);
            }
            if ( ! forHSQLcomparison) {
                for (LikeTest otherTest : hsqlDiscrepencies) {
                    doTestViaAdHoc(client, otherTest);
                }
            }

            // Tests using PAT column for pattern data
            {
                //Without an escape this works
                String query = "select * from strings where val like pat";
                VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
                assertEquals(String.format("LIKE column test: bad row count:"),
                            rowData.length, result.getRowCount());

                //With an escape there is no way for the escape to propagate to the expression in EE
                //So HSQL should reject it due to our modifications
                query = "select * from strings where val like pat ESCAPE '?'";
                try {
                    result = client.callProcedure("@AdHoc", query).getResults()[0];
                    assertEquals(String.format("LIKE column test: bad row count:"),
                            rowData.length, result.getRowCount());
                } catch (ProcCallException e) {
                    System.out.println("LIKE column test failed (expected for now)");
                }
            }
        }

        private void doTestViaStoredProc(Client client, LikeTest test) throws IOException {
            String procName = null;
            if (test.getClass() == LikeTest.class) {
                procName = "SelectLike";
            } else if (test instanceof NotLikeTest) {
                procName = "NotLike";
            } else if (test instanceof EscapeLikeTest) {
                return;
            }
            if (test.getClass() == LikeTest.class) {
                System.out.printf("SelectLike pattern \"%s\"\n", test.pattern);
                try {
                    VoltTable result = client.callProcedure(procName, test.pattern).getResults()[0];
                    assertEquals(String.format("\"%s\": bad row count:", test.pattern),
                                 test.matches, result.getRowCount());
                    System.out.println(result.toString());
                    assertFalse(String.format("Expected to crash on \"%s\", but didn't", test.pattern), test.crashes);
                } catch (ProcCallException e) {
                    System.out.printf("LIKE pattern \"%s\" failed\n", test.pattern);
                    System.out.println(e.toString());
                    assertTrue("This failure was unexpected", test.crashes);
                    System.out.println("(This failure was expected)");
                }
            }
        }

        private void doTestViaAdHoc(Client client, LikeTest test) throws IOException {
            String clause = test.getClause();
            String query = String.format("select * from strings where val %s", clause);
            System.out.printf("LIKE clause \"%s\"\n", clause);
            try {
                VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
                assertEquals(String.format("\"%s\": bad row count:", clause),
                        test.matches, result.getRowCount());
                System.out.println(result.toString());
                assertFalse(String.format("Expected to crash on \"%s\", but didn't", clause), test.crashes);
            } catch (ProcCallException e) {
                System.out.printf("LIKE clause \"%s\" failed\n", clause);
                System.out.println(e.toString());
                assertTrue("This failure was unexpected on " + query, test.crashes);
                System.out.println("(This failure was expected)");
            }
        }
    }

    public void testLikeClause() throws Exception {

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhoc_like.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhoc_like.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(schema);
        builder.addPartitionInfo("STRINGS", "ID");
        builder.addStmtProcedure("Insert", "insert into strings values (?, ?, ?);");
        builder.addStmtProcedure("SelectLike", "select * from strings where  val like ?;");
        builder.addStmtProcedure("SelectNotLike", "select * from strings where  val not like ?;");
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Insert compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;
        ServerThread localServer = new ServerThread(config);

        Client client = null;

        try {
            localServer.start();
            localServer.waitForInitialization();

            client = ClientFactory.createClient();
            client.createConnection("localhost");

            LikeSuite tests = new LikeSuite();
            tests.doTests(client, false);
        }
        finally {
            if (client != null) client.close();

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
        }
    }
}
