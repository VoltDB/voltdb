/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestLikeQueries extends TestCase {

    static class LikeTest
    {
        String pattern;
        int matches;
        boolean crashes;
        boolean addNot = false;
        String escape  = null;

        public LikeTest(String pattern, int matches) {
            this.pattern = pattern;
            this.matches = matches;
            this.crashes = false;
        }

        public LikeTest(String pattern, int matches, boolean crashes, boolean addNot, String escape) {
            this.pattern = pattern;
            this.matches = matches;
            this.crashes = crashes;
            this.addNot  = addNot;
            this.escape  = escape;
        }

        public String getClause() {
            String not = (this.addNot ? "NOT " : "");
            String escape = (this.escape != null ? String.format(" ESCAPE '%s'", this.escape) : "");
            String clause = String.format("%sLIKE '%s'%s", not, this.pattern, escape);
            return clause;
        }
    }

    static class NotLikeTest extends LikeTest {
        public NotLikeTest(String pattern, int matches) {
            super(pattern, matches, false, true, null);
        }
    }

    static class EscapeLikeTest extends LikeTest {
        public EscapeLikeTest(String pattern, int matches, String escape) {
            super(pattern, matches, false, false, escape);
        }
    }

    static class UnsupportedLikeTest extends LikeTest {
        public UnsupportedLikeTest(String pattern, int matches) {
            super(pattern, matches, true, false, null);
        }
    }

    static class UnsupportedEscapeLikeTest extends LikeTest {
        public UnsupportedEscapeLikeTest(String pattern, int matches, String escape) {
            super(pattern, matches, true, false, escape);
        }
    }

    static class LikeTestData {
        public final String val;
        public final String pat;
        public LikeTestData(String val, String pat) {
            this.val = val;
            this.pat = pat;
        }
    }

    public void testLikeClause() throws Exception {

        final String schema =
            "create table STRINGS (" +
            "ID int default 0 not null, " +
            "VAL varchar(32) default null," +
            "PAT varchar(32) default null," +
            "PRIMARY KEY(ID));";

        final LikeTestData[] rowData = {
            new LikeTestData("aaaaaaa", "aaa%"),
            new LikeTestData("abcccc%", "abc%"),
            new LikeTestData("abcdefg", "abcdefg"),
            new LikeTestData("âxxxéyy", "âxxx%"),
        };

        final LikeTest[] tests = {
            // Patterns that pass (currently supported)
            new LikeTest("aaa%", 1),
            new LikeTest("abc%", 2),
            new LikeTest("AbC%", 0),
            new LikeTest("zzz%", 0),
            new LikeTest("%", rowData.length),
            new LikeTest("a%", 3),
            new LikeTest("âxxx%", 1),
            new NotLikeTest("aaa%", rowData.length - 1),
            new EscapeLikeTest("abcccc|%", 1, "|"),
            new EscapeLikeTest("abc%", 2, "|"),
            new EscapeLikeTest("aaa", 0, "|"),
            // Patterns that fail (unsupported until we fix the parser)
            new UnsupportedLikeTest("aaaaaaa", 0),
            new UnsupportedLikeTest("aaa", 0),
            new UnsupportedLikeTest("abcdef_", 1),
            new UnsupportedLikeTest("ab_d_fg", 1),
            new UnsupportedLikeTest("%defg", 1),
            new UnsupportedLikeTest("%de%", 1),
            new UnsupportedLikeTest("%de% ", 0),
            new UnsupportedEscapeLikeTest("abcd!%%", 0, "!"),
        };

        String pathToCatalog = Configuration.getPathToCatalogForTest("adhoc_like.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhoc_like.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(schema);
        builder.addPartitionInfo("STRINGS", "ID");
        builder.addStmtProcedure("Insert", "insert into strings values (?, ?, ?);", null);
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

            // do the test
            client = ClientFactory.createClient();
            client.createConnection("localhost");

            int id = 0;
            for (LikeTestData data : rowData) {
                id++;
                String query = String.format("insert into strings values (%d,'%s','%s');", id, data.val, data.pat);
                VoltTable modCount = client.callProcedure("@AdHoc", query).getResults()[0];
                assertEquals("Bad insert row count:", 1, modCount.getRowCount());
                assertEquals("Bad insert modification count:", 1, modCount.asScalarLong());
            }

            // Tests based on LikeTest list
            for (LikeTest test : tests) {
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
                    assertTrue("This failure was unexpected", test.crashes);
                    System.out.println("(This failure was expected)");
                }
            }

            // Tests using PAT column for pattern data
            {
                // Expact all PAT column patterns to produce a match with the VAL column string.
                // We don't support non-literal likes yet. Remove the catch when we do.
                String query = "select * from strings where val like pat";
                try {
                    VoltTable result = client.callProcedure("@AdHoc", query).getResults()[0];
                    assertEquals(String.format("LIKE column test: bad row count:"),
                                 tests.length, result.getRowCount());
                } catch (ProcCallException e) {
                    System.out.println("LIKE column test failed (expected for now)");
                }
            }
        }
        finally {
            if (client != null) client.close();
            client = null;

            if (localServer != null) {
                localServer.shutdown();
                localServer.join();
            }
            localServer = null;

            // no clue how helpful this is
            System.gc();
        }
    }
}
