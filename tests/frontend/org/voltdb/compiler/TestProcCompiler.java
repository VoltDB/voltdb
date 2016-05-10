/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

package org.voltdb.compiler;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;

public class TestProcCompiler extends TestCase {

    public void testIndexOnConstant() throws Exception {
        String simpleSchema =
            "create table indexed_replicated_blah (" +
            "ival smallint default 0 not null, " +
            "sval varchar(255) not null, " +
            "PRIMARY KEY(ival)" +
            ");" +
            "";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        builder.setCompilerDebugPrintStream(capturing);
        builder.addLiteralSchema(simpleSchema);
        // Note: indexed_replicated_blah is left as a replicated table.

        // Test for index use on a constant equality using inequality queries as "control" cases.
        builder.addStmtProcedure("StmtIndexOnConstant",
                                 "select * from indexed_replicated_blah where ival = 1", null);
        builder.addStmtProcedure("StmtScanOnConstant",
                                 "select * from indexed_replicated_blah where ival <> 1", null);
        builder.addStmtProcedure("StmtScanOnConstant2",
                                 "select * from indexed_replicated_blah where ival <> 0", null);


        boolean success = builder.compile(Configuration.getPathToCatalogForTest("index_on_constant.jar"));
        assert(success);
        String captured = capturer.toString("UTF-8");
        System.out.println(captured);
        String[] lines = captured.split("\n");
        // ENG-4606 Verify that the compiled statement uses the index for a constant equality filter.
        // A result of 2 confirms that the legitimate table scans are getting trapped.
        // This builds confidence that there isn't a third one sneaking by in place of the index scan.
        assertEquals(2, countLinesMatching(lines, "^\\s*\\[TABLE SCAN]\\sselect.*indexed_replicated_blah.*"));
    }

    private String getQueryWithMaxPredicates() {
        // Query with 233 predicates - max allowable limit is 232 predicate
        final String query = "SELECT count(*) FROM FOO WHERE "
                        + "ID = 123 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test';";
        return query;
    }

    protected String getQueryWithMaxPlusOnePredicates() {
        // Query with 233 predicates - max allowable limit is 232 predicate
        final String query = "SELECT count(*) FROM FOO WHERE "
                        + "ID = 123 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102 and NAME = 'test' and ID > 102 and NAME = 'test' and "
                        + "ID > 102;";
        return query;
    }

    public void testStmntWithMaxPredicates() throws Exception {
        String simpleSchema =  "Create Table foo ( " +
                               "id BIGINT DEFAULT 0 NOT NULL, " +
                               "name VARCHAR(255) NOT NULL, " +
                               "PRIMARY KEY(id));";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);

        // Test for stored procedure with max allowable predicates
        String sql = getQueryWithMaxPredicates();
        builder.addStmtProcedure("StmntWithMaxPredicates", sql, null);

        boolean success = builder.compile(Configuration.getPathToCatalogForTest("max_predicates.jar"));
        assert(success);
    }

    public void testStmntWithMaxPlusOnePredicates() throws Exception {
        String schema =  "Create Table foo ( " +
                         "id BIGINT DEFAULT 0 NOT NULL, " +
                         "name VARCHAR(255) NOT NULL, " +
                         "PRIMARY KEY(id));";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        builder.setCompilerDebugPrintStream(capturing);
        builder.addLiteralSchema(schema);

        // Test for stored procedure which have more than max allowable predicates in the
        // results in error and does not crash or hang the system
        String sql = getQueryWithMaxPlusOnePredicates();
        builder.addStmtProcedure("StmntWithMaxPlusOnePredicates", sql, null);

        boolean success = builder.compile(Configuration.getPathToCatalogForTest("max_plus_one_predicates.jar"));
        assert(!success);
        String captured = capturer.toString("UTF-8");
        String errMsg = "Error: \"Limit of predicate expressions in \"where\" clause exceeded the maximum "
                      + "limit of 232 predicates, predicates detected: 233. Reduce the number of predicates "
                      + "in the \"where\" clause to 232";
        assert(captured.contains(errMsg));
    }

    private int countLinesMatching(String[] lines, String pattern) {
        int count = 0;
        for (String string : lines) {
            if (string.matches(pattern)) {
                ++count;
            }
        }
        return count;
    }
}
