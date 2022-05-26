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

package org.voltdb.compiler;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.voltdb.VoltDB.Configuration;

import junit.framework.TestCase;

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
                                 "select * from indexed_replicated_blah where ival = 1");
        builder.addStmtProcedure("StmtScanOnConstant",
                                 "select * from indexed_replicated_blah where ival <> 1");
        builder.addStmtProcedure("StmtScanOnConstant2",
                                 "select * from indexed_replicated_blah where ival <> 0");


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

    protected static String getQueryForFoo(int numberOfPredicates) {
        StringBuilder string = new StringBuilder("SELECT * FROM FOO ");
        if (numberOfPredicates > 0) {
            string.append("WHERE ID = 10 ");
            for (int i = 1; i < numberOfPredicates; i++) {
                string.append("AND ID > 1 ");
            }
        }
        string.append(";");
        return string.toString();
    }

    public void testStmtWithLotsOfPredicates() throws Exception {
        String simpleSchema =  "Create Table foo ( " +
                               "id BIGINT DEFAULT 0 NOT NULL, " +
                               "name VARCHAR(255) NOT NULL, " +
                               "PRIMARY KEY(id));";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(simpleSchema);

        // Generate a stored procedure with a query with a lot of predicates.
        String sql = getQueryForFoo(350);
        builder.addStmtProcedure("StmtWithPredicates", sql);

        boolean success = builder.compile(Configuration.getPathToCatalogForTest("lots_of_predicates.jar"));
        assert(success);
    }

    public void testStmtForStackOverflowCondition() throws Exception {
        boolean success = true;
        ByteArrayOutputStream capturer = null;
        for (int npreds = 2000; success && npreds < 100000; npreds += 1000) {
            String schema =  "Create Table foo ( " +
                             "id BIGINT DEFAULT 0 NOT NULL, " +
                             "name VARCHAR(255) NOT NULL, " +
                             "PRIMARY KEY(id));";

            VoltProjectBuilder builder = new VoltProjectBuilder();
            capturer = new ByteArrayOutputStream();
            PrintStream capturing = new PrintStream(capturer);
            builder.setCompilerDebugPrintStream(capturing);
            builder.addLiteralSchema(schema);

            // Test that a stored procedure with more than the max allowable predicates
            // results in an error and does not crash or hang the system.
            String sql = getQueryForFoo(npreds);
            builder.addStmtProcedure("StmtForStackOverFlow", sql);
            success = builder.compile(Configuration.getPathToCatalogForTest("max_plus_predicates.jar"));
        }

        assert(!success);
        String captured = capturer.toString("UTF-8");
        String errMsg = "Encountered stack overflow error. " +
                        "Try reducing the number of predicate expressions in the query";
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
