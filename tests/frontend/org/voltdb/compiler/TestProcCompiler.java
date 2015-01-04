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
