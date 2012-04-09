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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicRONonSeqProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicROSeqProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicRWProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.NondeterministicROProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.NondeterministicRWProc;

public class TestVoltCompilerAnnotationsAndWarnings extends TestCase {
    public void testSimple() throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null" +
            ");" +
            "create table indexed_blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null, " +
            "PRIMARY KEY(ival)" +
            ");";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        builder.setCompilerDebugPrintStream(capturing);
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("blah", "ival");
        builder.addStmtProcedure("Insert", "insert into blah values (?, ?);", null);
        builder.addProcedures(NondeterministicROProc.class);
        builder.addProcedures(NondeterministicRWProc.class);
        builder.addProcedures(DeterministicRONonSeqProc.class);
        builder.addProcedures(DeterministicROSeqProc.class);
        builder.addProcedures(DeterministicRWProc.class);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("annotations.jar"));
        assert(success);
        String captured = capturer.toString("UTF-8");
        String[] lines = captured.split("\n");
        assertTrue(foundLineMatching(lines, ".*\\[RO].*NondeterministicROProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[RO].*NondeterministicROProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[RO].*DeterministicRONonSeqProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[RO].*\\[Seq].*DeterministicROSeqProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[RW].*Insert.*"));
        assertTrue(foundLineMatching(lines, ".*\\[RW].*BLAH.insert.*"));
        assertTrue(foundLineMatching(lines, ".*\\[RW].*NondeterministicRWProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[RW].*DeterministicRWProc.*"));

        assertTrue(countLinesMatching(lines, ".*\\[NDC].*NDC=true.*") == 2);

        assertFalse(foundLineMatching(lines, ".*\\[NDC].*NDC=false.*"));

        assertFalse(foundLineMatching(lines, ".*\\[RW].*NondeterministicROProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[RW].*DeterministicRONonSeqProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[RW].*\\[Seq].*DeterministicROSeqProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[Seq].*DeterministicRONonSeqProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[RO].*Insert.*"));
        assertFalse(foundLineMatching(lines, ".*\\[RO].*BLAH.insert.*"));
        assertFalse(foundLineMatching(lines, ".*\\[Seq].*Insert.*"));
        assertFalse(foundLineMatching(lines, ".*\\[Seq].*BLAH.insert.*"));
        assertFalse(foundLineMatching(lines, ".*\\[RO].*NondeterministicRWProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[RO].*DeterministicRWProc.*"));

    }

    private boolean foundLineMatching(String[] lines, String pattern) {
        for (String string : lines) {
            if (string.matches(pattern)) {
                return true;
            }
        }
        return false;
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
