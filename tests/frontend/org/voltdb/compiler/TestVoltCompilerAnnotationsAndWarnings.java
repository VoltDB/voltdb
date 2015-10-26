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

import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.procedures.FloatParamToGetNiceComplaint;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicRONonSeqProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicROSeqProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicRWProc1;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicRWProc2;
import org.voltdb_testprocs.regressionsuites.failureprocs.FloatingInsertAggregates;
import org.voltdb_testprocs.regressionsuites.failureprocs.FloatingUpdateAggregates;
import org.voltdb_testprocs.regressionsuites.failureprocs.NondeterministicROProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.NondeterministicRWProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPNoncandidate1;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPNoncandidate2;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPNoncandidate3;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPNoncandidate4;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPNoncandidate5;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPNoncandidate6;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPcandidate1;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPcandidate2;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPcandidate3;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPcandidate4;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPcandidate5;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPcandidate6;
import org.voltdb_testprocs.regressionsuites.failureprocs.ProcSPcandidate7;

import junit.framework.TestCase;

public class TestVoltCompilerAnnotationsAndWarnings extends TestCase {

    /**
     * Test that a DDL stored procedure does not compile properly. The test
     * testSimple is not very useful for this.
     *
     * @param simpleSchema
     *            A string containing the test schema.
     * @param partitionInfo
     *            Partitioning information. This can be null if none is wanted.
     * @param procKlazz
     *            The class to test.
     * @param errorMessages
     *            The error messages we expect to see.
     * @throws Exception
     */
    public void testCompilationFailure(String simpleSchema,
                                       String[][] partitionInfo,
                                       Object     procObject,
                                       String[]   errorMessages) throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        builder.setCompilerDebugPrintStream(capturing);
        builder.addLiteralSchema(simpleSchema);
        if (partitionInfo != null) {
            for (String[] pair : partitionInfo) {
                assertTrue(pair.length == 2);
                builder.addPartitionInfo(pair[0], pair[1]);
            }
        }
        if (procObject instanceof String[]) {
            String[] stmtProcDescrip = (String[]) procObject;
            assertTrue(stmtProcDescrip.length == 2);
            builder.addStmtProcedure(stmtProcDescrip[0], stmtProcDescrip[1]);
        } else if (procObject instanceof Class<?>) {
            Class<?> procKlazz = (Class<?>) procObject;
            builder.addProcedures(procKlazz);
        } else {
            assertTrue("Bad type of object for parameter \"procObject\"", false);
        }

        boolean success = builder.compile(Configuration.getPathToCatalogForTest("annotations.jar"));
        assertFalse(success);
        String captured = capturer.toString("UTF-8");
        String[] lines = captured.split("\n");
        // Output should include a line suggesting replacement of float with
        // double.
        for (String oneMessagePattern : errorMessages) {
            assertTrue(foundLineMatching(lines, oneMessagePattern));
        }
    }
    public void testFloatParamComplaint() throws Exception {
        String simpleSchema =
            "create table floatie (" +
            "ival bigint default 0 not null, " +
            "fval float not null," +
            "PRIMARY KEY(ival)" +
            ");";
        String[][] partitionInfo = new String[][] { { "floatie", "ival" } };
        testCompilationFailure(simpleSchema,
                               partitionInfo,
                               FloatParamToGetNiceComplaint.class,
                               new String[] { ".*FloatParamToGetNiceComplaint.* float.* double.*" });
    }

    public void testFloatingInsertAggregates() throws Exception {
        String simpleSchema =
                "create table floatingaggs_input ( alpha float );" +
                "create table floatingaggs_output ( beta float );" +
                "";
        testCompilationFailure(simpleSchema,
                               null,
                               FloatingInsertAggregates.class,
                               new String[] { ".*FloatingInsertAggregates.*" });
    }

    public void testFloatingAggregatesDDL() throws Exception {
        String simpleSchema = "create table floatingaggs_input ( alpha float );" + "create table floatingaggs_output ( beta float );" + "";
        testCompilationFailure(simpleSchema,
                               null,
                               new String[] { "FLoatingDDLInsertAggregates", "create procedure FloatingDDLInsertAggregates as insert into floatingaggs_output select sum(alpha) from floatingaggs_input;",
                               },
                               new String[] { ".*FloatingDDLInsertAggregates.*" });
    }

    public void testFloatingUpdateAggregates() throws Exception {
        String simpleSchema = "create table floatingaggs_input ( alpha float );" + "create table floatingaggs_output ( beta float );" + "";
        testCompilationFailure(simpleSchema,
                               null,
                               FloatingUpdateAggregates.class,
                               new String[] { ".*FloatingUpdateAggregates.*" });
    }

    public void testSimple() throws Exception {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null" +
            ");" +
            "create table indexed_replicated_blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null, " +
            "PRIMARY KEY(ival)" +
            ");" +
            "create table indexed_partitioned_blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null, " +
            "PRIMARY KEY(ival)" +
            ");" +
            "create table floatingaggs_input (" +
            "alpha float" +
            ");" +
            "create table floatingaggs_output (" +
            "beta float" +
            ");" +
            "";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        builder.setCompilerDebugPrintStream(capturing);
        builder.addLiteralSchema(simpleSchema);
        builder.addPartitionInfo("blah", "ival");
        builder.addPartitionInfo("indexed_partitioned_blah", "ival");
        // Note: indexed_replicated_blah is left as a replicated table.
        builder.addStmtProcedure("Insert",
                // Include lots of filthy whitespace to test output cleanup.
                "insert                            into\t \tblah values\n\n(? \t ,\t\t\t?)                           ;", null);
        builder.addProcedures(NondeterministicROProc.class);
        builder.addProcedures(NondeterministicRWProc.class);
        builder.addProcedures(DeterministicRONonSeqProc.class);
        builder.addProcedures(DeterministicROSeqProc.class);
        builder.addProcedures(DeterministicRWProc1.class);
        builder.addProcedures(DeterministicRWProc2.class);
        builder.addProcedures(FloatingUpdateAggregates.class);

        builder.addProcedures(ProcSPcandidate1.class);
        builder.addProcedures(ProcSPcandidate2.class);
        builder.addProcedures(ProcSPcandidate3.class);
        builder.addProcedures(ProcSPcandidate4.class);
        builder.addProcedures(ProcSPcandidate5.class);
        builder.addProcedures(ProcSPcandidate6.class);
        builder.addProcedures(ProcSPcandidate7.class);

        builder.addProcedures(ProcSPNoncandidate1.class);
        builder.addProcedures(ProcSPNoncandidate2.class);
        builder.addProcedures(ProcSPNoncandidate3.class);
        builder.addProcedures(ProcSPNoncandidate4.class);
        builder.addProcedures(ProcSPNoncandidate5.class);
        builder.addProcedures(ProcSPNoncandidate6.class);

        builder.addStmtProcedure("StmtSPcandidate1", "select count(*) from blah where ival = ?", null);
        builder.addStmtProcedure("StmtSPcandidate2", "select count(*) from blah where ival = 12345678", null);
        builder.addStmtProcedure("StmtSPcandidate3",
                                 "select count(*) from blah, indexed_replicated_blah " +
                                 "where indexed_replicated_blah.sval = blah.sval and blah.ival = 12345678", null);
        builder.addStmtProcedure("StmtSPcandidate4",
                                 "select count(*) from blah, indexed_replicated_blah " +
                                 "where indexed_replicated_blah.sval = blah.sval and blah.ival = abs(1)+1", null);
        builder.addStmtProcedure("StmtSPcandidate5", "select count(*) from blah where sval = ? and ival = 12345678", null);
        builder.addStmtProcedure("StmtSPcandidate6", "select count(*) from blah where sval = ? and ival = ?", null);
        builder.addStmtProcedure("StmtSPNoncandidate1", "select count(*) from blah where sval = ?", null);
        builder.addStmtProcedure("StmtSPNoncandidate2", "select count(*) from blah where sval = '12345678'", null);
        builder.addStmtProcedure("StmtSPNoncandidate3", "select count(*) from indexed_replicated_blah where ival = ?", null);
        builder.addStmtProcedure("FullIndexScan", "select ival, sval from indexed_replicated_blah", null);
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("annotations.jar"));
        assert(success);
        String captured = capturer.toString("UTF-8");
        System.out.println(captured);
        String[] lines = captured.split("\n");

        assertTrue(foundLineMatching(lines, ".*\\[READ].*NondeterministicROProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[READ].*NondeterministicROProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[READ].*DeterministicRONonSeqProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[READ].*DeterministicROSeqProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[WRITE].*Insert.*"));
        assertTrue(foundLineMatching(lines, ".*\\[WRITE].*NondeterministicRWProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[WRITE].*DeterministicRWProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[TABLE SCAN].*select ival, sval from indexed_replicated_blah.*"));
        // Find FloatingUpdate error lines.
        assertTrue(foundLineMatching(lines, ".*\\[MP]\\[WRITE].*FloatingUpdateAggregates"));
        assertTrue(foundLineMatching(lines,
                                     ".*\\[TABLE SCAN].*\\[NDC] select sum[(]alpha[)] from floatingaggs_input;.*"));
        assertTrue(foundLineMatching(lines,
                                     ".*Procedure FloatingUpdateAggregates.*Aggregate functions of floating point columns may not be deterministic[.]  We suggest converting to DECIMAL[.]"));

        assertEquals(1, countLinesMatching(lines, ".*\\[NDC].*NDC=true.*"));

        assertFalse(foundLineMatching(lines, ".*\\[NDC].*NDC=false.*"));

        assertFalse(foundLineMatching(lines, ".*\\[WRITE].*NondeterministicROProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[WRITE].*DeterministicRONonSeqProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[WRITE].*DeterministicROSeqProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[TABLE SCAN].*DeterministicRONonSeqProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[READ].*Insert.*"));
        assertFalse(foundLineMatching(lines, ".*\\[READ].*BLAH.insert.*"));
        assertFalse(foundLineMatching(lines, ".*\\[TABLE SCAN].*Insert.*"));
        assertFalse(foundLineMatching(lines, ".*\\[TABLE SCAN].*BLAH.insert.*"));
        assertFalse(foundLineMatching(lines, ".*\\[READ].*NondeterministicRWProc.*"));
        assertFalse(foundLineMatching(lines, ".*\\[READ].*DeterministicRWProc.*"));

        assertFalse(foundLineMatching(lines, ".*DeterministicRWProc.*non-deterministic.*"));

        // test SP improvement warnings
        String absPattern = "abs\\s*\\(\\s*1\\s*\\)\\s*\\+\\s*1"; // Pattern for abs(1) + 1, with whitespace between tokens
        String pattern0 = "adding a \\'PARTITION ON TABLE BLAH COLUMN ON IVAL PARAMETER 0\\' .*";
        String pattern1 = "adding a \\'PARTITION ON TABLE BLAH COLUMN ON IVAL PARAMETER 1\\' .*";

        assertEquals(4, countLinesMatching(lines, ".*" + pattern0 + ".*")); // StmtSPcandidates 1,2,3,4
        assertEquals(2, countLinesMatching(lines, ".*\\[StmtSPcandidate.].*12345678.*" + pattern0)); // 2, 3
        assertEquals(1, countLinesMatching(lines, ".*\\[StmtSPcandidate.].*" + absPattern + ".*" + pattern0)); // just 4
        assertEquals(1, countLinesMatching(lines, ".*\\[StmtSPcandidate.].*12345678.*" + pattern1)); // just 5
        assertEquals(2, countLinesMatching(lines, ".*\\[StmtSPcandidate.].*" + pattern1)); // 5, 6

        assertEquals(1, countLinesMatching(lines, ".*\\[ProcSPcandidate.\\.class].*designating parameter 0 .*")); // ProcSPcandidate 1
        assertEquals(4, countLinesMatching(lines, ".*\\[ProcSPcandidate.\\.class].*added parameter .*87654321.*")); // 2, 3, 5, 6
        assertEquals(1, countLinesMatching(lines, ".*\\[ProcSPcandidate.\\.class].*added parameter .*" + absPattern + ".*")); // just 4
        assertEquals(1, countLinesMatching(lines, ".*\\[ProcSPcandidate.\\.class].*designating parameter 1 .*")); // 7

        // Non-candidates disqualify themselves by various means.
        assertEquals(0, countLinesMatching(lines, ".*\\[SPNoncandidate.].*partitioninfo=BLAH\\.IVAL:0.*"));
        assertEquals(0, countLinesMatching(lines, ".*\\[SPNoncandidate.].*partitioninfo=BLAH\\.IVAL:1.*"));

        assertEquals(0, countLinesMatching(lines, ".*\\[ProcSPNoncandidate.\\.class].* parameter .*"));
        assertEquals(0, countLinesMatching(lines, ".*\\[ProcSPNoncandidate.\\.class].* parameter .*"));

        // test prettying-up of statements in feedback output. ("^[^ ].*") is used to confirm that the (symbol-prefixed) log statements contain the original ugliness.
        // While ("^ .*") is used to confirm that the (space-prefixed) feedback to the user is cleaned up.
        assertTrue(foundLineMatching(lines, "^[^ ].*values.*  .*")); // includes 2 embedded or trailing spaces.
        assertFalse(foundLineMatching(lines, "^ .*values.*  .*")); // includes 2 embedded or trailing spaces.
        assertTrue(foundLineMatching(lines, "^[^ ].*nsert.*  .*values.*")); // includes 2 embedded spaces.
        assertFalse(foundLineMatching(lines, "^ .*nsert.*  .*values.*")); // includes 2 embedded spaces.
        assertTrue(foundLineMatching(lines, "^[^ ].*values.*\u0009.*")); // that's an embedded or trailing unicode tab.
        assertFalse(foundLineMatching(lines, "^ .*values.*\u0009.*")); // that's an embedded or trailing unicode tab.
        assertTrue(foundLineMatching(lines, "^[^ ].*nsert.*\u0009.*values.*")); // that's an embedded unicode tab.
        assertFalse(foundLineMatching(lines, "^ .*nsert.*\u0009.*values.*")); // that's an embedded unicode tab.
        assertTrue(foundLineMatching(lines, "^[^ ].*values.*\\s\\s.*")); // includes 2 successive embedded or trailing whitespace of any kind
        assertFalse(foundLineMatching(lines, "^ .*values.*\\s\\s.*")); // includes 2 successive embedded or trailing whitespace of any kind
        assertTrue(foundLineMatching(lines, "^[^ ].*nsert.*\\s\\s.*values.*")); // includes 2 successive embedded whitespace of any kind
        assertFalse(foundLineMatching(lines, "^ .*nsert.*\\s\\s.*values.*")); // includes 2 successive embedded whitespace of any kind
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
