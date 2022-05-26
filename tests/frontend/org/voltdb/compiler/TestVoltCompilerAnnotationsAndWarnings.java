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
import org.voltdb.compiler.procedures.FloatParamToGetNiceComplaint;
import org.voltdb.compiler.procedures.InsertAggregatesOfFloat;
import org.voltdb.compiler.procedures.InsertAggregatesOfFloatInHaving;
import org.voltdb.compiler.procedures.InsertAggregatesOfFloatWithSetops;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicRONonSeqProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicROSeqProc;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicRWProc1;
import org.voltdb_testprocs.regressionsuites.failureprocs.DeterministicRWProc2;
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
     * Test whether a DDL stored procedure does not compile properly. The test
     * testSimple cannot test compilation failure. It's sometimes useful to have
     * a single test to test compilation success.
     *
     * @param simpleSchema
     *            A string containing the test schema.
     * @param procObject
     *            This tells what procedure to test. If this is an object of
     *            type Class<?> we we add its procedures to the builder. If it's
     *            an array of two strings we add a statement procedure whose
     *            name is the first string and whose DDL definition is the
     *            second string. Note that the "create procedure as" part is
     *            added for you, so all you need is the SQL text for the
     *            procedure.
     * @param errorMessages
     *            The error messages we expect to see, as Java regular
     *            expressions. This may be true of no errors are expected.
     * @param expectSuccess
     *            If this is true, the compilation should succeed.
     * @throws Exception
     */
    public void testCompilationFailure(String     testName,
                                       String     simpleSchema,
                                       Object     procObject,
                                       String[]   errorMessages,
                                       boolean    expectSuccess) throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        builder.setCompilerDebugPrintStream(capturing);
        builder.addLiteralSchema(simpleSchema);
        if (procObject instanceof String[]) {
            String[] stmtProcDescrip = (String[]) procObject;
            assertTrue(stmtProcDescrip.length == 2);
            builder.addStmtProcedure(stmtProcDescrip[0], stmtProcDescrip[1]);
        } else if (procObject instanceof Class<?>) {
            Class<?> procKlazz = (Class<?>) procObject;
            builder.addProcedure(procKlazz);
        } else {
            assertTrue("Bad type of object for parameter \"procObject\"", false);
        }

        boolean success = builder.compile(Configuration.getPathToCatalogForTest("annotations.jar"));
        assertEquals(String.format("Expected compilation %s",
                                   (expectSuccess ? "success" : "failure")),
                     expectSuccess, success);
        if (errorMessages != null) {
            String captured = capturer.toString("UTF-8");
            String[] lines = captured.split("\n");
            System.out.printf("\n" + ":----------------------------------------------------------------------:\n" + ":  %s: Start of captured output\n" + ":----------------------------------------------------------------------:\n",
                              testName);
            System.out.println(captured);
            System.out.printf("\n" + ":----------------------------------------------------------------------:\n" + ":  %s: End of captured output\n" + ":----------------------------------------------------------------------:\n",
                              testName);
            // Output should include a line suggesting replacement of float with
            // double.
            for (String oneMessagePattern : errorMessages) {
                assertTrue(foundLineMatching(lines, oneMessagePattern));
            }
        }
    }

    /**
     * Test that a stored procedure with a parameter type of float will not
     * compile. The Java type float is not legal for stored procedures.
     *
     * @throws Exception
     */
    public void testFloatParamComplaint() throws Exception {
        String simpleSchema =
            "create table floatie (" +
            "ival bigint default 0 not null, " +
            "fval float not null," +
            "PRIMARY KEY(ival)" +
            ");" +
            "partition table floatie on column ival;";
        testCompilationFailure("testFloatParamComplaint",
                               simpleSchema,
                               FloatParamToGetNiceComplaint.class,
                               new String[] { ".*FloatParamToGetNiceComplaint.* float.* double.*" },
                               false);
    }

    /**
     * Test that a stored procedure with an aggregate whose parameter is FLOAT
     * causes a compilation error.
     *
     * @throws Exception
     */
    public void testInsertAggregatesOfFloat() throws Exception {
        String simpleSchema =
                "create table floatingaggs_input ( alpha float );" +
                "create table floatingaggs_output ( beta float );" +
                "";
        testCompilationFailure("testInsertAggregatesOfFloat",
                               simpleSchema,
                               InsertAggregatesOfFloat.class,
                               new String[] { ".*InsertAggregatesOfFloat.*Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.*" },
                               false);
    }

    /**
     * Test that a stored procedure with an aggregate of floating point type in
     * a having clause causes a compilation error.
     *
     * @throws Exception
     */
    public void testInsertAggregatesOfFloatInHaving() throws Exception {
        String simpleSchema =
                "create table floatingaggs_input ( alpha float );" +
                "create table floatingaggs_output ( beta float );" +
                "create table intaggs ( gamma integer );" +
                "";
        testCompilationFailure("testInsertAggregatesOfFloatInHaving",
                               simpleSchema,
                               InsertAggregatesOfFloatInHaving.class,
                               new String[] { ".*InsertAggregatesOfFloatInHaving.*Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.*" },
                               false);
    }

    /**
     * Test that a statement procedure with an aggregate of floating point type
     * causes a compilation error.
     *
     * @throws Exception
     */
    public void testAggregatesOfFloatDDL() throws Exception {
        String simpleSchema = "create table floatingaggs_input ( alpha float );" + "create table floatingaggs_output ( beta float );" + "";
        testCompilationFailure("testAggregatesOfFloatDDL",
                               simpleSchema,
                               new String[] { "InsertAggregatesOfFloatDDL", "insert into floatingaggs_output select sum(alpha) from floatingaggs_input;",
                               },
                               new String[] { ".*InsertAggregatesOfFloatDDL.*Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.*" },
                               false);
    }

    /**
     * Test that a statement procedure with an aggregate of floating point type
     * in a subquery in a from clause causes an error.
     *
     * @throws Exception
     */
    public void testAggregatesOfFloatInSubquery() throws Exception {
        String simpleSchema = "create table floatingaggs_input ( alpha float );" + "create table floatingaggs_output ( beta float );" + "";
        testCompilationFailure("testAggregatesOfFloatInSubquery",
                               simpleSchema,
                               new String[] { "AggregatesOfFloatInSubquery", "insert into floatingaggs_output select sq.ss from ( select sum(alpha) as ss from floatingaggs_input where alpha > 0.0 order by ss ) as sq;" },
                               new String[] { ".*AggregatesOfFloatInSubquery.*Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.*" },
                               false);
    }

    /**
     * Test that a statement procedure with an expression which has a
     * subexpression which is an aggregate of floating point type in a subquery
     * in a from clause causes an error.
     *
     * @throws Exception
     */
    public void testAggregatesOfFloatInComplexSubquery() throws Exception {
        String simpleSchema = "create table floatingaggs_input ( alpha float );" + "create table floatingaggs_output ( beta float );" + "";
        testCompilationFailure("testAggregatesOfFloatInComplexSubquery",
                               simpleSchema,
                               new String[] { "AggregatesOfFloatInComplexSubquery", "insert into floatingaggs_output select sq.ss + 100 from ( select sum(alpha) as ss from floatingaggs_input where alpha > 0.0 order by ss ) as sq;" },
                               new String[] { ".*AggregatesOfFloatInComplexSubquery.*Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.*" },
                               false);
    }

    /**
     * Test that a statement procedure with an expression which has a
     * subexpression which is an aggregate whose type is float and whose
     * expression is more than a column reference and which is also part of a
     * larger expression causes a compilation error.
     *
     * @throws Exception
     */
    public void testAggregatesOfFloatInComplexSubquery2() throws Exception {
        String simpleSchema = "create table floatingaggs_input ( alpha float );" + "create table floatingaggs_output ( beta float );" + "";
        testCompilationFailure("testAggregatesOfFloatInComplexSubquery2",
                               simpleSchema,
                               new String[] { "AggregatesOfFloatInComplexSubquery2", "insert into floatingaggs_output select sq.ss + 100 from ( select sum(alpha + 42) as ss from floatingaggs_input where alpha > 0.0 order by ss ) as sq;" },
                               new String[] { ".*AggregatesOfFloatInComplexSubquery2.*Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.*" },
                               false);
    }

    /**
     * Test that a statement procedure with an expression which has a
     * subexpression which is an aggregate whose type is float and is in a
     * subquery causes a compilation error.
     *
     * @throws Exception
     */
    public void testAggregatesOfFloatInLeftOfJoin() throws Exception {
        String simpleSchema = "create table alpha ( af float );" + "create table beta ( bf float );" + "";
        testCompilationFailure("testAggregatesOfFloatInLeftOfJoin",
                               simpleSchema,
                               new String[] { "AggregatesOfFloatInLeftOfJoin",
                                              "insert into alpha select lf.ss+rf.ss from (select sum(af) as ss from alpha ) as lf inner join ( select bf as ss from beta ) as rf on true;" },
                               new String[] { ".*AggregatesOfFloatInLeftOfJoin.*Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.*" },
                               false);
    }

    /**
     * Test that a statement procedure with an expression which has a
     * subexpression which is an aggregate whose type is float and is in a
     * subquery causes a compilation error.
     *
     * @throws Exception
     */
    public void testAggregatesOfFloatInRightOfJoins() throws Exception {
        String simpleSchema = "create table alpha ( af float );" + "create table beta ( bf float );" + "";
        testCompilationFailure("testAggregatesOfFloatInRightOfJoin",
                               simpleSchema,
                               new String[] { "AggregatesOfFloatInRightOfJoin",
                                              "insert into alpha select lf.ss+rf.ss from (select af as ss from alpha ) as lf inner join ( select sum(bf) as ss from beta ) as rf on true;" },
                               new String[] { ".*AggregatesOfFloatInRightOfJoin.*Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.*" },
                               false);
    }

    /**
     * Test that a statement procedure with an aggregate expression of floating
     * point type in a subquery which is found in a union expression causes a
     * compilation error. This can't be a single statement procedure. It has to
     * be a Java Stored Procedure. This will only be a warning, so the
     * compilation will pass.
     */
    public void testAggregatesOfFloatInSetops() throws Exception {
        String simpleSchema = "create table floatingaggs_input ( alpha float );" + "create table floatingaggs_output ( beta float );" + "";
        testCompilationFailure("testAggregatesOfFloatInSetops",
                               simpleSchema,
                               InsertAggregatesOfFloatWithSetops.class,
                               new String[] { ".*InsertAggregatesOfFloatWithSetops.*Aggregate functions of floating point columns may not be deterministic.  We suggest converting to DECIMAL.*" },
                               true);
    }

    /**
     * Test that Min does not trigger non-determinism errors.
     *
     * @throws Exception
     */
    public void testMinOfFloatIsOk() throws Exception {
        String simpleSchema = "create table alpha ( af float );" + "create table beta ( bf float );" + "";
        testCompilationFailure("testMinOfFloat",
                               simpleSchema,
                               new String[] { "MinOfFloat", "insert into alpha select lf.ss+rf.ss from (select af as ss from alpha ) as lf inner join ( select min(bf) as ss from beta ) as rf on true;" },
                               null,
                               true);
    }

    /**
     * Test that Max does not trigger non-determinism errors.
     *
     * @throws Exception
     */
    public void testMaxOfFloatIsOk() throws Exception {
        String simpleSchema = "create table alpha ( af float );" + "create table beta ( bf float );" + "";
        testCompilationFailure("testMaxOfFloat",
                               simpleSchema,
                               new String[] { "MaxOfFloat", "insert into alpha select lf.ss+rf.ss from (select af as ss from alpha ) as lf inner join ( select max(bf) as ss from beta ) as rf on true;" },
                               null,
                               true);
    }

    /**
     * Test that we haven't broken the obvious good case.
     *
     * @throws Exception
     */
    public void testGoodInsert() throws Exception {
        String simpleSchema = "create table floatingaggs_input ( alpha float );" + "create table floatingaggs_output ( beta float );" + "";
        testCompilationFailure("testAggregatesOfFloatInComplexSubquery2",
                               simpleSchema,
                               new String[] { "AggregatesOfFloatInComplexSubquery2", "insert into floatingaggs_output select alpha from floatingaggs_input;" },
                               null,
                               true);
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
                "insert                            into\t \tblah values\n\n(? \t ,\t\t\t?)                           ;");
        builder.addProcedure(NondeterministicROProc.class);
        builder.addProcedure(NondeterministicRWProc.class);
        builder.addProcedure(DeterministicRONonSeqProc.class);
        builder.addProcedure(DeterministicROSeqProc.class);
        builder.addProcedure(DeterministicRWProc1.class);
        builder.addProcedure(DeterministicRWProc2.class);

        builder.addProcedure(ProcSPcandidate1.class);
        builder.addProcedure(ProcSPcandidate2.class);
        builder.addProcedure(ProcSPcandidate3.class);
        builder.addProcedure(ProcSPcandidate4.class);
        builder.addProcedure(ProcSPcandidate5.class);
        builder.addProcedure(ProcSPcandidate6.class);
        builder.addProcedure(ProcSPcandidate7.class);

        builder.addProcedure(ProcSPNoncandidate1.class);
        builder.addProcedure(ProcSPNoncandidate2.class);
        builder.addProcedure(ProcSPNoncandidate3.class);
        builder.addProcedure(ProcSPNoncandidate4.class);
        builder.addProcedure(ProcSPNoncandidate5.class);
        builder.addProcedure(ProcSPNoncandidate6.class);

        builder.addStmtProcedure("StmtSPcandidate1", "select count(*) from blah where ival = ?");
        builder.addStmtProcedure("StmtSPcandidate2", "select count(*) from blah where ival = 12345678");
        builder.addStmtProcedure("StmtSPcandidate3",
                                 "select count(*) from blah, indexed_replicated_blah " +
                                 "where indexed_replicated_blah.sval = blah.sval and blah.ival = 12345678");
        builder.addStmtProcedure("StmtSPcandidate4",
                                 "select count(*) from blah, indexed_replicated_blah " +
                                 "where indexed_replicated_blah.sval = blah.sval and blah.ival = abs(1)+1");
        builder.addStmtProcedure("StmtSPcandidate5", "select count(*) from blah where sval = ? and ival = 12345678");
        builder.addStmtProcedure("StmtSPcandidate6", "select count(*) from blah where sval = ? and ival = ?");
        builder.addStmtProcedure("StmtSPNoncandidate1", "select count(*) from blah where sval = ?");
        builder.addStmtProcedure("StmtSPNoncandidate2", "select count(*) from blah where sval = '12345678'");
        builder.addStmtProcedure("StmtSPNoncandidate3", "select count(*) from indexed_replicated_blah where ival = ?");
        builder.addStmtProcedure("FullIndexScan", "select ival, sval from indexed_replicated_blah");
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("annotations.jar"));
        assert(success);
        String captured = capturer.toString("UTF-8");
        System.out.print("\n"
                         + ":----------------------------------------------------------------------:\n"
                         + ":  Start of captured output\n"
                         + ":----------------------------------------------------------------------:\n");
        System.out.println(captured);
        System.out.print("\n"
                        + ":----------------------------------------------------------------------:\n"
                        + ":  End of captured output\n"
                        + ":----------------------------------------------------------------------:\n");
        String[] lines = captured.split("\n");

        assertTrue(foundLineMatching(lines, ".*\\[READ].*NondeterministicROProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[READ].*NondeterministicROProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[READ].*DeterministicRONonSeqProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[READ].*DeterministicROSeqProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[WRITE].*Insert.*"));
        assertTrue(foundLineMatching(lines, ".*\\[WRITE].*NondeterministicRWProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[WRITE].*DeterministicRWProc.*"));
        assertTrue(foundLineMatching(lines, ".*\\[TABLE SCAN].*select ival, sval from indexed_replicated_blah.*"));
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
        String pattern0 = "adding a \\'PARTITION ON TABLE BLAH COLUMN IVAL PARAMETER 0\\' .*";
        String pattern1 = "adding a \\'PARTITION ON TABLE BLAH COLUMN IVAL PARAMETER 1\\' .*";

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
