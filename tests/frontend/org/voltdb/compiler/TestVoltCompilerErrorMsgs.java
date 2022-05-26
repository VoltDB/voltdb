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


public class TestVoltCompilerErrorMsgs extends TestCase {

    private void statementTest(String feature, boolean expectError, boolean testStmtIsDdl, String... statements)
        throws Exception
    {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null," +
            "tinyval tinyint," +
            "floatval float," +
            "decval decimal" +
            ");" +
            "create table indexed_blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null, " +
            "PRIMARY KEY(ival)" +
            ");" +
            "create table partitioned_blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null," +
            "tinyval tinyint," +
            "floatval float," +
            "decval decimal" +
            ");" +
            "partition table partitioned_blah on column sval;" +
            "";

        VoltProjectBuilder builder = new VoltProjectBuilder();

        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        builder.setCompilerDebugPrintStream(capturing);

        builder.addLiteralSchema(simpleSchema);

        for (String statement : statements) {
            if (testStmtIsDdl) {
                builder.addLiteralSchema(statement);
            } else {
                builder.addStmtProcedure(feature, statement);
            }
        }

        boolean success = builder.compile(Configuration.getPathToCatalogForTest("errors.jar"));
        String captured = capturer.toString("UTF-8");
        String[] lines = captured.split("\n");

        if (expectError) {
            assertFalse("Expected an error containing \"" + feature + "\", but compilation succeeded.", success);
            String pattern = ".*[Ee]rror.*" + feature + ".*";
            assertTrue("Expected an error matching pattern \"" + pattern + "\" in output \"" + captured + "\", but no matching line was found",
                    foundLineMatching(lines, pattern));
        }
        else {
            assertTrue("Expected no errors, but compilation failed with this output: \"" + captured + "\".", success);
        }
    }

    private boolean foundLineMatching(String[] lines, String pattern) {
        for (String string : lines) {
            if (string.matches(pattern)) {
                return true;
            }
        }
        return false;
    }

    void statementErrorTest(String feature, String... statements) throws Exception {
        statementTest(feature, true, true, statements);
    }

    void statementNonErrorTest(String feature, String... statements) throws Exception {
        statementTest(feature, false, false, statements);
    }

    void ddlErrorTest(String feature, String... statements) throws Exception {
        statementTest(feature, true, true, statements);
    }

    void ddlNonErrorTest(String feature, String... statements) throws Exception {
        statementTest(feature, false, true, statements);
    }

    public void testNoErrorOnParens() throws Exception {
        statementNonErrorTest("PARENS", "(select ival from blah);");
    }

    public void testErrorOnVarchar0() throws Exception {
        ddlErrorTest("out of range", "create table hassize0vc (vc varchar(0));");
    }

    public void testErrorOnVarbinary0() throws Exception {
        ddlErrorTest("out of range", "create table hassize0vb (vb varbinary(0));");
    }

    public void testErrorOnSizedInteger() throws Exception {
        // We do not support sized integers.
        ddlErrorTest("unexpected token", "create table hassizedint (i integer(5));");
    }

    public void testLargeVarcharColumns() throws Exception {
        // This is a regression test for ENG-10560.

        // Tests for sizes of complete row
        ddlErrorTest("Table T has a maximum row size of 2097156 but the maximum supported row size is 2097152",
                "create table t (vc1 varchar(262144), vc2 varchar(262143));");

        ddlErrorTest("Table T has a maximum row size of 2097153 but the maximum supported row size is 2097152",
                "create table t (vc1 varchar(1048572 bytes), vc2 varchar(1048573 bytes));");

        ddlNonErrorTest(null, "create table t (vc1 varchar(262143), vc2 varchar(262143));");
        ddlNonErrorTest(null, "create table t (vc1 varchar(1048572 bytes), vc2 varchar(1048572 bytes));");
    }

    public void testErrorOnInsertIntoSelect() throws Exception {

        // This should fail.
        ddlErrorTest("statement manipulates data in a content non-deterministic way",
                "create procedure MyInsert as insert into partitioned_blah (sval, ival) select sval, ival from blah where sval = ? limit 1;",
                "partition procedure MyInsert on table partitioned_blah column sval;");

        // limit with no order by implies non-deterministic content, so we won't compile the statement.
        ddlErrorTest("statement manipulates data in a content non-deterministic way",
                "create procedure MyInsert as insert into partitioned_blah (sval, ival) " +
                "select sval, ival from blah where sval = ? limit 1;",
                "partition procedure MyInsert on table partitioned_blah column sval;");

        // Limit with an order by is okay.
        ddlNonErrorTest("INSERT",
                "create procedure MyInsert as insert into partitioned_blah (sval, ival) " +
                "select sval, ival from blah where sval = ? order by 1, 2 limit 1;",
                "partition procedure MyInsert on table partitioned_blah column sval;");

        // if it's marked as single-partition, it's ok.
        ddlNonErrorTest("INSERT",
                "create procedure MyInsert as insert into partitioned_blah (sval) select sval from indexed_blah where sval = ?;",
                "partition procedure MyInsert on table partitioned_blah column sval;");

        // insert into replicated is fine for MP stored procedure
        ddlNonErrorTest("INSERT",
                "create procedure MyInsert as insert into blah (sval) select sval from indexed_blah where sval = ?;");

        // ...but should fail for SP stored procedure
        ddlErrorTest("Trying to write to replicated table 'BLAH' in a single-partition procedure.",
                "create procedure MyInsert as insert into blah (sval) select sval from indexed_blah where sval = ?;",
                "partition procedure MyInsert on table partitioned_blah column sval;");

        // ...and insert into replicated table should fail if the select accesses any partitioned tables
        ddlErrorTest("Subquery in INSERT INTO ... SELECT statement may not access partitioned data " +
                "for insertion into replicated table BLAH.",
                "create procedure MyInsert as insert into blah (sval) select sval from partitioned_blah where sval = ?;");

        // UNION is still unsupported
        ddlErrorTest("not supported for UNION or other set operations",
                "create procedure MyInsert as insert into blah (sval) " +
                    "select sval from indexed_blah where sval = ? union select sval from indexed_blah where sval = ?;");

        // query expression/target column degree mismatch
        ddlErrorTest("number of target columns does not match that of query expression",
                "create procedure MyInsert as insert into partitioned_blah (sval) select sval, sval || '!' from indexed_blah where sval = ?;",
                "partition procedure MyInsert on table partitioned_blah column sval;");

        // parameter in select list needs a cast.
        ddlErrorTest("data type cast needed for parameter or null literal",
                "create procedure insert_param_in_select_list as " +
                    "insert into partitioned_blah (ival, sval) " +
                        "select ival, ? from blah order by ival, sval;",
                "partition procedure insert_param_in_select_list on table partitioned_blah column sval;");

        // inserting into replicated table should fail
        ddlErrorTest("Trying to write to replicated table 'BLAH' in a single-partition procedure.",
                "create procedure insert_into_replicated_select as " +
                "insert into blah select * from partitioned_blah;" +
                "partition procedure insert_into_replicated_select on table partitioned_blah column sval;");
    }

    public void testHexLiterals() throws Exception {

        // 0 digits is not valid to specify a number.
        ddlErrorTest("invalid format for a constant bigint value",
                "create procedure insHex as insert into blah (ival) values (x'');");

        // The HSQL parser complains about an odd number of digits.
        ddlErrorTest("malformed binary string",
                "create procedure insHex as insert into blah (ival) values (x'0123456789abcdef0');");

        // Too many digits for a BIGINT.
        ddlErrorTest("invalid format for a constant bigint value",
                "create procedure insHex as insert into blah (ival) values (x'0123456789abcdef01');");

        // Check that we get range checks right when inserting into a field narrower
        // than BIGINT.

        ddlErrorTest("Constant value overflows/underflows TINYINT type.",
                "create procedure insHex as insert into blah (tinyval) values (x'FF');");

        ddlErrorTest("Constant value overflows/underflows TINYINT type.",
                "create procedure insHex as insert into blah (tinyval) values (x'80');");

        // This is -128, the null value.
        ddlErrorTest("Constant value overflows/underflows TINYINT type.",
                "create procedure insHex as insert into blah (tinyval) values (x'FfffFfffFfffFf80');");

        // This is -129.
        ddlErrorTest("Constant value overflows/underflows TINYINT type.",
                "create procedure insHex as insert into blah (tinyval) values (x'FfffFfffFfffFf7f');");

        // Hex constants not allowed to initialize DECIMAL or FLOAT.

        ddlErrorTest("invalid format for a constant float value",
                "create procedure insHex as insert into blah (floatval) values (x'80');");

        ddlErrorTest("invalid format for a constant decimal value",
                "create procedure insHex as insert into blah (decval) values (x'80');");

        // In arithmetic or logical expressions, we catch malformed (for BIGINT) x-literals
        // in HSQL.

        ddlErrorTest("malformed numeric constant",
                "create procedure selHex as select 30 + X'' from blah;");

        // (too many hex digits)
        ddlErrorTest("malformed numeric constant",
                "create procedure selHex as select tinyval from blah where X'0000000000000000FF' < tinyval;");
    }

    public void testHexLiteralDefaultValues() throws Exception {
        ddlErrorTest("malformed numeric constant",
                "create table t (bi bigint default X'');");

        ddlErrorTest("malformed numeric constant",
                "create table t (bi bigint default X'FFFF0000FFFF0000FF');");

        ddlErrorTest("numeric value out of range",
                "create table t (ti tinyint default X'80');");

        ddlErrorTest("numeric value out of range",
                "create table t (ti tinyint default X'FF');");

        // This does not fail, but it seems like it should fail with an
        // out of range error.  -128 is reserved for the TINYINT null value
        // This is ENG-8148.
        ddlNonErrorTest("create table t (ti tinyint default -X'80');");

        ddlErrorTest("numeric value out of range",
                "create table t (ti tinyint default -X'81');");
    }
}
