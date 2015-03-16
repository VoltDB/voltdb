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


public class TestVoltCompilerErrorMsgs extends TestCase {

    private void statementTest(String feature, boolean expectError, boolean testStmtIsDdl, String... statements)
        throws Exception
    {
        String simpleSchema =
            "create table blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null" +
            ");" +
            "create table indexed_blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null, " +
            "PRIMARY KEY(ival)" +
            ");" +
            "create table partitioned_blah (" +
            "ival bigint default 0 not null, " +
            "sval varchar(255) not null" +
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

    public void testErrorOnLimitPartitionRows() throws Exception {

        ddlErrorTest("Table T1 has invalid DELETE statement for LIMIT PARTITION ROWS constraint: not a DELETE statement",
                "create table t1 (i integer, "
                + "constraint row_limit limit partition rows 5 "
                + "execute (insert into t1 values(3)));");

        ddlErrorTest("Table T1 has invalid DELETE statement for LIMIT PARTITION ROWS constraint: target of DELETE must be T1",
                "create table t1 (i integer, "
                + "constraint row_limit limit partition rows 5 "
                + "execute (delete from partitioned_blah));");

        ddlErrorTest("Table T1 has invalid DELETE statement for LIMIT PARTITION ROWS constraint: parse error: SQL Syntax error",
                "create table t1 (i integer, "
                + "constraint row_limit limit partition rows 5 "
                + "execute (delete frm partitioned_blah));");

    }
}
