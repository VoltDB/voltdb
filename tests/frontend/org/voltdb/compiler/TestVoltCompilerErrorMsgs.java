/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

    private void statementTest(String statement, String feature, boolean expectError, boolean testStmtIsDdl)
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
            ");";

        VoltProjectBuilder builder = new VoltProjectBuilder();

        ByteArrayOutputStream capturer = new ByteArrayOutputStream();
        PrintStream capturing = new PrintStream(capturer);
        builder.setCompilerDebugPrintStream(capturing);

        builder.addLiteralSchema(simpleSchema);

        if (testStmtIsDdl) {
            builder.addLiteralSchema(statement);
        } else {
            builder.addStmtProcedure(feature, statement);
        }
        boolean success = builder.compile(Configuration.getPathToCatalogForTest("errors.jar"));
        assertEquals(expectError, ! success);
        String captured = capturer.toString("UTF-8");
        String[] lines = captured.split("\n");
        assertEquals(expectError, foundLineMatching(lines, ".*[Ee]rror.*" + feature + ".*"));
    }

    private boolean foundLineMatching(String[] lines, String pattern) {
        for (String string : lines) {
            if (string.matches(pattern)) {
                return true;
            }
        }
        return false;
    }

    void statementErrorTest(String feature, String statement) throws Exception {
        statementTest(statement, feature, true, true);
    }

    void statementNonErrorTest(String feature, String statement) throws Exception {
        statementTest(statement, feature, false, false);
    }

    void ddlErrorTest(String feature, String statement) throws Exception {
        statementTest(statement, feature, true, true);
    }

    void ddlNonErrorTest(String feature, String statement) throws Exception {
        statementTest(statement, feature, false, true);
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
}
