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

package org.voltdb.regressionsuites;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.compiler.VoltProjectBuilder;

/**
 * Actual regression tests for SQL that I found that was broken and
 * have fixed.  Didn't like any of the other potential homes that already
 * existed for this for one reason or another.
 */

public class TestHexLiteralsSuite extends RegressionSuite {

    static private long[] interestingValues = new long[] {
            Long.MIN_VALUE,
            Long.MIN_VALUE + 1,
            Long.MIN_VALUE + 1000,
            -1,
            0,
            1,
            1000,
            Long.MAX_VALUE -1,
            Long.MAX_VALUE
        };

    private static String longToHexLiteral(long val) {
        return "X'" + makeEvenDigits(val) + "'";
    }

    private static String makeEvenDigits(long val) {
        String valAsHex = Long.toHexString(val).toUpperCase();
        if ((valAsHex.length() % 2) == 1) {
            valAsHex = "0" + valAsHex;
        }

        return valAsHex;
    }

    @Test
    public void testHexLiteralsAsParams() throws Exception {
        Client client = getClient();

        for (int i = 0; i < interestingValues.length; ++i) {
            long val = interestingValues[i];
            client.callProcedure("T.Insert", i, longToHexLiteral(val));
            // Verify that the right constant was inserted
            VoltTable vt = client.callProcedure("@AdHoc", "select bi from t where pk = ?", i)
                    .getResults()[0];
            validateRowOfLongs(vt, new long[] {val});
        }

        // 0 digits is not a valid value
        verifyProcFails(client, "Unable to convert string x'' to long value for target parameter",
                "T.Insert", 21, "x''");

        // Too many digits (more than 16) won't fit into a BIGINT.
        verifyProcFails(client, "Unable to convert string x'FFFFffffFFFFffffF' to long value for target parameter",
                "T.Insert", 21, "x'FFFFffffFFFFffffF'");
    }

    @Test
    public void testProcsWithEmbeddedHexLiteralsSelect() throws Exception {
        Client client = getClient();

        // Insert one row, so calls below produce just one row.
        client.callProcedure("T.Insert", 0, 0);

        // For each interesting value,
        // invoke a corresponding procedure that does XOR against that value.
        // Make sure that the literal in the procedure was interpreted correctly.

        for (long val : interestingValues) {
            VoltTable result = client.callProcedure("HEX_LITERAL_PROC_" + makeEvenDigits(val),
                    0xF0F0F0F0F0F0F0F0L)
                    .getResults()[0];
            assertTrue(result.advanceRow());
            String actual = result.getString(0);
            long expectedNum = val ^ 0xF0F0F0F0F0F0F0F0L;
            if (expectedNum != Long.MIN_VALUE && val != Long.MIN_VALUE) {
                String expected = Long.toHexString(expectedNum).toUpperCase();
                assertEquals(expected, actual);
            }
            else {
                // Input or output to bit op was null value
                // So output of HEX will be null.
                assertEquals(null, actual);
            }
        }
    }

    @Test
    public void testProcsWithEmbeddedHexLiteralsWhere() throws Exception {
        Client client = getClient();

        // Insert all the interesting values in the table,
        // and make sure we can select them back with a constant
        // embedded in the where clause

        for (int i = 0; i < interestingValues.length; ++i) {
            client.callProcedure("T.Insert", i, interestingValues[i]);
        }

        for (int i = 0; i < interestingValues.length; ++i) {
            long val = interestingValues[i];
            String procName = "HEX_LITERAL_PROC_WHERE_" + makeEvenDigits(interestingValues[i]);
            VoltTable vt = client.callProcedure(procName).getResults()[0];
            if (val != Long.MIN_VALUE) {
                assertTrue(vt.advanceRow());
                assertEquals(i, vt.getLong(0));
            }
            else {
                assertFalse(vt.advanceRow());
            }
        }
    }

    //
    // JUnit / RegressionSuite boilerplate
    //
    public TestHexLiteralsSuite(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestHexLiteralsSuite.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();

        String literalSchema =
                "CREATE TABLE T (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"
                + "  BI BIGINT\n"
                + ");\n"
                ;

        // Create a bunch of procedures with various embedded literals
        // in the select list
        for (long val : interestingValues) {
            literalSchema += "CREATE PROCEDURE HEX_LITERAL_PROC_" + makeEvenDigits(val) + " AS\n"
                    + "  SELECT HEX(BITXOR(X'" + makeEvenDigits(val) + "', ?)) FROM T;\n";
        }

        // Create a bunch of procedures with various embedded literals
        // in the where clause
        for (long val : interestingValues) {
            literalSchema += "CREATE PROCEDURE HEX_LITERAL_PROC_WHERE_" + makeEvenDigits(val) + " AS\n"
                    + "  SELECT PK FROM T WHERE BI = X'" + makeEvenDigits(val) + "';\n";
        }

        try {
            project.addLiteralSchema(literalSchema);
        }
        catch (Exception e) {
            fail();
        }

        config = new LocalCluster("fixedsql-threesite.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
