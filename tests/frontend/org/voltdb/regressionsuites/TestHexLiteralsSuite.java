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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.VoltProjectBuilder;

public class TestHexLiteralsSuite extends RegressionSuite {

    /*
     * Make sure that x-quoted literals (indicating a sequence of bytes
     * denoted by hexadecimal digits) are not interpreted as integers,
     * but that they *do* work as VARBINARY literals.
     */

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
        return "X'" + makeSixteenDigits(val) + "'";
    }

    private static String makeSixteenDigits(long val) {
        String valAsHex = Long.toHexString(val).toUpperCase();
        if (valAsHex.length() < 16) {
            int numZerosNeeded = 16 - valAsHex.length();
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < numZerosNeeded; ++i) {
                sb.append('0');
            }
            sb.append(valAsHex);
            valAsHex = sb.toString();
        }

        return valAsHex;
    }

    @Test
    public void testHexLiteralsAsVarbinaryParams() throws Exception {
        Client client = getClient();

        for (int i = 0; i < interestingValues.length; ++i) {
            long val = interestingValues[i];
            client.callProcedure("InsertVarbinary", i, longToHexLiteral(val));
            // Verify that the right constant was inserted
            VoltTable vt = client.callProcedure("@AdHoc", "select vb from t where pk = ?", i)
                    .getResults()[0];
            assertTrue(vt.advanceRow());
            assertTrue(Arrays.equals(longToBytes(val), vt.getVarbinary(0)));
        }

        // Mixed case literals are okay.
        ClientResponse cr = client.callProcedure("InsertVarbinary", 20, "X'AaBbCcDdEeFf'");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        // Must be an even number of digits
        verifyProcFails(client, "String is not properly hex-encoded",
                "InsertVarbinary", 21, "x'F'");

        verifyProcFails(client, "String is not properly hex-encoded",
                "InsertVarbinary", 21, "x'XYZZY'");

        // Too many digits for type
        verifyProcFails(client, "The size 9 of the value exceeds the size of the VARBINARY\\(8\\) column",
                "InsertVarbinary", 21, "x'FfffFfffFfffFfffFf'");
    }

    private static byte[] longToBytes(long x) {
        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putLong(x);
        return buffer.array();
    }
    @Test
    public void testProcsWithEmbeddedVarbinaryLiterals() throws Exception {
        Client client = getClient();

        // Insert all the interesting values in the table,
        // and make sure we can select them back with a constant
        // embedded in the where clause

        for (int i = 0; i < interestingValues.length; ++i) {
            client.callProcedure("InsertVarbinary", i, longToBytes(interestingValues[i]));
        }

        VoltTable vvt = client.callProcedure("@AdHoc", "select * from t").getResults()[0];
        System.out.println(vvt);

        for (int i = 0; i < interestingValues.length; ++i) {
            long val = interestingValues[i];
            String digits = makeSixteenDigits(val);
            String procName = "XQUOTE_VARBINARY_PROC_" + digits;
            VoltTable vt = client.callProcedure(procName).getResults()[0];
            validateRowOfLongs(vt, new long[] {i});
        }
    }

    @Test
    public void testIntegerHexLiteralsFail() throws IOException {
        Client client = getClient();

        verifyProcFails(client, "Unable to convert string X'15' to long value for target parameter",
                "InsertBigint", 0, "X'15'");
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
                + "  BI BIGINT,"
                + "  VB VARBINARY(8)\n"
                + ");\n"
                ;

        literalSchema += "CREATE PROCEDURE InsertVarbinary AS "
                + "INSERT INTO T (PK, VB) VALUES (?, ?);";

        literalSchema += "CREATE PROCEDURE InsertBigint AS "
                + "INSERT INTO T (PK, BI) VALUES (?, ?);";

        // Create a bunch of procedures with various embedded literals
        // in the where clause
        for (long val : interestingValues) {
            literalSchema += "CREATE PROCEDURE XQUOTE_VARBINARY_PROC_" + makeSixteenDigits(val) + " AS\n"
                    + "  SELECT PK FROM T WHERE VB = " + longToHexLiteral(val) + ";\n";
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
