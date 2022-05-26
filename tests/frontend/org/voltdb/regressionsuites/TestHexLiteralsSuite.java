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

package org.voltdb.regressionsuites;

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
     * Test hex literals in VARBINARY contexts.
     *
     * Test hex literals as integers:
     *  - In arithmetic (integer type should be inferred)
     *  - In relational operators
     *  - In the bitwise functions
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

    // Some values that we can do arithmetic on,
    // without overflowing to different types.
    // 3 billion is roughly the square root of 2^63 - 1.
    static private long[] boringValues = new long[] {
        -3037000400L,
        1500000000,
        1024,
        -1,
        0,
        1,
        16,
        2600000075L,
        3037000400L
    };

    private static String longToHexLiteral(long val) {
        return "X'" + makeEvenDigits(val) + "'";
    }

    private static String longToEightByteHexLiteral(long val) {
        return "X'" + makeSixteenDigits(val) + "'";
    }

    private static String makeEvenDigits(long val) {
        String valAsHex = Long.toHexString(val).toUpperCase();
        if ((valAsHex.length() % 2) == 1) {
            valAsHex = "0" + valAsHex;
        }

        return valAsHex;
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
    public void testVarbinaryHexLiteralsAsParams() throws Exception {
        Client client = getClient();

        for (int i = 0; i < interestingValues.length; ++i) {
            long val = interestingValues[i];
            client.callProcedure("InsertVarbinary", i, longToEightByteHexLiteral(val));
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
    public void testVarbinaryProcsWithEmbeddedLiterals() throws Exception {
        Client client = getClient();

        // Insert all the interesting values in the table,
        // and make sure we can select them back with a constant
        // embedded in the where clause

        for (int i = 0; i < interestingValues.length; ++i) {
            client.callProcedure("InsertVarbinary", i, longToBytes(interestingValues[i]));
        }

        for (int i = 0; i < interestingValues.length; ++i) {
            long val = interestingValues[i];
            String digits = makeSixteenDigits(val);
            String procName = "XQUOTE_VARBINARY_PROC_" + digits;
            VoltTable vt = client.callProcedure(procName).getResults()[0];
            validateRowOfLongs(vt, new long[] {i});
        }
    }

    @Test
    public void testIntegerHexLiteralsAsParams() throws Exception {
        Client client = getClient();

        for (int i = 0; i < interestingValues.length; ++i) {
            long val = interestingValues[i];
            client.callProcedure("InsertBigint", i, longToHexLiteral(val));
            // Verify that the right constant was inserted
            VoltTable vt = client.callProcedure("@AdHoc", "select bi from t where pk = ?", i)
                    .getResults()[0];
            validateRowOfLongs(vt, new long[] {val});
        }

        // 0 digits is not a valid value
        verifyProcFails(client, "Unable to convert string x'' to long value for target parameter",
                "InsertBigint", 21, "x''");

        // Too many digits (more than 16) won't fit into a BIGINT.
        verifyProcFails(client, "Unable to convert string x'FFFFffffFFFFffffF' to long value for target parameter",
                "InsertBigint", 21, "x'FFFFffffFFFFffffF'");
    }

    @Test
    public void testIntegerProcsWithEmbeddedHexLiteralsSelect() throws Exception {
        Client client = getClient();

        // Insert one row, so calls below produce just one row.
        client.callProcedure("InsertBigint", 0, 0);

        // For each interesting value,
        // invoke a corresponding procedure that does XOR against that value.
        // Make sure that the literal in the procedure was interpreted correctly.

        for (long val : interestingValues) {
            VoltTable result = client.callProcedure("INT_HEX_LITERAL_PROC_" + makeEvenDigits(val),
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
    public void testIntegerProcsWithEmbeddedLiteralsWhere() throws Exception {
        Client client = getClient();

        // Insert all the interesting values in the table,
        // and make sure we can select them back with a constant
        // embedded in the where clause

        for (int i = 0; i < interestingValues.length; ++i) {
            client.callProcedure("InsertBigint", i, interestingValues[i]);
        }

        for (int i = 0; i < interestingValues.length; ++i) {
            long val = interestingValues[i];
            String procName = "INT_HEX_LITERAL_PROC_WHERE_" + makeEvenDigits(interestingValues[i]);
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

    @Test
    public void testIntegerHexLiteralsInArithmetic() throws Exception {
        Client client = getClient();

        // Insert one row, so calls below produce just one row.
        client.callProcedure("InsertBigint", 0, 0);

        for (long procVal : boringValues) {
            String procName = "HEX_LITERAL_PROC_ARITH_" + makeEvenDigits(procVal);
            for (long paramVal : boringValues) {
                VoltTable actual = client.callProcedure(procName,
                        paramVal,
                        paramVal,
                        paramVal,
                        paramVal == 0 ? 1 : paramVal) // avoid divide by zero
                        .getResults()[0];
                long[] expected = {
                        procVal + paramVal,
                        procVal - paramVal,
                        procVal * paramVal,
                        procVal / (paramVal == 0 ? 1 : paramVal),
                        -procVal};
                validateRowOfLongs(actual, expected);
            }
        }
    }

    // Users may want to initialize or update narrower integer fields (TINYINT, SMALLINT, BIGINT)
    // with hexadecimal literals.  The tests below just use TINYINT as a representative
    // of the non-BIGINT type class.

   @Test
   public void testIntegerHexLiteralInsertTinyintParams() throws Exception {
       Client client = getClient();
       int pk = 0;

       client.callProcedure("InsertTinyint", pk, "x'00'");
       validateTableOfScalarLongs(client, "select ti from t where pk = " + pk, new long[] {0x00});
       ++pk;

       client.callProcedure("InsertTinyint", pk, "x'7F'");
       validateTableOfScalarLongs(client, "select ti from t where pk = " + pk, new long[] {0x7F});
       ++pk;

       // This is -127, the smallest allowed value.
       client.callProcedure("InsertTinyint", pk, "x'FfffFfffFfffFf81'");
       validateTableOfScalarLongs(client, "select ti from t where pk = " + pk, new long[] {0xFfffFfffFfffFf81L});
       ++pk;

       verifyProcFails(client, "Type BIGINT with value 255 can't be cast as TINYINT "
               + "because the value is out of range",
               "InsertTinyint", pk, "x'FF'");

       verifyProcFails(client, "Type BIGINT with value 128 can't be cast as TINYINT "
               + "because the value is out of range",
               "InsertTinyint", pk, "x'80'");

       // -128 fits into a signed byte but is our null value and therefore out of range
       verifyProcFails(client, "Type BIGINT with value -128 can't be cast as TINYINT "
               + "because the value is out of range",
               "InsertTinyint", pk, "x'FfffFfffFfffFf80'");
   }

   @Test
   public void testIntegerHexLiteralInsertTinyintConstants() throws Exception {
       Client client = getClient();
       int pk = 0;

       client.callProcedure("InsertTinyintConstantMin", pk);
       validateTableOfScalarLongs(client, "select ti from t where pk = " + pk, new long[] {-127});
       ++pk;

       client.callProcedure("InsertTinyintConstantMax", pk);
       validateTableOfScalarLongs(client, "select ti from t where pk = " + pk, new long[] {127});
       ++pk;
   }

   @Test
   public void testIntegerHexLiteralUpdateTinyintParams() throws Exception {
       Client client = getClient();
       int pk = 37;

       client.callProcedure("InsertTinyint", pk, "x'00'");
       client.callProcedure("UpdateTinyint", "X'7F'", pk);
       validateTableOfScalarLongs(client, "select ti from t where pk = " + pk, new long[] {0x7F});

       client.callProcedure("UpdateTinyint", "X'FfffFfffFfffFf81'", pk);
       validateTableOfScalarLongs(client, "select ti from t where pk = " + pk, new long[] {-127});


       verifyProcFails(client, "Type BIGINT with value 255 can't be cast as TINYINT "
               + "because the value is out of range",
               "UpdateTinyint", "x'FF'", pk);


       verifyProcFails(client, "Type BIGINT with value 128 can't be cast as TINYINT "
               + "because the value is out of range",
               "UpdateTinyint", "x'80'", pk);

       // -128 fits into a signed byte but is our null value and therefore out of range
       verifyProcFails(client, "Type BIGINT with value -128 can't be cast as TINYINT "
               + "because the value is out of range",
               "UpdateTinyint", "x'FFFFFFFFFFFFFF80'", pk);
   }

   @Test
   public void testIntegerHexLiteralUpdateTinyintConstants() throws Exception {
       Client client = getClient();
       int pk = 37;

       client.callProcedure("InsertTinyint", pk, "x'3F'");
       client.callProcedure("UpdateTinyintConstantMin", pk);
       validateTableOfScalarLongs(client, "select ti from t where pk = " + pk, new long[] {-127});

       client.callProcedure("UpdateTinyintConstantMax", pk);
       validateTableOfScalarLongs(client, "select ti from t where pk = " + pk, new long[] {127});
   }

   @Test
   public void testIntegerHexLiteralMixedMath() throws Exception {
       Client client = getClient();

       // Insert one row, so calls below produce just one row.
       client.callProcedure("InsertBigint", 0, 0);

       for (long val : boringValues) {
           System.out.println("   ***   " + val);
           VoltTable vt = client.callProcedure("MixedTypeMath", val, val, val, val)
                   .getResults()[0];
           assertTrue(vt.advanceRow());
           assertEquals(val + 33, vt.getLong(0));
           assertEquals(val + 33.0, vt.getDouble(1));
           assertEquals(val + 33, vt.getLong(2));
           assertEquals(10000000000000000033.0 + val, vt.getDouble(3));

           String hexVal = longToHexLiteral(val);

           vt = client.callProcedure("MixedTypeMath", hexVal, val, hexVal, val)
                   .getResults()[0];
           assertTrue(vt.advanceRow());
           assertEquals(val + 33, vt.getLong(0));
           assertEquals(val + 33.0, vt.getDouble(1));
           assertEquals(val + 33, vt.getLong(2));
           assertEquals(10000000000000000033.0 + val, vt.getDouble(3));
       }

       // When parameters are typed as double, you can't pass an x-literal to them.
       verifyProcFails(client, "Unable to convert string X'21' to double value for target parameter",
               "MixedTypeMath", "X'21'", "X'21'", "X'21'", "X'21'");
   }

   @Test
   public void testIntegerHexLiteralDefaultValues() throws Exception {
       Client client = getClient();

       // Insert one row, so calls below produce just one row.
       client.callProcedure("@AdHoc", "insert into t_defaults (pk) values (0);");

       validateTableOfLongs(client, "select * from t_defaults;",
               new long[][] {{
                   0,
                   0, 127, -127, -127, 127, 109, -109,
                   0, Long.MAX_VALUE, Long.MIN_VALUE + 1, Long.MIN_VALUE + 1, Long.MAX_VALUE, 1000001, -1000001
               }});
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
                + "  BI BIGINT,\n"
                + "  TI TINYINT,\n"
                + "  VB VARBINARY(8)\n"
                + ");\n"
                ;

        literalSchema +=
                "CREATE TABLE T_DEFAULTS (\n"
                + "  PK INTEGER NOT NULL PRIMARY KEY,\n"

                + "  TI1 TINYINT DEFAULT X'00',\n"
                + "  TI2 TINYINT DEFAULT X'7F',\n" // max for type
                + "  TI3 TINYINT DEFAULT X'FfffFfffFfffFf81',\n" // min for type
                + "  TI4 TINYINT DEFAULT -X'7F',\n" // min for type, using unary minus
                + "  TI5 TINYINT DEFAULT -X'FfffFfffFfffFf81',\n" // max for type, using unary minus
                + "  TI6 TINYINT DEFAULT X'6D',\n" // decimal 109
                + "  TI7 TINYINT DEFAULT -X'6D',\n" // decimal -109

                + "  BI1 BIGINT DEFAULT X'00',\n"
                + "  BI2 BIGINT DEFAULT X'7FFFFFFFFFFFFFFF',\n" // max for type
                + "  BI3 BIGINT DEFAULT X'8000000000000001',\n" // min for type
                + "  BI4 BIGINT DEFAULT -X'7FFFFFFFFFFFFFFF',\n" // min for type, with unary minus
                + "  BI5 BIGINT DEFAULT -X'8000000000000001',\n" // min for type with unary minus
                + "  BI6 BIGINT DEFAULT  X'0F4241',\n" // decimal 1,000,001
                + "  BI7 BIGINT DEFAULT -X'0F4241',\n" // decimal -1,000,001

                + ");\n"
                ;

        literalSchema += "CREATE PROCEDURE InsertVarbinary AS "
                + "INSERT INTO T (PK, VB) VALUES (?, ?);";

        literalSchema += "CREATE PROCEDURE InsertBigint AS "
                + "INSERT INTO T (PK, BI) VALUES (?, ?);";

        literalSchema += "CREATE PROCEDURE InsertTinyint AS "
                + "INSERT INTO T (PK, TI) VALUES (?, ?);";

        literalSchema += "CREATE PROCEDURE UpdateTinyint AS "
                + "UPDATE T SET TI = ? WHERE PK = ?;";

        literalSchema += "CREATE PROCEDURE InsertTinyintConstantMax AS "
                + "INSERT INTO T (PK, TI) VALUES (?, X'7F');";

        literalSchema += "CREATE PROCEDURE InsertTinyintConstantMin aS "
                + "INSERT INTO T (PK, TI) VALUES (?, X'FfffFfffFfffFf81');";

        literalSchema += "CREATE PROCEDURE UpdateTinyintConstantMax AS "
                + "UPDATE T SET TI = X'7F' WHERE PK = ?;";

        literalSchema += "CREATE PROCEDURE UpdateTinyintConstantMin AS "
                + "UPDATE T SET TI = X'FfffFfffFfffFf81' WHERE PK = ?;";

        literalSchema += "CREATE PROCEDURE MixedTypeMath AS \n"
                + "SELECT\n"
                + "  33 + ?,\n" //
                + "  33.0 + ?,\n"
                + "  X'21' + ?,\n"
                + "  10000000000000000033 + ?"
                + "FROM T;"
                + "";

        for (long val : interestingValues) {
            literalSchema += "CREATE PROCEDURE XQUOTE_VARBINARY_PROC_" + makeSixteenDigits(val) + " AS\n"
                    + "  SELECT PK FROM T WHERE VB = " + longToEightByteHexLiteral(val) + ";\n";
        }

        // Create a bunch of procedures with various embedded literals
        // in the select list
        for (long val : interestingValues) {
            literalSchema += "CREATE PROCEDURE INT_HEX_LITERAL_PROC_" + makeEvenDigits(val) + " AS\n"
                    + "  SELECT HEX(BITXOR(X'" + makeEvenDigits(val) + "', ?)) FROM T;\n";
        }

        // Create a bunch of procedures with various embedded literals
        // in the where clause
        for (long val : interestingValues) {
            literalSchema += "CREATE PROCEDURE INT_HEX_LITERAL_PROC_WHERE_" + makeEvenDigits(val) + " AS\n"
                    + "  SELECT PK FROM T WHERE BI = X'" + makeEvenDigits(val) + "';\n";
        }

        // Create a bunch of procedures with various embedded literals
        // in arithmetic expressions
        for (long val : boringValues) {
            literalSchema += "CREATE PROCEDURE HEX_LITERAL_PROC_ARITH_" + makeEvenDigits(val) + " AS\n"
                    + "  SELECT \n"
                    + "? + X'" + makeEvenDigits(val) + "',\n"
                    + "X'" + makeEvenDigits(val) + "' - ?,\n"
                    + "? * X'" + makeEvenDigits(val) + "',\n"
                    + "X'" + makeEvenDigits(val) + "' / ?,\n"
                    + "- X'" + makeEvenDigits(val) + "'\n"
                    + "FROM T;";
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
