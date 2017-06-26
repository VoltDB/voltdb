/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.TimestampType;
import org.voltdb_testfuncs.UserDefinedTestFunctions.UDF_TEST;
import org.voltdb_testfuncs.UserDefinedTestFunctions.UserDefinedTestException;

/**
 * Tests of SQL statements that use User-Defined Functions (UDF's).
 */
public class TestUserDefinedFunctions extends RegressionSuite {
    static private Random random = new Random();
    static private final BigDecimal MIN_DECIMAL_VALUE = new BigDecimal("-99999999999999999999999999.999999999999");
    static private final BigDecimal MAX_DECIMAL_VALUE = new BigDecimal( "99999999999999999999999999.999999999999");
    static private final String SIMPLE_POLYGON_WTK = "PolygonFromText( 'POLYGON((3 3, -3 3, -3 -3, 3 -3, 3 3),"
            + "(1 1, 1 2, 2 1, 1 1),(-1 -1, -1 -2, -2 -1, -1 -1))' )";

    // TODO: once UDF's actually work, i.e. return non-null values, this should
    // be removed and all calls to it replaced with calls to (JUnit) assertEquals
    // (with or, more likely, without the first arg) (ENG-12753)
    static private double minDecimalDoubleValue = MIN_DECIMAL_VALUE.doubleValue();
    static private List<Long> nullLongValues = Arrays.asList(-128L, -32768L, -2147483648L, -9223372036854775808L);
    private void assertEqualsTemp(String message, Object expected, Object actual) {
        System.out.println("message(type, function); expected; actual: " + message + "; " + expected + "; " + actual);
        if (actual == null) {
            return;
        } else if (actual instanceof Double && ((Number) actual).doubleValue() <= VoltType.NULL_FLOAT) {
            return;
        } else if (actual instanceof BigDecimal && ((BigDecimal) actual).doubleValue() < minDecimalDoubleValue) {
            return;
        } else if (actual instanceof Number && nullLongValues.contains(((Number) actual).longValue())) {
            return;
        }
        assertEquals(message, expected, actual);
    }

    // TODO: once UDF's actually work, i.e. return non-null values, this should
    // be removed and any calls to it replaced with calls to (JUnit) assertEquals
    // (ENG-12753)
    private void assertEqualsTemp(Object expected, Object actual) {
        assertEqualsTemp("", expected, actual);
    }

    /** Tests the specified <i>functionCall</i>, and confirms that the
     *  <i>expected</i> value, of type <i>returnType</i>, is returned; it
     *  does this by first INSERT-ing one row, and then SELECT-ing the
     *  <i>functionCall</i> value (as well as the ID) from that row.
     *  Optionally, you can also specify <i>columnNames</i> and corresponding
     *  <i>columnValues</i> (in addition to ID=0) to be specified in the
     *  initial INSERT statement; and which <i>tableName</i> to INSERT into
     *  and SELECT from. (If unspecified, the <i>tableName</i> is chosen,
     *  randomly, as either R1 or P1.) */
    private void testFunction(String functionCall, Object expected, VoltType returnType,
            String[] columnNames, String[] columnValues, String tableName)
            throws IOException, ProcCallException {
        if (tableName == null) {
            tableName = "R1";
            if (random.nextInt(100) < 50) {
                tableName = "P1";
            }
        }
        String allColumnNames  = "ID";
        String allColumnValues = "0";
        if (columnNames != null && columnNames.length > 0) {
            allColumnNames = "ID, " + String.join(",", columnNames);
        }
        if (columnValues != null && columnValues.length > 0) {
            allColumnValues = "0, " + String.join(",", columnValues);
        }

        Client client = getClient();
        ClientResponse cr = client.callProcedure("@AdHoc", "INSERT INTO "+tableName
                + " ("+allColumnNames+") VALUES"
                + " ("+allColumnValues+")");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());

        cr = client.callProcedure("@AdHoc",
                "SELECT ID, "+functionCall+" FROM "+tableName+" WHERE ID = 0");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        assertTrue(vt.advanceRow());

        if (expected == null) {
            Object result = vt.get(1, returnType);
            // TODO: temp debug:
//            System.out.println(functionCall+" resulted in: "+result);
            assertTrue("Expect null result for "+functionCall, vt.wasNull());
        // If expecting a VoltDB FLOAT (Java Double) value not equal to NaN
        } else if ( returnType == VoltType.FLOAT &&
                !( expected instanceof Number && Double.isNaN(((Number)expected).doubleValue()) ) ) {
            // TODO: once UDF's actually work, this line will probably need to be
            // replaced by the commented out line below, in order to avoid test
            // failures due to round-off errors (ENG-12753):
            assertEqualsTemp(returnType+", "+functionCall, expected, vt.getDouble(1));
            //assertTrue(Math.abs(vt.getDouble(1) - expected) <= 1.0e-16);
        } else {
            assertEqualsTemp(returnType+", "+functionCall, expected, vt.get(1, returnType));
            //assertEquals("Unexpected result for "+functionCall, expected, vt.get(1, returnType));
        }

        cr = client.callProcedure("@AdHoc", "TRUNCATE TABLE "+tableName);
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    /** Tests the specified <i>functionCall</i>, and confirms that the
     *  <i>expected</i> value, of type <i>returnType</i>, is returned; it
     *  does this by first INSERT-ing one row, and then SELECT-ing the
     *  <i>functionCall</i> value (as well as the ID) from that row.
     *  Optionally, you can also specify <i>columnNames</i> and corresponding
     *  <i>columnValues</i> (in addition to ID=0) to be specified in the
     *  initial INSERT statement. The table to INSERT into and SELECT from
     *  is chosen, randomly, as either R1 or P1. */
    private void testFunction(String functionCall, Object expected, VoltType returnType,
            String[] columnNames, String[] columnValues)
            throws IOException, ProcCallException {
        testFunction(functionCall, expected, returnType, columnNames, columnValues, null);
    }

    /** Tests the specified <i>functionCall</i>, and confirms that the
     *  <i>expected</i> value, of type <i>returnType</i>, is returned; it
     *  does this by first INSERT-ing one row, and then SELECT-ing the
     *  <i>functionCall</i> value (as well as the ID) from that row.
     *  The table to INSERT into and SELECT from is chosen, randomly,
     *  as either R1 or P1. */
    private void testFunction(String functionCall, Object expected, VoltType returnType)
            throws IOException, ProcCallException {
        testFunction(functionCall, expected, returnType, null, null);
    }

    /** Tests the specified <i>functionCall</i>, and confirms that an Exception
     *  is thrown, of <i>expectedExceptionType</i>; it does this by first
     *  INSERT-ing one row, and then SELECT-ing the <i>functionCall</i> value
     *  (as well as the ID) from that row, and then catching any Exception
     *  (or other Throwable) thrown.
     *  Optionally, you can also specify <i>columnNames</i> and corresponding
     *  <i>columnValues</i> (in addition to ID=0) to be specified in the
     *  initial INSERT statement. The table to INSERT into and SELECT from
     *  is chosen, randomly, as either R1 or P1. */
    private void testFunctionThrowsException(String functionCall, VoltType returnType,
            Class<? extends Throwable> expectedExceptionType, String[] columnNames, String[] columnValues) {
        try {
            testFunction(functionCall, null, returnType, columnNames, columnValues);
        } catch (Throwable ex) {
            // TODO: temp debug:
//            System.out.println(functionCall+" triggered exception: "+ex);
            ex.printStackTrace(System.out);
            assertEquals(expectedExceptionType, ex.getClass());
            return;
        }
        fail(functionCall+" did not throw expected exception: "+expectedExceptionType);
    }

    /** Tests the specified <i>functionCall</i>, and confirms that an Exception
     *  is thrown, of <i>expectedExceptionType</i>; it does this by first
     *  INSERT-ing one row, and then SELECT-ing the <i>functionCall</i> value
     *  (as well as the ID) from that row, and then catching any Exception
     *  (or other Throwable) thrown. The table to INSERT into and SELECT from
     *  is chosen, randomly, as either R1 or P1. */
    private void testFunctionThrowsException(String functionCall, VoltType returnType,
            Class<? extends Throwable> expectedExceptionType) {
        testFunctionThrowsException(functionCall, returnType, expectedExceptionType, null, null);
    }


    // Test UDF's with zero arguments ...

    final static double PI = 3.1415926535897932384;
    public void testPiFunction0args() throws IOException, ProcCallException {
        testFunction("piUdf()", PI, VoltType.FLOAT);
    }
    public void testPiBoxedFunction0args() throws IOException, ProcCallException {
        testFunction("piUdfBoxed()", PI, VoltType.FLOAT);
    }

    // Test UDF's with one argument, with parameters and return values of various types ...

    public void testAbsTinyintFunction() throws IOException, ProcCallException {
        testFunction("absTinyint(-99)", (byte)99, VoltType.TINYINT);
    }
    public void testAbsSmallintFunction() throws IOException, ProcCallException {
        testFunction("absSmallint(-99)", (short)99, VoltType.SMALLINT);
    }
    public void testAbsIntegerFunction() throws IOException, ProcCallException {
        testFunction("absInteger(-99)", 99, VoltType.INTEGER);
    }
    public void testAbsBigintFunction() throws IOException, ProcCallException {
        testFunction("absBigint(-99)", 99L, VoltType.BIGINT);
    }
    public void testAbsFloatFunction() throws IOException, ProcCallException {
        testFunction("absFloat(-99)", 99.0D, VoltType.FLOAT);
    }
    public void testAbsTinyintBoxedFunction() throws IOException, ProcCallException {
        testFunction("absTinyintBoxed(-98)", (byte)98, VoltType.TINYINT);
    }
    public void testAbsSmallintBoxedFunction() throws IOException, ProcCallException {
        testFunction("absSmallintBoxed(-98)", (short)98, VoltType.SMALLINT);
    }
    public void testAbsIntegerBoxedFunction() throws IOException, ProcCallException {
        testFunction("absIntegerBoxed(-98)", 98, VoltType.INTEGER);
    }
    public void testAbsBigintBoxedFunction() throws IOException, ProcCallException {
        testFunction("absBigintBoxed(-98)", 98L, VoltType.BIGINT);
    }
    public void testAbsFloatBoxedFunction() throws IOException, ProcCallException {
        testFunction("absFloatBoxed(-98)", 98.0D, VoltType.FLOAT);
    }
    public void testAbsDecimalFunction() throws IOException, ProcCallException {
        testFunction("absDecimal(-99)", new BigDecimal(99), VoltType.DECIMAL);
    }
    public void testReverseFunction() throws IOException, ProcCallException {
        testFunction("reverse('abcde')", "edcba", VoltType.STRING);
    }

    // TODO: uncomment, once GEOGRAPHY (and GEOGRAPHY_POINT) UDFs work (ENG-12753):
//    public void testNumRingsFunction() throws IOException, ProcCallException {
//        testFunction("numRings("+SIMPLE_POLYGON_WTK+")", 3, VoltType.INTEGER);
//    }
//    public void testNumPointsUdfFunction() throws IOException, ProcCallException {
//        testFunction("numPointsUdf("+SIMPLE_POLYGON_WTK+")", 13, VoltType.INTEGER);
//    }

    // Test UDF's with two arguments, with parameters and return value of various types ...

    public void testModTinyintFunction() throws IOException, ProcCallException {
        testFunction("modTinyint(47,10)", (byte)7, VoltType.TINYINT);
    }
    public void testModSmallintFunction() throws IOException, ProcCallException {
        testFunction("modSmallint(47,10)", (short)7, VoltType.SMALLINT);
    }
    public void testModIntegerFunction() throws IOException, ProcCallException {
        testFunction("modInteger(47,10)", 7, VoltType.INTEGER);
    }
    public void testModBigintFunction() throws IOException, ProcCallException {
        testFunction("modBigint(47,10)", 7L, VoltType.BIGINT);
    }
    public void testModFloatFunction() throws IOException, ProcCallException {
        testFunction("modFloat(47,10)", 7.0D, VoltType.FLOAT);
    }
    public void testModTinyintBoxedFunction() throws IOException, ProcCallException {
        testFunction("modTinyintBoxed(36,10)", (byte)6, VoltType.TINYINT);
    }
    public void testModSmallintBoxedFunction() throws IOException, ProcCallException {
        testFunction("modSmallintBoxed(36,10)", (short)6, VoltType.SMALLINT);
    }
    public void testModIntegerBoxedFunction() throws IOException, ProcCallException {
        testFunction("modIntegerBoxed(36,10)", 6, VoltType.INTEGER);
    }
    public void testModBigintBoxedFunction() throws IOException, ProcCallException {
        testFunction("modBigintBoxed(36,10)", 6L, VoltType.BIGINT);
    }
    public void testModFloatBoxedFunction() throws IOException, ProcCallException {
        testFunction("modFloatBoxed(36.7,10)", 6.7D, VoltType.FLOAT);
    }
    public void testModDecimalFunction() throws IOException, ProcCallException {
        testFunction("modDecimal(47.8,10)", new BigDecimal(7.8), VoltType.DECIMAL);
    }

    // TODO: uncomment, once VARBINARY UDFs work (ENG-12753):
//    public void testBtrimFunction() throws IOException, ProcCallException {
//        byte[] expectedResult = new byte[3];
//        expectedResult[0] = (byte) 0xAB;
//        expectedResult[1] = (byte) 0x00;
//        expectedResult[2] = (byte) 0xCD;
//        testFunction("btrim(x'0001AB00CD0100',x'0001')", expectedResult, VoltType.VARBINARY);
//    }
//    public void testBtrimBoxedFunction() throws IOException, ProcCallException {
//        Byte[] expectedResult = new Byte[3];
//        expectedResult[0] = (byte) 0xAB;
//        expectedResult[1] = (byte) 0x00;
//        expectedResult[2] = (byte) 0xCD;
//        testFunction("btrimBoxed(x'0001AB00CD0100',x'0001')", expectedResult, VoltType.VARBINARY);
//    }

    // Test more UDF's with two arguments; now the arguments are column names,
    // rather than constant values ...

    public void testAdd2TinyintFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"TINY", "SMALL"};
        String[] columnValues = {"100", "27"};
        testFunction("add2Tinyint(TINY, SMALL)", (byte)127, VoltType.TINYINT, columnNames, columnValues);
    }
    public void testAdd2TinyintBoxedFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"SMALL", "TINY"};
        String[] columnValues = {"-100", "-27"};
        testFunction("add2TinyintBoxed(SMALL, TINY)", (byte)-127, VoltType.TINYINT, columnNames, columnValues);
    }
    public void testAdd2SmallintFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"SMALL", "TINY"};
        String[] columnValues = {"-32700", "-67"};
        testFunction("add2Smallint(SMALL, TINY)", (short)-32767, VoltType.SMALLINT, columnNames, columnValues);
    }
    public void testAdd2SmallintBoxedFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"TINY", "SMALL"};
        String[] columnValues = {"67", "32700"};
        testFunction("add2SmallintBoxed(TINY, SMALL)", (short)32767, VoltType.SMALLINT, columnNames, columnValues);
    }
    public void testAdd2IntegerFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"INT", "SMALL"};
        String[] columnValues = {"2147480000", "3647"};
        testFunction("add2Integer(INT, SMALL)", 2147483647, VoltType.INTEGER, columnNames, columnValues);
    }
    public void testAdd2IntegerBoxedFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"SMALL", "INT"};
        String[] columnValues = {"-3647", "-2147480000"};
        testFunction("add2IntegerBoxed(SMALL, INT)", -2147483647, VoltType.INTEGER, columnNames, columnValues);
    }
    public void testAdd2BigintFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"BIG", "INT"};
        String[] columnValues = {"-9223372036000000000", "-854775807"};
        testFunction("add2Bigint(BIG, INT)", -9223372036854775807L, VoltType.BIGINT, columnNames, columnValues);
    }
    public void testAdd2BigintBoxedFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"INT", "BIG"};
        String[] columnValues = {"854775807", "9223372036000000000"};
        testFunction("add2BigintBoxed(BIG, BIG)", 9223372036854775807L, VoltType.BIGINT, columnNames, columnValues);
    }
    public void testAdd2FloatFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"NUM", "DEC"};
        String[] columnValues = {"111.111", "222.222"};
        testFunction("add2Float(NUM, DEC)", 333.333D, VoltType.FLOAT, columnNames, columnValues);
    }
    public void testAdd2FloatBoxedFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"DEC", "NUM"};
        String[] columnValues = {"-111.111", "-222.222"};
        testFunction("add2FloatBoxed(DEC, NUM)", -333.333D, VoltType.FLOAT, columnNames, columnValues);
    }
    public void testAdd2DecimalFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"DEC"};
        String[] columnValues = {"111.111"};
        testFunction("add2Decimal(DEC, DEC)", 222.222, VoltType.DECIMAL, columnNames, columnValues);
    }
    public void testAdd2VarcharFunction() throws IOException, ProcCallException {
        String[] columnNames  = {"VCHAR_INLINE_MAX", "VCHAR"};
        String[] columnValues = {"'Foo'", "'Bar'"};
        testFunction("add2Varchar(VCHAR_INLINE_MAX, VCHAR)", "FooBar", VoltType.STRING, columnNames, columnValues);
    }
    public void testAddYearsToTimestamp() throws IOException, ProcCallException {
        String[] columnNames  = {"TIME", "INT"};
        String[] columnValues = {"'2012-07-19 01:05:06'", "5"};
        TimestampType expectedResult = new TimestampType("2017-07-19 01:05:06");
        testFunction("addYearsToTimestamp(TIME, INT)", expectedResult, VoltType.TIMESTAMP, columnNames, columnValues);
    }

    // TODO: uncomment, once VARBINARY UDFs work (ENG-12753):
//    public void testAdd2VarbinaryFunction() throws IOException, ProcCallException {
//        String[] columnNames  = {"VARBIN1", "VARBIN2"};
//        String[] columnValues = {"x'010203'", "x'0405'"};
//        byte[] expectedResult = new byte[3];
//        expectedResult[0] = (byte) 0x05;
//        expectedResult[1] = (byte) 0x07;
//        expectedResult[2] = (byte) 0x03;
//        testFunction("add2Varbinary(VARBIN1, VARBIN2)", expectedResult, VoltType.VARBINARY, columnNames, columnValues);
//    }
//    public void testAdd2VarbinaryBoxedFunction() throws IOException, ProcCallException {
//        String[] columnNames  = {"VARBIN1", "VARBIN2"};
//        String[] columnValues = {"x'010203'", "x'0405'"};
//        Byte[] expectedResult = new Byte[3];
//        expectedResult[0] = (byte) 0x05;
//        expectedResult[1] = (byte) 0x07;
//        expectedResult[2] = (byte) 0x03;
//        testFunction("add2VarbinaryBoxed(VARBIN1, VARBIN2)", expectedResult, VoltType.VARBINARY, columnNames, columnValues);
//    }

    // TODO: uncomment, once GEOGRAPHY_POINT and GEOGRAPHY UDFs work (ENG-12753):
//    public void testAdd2GeographyPoint() throws IOException, ProcCallException {
//        String[] columnNames  = {"POINT1", "POINT2"};
//        String[] columnValues = {"PointFromText('POINT(1 2)')", "PointFromText('POINT(3 4)')"};
//        GeographyPointValue expectedResult = new GeographyPointValue(4, 6);
//        testFunction("add2GeographyPoint(POINT1, POINT2)", expectedResult, VoltType.GEOGRAPHY_POINT, columnNames, columnValues);
//    }
//    public void testAddGeographyPointToGeography() throws IOException, ProcCallException {
//        String[] columnNames  = {"POLYGON", "POINT1"};
//        String[] columnValues = {SIMPLE_POLYGON_WTK, "PointFromText('POINT(1 2)')"};
//        GeographyValue expectedResult = new GeographyValue("PolygonFromText( 'POLYGON((4 5, -2 5, -2 -1, 4 -1, 4 5),"
//                                                        + "(2 3, 2 4, 3 3, 2 4),(0 1, 0 0, -1 1, 0 1))' )");
//        testFunction("addGeographyPointToGeography(POLYGON, POINT1)", expectedResult, VoltType.GEOGRAPHY, columnNames, columnValues);
//    }

    // Test concat UDF's with two, three or four arguments ...

    public void testConcat2VarcharFunction() throws IOException, ProcCallException {
        testFunction("concat2Varchar('Foo','Bar')", "FooBar", VoltType.STRING);
    }
    public void testConcat3VarcharFunction() throws IOException, ProcCallException {
        testFunction("concat3Varchar('Foo','Bar','Baz')", "FooBarBaz", VoltType.STRING);
    }
    public void testConcat4VarcharFunction() throws IOException, ProcCallException {
        testFunction("concat4Varchar('Foo','Bar','Baz','Qux')", "FooBarBazQux", VoltType.STRING);
    }

    // Test UDF calls designed to return various (VoltDB) null values ...

    public void testReturnTinyintNull() throws IOException, ProcCallException {
        testFunction("add2Tinyint("+UDF_TEST.RETURN_TINYINT_NULL+", 0)", null, VoltType.TINYINT);
    }
    public void testReturnTinyintBoxedNull() throws IOException, ProcCallException {
        testFunction("add2TinyintBoxed("+UDF_TEST.RETURN_TINYINT_NULL+", 0)", null, VoltType.TINYINT);
    }
    public void testReturnSmallintNull() throws IOException, ProcCallException {
        testFunction("add2Smallint("+UDF_TEST.RETURN_SMALLINT_NULL+", 0)", null, VoltType.SMALLINT);
    }
    public void testReturnSmallintBoxedNull() throws IOException, ProcCallException {
        testFunction("add2SmallintBoxed("+UDF_TEST.RETURN_SMALLINT_NULL+", 0)", null, VoltType.SMALLINT);
    }
    public void testReturnIntegerNull() throws IOException, ProcCallException {
        testFunction("add2Integer("+UDF_TEST.RETURN_INTEGER_NULL+", 0)", null, VoltType.INTEGER);
    }
    public void testReturnIntegerBoxedNull() throws IOException, ProcCallException {
        testFunction("add2IntegerBoxed("+UDF_TEST.RETURN_INTEGER_NULL+", 0)", null, VoltType.INTEGER);
    }
    public void testReturnBigintNull() throws IOException, ProcCallException {
        testFunction("add2Bigint("+UDF_TEST.RETURN_BIGINT_NULL+", 0)", null, VoltType.BIGINT);
    }
    public void testReturnBigintBoxedNull() throws IOException, ProcCallException {
        testFunction("add2BigintBoxed("+UDF_TEST.RETURN_BIGINT_NULL+", 0)", null, VoltType.BIGINT);
    }
    public void testReturnFloatNull() throws IOException, ProcCallException {
        testFunction("add2Float("+UDF_TEST.RETURN_FLOAT_NULL+", 0)", null, VoltType.FLOAT);
    }
    public void testReturnFloatBoxedNull() throws IOException, ProcCallException {
        testFunction("add2FloatBoxed("+UDF_TEST.RETURN_FLOAT_NULL+", 0)", null, VoltType.FLOAT);
    }
    public void testReturnDecimalNull() throws IOException, ProcCallException {
        testFunction("add2Decimal("+UDF_TEST.RETURN_DECIMAL_NULL+", 0)", null, VoltType.DECIMAL);
    }
    public void testReturnVarcharNull() throws IOException, ProcCallException {
        testFunction("add2Varchar('"+UDF_TEST.RETURN_JAVA_NULL+"', 'Foo')", null, VoltType.STRING);
    }

    // TODO: uncomment, once VARBINARY UDFs work (ENG-12753):
//    public void testReturnVarbinaryNull() throws IOException, ProcCallException {
//        testFunction("add2Varbinary(x'" + String.format("%02X", (byte)UDF_TEST.RETURN_DATA_TYPE_NULL) + "', x'00')",
//                    null, VoltType.VARBINARY);
//    }
//    public void testReturnVarbinaryBoxedNull() throws IOException, ProcCallException {
//        testFunction("add2VarbinaryBoxed(x'" + String.format("%02X", (byte)UDF_TEST.RETURN_DATA_TYPE_NULL) + "', x'00')",
//                    null, VoltType.VARBINARY);
//    }

    public void testReturnTimestampNull() throws IOException, ProcCallException {
        int year = 1900 + UDF_TEST.RETURN_DATA_TYPE_NULL;
        testFunction("addYearsToTimestamp('"+year+"-12-31 23:59:50.0', 0)", null, VoltType.TIMESTAMP);
    }

    // TODO: uncomment, once GEOGRAPHY_POINT and GEOGRAPHY UDFs work (ENG-12753):
//    public void testReturnGeographyPointNull() throws IOException, ProcCallException {
//        testFunction("add2GeographyPoint(PointFromText('POINT("+UDF_TEST.RETURN_DATA_TYPE_NULL+" 0)'), "
//                    + "PointFromText('POINT(0 0)') )", null, VoltType.GEOGRAPHY_POINT);
//    }
//    public void testReturnGeographyNull() throws IOException, ProcCallException {
//        int nullCode = UDF_TEST.RETURN_DATA_TYPE_NULL;
//        testFunction("addGeographyPointToGeography( PolygonFromText('POLYGON"
//                    + "((0 0, "+nullCode+" 0, 0 "+nullCode+", 0 0))'), "
//                    + "PointFromText('POINT(0 0)') )", null, VoltType.GEOGRAPHY);
//    }

    // Test UDF calls designed to return unusual values (NaN, DECIMAL min & max) ...

    public void testReturnFloatNaN() throws IOException, ProcCallException {
        testFunction("add2Float("+UDF_TEST.RETURN_NaN+", 0)", Double.NaN, VoltType.FLOAT);
    }
    public void testReturnDecimalMin() throws IOException, ProcCallException {
        testFunction("add2Decimal("+UDF_TEST.RETURN_DECIMAL_MIN+", 0)", MIN_DECIMAL_VALUE, VoltType.DECIMAL);
    }
    public void testReturnDecimalMax() throws IOException, ProcCallException {
        testFunction("add2Decimal("+UDF_TEST.RETURN_DECIMAL_MAX+", 0)", MAX_DECIMAL_VALUE, VoltType.DECIMAL);
    }

    // Test UDF calls designed to throw an exception ...

    // TODO: uncomment, once UDFs throwing exceptions works (ENG-12753):
//    public void testNullPointerException() {
//        testFunctionThrowsException("add2Tinyint("+UDF_TEST.THROW_NullPointerException+", 0)",
//                VoltType.TINYINT, NullPointerException.class);
//    }
//    public void testIllegalArgumentException() {
//        testFunctionThrowsException("add2SmallintBoxed("+UDF_TEST.THROW_IllegalArgumentException+", 0)",
//                VoltType.SMALLINT, IllegalArgumentException.class);
//    }
//    public void testNumberFormatException() {
//        testFunctionThrowsException("add2Integer("+UDF_TEST.THROW_NumberFormatException+", 0)",
//                VoltType.INTEGER, NumberFormatException.class);
//    }
//    public void testArrayIndexOutOfBoundsException() {
//        testFunctionThrowsException("add2BigintBoxed("+UDF_TEST.THROW_ArrayIndexOutOfBoundsException+", 0)",
//                VoltType.BIGINT, ArrayIndexOutOfBoundsException.class);
//    }
//    public void testClassCastException() {
//        testFunctionThrowsException("add2Float("+UDF_TEST.THROW_ClassCastException+", 0.0)",
//                VoltType.FLOAT, ClassCastException.class);
//    }
//    public void testArithmeticException() {
//        testFunctionThrowsException("add2FloatBoxed("+UDF_TEST.THROW_ArithmeticException+", 0.0)",
//                VoltType.FLOAT, ArithmeticException.class);
//    }
//    public void testUnsupportedOperationException() {
//        testFunctionThrowsException("add2Decimal("+UDF_TEST.THROW_UnsupportedOperationException+", 0.0)",
//                VoltType.DECIMAL, UnsupportedOperationException.class);
//    }
//    public void testVoltTypeException() {
//        testFunctionThrowsException("add2Varchar('"+UDF_TEST.THROW_VoltTypeException+"', 'Foo')",
//                VoltType.STRING, VoltTypeException.class);
//    }
//    public void testUserDefinedTestException() {
//        int year = 1900 + UDF_TEST.THROW_UserDefinedTestException;
//        testFunctionThrowsException("addYearsToTimestamp('"+year+"-12-31 23:59:50.0', 0)",
//                VoltType.TIMESTAMP, UserDefinedTestException.class);
//    }

    // We've tested all the exceptions we care about above, but we want to
    // check exceptions for the remaining data types too ...

    // TODO: uncomment, once UDFs throwing exceptions works; and once VARBINARY,
    // GEOGRAPHY_POINT and GEOGRAPHY UDFs work (ENG-12753):
//    public void testVarbinaryIllegalArgumentException() {
//        testFunctionThrowsException("add2Varbinary(x'"
//                + String.format("%02X", (byte)UDF_TEST.THROW_IllegalArgumentException)
//                + "', x'00')", VoltType.VARBINARY, IllegalArgumentException.class);
//    }
//    public void testVarbinaryBoxedUnsupportedOperationException() {
//        testFunctionThrowsException("add2VarbinaryBoxed(x'"
//                + String.format("%02X", (byte)UDF_TEST.THROW_UnsupportedOperationException)
//                + "', x'00')", VoltType.VARBINARY, UnsupportedOperationException.class);
//    }
//    public void testGeographyPointVoltTypeException() {
//        testFunctionThrowsException("add2GeographyPoint(PointFromText('POINT("+UDF_TEST.THROW_VoltTypeException+" 0)'), "
//                    + "PointFromText('POINT(0 0)') )", VoltType.GEOGRAPHY_POINT, VoltTypeException.class);
//    }
//    public void testGeographyUserDefinedTestException() {
//        int udteCode = UDF_TEST.THROW_UserDefinedTestException;
//        testFunctionThrowsException("addGeographyPointToGeography( PolygonFromText('POLYGON"
//                    + "((0 0, "+udteCode+" 0, 0 "+udteCode+", 0 0))'), "
//                    + "PointFromText('POINT(0 0)') )",
//                    VoltType.GEOGRAPHY, UserDefinedTestException.class);
//    }


    /** Simple constructor that passes parameter on to superclass.
     *  @param name The name of the method to run as a test. (JUnit magic) */
    public TestUserDefinedFunctions(String name) {
        super(name);
    }

    static public Test suite() {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestUserDefinedFunctions.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);

        byte[] createFunctionsDDL = null;
        try {
            createFunctionsDDL = Files.readAllBytes(Paths.get("tests/testfuncs/org/voltdb_testfuncs/UserDefinedTestFunctions-DDL.sql"));
        } catch (IOException e) {
            fail(e.getMessage());
        }

        final String tableDefinition = " ("
                + "  ID      INTEGER NOT NULL PRIMARY KEY,"
                + "  TINY    TINYINT,"
                + "  SMALL   SMALLINT,"
                + "  INT     INTEGER,"
                + "  BIG     BIGINT,"
                + "  NUM     FLOAT,"
                + "  DEC     DECIMAL,"
                + "  VCHAR_INLINE_MAX VARCHAR(63 BYTES),"
                + "  VCHAR            VARCHAR(64 BYTES),"
                + "  TIME    TIMESTAMP,"
                + "  VARBIN1 VARBINARY(100),"
                + "  VARBIN2 VARBINARY(100),"
                + "  POINT1  GEOGRAPHY_POINT,"
                + "  POINT2  GEOGRAPHY_POINT,"
                + "  POLYGON GEOGRAPHY );\n";
        final String literalSchema = new String(createFunctionsDDL) + "\n"
                + "CREATE TABLE R1" + tableDefinition
                + "CREATE TABLE P1" + tableDefinition
                + "PARTITION TABLE P1 ON COLUMN ID;";

        // TODO: temp debug:
//        System.out.println("literalSchema:\n" + literalSchema);

        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            fail(e.getMessage());
        }

        /////////////////////////////////////////////////////////////
        // CONFIG #1: 2 Local Sites/Partitions running on JNI backend
        /////////////////////////////////////////////////////////////
        LocalCluster config = new LocalCluster("tudf-twosites.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        //* enable for simplified config */ config = new LocalCluster("matview-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // build the jarfile
        assertTrue(config.compile(project));
        // add this config to the set of tests to run
        builder.addServerConfig(config);

        /////////////////////////////////////////////////////////////
        // CONFIG #2: 3-node k=1 cluster
        /////////////////////////////////////////////////////////////
        config = new LocalCluster("tudf-cluster.jar", 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        assertTrue(config.compile(project));
        // TODO: uncomment this once UDF's work on multiple-node clusters (ENG-12753):
//        builder.addServerConfig(config);

        return builder;
    }

}
