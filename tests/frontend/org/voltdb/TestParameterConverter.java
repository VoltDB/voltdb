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


package org.voltdb;

import java.math.BigDecimal;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import junit.framework.TestCase;


public class TestParameterConverter extends TestCase
{
    // Tests use naming convention:
    // (invocation deserialized type) To (stored procedure parameter type)

    public void testIntegerToInt() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(int.class, new Integer(1));
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1, ((Integer)r).intValue());
    }

    public void testIntToInt() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(int.class, 2);
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(2, ((Integer)r).intValue());
    }

    public void testIntToInteger() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(Integer.class, 1);
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(new Integer(1), (Integer)r);
    }

    public void testIntToLong() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(long.class, -1000);
        assertTrue("expect long", r instanceof Number);
        assertEquals(-1000L, ((Number)r).longValue());
    }

    public void testLongToIntException() throws Exception {
        try {
            ParameterConverter.
                tryToMakeCompatible(int.class, -9000000000L);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("The provided value: (-9000000000) of type:"
                    + " java.lang.Long is not a match or is out of range for the target parameter type: int"));
        }
    }

    public void testLongToIntegerException() throws Exception {
        try {
            ParameterConverter.
                tryToMakeCompatible(Integer.class, -9000000000L);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains("The provided value: (-9000000000) of type:"
                    + " java.lang.Long is not a match or is out of range for the target parameter type: java.lang.Integer"));
        }
    }

    public void testStringToInt() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(int.class, "1000");
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1000, ((Integer)r).intValue());
    }

    public void testStringToInteger() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(Integer.class, "1000");
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(new Integer(1000), r);
    }

    public void testStringToDouble() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(double.class, "34.56");
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(34.56, ((Double)r).doubleValue());
    }

    public void testStringToBoxedDouble() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(Double.class, "34.56");
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(new Double(34.56), r);
    }

    // Add more test unit cases
    public void testStringWithWhitespaceToDouble() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(double.class, "  34.56  ");
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(34.56, ((Double)r).doubleValue());
    }

    public void testStringWithWhitespaceToBoxedDouble() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(Double.class, "  34.56  ");
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(new Double(34.56), r);
    }

    public void testCommasStringIntegerToInt() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(int.class, "1,100");
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1100, ((Integer)r).intValue());
    }

    public void testCommasStringIntegerToInteger() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(Integer.class, "1,100");
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(new Integer(1100), r);
    }

    public void testCommasStringIntegerToDouble() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(double.class, "2,301,100.23");
        assertTrue("expect integer", r.getClass() == Double.class);
        assertEquals(2301100.23, ((Double)r).doubleValue());
    }

    public void testCommasStringIntegerToBoxedDouble() throws Exception {
        Object r = ParameterConverter.
            tryToMakeCompatible(Double.class, "2,301,100.23");
        assertTrue("expect integer", r.getClass() == Double.class);
        assertEquals(new Double(2301100.23), r);
    }

    public void testStringToTimestamp() throws Exception {
        TimestampType t = new TimestampType();
        Object r = ParameterConverter.
            tryToMakeCompatible(TimestampType.class, t);
        assertTrue("expect timestamp", r.getClass() == TimestampType.class);
        assertEquals(t, r);
    }

    public void testStringToVarBinary() throws Exception {
        String t = "1E3A";
        Object r = ParameterConverter.
            tryToMakeCompatible(byte[].class, t);
        assertTrue("expect varbinary", r.getClass() == byte[].class);
        assertEquals(t, Encoder.hexEncode((byte[])r));
    }

    public void testStringToBoxedVarBinary() throws Exception {
        String t = "1E3A";
        Object r = ParameterConverter.
            tryToMakeCompatible(Byte[].class, t);
        assertTrue("expect varbinary", r.getClass() == Byte[].class);
        assertEquals(t, Encoder.hexEncode( ArrayUtils.toPrimitive((Byte[])r) ));
    }

    public void testEmptyStringToVarBinary() throws Exception {
        String t = "";
        Object r = ParameterConverter.
            tryToMakeCompatible(byte[].class, t);
        assertTrue("expect varbinary", r.getClass() == byte[].class);
        assertEquals(t, Encoder.hexEncode((byte[])r));
    }

    public void testEmptyStringToBoxedVarBinary() throws Exception {
        String t = "";
        Object r = ParameterConverter.
            tryToMakeCompatible(Byte[].class, t);
        assertTrue("expect varbinary", r.getClass() == Byte[].class);
        assertEquals(t, Encoder.hexEncode( ArrayUtils.toPrimitive((Byte[])r) ));
    }

    public void testByteToBoxedByte() throws Exception {
        String t = "1E3A";
        Object byteArr = ParameterConverter.
            tryToMakeCompatible(byte[].class, t);
        assertTrue("expect varbinary", byteArr.getClass() == byte[].class);

        Object r2 = ParameterConverter.
                tryToMakeCompatible(Byte[].class, byteArr);
        assertTrue("expect Byte[]", r2.getClass() == Byte[].class);
        assertEquals(Encoder.hexEncode((byte[])byteArr),
                Encoder.hexEncode( ArrayUtils.toPrimitive((Byte[])r2)) );
    }

    public void testOneStringToPoint(String rep, GeographyPointValue pt, double epsilon) throws Exception {
        Object r = ParameterConverter.tryToMakeCompatible(GeographyPointValue.class, rep);
        assertTrue("expected GeographyPointValue", r.getClass() == GeographyPointValue.class);
        GeographyPointValue rpt = (GeographyPointValue)r;
        assertEquals("Cannot convert string to geography point.", pt.getLatitude(),  rpt.getLatitude(),  epsilon);
        assertEquals("Cannot convert string to geography point.", pt.getLongitude(), rpt.getLongitude(), epsilon);
    }

    public void testOneStringToPolygon(String actualAsString, GeographyValue expected) throws Exception {
        Object actual = ParameterConverter.tryToMakeCompatible(GeographyValue.class, actualAsString);
        assertTrue("expected GeographyValue", actual.getClass() == GeographyValue.class);
        assertEquals("Cannot convert string to polygon.", expected.toString(), actual.toString());
    }

    public void testStringToGeographyPointValue() throws Exception {
        double epsilon = 1.0e-3;
        // The unfortunately eccentric spacing here is to test parsing white space.
        testOneStringToPoint("point(20.666 10.333)",               new GeographyPointValue( 20.666,  10.333), epsilon);
        testOneStringToPoint("  point  (20.666 10.333)    ",       new GeographyPointValue( 20.666,  10.333), epsilon);
        testOneStringToPoint("point(-20.666 -10.333)",             new GeographyPointValue(-20.666, -10.333), epsilon);
        testOneStringToPoint("  point  (-20.666   -10.333)    ",   new GeographyPointValue(-20.666, -10.333), epsilon);
        testOneStringToPoint("point(10 10)",                       new GeographyPointValue(10.0,    10.0),    epsilon);
        testOneStringToPoint("point(10.0 10.0)",                   new GeographyPointValue(10.0, 10.0),       epsilon);
        testOneStringToPoint("point(10 10)",                       new GeographyPointValue(10.0, 10.0),       epsilon);
        // testOneStringToPoint(null, "null");
    }

    public void testStringToPolygonType() throws Exception {
        testOneStringToPolygon("polygon((0 0, 1 0, 1 1, 0 1, 0 0))",
                               new GeographyValue(Collections.singletonList(Arrays.asList(new GeographyPointValue[]{ new GeographyPointValue(0,0),
                                                                                                           new GeographyPointValue(1, 0),
                                                                                                           new GeographyPointValue(1, 1),
                                                                                                           new GeographyPointValue(0, 1),
                                                                                                           new GeographyPointValue(0, 0) }))));
        GeographyValue geog;
        // The Bermuda Triangle, counter clockwise.
        List<GeographyPointValue> outerLoop = Arrays.asList(new GeographyPointValue(-64.751, 32.305),
                                                  new GeographyPointValue(-80.437, 25.244),
                                                  new GeographyPointValue(-66.371, 18.476),
                                                  new GeographyPointValue(-64.751, 32.305));
        // A triangular hole
        // Note that this needs to be clockwise.
        List<GeographyPointValue> innerLoop = Arrays.asList(new GeographyPointValue(-68.855, 25.361),
                                                            new GeographyPointValue(-73.381, 28.376),
                                                            new GeographyPointValue(-68.874, 28.066),
                                                            new GeographyPointValue(-68.855, 25.361));
        geog = new GeographyValue(Arrays.asList(outerLoop, innerLoop));
        String geogRep = "POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), " + "(-68.855 25.361, -73.381 28.376, -68.874 28.066, -68.855 25.361))";
        testOneStringToPolygon(geogRep, geog);
        // round trip
        geog = new GeographyValue(geogRep);
        testOneStringToPolygon(geogRep, geog);
    }

    public void testNulls() {
        assertEquals(VoltType.NULL_TINYINT, ParameterConverter.tryToMakeCompatible(byte.class, VoltType.NULL_TINYINT));
        assertEquals(VoltType.NULL_SMALLINT, ParameterConverter.tryToMakeCompatible(short.class, VoltType.NULL_SMALLINT));
        assertEquals(VoltType.NULL_INTEGER, ParameterConverter.tryToMakeCompatible(int.class, VoltType.NULL_INTEGER));
        assertEquals(VoltType.NULL_BIGINT, ParameterConverter.tryToMakeCompatible(long.class, VoltType.NULL_BIGINT));
        assertEquals(VoltType.NULL_FLOAT, ParameterConverter.tryToMakeCompatible(double.class, VoltType.NULL_FLOAT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Byte.class, VoltType.NULL_TINYINT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Short.class, VoltType.NULL_SMALLINT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Integer.class, VoltType.NULL_INTEGER));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Long.class, VoltType.NULL_BIGINT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Double.class, VoltType.NULL_FLOAT));
        assertEquals(VoltType.NULL_TINYINT, ParameterConverter.tryToMakeCompatible(byte.class, null));
        assertEquals(VoltType.NULL_SMALLINT, ParameterConverter.tryToMakeCompatible(short.class, null));
        assertEquals(VoltType.NULL_INTEGER, ParameterConverter.tryToMakeCompatible(int.class, null));
        assertEquals(VoltType.NULL_BIGINT, ParameterConverter.tryToMakeCompatible(long.class, null));
        assertEquals(VoltType.NULL_FLOAT, ParameterConverter.tryToMakeCompatible(double.class, null));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Byte.class, null));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Short.class, null));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Integer.class, null));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Long.class, null));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Double.class, null));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(byte[].class, VoltType.NULL_TINYINT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Byte[].class, VoltType.NULL_TINYINT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(short[].class, VoltType.NULL_SMALLINT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Short[].class, VoltType.NULL_SMALLINT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(int[].class, VoltType.NULL_INTEGER));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Integer[].class, VoltType.NULL_INTEGER));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(long[].class, VoltType.NULL_BIGINT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Long[].class, VoltType.NULL_BIGINT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(double[].class, VoltType.NULL_FLOAT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Double[].class, VoltType.NULL_FLOAT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(TimestampType.class, VoltType.NULL_TIMESTAMP));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Timestamp.class, VoltType.NULL_TIMESTAMP));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Date.class, VoltType.NULL_TIMESTAMP));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(java.sql.Date.class, VoltType.NULL_TIMESTAMP));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(String.class, VoltType.NULL_STRING_OR_VARBINARY));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(BigDecimal.class, VoltType.NULL_DECIMAL));
    }

    public void testNULLValueToByteException() throws Exception {
        try {
            ParameterConverter.
                tryToMakeCompatible(Byte.class, -128);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "The provided short, int or long value: (-128) might be interpreted "
                    + "as tinyint null. Try explicitly using a byte parameter."));
        }
    }

    public void testNULLValueToShortException() throws Exception {
        try {
            ParameterConverter.
                tryToMakeCompatible(Short.class, -32768);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "The provided int or long value: (-32768) might be interpreted "
                    + "as smallint null. Try explicitly using a short parameter."));
        }
    }

    public void testNULLValueToIntException() throws Exception {
        try {
            ParameterConverter.
                tryToMakeCompatible(int.class, -2147483648L);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "The provided long value: (-2147483648) might be interpreted "
                    + "as integer null. Try explicitly using a int parameter."));
        }
    }

    public void testNULLValueToIntegerException() throws Exception {
        try {
            ParameterConverter.
                tryToMakeCompatible(Integer.class, -2147483648L);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "The provided long value: (-2147483648) might be interpreted "
                    + "as integer null. Try explicitly using a int parameter."));
        }
    }

    // hexString should be an (even-length) hexadecimal string to be decoded
    public void testStringTobyteException() throws Exception {
        try {
            ParameterConverter.
                tryToMakeCompatible(byte.class, "ABC");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "Unable to convert string ABC"
                    + " to byte value for target parameter"));
        }
    }

    public void testStringToByteException() throws Exception {
        try {
            ParameterConverter.
                tryToMakeCompatible(Byte.class, "ABC");
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "Unable to convert string ABC"
                    + " to java.lang.Byte value for target parameter"));
        }
    }

    public void testBigDecimalToLong() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(long.class, new BigDecimal(1000));
        assertTrue("expect long", r.getClass() == Long.class);
        assertEquals(1000L, ((Long)r).longValue());

        // No lossy conversion
        testBigDecimalFailWithInvalidConversion(long.class, new BigDecimal(1000.01));

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(long.class, new BigDecimal("10000000000000000000000000000000000"));
    }

    public void testBigDecimalToBoxedLong() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(Long.class, new BigDecimal(1000));
        assertTrue("expect long", r.getClass() == Long.class);
        assertEquals(1000L, r);

        // No lossy conversion
        testBigDecimalFailWithInvalidConversion(Long.class, new BigDecimal(1000.01));

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(Long.class, new BigDecimal("10000000000000000000000000000000000"));
    }

    public void testBigDecimalToInt() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(int.class, new BigDecimal(-1000));
        assertTrue("expect int", r.getClass() == Integer.class);
        assertEquals(-1000, ((Integer)r).intValue());

        // No lossy conversion
        testBigDecimalFailWithInvalidConversion(int.class, new BigDecimal(-1000.01));

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(int.class, new BigDecimal("-10000000000000000000000000000000000"));
    }

    public void testBigDecimalToInteger() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(Integer.class, new BigDecimal(-1000));
        assertTrue("expect int", r.getClass() == Integer.class);
        assertEquals(new Integer(-1000), r);

        // No lossy conversion
        testBigDecimalFailWithInvalidConversion(Integer.class, new BigDecimal(-1000.01));

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(Integer.class, new BigDecimal("-10000000000000000000000000000000000"));
    }

    public void testBigDecimalToShort() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(short.class, new BigDecimal(15));
        assertTrue("expect short", r.getClass() == Short.class);
        assertEquals((short) 15, ((Short)r).shortValue());

        // No lossy conversion
        testBigDecimalFailWithInvalidConversion(short.class, new BigDecimal(10.99));

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(short.class, new BigDecimal("10000000000000000000000000000000000"));
    }

    public void testBigDecimalToBoxedShort() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(Short.class, new BigDecimal(15));
        assertTrue("expect short", r.getClass() == Short.class);
        assertEquals(new Short((short) 15), r);

        // No lossy conversion
        testBigDecimalFailWithInvalidConversion(Short.class, new BigDecimal(10.99));

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(Short.class, new BigDecimal("10000000000000000000000000000000000"));
    }

    public void testBigDecimalToDouble() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(double.class, new BigDecimal(-3.568));
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(-3.568, ((Double)r).doubleValue());

        // Conversion to double can be lossy anyway

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(double.class, new BigDecimal("4e400"));
    }

    public void testBigDecimalToBoxedDouble() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(Double.class, new BigDecimal(-3.568));
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(new Double(-3.568), r);

        // Conversion to double can be lossy anyway

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(Double.class, new BigDecimal("4e400"));
    }

    public void testBigDecimalToByte() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(byte.class, new BigDecimal(9));
        assertTrue("expect byte", r.getClass() == Byte.class);
        assertEquals((byte) 9, ((Byte)r).byteValue());

        // No lossy conversion
        testBigDecimalFailWithInvalidConversion(byte.class, new BigDecimal(10.99));

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(byte.class, new BigDecimal("10000000000000000000000000000000000"));
    }

    public void testBigDecimalToBoxedByte() {
        // Normal conversion
        Object r = ParameterConverter.tryToMakeCompatible(Byte.class, new BigDecimal(9));
        assertTrue("expect byte", r.getClass() == Byte.class);
        assertEquals(new Byte((byte) 9), r);

        // No lossy conversion
        testBigDecimalFailWithInvalidConversion(Byte.class, new BigDecimal(10.99));

        // No out-of-range conversion
        testBigDecimalFailWithInvalidConversion(Byte.class, new BigDecimal("10000000000000000000000000000000000"));
    }

    /*
     * The helper function to test lossy / out-of-range conversions from BigDecimal. This function
     * expects the conversion to fail.
     */
    public static void testBigDecimalFailWithInvalidConversion(Class<?> expectedClz, BigDecimal param) {
        boolean hasException = false;
        try {
            ParameterConverter.tryToMakeCompatible(expectedClz, param);
        } catch (VoltTypeException e) {
            hasException = true;
        }
        assertEquals(true, hasException);
    }

    public void testArrayToScalarTypeException() throws Exception {
        int[] t = {1, 2, 3};
        try{
            ParameterConverter.
                tryToMakeCompatible(Integer.class, t);
            /* Arrays can be quite large so it doesn't make sense to silently do the conversion
            * and incur the performance hit. The client should serialize the correct invocation
            * parameters */
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "Array / Scalar parameter mismatch ([I to java.lang.Integer)"));
        }
    }

    public void testIntArrayToIntegerArray() throws Exception {
        int[] t = {1, 2, 3};
        try{
            ParameterConverter.
                tryToMakeCompatible(Integer[].class, t);
            /* Arrays can be quite large so it doesn't make sense to silently do the conversion
            * and incur the performance hit. The client should serialize the correct invocation
            * parameters */
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "tryScalarMakeCompatible: Unable to match parameter array:java.lang.Integer to provided int"));
        }
    }

    // arrays should be exactly the same type (including boxing)
    public void testIntegerArray() throws Exception {
        Integer[] t = {1, 2, 3};
        Object r = ParameterConverter.
                tryToMakeCompatible(Integer[].class, t);
        assertTrue("expect Integer[]", r.getClass() == Integer[].class);

        assertEquals(t, (Integer[])r);
    }

    public void testStringArrayToByteArray() throws Exception {
        String[] t = {"1234", "0A1B"};
        Object r = ParameterConverter.
                tryToMakeCompatible(Byte[][].class, t);
        assertTrue("expect Byte[][]", r.getClass() == Byte[][].class);

        assertEquals(t[0], Encoder.hexEncode( ArrayUtils.toPrimitive( ((Byte[][])r)[0]) ));
        assertEquals(t[1], Encoder.hexEncode( ArrayUtils.toPrimitive( ((Byte[][])r)[1]) ));
    }

    public void testEmptyStringArrayToByteArray() throws Exception {
        String[] t = {"", ""};
        Object r = ParameterConverter.
                tryToMakeCompatible(Byte[][].class, t);
        assertTrue("expect Byte[][]", r.getClass() == Byte[][].class);

        assertEquals(t[0], Encoder.hexEncode( ArrayUtils.toPrimitive( ((Byte[][])r)[0]) ));
        assertEquals(t[1], Encoder.hexEncode( ArrayUtils.toPrimitive( ((Byte[][])r)[1]) ));
    }

    public void testIncorrectStringArrayToByteArray() throws Exception {
        String[] t = {"ABC"};
        try {
            ParameterConverter.
                tryToMakeCompatible(Byte[][].class, t);
        } catch (Exception ex) {
            assertTrue(ex.getMessage().contains(
                    "String is not properly hex-encoded"));
        }
    }
}
