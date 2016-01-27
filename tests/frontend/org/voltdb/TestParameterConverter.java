/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
import org.voltdb.utils.Encoder;

import junit.framework.TestCase;


public class TestParameterConverter extends TestCase
{
    // Tests use naming convention:
    // (invocation deserialized type) To (stored procedure parameter type)

    public void testIntegerToInt() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(int.class, new Integer(1));
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1, ((Integer)r).intValue());
    }

    public void testIntToInt() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(int.class, 2);
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(2, ((Integer)r).intValue());
    }

    public void testIntToLong() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(long.class, -1000);
        assertTrue("expect long", r instanceof Number);
        assertEquals(-1000L, ((Number)r).longValue());
    }

    public void testStringToInt() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(int.class, "1000");
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1000, ((Integer)r).intValue());
    }

    public void testStringToDouble() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(double.class, "34.56");
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(new Double(34.56), ((Double)r).doubleValue());
    }

    // Add more test unit cases
    public void testStringWithWhitespaceToDouble() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(double.class, "  34.56  ");
        assertTrue("expect double", r.getClass() == Double.class);
        assertEquals(new Double(34.56), ((Double)r).doubleValue());
    }

    public void testCommasStringIntegerToInt() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(int.class, "1,100");
        assertTrue("expect integer", r.getClass() == Integer.class);
        assertEquals(1100, ((Integer)r).intValue());
    }

    public void testCommasStringIntegerToDouble() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(double.class, "2,301,100.23");
        assertTrue("expect integer", r.getClass() == Double.class);
        assertEquals(new Double(2301100.23), ((Double)r).doubleValue());
    }

    public void testNULLToInt() throws Exception
    {
        Object r = ParameterConverter.
            tryToMakeCompatible(int.class, null);
        assertTrue("expect null integer", r.getClass() == Integer.class);
        assertEquals(VoltType.NULL_INTEGER, r);
    }

    public void testStringToTimestamp() throws Exception
    {
        TimestampType t = new TimestampType();
        Object r = ParameterConverter.
            tryToMakeCompatible(TimestampType.class, t);
        assertTrue("expect timestamp", r.getClass() == TimestampType.class);
        assertEquals(t, r);
    }

    public void testStringToVarBinary() throws Exception
    {
        String t = "1E3A";
        Object r = ParameterConverter.
            tryToMakeCompatible(byte[].class, t);
        assertTrue("expect varbinary", r.getClass() == byte[].class);
        assertEquals(t, Encoder.hexEncode((byte[])r));
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

    public void testNulls()
    {
        assertEquals(VoltType.NULL_TINYINT, ParameterConverter.tryToMakeCompatible(byte.class, VoltType.NULL_TINYINT));
        assertEquals(VoltType.NULL_SMALLINT, ParameterConverter.tryToMakeCompatible(short.class, VoltType.NULL_SMALLINT));
        assertEquals(VoltType.NULL_INTEGER, ParameterConverter.tryToMakeCompatible(int.class, VoltType.NULL_INTEGER));
        assertEquals(VoltType.NULL_BIGINT, ParameterConverter.tryToMakeCompatible(long.class, VoltType.NULL_BIGINT));
        assertEquals(VoltType.NULL_FLOAT, ParameterConverter.tryToMakeCompatible(double.class, VoltType.NULL_FLOAT));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(TimestampType.class, VoltType.NULL_TIMESTAMP));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Timestamp.class, VoltType.NULL_TIMESTAMP));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(Date.class, VoltType.NULL_TIMESTAMP));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(java.sql.Date.class, VoltType.NULL_TIMESTAMP));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(String.class, VoltType.NULL_STRING_OR_VARBINARY));
        assertEquals(null, ParameterConverter.tryToMakeCompatible(BigDecimal.class, VoltType.NULL_DECIMAL));
    }
}
