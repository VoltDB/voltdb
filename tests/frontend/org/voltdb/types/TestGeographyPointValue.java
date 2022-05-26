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

package org.voltdb.types;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import junit.framework.TestCase;

public class TestGeographyPointValue extends TestCase {
    // Points should have this much precision.
    private final double EPSILON = 1.0e-14;

    private void assertConstructorThrows(String expectedMessage, double lng, double lat) {
        try {
            new GeographyPointValue(lng, lat);
            fail("Expected constructor to throw an exception");
    }
        catch (IllegalArgumentException iae) {
            assertTrue("Didn't find expected message in exception thrown by PointType constructor",
                    iae.getMessage().contains(expectedMessage));
        }
    }

    public void testPointCtor() {
        assertEquals(16, GeographyPointValue.getLengthInBytes());

        GeographyPointValue point = new GeographyPointValue(10.333, 20.666);
        assertEquals(20.666, point.getLatitude(), EPSILON);
        assertEquals(10.333, point.getLongitude(), EPSILON);

        assertEquals("POINT (10.333 20.666)", point.toString());

        // Make sure that it's not possible to create points
        // with bogus latitude or longitude.
        assertConstructorThrows("Latitude out of bounds", 100, -91.0);
        assertConstructorThrows("Latitude out of bounds", 100, 91.0);
        assertConstructorThrows("Latitude out of bounds", 100, Double.NaN);
        assertConstructorThrows("Longitude out of bounds", 181.0, 45.0);
        assertConstructorThrows("Longitude out of bounds", -181.0, 45.0);
        assertConstructorThrows("Longitude out of bounds", Double.NaN, 45.0);
    }

    public void testPointEquals() {
        // We Points within 12 digits of precision should be considered equal for our purposes.
        final double lessThanEps = 1e-13;
        final double moreThanEps = 2 * GeographyPointValue.EPSILON;

        GeographyPointValue point = new GeographyPointValue(10.333, 20.666);
        GeographyPointValue closePoint = new GeographyPointValue(10.333 + lessThanEps, 20.666 - lessThanEps);
        assertTrue(point.equals(point));
        assertTrue(point.equals(closePoint));
        assertTrue(closePoint.equals(point));

        GeographyPointValue farPoint = new GeographyPointValue(0.0, 10.0);
        assertFalse(point.equals(farPoint));
        assertFalse(farPoint.equals(point));

        // Points at the north (or south) pole should compare equal regardless of longitude.

        GeographyPointValue northPole1 = new GeographyPointValue( 50.0, 90.0);
        GeographyPointValue northPole2 = new GeographyPointValue(-70.0, 90.0);
        GeographyPointValue northPole3 = new GeographyPointValue( 10.0, 90.0 - lessThanEps);
        GeographyPointValue northPole4 = new GeographyPointValue( 180.0, 90.0 - lessThanEps);
        GeographyPointValue northPole5 = new GeographyPointValue( -180.0, 90.0);
        assertTrue(northPole1.equals(northPole2));
        assertTrue(northPole2.equals(northPole1));
        assertTrue(northPole1.equals(northPole3));
        assertTrue(northPole3.equals(northPole1));
        assertTrue(northPole1.equals(northPole4));
        assertTrue(northPole4.equals(northPole1));
        assertTrue(northPole3.equals(northPole5));
        assertTrue(northPole5.equals(northPole3));

        GeographyPointValue notNorthPole = new GeographyPointValue( 10.0, 90.0 - moreThanEps);
        assertFalse(notNorthPole.equals(northPole1));
        assertFalse(northPole1.equals(notNorthPole));

        GeographyPointValue southPole1 = new GeographyPointValue( 50.0, -90.0);
        GeographyPointValue southPole2 = new GeographyPointValue(-70.0, -90.0);
        GeographyPointValue southPole3 = new GeographyPointValue( 10.0, -90.0 + lessThanEps);
        GeographyPointValue southPole4 = new GeographyPointValue( 180.0, -90.0);
        GeographyPointValue southPole5 = new GeographyPointValue( -180.0, -90.0 + lessThanEps);
        assertTrue(southPole1.equals(southPole2));
        assertTrue(southPole2.equals(southPole1));
        assertTrue(southPole1.equals(southPole3));
        assertTrue(southPole3.equals(southPole1));
        assertTrue(southPole3.equals(southPole5));
        assertTrue(southPole5.equals(southPole4));
        assertTrue(southPole3.equals(southPole5));
        assertTrue(southPole1.equals(southPole4));

        GeographyPointValue notSouthPole = new GeographyPointValue( 10.0, -90.0 + moreThanEps);
        assertFalse(notSouthPole.equals(southPole1));
        assertFalse(southPole1.equals(notSouthPole));

        assertFalse(southPole2.equals(northPole2));

        // For a given latitude, points at 180 and -180 lontitude are the same.

        GeographyPointValue onIDLNeg1 = new GeographyPointValue(-180.0              , 37.0);
        GeographyPointValue onIDLNeg2 = new GeographyPointValue(-180.0 + lessThanEps, 37.0);
        GeographyPointValue onIDLPos1 = new GeographyPointValue( 180.0              , 37.0);
        GeographyPointValue onIDLPos2 = new GeographyPointValue( 180.0 - lessThanEps, 37.0);
        assertTrue(onIDLNeg1.equals(onIDLPos1));
        assertTrue(onIDLNeg2.equals(onIDLPos2));
        assertTrue(onIDLNeg1.equals(onIDLPos2));
        assertTrue(onIDLNeg2.equals(onIDLPos1));

        GeographyPointValue notOnIDLNeg = new GeographyPointValue(-180.0 + moreThanEps, 37.0);
        GeographyPointValue notOnIDLPos = new GeographyPointValue( 180.0 - moreThanEps, 37.0);
        assertFalse(onIDLNeg1.equals(notOnIDLPos));
        assertFalse(onIDLPos1.equals(notOnIDLNeg));
    }

    public void testPointSerialization() {

        int len = GeographyPointValue.getLengthInBytes();
        assertEquals(16, len);

        ByteBuffer bb = ByteBuffer.allocate(len);
        bb.putDouble(33.0);
        bb.putDouble(45.0);

        // Test deserialization
        bb.position(0);
        GeographyPointValue pt = GeographyPointValue.unflattenFromBuffer(bb);
        assertEquals("POINT (33.0 45.0)", pt.toString());

        // Test deserialization with offset argument
        bb.position(0);
        pt = GeographyPointValue.unflattenFromBuffer(bb, 0);
        assertEquals("POINT (33.0 45.0)", pt.toString());

        // Test serialization
        pt = new GeographyPointValue(-64.0, -77.0);
        bb.position(0);
        pt.flattenToBuffer(bb);
        bb.position(0);
        assertEquals(-64.0, bb.getDouble());
        assertEquals(-77.0, bb.getDouble());

        // Null serialization puts 360.0 in both lat and long
        bb.position(0);
        GeographyPointValue.serializeNull(bb);
        bb.position(0);
        assertEquals(360.0, bb.getDouble());
        assertEquals(360.0, bb.getDouble());
    }

    /*
     * Note: The parameters are (latitude, longitude), which is the
     *       opposite of order in which we construct points, and
     *       the order used in WKT.  This is to try to test that we
     *       have gotten it right.
     */
    private void testOnePointFromFactory(String aWKT, double aLatitude, double aLongitude, double aEpsilon, String aErrMsg) {
        try {
            GeographyPointValue point = GeographyPointValue.fromWKT(aWKT);
            assertEquals(aLatitude, point.getLatitude(), aEpsilon);
            if (aErrMsg != null) {
                assertTrue(String.format("Expected error message matching \"%s\", but got no error.", aErrMsg), aErrMsg == null);
            }
        } catch (Exception ex) {
            if (aErrMsg != null) {
                assertTrue(String.format("Expected error message matching \"%s\", but got \"%s\"",
                                         aErrMsg, ex.getMessage()),
                           Pattern.matches(aErrMsg, ex.getMessage()));
            } else {
                assertTrue(String.format("Unexpected error message: \"%s\"", ex.getMessage()), false);
            }
        }
    }

    public void testPointFactory() {
        // Note that the WKT strings and the test factory parameters are swapped.
        // This is purposeful.
        testOnePointFromFactory("point(0 0)",                                    0.0,            0.0,          EPSILON, null);
        testOnePointFromFactory("point(10.3330000000 20.6660000000)",           20.6660000000,  10.3330000000, EPSILON, null);
        testOnePointFromFactory("  point  (10.3330000000   20.6660000000)    ", 20.666,         10.333,        EPSILON, null);
        testOnePointFromFactory("point(10.333 20.666)",                         20.666,         10.333,        EPSILON, null);
        testOnePointFromFactory("  point  (10.333   20.666)    ",               20.666,         10.333,        EPSILON, null);
        testOnePointFromFactory("point(-10.333 -20.666)",                      -20.666,        -10.333,        EPSILON, null);
        testOnePointFromFactory("  point  (-10.333   -20.666)    ",            -20.666,        -10.333,        EPSILON, null);
        testOnePointFromFactory("point(10 10)",                                 10.0,           10.0,          EPSILON, null);
        // Test latitude/longitude ranges.
        testOnePointFromFactory("point( 100.0   100.0)", 100.0, 100.0, EPSILON, "Latitude out of bounds: 100.0");
        testOnePointFromFactory("point( 360.0    45.0)", 360.0, 45.0, EPSILON, "Longitude out of bounds: 360.0");
        testOnePointFromFactory("point( 270.0    45.0)", 360.0, 45.0, EPSILON, "Longitude out of bounds: 270.0");
        testOnePointFromFactory("point(-100.0  -100.0)",
                                -100.0,
                                -100.0,
                                EPSILON,
                                "Latitude out of bounds: -100.0");
        testOnePointFromFactory("point(-360.0  -45.0)",
                                -45.0,
                                -360.0,
                                EPSILON,
                                "Longitude out of bounds: -360.0");
        testOnePointFromFactory("point(-270.0  -45.0)",
                                -45.0,
                                -360.0,
                                EPSILON,
                                "Longitude out of bounds: -270.0");
        // Syntax errors
        //   Comma separating the coordinates.
        testOnePointFromFactory("point(0.0, 0.0)",
                                0.0,
                                0.0,
                                EPSILON,
                                "Cannot construct GeographyPointValue value from \"point\\(0[.]0, 0[.]0\\)\"");
    }

    public void checkGeographyPointAdd(double lng1,   double lat1,
                                       double scale,
                                       double lng2,   double lat2,
                                       double lngans, double latans,
                                       double epsilon) {
        GeographyPointValue p1 = GeographyPointValue.normalizeLngLat(lng1, lat1);
        GeographyPointValue p2 = GeographyPointValue.normalizeLngLat(lng2, lat2);
        GeographyPointValue p3 = p1.add(p2, scale);
        assertEquals(lngans, p3.getLongitude(), epsilon);
        assertEquals(latans, p3.getLatitude(), epsilon);
    }

    public void checkGeographyPointSub(double lng1,   double lat1,
                                       double scale,
                                       double lng2,   double lat2,
                                       double lngans, double latans,
                                       double epsilon) {
        GeographyPointValue p1 = GeographyPointValue.normalizeLngLat(lng1, lat1);
        GeographyPointValue p2 = GeographyPointValue.normalizeLngLat(lng2, lat2);
        GeographyPointValue p3 = p1.sub(p2, scale);
        assertEquals(lngans, p3.getLongitude(), epsilon);
        assertEquals(latans, p3.getLatitude(), epsilon);
    }

    public void checkGeographyPointMul(double lng1,   double lat1,
                                       double scale,
                                       double lngans, double latans,
                                       double epsilon) {
        GeographyPointValue p1 = GeographyPointValue.normalizeLngLat(lng1, lat1);
        GeographyPointValue p2 = p1.mul(scale);
        assertEquals(lngans, p2.getLongitude(), epsilon);
        assertEquals(latans, p2.getLatitude(), epsilon);
    }

    public void checkGeographyPointRotate(double lng1, double lat1,
                                          double phi,
                                          double ctrlng, double ctrlat,
                                          double explng, double explat,
                                          double epsilon) {
        GeographyPointValue p1 = GeographyPointValue.normalizeLngLat(lng1, lat1);
        GeographyPointValue ctr = GeographyPointValue.normalizeLngLat(ctrlng, ctrlat);
        GeographyPointValue ans = p1.rotate(phi, ctr);
        assertEquals(explng, ans.getLongitude(), epsilon);
        assertEquals(explat, ans.getLatitude(), epsilon);
    }
    public void testGeographyPointAdd() throws Exception {
        checkGeographyPointAdd(0.0, 1.0, 1.0, 1.0, 1.0, 1.0, 2.0, EPSILON);
        checkGeographyPointAdd(0.0, 1.0, 2.0, 2.0, 4.0, 4.0, 9.0, EPSILON);
    }

    public void testGeographyPointSub() throws Exception {
        checkGeographyPointSub(2.0,  4.0,
                               1.0,
                               0.0,  2.0,
                               2.0,  2.0,
                               EPSILON);
        checkGeographyPointSub(10.0,  20.0,
                                2.0,
                                2.0,  40.0,
                                6.0, -60.0, EPSILON);
    }

    public void testGeographyPointMul() throws Exception {
        checkGeographyPointMul( 10.0, 20.0,
                                 4.0,
                                40.0, 80.0,
                                EPSILON);
        checkGeographyPointMul( 20.0,  80.0,
                                 4.0,
                                80.0, -40.0, EPSILON);
        checkGeographyPointMul( 20.0,   70.0,
                                 4.0,
                                80.0,  -80.0, EPSILON);
    }

    public void testGeographyPointRotate() throws Exception {
        checkGeographyPointRotate(10.0, 20.0, 90, 0.0, 0.0, -20.0, 10.0, EPSILON);
    }
}
