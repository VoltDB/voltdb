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

package org.voltdb.types;

import java.nio.ByteBuffer;
import java.util.regex.Pattern;

import junit.framework.TestCase;

public class TestPointType extends TestCase {
    // Points should have this much precision.
    private final double EPSILON = 1.0e-15;

    private void assertConstructorThrows(String expectedMessage, double lng, double lat) {
        try {
            new PointType(lng, lat);
            fail("Expected constructor to throw an exception");
    }
        catch (IllegalArgumentException iae) {
            assertTrue("Didn't find expected message in exception thrown by PointType constructor",
                    iae.getMessage().contains(expectedMessage));
        }
    }

    public void testPointCtor() {
        assertEquals(16, PointType.getLengthInBytes());

        PointType point = new PointType(20.666, 10.333);
        assertEquals(10.333, point.getLatitude(), EPSILON);
        assertEquals(20.666, point.getLongitude(), EPSILON);

        assertTrue(point.equals(point));
        assertFalse(point.equals(new PointType(10.0, 0.0)));

        assertEquals("POINT (20.666 10.333)", point.toString());

        // Make sure that it's not possible to create points
        // with bogus latitude or longitude.
        assertConstructorThrows("Latitude out of range", 100, -91.0);
        assertConstructorThrows("Latitude out of range", 100, 91.0);
        assertConstructorThrows("Longitude out of range", 181.0, 45.0);
        assertConstructorThrows("Longitude out of range", -181.0, 45.0);
    }

    public void testPointSerialization() {

        int len = PointType.getLengthInBytes();
        assertEquals(16, len);

        ByteBuffer bb = ByteBuffer.allocate(len);
        bb.putDouble(45.0);
        bb.putDouble(33.0);

        // Test deserialization
        bb.position(0);
        PointType pt = PointType.unflattenFromBuffer(bb);
        assertEquals("POINT (45.0 33.0)", pt.toString());

        // Test deserialization with offset argument
        bb.position(0);
        pt = PointType.unflattenFromBuffer(bb, 0);
        assertEquals("POINT (45.0 33.0)", pt.toString());

        // Test serialization
        pt = new PointType(-77.0, -64.0);
        bb.position(0);
        pt.flattenToBuffer(bb);
        bb.position(0);
        assertEquals(-77.0, bb.getDouble());
        assertEquals(-64.0, bb.getDouble());

        // Null serialization puts 360.0 in both lat and long
        bb.position(0);
        PointType.serializeNull(bb);
        bb.position(0);
        assertEquals(360.0, bb.getDouble());
        assertEquals(360.0, bb.getDouble());
    }

    /*
     * Note: The parameters are (latitude, longitude), which is the
     *       opposite of the user in which we construct points, and
     *       the order used in WKT.  This is to try to test that we
     *       have gotten it right.
     */
    private void testOnePointFromFactory(String aWKT, double aLatitude, double aLongitude, double aEpsilon, String aErrMsg) {
        try {
            PointType point = PointType.pointFromText(aWKT);
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
        /*
         * Note: The lsecond and third parameters are latitude and
         *       longitude, in that order.  This is the reverse of
         *       the usual order, in an attempt to try to detect errors.
         */
        testOnePointFromFactory("point(0 0)",                                    0.0,            0.0,          EPSILON, null);
        testOnePointFromFactory("point(20.6660000000 10.3330000000)",           10.3330000000,  20.6660000000, EPSILON, null);
        testOnePointFromFactory("  point  (20.6660000000   10.3330000000)    ", 10.333,         20.666,        EPSILON, null);
        testOnePointFromFactory("point(20.666 10.333)",                         10.333,         20.666,        EPSILON, null);
        testOnePointFromFactory("  point  (20.666   10.333)    ",               10.333,         20.666,        EPSILON, null);
        testOnePointFromFactory("point(-20.666 -10.333)",                      -10.333,        -20.666,        EPSILON, null);
        testOnePointFromFactory("  point  (-20.666   -10.333)    ",            -10.333,        -20.666,        EPSILON, null);
        testOnePointFromFactory("point(10 10)",                                 10.0,           10.0,          EPSILON, null);
        // Test latitude/longitude ranges.
        testOnePointFromFactory("point( 100.0   100.0)", 100.0, 100.0, EPSILON, "Latitude \"100.0+\" out of bounds.");
        testOnePointFromFactory("point( 360.0    45.0)",  45.0, 360.0, EPSILON, "Longitude \"360.0+\" out of bounds.");
        testOnePointFromFactory("point(270.0     45.0)",  45.0, 360.0, EPSILON, "Longitude \"270.0+\" out of bounds.");
        testOnePointFromFactory("point(-100.0  -100.0)",
                                -100.0,
                                -100.0,
                                EPSILON,
                                "Latitude \"-100.0+\" out of bounds.");
        testOnePointFromFactory("point(-360.0   -45.0)",
                                -45.0,
                                -360.0,
                                EPSILON,
                                "Longitude \"-360.0+\" out of bounds.");
        testOnePointFromFactory("point(-270.0   -45.0)",
                                -45.0,
                                -360.0,
                                EPSILON,
                                "Longitude \"-270.0+\" out of bounds.");
        // Syntax errors
        //   Comma separating the coordinates.
        testOnePointFromFactory("point(0.0, 0.0)",
                                0.0,
                                0.0,
                                EPSILON,
                                "Cannot construct PointType value from \"point\\(0[.]0, 0[.]0\\)\"");
    }
}
