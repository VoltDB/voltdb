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
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

public class TestGeographyValue extends TestCase {

    public void testBasic() {
        GeographyValue geog;
        // The Bermuda Triangle
        List<PointType> outerLoop = Arrays.asList(
                new PointType(32.305, -64.751),
                new PointType(25.244, -80.437),
                new PointType(18.476, -66.371),
                new PointType(32.305, -64.751));

        // A triangular hole
        List<PointType> innerLoop = Arrays.asList(
                new PointType(28.066, -68.874),
                new PointType(25.361, -68.855),
                new PointType(28.376, -73.381),
                new PointType(28.066, -68.874));

        geog = new GeographyValue(Arrays.asList(outerLoop, innerLoop));
        assertEquals("POLYGON((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751), "
                + "(28.066 -68.874, 25.361 -68.855, 28.376 -73.381, 28.066 -68.874))",
                geog.toString());

        // round trip
        geog = new GeographyValue("POLYGON((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751), "
                + "(28.066 -68.874, 25.361 -68.855, 28.376 -73.381, 28.066 -68.874))");
        assertEquals("POLYGON((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751), "
                + "(28.066 -68.874, 25.361 -68.855, 28.376 -73.381, 28.066 -68.874))",
                geog.toString());

        ByteBuffer buf = ByteBuffer.allocate(geog.getLengthInBytes());
        geog.flattenToBuffer(buf);
        assertEquals(270, buf.position());

        buf.position(0);
        GeographyValue newGeog = GeographyValue.unflattenFromBuffer(buf);
        assertEquals("POLYGON((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751), "
                + "(28.066 -68.874, 25.361 -68.855, 28.376 -73.381, 28.066 -68.874))",
                newGeog.toString());
        assertEquals(270, buf.position());

        // Try the absolute version of unflattening
        buf.position(77);
        newGeog = GeographyValue.unflattenFromBuffer(buf, 0);
        assertEquals("POLYGON((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751), "
                + "(28.066 -68.874, 25.361 -68.855, 28.376 -73.381, 28.066 -68.874))",
                newGeog.toString());
        assertEquals(77, buf.position());
    }

    private static String canonicalizeWkt(String wkt) {
        return (new GeographyValue(wkt)).toString();
    }

    public void testWktParsingPositive() {

        // Parsing is case-insensitive
        String expected = "POLYGON((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751))";
        assertEquals(expected, canonicalizeWkt("Polygon((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751))"));
        assertEquals(expected, canonicalizeWkt("polygon((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751))"));
        assertEquals(expected, canonicalizeWkt("PoLyGoN((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751))"));

        // Parsing is whitespace-insensitive
        assertEquals(expected, canonicalizeWkt("  POLYGON  (  (  32.305  -64.751  ,  25.244  -80.437  ,  18.476  -66.371  ,  32.305   -64.751  )  ) "));
        assertEquals(expected, canonicalizeWkt("\nPOLYGON\n(\n(\n32.305\n-64.751\n,\n25.244\n-80.437\n,\n18.476\n-66.371\n,32.305\n-64.751\n)\n)\n"));
        assertEquals(expected, canonicalizeWkt("\tPOLYGON\t(\t(\t32.305\t-64.751\t,\t25.244\t-80.437\t,\t18.476\t-66.371\t,\t32.305\t-64.751\t)\t)\t"));

        // Parsing with more than one loop should work the same.
        expected = "POLYGON((32.305 -64.751, 25.244 -80.437, 18.476 -66.371, 32.305 -64.751), "
                + "(28.066 -68.874, 25.361 -68.855, 28.376 -73.381, 28.066 -68.874))";
        assertEquals(expected, canonicalizeWkt("PoLyGoN\t(  (\n32.305\n-64.751   ,    25.244\t-80.437\n,18.476-66.371,32.305\t\t\t-64.751   ),\t "
                + "(\n28.066-68.874,\t25.361    -68.855\n,28.376      -73.381,28.066\n\n-68.874\t)\n)\t"));
    }

    private void assertWktParseError(String error, String wkt) {
        try {
            new GeographyValue(wkt);
            fail("Expected an expection parsing WKT, but it didn't happen");
        }
        catch (IllegalArgumentException iae) {
            assertTrue("Did not find \n"
                    + "  \"" + error + "\"\n"
                    + "in exception message\n"
                    + "  \"" + iae.getMessage() + "\"\n",
                    iae.getMessage().contains(error));
        }
    }
    /**
     * This tests that the maximum error when we transform a latitude/longitude pair to an
     * S2, 3-dimensinal point and then back again is less than 1.0e-13.
     *
     * We sample the sphere, looking at NUM_PTS X NUM_PTS pairs.  At each pair, <m, n>,
     * we calculate a latitude and longitude, convert the PointType with this latitude and
     * longitude to an XYZPoint, and then back.  We then take the maximum error over the
     * entire sphere.
     *
     * Setting NUM_PTS to 2000000 is a very bad idea.
     *
     * The error bound is 1.0e-13, which is the value of EPSILON below.  We tested 1.0e-14,
     * but that fails.
     *
     * Note that no conversions from text to floating point happen anywhere here, so that
     * is not a source of precision loss.  Only calculation cause these precision losses.
     *
     * @throws Exception
     */
    public void testXYZPoint() throws Exception {
        final double EPSILON = 1.0e-13;
        // We transform from latitude, longitude to XYZ this many
        // times for each point.  This could be set higher.  At about 85, there
        // is more than 1.0e-13 error.  I leave this at 1, because the test
        // time burden is somewhat severe at higher numbers.
        final int    NUMBER_TRANSFORMS = 1;
        // This has been tested at 10000, but it takes too long.
        final int NUM_PTS = 2000;
        final int MIN_PTS = -(NUM_PTS/2);
        final int MAX_PTS = (NUM_PTS/2);
        double max_latitude_error = 0;
        double max_longitude_error = 0;
        for (int ycoord = MIN_PTS; ycoord <= MAX_PTS; ycoord += 1) {
            double latitude = ycoord*(90.0/NUM_PTS);
            for (int xcoord = MIN_PTS; xcoord <= MAX_PTS; xcoord += 1) {
                double longitude = xcoord*(180.0/NUM_PTS);
                PointType PT_point = new PointType(latitude, longitude);
                for (int idx = 0; idx < NUMBER_TRANSFORMS; idx += 1) {
                    GeographyValue.XYZPoint xyz_point = GeographyValue.XYZPoint.fromPointType(PT_point);
                    PT_point = xyz_point.toPointType();
                    double laterr = Math.abs(latitude-PT_point.getLatitude());
                    double lngerr = Math.abs(longitude-PT_point.getLongitude());
                    if (laterr > max_latitude_error) {
                        max_latitude_error = laterr;
                        assertTrue(String.format("Maximum Latitude Error out of range: error=%e >= epsilon = %e, latitude = %f, num_transforms = %d\n",
                                                 max_latitude_error, EPSILON, latitude, idx),
                                max_latitude_error < EPSILON);
                    }
                    if (lngerr > max_longitude_error) {
                        max_longitude_error = lngerr;
                        assertTrue(String.format("Maximum LongitudeError out of range: error=%e >= epsilon = %e, longitude = %f, num_transforms = %d\n",
                                                 max_longitude_error, EPSILON, longitude, idx),
                                   max_longitude_error < EPSILON);
                    }
                }
            }
        }
    }

    public void testWktParsingNegative() {
        assertWktParseError("expected WKT to start with POLYGON", "NOT_A_POLYGON(...)");
        assertWktParseError("expected left parenthesis after POLYGON", "POLYGON []");
        assertWktParseError("missing opening parenthesis", "POLYGON(3 3, 4 4, 5 5, 3 3)");
        assertWktParseError("missing longitude", "POLYGON ((80 80, 60, 70 70, 90 90))");
        assertWktParseError("missing comma", "POLYGON ((80 80 60 60, 70 70, 90 90))");
        assertWktParseError("premature end of input", "POLYGON ((80 80, 60 60, 70 70,");
        assertWktParseError("missing closing parenthesis", "POLYGON ((80 80, 60 60, 70 70, (30 15, 15 30, 15 45)))");
        assertWktParseError("unrecognized token", "POLYGON ((80 80, 60 60, 70 70, 80 80)z)");
        assertWktParseError("unrecognized input after WKT", "POLYGON ((80 80, 60 60, 70 70, 90 90, 80 80))blahblah");
        assertWktParseError("polygon should contain atleast one loop, with each loop containing minimum of 4 vertices - " +
                "start and end vertices being equal", "POLYGON ()");
        assertWktParseError("each loop in polygon should have 4 vertices, with start and end vertices equal",
                            "POLYGON (())");
        assertWktParseError("each loop in polygon should have 4 vertices, with start and end vertices equal",
                            "POLYGON ((10 10, 20 20, 30 30))");
        assertWktParseError("start and end vertices of loop are not equal", "POLYGON ((10 10, 20 20, 30 30, 40 40))");

    }
}
