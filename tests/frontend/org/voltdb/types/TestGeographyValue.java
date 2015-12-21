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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

public class TestGeographyValue extends TestCase {

    public void testGeographyValuePositive() {
        GeographyValue geog;
        // The Bermuda Triangle
        List<GeographyPointValue> outerLoop = Arrays.asList(
                new GeographyPointValue(-64.751, 32.305),
                new GeographyPointValue(-80.437, 25.244),
                new GeographyPointValue(-66.371, 18.476),
                new GeographyPointValue(-64.751, 32.305));

        // A triangular hole
        List<GeographyPointValue> innerLoop = Arrays.asList(
                new GeographyPointValue(-68.874, 28.066),
                new GeographyPointValue(-68.855, 25.361),
                new GeographyPointValue(-73.381, 28.376),
                new GeographyPointValue(-68.874, 28.066));

        geog = new GeographyValue(Arrays.asList(outerLoop, innerLoop));
        assertEquals("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066))",
                geog.toString());

        // round trip
        geog = new GeographyValue("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-73.381 28.376, -68.874 28.066, -68.855 25.361, -73.381 28.376))");
        assertEquals("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-73.381 28.376, -68.874 28.066, -68.855 25.361, -73.381 28.376))",
                geog.toString());

        // serialize this.
        ByteBuffer buf = ByteBuffer.allocate(geog.getLengthInBytes());
        geog.flattenToBuffer(buf);
        assertEquals(270, buf.position());

        buf.position(0);
        GeographyValue newGeog = GeographyValue.unflattenFromBuffer(buf);
        assertEquals("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-73.381 28.376, -68.874 28.066, -68.855 25.361, -73.381 28.376))",
                newGeog.toString());
        assertEquals(270, buf.position());

        // Try the absolute version of unflattening
        // Note that the hole's coordinates have been reversed again.
        buf.position(77);
        newGeog = GeographyValue.unflattenFromBuffer(buf, 0);
        assertEquals("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-73.381 28.376, -68.874 28.066, -68.855 25.361, -73.381 28.376))",
                newGeog.toString());
        assertEquals(77, buf.position());
    }

    public void testGeographyValueNegativeCases() {
        List<GeographyPointValue> outerLoop = new ArrayList<GeographyPointValue>();
        outerLoop.add(new GeographyPointValue(-64.751, 32.305));
        outerLoop.add(new GeographyPointValue(-80.437, 25.244));
        outerLoop.add(new GeographyPointValue(-66.371, 18.476));
        outerLoop.add(new GeographyPointValue(-76.751, 20.305));
        outerLoop.add(new GeographyPointValue(-64.751, 32.305));
        GeographyValue geoValue;
        // start with valid loop
        geoValue = new GeographyValue(Arrays.asList(outerLoop));
        assertEquals("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -76.751 20.305, -64.751 32.305))",
                geoValue.toString());

        Exception exception = null;
        // first and last vertex are not equal in the loop
        outerLoop.remove(outerLoop.size() - 1);
        try {
            geoValue = new GeographyValue(Arrays.asList(outerLoop));
        }
        catch (IllegalArgumentException illegalArgs) {
            exception = illegalArgs;
            assertTrue(exception.getMessage().contains("closing points of ring are not equal"));
        }
        finally {
            assertNotNull(exception);
        }

        // loop has less than 4 vertex
        exception = null;
        outerLoop.remove(outerLoop.size() - 1);
        try {
            geoValue = new GeographyValue(Arrays.asList(outerLoop));
        }
        catch (IllegalArgumentException illegalArgs) {
            exception = illegalArgs;
            assertTrue(exception.getMessage().contains("a polygon ring must contain at least 4 points " +
                        "(including repeated closing vertex"));
        }
        finally {
            assertNotNull(exception);
        }

        // loop is empty
        outerLoop.clear();
        try {
            geoValue = new GeographyValue(Arrays.asList(outerLoop));
        }
        catch (IllegalArgumentException illegalArgs) {
            exception = illegalArgs;
            assertTrue(exception.getMessage().contains("a polygon ring must contain at least 4 points " +
                        "(including repeated closing vertex"));
        }
        finally {
            assertNotNull(exception);
        }
    }

    private static String canonicalizeWkt(String wkt) {
        return (new GeographyValue(wkt)).toString();
    }

    public void testWktParsingPositive() {

        // Parsing is case-insensitive
        String expected = "POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305))";
        assertEquals(expected, canonicalizeWkt("Polygon((-64.751 32.305,-80.437  25.244,-66.371  18.476,-64.751  32.305))"));
        assertEquals(expected, canonicalizeWkt("polygon((-64.751 32.305,-80.437  25.244,-66.371  18.476,-64.751  32.305))"));
        assertEquals(expected, canonicalizeWkt("PoLyGoN((-64.751 32.305,-80.437  25.244,-66.371  18.476,-64.751  32.305))"));

        // Parsing is whitespace-insensitive
        assertEquals(expected, canonicalizeWkt("  POLYGON  (  (   -64.751 32.305  ,   -80.437   25.244  ,   -66.371  18.476  ,   -64.751 32.305   )  ) "));
        assertEquals(expected, canonicalizeWkt("\nPOLYGON\n(\n(\n-64.751\n32.305\n,\n-80.437\n25.244\n,\n-66.371\n18.476\n,-64.751\n32.305\n)\n)\n"));
        assertEquals(expected, canonicalizeWkt("\tPOLYGON\t(\t(\t-64.751\t32.305\t,\t-80.437\t25.244\t,\t-66.371\t18.476\t,\t-64.751\t32.305\t)\t)\t"));

        // Parsing with more than one loop should work the same.
        expected = "POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066))";
        assertEquals(expected, canonicalizeWkt("PoLyGoN\t(  (\n-64.751\n32.305   ,    -80.437\t25.244\n, -66.371 18.476,-64.751\t\t\t32.305   ),\t "
                + "(\n-68.874 28.066,\t    -68.855\n25.361\n,      -73.381\t28.376,\n\n-68.874\t28.066\t)\n)\t"));
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
     * we calculate a latitude and longitude, convert the GeographyPointValue with this latitude and
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
                GeographyPointValue PT_point = new GeographyPointValue(longitude, latitude);
                for (int idx = 0; idx < NUMBER_TRANSFORMS; idx += 1) {
                    GeographyValue.XYZPoint xyz_point = GeographyValue.XYZPoint.fromGeographyPointValue(PT_point);
                    PT_point = xyz_point.toGeographyPointValue();
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
        assertWktParseError("missing latitude", "POLYGON ((80 80, 60, 70 70, 90 90))");
        assertWktParseError("missing comma", "POLYGON ((80 80 60 60, 70 70, 90 90))");
        assertWktParseError("premature end of input", "POLYGON ((80 80, 60 60, 70 70,");
        assertWktParseError("missing closing parenthesis", "POLYGON ((80 80, 60 60, 70 70, (30 15, 15 30, 15 45)))");
        assertWktParseError("unrecognized token", "POLYGON ((80 80, 60 60, 70 70, 80 80)z)");
        assertWktParseError("unrecognized input after WKT", "POLYGON ((80 80, 60 60, 70 70, 90 90, 80 80))blahblah");
        assertWktParseError("a polygon must contain at least one ring " +
                            "(with each ring at least 4 points, including repeated closing vertex)",
                            "POLYGON ()");
        assertWktParseError("a polygon ring must contain at least 4 points " +
                            "(including repeated closing vertex)",
                            "POLYGON (())");
        assertWktParseError("a polygon ring must contain at least 4 points " +
                            "(including repeated closing vertex)",
                            "POLYGON ((10 10, 20 20, 30 30))");
        assertWktParseError("closing points of ring are not equal", "POLYGON ((10 10, 20 20, 30 30, 40 40))");

    }
}
