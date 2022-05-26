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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.voltdb.messaging.FastSerializer;
import org.voltdb.types.GeographyValue.XYZPoint;

import junit.framework.TestCase;

public class TestGeographyValue extends TestCase {
    private void assertEquals(GeographyPointValue expected, GeographyPointValue actual, double epsilon) {
        String message = String.format("Expected: %s, actual %s", expected, actual);
        assertTrue(message,
                   Math.abs(expected.getLongitude() - actual.getLongitude()) < epsilon
                   && Math.abs(expected.getLatitude() - actual.getLatitude()) < epsilon);
    }

    private static final String WKT = "polygon((0 0, 1 0, 1 1, 0 1, 0 0), (0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1))";
    private void printOneXYZPointForDoc(GeographyPointValue pt) {
        XYZPoint xyzpt = XYZPoint.fromGeographyPointValue(pt);
        System.out.printf("  <tr><td>%f</td><td>%f</td><td>%f</td><td>%f</td><td>%f</td></tr>\n",
                          pt.getLongitude(), pt.getLatitude(),
                          xyzpt.x(), xyzpt.y(), xyzpt.z());
    }
    /*
     * This just prints out the XYZPoint coordinates for a polygon.  We use
     * this in generating the documentation.  Run this as a junit test and the
     * XYZPoint coordinates will be printed on the console as the body of an
     * HTML table.  Note that we don't reverse any order here, as we don't really
     * need to for this application.
     */
    public void notestXYZPointForDoc() {
        GeographyValue poly = GeographyValue.fromWKT(WKT);
        List<List<GeographyPointValue>> rings = poly.getRings();
        for (List<GeographyPointValue> oneRing : rings) {
            for (int idx = 0; idx != oneRing.size()-1; idx += 1) {
                printOneXYZPointForDoc(oneRing.get(idx));
            }
        }
    }

    private void printOneGVRowMessageForDoc(String message) {
        System.out.printf("  <tr><td colspan=\"5\">%s</td></tr>\n", message);
    }

    private int printOneGVRowForDoc(int bytePos, int length, String value, String type, String meaning) {
        System.out.printf("  <tr><td>%d</td><td>%d</td><td>%s</td><td>%s</td><td>%s</td></tr>\n",
                          bytePos,
                          length,
                          value,
                          type,
                          meaning);
        try {
            return meaning.getBytes("UTF-16BE").length;
        } catch (UnsupportedEncodingException ex) {
            return 0;
        }
    }

    private int printOneGVRowForDoc(int bytePos, byte value, String meaning) {
        printOneGVRowForDoc(bytePos, 1, "byte", String.format("%x", value), meaning);
        return 1;
    }

    private int printOneGVRowForDoc(int bytePos, int value, String meaning) {
        printOneGVRowForDoc(bytePos, 4, "32 bit int", String.format("%d", value), meaning);
        return 4;
    }

    private int printOneGVRowForDoc(int bytePos, double value, String meaning) {
        printOneGVRowForDoc(bytePos, 8, "double", String.format("%f", value), meaning);
        return 8;
    }

    private int printOneGVRowOfZerosForDoc(int pos, int length, String meaning) {
        printOneGVRowForDoc(pos, length, "0", "blob of zeros", meaning);
        return length;
    }

    /*
     * Print the wire protocol representation for a polygon.  This is used to generate
     * the wire protocol documentation.
     */
    public void notestGeographyValueForDoc() {
        GeographyValue poly = GeographyValue.fromWKT(WKT);
        ByteBuffer buf = ByteBuffer.allocate(poly.getLengthInBytes());
        poly.flattenToBuffer(buf);
        buf.position(0);
        int pos = 0;
        pos += printOneGVRowForDoc(pos, buf.get(pos), "IsValid.  Initially zero (0)");
        pos += printOneGVRowForDoc(pos, buf.get(pos), "Internal.  Initially one (1)");
        pos += printOneGVRowForDoc(pos, buf.get(pos), "Polygon has holes.");
        int nrings = buf.getInt(pos);
        pos += printOneGVRowForDoc(pos, nrings, "Number of Rings");
        printOneGVRowMessageForDoc("Vertices follow here.");
        for (int ringNo = 0; ringNo < nrings; ringNo += 1) {
            printOneGVRowMessageForDoc(String.format("Ring %d", ringNo + 1));
            pos += printOneGVRowForDoc(pos, buf.get(pos), "Is initialized.  Initially zero (0)");
            int numVerts = buf.getInt(pos);
            pos += printOneGVRowForDoc(pos, numVerts,
                                       String.format("Number Vertices in ring %d", ringNo + 1));
            for (int vertNo = 0; vertNo < numVerts; vertNo += 1) {
                pos += printOneGVRowForDoc(pos, buf.getDouble(pos),
                                           String.format("X Coordinate for ring %d, vertex %d",
                                                         ringNo + 1, vertNo + 1));
                pos += printOneGVRowForDoc(pos, buf.getDouble(pos),
                                           String.format("Y Coordinate for ring %d, vertex %d",
                                                         ringNo + 1, vertNo + 1));
                pos += printOneGVRowForDoc(pos, buf.getDouble(pos),
                                           String.format("Z Coordinate for ring %d, vertex %d",
                                                         ringNo + 1, vertNo + 1));
            }
            pos += printOneGVRowOfZerosForDoc(pos, 38, "Internal plus the bounding box of the ring.  Initially zero (0).");
        }
        pos += printOneGVRowOfZerosForDoc(pos, 33, "Internal fields plus the bounding box of the polygon.  Initially zero(0).");
    }

    public void testGeographyValuePositive() throws IOException {
        GeographyValue geog;
        GeographyValue rtGeog;
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
        List<List<GeographyPointValue>> expectedLol = Arrays.asList(outerLoop, innerLoop);
        geog = new GeographyValue(expectedLol);
        assertEquals("POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066))",
                geog.toString());

        // round trip
        geog = new GeographyValue("POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066,-68.855 25.361, -73.381 28.376, -68.874 28.066))");
        assertEquals("POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066))",
                geog.toString());
        String rtStr = "POLYGON ((0.0 20.0, -17.320508076 -10.0, 17.320508076 -10.0, 0.0 20.0))";
        rtGeog = new GeographyValue(rtStr);
        assertEquals(rtStr, rtGeog.toString());
        assertEquals(rtGeog, new GeographyValue(rtGeog.toWKT()));

        // serialize this.
        ByteBuffer buf = ByteBuffer.allocate(geog.getLengthInBytes());
        geog.flattenToBuffer(buf);
        assertEquals(270, buf.position());

        buf.position(0);

        FastSerializer fs = new FastSerializer();
        geog.serialize(fs);
        ByteBuffer serBuf = fs.getBuffer();
        assertEquals(buf, serBuf);
        fs.discard();

        GeographyValue newGeog = GeographyValue.unflattenFromBuffer(buf);
        assertEquals("POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066))",
                newGeog.toString());
        assertEquals(270, buf.position());

        // Try the absolute version of unflattening
        // Note that the hole's coordinates have been reversed again.
        buf.position(77);
        newGeog = GeographyValue.unflattenFromBuffer(buf, 0);
        assertEquals("POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066))",
                newGeog.toString());
        assertEquals(77, buf.position());

        // Try getting the loops as loops, and see if we get what we put in.
        geog = new GeographyValue(expectedLol);
        final double EPSILON = 1.0e-13;
        List<List<GeographyPointValue>> lol = geog.getRings();
        assertEquals(expectedLol.size(), lol.size());
        for (int oidx = 0; oidx < lol.size(); oidx += 1) {
            List<GeographyPointValue> loop = lol.get(oidx);
            List<GeographyPointValue> expectedLoop = expectedLol.get(oidx);
            assertEquals(expectedLoop.size(), loop.size());
            for (int iidx = 0; iidx < loop.size(); iidx += 1) {
                GeographyPointValue expected = expectedLoop.get(iidx);
                GeographyPointValue actual = loop.get(iidx);
                assertEquals(expected, actual, EPSILON);
            }
        }
    }

    //
    // Test GeographyValue objects which extend over the
    // discontinuities between -180 and 180, and the poles.
    //
    public void testGeographyValueOverDiscontinuities() {
        String geoWKT = "POLYGON ((160.0 40.0, -160.0 40.0, -160.0 60.0, 160.0 60.0, 160.0 40.0))";
        GeographyValue disPoly = GeographyValue.fromWKT(geoWKT);
        assertEquals(geoWKT, disPoly.toString());
        GeographyPointValue offset = new GeographyPointValue(10.0, -10.0);
        GeographyValue disPolyOver = disPoly.add(offset);
        String geoWKTMoved = "POLYGON ((170.0 30.0, -150.0 30.0, -150.0 50.0, 170.0 50.0, 170.0 30.0))";
        assertEquals(geoWKTMoved, disPolyOver.toString());
    }

    public void testGeographyValueNegativeCases() {
        List<GeographyPointValue> outerLoop = new ArrayList<>();
        outerLoop.add(new GeographyPointValue(-64.751, 32.305));
        outerLoop.add(new GeographyPointValue(-80.437, 25.244));
        outerLoop.add(new GeographyPointValue(-66.371, 18.476));
        outerLoop.add(new GeographyPointValue(-76.751, 20.305));
        outerLoop.add(new GeographyPointValue(-64.751, 32.305));
        GeographyValue geoValue;
        // start with valid loop
        geoValue = new GeographyValue(Arrays.asList(outerLoop));
        assertEquals("POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -76.751 20.305, -64.751 32.305))",
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
        String expected = "POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305))";
        assertEquals(expected, canonicalizeWkt("Polygon((-64.751 32.305,-80.437  25.244,-66.371  18.476,-64.751  32.305))"));
        assertEquals(expected, canonicalizeWkt("polygon((-64.751 32.305,-80.437  25.244,-66.371  18.476,-64.751  32.305))"));
        assertEquals(expected, canonicalizeWkt("PoLyGoN((-64.751 32.305,-80.437  25.244,-66.371  18.476,-64.751  32.305))"));

        // Parsing is whitespace-insensitive
        assertEquals(expected, canonicalizeWkt("  POLYGON  (  (   -64.751 32.305  ,   -80.437   25.244  ,   -66.371  18.476  ,   -64.751 32.305   )  ) "));
        assertEquals(expected, canonicalizeWkt("\nPOLYGON\n(\n(\n-64.751\n32.305\n,\n-80.437\n25.244\n,\n-66.371\n18.476\n,-64.751\n32.305\n)\n)\n"));
        assertEquals(expected, canonicalizeWkt("\tPOLYGON\t(\t(\t-64.751\t32.305\t,\t-80.437\t25.244\t,\t-66.371\t18.476\t,\t-64.751\t32.305\t)\t)\t"));

        // Parsing with more than one loop should work the same.
        expected = "POLYGON ((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
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

    public void testAddFunction() {
        String WKT = "POLYGON((3 3, -3 3, -3 -3, 3 -3, 3 3), (1 1, 1 2, 2 1, 1 1), (-1 -1, -1 -2, -2 -1, -1 -1))";
        String WKTOff = "POLYGON ((4.0 5.0, -2.0 5.0, -2.0 -1.0, 4.0 -1.0, 4.0 5.0), (2.0 3.0, 2.0 4.0, 3.0 3.0, 2.0 3.0), (0.0 1.0, 0.0 0.0, -1.0 1.0, 0.0 1.0))";
        GeographyValue gv = GeographyValue.fromWKT(WKT);
        GeographyPointValue gpv = new GeographyPointValue(1, 2);
        GeographyPointValue origin = new GeographyPointValue(0, 0);
        GeographyValue gvoff = gv.add(origin);
        assertEquals(gvoff, gv);
        gvoff = gv.add(gpv);
        assertEquals(GeographyValue.fromWKT(WKTOff), gvoff);
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

    public void testGetValueDisplaySize() {

        // Minumum size of serialized polygon is 155, which would be just
        // three vertices.
        try {
            GeographyValue.getValueDisplaySize(154);
            fail("Expected exception to be thrown");
        }
        catch (IllegalArgumentException iae) {
            assertEquals(iae.getMessage(),
                    "Cannot compute max display size for a GEOGRAPHY value of size 154 bytes, "
                    + "since minimum allowed size is 155");
        }

        // We need a max 120 characters to represent a triangle
        assertEquals(120, GeographyValue.getValueDisplaySize(155));

        // An extra 10 bytes is not enough to represent another vertex, so
        // display size is the same.
        assertEquals(120, GeographyValue.getValueDisplaySize(165));

        // We can fit 4 vertices in 179 bytes.
        assertEquals(120 + 36, GeographyValue.getValueDisplaySize(179));
    }
}
