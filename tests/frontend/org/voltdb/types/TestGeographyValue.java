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
                new PointType(-64.751, 32.305),
                new PointType(-80.437, 25.244),
                new PointType(-66.371, 18.476),
                new PointType(-64.751, 32.305));

        // A triangular hole
        List<PointType> innerLoop = Arrays.asList(
                new PointType(-68.874, 28.066),
                new PointType(-68.855, 25.361),
                new PointType(-73.381, 28.376),
                new PointType(-68.874, 28.066));

        geog = new GeographyValue(Arrays.asList(outerLoop, innerLoop));
        assertEquals("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066))",
                geog.toString());

        // round trip
        geog = new GeographyValue("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066,-68.855 25.361, -73.381 28.376, -68.874 28.066))");
        assertEquals("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066,-68.855 25.361, -73.381 28.376, -68.874 28.066))",
                geog.toString());

        ByteBuffer buf = ByteBuffer.allocate(geog.getLengthInBytes());
        geog.flattenToBuffer(buf);
        assertEquals(270, buf.position());

        buf.position(0);
        GeographyValue newGeog = GeographyValue.unflattenFromBuffer(buf);
        assertEquals("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066))",
                newGeog.toString());
        assertEquals(270, buf.position());

        // Try the absolute version of unflattening
        buf.position(77);
        newGeog = GeographyValue.unflattenFromBuffer(buf, 0);
        assertEquals("POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, 32.305  -64.751, "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066))",
                newGeog.toString());
        assertEquals(77, buf.position());
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
        assertEquals(expected, canonicalizeWkt("  POLYGON  (  (   32.305  -64.751 ,   25.244  -80.437 ,   18.476  -66.371 ,   32.305   -64.751 )  ) "));
        assertEquals(expected, canonicalizeWkt("\nPOLYGON\n(\n(\n-64.751\n32.305\n,\n-80.437\n25.244\n,\n-66.371\n18.476\n,-64.751\n32.305\n)\n)\n"));
        assertEquals(expected, canonicalizeWkt("\tPOLYGON\t(\t(\t-64.751\t32.305\t,\t-80.437\t25.244\t,\t-66.371\t18.476\t,\t-64.751\t32.305\t)\t)\t"));

        // Parsing with more than one loop should work the same.
        expected = "POLYGON((-64.751 32.305, -80.437 25.244, -66.371 18.476, -64.751 32.305), "
                + "(-68.874 28.066, -68.855 25.361, -73.381 28.376, -68.874 28.066 ))";
        assertEquals(expected, canonicalizeWkt("PoLyGoN\t(  (\n-64.751\n32.305   ,    25.244\t-80.437\n, -66.371 18.476,-64.751\t\t\t32.305   ),\t "
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

    public void testWktParsingNegative() {
        assertWktParseError("expected WKT to start with POLYGON", "NOT_A_POLYGON(...)");
        assertWktParseError("expected left parenthesis after POLYGON", "POLYGON []");
        assertWktParseError("missing opening parenthesis", "POLYGON(3 3, 4 4, 5 5, 3 3)");
        assertWktParseError("missing longitude", "POLYGON ((80 80, 60, 70 70, 90 90))");
        assertWktParseError("missing comma", "POLYGON ((80 80 60 60, 70 70, 90 90))");
        assertWktParseError("premature end of input", "POLYGON ((80 80, 60 60, 70 70,");
        assertWktParseError("missing closing parenthesis", "POLYGON ((80 80, 60 60, 70 70, (30 15, 15 30, 15 45)))");
        assertWktParseError("unrecognized token", "POLYGON ((80 80, 60 60, 70 70, 80 80)z)");
        assertWktParseError("unrecognized input after WKT", "POLYGON ((80 80, 60 60, 70 70, 90 90))blahblah");
    }
}
