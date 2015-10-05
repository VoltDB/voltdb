package org.voltdb.types;

import java.util.Arrays;
import java.util.List;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

import junit.framework.TestCase;

public class TestGeographyValue extends TestCase {

    public void testBasic() {
        GeographyValue geog;
        // The Bermuda Triangle
        List<PointType> outerLoop = Arrays.asList(
                new PointType(32.305f, -64.751f),
                new PointType(25.244f, -80.437f),
                new PointType(18.476f, -66.371f),
                new PointType(32.305f, -64.751f));

        // A triangular hole
        List<PointType> innerLoop = Arrays.asList(
                new PointType(28.066f, -68.874f),
                new PointType(25.361f, -68.855f),
                new PointType(28.376f, -73.381f),
                new PointType(28.066f, -68.874f));

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

        // PointType will change size, from (float, float) to (double, double)
        // at that time, we'll need to update this test.
        //
        // 4 (loop count prefix) +
        // 4 (vertex count prefix) + 4 * sizeof(PointType) +
        // 4 (vertex count prefix) + 4 * sizeof(PointType) +
        assertEquals(76, geog.getLengthInBytes());
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

    public void testParseNegative() {
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
