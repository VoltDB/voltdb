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

package org.voltdb.regressionsuites;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

public class TestGeospatialFunctions extends RegressionSuite {
    /*
     * Distances are within these tolerances.
     */
    public final double GEOGRAPHY_DISTANCE_EPSILON = 1.0e-8;

    public TestGeospatialFunctions(String name) {
        super(name);
    }

    static private void setUpSchema(VoltProjectBuilder project) throws IOException {
        String literalSchema =
                "CREATE TABLE places (\n"
                + "  pk         INTEGER NOT NULL PRIMARY KEY,\n"
                + "  name       VARCHAR(64),\n"
                + "  loc        GEOGRAPHY_POINT\n"
                + ");\n"
                + "CREATE TABLE borders (\n"
                + "  pk         INTEGER NOT NULL PRIMARY KEY,\n"
                + "  name       VARCHAR(64),\n"
                + "  message    VARCHAR(64),\n"
                + "  region     GEOGRAPHY,\n"
                + "  isvalid    TINYINT,\n"
                + "  isfixable  TINYINT\n"
                + ");\n"
                + "\n"
                ;
        project.addLiteralSchema(literalSchema);
    }

    /*
     * We want to store the borders table once and for all, and insert and test
     * without repeating ourselves. This class holds geometry values for us for
     * inserting and for testing later on.
     *
     * The message is for holding error messages.  It is inserted into the
     * table.
     */
    static class Border {
        Border(long pk,
               String name,
               String message,
               GeographyValue region,
               boolean isValid,
               boolean isFixable) {
            m_pk = pk;
            m_name = name;
            m_region = region;
            m_message = message;
            m_valid = isValid;
            m_fixable = isFixable;
            // m_message must not be null, unless m_region is also null.
            // and if m_region is null then m_message must be null.
            // The message may be "Valid Polygon", or it may be a
            // regexp pattern for an error message from isinvalidreason().
            assertTrue((message == null) == (region == null));
        }

        public final long getPk() {
            return m_pk;
        }

        public final String getName() {
            return m_name;
        }

        public final GeographyValue getRegion() {
            return m_region;
        }

        public final String getMessage() {
            return m_message;
        }

        public final boolean isFixable() {
            return m_fixable;
        }

        public final boolean isValid() {
            return m_valid;
        }

        private final long m_pk;
        private final String m_name;
        private final GeographyValue m_region;
        /**
         * This is the expected message when we don't try to repair the
         * polygon.
         */
        private final String m_message;
        /**
         * This is true iff this polygon is valid.
         */
        private final boolean m_valid;
        /**
         * This is the expected message when we do try to repair the polygon,
         * but we find that we cannot.  For example, we can't repair a polygon
         * whose edges cross.  This may be null.
         *
         * The vertices may be reordered, so the edges may have different indices.
         */
        private final boolean m_fixable;
    }

    /*
     *   X      X
     *   |\    /|
     *   | \  / |
     *   |  \/  |
     *   |  /\  |
     *   | /  \ |
     *   |/    \|
     *   X      X
     */
    final private static String CROSSED_EDGES
      = "POLYGON((0 0, 0 1, 1 0, 1 1, 0 0))";

    /*
     *  X----------->X
     *  ^            |
     *  |            |
     *  |            |
     *  |            |
     *  |            V
     *  X<-----------X
     */
    final private static String CW_EDGES
      = "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0))";
    /*
     *             X----------X
     *             |          |
     *             |          |
     *  X----------X----------X
     *  |          |
     *  |          |
     *  X----------X
     *
     */
    final private static String MULTI_POLYGON
      = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (0 0, 0 -1, -1 -1, -1 0, 0 0))";
    /*
     *
     *  X------------------------------X
     *  | X------------X--------------X|
     *  | |            |              ||
     *  | |            |              ||
     *  | |            |              ||
     *  | X------------X--------------X|
     *  X------------------------------X
     */
    final private static String SHARED_INNER_VERTICES
        = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (.1 .1, .1 .9, .5 .9, .9 .1, .1 .1), (.5 .1, .5 .9, .9 .9, .9 .1, .5 .1))";
    /*
     *
     *  X------------------------------X
     *  | X-----------X                |
     *  | |           X---------------X|
     *  | |           |               ||
     *  | |           X---------------X|
     *  | X-----------X                |
     *  X------------------------------X
     */
    final private static String SHARED_INNER_EDGES
      = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (.1 .1, .1 .9, .5 .9, .5 .1, .1 .1), (.5 .2, .5 .8, .9 .8, .9 .2, .5 .2))";

    // The collinear polygons are currently legal.  But they should not be.
    // So we are going to leave them here until we can figure out what to do
    // with them.
    /*
     * X-----X-----X
     */
    @SuppressWarnings("unused")
    final private static String COLLINEAR3
      = "POLYGON((0 0, 1 0 , 2 0 , 0 0))";
    /*
     * X-----X-----X-----X
     */
    @SuppressWarnings("unused")
    final private static String COLLINEAR4
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 0 0))";
    /*
     * X-----X-----X-----X----X
     */
    @SuppressWarnings("unused")
    final private static String COLLINEAR5
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X
     */
    @SuppressWarnings("unused")
    final private static String COLLINEAR6
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X----X
     */
    @SuppressWarnings("unused")
    final private static String COLLINEAR7
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 6 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X----X----X
     */
    @SuppressWarnings("unused")
    final private static String COLLINEAR8
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 6 0, 7 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X----X----X----X
     */
    @SuppressWarnings("unused")
    final private static String COLLINEAR9
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 6 0, 7 0, 8 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X----X----X----X----X
     */
    @SuppressWarnings("unused")
    final private static String COLLINEAR10
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 6 0, 7 0, 8 0, 9 0, 0 0))";
    /*
     * It's hard to draw this with ascii art, but the outer shell
     * is a rectangle, and the would-be holes are triangles.  The
     * first one, (33 67, 67 67, 50 33, 33 67), points down, and
     * the second one, (33, 50 67, 67 33, 33 33), points up.  These
     * two intersect, which is why this is not valid.
     */
    final private static String INTERSECTING_HOLES
        = "POLYGON((0 0, 80 0, 80 80, 0 80, 0 0),"
               +  "(33 67, 67 67, 50 33, 33 67),"
               +  "(33 33, 50 67, 67 33, 33 33))";
    /*
     * It's not easy to see this in ascii art, but the hole
     * here leaks out of the shell to the top and right.
     */
    final private static String OUTER_INNER_INTERSECT
       = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0),"
              +  "(.1 .1, .1 1.1, 1.1 1.1, 1.1 .1, .1 .1)"
              + ")";

    /*
     * For these loops, only one coordinate changes at a time, either the
     * X or Y coordinate.  If the changes are Y X Y X then the loop
     * is Sunwise (or CW).  If the changes are X Y X Y then the
     * loop is Widdershins (or CCW).
     *
     * This is a single Sunwise (CW) loop.  It needs to be inverted.
     */
    final private static String ONE_SUNWISE_LOOP
        = "POLYGON((0.0 0.0, 0.0 1.0, 1.0 1.0, 1.0 0.0, 0.0 0.0))"; // Changes: Y X Y X (CW)
    /*
     * These are two nested Clockwise (Sunwise) rectangles.  The
     * outer loop must be inverted, but the inner one is ok.
     */
    final private static String TWO_NESTED_SUNWISE_IN_SUNWISE
       = "POLYGON((0.0 0.0, 0.0 1.0, 1.0 1.0, 1.0 0.0, 0.0 0.0),"  // Changes: Y X Y X (CW)
              +  "(0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1)"   // Changes: Y X Y X (CW)
              + ")";

    /*
     * This is a Widdershins loop nested in a Sunwise loop.  Both
     * the outer and inner loops must be inverted.
     */
    final private static String TWO_NESTED_WIDDERSHINS_IN_SUNWISE
       = "POLYGON((0.0 0.0, 0.0 1.0, 1.0 1.0, 1.0 0.0, 0.0 0.0),"   // Changes: Y X Y X (CW)
              +  "(0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1)"    // Changes: X Y X Y (CCW)
              + ")";

    /*
     * This is a Sunwise loop nested in a Widdershins loop.  This
     * is exactly right, and nothing needs to be inverted.
     */
    final private static String TWO_NESTED_SUNWISE_IN_WIDDERSHINS
       = "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0),"  // Changes: X Y X Y (CCW)
              +  "(0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1)"   // Changes: Y X Y X (CW)
              + ")";

    /*
     * This is a Widdershins loop nested in another Widdershins
     * loop. The outer loop is ok but the inner loop needs to be
     * inverted.
     */
    final private static String TWO_NESTED_WIDDERSHINS_IN_WIDDERSHINS
       = "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0)," // Changes: X Y X Y (CCW)
              +  "(0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1)"  // Changes: X Y X Y (CCW)
              + ")";

    /*
     * One CCW (Widdershins) shell and two CCW (Widdershins inner
     * loops.  These are the wrong orientation for holes, and they
     * will be repaired.
     */
    final private static String TWO_WIDDERSHINS_HOLES_IN_WIDDERSHINS
      = "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0),"  // Changes: X Y X Y (CCW)
           +  "(0.1 0.1, 0.9 0.1, 0.9 0.4, 0.1 0.4, 0.1 0.1),"    // Changes: X Y X Y (CCW)
           +  "(0.1 0.6, 0.9 0.6, 0.9 0.9, 0.1 0.9, 0.1 0.6)"     // Changes: X Y X Y (CCW)
           + ")";

    final private static String ISLAND_IN_A_LAKE
    = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),"  // This is CCW
            + "(1 1,  1 9,  9  9, 9  1, 1 1),"  // This is CW.
            + "(2 2,  2 8,  8  8, 8  2, 2 2)"   // This is CW.
            + ")";


    /*
     * This is the array of borders we know about.  All the
     * borders are here, even invalid and unfixable ones.  The
     * user of this array needs to know which are which.
     */
    static Border borders[] = {
        new Border(0, "Colorado", "Valid Polygon",
                   GeographyValue.fromWKT(
                           "POLYGON(("
                                   + "-102.052 41.002, "
                                   + "-109.045 41.002,"
                                   + "-109.045 36.999,"
                                   + "-102.052 36.999,"
                                   + "-102.052 41.002))"),
                   true,
                   true
                   ),
        new Border(1, "Wyoming",
                   "Valid Polygon",
                   new GeographyValue("POLYGON(("
                                      + "-104.061 44.978, "
                                      + "-111.046 44.978, "
                                      + "-111.046 40.998, "
                                      + "-104.061 40.998, "
                                      + "-104.061 44.978))"),
                   true,
                   true),
       new Border(2, "Colorado with a hole around Denver",
                  "Valid Polygon",
                  new GeographyValue("POLYGON("
                                  + "(-102.052 41.002, "
                                  + "-109.045 41.002,"
                                  + "-109.045 36.999,"
                                  + "-102.052 36.999,"
                                  + "-102.052 41.002), "
                                  + "(-104.035 40.240, "
                                  + "-104.035 39.188,"
                                  + "-105.714 39.188, "
                                  + "-105.714 40.240, "
                                  + "-104.035 40.240))"),
                  true,
                  true),
       // Wonderland is valid here.  It's not going to be
       // flagged as an invalid polygon, though isvalid(),
       // isinvalidreason() and makevalidpolygon() will all
       // return NULL for it.
       new Border(3, "Wonderland", null, null, true, true),
       new Border(100, "CrossedEdges", "Edges 1 and 3 cross",
                  GeographyValue.fromWKT(CROSSED_EDGES),
                  false, false),
       new Border(102, "MultiPolygon", "Polygons can have only one shell",
                  GeographyValue.fromWKT(MULTI_POLYGON),
                  false, false),
       new Border(103, "SharedInnerVertices", "Ring 1 crosses ring 2",
                  GeographyValue.fromWKT(SHARED_INNER_VERTICES),
                  false, false),
       new Border(104, "SharedInnerEdges", "Ring 1 crosses ring 2",
                  GeographyValue.fromWKT(SHARED_INNER_EDGES),
                  false, false),
       new Border(105, "IntersectingHoles", "Ring 1 crosses ring 2",
                  GeographyValue.fromWKT(INTERSECTING_HOLES),
                  false, false),
       new Border(106, "OuterInnerIntersect", "Ring 0 crosses ring 1",
                  GeographyValue.fromWKT(OUTER_INNER_INTERSECT),
                  false, false),
       new Border(107,
                  "IslandInALake", "Polygons can only be shells or holes",
                  GeographyValue.fromWKT(ISLAND_IN_A_LAKE),
                  false, false),
       // We fix these up.  So it's an undetected error to send these
       // by object.  Sending these by validpolygonfromtext causes
       // a SQL exception.
       new Border(200,
                  "Sunwise Loop",
                  "Ring 0 encloses more than half the sphere",
                  GeographyValue.fromWKT(ONE_SUNWISE_LOOP),
                  false,
                  true),
       new Border(201,
                  "Sunwise in Sunwise",
                  "Ring 0 encloses more than half the sphere",
                  GeographyValue.fromWKT(TWO_NESTED_SUNWISE_IN_SUNWISE),
                  false,
                  true),
       new Border(202,
                  "Sunwise in Widdershins",
                  "Valid Polygon",
                  GeographyValue.fromWKT(TWO_NESTED_SUNWISE_IN_WIDDERSHINS),
                  true,
                  true),
       new Border(203,
                  "Widdershins in Sunwise",
                  "Ring 0 encloses more than half the sphere",
                  GeographyValue.fromWKT(TWO_NESTED_WIDDERSHINS_IN_SUNWISE),
                  false,
                  true),
       new Border(204,
                  "Widdershins in Widdershins",
                  "Ring 1 encloses more than half the sphere",
                  GeographyValue.fromWKT(TWO_NESTED_WIDDERSHINS_IN_WIDDERSHINS),
                  false,
                  true),
       new Border(205,
                  "Two Widdershins Holes in a Widdershins shell.",
                  "Ring 1 encloses more than half the sphere",
                  GeographyValue.fromWKT(TWO_WIDDERSHINS_HOLES_IN_WIDDERSHINS),
                  false,
                  true),
    };

    private static void populateBorders(Client client, Border borders[]) throws NoConnectionsException, IOException, ProcCallException {
        for (Border b : borders) {
            client.callProcedure("borders.Insert",
                                 b.getPk(),
                                 b.getName(),
                                 b.getMessage(),
                                 b.getRegion(),
                                 b.isValid() ? 1 : 0,
                                 b.isFixable() ? 1 : 0);
        }
    }

    private static void populateTables(Client client, boolean doBorders) throws NoConnectionsException, IOException, ProcCallException {
        // Note: These are all WellKnownText strings.  So they should
        //       be "POINT(...)" and not "GEOGRAPHY_POINT(...)".
        client.callProcedure("places.Insert", 0, "Denver",
                GeographyPointValue.fromWKT("POINT(-104.959 39.704)"));
        client.callProcedure("places.Insert", 1, "Albuquerque",
                GeographyPointValue.fromWKT("POINT(-106.599 35.113)"));
        client.callProcedure("places.Insert", 2, "Cheyenne",
                GeographyPointValue.fromWKT("POINT(-104.813 41.134)"));
        client.callProcedure("places.Insert", 3, "Fort Collins",
                GeographyPointValue.fromWKT("POINT(-105.077 40.585)"));
        client.callProcedure("places.Insert", 4, "Point near N Colorado border",
                GeographyPointValue.fromWKT("POINT(-105.04 41.002)"));
        client.callProcedure("places.Insert", 5, "North Point Not On Colorado Border",
                GeographyPointValue.fromWKT("POINT(-109.025 41.005)"));
        client.callProcedure("places.Insert", 6, "Point on N Wyoming Border",
                GeographyPointValue.fromWKT("POINT(-105.058 44.978)"));
        client.callProcedure("places.Insert", 7, "North Point Not On Wyoming Border",
                GeographyPointValue.fromWKT("POINT(-105.060 45.119)"));
        client.callProcedure("places.Insert", 8, "Point on E Wyoming Border",
                GeographyPointValue.fromWKT("POINT(-104.078 42.988)"));
        client.callProcedure("places.Insert", 9, "East Point Not On Wyoming Border",
                GeographyPointValue.fromWKT("POINT(-104.061 42.986)"));
        client.callProcedure("places.Insert", 10, "Point On S Wyoming Border",
                GeographyPointValue.fromWKT("POINT(-110.998 41.099)"));
        client.callProcedure("places.Insert", 11, "South Point Not On Colorado Border",
                GeographyPointValue.fromWKT("POINT(-103.008 37.002)"));
        client.callProcedure("places.Insert", 12, "Point On W Wyoming Border",
                GeographyPointValue.fromWKT("POINT(-110.998 42.999)"));
        client.callProcedure("places.Insert", 13, "West Point Not on Wyoming Border",
                GeographyPointValue.fromWKT("POINT(-111.052 41.999)"));

        // A null-valued point
        client.callProcedure("places.Insert", 99, "Neverwhere", null);

        // Just populate them all.  The callers may need
        // to filter out the ones they don't want.
        if (doBorders) {
            populateBorders(client, borders);
        }
    }

    public void testContains() throws Exception {
        Client client = getClient();

        populateTables(client, true);

        // Fetch out the pairs of places and borders where the
        // places are in the borders.  Ignore the invalid borders.
        VoltTable vt = client.callProcedure("@AdHoc",
                "select places.name || ', ' || borders.name "
                + "from places, borders "
                + "where borders.pk < 100 and contains(borders.region, places.loc) "
                + "order by places.pk, borders.pk").getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Denver, Colorado"},
                 {"Cheyenne, Wyoming"},
                 {"Fort Collins, Colorado"},
                 {"Fort Collins, Colorado with a hole around Denver"},
                 {"Point near N Colorado border, Colorado"},
                 {"Point near N Colorado border, Colorado with a hole around Denver"},
                 {"Point on N Wyoming Border, Wyoming"},
                 {"Point on E Wyoming Border, Wyoming"},
                 {"Point On S Wyoming Border, Wyoming"},
                 {"Point On W Wyoming Border, Wyoming"},
                }, vt);

        // Fetch out the pairs of places and borders where the
        // places are not in the borders.  Ignore the invalid borders,
        // and the ones used only for testing validity.
        vt = client.callProcedure("@AdHoc", "select places.name, borders.name "
                + "from places, borders "
                + "where borders.pk < 100 and not contains(borders.region, places.loc) "
                + "order by places.pk, borders.pk").getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Denver",                             "Wyoming"},
                 {"Denver",                             "Colorado with a hole around Denver"},
                 {"Albuquerque",                        "Colorado"},
                 {"Albuquerque",                        "Wyoming"},
                 {"Albuquerque",                        "Colorado with a hole around Denver"},
                 {"Cheyenne",                           "Colorado"},
                 {"Cheyenne",                           "Colorado with a hole around Denver"},
                 {"Fort Collins",                       "Wyoming"},
                 {"Point near N Colorado border",       "Wyoming"},
                 {"North Point Not On Colorado Border", "Colorado"},
                 {"North Point Not On Colorado Border", "Wyoming"},
                 {"North Point Not On Colorado Border", "Colorado with a hole around Denver"},
                 {"Point on N Wyoming Border",          "Colorado"},
                 {"Point on N Wyoming Border",          "Colorado with a hole around Denver"},
                 {"North Point Not On Wyoming Border",  "Colorado"},
                 {"North Point Not On Wyoming Border",  "Wyoming"},
                 {"North Point Not On Wyoming Border",  "Colorado with a hole around Denver"},
                 {"Point on E Wyoming Border",          "Colorado"},
                 {"Point on E Wyoming Border",          "Colorado with a hole around Denver"},
                 {"East Point Not On Wyoming Border",   "Colorado"},
                 {"East Point Not On Wyoming Border",   "Wyoming"},
                 {"East Point Not On Wyoming Border",   "Colorado with a hole around Denver"},
                 {"Point On S Wyoming Border",          "Colorado"},
                 {"Point On S Wyoming Border",          "Colorado with a hole around Denver"},
                 {"South Point Not On Colorado Border", "Colorado"},
                 {"South Point Not On Colorado Border", "Wyoming"},
                 {"South Point Not On Colorado Border", "Colorado with a hole around Denver"},
                 {"Point On W Wyoming Border",          "Colorado"},
                 {"Point On W Wyoming Border",          "Colorado with a hole around Denver"},
                 {"West Point Not on Wyoming Border",   "Colorado"},
                 {"West Point Not on Wyoming Border",   "Wyoming"},
                 {"West Point Not on Wyoming Border",   "Colorado with a hole around Denver"}
                }, vt);

    }

    public void testPolygonInteriorRings() throws Exception {
        Client client = getClient();
        populateTables(client, true);

        String sql = "select borders.name,  numInteriorRing(borders.region) "
                        + "from borders where borders.pk < 100 order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Colorado",                           0},
                 {"Wyoming",                            0},
                 {"Colorado with a hole around Denver", 1},
                 {"Wonderland",                         Integer.MIN_VALUE},
                }, vt);
    }

    public void testNullPolygonFunctions() throws Exception {
        Client client = getClient();
        populateTables(client, true);
        // All the polygon functions should return NULL.
        String sql = "select "
                + "  borders.name, "
                + "  numPoints(region), "
                + "  case when isvalid(region) then 1 else 0 end, "
                + "  makevalidpolygon(region), "
                + "  isinvalidreason(region) "
                + "from borders "
                + "where borders.pk < 100 and borders.region is NULL "
                + "order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Wonderland",
                    null,
                    0,
                    null,
                    null
                }}, vt);
    }

    public void testPolygonNumberOfPoints() throws Exception {
        Client client = getClient();
        populateTables(client, true);

        // polygon with no holes has exterior ring.
        // number of points will be number of them on the exterior ring
        String sql = "select borders.name, numPoints(borders.region) from borders "
                        + "where borders.pk < 100 and numInteriorRing(borders.region) = 0 "
                        + "order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Colorado",   5},
                 {"Wyoming",    5}
                }, vt);

        // polygon with holes will atleast exterior and interior ring
        // number of points will be sum of points on interior and exterior ring
        // query uses alias function numinteriorrings
        sql = "select borders.name, numPoints(borders.region) from borders "
                + "where borders.pk < 100 and numInteriorRings(borders.region) = 1 "
                + "order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Colorado with a hole around Denver", 10}
                }, vt);

    }

    public void testLongitudeLatitude() throws Exception {
        Client client = getClient();
        populateTables(client, true);

        String sql = "select places.name, LONGITUDE(places.loc), LATITUDE(places.loc) "
                        + "from places order by places.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
            {{"Denver",                             -104.959, 39.704},
             {"Albuquerque",                        -106.599, 35.113},
             {"Cheyenne",                           -104.813, 41.134},
             {"Fort Collins",                       -105.077, 40.585},
             {"Point near N Colorado border",       -105.04,  41.002},
             {"North Point Not On Colorado Border", -109.025, 41.005},
             {"Point on N Wyoming Border",          -105.058, 44.978},
             {"North Point Not On Wyoming Border",  -105.06 , 45.119},
             {"Point on E Wyoming Border",          -104.078, 42.988},
             {"East Point Not On Wyoming Border",   -104.061, 42.986},
             {"Point On S Wyoming Border",          -110.998, 41.099},
             {"South Point Not On Colorado Border", -103.008, 37.002},
             {"Point On W Wyoming Border",          -110.998, 42.999},
             {"West Point Not on Wyoming Border",   -111.052, 41.999},
             {"Neverwhere",                          null,    null},
            }, vt);

        sql = "select places.name, LONGITUDE(places.loc), LATITUDE(places.loc) "
                + "from places, borders "
                + "where borders.pk < 100 and contains(borders.region, places.loc) "
                + "group by places.name, places.loc "
                + "order by places.name";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
            {{"Cheyenne",                       -104.813, 41.134 },
             {"Denver",                         -104.959, 39.704 },
             {"Fort Collins",                   -105.077, 40.585},
             {"Point On S Wyoming Border",      -110.998, 41.099},
             {"Point On W Wyoming Border",      -110.998, 42.999},
             {"Point near N Colorado border",   -105.04, 41.002},
             {"Point on E Wyoming Border",      -104.078, 42.988},
             {"Point on N Wyoming Border",      -105.058, 44.978},
            }, vt);
    }

    public void testPolygonFloatingPrecision() throws Exception {
        final double EPSILON = -1.0;
        Client client = getClient();
        populateTables(client, true);

        String sql = "select name, region "
                        + "from borders where borders.pk < 100 order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        //
        // Note that there is some collusion here.  The first few
        // elements of the borders table contain all the valid
        // polygons.
        //
        assertApproximateContentOfTable(new Object[][]
                                        {{borders[0].getName(), borders[0].getRegion()},
                                         {borders[1].getName(), borders[1].getRegion()},
                                         {borders[2].getName(), borders[2].getRegion()},
                                         {borders[3].getName(), borders[3].getRegion()}},
                                        vt,
                                        EPSILON);
    }

    public void testPolygonCentroidAndArea() throws Exception {
        // The AREA_EPSILON here is 1.0e-1, because the values are in the range
        // 1.0e11, and we expect 1.0e12 precision.
        final double AREA_EPSILON=1.0e-1;
        Client client = getClient();
        populateTables(client, true);

        String sql = "select borders.name, Area(borders.region) "
                        + "from borders where borders.pk < 100 order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        // in the calculation below, areas of states are close to actual area of the state (vertices
        // used for polygon are close approximations, not exact, values of the state vertices).
        // Area for Colorado - 269601 sq km and Wyoming 253350 sq km
        assertApproximateContentOfTable(new Object[][]
                {{ "Colorado",      2.6886220370448795E11},
                 { "Wyoming",       2.512656175743851E11},
                 { "Colorado with a hole around Denver",
                                    2.5206301526291238E11},
                 { "Wonderland",    null},
                }, vt, AREA_EPSILON);
        // Test the centroids.  For centroid, the value in table is based on the answer provide by S2 for the given polygons
        // The CENTROID relative precision is greater than the AREA relative precision.
        final double CENTROID_EPSILON=1.0e-12;
        sql = "select borders.name, LATITUDE(centroid(borders.region)), LONGITUDE(centroid(borders.region)) "
                + "from borders where borders.pk < 100 order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                {{ "Colorado",      39.03372408765194,      -105.5485 },
                 { "Wyoming",       43.01953179182205,      -107.55349999999999 },
                 { "Colorado with a hole around Denver",
                                    38.98811213712535,      -105.5929789796371 },
                 { "Wonderland",    null,                    null},
                }, vt, CENTROID_EPSILON);

        sql = "select borders.name, LATITUDE(centroid(borders.region)), LONGITUDE(centroid(borders.region)) "
                + "from borders "
                + "where borders.pk < 100 and borders.region is not null "
                + "order by borders.pk ";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                {{ "Colorado",      39.03372408765194,      -105.5485 },
                 { "Wyoming",       43.01953179182205,      -107.55349999999999 },
                 { "Colorado with a hole around Denver",
                                    38.98811213712535,      -105.5929789796371 }
                }, vt, CENTROID_EPSILON);
    }

    public void testPolygonPointDistance() throws Exception {
        // The distances we consider are all in the thousands of
        // meters.  We expect 1.0e-12 precision, so that's 1.0e-8 relative
        // precision.  Note that we have determined empirically that
        // 1.0e-9 fails.
        final double DISTANCE_EPSILON = 1.0e-8;
        Client client = getClient();
        populateTables(client, true);

        client.callProcedure("places.Insert", 50, "San Jose",
                GeographyPointValue.fromWKT("POINT(-121.903692 37.325464)"));
        client.callProcedure("places.Insert", 51, "Boston",
                GeographyPointValue.fromWKT("POINT(-71.069862 42.338100)"));

        VoltTable vt;
        String sql;

        // distance of all points with respect to a specific polygon
        sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                + "from borders, places where borders.pk = 1"
                + "order by distance, places.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                {{"Wyoming",    "Neverwhere",                           null},
                 {"Wyoming",    "Cheyenne",                             0.0},
                 {"Wyoming",    "Point on N Wyoming Border",            0.0},
                 {"Wyoming",    "Point on E Wyoming Border",            0.0},
                 {"Wyoming",    "Point On S Wyoming Border",            0.0},
                 {"Wyoming",    "Point On W Wyoming Border",            0.0},
                 {"Wyoming",    "East Point Not On Wyoming Border",     1.9770308670798656E-10},
                 {"Wyoming",    "West Point Not on Wyoming Border",     495.81208205561956},
                 {"Wyoming",    "Point near N Colorado border",         2382.072566994318},
                 {"Wyoming",    "North Point Not On Colorado Border",   4045.11696044222},
                 {"Wyoming",    "North Point Not On Wyoming Border",    12768.354425089678},
                 {"Wyoming",    "Fort Collins",                         48820.514427535185},
                 {"Wyoming",    "Denver",                               146450.5648140179},
                 {"Wyoming",    "South Point Not On Colorado Border",   453546.1064887051},
                 {"Wyoming",    "Albuquerque",                          659770.3125551793},
                 {"Wyoming",    "San Jose",                             1020359.8369329285},
                 {"Wyoming",    "Boston",                               2651698.17837178}
                }, vt, DISTANCE_EPSILON);

        // Validate result set obtained using distance between point and polygon is same as
        // distance between polygon and point
        sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                + "from borders, places where borders.pk < 100 and ( not contains(borders.region, places.loc)) "
                + "order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        // distance between point and polygon
        sql = "select borders.name, places.name, distance(places.loc, borders.region) as distance "
                + "from borders, places where borders.pk < 100 and not contains(borders.region, places.loc) "
                + "order by borders.pk";
        VoltTable vt1 = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(vt1, vt);

        // get distance of points contained in a polygon to polygon's centroid
        sql = "select borders.name as State, places.name as Location, "
                + "distance(centroid(borders.region), places.loc) as distance "
                + "from borders, places where borders.pk < 100 and contains(borders.region, places.loc) "
                + "order by distance";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                {{"Colorado",   "Denver",                               90126.44134902404},
                 {"Colorado",   "Fort Collins",                         177132.68582044652},
                 {"Colorado with a hole around Denver", "Fort Collins", 182956.55626884513},
                 {"Colorado",   "Point near N Colorado border",         223104.00553344024},
                 {"Colorado with a hole around Denver", "Point near N Colorado border",
                                                                        228833.82026300067},
                 {"Wyoming",    "Point On W Wyoming Border",            280064.27022410504},
                 {"Wyoming",    "Point on E Wyoming Border",            282622.1083568741},
                 {"Wyoming",    "Point on N Wyoming Border",            295384.0984736869},
                 {"Wyoming",    "Cheyenne",                             308378.91700889106},
                 {"Wyoming",    "Point On S Wyoming Border",            355574.4468866087}
                }, vt, DISTANCE_EPSILON);

        sql = "select distance(A.loc, B.loc) as distance "
                + "from places A, places B where A.name = 'Boston' and B.name = 'San Jose' "
                + "order by distance;";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][] {{4311575.515808559}}, vt, DISTANCE_EPSILON);

        // distance between polygon and polygon - currently not supported and should generate
        // exception saying incompatible data type supplied
        ProcCallException exception = null;
        try {
            sql = "select places.name, distance(borders.region, borders.region) "
                    + "from borders, places where borders.pk = places.pk "
                    + "order by borders.pk";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        }
        catch (ProcCallException excp) {
            exception = excp;
            assertTrue(exception.getMessage().contains("incompatible data type in operation"));
            assertTrue(exception.getMessage().contains("DISTANCE between two POLYGONS not supported"));
        } finally {
            assertNotNull(exception);
        }

        // distance between types others than point and poygon not supported
        exception = null;
        try {
            sql = "select places.name, distance(borders.region, borders.pk) "
                    + "from borders, places where borders.pk < 100 and borders.pk = places.pk "
                    + "order by borders.pk";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        }
        catch (ProcCallException excp) {
            exception = excp;
            assertTrue(exception.getMessage().contains("SQL error while compiling query"));
            assertTrue(exception.getMessage().contains("incompatible data type in operation"));
        } finally {
            assertNotNull(exception);
        }
    }

   final private static Border invalidBorders[] = {
    };

    public void testInvalidPolygons() throws Exception {
        Client client = getClient();
        populateTables(client, true);

        VoltTable vt = client.callProcedure("@AdHoc", "select pk, name from borders where borders.isvalid = 0 and isValid(region)").getResults()[0];
        StringBuffer sb = new StringBuffer("Expected no valid polygons in the invalid polygons table, found: ");
        long rowCount = vt.getRowCount();

        // Just computing an error message here.
        String sep = "";
        while (vt.advanceRow()) {
            sb.append(sep).append(vt.getString(1));
            sep = ", ";
        }
        assertEquals(sb.toString(), 0, rowCount);
    }

    public void testInvalidPolygonReasons() throws Exception {
        Client client = getClient();
        populateTables(client, true);

        VoltTable vt = client.callProcedure("@AdHoc", "select pk, name, isinvalidreason(region), message, region from borders").getResults()[0];
        while (vt.advanceRow()) {
            long pk = vt.getLong(0);
            String expected = vt.getString(3);
            String actual = vt.getString(2);
            if (expected == null) {
                assertNull(actual);
            } else {
                Pattern pattern = Pattern.compile(".*" + expected + ".*", Pattern.MULTILINE);
                Matcher m = pattern.matcher(actual);
                assertTrue(String.format("Border %s, key %d, Expected error message containing \"%s\" but got \"%s\"",
                                           vt.getString(1),
                                           pk,
                                           expected,
                                           actual),
                           m.matches());
            }
        }
    }

    public void testValidPolygonFromText() throws Exception {
        Client client = getClient();
        populateTables(client, true);
        // These should all fail.
        for (Border b : borders) {
            String expectedPattern = b.getMessage();
            String wkt = (b.getRegion() == null) ? "null" : ("'" + b.getRegion().toWKT() + "'");
            // Note that we only want one row, and we don't care what it is.
            //
            // It's unfortunate that we can't pass a parameter for wkt.  But
            // we need a parameterless sql string for verifyStmtFails.
            String sql = String.format("select validpolygonfromtext(%s) from borders where pk = 0",
                                       wkt);
            if (b.isValid() || b.isFixable()) {
                // If this is valid or else is fixable we expect
                // validpolygonfromtext to succeed.
                try {
                    ClientResponse cr = client.callProcedure("@AdHoc", sql);
                    assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                } catch (ProcCallException ex) {
                    fail(String.format("Expected no exceptions in polygon %s: %s, but got %s",
                                       b.getName(),
                                       (wkt != null) ? wkt : "null",
                                       ex.getMessage()));
                }
            } else {
                // If this border is not valid and not fixable we
                // expect the select statement to fail with the
                // pattern in the border.
                verifyStmtFails(client, sql, expectedPattern);
            }
        }
    }

    /**
     * Insert the polygons in the borders table, using validpolygonfromtext.  Some
     * polygons are so badly constructed that we can't fix them up.  But if
     * the shell or rings are misoriented, we can reverse the vertices and fix
     * things up.
     *
     * So, for each polygon in the borders table,
     * <ul>
     *   <li>
     *     If it is ok for polygonfromtext, then just insert them, and expect all is ok.
     *     These are the invalid polygons we can fix up.
     *   </li>
     *   <li>
     *     If they are not, then expect to get an error message.  These are the
     *     invalid polygons we cannot fix up.
     *   </li>
     * </ul>
     * @throws Exception
     */
    public void testInvalidPolygonFromText() throws Exception {
        Client client = getClient();
        ClientResponse cr;

        for (Border b : borders) {
            if ((b.getRegion() == null) || (b.getMessage() == null)) {
                continue;
            }
            boolean sawError = false;
            String msg = null;
            try {
                cr = client.callProcedure("@AdHoc",
                                          String.format("insert into borders values (%d, '%s', '%s', validpolygonfromtext('%s'), %d, %d);",
                                                        b.getPk(),
                                                        b.getName(),
                                                        b.getMessage(),
                                                        b.getRegion().toWKT(),
                                                        (b.isValid() ? 1 : 0),
                                                        (b.isFixable() ? 1 : 0)));
            } catch (Exception ex) {
                sawError = true;
                msg = ex.getMessage();
            }
            if (sawError) {
                assertTrue( ! ( b.isValid() || b.isFixable() ) );
                assertTrue(msg.contains(b.getMessage()));
            } else {
                assertTrue( b.isValid() || b.isFixable() );
            }
        }
    }

    /**
     * Test makevalidpolygon.  This should leave valid polygons invariant,
     * and reverse the loops for fixable invalid polygons.
     *
     * @throws Exception
     */
    public void testMakeValidPolygon() throws Exception {
        Client client = getClient();
        populateTables(client, true);
        ClientResponse cr;
        // Search all the polygons which are either valid
        // or else fixable.  If they are valid the region and
        // fixed region should be equal.  If they are not valid
        // the region and original should not be equal.  There
        // should be no SQL exceptions caused by the call to
        // makevalidpolygon.
        cr = client.callProcedure("@AdHoc",
                "select "
                + "   pk, "
                + "   isvalid, "
                + "   region as original, "
                + "   makevalidpolygon(region) as fixed "
                + "from borders where isvalid = 1 or isfixable = 1 order by pk;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable vt = cr.getResults()[0];
        while (vt.advanceRow()) {
            long pk = vt.getLong(0);
            boolean valid = (vt.getLong(1) == 1);
            GeographyValue orig = vt.getGeographyValue(2);
            GeographyValue fixed = vt.getGeographyValue(3);
            if (valid) {
                assertEquals(String.format("Border %d is valid, "
                                           + "but makevalidpolygon changed the rings",
                                           pk),
                             orig, fixed);
            } else {
                assertNotSame(String.format("Border %d is not valid but fixable, "
                                            + "but makevalidpolygon did not change the rings",
                                            pk),
                              orig, fixed);
            }
        }
    }

    public void testPointAsText() throws Exception {
        Client client = getClient();
        populateTables(client, true);

        // test for border case of rounding up the decimal number
        client.callProcedure("places.Insert", 50, "Someplace1",
                GeographyPointValue.fromWKT("POINT(13.4999999999995 17)"));
        // test for border case of rounding up the decimal number
        client.callProcedure("places.Insert", 51, "Someplace2",
                GeographyPointValue.fromWKT("POINT(-13.499999999999999995 -17)"));

        // get WKT representation using asText()
        VoltTable asTextVT = client.callProcedure("@AdHoc",
                "select loc, asText(loc) from places order by pk").getResults()[0];

        // get WKT representation using cast(point as varchar)
        VoltTable castVT =  client.callProcedure("@AdHoc",
                "select loc, cast(loc as VARCHAR) from places order by pk").getResults()[0];

        // verify results of asText from EE matches WKT format defined in frontend/java
        while (asTextVT.advanceRow()) {
            GeographyPointValue gpv = asTextVT.getGeographyPointValue(0);
            if (gpv == null) {
                assertEquals(null, asTextVT.getString(1));
            }
            else {
                assertEquals(gpv.toString(), asTextVT.getString(1));
            }
        }
        // verify WKT from asText and cast(point as varchar) results in same result WKT.
        assertEquals(asTextVT, castVT);
    }

    public void testPolygonAsText() throws Exception {
        Client client = getClient();
        populateTables(client, false);
        // polygon whose co-ordinates are mix of decimal and whole numbers - test
        // decimal rounding border cases
        Border someWhere = new Border(50, "someWhere", "someWhere",
                new GeographyValue("POLYGON ((-10.1234567891234 10.1234567891234, " +
                                             "-14.1234567891264 10.1234567891234, " +
                                             "-14.0 4.1234567891235, " +
                                             "-12.0 4.4555555555555555550, " +
                                             "-11.0 4.4999999999996, " +
                                             "-10.1234567891234 10.1234567891234))"),
                true, true);
        VoltTable vt = client.callProcedure("BORDERS.Insert",
                someWhere.getPk(),
                someWhere.getName(),
                someWhere.getMessage(),
                someWhere.getRegion(),
                someWhere.isValid() ? 1 : 0,
                someWhere.isFixable() ? 1 : 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        // polygon with 2 holes and whose vertices are whole numbers - test for whole number rounding
        someWhere = new Border(51, "someWhereWithHoles", "someWhereWithHoles",
                new GeographyValue("POLYGON ((10 10, -10 10, -10 1, 10 1, 10 10)," +
                                            "(-8 9, -8 8, -9 8, -9 9, -8 9)," +
                                            "(9 9,  9 8, 8 8, 8 9, 9 9))"),
                true, true);
        vt = client.callProcedure("BORDERS.Insert",
                someWhere.getPk(),
                someWhere.getName(),
                someWhere.getMessage(),
                someWhere.getRegion(),
                someWhere.isValid() ? 1 : 0,
                someWhere.isFixable() ? 1 : 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // polygon with hole whose co-ordinates are whole numbers
        someWhere = new Border(52, "someWhereWithHoles", "someWhereWithHoles",
                new GeographyValue("POLYGON ((10 10, -10 10, -10 1, 10 1, 10 10)," +
                                            "(9 9, 9 8, 8 8, 8 9, 9 9)," +
                                            "(-8 9, -8 8, -9 8, -9 9, -8 9))"),
                true, true);
        vt = client.callProcedure("BORDERS.Insert",
                    someWhere.getPk(),
                    someWhere.getName(),
                    someWhere.getMessage(),
                    someWhere.getRegion(),
                    someWhere.isValid() ? 1 : 0,
                    someWhere.isFixable() ? 1 : 0).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // get WKT representation using asText()
        vt = client.callProcedure("@AdHoc",
                "select region, asText(region) from borders order by pk").getResults()[0];

        // get WKT representation using cast(polygon as varchar)
        VoltTable castVT = client.callProcedure("@AdHoc",
                "select region, cast(region as VARCHAR) from borders order by pk").getResults()[0];

        // verify results of asText from EE matches WKT format defined in frontend/java
        GeographyValue gv;
        while (vt.advanceRow()) {
            gv = vt.getGeographyValue(0);
            if (gv == null) {
                assertEquals(null, vt.getString(1));
            }
            else {
                assertEquals(gv.toString(), vt.getString(1));
            }
        }

        // verify WKT from asText and cast(polygon as varchar) results are same.
        assertEquals(vt, castVT);
    }

    public void testPointPolygonAsTextNegative() throws Exception {
        Client client = getClient();
        populateTables(client, true);

        verifyStmtFails(client, "select asText(?) from places order by pk",
                                "data type cast needed for parameter or null literal: "
                              + "input type to ASTEXT function is ambiguous");
        verifyStmtFails(client, "select asText(null) from places order by pk",
                                "data type cast needed for parameter or null literal: "
                              + "input type to ASTEXT function is ambiguous");
        verifyStmtFails(client, "select asText(pk) from borders order by pk",
                                "incompatible data type in operation: "
                              + "The asText function accepts only GEOGRAPHY and GEOGRAPHY_POINT types");
    }

    public void testPolygonPointDWithin() throws Exception {
        final double DISTANCE_EPSILON = 1.0e-8;
        Client client = getClient();
        populateTables(client, true);
        String sql;

        // polygon-to-point
        sql = "create procedure DWithin_Proc as select borders.name, places.name, distance(borders.region, places.loc) as distance "
                + "from borders, places where DWithin(borders.region, places.loc, ?) and borders.pk = 1 "
                + "order by distance, borders.pk, places.pk;";
        client.callProcedure("@AdHoc", sql);
        client.callProcedure("places.Insert", 50, "San Jose",
                GeographyPointValue.fromWKT("POINT(-121.903692 37.325464)"));
        client.callProcedure("places.Insert", 51, "Boston",
                GeographyPointValue.fromWKT("POINT(-71.069862 42.338100)"));

        VoltTable vt1;
        VoltTable vt2;

        String prefix;

        // polygon-to-point
        vt1 = client.callProcedure("DWithin_Proc", 50000.1).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                {{"Wyoming",    "Cheyenne",                             0.0},
                 {"Wyoming",    "Point on N Wyoming Border",            0.0},
                 {"Wyoming",    "Point on E Wyoming Border",            0.0},
                 {"Wyoming",    "Point On S Wyoming Border",            0.0},
                 {"Wyoming",    "Point On W Wyoming Border",            0.0},
                 {"Wyoming",    "East Point Not On Wyoming Border",     1.9770308670798656E-10},
                 {"Wyoming",    "West Point Not on Wyoming Border",     495.81208205561956},
                 {"Wyoming",    "Point near N Colorado border",         2382.072566994318},
                 {"Wyoming",    "North Point Not On Colorado Border",   4045.11696044222},
                 {"Wyoming",    "North Point Not On Wyoming Border",    12768.354425089678},
                 {"Wyoming",    "Fort Collins",                         48820.514427535185},
                }, vt1, DISTANCE_EPSILON);
        vt1.resetRowPosition();
        prefix = "Assertion failed comparing results from DWithin and Distance functions: ";

        // verify results of within using DISTANCE function
        sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                + "from borders, places where (distance(borders.region, places.loc) <= 50000.1) and borders.pk = 1 "
                + "order by distance, borders.pk, places.pk;";
        vt2 = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTablesAreEqual(prefix, vt2, vt1, GEOGRAPHY_DISTANCE_EPSILON);

        // distance argument is null
        sql = "select places.name from borders, places where  DWithin(borders.region, places.loc, NULL);";
        vt1 = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(0, vt1.getRowCount());

        // point-to-point
        sql = "select A.name, B.name, distance(A.loc, B.loc) as distance "
                + "from places A, places B where DWithin(A.loc, B.loc, 100000) and A.pk <> B.pk "
                + "order by distance, A.pk, B.pk;";
        vt1 = client.callProcedure("@AdHoc", sql).getResults()[0];

        sql = "select A.name, B.name, distance(A.loc, B.loc) as distance "
                + "from places A, places B where distance(A.loc, B.loc) <= 100000 and A.pk <> B.pk "
                + "order by distance, A.pk, B.pk;";
        vt2 = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTablesAreEqual(prefix, vt2, vt1, GEOGRAPHY_DISTANCE_EPSILON);

        // test results of within using contains function
        prefix = "Assertion failed comparing results from DWithin and Contains functions: ";
        sql = "select borders.name, places.name "
                + "from borders, places where DWithin(borders.region, places.loc, 0) "
                + "order by borders.pk, places.pk;";
        vt1 = client.callProcedure("@AdHoc", sql).getResults()[0];

        sql = "select borders.name, places.name "
                + "from borders, places where Contains(borders.region, places.loc) "
                + "order by borders.pk, places.pk;";
        vt2 = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTablesAreEqual(prefix, vt2, vt1, GEOGRAPHY_DISTANCE_EPSILON);

        sql = "select borders.name, places.name "
                + "from borders, places where NOT DWithin(borders.region, places.loc, 0) "
                + "order by borders.pk, places.pk;";
        vt1 = client.callProcedure("@AdHoc", sql).getResults()[0];

        sql = "select borders.name, places.name "
                + "from borders, places where NOT Contains(borders.region, places.loc) "
                + "order by borders.pk, places.pk;";
        vt2 = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTablesAreEqual(prefix, vt2, vt1, GEOGRAPHY_DISTANCE_EPSILON);

        // Restore catalog changes:
        ClientResponse cr = client.callProcedure("@AdHoc", "drop procedure DWithin_Proc;");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
    }

    public void testPolygonPointDWithinNegative() throws Exception {
        Client client = getClient();
        populateTables(client, true);

        String sql;
        String expectedMsg;

        // DWITHIN between polygon and polygon is not supported
        sql = "select A.name, B.name from borders A, borders B where DWithin(A.region, B.region, 100);";
        expectedMsg = "incompatible data type in operation: DWITHIN between two POLYGONS not supported";
        verifyStmtFails(client, sql, expectedMsg);

        // types others than point and polygon in first two input arguments not supported
        sql = "select places.name, DWithin(borders.region, borders.pk, 100) from borders, places where borders.pk = places.pk;";
        expectedMsg = "incompatible data type in operation: DWITHIN function evaulates if geographies are within specified "
                    + "distance of one-another for POINT-to-POINT, POINT-to-POLYGON and POLYGON-to-POINT geographies only.";
        verifyStmtFails(client, sql, expectedMsg);

        // input type for distance argument other than numeric
        sql = "select places.name, DWithin(borders.region, places.loc, borders.name) from borders, places;";
        expectedMsg = "incompatible data type in operation: input type DISTANCE to DWITHIN function must be non-negative numeric value";
        verifyStmtFails(client, sql, expectedMsg);

        // negative value used for input distance argument
        sql = "select places.name from borders, places where  DWithin(borders.region, places.loc, -1) ;";
        expectedMsg = "Invalid input to DWITHIN function: 'Value of DISTANCE argument must be non-negative'";
        verifyStmtFails(client, sql, expectedMsg);
    }

    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder =
                new MultiConfigSuiteBuilder(TestGeospatialFunctions.class);
        VoltProjectBuilder project = new VoltProjectBuilder();

        try {
            VoltServerConfig config = null;
            boolean success;

            setUpSchema(project);
            config = new LocalCluster("geography-value-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            project.setUseDDLSchema(true);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);
        } catch  (IOException excp) {
            assert (false);
        }
        return builder;
    }
}
