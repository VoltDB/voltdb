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

package org.voltdb.regressionsuites;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;

public class TestGeospatialFunctions extends RegressionSuite {
    /*
     * Distances are within these tolerances.
     */
    public final double GEOGRAPHY_DISTANCE_EPSILON = 1.0e-8;
    /*
     * The other operations have this tolerance.
     */
    public final double GEOGRAPHY_CENTROID_EPSILON = 1.0e-3;

    public TestGeospatialFunctions(String name) {
        super(name);
    }

    /*
     * We want to store the borders table once and for all, and insert and test
     * without repeating ourselves. This class holds geometry values for us for
     * inserting and for testing later on.
     */
    private static class Borders {
        Borders(long pk, String name, GeographyValue region) {
            m_pk = pk;
            m_name = name;
            m_region = region;
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

        private final long m_pk;
        private final String m_name;
        private final GeographyValue m_region;
    }

    /*
     * This is the array of borders we know about. We will insert these
     * borders and then extract them.
     */
    private static Borders borders[] = {
        new Borders(0, "Colorado", new GeographyValue("POLYGON(("
                                                  + "41.002 -102.052, "
                                                  + "41.002 -109.045,"
                                                  + "36.999 -109.045,"
                                                  + "36.999 -102.052,"
                                                  + "41.002 -102.052))")),
        new Borders(1, "Wyoming", new GeographyValue("POLYGON(("
                                                + "44.978 -104.061, "
                                                + "44.978 -111.046, "
                                                + "40.998 -111.046, "
                                                + "40.998 -104.061, "
                                                + "44.978 -104.061))")),
       new Borders(2, "Colorado with a hole around Denver",
               new GeographyValue("POLYGON("
                                  + "(41.002 -102.052, "
                                  + "41.002 -109.045,"
                                  + "36.999 -109.045,"
                                  + "36.999 -102.052,"
                                  + "41.002 -102.052), "
                                  + "(40.240 -104.035, "
                                  + "40.240 -105.714, "
                                  + "39.188 -105.714, "
                                  + "39.188 -104.035,"
                                  + "40.240 -104.035))")),
       new Borders(3, "Wonderland", null)
    };

    private static void populateBorders(Client client, Borders borders[]) throws Exception {
        for (Borders b : borders) {
            client.callProcedure("borders.Insert",
                                 b.getPk(),
                                 b.getName(),
                                 b.getRegion());
        }
    }

    private static void populateTables(Client client) throws Exception {
        // Note: These are all WellKnownText strings.  So they should
        //       be "POINT(...)" and not "GEOGRAPHY_POINT(...)".
        client.callProcedure("places.Insert", 0, "Denver",
                GeographyPointValue.geographyPointFromText("POINT(39.704 -104.959)"));
        client.callProcedure("places.Insert", 1, "Albuquerque",
                GeographyPointValue.geographyPointFromText("POINT(35.113 -106.599)"));
        client.callProcedure("places.Insert", 2, "Cheyenne",
                GeographyPointValue.geographyPointFromText("POINT(41.134 -104.813)"));
        client.callProcedure("places.Insert", 3, "Fort Collins",
                GeographyPointValue.geographyPointFromText("POINT(40.585 -105.077)"));
        client.callProcedure("places.Insert", 4, "Point near N Colorado border",
                GeographyPointValue.geographyPointFromText("POINT(41.002 -105.04)"));
        client.callProcedure("places.Insert", 5, "Point Not On N Colorado Border",
                GeographyPointValue.geographyPointFromText("POINT(41.005 -109.025)"));
        client.callProcedure("places.Insert", 6, "Point on N Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(44.978 -105.058)"));
        client.callProcedure("places.Insert", 7, "North Point Not On Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(45.119 -105.060)"));
        client.callProcedure("places.Insert", 8, "Point on E Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(42.988 -104.078)"));
        client.callProcedure("places.Insert", 9, "East Point Not On Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(42.986 -104.061)"));
        client.callProcedure("places.Insert", 10, "Point On S Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(41.099 -110.998)"));
        client.callProcedure("places.Insert", 11, "Point On S Colorado Border",
                GeographyPointValue.geographyPointFromText("POINT(37.002 -103.008)"));
        client.callProcedure("places.Insert", 12, "Point On W Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(42.999 -110.998)"));
        client.callProcedure("places.Insert", 13, "West not On South Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(41.999 -111.052)"));

        // A null-valued point
        client.callProcedure("places.Insert", 99, "Neverwhere", null);

        populateBorders(client, borders);
    }

    public void notestContains() throws Exception {
        Client client = getClient();

        populateTables(client);

        VoltTable vt = client.callProcedure("@AdHoc",
                "select places.name || ', ' || borders.name "
                + "from places, borders "
                + "where contains(borders.region, places.loc) "
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

    }

    public void notestPolygonInteriorRings() throws Exception {
        Client client = getClient();
        populateTables(client);

        String sql = "select borders.name,  numInteriorRing(borders.region) "
                        + "from borders order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Colorado",                           0},
                 {"Wyoming",                            0},
                 {"Colorado with a hole around Denver", 1},
                 {"Wonderland",                         Integer.MIN_VALUE},
                }, vt);
    }

    public void notestPolygonNumberOfPoints() throws Exception {
        Client client = getClient();
        populateTables(client);

        // polygon with no holes has exterior ring.
        // number of points will be number of them on the exterior ring
        String sql = "select borders.name, numPoints(borders.region) from borders "
                        + "where numInteriorRing(borders.region) = 0 "
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
                + "where numInteriorRings(borders.region) = 1 "
                + "order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Colorado with a hole around Denver", 10}
                }, vt);

        // null rings for null valued polygon
        sql = "select borders.name, numPoints(borders.region) from borders "
                + "where borders.region is NULL "
                + "order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Wonderland", Integer.MIN_VALUE}
                }, vt);

    }

    public void notestLongitudeLatitude() throws Exception {
        Client client = getClient();
        populateTables(client);

        String sql = "select places.name, LATITUDE(places.loc), LONGITUDE(places.loc) "
                        + "from places order by places.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Denver",                             39.704, -104.959},
                 {"Albuquerque",                        35.113, -106.599},
                 {"Cheyenne",                           41.134, -104.813},
                 {"Fort Collins",                       40.585, -105.077},
                 {"Point near N Colorado border",       41.002, -105.04},
                 {"Point Not On N Colorado Border",     41.005, -109.025},
                 {"Point on N Wyoming Border",          44.978, -105.058},
                 {"North Point Not On Wyoming Border",  45.119, -105.06},
                 {"Point on E Wyoming Border",          42.988, -104.078},
                 {"East Point Not On Wyoming Border",   42.986, -104.061},
                 {"Point On S Wyoming Border",          41.099, -110.998},
                 {"Point On S Colorado Border",         37.002, -103.008},
                 {"Point On W Wyoming Border",          42.999, -110.998},
                 {"West not On South Wyoming Border",   41.999, -111.052},
                 {"Neverwhere",     Double.MIN_VALUE,   Double.MIN_VALUE},
                }, vt);

        sql = "select places.name, LATITUDE(places.loc), LONGITUDE(places.loc) "
                + "from places, borders "
                + "where contains(borders.region, places.loc) "
                + "group by places.name, places.loc "
                + "order by places.name";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Cheyenne",                       41.134,  -104.813},
                 {"Denver",                         39.704,  -104.959},
                 {"Fort Collins",                   40.585, -105.077},
                 {"Point On S Wyoming Border",      41.099, -110.998},
                 {"Point On W Wyoming Border",      42.999, -110.998},
                 {"Point near N Colorado border",   41.002, -105.04},
                 {"Point on E Wyoming Border",      42.988, -104.078},
                 {"Point on N Wyoming Border",      44.978, -105.058},
                }, vt);
    }

    public void notestPolygonFloatingPrecision() throws Exception {
        final double EPSILON = -1.0;
        Client client = getClient();
        populateTables(client);

        String sql = "select name, region "
                        + "from borders order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                                        {{borders[0].getName(), borders[0].getRegion()},
                                         {borders[1].getName(), borders[1].getRegion()},
                                         {borders[2].getName(), borders[2].getRegion()},
                                         {borders[3].getName(), borders[3].getRegion()}},
                                        vt,
                                        EPSILON);
    }

    public void notestPolygonCentroidAndArea() throws Exception {
        Client client = getClient();
        populateTables(client);

        String sql = "select borders.name, Area(borders.region), LATITUDE(centroid(borders.region)), LONGITUDE(centroid(borders.region)) "
                        + "from borders order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        // in the calculation below, areas of states are close to actual area of the state (vertices
        // used for polygon are close approximations, not exact, values of the state vertices).
        // Area for Colorado - 269601 sq km and Wyoming 253350 sq km
        // For centroid, the value in table is based on the answer provide by S2 for the given polygons
        assertApproximateContentOfTable(new Object[][]
                {{ "Colorado",      2.6886542912139893E11,  39.03372408765194,      -105.5485 },
                 { "Wyoming",       2.5126863189309894E11,  43.01953179182205,      -107.55349999999999 },
                 { "Colorado with a hole around Denver",
                                    2.5206603914764166E11,  38.98811213712535,      -105.5929789796371 },
                 { "Wonderland",    Double.MIN_VALUE,       Double.MIN_VALUE,       Double.MIN_VALUE},
                }, vt, GEOGRAPHY_CENTROID_EPSILON);
    }

    public void notestPolygonPointDistance() throws Exception {
        Client client = getClient();
        populateTables(client);

        VoltTable vt;

        // distance of all points with respect to a specific polygon
        String sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                + "from borders, places where borders.pk = 1"
                + "order by distance, places.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                {{"Wyoming",    "Neverwhere",                       Double.MIN_VALUE},
                 {"Wyoming",    "Cheyenne",                         0.0},
                 {"Wyoming",    "Point on N Wyoming Border",        0.0},
                 {"Wyoming",    "Point on E Wyoming Border",        0.0},
                 {"Wyoming",    "Point On S Wyoming Border",        0.0},
                 {"Wyoming",    "Point On W Wyoming Border",        0.0},
                 {"Wyoming",    "East Point Not On Wyoming Border", 1.9770281362922971E-10},
                 {"Wyoming",    "West not On South Wyoming Border", 495.8113972117496},
                 {"Wyoming",    "Point near N Colorado border",     2382.0692767400983},
                 {"Wyoming",    "Point Not On N Colorado Border",   4045.111373096421},
                 {"Wyoming",    "North Point Not On Wyoming Border",12768.336788711755},
                 {"Wyoming",    "Fort Collins",                     48820.44699386174},
                 {"Wyoming",    "Denver",                           146450.36252816164},
                 {"Wyoming",    "Point On S Colorado Border",       453545.4800250064},
                 {"Wyoming",    "Albuquerque",                      659769.4012428687}

                }, vt, GEOGRAPHY_DISTANCE_EPSILON);

        // Validate result set obtained using distance between point and polygon is same as
        // distance between polygon and point
        sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                        + "from borders, places "
                        + "order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        // distance between point and polygon
        sql = "select borders.name, places.name, distance(places.loc, borders.region) as distance "
                + "from borders, places "
                + "order by borders.pk";
        VoltTable vt1 = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertEquals(vt1, vt);

        // get distance of points contained in a polygon to polygon's centroid
        sql = "select borders.name as State, places.name as Location, "
                + "distance(centroid(borders.region), places.loc) as distance "
                + "from borders, places where contains(borders.region, places.loc) "
                + "order by distance";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                {{"Colorado",   "Denver",                               90126.31686125314},
                 {"Colorado",   "Fort Collins",                         177132.44115469826},
                 {"Colorado with a hole around Denver", "Fort Collins", 182956.3035588355},
                 {"Colorado",   "Point near N Colorado border",         223103.69736948845},
                 {"Colorado with a hole around Denver", "Point near N Colorado border",
                                                                        228833.50418470264},
                 {"Wyoming",    "Point On W Wyoming Border",            280063.8833833934},
                 {"Wyoming",    "Point on E Wyoming Border",            282621.71798313083},
                 {"Wyoming",    "Point on N Wyoming Border",            295383.69047235645},
                 {"Wyoming",    "Cheyenne",                             308378.4910583776},
                 {"Wyoming",    "Point On S Wyoming Border",            355573.95574694296}
                }, vt, GEOGRAPHY_DISTANCE_EPSILON);

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
            assertTrue(exception.getMessage().contains("Distance between two polygons not supported"));
        } finally {
            assertNotNull(exception);
        }

        // distance between types others than point and poygon not supported
        exception = null;
        try {
            sql = "select places.name, distance(borders.region, borders.pk) "
                    + "from borders, places where borders.pk = places.pk "
                    + "order by borders.pk";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        }
        catch (ProcCallException excp) {
            exception = excp;
            assertTrue(exception.getMessage().contains("Error compiling query"));
            assertTrue(exception.getMessage().contains("incompatible data type in operation"));
        } finally {
            assertNotNull(exception);
        }
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
    private static String CROSSED_EDGES
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
    private static String CW_EDGES
      = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0))";
    private static String MULTI_POLYGON
      = "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (0 0, 0 -1, -1 -1, -1 0, 0 0))";
    private static String SHARED_INNER_VERTICES
      = "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (.1 .1, .1 .9, .5 .9, .9 .1, .1 .1), (.5 .1, .5 .9, .9 .9, .9 .1, .5 .1))";
    private static String SHARED_INNER_EDGES
      = "POLYGON((0 0, 0 1, 1 1, 1 0, 0 0), (.1 .1, .1 .9, .5 .9, .5 .1, .1 .1), (.5 .2, .5 .8, .9 .8, .9 .2, .5 .2))";
    private static String COLLINEAR3
      = "POLYGON((0 0, 0 1, 0 2, 0 0))";
    private static String COLLINEAR4
    = "POLYGON((0 0, 0 1, 0 2, 0 3, 0 0))";
    private static String COLLINEAR5
    = "POLYGON((0 0, 0 1, 0 2, 0 3, 0 4, 0 0))";
    private static String COLLINEAR6
    = "POLYGON((0 0, 0 1, 0 2, 0 3, 0 4, 0 5, 0 0))";
    private static String COLLINEAR7
    = "POLYGON((0 0, 0 1, 0 2, 0 3, 0 4, 0 5, 0 6, 0 0))";
    private static String COLLINEAR8
    = "POLYGON((0 0, 0 1, 0 2, 0 3, 0 4, 0 5, 0 6, 0 7, 0 0))";
    private static String COLLINEAR9
    = "POLYGON((0 0, 0 1, 0 2, 0 3, 0 4, 0 5, 0 6, 0 7, 0 8, 0 0))";
    private static String COLLINEAR10
    = "POLYGON((0 0, 0 1, 0 2, 0 3, 0 4, 0 5, 0 6, 0 7, 0 8, 0 9, 0 0))";

    private static Borders invalidBorders[] = {
        new Borders(100, "CrossedEdges",        GeographyValue.geographyValueFromText(CROSSED_EDGES)),
        new Borders(101, "Sunwise",             GeographyValue.geographyValueFromText(CW_EDGES)),
        new Borders(102, "MultiPolygon",        GeographyValue.geographyValueFromText(MULTI_POLYGON)),
        new Borders(103, "SharedInnerVertices", GeographyValue.geographyValueFromText(SHARED_INNER_VERTICES)),
        new Borders(104, "SharedInnerEdges",    GeographyValue.geographyValueFromText(SHARED_INNER_EDGES)),
        /*
         * These are apparently legal.  Should they be?
         */
     // new Borders(105, "Collinear3",          GeographyValue.geographyValueFromText(COLLINEAR3)),
     // new Borders(106, "Collinear4",          GeographyValue.geographyValueFromText(COLLINEAR4)),
     // new Borders(107, "Collinear5",          GeographyValue.geographyValueFromText(COLLINEAR5)),
     // new Borders(108, "Collinear6",          GeographyValue.geographyValueFromText(COLLINEAR6)),
     // new Borders(109, "Collinear7",          GeographyValue.geographyValueFromText(COLLINEAR7)),
     // new Borders(110, "Collinear8",          GeographyValue.geographyValueFromText(COLLINEAR8)),
     // new Borders(111, "Collinear9",          GeographyValue.geographyValueFromText(COLLINEAR9)),
     // new Borders(112, "Collinear10",         GeographyValue.geographyValueFromText(COLLINEAR10)),
    };

    public void testInvalidPolygons() throws Exception {
        Client client = getClient();
        populateBorders(client, invalidBorders);

        VoltTable vt = client.callProcedure("@AdHoc", "select pk, name from borders where isValid(region)").getResults()[0];
        StringBuffer sb = new StringBuffer("Expected no polygons in the involid polygons table, found: ");
        long rowCount = vt.getRowCount();

        String sep = "";
        while (vt.advanceRow()) {
            sb.append(sep).append(vt.getString(1));
            sep = ", ";
        }
        assertEquals(sb.toString(), 0, rowCount);
    }

    static public junit.framework.Test suite() {

        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestGeospatialFunctions.class);
        boolean success;

        VoltProjectBuilder project = new VoltProjectBuilder();

        String literalSchema =
                "CREATE TABLE places (\n"
                + "  pk INTEGER NOT NULL PRIMARY KEY,\n"
                + "  name VARCHAR(64),\n"
                + "  loc GEOGRAPHY_POINT\n"
                + ");\n"
                + "CREATE TABLE borders (\n"
                + "  pk INTEGER NOT NULL PRIMARY KEY,\n"
                + "  name VARCHAR(64),\n"
                + "  region GEOGRAPHY\n"
                + ");\n"
                + "\n"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        }
        catch (Exception e) {
            fail();
        }

        config = new LocalCluster("geography-value-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
