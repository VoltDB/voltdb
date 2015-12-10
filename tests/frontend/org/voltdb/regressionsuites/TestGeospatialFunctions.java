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
                                                  + "-102.052 41.002, "
                                                  + "-109.045 41.002,"
                                                  + "-109.045 36.999,"
                                                  + "-102.052 36.999,"
                                                  + "-102.052 41.002))")),
        new Borders(1, "Wyoming", new GeographyValue("POLYGON(("
                                                + "-104.061 44.978, "
                                                + "-111.046 44.978, "
                                                + "-111.046 40.998, "
                                                + "-104.061 40.998, "
                                                + "-104.061 44.978))")),
       new Borders(2, "Colorado with a hole around Denver",
               new GeographyValue("POLYGON("
                                  + "(-102.052 41.002, "
                                  + "-109.045 41.002,"
                                  + "-109.045 36.999,"
                                  + "-102.052 36.999,"
                                  + "-102.052 41.002), "
                                  + "(-104.035 40.240, "
                                  + "-105.714 40.240, "
                                  + "-105.714 39.188, "
                                  + "-104.035 39.188,"
                                  + "-104.035 40.240))")),
       new Borders(3, "Wonderland", null)
    };
    private static void populateTables(Client client) throws Exception {
        // Note: These are all WellKnownText strings.  So they should
        //       be "POINT(...)" and not "GEOGRAPHY_POINT(...)".
        client.callProcedure("places.Insert", 0, "Denver",
                GeographyPointValue.geographyPointFromText("POINT(-104.959 39.704)"));
        client.callProcedure("places.Insert", 1, "Albuquerque",
                GeographyPointValue.geographyPointFromText("POINT(-106.599 35.113)"));
        client.callProcedure("places.Insert", 2, "Cheyenne",
                GeographyPointValue.geographyPointFromText("POINT(-104.813 41.134)"));
        client.callProcedure("places.Insert", 3, "Fort Collins",
                GeographyPointValue.geographyPointFromText("POINT(-105.077 40.585)"));
        client.callProcedure("places.Insert", 4, "Point near N Colorado border",
                GeographyPointValue.geographyPointFromText("POINT(-105.04 41.002)"));
        client.callProcedure("places.Insert", 5, "North Point Not On Colorado Border",
                GeographyPointValue.geographyPointFromText("POINT(-109.025 41.005)"));
        client.callProcedure("places.Insert", 6, "Point on N Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(-105.058 44.978)"));
        client.callProcedure("places.Insert", 7, "North Point Not On Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(-105.060 45.119)"));
        client.callProcedure("places.Insert", 8, "Point on E Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(-104.078 42.988)"));
        client.callProcedure("places.Insert", 9, "East Point Not On Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(-104.061 42.986)"));
        client.callProcedure("places.Insert", 10, "Point On S Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(-110.998 41.099)"));
        client.callProcedure("places.Insert", 11, "South Point Not On Colorado Border",
                GeographyPointValue.geographyPointFromText("POINT(-103.008 37.002)"));
        client.callProcedure("places.Insert", 12, "Point On W Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(-110.998 42.999)"));
        client.callProcedure("places.Insert", 13, "West Point Not on Wyoming Border",
                GeographyPointValue.geographyPointFromText("POINT(-111.052 41.999)"));

        // A null-valued point
        client.callProcedure("places.Insert", 99, "Neverwhere", null);

        for (int idx = 0; idx < borders.length; idx += 1) {
            Borders b = borders[idx];
            client.callProcedure("borders.Insert",
                                 b.getPk(),
                                 b.getName(),
                                 b.getRegion());
        }
    }

    public void testContains() throws Exception {
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

        vt = client.callProcedure("@AdHoc", "select places.name, borders.name "
                + "from places, borders "
                + "where not contains(borders.region, places.loc) "
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

    public void testPolygonNumberOfPoints() throws Exception {
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

    public void testLongitudeLatitude() throws Exception {
        Client client = getClient();
        populateTables(client);

        String sql = "select places.name, LONGITUDE(places.loc), LATITUDE(places.loc) "
                        + "from places order by places.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
            {{"Denver",                             -104.959, 39.704},
             {"Albuquerque",                        -106.599, 35.113},
             {"Cheyenne",                           -104.813, 41.134},
             {"Fort Collins",                       -105.077, 40.585},
             {"Point near N Colorado border",       -105.04, 41.002},
             {"North Point Not On Colorado Border", -109.025, 41.005},
             {"Point on N Wyoming Border",          -105.058, 44.978},
             {"North Point Not On Wyoming Border",  -105.06, 45.119},
             {"Point on E Wyoming Border",          -104.078, 42.988},
             {"East Point Not On Wyoming Border",   -104.061, 42.986},
             {"Point On S Wyoming Border",          -110.998, 41.099},
             {"South Point Not On Colorado Border", -103.008, 37.002},
             {"Point On W Wyoming Border",          -110.998, 42.999},
             {"West Point Not on Wyoming Border",   -111.052, 41.999},
             {"Neverwhere",     Double.MIN_VALUE,   Double.MIN_VALUE},
            }, vt);

        sql = "select places.name, LONGITUDE(places.loc), LATITUDE(places.loc) "
                + "from places, borders "
                + "where contains(borders.region, places.loc) "
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

    public void testPolygonCentroidAndArea() throws Exception {
        // The AREA_EPSILON here is 1.0e-1, because the values are in the range
        // 1.0e11, and we expect 1.0e12 precision.
        final double AREA_EPSILON=1.0e-1;
        Client client = getClient();
        populateTables(client);

        String sql = "select borders.name, Area(borders.region) "
                        + "from borders order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        // in the calculation below, areas of states are close to actual area of the state (vertices
        // used for polygon are close approximations, not exact, values of the state vertices).
        // Area for Colorado - 269601 sq km and Wyoming 253350 sq km
        assertApproximateContentOfTable(new Object[][]
                {{ "Colorado",      2.6886542912139893E11},
                 { "Wyoming",       2.5126863189309894E11},
                 { "Colorado with a hole around Denver",
                                    2.5206603914764166E11},
                 { "Wonderland",    Double.MIN_VALUE},
                }, vt, AREA_EPSILON);
        // Test the centroids.  For centroid, the value in table is based on the answer provide by S2 for the given polygons
        // The CENTROID relative precision is greater than the AREA relative precision.
        final double CENTROID_EPSILON=1.0e-12;
        sql = "select borders.name, LATITUDE(centroid(borders.region)), LONGITUDE(centroid(borders.region)) "
                + "from borders order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                {{ "Colorado",      39.03372408765194,      -105.5485 },
                 { "Wyoming",       43.01953179182205,      -107.55349999999999 },
                 { "Colorado with a hole around Denver",
                                    38.98811213712535,      -105.5929789796371 },
                 { "Wonderland",    Double.MIN_VALUE,       Double.MIN_VALUE},
                }, vt, CENTROID_EPSILON);
    }

    public void testPolygonPointDistance() throws Exception {
        // The distances we consider are all in the thousands of square
        // meters.  We expect 1.0e-12 precision, so that's 1.0e-8 relative
        // precision.  Note that we have determined empirically that
        // 1.0e-9 fails.
        final double DISTANCE_EPSILON = 1.0e-8;
        Client client = getClient();
        populateTables(client);

        VoltTable vt;

        // distance of all points with respect to a specific polygon
        String sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                + "from borders, places where borders.pk = 1"
                + "order by distance, places.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]
                {{"Wyoming",    "Neverwhere",                           Double.MIN_VALUE},
                 {"Wyoming",    "Cheyenne",                             0.0},
                 {"Wyoming",    "Point on N Wyoming Border",            0.0},
                 {"Wyoming",    "Point on E Wyoming Border",            0.0},
                 {"Wyoming",    "Point On S Wyoming Border",            0.0},
                 {"Wyoming",    "Point On W Wyoming Border",            0.0},
                 {"Wyoming",    "East Point Not On Wyoming Border",     1.9770281362922971E-10},
                 {"Wyoming",    "West Point Not on Wyoming Border",     495.8113972117496},
                 {"Wyoming",    "Point near N Colorado border",         2382.0692767400983},
                 {"Wyoming",    "North Point Not On Colorado Border",   4045.111373096421},
                 {"Wyoming",    "North Point Not On Wyoming Border",    12768.336788711755},
                 {"Wyoming",    "Fort Collins",                         48820.44699386174},
                 {"Wyoming",    "Denver",                               146450.36252816164},
                 {"Wyoming",    "South Point Not On Colorado Border",   453545.4800250064},
                 {"Wyoming",    "Albuquerque",                          659769.4012428687},
                }, vt, DISTANCE_EPSILON);

        // Validate result set obtained using distance between point and polygon is same as
        // distance between polygon and point
        sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                + "from borders, places where not contains(borders.region, places.loc) "
                + "order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        // distance between point and polygon
        sql = "select borders.name, places.name, distance(places.loc, borders.region) as distance "
                + "from borders, places where not contains(borders.region, places.loc) "
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
                }, vt, DISTANCE_EPSILON);

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
