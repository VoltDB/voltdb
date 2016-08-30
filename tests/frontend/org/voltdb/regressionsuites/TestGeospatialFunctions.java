/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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
                + "  region     GEOGRAPHY\n"
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
        Border(long pk, String name, String message, GeographyValue region) {
            m_pk = pk;
            m_name = name;
            m_region = region;
            m_message = message;
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

        private final long m_pk;
        private final String m_name;
        private final GeographyValue m_region;
        private final String m_message;
    }

    /*
     * This is the array of borders we know about. We will insert these
     * borders and then extract them.
     */
    static Border borders[] = {
        new Border(0, "Colorado", null,
                   new GeographyValue("POLYGON(("
                                      + "-102.052 41.002, "
                                      + "-109.045 41.002,"
                                      + "-109.045 36.999,"
                                      + "-102.052 36.999,"
                                      + "-102.052 41.002))")),
        new Border(1, "Wyoming", null,
                   new GeographyValue("POLYGON(("
                                      + "-104.061 44.978, "
                                      + "-111.046 44.978, "
                                      + "-111.046 40.998, "
                                      + "-104.061 40.998, "
                                      + "-104.061 44.978))")),
       new Border(2, "Colorado with a hole around Denver", null,
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
                                  + "-104.035 40.240))")),
       new Border(3, "Wonderland", null, null)
    };

    private static void populateBorders(Client client, Border borders[]) throws NoConnectionsException, IOException, ProcCallException {
        for (Border b : borders) {
            client.callProcedure("borders.Insert",
                                 b.getPk(),
                                 b.getName(),
                                 b.getMessage(),
                                 b.getRegion());
        }
    }

    private static void populateTables(Client client) throws NoConnectionsException, IOException, ProcCallException {
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

        populateBorders(client, borders);
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
                {{ "Colorado",      2.6886220370448795E11},
                 { "Wyoming",       2.512656175743851E11},
                 { "Colorado with a hole around Denver",
                                    2.5206301526291238E11},
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

        sql = "select borders.name, LATITUDE(centroid(borders.region)), LONGITUDE(centroid(borders.region)) "
                + "from borders "
                + "where borders.region is not null "
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
        populateTables(client);

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
                {{"Wyoming",    "Neverwhere",                           Double.MIN_VALUE},
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
    private static String MULTI_POLYGON
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
    private static String SHARED_INNER_VERTICES
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
    private static String SHARED_INNER_EDGES
      = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0), (.1 .1, .1 .9, .5 .9, .5 .1, .1 .1), (.5 .2, .5 .8, .9 .8, .9 .2, .5 .2))";
    // The collinear polygons are currently legal.  But they should not be.
    // So we are going to leave them here until we can figure out what to do
    // with them.
    /*
     * X-----X-----X
     */
    @SuppressWarnings("unused")
    private static String COLLINEAR3
      = "POLYGON((0 0, 1 0 , 2 0 , 0 0))";
    /*
     * X-----X-----X-----X
     */
    @SuppressWarnings("unused")
    private static String COLLINEAR4
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 0 0))";
    /*
     * X-----X-----X-----X----X
     */
    @SuppressWarnings("unused")
    private static String COLLINEAR5
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X
     */
    @SuppressWarnings("unused")
    private static String COLLINEAR6
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X----X
     */
    @SuppressWarnings("unused")
    private static String COLLINEAR7
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 6 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X----X----X
     */
    @SuppressWarnings("unused")
    private static String COLLINEAR8
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 6 0, 7 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X----X----X----X
     */
    @SuppressWarnings("unused")
    private static String COLLINEAR9
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 6 0, 7 0, 8 0, 0 0))";
    /*
     * X-----X-----X-----X----X----X----X----X----X----X
     */
    @SuppressWarnings("unused")
    private static String COLLINEAR10
      = "POLYGON((0 0, 1 0, 2 0, 3 0, 4 0, 5 0, 6 0, 7 0, 8 0, 9 0, 0 0))";
    /*
     * It's hard to draw this with ascii art, but the outer shell
     * is a rectangle, and the would-be holes are triangles.  The
     * first one, (33 67, 67 67, 50 33, 33 67), points down, and
     * the second one, (33, 50 67, 67 33, 33 33), points up.  These
     * two intersect, which is why this is not valid.
     */
    private static String INTERSECTING_HOLES
        = "POLYGON((0 0, 80 0, 80 80, 0 80, 0 0),"
               +  "(33 67, 67 67, 50 33, 33 67),"
               +  "(33 33, 50 67, 67 33, 33 33))";
    /*
     * It's not easy to see this in ascii art, but the hole
     * here leaks out of the shell to the top and right.
     */
    private static String OUTER_INNER_INTERSECT
       = "POLYGON((0 0, 1 0, 1 1, 0 1, 0 0),"
              +  "(.1 .1, .1 1.1, 1.1 1.1, 1.1 .1, .1 .1)"
              + ")";

    /*
     * These are two nested Clockwise (Sunwise) rectangles.
     */
    private static String TWO_NESTED_SUNWISE
       = "POLYGON((0.0 0.0, 0.0 1.0, 1.0 1.0, 1.0 0.0, 0.0 0.0),"
              +  "(0.1 0.1, 0.1 0.9, 0.9 0.9, 0.9 0.1, 0.1 0.1)"
              + ")";

    /*
     * These are two nested CCW (Widdershins) rectangles.
     */
    private static String TWO_NESTED_WIDDERSHINS
    = "POLYGON((0.0 0.0, 1.0 0.0, 1.0 1.0, 0.0 1.0, 0.0 0.0),"
           +  "(0.1 0.1, 0.9 0.1, 0.9 0.9, 0.1 0.9, 0.1 0.1)"
           + ")";

    private static String ISLAND_IN_A_LAKE
    = "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0),"  // This is CCW
            + "(1 1,  1 9,  9  9, 9  1, 1 1),"  // This is CW.
            + "(2 2,  2 8,  8  8, 8  2, 2 2)"   // This is CW.
            + ")";


   private static Border invalidBorders[] = {
       new Border(100, "CrossedEdges", "Edges 1 and 3 cross",
                  GeographyValue.fromWKT(CROSSED_EDGES)),
       new Border(101, "Sunwise", "Ring 0 encloses more than half the sphere",
                  GeographyValue.fromWKT(CW_EDGES)),
       new Border(102, "MultiPolygon", "Polygons can have only one shell",
                  GeographyValue.fromWKT(MULTI_POLYGON)),
       new Border(103, "SharedInnerVertices", "Ring 1 crosses ring 2",
                  GeographyValue.fromWKT(SHARED_INNER_VERTICES)),
       new Border(104, "SharedInnerEdges", "Ring 1 crosses ring 2",
                  GeographyValue.fromWKT(SHARED_INNER_EDGES)),
       new Border(105, "IntersectingHoles", "Ring 1 crosses ring 2",
                  GeographyValue.fromWKT(INTERSECTING_HOLES)),
       new Border(106, "OuterInnerIntersect", "Ring 0 crosses ring 1",
                  GeographyValue.fromWKT(OUTER_INNER_INTERSECT)),
       new Border(108, "TwoNestedSunwise", "Ring 0 encloses more than half the sphere",
                  GeographyValue.fromWKT(TWO_NESTED_SUNWISE)),
       new Border(109, "TwoNestedWiddershins", "Ring 0 encloses more than half the sphere",
                  GeographyValue.fromWKT(TWO_NESTED_WIDDERSHINS)),
       new Border(110, "IslandInALake", "Polygons can only be shells or holes",
                  GeographyValue.fromWKT(ISLAND_IN_A_LAKE)),
      /*
       * These are apparently legal. Should they be?
       */
    // new Border(205, "Collinear3", null,          GeographyValue.geographyValueFromText(COLLINEAR3)),
    // new Border(206, "Collinear4", null,          GeographyValue.geographyValueFromText(COLLINEAR4)),
    // new Border(207, "Collinear5", null,          GeographyValue.geographyValueFromText(COLLINEAR5)),
    // new Border(208, "Collinear6", null,          GeographyValue.geographyValueFromText(COLLINEAR6)),
    // new Border(209, "Collinear7", null,          GeographyValue.geographyValueFromText(COLLINEAR7)),
    // new Border(210, "Collinear8", null,          GeographyValue.geographyValueFromText(COLLINEAR8)),
    // new Border(211, "Collinear9", null,          GeographyValue.geographyValueFromText(COLLINEAR9)),
    // new Border(212, "Collinear10", null,         GeographyValue.geographyValueFromText(COLLINEAR10)),
    };

    public void testInvalidPolygons() throws Exception {
        Client client = getClient();
        populateBorders(client, invalidBorders);

        VoltTable vt = client.callProcedure("@AdHoc", "select pk, name from borders where isValid(region)").getResults()[0];
        StringBuffer sb = new StringBuffer("Expected no polygons in the invalid polygons table, found: ");
        long rowCount = vt.getRowCount();

        String sep = "";
        while (vt.advanceRow()) {
            sb.append(sep).append(vt.getString(1));
            sep = ", ";
        }
        assertEquals(sb.toString(), 0, rowCount);
    }

    public void testInvalidPolygonReasons() throws Exception {
        Client client = getClient();
        populateBorders(client, invalidBorders);

        VoltTable vt = client.callProcedure("@AdHoc", "select pk, name, isinvalidreason(region), message from borders").getResults()[0];
        while (vt.advanceRow()) {
            long pk = vt.getLong(0);
            String expected = vt.getString(3);
            String actual = vt.getString(2);
            assertTrue(String.format("Border %s, key %d, Expected error message containing \"%s\" but got \"%s\"",
                                       vt.getString(1),
                                       pk,
                                       expected,
                                       actual),
                         vt.getString(2).equals(vt.getString(3)));
        }
    }

    public void testValidPolygonFromText() throws Exception {
        Client client = getClient();
        populateBorders(client, invalidBorders);
        // These should all fail.
        for (Border b : invalidBorders) {
            String expectedPattern = b.getMessage();
            String sql = String.format("select validpolygonfromtext('%s') from borders where pk = 100",
                                       b.getRegion().toWKT());
            verifyStmtFails(client, sql, expectedPattern);
        }
        // These should all succeed.
        for (Border b : borders) {
            if (b.getRegion() != null) {
                String stmt = String.format("select validpolygonfromtext('%s') from borders where pk = 100",
                                            b.getRegion().toWKT());
                ClientResponse cr = client.callProcedure("@AdHoc", stmt);
                assertEquals(ClientResponse.SUCCESS, cr.getStatus());
                VoltTable vt = cr.getResults()[0];
                while (vt.advanceRow()) {
                    assertEquals(b.getRegion(), vt.getGeographyValue(0));
                }
            }
        }
    }

    public void testPointAsText() throws Exception {
        Client client = getClient();
        populateTables(client);

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
        populateTables(client);
        // polygon whose co-ordinates are mix of decimal and whole numbers - test
        // decimal rounding border cases
        Border someWhere = new Border(50, "someWhere", "someWhere",
                new GeographyValue("POLYGON ((-10.1234567891234 10.1234567891234, " +
                                             "-14.1234567891264 10.1234567891234, " +
                                             "-14.0 4.1234567891235, " +
                                             "-12.0 4.4555555555555555550, " +
                                             "-11.0 4.4999999999996, " +
                                             "-10.1234567891234 10.1234567891234))"));
        VoltTable vt = client.callProcedure("BORDERS.Insert",
                someWhere.getPk(), someWhere.getName(), someWhere.getMessage(), someWhere.getRegion()).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});
        // polygon with 2 holes and whose vertices are whole numbers - test for whole number rounding
        someWhere = new Border(51, "someWhereWithHoles", "someWhereWithHoles",
                new GeographyValue("POLYGON ((10 10, -10 10, -10 1, 10 1, 10 10)," +
                                            "(-8 9, -8 8, -9 8, -9 9, -8 9)," +
                                            "(9 9,  9 8, 8 8, 8 9, 9 9))"));
        vt = client.callProcedure("BORDERS.Insert",
                someWhere.getPk(), someWhere.getName(), someWhere.getMessage(), someWhere.getRegion()).getResults()[0];
        validateTableOfScalarLongs(vt, new long[] {1});

        // polygon with hole whose co-ordinates are whole numbers
        someWhere = new Border(52, "someWhereWithHoles", "someWhereWithHoles",
                new GeographyValue("POLYGON ((10 10, -10 10, -10 1, 10 1, 10 10)," +
                                            "(9 9, 9 8, 8 8, 8 9, 9 9)," +
                                            "(-8 9, -8 8, -9 8, -9 9, -8 9))"));
        vt = client.callProcedure("BORDERS.Insert",
                someWhere.getPk(), someWhere.getName(), someWhere.getMessage(), someWhere.getRegion()).getResults()[0];
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
        populateTables(client);

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
        populateTables(client);
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
    }

    public void testPolygonPointDWithinNegative() throws Exception {
        Client client = getClient();
        populateTables(client);

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
