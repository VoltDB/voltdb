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
import org.voltdb.types.GeographyValue;
import org.voltdb.types.PointType;

public class TestGeospatialFunctions extends RegressionSuite {

    public TestGeospatialFunctions(String name) {
        super(name);
    }

    private static void populateTables(Client client) throws Exception {
        client.callProcedure("places.Insert", 0, "Denver",
                PointType.pointFromText("POINT(39.704 -104.959)"));
        client.callProcedure("places.Insert", 1, "Albuquerque",
                PointType.pointFromText("POINT(35.113 -106.599)"));
        client.callProcedure("places.Insert", 2, "Cheyenne",
                PointType.pointFromText("POINT(41.134 -104.813)"));
        client.callProcedure("places.Insert", 3, "Fort Collins",
                PointType.pointFromText("POINT(40.585 -105.077)"));
        client.callProcedure("places.Insert", 4, "Point near N Colorado border",
                PointType.pointFromText("POINT(41.002 -105.04)"));
        client.callProcedure("places.Insert", 5, "Point Not On N Colorado Border",
                PointType.pointFromText("POINT(41.005 -109.025)"));
        client.callProcedure("places.Insert", 6, "Point on N Wyoming Border",
                PointType.pointFromText("POINT(44.978 -105.058)"));
        client.callProcedure("places.Insert", 7, "North Point Not On Wyoming Border",
                PointType.pointFromText("POINT(45.119 -105.060)"));
        client.callProcedure("places.Insert", 8, "Point on E Wyoming Border",
                PointType.pointFromText("POINT(42.988 -104.078)"));
        client.callProcedure("places.Insert", 9, "East Point Not On Wyoming Border",
                PointType.pointFromText("POINT(42.986 -104.061)"));
        client.callProcedure("places.Insert", 10, "Point On S Wyoming Border",
                PointType.pointFromText("POINT(41.099 -110.998)"));
        client.callProcedure("places.Insert", 11, "Point On S Colorado Border",
                PointType.pointFromText("POINT(37.002 -103.008)"));
        client.callProcedure("places.Insert", 12, "Point On W Wyoming Border",
                PointType.pointFromText("POINT(42.999 -110.998)"));
        client.callProcedure("places.Insert", 13, "West not On South Wyoming Border",
                PointType.pointFromText("POINT(41.999 -111.052)"));

        // A null-valued point
        client.callProcedure("places.Insert", 99, "Neverwhere", null);

        client.callProcedure("borders.Insert", 0, "Colorado",
                new GeographyValue("POLYGON(("
                        + "41.002 -102.052, "
                        + "41.002 -109.045,"
                        + "36.999 -109.045,"
                        + "36.999 -102.052,"
                        + "41.002 -102.052))"));
        client.callProcedure("borders.Insert", 1, "Wyoming",
                new GeographyValue("POLYGON(("
                        + "44.978 -104.061, "
                        + "44.978 -111.046, "
                        + "40.998 -111.046, "
                        + "40.998 -104.061, "
                        + "44.978 -104.061))"));

        // This polygon should not contain Denver, due to the hole.
        client.callProcedure("borders.Insert", 2, "Colorado with a hole around Denver",
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
                        + "40.240 -104.035))"));

        // a null-valued-polygon
        client.callProcedure("borders.Insert", 3, "Wonderland", null);
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

    public void testPolygonCentroidAndArea() throws Exception {
        Client client = getClient();
        populateTables(client);

        String sql = "select borders.name, Area(borders.region), LATITUDE(centroid(borders.region)), LONGITUDE(centroid(borders.region)) "
                        + "from borders order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{ "Colorado",      2.6886542912139893E11,  39.03372408765194,      -105.5485 },
                 { "Wyoming",       2.5126863189309897E11,  43.01953179182205,      -107.55349999999999 },
                 { "Colorado with a hole around Denver",
                                    2.5206603914764166E11,  38.98811213712535,      -105.5929789796371 },
                 { "Wonderland",    Double.MIN_VALUE,       Double.MIN_VALUE,       Double.MIN_VALUE},
                }, vt);
    }

    public void testPolygonDistance() throws Exception {
        Client client = getClient();
        populateTables(client);

        VoltTable vt;

        // distance of all points with respect to a specific polygon
        String sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                + "from borders, places where borders.pk = 1"
                + "order by distance, places.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
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

                }, vt);

        // Validate result set obtained using distance between point and polygon is same as
        // polygon and point
        sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                        + "from borders, places "
                        + "order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
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
        assertContentOfTable(new Object[][]
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
                }, vt);

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
            assertTrue(exception.getMessage().contains("Distance between two polygon not supported"));
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
                + "  loc POINT\n"
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
