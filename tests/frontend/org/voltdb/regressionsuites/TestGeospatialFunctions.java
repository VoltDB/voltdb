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
import org.voltdb.VoltType;
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

        // A null-valued point
        client.callProcedure("places.Insert", 4, "Neverwhere", null);

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
                {{"Denver",         39.70399856567383,  -104.95899963378906},
                 {"Albuquerque",    35.112998962402344, -106.5989990234375},
                 {"Cheyenne",       41.13399887084961,  -104.81300354003906},
                 {"Fort Collins",   40.584999084472656, -105.0770034790039},
                 {"Neverwhere",     Double.MIN_VALUE,   Double.MIN_VALUE},
                }, vt);

        sql = "select places.name, LATITUDE(places.loc), LONGITUDE(places.loc) "
                + "from places, borders "
                + "where contains(borders.region, places.loc) "
                + "group by places.name, places.loc "
                + "order by places.name";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertContentOfTable(new Object[][]
                {{"Cheyenne",       41.13399887084961,  -104.81300354003906},
                 {"Denver",         39.70399856567383,  -104.95899963378906},
                 {"Fort Collins",   40.584999084472656, -105.0770034790039}
                }, vt);
    }

    public void testPolygonCentroidAndArea() throws Exception {
        Client client = getClient();
        populateTables(client);

        String sql = "select borders.name, Area(borders.region), LATITUDE(centroid(borders.region)), LONGITUDE(centroid(borders.region)) "
                        + "from borders order by borders.pk";
        VoltTable vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt.toString());
    }

    public void testPolygonDistance() throws Exception {
        Client client = getClient();
        populateTables(client);

        client.callProcedure("places.Insert", 5, "Point almost on Colorado border",
                PointType.pointFromText("POINT(41.002 -105.04)"));

        client.callProcedure("places.Insert", 6, "Point Not On Colorado Border",
                PointType.pointFromText("POINT(41.005 -109.025)"));

        VoltTable vt;

        // distance between polygon and point
        String sql = "select borders.name, places.name, distance(borders.region, places.loc) as distance "
                        + "from borders, places "
                        + "order by borders.pk";
        /*
         * String sql = "select places.name, distance(borders.region, places.loc)"
                        + "from borders, places where borders.pk = places.pk "
                        + "order by borders.pk";
         */
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt.toString());

        // distance between point and polygon
        sql = "select places.name, distance(places.loc, borders.region)"
                + "from borders, places where borders.pk = places.pk "
                + "order by borders.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt.toString());

        // distance between point and point
        sql = "select A.name, B.name, distance(A.loc, B.loc)"
                + "from places as A, places as B "
                + "order by A.pk";
        vt = client.callProcedure("@AdHoc", sql).getResults()[0];
        System.out.println(vt.toString());

        // distance between polygon and polygon
        ProcCallException exception = null;
        try {
            sql = "select places.name, distance(borders.region, borders.region)"
                    + "from borders, places where borders.pk = places.pk "
                    + "order by borders.pk";
            vt = client.callProcedure("@AdHoc", sql).getResults()[0];
            System.out.println(vt.toString());
        }
        catch (ProcCallException excp) {
            exception = excp;
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
