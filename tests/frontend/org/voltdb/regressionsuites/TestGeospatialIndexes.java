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
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.utils.PolygonFactory;

public class TestGeospatialIndexes extends RegressionSuite{

    private Random m_random = new Random(777);

    public TestGeospatialIndexes(String name) {
        super(name);
    }

    private class PolygonPoints {
        private GeographyValue      m_polygon;
        private GeographyPointValue m_center;
        // stores that are inside and outside polygons
        private List<GeographyPointValue> m_pointsForPolygons = new ArrayList<GeographyPointValue>();
        private final double        m_incrementFactor = 0.01;               // used to calculate point inside polygon with or without hole

        PolygonPoints(GeographyPointValue center,
                      double radiusInDegrees,
                      int numOfVertexes,
                      double sizeOfHole) {
            assert((sizeOfHole >= 0) && (sizeOfHole + m_incrementFactor < 1));
            assert(numOfVertexes >= 3);

            m_center = center;
            int numbOfAdditionalPointsInsideShell = 8;

            if (isValgrind()) {
                // generate only limited number of points for test in valgrind environment
                // else the resultant result set for not contains is too large for IPC backend
                // in valgrind environment to handle
                numbOfAdditionalPointsInsideShell = 4;
            }

            double centerLatitude =  m_center.getLatitude();
            double centerLongitude = m_center.getLongitude();

            GeographyPointValue zeroDegreePoint  = GeographyPointValue.normalizeLngLat(centerLongitude + radiusInDegrees,
                                                                                       centerLatitude);
            // generate a random first vertex w.r.t. center given the distance of vertex from center of polygon.
            // First vertex can be placed anywhere between 1 to 359 degrees;
            int degreeRotate = m_random.nextInt(358) + 1;
            GeographyPointValue firstVertex = zeroDegreePoint.rotate(degreeRotate, m_center);
            m_polygon = PolygonFactory.CreateRegularConvex(m_center, firstVertex, numOfVertexes, sizeOfHole);
            assert(m_polygon.getRings().size() > 0);

            // fetch the vertices of the polygon
            List<GeographyPointValue> outerRing = m_polygon.getRings().get(0);
            outerRing.remove(0);        // drop the first vertex - first and last vertex are duplicates
            assert (outerRing.size() == numOfVertexes);

            // cultivate some points that are inside the polygon
            for (GeographyPointValue geographyPointValue : outerRing) {
                m_pointsForPolygons.add(geographyPointValue.scale(m_center, sizeOfHole + m_incrementFactor));
            }

            // generate points inside polygon's outer shell. For simplicity, when generating points,
            // logic does not take into account if polygon has hole or not
            for (int i = 0; i < numbOfAdditionalPointsInsideShell; i++) {
                m_pointsForPolygons.add(outerRing.get(i % numOfVertexes).scale(m_center, m_random.nextDouble()));
            }

            // get the point which are near the cell covering the polygon and outside of the polygon
            m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude + radiusInDegrees, centerLatitude + radiusInDegrees));
            m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude - radiusInDegrees, centerLatitude + radiusInDegrees));
            m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude - radiusInDegrees, centerLatitude - radiusInDegrees));
            m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude + radiusInDegrees, centerLatitude - radiusInDegrees));

            // point outside the cell
            m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude + radiusInDegrees + 1, centerLatitude + radiusInDegrees + 1));
        }

        GeographyValue getPolygon() { return m_polygon; }
        List<GeographyPointValue> getPoints() { return m_pointsForPolygons; }
    };

    private List<PolygonPoints> m_generatedPolygonPoints = new ArrayList<PolygonPoints>();

    // Polygon vertex just shy of the vertexes of it's bounding box
    static private GeographyValue fixedPolygonWithVertexNearBoundingBox
            = new GeographyValue("POLYGON((-102.001 41.001, -102.003 41.003, -107.009 41.001, -107.012 38.003, -107.009 38.001, -102.003 38.001, -102.001 38.003, -102.001 41.001,))");

    // Polygon with couples of holes formed using same outer ring as polygon above
    static private GeographyValue fixedPolygonWithHolesWithVertexNearBoundingBox
            = new GeographyValue("POLYGON((-102.001 41.001, -102.003 41.003, -107.009 41.001, -107.012 38.003, -107.009 38.001, -102.003 38.001, -102.001 38.003, -102.001 41.001,),"
                                       + "(-105.001 40.8, -104.798 40.798, -104.798 40.298, -105.001 40.298, -105.001 40.8), "
                                       + "(-104.412 39.612, -104.412 39.412, -105.512 39.412, -105.512 39.612, -104.412 39.612), "
                                       + "(-103.001 40.8, -102.798 40.798, -102.798 40.298, -103.001 40.298, -103.001 40.8))");

    static private GeographyPointValue fixedPointOnBBVertexOutsidePolygon = new GeographyPointValue(-102.001, 41.003);      // vertex of bounding box
    static private GeographyPointValue fixedPointInDisjointRegionCellNPolygon = new GeographyPointValue(-102.0011, 41.002);        // in disjoint region of polygon and bounding box
    static private GeographyPointValue fixedPointOutsidePolygon = new GeographyPointValue(-107.015, 41.002);                // outside bounding box and polygon
    static private GeographyPointValue fixedPointCentroidOfPolygonWithNoHole = new GeographyPointValue(-104.505, 39.517);

    static private void setupGeoSchema(VoltProjectBuilder project) throws IOException {
        String geoSchema =
                "CREATE TABLE PLACES (\n"
                + "  id         INTEGER NOT NULL,\n"
                + "  loc        GEOGRAPHY_POINT\n"
                + ");\n"
                + "PARTITION TABLE PLACES ON COLUMN ID;\n"
                + "CREATE TABLE BORDERS(\n"
                + "  id         INTEGER NOT NULL,\n"
                + "  region     GEOGRAPHY\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE INDEXED_BORDERS (\n"
                + "  id         INTEGER NOT NULL,\n"
                + "  region     GEOGRAPHY\n"
                + ");\n"
                + "CREATE INDEX INDEX_REGION ON INDEXED_BORDERS(Region)\n;"
                + "CREATE PROCEDURE P_CONTAINS_INDEXED AS "
                + "  SELECT A.Region FROM INDEXED_BORDERS A \n"
                + "         WHERE CONTAINS(A.region, ?) ORDER BY A.Region;\n"
                + "CREATE PROCEDURE P_CONTAINS AS "
                + "  SELECT A.Region FROM BORDERS A \n"
                + "         WHERE CONTAINS(A.region, ?) ORDER BY A.Region;\n"
                + "CREATE PROCEDURE P_NOT_CONTAINS_INDEXED AS "
                + "  SELECT A.Region FROM INDEXED_BORDERS A \n"
                + "         WHERE NOT CONTAINS(A.region, ?) ORDER BY A.Region;\n"
                + "CREATE PROCEDURE P_NOT_CONTAINS AS "
                + "  SELECT A.Region FROM BORDERS A \n"
                + "         WHERE NOT CONTAINS(A.region, ?) ORDER BY A.Region;\n"
                + "\n"
                ;
        project.addLiteralSchema(geoSchema);
    }

    // function generates n number of uniform regular convex polygons with specified number of sides, radius and a hole. For
    // each new polygon the center is shifted by an offset provided by caller.
    // returns center of the last generated polygon
    private GeographyPointValue generatePolygonPointData(GeographyPointValue center, GeographyPointValue centerShiftOffset,
                                          double radiusInDegrees, int numOfVertexes, double sizeOfHole,
                                          int numOfPolygonPoints) {
        for(int generatedPolygons = 0; generatedPolygons < numOfPolygonPoints; generatedPolygons++) {
            // shift center for next polygon
            center = center.add(centerShiftOffset);
            m_generatedPolygonPoints.add(new PolygonPoints(center, radiusInDegrees, numOfVertexes, sizeOfHole));
        }
        return center;
    }

    // function generates and fills table with polygon and points. For each generated polygon, there are corresponding points
    // generated such that:
    // - n points that are inside polygon's (this number is equal to number of vertex the polygon has)
    // - 8 random points (4 in valgrind env) that are inside polygon's outer shell and along the axis line of vertex
    // - 4 points that are outside polygon and near cell covering polygon
    // - another one that is outside the bounding box of polygon.
    // All the generated data is populated in (indexed/non-indexed) borders and places tables.
    // Polygons are generated along horizontal grid
    // Generated data is cached in m_polygonPoints list and used in data verification later
    private void populateGeoTables(Client client) throws NoConnectionsException, IOException, ProcCallException {
        final int polygonRadius = 3;
        final int numberOfPolygonPointsForEachShape;
        if (isValgrind()) {
            // limit the data for valgrind environment as IPC can't handle data beyond 5000 rows.
            // Not Contains query hangs otherwise in valgrind environment
            numberOfPolygonPointsForEachShape = 2;
        }
        else {
            numberOfPolygonPointsForEachShape = 10;
        }

        // generate latitude and longitudes for somewhere in north-west hemisphere
        final int longitude = m_random.nextInt(10) - 178;   // generate longitude between -178 to -168
        final int latitude = m_random.nextInt(10) + 72;     // generate latitude between 82 to 72
        GeographyPointValue center = new GeographyPointValue(longitude, latitude);
        // offset to use for generating new center of polygon. this is not randomized, it will generate polygons in same order
        // in horizontal grid running along latitude line
        final GeographyPointValue centerShiftOffset = new GeographyPointValue(0.33 * polygonRadius, 0);

        // triangles without holes
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 3, 0, numberOfPolygonPointsForEachShape);

        // triangles with holes
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 3, 0.33, numberOfPolygonPointsForEachShape);

        // pentagons without holes
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 5, 0, numberOfPolygonPointsForEachShape);

        // pentagons with holes
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 5, 0.33, numberOfPolygonPointsForEachShape);

        // octagon without holes
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 8, 0, numberOfPolygonPointsForEachShape);

        // octagons with holes
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 8, 0.33, numberOfPolygonPointsForEachShape);

        int polygonEntries = 0;
        int pointEntries = 0;
        List<GeographyPointValue> listOfPoints;
        for(PolygonPoints polygonPoints: m_generatedPolygonPoints) {
            listOfPoints = polygonPoints.getPoints();
            for(GeographyPointValue point : listOfPoints) {
                client.callProcedure("PLACES.Insert", pointEntries, point);
                pointEntries++;
            }
            client.callProcedure("BORDERS.Insert", polygonEntries, polygonPoints.getPolygon());
            client.callProcedure("INDEXED_BORDERS.Insert", polygonEntries, polygonPoints.getPolygon());
            polygonEntries++;
        }
    }

    private void populateGeoTableWithFixedData(Client client) throws NoConnectionsException, IOException, ProcCallException {
        client.callProcedure("PLACES.Insert", 0, fixedPointCentroidOfPolygonWithNoHole);
        client.callProcedure("PLACES.Insert", 1, fixedPointInDisjointRegionCellNPolygon);
        client.callProcedure("PLACES.Insert", 2, fixedPointOnBBVertexOutsidePolygon);
        client.callProcedure("PLACES.Insert", 3, fixedPointOutsidePolygon);

        client.callProcedure("BORDERS.INSERT", 0, fixedPolygonWithVertexNearBoundingBox);
        client.callProcedure("BORDERS.INSERT", 1, fixedPolygonWithHolesWithVertexNearBoundingBox);

        client.callProcedure("INDEXED_BORDERS.INSERT", 0, fixedPolygonWithVertexNearBoundingBox);
        client.callProcedure("INDEXED_BORDERS.INSERT", 1, fixedPolygonWithHolesWithVertexNearBoundingBox);
    }

    // verifies the data in indexed and non-indexed borders table is same
    private void subTestVerifyBordersData(Client client) throws NoConnectionsException, IOException, ProcCallException {
        VoltTable resultsUsingGeoIndex, resultsFromNonGeoIndex;
        String sql;
        String prefixMsg;

        sql = "Select * from BORDERS order by id, region;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];

        sql = "Select * from INDEXED_BORDERS order by id, region;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        prefixMsg = "Assertion failed comparing contents of places and indexed_borders to be same: ";
        assertTablesAreEqual(prefixMsg, resultsUsingGeoIndex, resultsFromNonGeoIndex);
    }

    // simple test contains and not contains with tables populate with fixed data
    public void testContainsWithFixedData() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("Starting tests Contains() with fixed data ... ");

        VoltTable resultsUsingGeoIndex, resultsFromNonGeoIndex;
        String sql;
        String prefixMsg;
        final double EPSILON = 1.0e-12;
        Client client = getClient();

        populateGeoTableWithFixedData(client);
        subTestVerifyBordersData(client);

        // Test contains with fixed data
        sql = "Select A.region, B.loc from INDEXED_BORDERS A, PLACES B "
              + "where Contains(A.region, B.loc) "
              + "order by A.region, B.loc;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]{{fixedPolygonWithVertexNearBoundingBox,            fixedPointCentroidOfPolygonWithNoHole}},
                                        resultsUsingGeoIndex, EPSILON);
        resultsUsingGeoIndex.resetRowPosition();

        // match the results of indexed and non-indexed tables
        prefixMsg = "Assertion failed comparing results of Contains on fixed data set: ";
        sql = "Select A.region, B.loc from BORDERS A, PLACES B "
                + "where Contains(A.region, B.loc) "
                + "order by A.region, B.loc;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTablesAreEqual(prefixMsg, resultsUsingGeoIndex, resultsFromNonGeoIndex);

        // test not contains with fixed data
        sql = "Select A.region, B.loc from INDEXED_BORDERS A, PLACES B "
                + "where NOT Contains(A.region, B.loc) "
                + "order by A.region, B.loc;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]{{fixedPolygonWithVertexNearBoundingBox,           fixedPointOutsidePolygon},
                                                       {fixedPolygonWithVertexNearBoundingBox,           fixedPointInDisjointRegionCellNPolygon},
                                                       {fixedPolygonWithVertexNearBoundingBox,           fixedPointOnBBVertexOutsidePolygon},
                                                       {fixedPolygonWithHolesWithVertexNearBoundingBox, fixedPointOutsidePolygon},
                                                       {fixedPolygonWithHolesWithVertexNearBoundingBox, fixedPointCentroidOfPolygonWithNoHole},
                                                       {fixedPolygonWithHolesWithVertexNearBoundingBox, fixedPointInDisjointRegionCellNPolygon},
                                                       {fixedPolygonWithHolesWithVertexNearBoundingBox, fixedPointOnBBVertexOutsidePolygon}},
                                        resultsUsingGeoIndex, EPSILON);
        resultsUsingGeoIndex.resetRowPosition();

        // match the results of indexed and non-indexed tables
        prefixMsg = "Assertion failed comparing results of Not Contains on fixed data set: ";
        sql = "Select A.region, B.loc from BORDERS A, PLACES B "
              + "where NOT Contains(A.region, B.loc) "
              + "order by A.region, B.loc;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTablesAreEqual(prefixMsg, resultsUsingGeoIndex, resultsFromNonGeoIndex);
        System.out.println("... completed testing Contains() with fixed data");
    }

    private void subTestParameterizedContains(Client client, Boolean testContains) throws NoConnectionsException, IOException, ProcCallException {
        VoltTable resultsUsingGeoIndex, resultsFromNonGeoIndex;
        String indexedProcName, nonIndexProcName, predicate;
        String prefixMsg;

        if (testContains) {
            indexedProcName = "P_CONTAINS_INDEXED";
            nonIndexProcName = "P_CONTAINS";
            predicate = "Contains";
        }
        else {
            indexedProcName = "P_NOT_CONTAINS_INDEXED";
            nonIndexProcName = "P_NOT_CONTAINS";
            predicate = "Not Contains";
        }

        int maxPolygonsContainSamePoint = 0;
        prefixMsg = "Assertion failed comparing results of "+ predicate +" on indexed with non-indexed tables: ";
        List<GeographyPointValue> points;
        for (PolygonPoints polygonPoints: m_generatedPolygonPoints) {
            points =  polygonPoints.getPoints();
            for(GeographyPointValue point : points) {
                resultsUsingGeoIndex = client.callProcedure(indexedProcName, point).getResults()[0];
                resultsFromNonGeoIndex = client.callProcedure(nonIndexProcName, point).getResults()[0];
                assertTablesAreEqual(prefixMsg, resultsFromNonGeoIndex, resultsUsingGeoIndex);

                maxPolygonsContainSamePoint = (maxPolygonsContainSamePoint < resultsUsingGeoIndex.getRowCount()) ?
                        resultsUsingGeoIndex.getRowCount() : maxPolygonsContainSamePoint;
            }
        }

        System.out.println("Max polygons for predicate '" + predicate +"': " + maxPolygonsContainSamePoint);
    }

    public void testContains() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("Starting tests for Contains() ... ");

        VoltTable resultsUsingGeoIndex, resultsFromNonGeoIndex;
        String sql;
        String prefixMsg;

        Client client = getClient();
        populateGeoTables(client);
        subTestVerifyBordersData(client);

        // Cross check Contains and NOT Contains result set from indexed and non-indexed tables

        if(isValgrind()) {
            System.out.println("*******Executing CONTAINS" );
        }

        // Cross check Contains and NOT Contains result set from indexed and non-indexed tables
        // match the Contains results on indexed and non-indexed tables
        prefixMsg = "Assertion failed comparing CONTAINS results of indexed with non-indexed tables: ";
        sql = "Select A.region, B.loc from INDEXED_BORDERS A, PLACES B "
                + "where CONTAINS(A.region, B.loc) "
                + "order by A.region, B.loc;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        sql = "Select A.region, B.loc from BORDERS A, PLACES B "
                + "where CONTAINS(A.region, B.loc) "
                + "order by A.region, B.loc;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTablesAreEqual(prefixMsg, resultsFromNonGeoIndex, resultsUsingGeoIndex);

        if(isValgrind()) {
            System.out.println("*******Executing NOT CONTAINS" );
        }

        sql = "Select A.region, B.loc from INDEXED_BORDERS A, PLACES B "
                + "where NOT CONTAINS(A.region, B.loc) "
                + "order by A.id, B.id;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        sql = "Select A.region, B.loc from BORDERS A, PLACES B "
                + "where NOT CONTAINS(A.region, B.loc) "
                + "order by A.id, B.id;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertTablesAreEqual(prefixMsg, resultsFromNonGeoIndex, resultsUsingGeoIndex);


        // Test parameterized Contains() - test with point argument to Contains() being parameterized
        // To verify this, point which is inside polygon is fetched from the cached generated data and is supplied to SP.
        // Output result of the query should have only one matching polygon that contains the supplied. This polygon is
        // same as geography value in corresponding entry of polygon-point data
        if (isValgrind()) {
            System.out.println("Test parameterized contains() ... ");
        }
        subTestParameterizedContains(client, true);
        subTestParameterizedContains(client, false);

        System.out.println(" ... completed tests for Contains().");
    }


    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestGeospatialIndexes.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        try {
            VoltServerConfig config = null;
            boolean success;

            setupGeoSchema(project);
            project.setUseDDLSchema(true);

            config = new LocalCluster("geography-indexes.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);

            config = new LocalCluster("geography-indexes.jar", 3, 1, 0, BackendTarget.NATIVE_EE_JNI);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);

        } catch (IOException except) {
            assert(false);
        }
        return builder;
    }
}
