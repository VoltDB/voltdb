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
import java.util.ArrayList;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.utils.PolygonFactory;

import scala.util.Random;

public class TestGeospatialIndexes extends RegressionSuite{

    public TestGeospatialIndexes(String name) {
        super(name);
    }

    private class PolygonPoints {
        private GeographyValue      m_polygon;
        private GeographyPointValue m_center;
        private GeographyPointValue m_pointInside;                          // point inside polygon which is not the center
        private GeographyPointValue m_polygonCellVertex;                    // point which is the vertex of surrounding cell regular convex polygon but is outside the polygon
        private GeographyPointValue m_pointOutSidePolygonCell;              // point which is outside the polygon's surrounding cell
        private Random              m_random = new Random(999);             // used for randomly generating phi for first vertex of the polygon w.r.t center's latitude line
        private final double        m_incrementFactor = 0.01;               // used to calculate point inside polygon with or without hole

        PolygonPoints(GeographyPointValue center,
                      double radiusInDegrees,
                      int numOfVertexes,
                      double sizeOfHole) {
            assert((sizeOfHole >= 0) && (sizeOfHole + m_incrementFactor < 1));
            assert(numOfVertexes >= 3);

            m_center = center;

            GeographyPointValue zeroDegreePoint  = GeographyPointValue.normalizeLngLat(m_center.getLongitude() + radiusInDegrees,
                                                                                       m_center.getLatitude());
            // randomly generate first vertex w.r.t. center given the radius of polygon. First vertex can be placed anywhere between 1 to 359 degrees;
            int degreeRotate = m_random.nextInt(358) + 1;
            GeographyPointValue firstVertex = zeroDegreePoint.rotate(degreeRotate, m_center);
            m_polygon = PolygonFactory.CreateRegularConvex(m_center, firstVertex, 5, sizeOfHole);

            // generate point inside polygon
            m_pointInside = firstVertex.scale(m_center, sizeOfHole + m_incrementFactor);

            // get the vertex of the cell covering the polygon
            m_polygonCellVertex = GeographyPointValue.normalizeLngLat(m_center.getLongitude() + radiusInDegrees, m_center.getLatitude() + radiusInDegrees);
            // point outside the cell
            m_pointOutSidePolygonCell = GeographyPointValue.normalizeLngLat(m_center.getLongitude() + radiusInDegrees + 1, m_center.getLatitude() + radiusInDegrees + 1);
        }

        GeographyValue getPolygon() { return m_polygon; }
        GeographyPointValue getCenter() { return m_center; }
        GeographyPointValue getInsidePoint() { return m_pointInside; }
        GeographyPointValue getPolygonCellVertex() { return m_polygonCellVertex; }
        GeographyPointValue getPointOutSidePolygon() { return m_pointOutSidePolygonCell; }
    };

    private List<PolygonPoints> m_generatedPolygonPoints = new ArrayList<PolygonPoints>();

    // Polygon vertex just shy of the vertexes of it's bounding box
    static private GeographyValue polygonWithVertexNearBoundingBox
            = new GeographyValue("POLYGON((-102.001 41.001, -102.003 41.003, -107.009 41.001, -107.012 38.003, -107.009 38.001, -102.003 38.001, -102.001 38.003, -102.001 41.001,))");

    // Polygon almost in it's bounding box with 2 holes which are almost filling up most of it's bounding box
    static private GeographyValue polygonWithVertexNearBoundingBoxWith2Holes
            = new GeographyValue("POLYGON((-102.001 41.001, -102.003 41.003, -107.009 41.001, -107.012 38.003, -107.009 38.001, -102.003 38.001, -102.001 38.003, -102.001 41.001,),"
                                       + "(-105.001 40.8, -104.798 40.798, -104.798 40.298, -105.001 40.298, -105.001 40.8), "
                                       + "(-104.412 39.612, -104.412 39.412, -105.512 39.412, -105.512 39.612, -104.412 39.612), "
                                       + "(-103.001 40.8, -102.798 40.798, -102.798 40.298, -103.001 40.298, -103.001 40.8))");

    static private GeographyPointValue pointBBVertexOutsidePolygon = new GeographyPointValue(-102.001, 41.003);    // vertex of bounding box
    static private GeographyPointValue pointDisjointRegionCellNPolygon = new GeographyPointValue(-102.0011, 41.002);    // in disjoint region of polygon and bounding box
    static private GeographyPointValue pointOutSidePolygon = new GeographyPointValue(-107.015, 41.002);    // outside bounding box and polygon
    static private GeographyPointValue pointCentroidOfPolygonWithNoHole = new GeographyPointValue(-104.505, 39.517);

    static private void setupGeoSchema(VoltProjectBuilder project) throws IOException {
        String geoSchema =
                "CREATE TABLE PLACES (\n"
                + "  id         INTEGER NOT NULL,\n"
                + "  loc        GEOGRAPHY_POINT\n"
                + ");\n"
                + "CREATE TABLE BORDERS(\n"
                + "  id         INTEGER NOT NULL,\n"
                + "  region     GEOGRAPHY\n"
                + ");\n"
                + "\n"
                + "CREATE TABLE INDEXED_PLACES (\n"
                + "  id         INTEGER NOT NULL,\n"
                + "  loc        GEOGRAPHY_POINT\n"
                + ");\n"
                + "CREATE TABLE INDEXED_BORDERS (\n"
                + "  id         INTEGER NOT NULL,\n"
                + "  region     GEOGRAPHY\n"
                + ");\n"
                //+ "CREATE INDEX INDEX_LOC_INDEXED_PLACES ON INDEXED_PLACES(loc)\n;"           // Enable this once Geo Indexes are implemented
                //+ "CREATE INDEX INDEX_REGION_INDEXED_PLACES ON INDEXED_BORDERS(region)\n;"    // Enable this once Geo Indexes are implemented
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

    // generates and fills table with polygon and points. For each generated polygon, there are corresponding points
    // also generated such that:
    // - center of polygon
    // - one is inside the polygon,
    // - one is at the vertex of bounding box of polygon and
    // - another one that is outside the bounding box of polygon.
    // All the generated data is populated in borders and places tables - indexed and non-indexed version.
    // Randomly generated data is cached in m_polygonPoints list
    private void populateGeoTables(Client client) throws NoConnectionsException, IOException, ProcCallException {
        final int polygonRadius = 3;
        final int numberOfPolygonPoints = 8;

        Random rand = new Random(777);
        // generate latitude and longitudes for somewhere in north-west hemisphere
        final int longitude = rand.nextInt(10) - 178;   // generate longitude between -178 to -168
        final int latitude = rand.nextInt(10) + 72;     // generate latitude between 82 to 72
        GeographyPointValue center = new GeographyPointValue(longitude, latitude);
        // offset to use for generating new center of polygon.
        final GeographyPointValue centerShiftOffset = new GeographyPointValue(2.1 * polygonRadius, -1 * polygonRadius);

        // triangles without holes
        //System.out.println("Generate triangles without hole, center: " + center.toString());
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 3, 0, numberOfPolygonPoints);

        // triangles with holes
        //System.out.println("Generate triangles with hole, center: " + center.toString());
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 3, 0.33, numberOfPolygonPoints);

        // pentagons without holes
        //System.out.println("Generate pentagon without hole, center: " + center.toString());
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 5, 0, numberOfPolygonPoints);

        //System.out.println("Generate pentagon with hole, center: " + center.toString());
        // pentagons with holes
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 5, 0.33, numberOfPolygonPoints);

        // octagon without holes
        //System.out.println("Generate octagon without hole, center: " + center.toString());
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 8, 0, numberOfPolygonPoints);


        // octagons with holes
        //System.out.println("Generate octagon with hole, center: " + center.toString());
        center = generatePolygonPointData(center, centerShiftOffset, polygonRadius, 8, 0.33, numberOfPolygonPoints);

        //System.out.println("LAST OCTAGON CENTER: " + center.toString());

        client.callProcedure("PLACES.Insert", 0, pointCentroidOfPolygonWithNoHole);
        client.callProcedure("PLACES.Insert", 0, pointDisjointRegionCellNPolygon);
        client.callProcedure("PLACES.Insert", 0, pointBBVertexOutsidePolygon);
        client.callProcedure("PLACES.Insert", 0, pointOutSidePolygon);

        client.callProcedure("BORDERS.INSERT", 0, polygonWithVertexNearBoundingBox);
        client.callProcedure("BORDERS.INSERT", 0, polygonWithVertexNearBoundingBoxWith2Holes);

        client.callProcedure("INDEXED_PLACES.Insert", 0, pointCentroidOfPolygonWithNoHole);
        client.callProcedure("INDEXED_PLACES.Insert", 0, pointDisjointRegionCellNPolygon);
        client.callProcedure("INDEXED_PLACES.Insert", 0, pointBBVertexOutsidePolygon);
        client.callProcedure("INDEXED_PLACES.Insert", 0, pointOutSidePolygon);

        client.callProcedure("INDEXED_BORDERS.INSERT", 0, polygonWithVertexNearBoundingBox);
        client.callProcedure("INDEXED_BORDERS.INSERT", 0, polygonWithVertexNearBoundingBoxWith2Holes);

        int entryIndex = 0;
        for(PolygonPoints polygonPoints: m_generatedPolygonPoints) {
            entryIndex++;
            client.callProcedure("PLACES.Insert", entryIndex, polygonPoints.getCenter());
            client.callProcedure("PLACES.Insert", entryIndex, polygonPoints.getInsidePoint());
            client.callProcedure("PLACES.Insert", entryIndex, polygonPoints.getPolygonCellVertex());
            client.callProcedure("PLACES.Insert", entryIndex, polygonPoints.getPointOutSidePolygon());
            client.callProcedure("BORDERS.Insert", entryIndex, polygonPoints.getPolygon());

            client.callProcedure("INDEXED_PLACES.Insert", entryIndex, polygonPoints.getCenter());
            client.callProcedure("INDEXED_PLACES.Insert", entryIndex, polygonPoints.getInsidePoint());
            client.callProcedure("INDEXED_PLACES.Insert", entryIndex, polygonPoints.getPolygonCellVertex());
            client.callProcedure("INDEXED_PLACES.Insert", entryIndex, polygonPoints.getPointOutSidePolygon());
            client.callProcedure("INDEXED_BORDERS.Insert", entryIndex, polygonPoints.getPolygon());
        }
    }

    public void testContains() throws Exception{
        Client client = getClient();

        populateGeoTables(client);
        VoltTable resultsUsingGeoIndex, resultsFromNonGeoIndex;
        String sql;
        String prefixMsg;
        final double EPSILON = 1.0e-12;

        sql = "Select * from PLACES order by id, loc;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];

        sql = "Select * from INDEXED_PLACES order by id, loc;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        prefixMsg = "Assertion failed comparing contents of places and indexed_places to be same: ";
        assertTablesAreEqual(prefixMsg, resultsUsingGeoIndex, resultsFromNonGeoIndex);

        sql = "Select * from BORDERS order by id, region;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        //System.out.println("borders: " + vt1.toString());

        sql = "Select * from INDEXED_BORDERS order by id, region;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        prefixMsg = "Assertion failed comparing contents of places and indexed_borders to be same: ";
        assertTablesAreEqual(prefixMsg, resultsUsingGeoIndex, resultsFromNonGeoIndex);

        // test contains with fixed data
        sql = "Select A.id, A.region, B.loc from INDEXED_BORDERS A, INDEXED_PLACES B "
              + "where Contains(A.region, B.loc) and A.id = B.id and A.id = 0 "
              + "order by A.id, A.region, B.loc;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]{{0, polygonWithVertexNearBoundingBox,            pointCentroidOfPolygonWithNoHole}},
                                        resultsUsingGeoIndex, EPSILON);

        // match the results of indexed and non-indexed tables
        sql = "Select A.id, A.region, B.loc from BORDERS A, PLACES B "
                + "where Contains(A.region, B.loc) and A.id = B.id and A.id = 0 "
                + "order by A.id, A.region, B.loc;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        prefixMsg = "Assertion failed comparing results of Contains on fixed data set: ";
        resultsUsingGeoIndex.resetRowPosition();
        assertTablesAreEqual(prefixMsg, resultsUsingGeoIndex, resultsFromNonGeoIndex);

        // test not contains with fixed data
        sql = "Select A.id, A.region, B.loc from INDEXED_BORDERS A, INDEXED_PLACES B "
                + "where NOT Contains(A.region, B.loc) and A.id = B.id and A.id = 0 "
                + "order by A.id, A.region, B.loc;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        assertApproximateContentOfTable(new Object[][]{{0, polygonWithVertexNearBoundingBox,           pointOutSidePolygon},
                                                       {0, polygonWithVertexNearBoundingBox,           pointDisjointRegionCellNPolygon},
                                                       {0, polygonWithVertexNearBoundingBox,           pointBBVertexOutsidePolygon},
                                                       {0, polygonWithVertexNearBoundingBoxWith2Holes, pointOutSidePolygon},
                                                       {0, polygonWithVertexNearBoundingBoxWith2Holes, pointCentroidOfPolygonWithNoHole},
                                                       {0, polygonWithVertexNearBoundingBoxWith2Holes, pointDisjointRegionCellNPolygon},
                                                       {0, polygonWithVertexNearBoundingBoxWith2Holes, pointBBVertexOutsidePolygon}},
                                        resultsUsingGeoIndex, EPSILON);

        // match the results of indexed and non-indexed tables
        sql = "Select A.id, A.region, B.loc from BORDERS A, PLACES B "
              + "where NOT Contains(A.region, B.loc) and A.id = B.id and A.id = 0 "
              + "order by A.id, A.region, B.loc;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        prefixMsg = "Assertion failed comparing results of Not Contains on fixed data set: ";
        resultsUsingGeoIndex.resetRowPosition();
        assertTablesAreEqual(prefixMsg, resultsUsingGeoIndex, resultsFromNonGeoIndex);

        // Cross check Contains and NOT Contains result set from indexed and non-indexed tables
        // match the Contains results on indexed and non-indexed tables with complete data set - fixed and pseudo-random generated
        sql = "Select A.id, A.region, B.loc from INDEXED_BORDERS A, INDEXED_PLACES B "
              + "where Contains(A.region, B.loc) and A.id = B.id "
              + "order by A.id, A.region, B.loc;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        sql = "Select A.id, A.region, B.loc from BORDERS A, PLACES B "
                + "where Contains(A.region, B.loc) and A.id = B.id "
                + "order by A.id, A.region, B.loc;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        prefixMsg = "Assertion failed comparing results of Contains on indexed with non-indexed tables: ";
        resultsUsingGeoIndex.resetRowPosition();
        assertTablesAreEqual(prefixMsg, resultsFromNonGeoIndex, resultsUsingGeoIndex);

        // match the NOT Contains results on indexed and non-indexed tables with complete data set - fixed and pseudo-random generated
        sql = "Select A.id, A.region, B.loc from INDEXED_BORDERS A, INDEXED_PLACES B "
              + "where NOT Contains(A.region, B.loc) and A.id = B.id "
              + "order by A.id, A.region, B.loc;";
        resultsUsingGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        sql = "Select A.id, A.region, B.loc from BORDERS A, PLACES B "
                + "where NOT Contains(A.region, B.loc) and A.id = B.id "
                + "order by A.id, A.region, B.loc;";
        resultsFromNonGeoIndex = client.callProcedure("@AdHoc", sql).getResults()[0];
        prefixMsg = "Assertion failed comparing results of Contains on indexed with non-indexed tables: ";
        resultsUsingGeoIndex.resetRowPosition();
        assertTablesAreEqual(prefixMsg, resultsFromNonGeoIndex, resultsUsingGeoIndex);

        // Test parameterized Contains() - test with point argument to Contains() being parameterized
        // To verify this, point which is inside polygon is fetched from the cached generated data and is supplied to SP.
        // Output result of the query should have only one matching polygon that contains the supplied. This polygon is
        // same as geography value in corresponding entry of polygon-point data

        // Create procedure on Contains functions
        sql = "Create procedure PointInPolygon as select A.region from INDEXED_BORDERS A "
              + "where Contains(A.region, ?) and A.id > 0;";
        client.callProcedure("@AdHoc", sql);

        //int indexEntry = 0;
        for (PolygonPoints polygonPoints: m_generatedPolygonPoints) {
            //indexEntry ++;
            resultsUsingGeoIndex = client.callProcedure("PointInPolygon", polygonPoints.getInsidePoint()).getResults()[0];
            //System.out.println("Entry: " + indexEntry + " " + resultsUsingGeoIndex.toString() + " " + polygonPoints.getInsidePoint());
            assertApproximateContentOfTable(new Object[][]{{polygonPoints.getPolygon()}}, resultsUsingGeoIndex, EPSILON);
        }

    }
    static public junit.framework.Test suite() {
        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestGeospatialIndexes.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        try {
            VoltServerConfig config = null;
            boolean success;

            setupGeoSchema(project);
            config = new LocalCluster("geography-indexes.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
            project.setUseDDLSchema(true);
            success = config.compile(project);
            assertTrue(success);
            builder.addServerConfig(config);
        } catch (IOException except) {
            assert(false);
        }
        return builder;
    }

}
