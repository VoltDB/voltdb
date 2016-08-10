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
import java.util.HashMap;
import java.util.List;
import java.util.Random;

import org.voltdb.BackendTarget;
import org.voltdb.LRRHelper;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.utils.CSVLoader;
import org.voltdb.utils.PolygonFactory;

public class TestLongRunningReadQuery extends RegressionSuite {

    private static int initTableSize = 1000;
    private static int maxSecondaryKey = 10;
    private static int genericColumnCount = 8;
    private int m_insertCount = 0;
    private int m_deleteCount = 0;
    private int m_updateCount = 0;
    private int m_errorCount = 0;
    private static int m_primaryKeyId = 1;
    private volatile boolean m_receivedResponse = false;
    private static Random m_random = new Random(1554);
    private static HashMap<String,ArrayList<Integer>> availableKeys = new HashMap<String,ArrayList<Integer>>();
    private List<GeographyPointValue> m_pointsForPolygons = new ArrayList<GeographyPointValue>();

    private final int OP_DELETE = 0;
    private final int OP_UPDATE = 1;
    private final int OP_INSERT = 2;


    private void resetStats() {

        initTableSize = initTableSize + m_insertCount - m_deleteCount;
        m_insertCount = 0;
        m_deleteCount = 0;
        m_updateCount = 0;
        m_receivedResponse = false;
        m_errorCount = 0;
    }

    private static String createRandomTupleStringWithPrimaryKey(int primaryKey) {
        String tuple = "(" + primaryKey;
        tuple += "," + m_random.nextInt(maxSecondaryKey);
        for (int i = 0; i < genericColumnCount; i++) {
            tuple += "," + m_random.nextInt();
        }
        tuple += ")";
        return tuple;
    }

    private void doRandomTableManipulation(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        int type = m_random.nextInt(3);
        if (type == OP_DELETE) {
            doRandomDelete(client, tableName);
        } else if (type == OP_UPDATE) {
            doRandomUpdate(client, tableName);
        } else if (type == OP_INSERT) {
            doRandomInsert(client, tableName);
        } else {
            assert(false);
        }
    }

    private void doRandomUpdate(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        if (availableKeys.get(tableName).isEmpty())
            return;
        int keyIdx = m_random.nextInt(availableKeys.get(tableName).size());
        String sql = "update " + tableName + " set " + tableName + ".sid = " + m_random.nextInt(maxSecondaryKey)
            + ", " + tableName + ".col2 = " + m_random.nextInt()
            + " where " + tableName + ".id = " + availableKeys.get(tableName).get(keyIdx);
        //System.out.println(sql);
        client.callProcedure("@AdHoc", sql);
        m_updateCount++;
    }

    private void doRandomInsert(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {

        String sql = "insert into " + tableName + " values " + createRandomTupleStringWithPrimaryKey(m_primaryKeyId);
        //System.out.println(sql);
        availableKeys.get(tableName).add(m_primaryKeyId);
        client.callProcedure("@AdHoc",sql);
        m_primaryKeyId++;
        m_insertCount++;
    }

    private void doRandomDelete(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        if (availableKeys.get(tableName).isEmpty())
            return;
        int keyIdx = m_random.nextInt(availableKeys.get(tableName).size());
        String sql = "delete from " + tableName + " where " + tableName + ".id = " + availableKeys.get(tableName).get(keyIdx);
        //System.out.println(sql);
        availableKeys.get(tableName).remove(keyIdx);
        client.callProcedure("@AdHoc",sql);
        m_deleteCount++;
    }

    private String loadTableAdHoc(Client client, String tableName) throws NoConnectionsException, IOException, ProcCallException {
        ArrayList<Integer> keys = new ArrayList<Integer>(initTableSize);
        availableKeys.put(tableName, keys);
        String sql = "";
        for (int i = 0; i < initTableSize; i++) {
            sql += "INSERT INTO " + tableName + " VALUES " + createRandomTupleStringWithPrimaryKey(m_primaryKeyId) + ";";
            availableKeys.get(tableName).add(m_primaryKeyId);
            m_primaryKeyId++;
        }
        client.callProcedure("@AdHoc", sql);
        return sql;
    }

    private void loadTableCSV(String tableName, String filePath) throws IOException, InterruptedException{
        String []myOptions = {
                "-f" + filePath,
                "--port=21312",
                "--limitrows=" + initTableSize,
                tableName
        };
        CSVLoader.testMode = true;
        CSVLoader.main(myOptions);

        ArrayList<Integer> keys = new ArrayList<Integer>(initTableSize);
        for (int i = 0; i < initTableSize; i++) {
            keys.add(i);
        }
        availableKeys.put(tableName, keys);

    }

    private GeographyValue generateGeoPoint() {
        final int polygonRadius = 3;
        final int numberOfPolygonPointsForEachShape = 8;
        int numOfVertexes = 5;
        double sizeOfHole = 0;
        double m_incrementFactor = 0.01;

        // generate latitude and longitudes for somewhere in north-west hemisphere
        final int longitude = m_random.nextInt(10) - 178;   // generate longitude between -178 to -168
        final int latitude = m_random.nextInt(10) + 72;     // generate latitude between 82 to 72
        GeographyPointValue center = new GeographyPointValue(longitude, latitude);
        // offset to use for generating new center of polygon. this is not randomized, it will generate polygons in same order
        // in horizontal grid running along latitude line
        final GeographyPointValue centerShiftOffset = new GeographyPointValue(0.33 * polygonRadius, 0);

        double centerLatitude =  center.getLatitude();
        double centerLongitude = center.getLongitude();

        GeographyPointValue zeroDegreePoint  = GeographyPointValue.normalizeLngLat(centerLongitude + polygonRadius,
                                                                                   centerLatitude);

        int degreeRotate = m_random.nextInt(358) + 1;
        GeographyPointValue firstVertex = zeroDegreePoint.rotate(degreeRotate, center);
        GeographyValue polygon = PolygonFactory.CreateRegularConvex(center, firstVertex, numOfVertexes, sizeOfHole);

        // Generate some points in relation to this polygon
        // fetch the vertices of the polygon
        List<GeographyPointValue> outerRing = polygon.getRings().get(0);
        outerRing.remove(0);        // drop the first vertex - first and last vertex are duplicates

        // cultivate some points that are inside the polygon
        for (GeographyPointValue geographyPointValue : outerRing) {
            m_pointsForPolygons.add(geographyPointValue.scale(center, sizeOfHole + m_incrementFactor));
        }

        // generate points inside polygon's outer shell. For simplicity, when generating points,
        // logic does not take into account if polygon has hole or not
        for (int i = 0; i < 5; i++) {
            m_pointsForPolygons.add(outerRing.get(i % numOfVertexes).scale(center, m_random.nextDouble()));
        }

        // get the point which are near the cell covering the polygon and outside of the polygon
        m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude + polygonRadius, centerLatitude + polygonRadius));
        m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude - polygonRadius, centerLatitude + polygonRadius));
        m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude - polygonRadius, centerLatitude - polygonRadius));
        m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude + polygonRadius, centerLatitude - polygonRadius));

        // point outside the cell
        m_pointsForPolygons.add(GeographyPointValue.normalizeLngLat(centerLongitude + polygonRadius + 1, centerLatitude + polygonRadius + 1));


        return polygon;
    }

    // function generates and fills table with polygon and points. For each generated polygon, there are corresponding points
    // generated inside and outside the polygon
    private void loadGeoTable(Client client) throws NoConnectionsException, IOException, ProcCallException {

        for (int i = 0; i < initTableSize; i++) {
            client.callProcedure("INDEXED_BORDERS.Insert", i, generateGeoPoint());
        }
        int pointEntries = 0;
        for(GeographyPointValue point : m_pointsForPolygons) {
            client.callProcedure("PLACES.Insert", pointEntries, point);
            ++pointEntries;
        }
    }

    public void testLongRunningReadQuery() throws IOException, ProcCallException, InterruptedException {
         System.out.println("testLongRunningReadQuery...");

         Client client = getClient();

         loadTableAdHoc(client, "R1");
         //loadGeoTable(client);

         resetStats();

         subtest1Select(client);

         resetStats();

         subtest2ConcurrentReadError(client);

         resetStats();

         subtest3Index(client);

         subtest4SecondaryIndex(client);

         // TODO: Geo type index testing
         //resetStats();
         //subtest5GeoIndex(client);

    }


    private void executeLRR(Client client, String sql) throws NoConnectionsException, IOException, ProcCallException {
        //System.out.println("LRR REF: " + sql);
        VoltTable vt = client.callProcedure("@AdHoc",sql).getResults()[0];
        int expectedRows = vt.getRowCount();
        //System.out.println("LRR REF TABLE:\n" + vt.toString());
        //System.out.println("LRR: " + sql);
        client.callProcedure(new ProcedureCallback() {
            @Override
            public void clientCallback(final ClientResponse clientResponse)
                    throws Exception {
                VoltTable vt;
                VoltTable files;

                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    System.out.println("LRR Error: " + clientResponse.getStatusString());
                    assert(clientResponse.getStatusString().equals("Concurrent @ReadOnlySlow calls are not supported."));
                    ++m_errorCount;
                    return;
                }
                m_receivedResponse = true;
                files = clientResponse.getResults()[0];
                vt = LRRHelper.getTableFromFileTable(files);
                //System.out.println("LRR RESULT TABLE:\n" + vt.toString());
                assertEquals(expectedRows,vt.getRowCount());

            }
        },"@ReadOnlySlow", sql);
    }

    public void subtest1Select(Client client) throws IOException, ProcCallException, InterruptedException {
        System.out.println("subtest1Select...");
        String sql;

        sql = "SELECT * FROM R1;";
        testLRRWithTableModifications(client,sql);
    }

    public void subtest2ConcurrentReadError(Client client) throws IOException, ProcCallException, InterruptedException {
        System.out.println("subtest2ConcurrentReadError...");
        String sql;

        sql = "SELECT * FROM R1;";
        executeLRR(client,sql);
        executeLRR(client,sql);
        while (!m_receivedResponse) {
            // Wait for LRR to finish
            Thread.sleep(1000);
        }
        assertEquals(1, m_errorCount);
        VoltTable vt = client.callProcedure("@AdHoc",sql).getResults()[0];
        assertEquals(initTableSize + m_insertCount - m_deleteCount,vt.getRowCount());
    }

    public void subtest3Index(Client client) throws IOException, ProcCallException, InterruptedException {
        System.out.println("subtest3Index...");
        String sql;

        int keyIdx = m_random.nextInt(availableKeys.get("R1").size());
        int key = availableKeys.get("R1").get(keyIdx);

        resetStats();
        sql = "SELECT * FROM R1 WHERE ID > " + key + ";";
        testLRRWithTableModifications(client,sql);

        resetStats();
        sql = "SELECT * FROM R1 WHERE ID < " + key + ";";
        testLRRWithTableModifications(client,sql);

        resetStats();
        sql = "SELECT * FROM R1 WHERE ID <= " + key + ";";
        testLRRWithTableModifications(client,sql);

        resetStats();
        sql = "SELECT * FROM R1 WHERE ID >= " + key + ";";
        testLRRWithTableModifications(client,sql);

        resetStats();
        sql = "SELECT * FROM R1 WHERE ID = " + key + ";";
        testLRRWithTableModifications(client,sql);

        resetStats();
        sql = "SELECT * FROM R1 WHERE ID = " + m_primaryKeyId*10 + ";";
        testLRRWithTableModifications(client,sql);

    }

    public void subtest4SecondaryIndex(Client client) throws IOException, ProcCallException, InterruptedException {
        System.out.println("subtest4SecondaryIndex...");
        String sql;

        int key = maxSecondaryKey/2;

        resetStats();
        sql = "SELECT * FROM R1 WHERE SID > " + key + ";";
        testLRRWithTableModifications(client,sql);

        resetStats();
        sql = "SELECT * FROM R1 WHERE SID < " + key + ";";
        testLRRWithTableModifications(client,sql);

        resetStats();
        sql = "SELECT * FROM R1 WHERE SID <= " + key + ";";
        testLRRWithTableModifications(client,sql);

        resetStats();
        sql = "SELECT * FROM R1 WHERE SID >= " + key + ";";
        testLRRWithTableModifications(client,sql);

        resetStats();
        sql = "SELECT * FROM R1 WHERE SID = " + key + ";";
        testLRRWithTableModifications(client,sql);

    }

    public void testLRRWithTableModifications(Client client, String sql) throws IOException, ProcCallException, InterruptedException {
        executeLRR(client,sql);
        while (!m_receivedResponse) {
            // Do random ad hoc queries until the long running read returns
            doRandomTableManipulation(client, "R1");

        }
        VoltTable vt = client.callProcedure("@AdHoc","SELECT * FROM R1 ORDER BY R1.ID;").getResults()[0];
        assertEquals(initTableSize + m_insertCount - m_deleteCount,vt.getRowCount());
        assertTrue(m_insertCount > 0 || m_deleteCount > 0 || m_updateCount > 0);
    }

    public void subtest5GeoIndex(Client client) throws IOException, ProcCallException, InterruptedException {
        System.out.println("subtest5GeoIndex...");
        String sql;
        m_receivedResponse = false;

        resetStats();
        sql = "Select * from INDEXED_BORDERS order by id, region;";
        executeLRR(client,sql);
        while (!m_receivedResponse) {
            Thread.sleep(1000);

        }
        resetStats();

        int key = m_random.nextInt(m_pointsForPolygons.size());
        sql = "Select A.region from INDEXED_BORDERS A "
                + "where Contains(A.region, POINTFROMTEXT('" + m_pointsForPolygons.get(key).toString() + "') ) "
                + "order by A.region;";

        executeLRR(client,sql);
        while (!m_receivedResponse) {
            Thread.sleep(1000);

        }
    }


    //
    // Suite builder boilerplate
    //

    public TestLongRunningReadQuery(String name) {
        super(name);
    }

    static public junit.framework.Test suite() {
        VoltServerConfig config = null;
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(
                TestLongRunningReadQuery.class);
        VoltProjectBuilder project = new VoltProjectBuilder();
        String literalSchema =
                "CREATE TABLE R1 ( " +
                " ID INT NOT NULL"
                + ", SID INT NOT NULL"
                + ", COL2 INT "
                + ", COL3 INT "
                + ", COL4 INT "
                + ", COL5 INT "
                + ", COL6 INT "
                + ", COL7 INT "
                + ", COL8 INT "
                + ", COL9 INT "
                + ", PRIMARY KEY (ID)"
                + ");"
                + ""
                + "CREATE TABLE R2 ( "
                + " ID INT DEFAULT 0 NOT NULL"
                + ", COL1 INT "
                + ", COL2 INT "
                + ", COL3 INT "
                + ", COL4 INT "
                + ", COL5 INT "
                + ", COL6 INT "
                + ", COL7 INT "
                + ", COL8 INT "
                + ", COL9 INT "
                + ");"
                + ""
                + "CREATE TABLE PLACES (\n"
                + "  id         INTEGER NOT NULL,\n"
                + "  loc        GEOGRAPHY_POINT\n"
                + ");\n"
                + "CREATE UNIQUE INDEX R1_TREE_1 ON R1 (ID);"
                + "CREATE INDEX R1_TREE_2 ON R1 (SID);"
                + ""
                + "CREATE TABLE INDEXED_BORDERS (\n"
                + "  id         INTEGER NOT NULL,\n"
                + "  region     GEOGRAPHY\n"
                + ");\n"
                + "CREATE INDEX INDEX_REGION ON INDEXED_BORDERS(Region)\n;"
                ;
        try {
            project.addLiteralSchema(literalSchema);
        } catch (IOException e) {
            assertFalse(true);
        }

        boolean success;
        project.setLongReadSettings(10);

        config = new LocalCluster("longreads-onesite.jar", 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        success = config.compile(project);
        assertTrue(success);
        builder.addServerConfig(config);

        return builder;
    }
}
