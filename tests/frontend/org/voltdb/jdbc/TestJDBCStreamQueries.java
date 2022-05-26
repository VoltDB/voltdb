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

package org.voltdb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ClientConfig;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestJDBCStreamQueries {
    private static final String TEST_XML = "jdbcstreamstest.xml";
    private static final String TEST_JAR = "jdbcstreamstest.jar";
    static String testjar;
    static ServerThread server;
    static Connection conn;
    static VoltProjectBuilder pb;

    public static final String stream_schema =
            "CREATE STREAM stream1 PARTITION ON COLUMN state " +
            "(" +
            "  id     integer     NOT NULL" +
            ", name   varchar(50) NOT NULL" +
            ", state  varchar(50) NOT NULL" +
            ");" +
            "CREATE VIEW count_by_state " +
            "(state, count_value) " +
            "AS SELECT state, count(*) FROM stream1 GROUP BY state;";

    @BeforeClass
    public static void setUp() throws Exception {
        pb = new VoltProjectBuilder();
        pb.setUseDDLSchema(true);
        pb.addLiteralSchema(stream_schema);
        boolean success = pb.compile(Configuration.getPathToCatalogForTest(TEST_JAR), 3, 1, 0);
        assert(success);
        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest(TEST_XML));
        testjar = Configuration.getPathToCatalogForTest(TEST_JAR);

        // Set up ServerThread and Connection
        startServer();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        stopServer();
        File f = new File(testjar);
        f.delete();
    }

    @After
    public void clearStreamView() throws Exception
    {
        PreparedStatement stmt= conn.prepareStatement("delete from count_by_state");
        stmt.execute();
    }

    private static void startServer() throws ClassNotFoundException, SQLException {
        server = new ServerThread(testjar, pb.getPathToDeployment(),
                                  BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();

        Class.forName("org.voltdb.jdbc.Driver");
        if (ClientConfig.ENABLE_SSL_FOR_TEST) {
            conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212?" + JDBCTestCommons.SSL_URL_SUFFIX);
        }
        else {
            conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
        }
    }

    private static void stopServer() throws SQLException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
        if (server != null) {
            try { server.shutdown(); } catch (InterruptedException e) { /*empty*/ }
            server = null;
        }
    }

    @Test
    public void testStreams() throws Exception
    {
        String[][] data = {
                { "1", "Johnny Cash", "TN" },
                { "2", "Taylor Swift", "TN" },
                { "3", "Meghan Trainor", "MA" },
        };
        Map<String, Integer> countByState = populateStream(data);

        // select, update and delete on streams should fail
        runQueryExpectFailure("select * from stream1");
        runQueryExpectFailure(String.format("UPDATE stream1 SET name='%s' WHERE id=1", "cashed"));
        runQueryExpectFailure("DELETE FROM stream1 WHERE id=1");

        verifyStreamViewSelect(countByState);

        // update stream view
        String query = "UPDATE count_by_state SET count_value=0 WHERE state=?";
        String state = "TN";
        PreparedStatement stmt = conn.prepareStatement(query);
        stmt.setString(1, state);
        stmt.execute();
        int count = stmt.getUpdateCount();
        assertEquals(1, count);
        countByState.put(state, 0);
        verifyStreamViewSelect(countByState);

        // now insert and see if count matches
        insertToStream(new String[] { "4",  "Miley Cyrus", "TN" }, countByState);
        verifyStreamViewSelect(countByState);

        // delete and verify
        query = "DELETE FROM count_by_state WHERE state=?";
        stmt = conn.prepareStatement(query);
        stmt.setString(1, state);
        stmt.execute();
        count = stmt.getUpdateCount();
        assertEquals(1, count);
        countByState.remove(state);
        verifyStreamViewSelect(countByState);

        // one more insert
        insertToStream(new String[] { "5",  "Blake Shelton", "TN" }, countByState);
        verifyStreamViewSelect(countByState);
    }

    private void verifyStreamViewSelect(Map<String, Integer> countByState) throws Exception
    {
        String query = "SELECT state, count_value FROM count_by_state";
        PreparedStatement stmt = conn.prepareStatement(query);
        ResultSet rs = stmt.executeQuery();
        int foundCount = 0;
        while (rs.next()) {
            String state = rs.getString(1);
            assertTrue(countByState.get(state)!=null);
            assertEquals(countByState.get(state).intValue(), rs.getInt(2));
            foundCount++;
        }
        assertEquals(countByState.size(), foundCount);
    }

    private Map<String, Integer> populateStream(String[][] data) throws Exception
    {
        Map<String, Integer> countByState = new HashMap<>();
        for (String[] row : data) {
            insertToStream(row, countByState);
        }

        return countByState;
    }

    private void insertToStream(String[] row, Map<String, Integer> countByState) throws Exception
    {
        PreparedStatement stmt = conn.prepareStatement("insert into stream1 values(?, ?, ?)");
        for (int i=0; i<row.length; i++) {
            stmt.setString(i+1, row[i]);
        }
        stmt.execute();
        int count = stmt.getUpdateCount();
        assertTrue(count==1);
        if (countByState.get(row[2]) == null) {
            countByState.put(row[2], 1);
        } else {
            countByState.put(row[2], countByState.get(row[2])+1);
        }
        stmt.close();
    }

    private void runQueryExpectFailure(String query) throws Exception
    {
        PreparedStatement stmt = conn.prepareStatement(query);
        try {
            stmt.executeQuery();
            fail("Expected failure on query [" + query + "]");
        } catch(SQLException e) {
            // expected
        }
    }
}
