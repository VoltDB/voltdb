/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.TestClientFeatures;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestJDBCReconnect {
    static String testjar;
    static ServerThread server;
    static Connection conn;
    static Connection myconn;
    static VoltProjectBuilder pb;

    @BeforeClass
    public static void setUp() throws Exception {
        // Fake out the constraints that were previously written against the
        // TPCC schema
        String ddl =
            "CREATE TABLE TT(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE ORDERS(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE UNIQUE INDEX UNIQUE_ORDERS_HASH ON ORDERS (A1, A2_ID); " +
            "CREATE INDEX IDX_ORDERS_HASH ON ORDERS (A2_ID);";


        pb = new VoltProjectBuilder();
        pb.addLiteralSchema(ddl);
        pb.addSchema(TestClientFeatures.class.getResource("clientfeatures.sql"));
        pb.addProcedures(ArbitraryDurationProc.class);
        pb.addPartitionInfo("TT", "A1");
        pb.addPartitionInfo("ORDERS", "A1");
        pb.addStmtProcedure("InsertA", "INSERT INTO TT VALUES(?,?);", "TT.A1: 0");
        pb.addStmtProcedure("SelectB", "SELECT * FROM TT;");
        boolean success = pb.compile(Configuration.getPathToCatalogForTest("jdbcreconnecttest.jar"), 3, 1, 0);
        assert(success);
        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest("jdbcreconnecttest.xml"));
        testjar = Configuration.getPathToCatalogForTest("jdbcreconnecttest.jar");

        // Set up ServerThread and Connection
        startServer();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        stopServer();
        File f = new File(testjar);
        f.delete();
    }

    private static void startServer() throws ClassNotFoundException, SQLException {
        server = new ServerThread(testjar, pb.getPathToDeployment(),
                                  BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();

        Class.forName("org.voltdb.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
        myconn = null;
    }

    private static Connection getJdbcConnection(String url, Properties props) throws Exception
    {
        Class.forName("org.voltdb.jdbc.Driver");
        return DriverManager.getConnection(url, props);
    }

    private static void stopServer() throws SQLException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
        if (myconn != null) {
            myconn.close();
            myconn = null;
        }
        if (server != null) {
            try { server.shutdown(); } catch (InterruptedException e) { /*empty*/ }
            server = null;
        }
    }

    /*
     * Make sure a connection can recover from a broken connection when there are
     * multiple clients. The JDBC4Connection code used reference counting that
     * made it work with just one client, but break with more.
     */
    @Test
    public void testReconnect() throws Exception {
        // Make a second client.
        Connection conn2 = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
        try {
            // Break the current connection and try to execute a procedure call.
            CallableStatement cs1 = conn.prepareCall("{call InsertA(?, ?)}");
            cs1.setInt(1, 55);
            cs1.setInt(2, 66);

            // Shut down unceremoneously
            server.shutdown();

            // Expect failure
            try {
                cs1.execute();
            }
            catch (SQLException e) {
                assertEquals(e.getSQLState(), SQLError.CONNECTION_FAILURE);
            }

            // Crank the server up again with enough delay to allow retry logic to kick in.
            server = new ServerThread(testjar, pb.getPathToDeployment(), BackendTarget.NATIVE_EE_JNI);
            server.setStartupDelayMS(15000);
            server.start();

            // Re-execute a new prepared statement with the restarted connection.
            CallableStatement cs2 = conn.prepareCall("{call InsertA(?, ?)}");
            cs2.setInt(1, 55);
            cs2.setInt(2, 66);
            try {
                cs2.execute();
            }
            catch (SQLException e) {
                fail("Expect the connection to function after reconnect.");
            }
        }
        finally {
            conn2.close();
        }
    }
}
