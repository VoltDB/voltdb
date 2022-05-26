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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.TestClientFeatures;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestJDBCAutoReconnectOnLoss {

    String m_testjar;

    static String driverUrl ="jdbc:voltdb://localhost:21212?autoreconnect=true";
    static final String  TEST_SQL = "select  count(*) from TT";

    VoltProjectBuilder m_builder;
    ServerThread m_server;
    Connection m_connection;

    @Before
    public void setUp() throws Exception {

        String ddl =
            "CREATE TABLE TT(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE ORDERS(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE UNIQUE INDEX UNIQUE_ORDERS_HASH ON ORDERS (A1, A2_ID); " +
            "CREATE INDEX IDX_ORDERS_HASH ON ORDERS (A2_ID);";

        m_builder = new VoltProjectBuilder();
        m_builder.addLiteralSchema(ddl);
        m_builder.addSchema(TestClientFeatures.class.getResource("clientfeatures.sql"));
        m_builder.addProcedure(ArbitraryDurationProc.class);
        m_builder.addPartitionInfo("TT", "A1");
        m_builder.addPartitionInfo("ORDERS", "A1");
        m_builder.addStmtProcedure("InsertA", "INSERT INTO TT VALUES(?,?);",
                new ProcedurePartitionData("TT", "A1"));
        m_builder.addStmtProcedure("SelectB", "SELECT * FROM TT;");

        assertTrue("failed to compile catalog", m_builder.compile(Configuration.getPathToCatalogForTest("jdbcreconnecttest.jar"), 3, 1, 0));

        MiscUtils.copyFile(m_builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("jdbcreconnecttest.xml"));
        m_testjar = Configuration.getPathToCatalogForTest("jdbcreconnecttest.jar");
        if (ClientConfig.ENABLE_SSL_FOR_TEST) {
            driverUrl = driverUrl + JDBCTestCommons.SSL_URL_SUFFIX;
        }
        startServer();
        connect();
    }


    @After
    public void tearDown() throws Exception {
        stopServer();
    }

    private void stopServer() throws SQLException {

        if (m_server != null) {
            try { m_server.shutdown(); } catch (InterruptedException e) { }
            try { m_server.join(); } catch (InterruptedException e) { }
            m_server = null;
        }
    }

    private void startServer()
    {
        m_server = new ServerThread(m_testjar, m_builder.getPathToDeployment(),
                                  BackendTarget.NATIVE_EE_JNI);
        m_server.start();
        m_server.waitForInitialization();
    }

    private void connect() throws ClassNotFoundException, SQLException
    {
        Class.forName("org.voltdb.jdbc.Driver");
        m_connection =  DriverManager.getConnection(driverUrl, new Properties());
    }

    @Test
    public void testAutoReconnect() throws Exception {

        Statement query = m_connection.createStatement();
        ResultSet results = query.executeQuery(TEST_SQL);
        assertTrue(results.next());
        results.close();
        query.close();

        stopServer();
        Thread.sleep(4000);

        try{
            query = m_connection.createStatement();
            query.executeQuery(TEST_SQL);
            fail("No connection");
        } catch (SQLException e){
            assertEquals("Connection failure: 'No connections.'", e.getMessage());
        }

        startServer();

        query = m_connection.createStatement();
        results = query.executeQuery(TEST_SQL);
        assertTrue(results.next());

        results.close();
        query.close();
        m_connection.close();
    }

    @Test
    public void testConnectionPool() throws Exception {

        org.apache.tomcat.jdbc.pool.DataSource ds = new org.apache.tomcat.jdbc.pool.DataSource();
        ds.setDriverClassName("org.voltdb.jdbc.Driver");
        ds.setUrl(driverUrl);
        ds.setInitialSize(5);
        ds.setMaxActive(10);
        ds.setMaxIdle(5);
        ds.setMinIdle(2);

        Connection conn = ds.getConnection();

        Statement query = conn.createStatement();
        ResultSet results = query.executeQuery(TEST_SQL);
        assertTrue(results.next());
        results.close();
        query.close();

        stopServer();
        Thread.sleep(4000);

        try{
            query = conn.createStatement();
            query.executeQuery(TEST_SQL);
           fail("No connection");
        } catch (SQLException e){
            assertEquals("Connection failure: 'No connections.'", e.getMessage());
        }

        startServer();

        query = conn.createStatement();
        results = query.executeQuery(TEST_SQL);
        assertTrue(results.next());

        results.close();
        query.close();
        conn.close();
        ds.close();
    }
}
