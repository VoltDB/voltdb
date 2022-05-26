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

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
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

public class TestJDBCMultiConnection {
    static String m_testJar;
    static ServerThread m_server;
    // Use multiple connections to make sure reference counting doesn't prevent
    // detection of broken connections.
    static Connection[] m_connections = new Connection[2];
    static VoltProjectBuilder m_projectBuilder;

    @BeforeClass
    public static void setUp() throws Exception {
        // Fake out the constraints that were previously written against the
        // TPCC schema
        String ddl =
            "CREATE TABLE TT(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE ORDERS(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE UNIQUE INDEX UNIQUE_ORDERS_HASH ON ORDERS (A1, A2_ID); " +
            "CREATE INDEX IDX_ORDERS_HASH ON ORDERS (A2_ID);";


        m_projectBuilder = new VoltProjectBuilder();
        m_projectBuilder.addLiteralSchema(ddl);
        m_projectBuilder.addSchema(TestClientFeatures.class.getResource("clientfeatures.sql"));
        m_projectBuilder.addProcedure(ArbitraryDurationProc.class);
        m_projectBuilder.addPartitionInfo("TT", "A1");
        m_projectBuilder.addPartitionInfo("ORDERS", "A1");
        m_projectBuilder.addStmtProcedure("InsertA", "INSERT INTO TT VALUES(?,?);",
                new ProcedurePartitionData("TT", "A1"));
        m_projectBuilder.addStmtProcedure("SelectB", "SELECT * FROM TT;");
        boolean success = m_projectBuilder.compile(Configuration.getPathToCatalogForTest("jdbcreconnecttest.jar"), 3, 1, 0);
        assert(success);
        MiscUtils.copyFile(m_projectBuilder.getPathToDeployment(), Configuration.getPathToCatalogForTest("jdbcreconnecttest.xml"));
        m_testJar = Configuration.getPathToCatalogForTest("jdbcreconnecttest.jar");

        // Set up server and connections.
        startServer();
        connectClients();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        stopServer();
        File f = new File(m_testJar);
        f.delete();
    }

    private static void startServer()
    {
        m_server = new ServerThread(m_testJar, m_projectBuilder.getPathToDeployment(),
                                  BackendTarget.NATIVE_EE_JNI);
        m_server.start();
        m_server.waitForInitialization();
    }

    private static void connectClients() throws ClassNotFoundException, SQLException
    {
        Class.forName("org.voltdb.jdbc.Driver");
        for (int i = 0; i < m_connections.length; ++i) {
            if (ClientConfig.ENABLE_SSL_FOR_TEST) {
                m_connections[i] = DriverManager.getConnection("jdbc:voltdb://localhost:21212?" + JDBCTestCommons.SSL_URL_SUFFIX);
            }
            else {
                m_connections[i] = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
            }
        }
    }

    private static void stopServer() throws SQLException {
        for (int i = 0; i < m_connections.length; ++i) {
            if (m_connections[i] != null) {
                m_connections[i].close();
                m_connections[i] = null;
            }
        }
        if (m_server != null) {
            try { m_server.shutdown(); } catch (InterruptedException e) { /*empty*/ }
            m_server = null;
        }
    }

    /*
     * Test that multiple client connections properly handle disconnection.
     */
    @Test
    public void testMultiDisconnect() throws Exception
    {
        class Tester
        {
            List<CallableStatement> m_callableStatements = new ArrayList<CallableStatement>();
            int m_value = 0;

            void makeStatements() throws SQLException
            {
                for (Connection connection : m_connections) {
                    CallableStatement cs = connection.prepareCall("{call InsertA(?, ?)}");
                    cs.setInt(1, m_value+100);
                    cs.setInt(2, m_value+1000);
                    m_value++;
                    m_callableStatements.add(cs);
                }
            }

            void testStatements(int expectConnectionFailures)
            {
                if (expectConnectionFailures < 0) {
                    expectConnectionFailures = m_callableStatements.size();
                }
                for (int i = 0; i < m_connections.length; ++i) {
                    try {
                        m_callableStatements.get(i).execute();
                        assertEquals(0, expectConnectionFailures);
                    }
                    catch(SQLException e) {
                        assertTrue(expectConnectionFailures > 0);
                        expectConnectionFailures--;
                        assertEquals(e.getSQLState(), SQLError.CONNECTION_FAILURE);
                    }
                }
            }
        }

        // Expect connection/query successes.
        {
            Tester tester = new Tester();
            tester.makeStatements();
            tester.testStatements(0);
        }

        // Shut down unceremoneously
        m_server.shutdown();

        // Expect connection failures.
        {
            Tester tester = new Tester();
            tester.makeStatements();
            tester.testStatements(-1);
        }

        // Restart server.
        startServer();

        // Expect connection/query successes.
        {
            Tester tester = new Tester();
            tester.makeStatements();
            tester.testStatements(0);
        }
    }
}
