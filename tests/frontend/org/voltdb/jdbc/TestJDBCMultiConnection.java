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

package org.voltdb.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.TestClientFeatures;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;

public class TestJDBCMultiConnection extends TestCase {
    private static Configuration m_config;
    private static ServerThread m_server;
    // Use multiple connections to make sure reference counting doesn't prevent
    // detection of broken connections.
    private static Connection[] m_connections = new Connection[2];

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        // Fake out the constraints that were previously written against the
        // TPCC schema
        CatalogBuilder cb = new CatalogBuilder(
                "CREATE TABLE TT(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
                "CREATE TABLE ORDERS(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1)); \n" +
                "CREATE UNIQUE INDEX UNIQUE_ORDERS_HASH ON ORDERS (A1, A2_ID); " +
                "CREATE INDEX IDX_ORDERS_HASH ON ORDERS (A2_ID); \n" +
                "PARTITION TABLE TT ON COLUMN A1; \n" +
                "PARTITION TABLE ORDERS ON COLUMN A1;\n" +
                "")
        .addSchema(TestClientFeatures.class.getResource("clientfeatures.sql"))
        .addProcedures(ArbitraryDurationProc.class)
        .addStmtProcedure("InsertA", "INSERT INTO TT VALUES(?,?);", "TT.A1", 0)
        .addStmtProcedure("SelectB", "SELECT * FROM TT;")
        ;
        String testcaseclassname = TestJDBCMultiConnection.class.getSimpleName();

        m_config = Configuration.compile(testcaseclassname, cb, new DeploymentBuilder(3));
        assertNotNull("Configuration failed to compile", m_config);
        // Set up server and connections.
        startServer();
        connectClients();
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        stopServer();
    }

    private static void startServer()
    {
        m_server = new ServerThread(m_config);
        m_server.start();
        m_server.waitForInitialization();
    }

    private static void connectClients() throws ClassNotFoundException, SQLException
    {
        Class.forName("org.voltdb.jdbc.Driver");
        for (int i = 0; i < m_connections.length; ++i) {
            m_connections[i] = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
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
