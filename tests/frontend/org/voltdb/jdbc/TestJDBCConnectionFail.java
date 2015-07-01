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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;


import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;

public class TestJDBCConnectionFail extends RegressionSuite
{

    static LocalCluster m_config;
    static int kfactor = 3;

    public TestJDBCConnectionFail(String name) {
        super(name);
    }

    static VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE T("
                + "A1 INTEGER NOT NULL, "
                + "A2 DECIMAL, "
                + "A3 DECIMAL DEFAULT 0, "
                + "A4 DECIMAL DEFAULT 999, "
                + "A5 DECIMAL DEFAULT 9.99E2, "
                + "A6 DECIMAL DEFAULT 1.012345678901, "
                + "PRIMARY KEY(A1));"
        );
        builder.addPartitionInfo("T", "A1");
        builder.addStmtProcedure("Insert", "INSERT INTO T(A1) VALUES(?);", "T.A1: 0");
        builder.addStmtProcedure("Select", "SELECT * FROM T WHERE A1 = ?;", "T.A1: 0");
        return builder;
    }

    private static Connection connectClients(String URL) throws ClassNotFoundException, SQLException {
        Class.forName("org.voltdb.jdbc.Driver");
        Connection conn = DriverManager.getConnection(URL);
        return conn;
    }

    public void testJDBCConnectionWithNoServersUp() throws Exception {
        // Need to build the URL before killing the cluster or we don't
        // add it to the URL list.
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:voltdb://");
        final List<String> listeners = m_config.getListenerAddresses();
        for (String listener : listeners) {
            sb.append(listener).append(",");
        }
        String JDBCURL = sb.toString();
        System.out.println("Connecting to JDBC URL: " + JDBCURL);

        // Shutdown the cluster.  Need to do this before connection so that we
        // don't have stale connections in the connection pool
        m_config.shutDown();

        boolean threw = false;
        try {
            connectClients(JDBCURL);
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("Unable to establish"));
            threw = true;
        }
        assertTrue("Connection which should have failed did not.", threw);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestJDBCConnectionFail.class);

        // build up a project builder for the workload
        VoltProjectBuilder project = getBuilderForTest();
        boolean success;
        m_config = new LocalCluster("decimal-default.jar", 4, 5, kfactor, BackendTarget.NATIVE_EE_JNI);
        m_config.setHasLocalServer(true);
        success = m_config.compile(project);
        assertTrue(success);

        // add this config to the set of tests to run
        builder.addServerConfig(m_config);
        return builder;
    }
}

