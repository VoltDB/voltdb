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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.regressionsuites.MultiConfigSuiteBuilder;
import org.voltdb.regressionsuites.RegressionSuite;

import junit.framework.Test;

public class TestJDBCMultiNodeConnection extends RegressionSuite
{

    static LocalCluster m_config;
    static int kfactor = 3;

    public TestJDBCMultiNodeConnection(String name) {
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
        builder.addStmtProcedure("Insert", "INSERT INTO T(A1) VALUES(?);",
                new ProcedurePartitionData("T", "A1"));
        builder.addStmtProcedure("Select", "SELECT * FROM T WHERE A1 = ?;",
                new ProcedurePartitionData("T", "A1"));
        return builder;
    }

    private static Connection connectClients(String URL) throws ClassNotFoundException, SQLException {
        Class.forName("org.voltdb.jdbc.Driver");
        Connection conn = DriverManager.getConnection(URL);
        return conn;
    }

    public void testMultiNodeJDBCConnection() throws Exception {
        // Need to build the URL before pre-killing the node or we don't
        // add it to the URL list.
        StringBuilder sb = new StringBuilder();
        sb.append("jdbc:voltdb://");
        final List<String> listeners = m_config.getListenerAddresses();
        for (String listener : listeners) {
            sb.append(listener).append(",");
        }
        if (ClientConfig.ENABLE_SSL_FOR_TEST) {
            sb.append("?").append(JDBCTestCommons.SSL_URL_SUFFIX);
        }
        String JDBCURL = sb.toString();
        System.out.println("Connecting to JDBC URL: " + JDBCURL);

        // ENG-6231.  Pre-kill one of the nodes.  Things should still work.
        m_config.killSingleHost(4);
        Thread.sleep(500); // give repair a chance to settle

        Connection conn = null;
        try {
            conn = connectClients(JDBCURL);
        }
        catch (Exception e) {
            e.printStackTrace();
            fail("Connection creation shouldn't fail: " + e.getMessage());
        }

        try {
            Client client = ClientFactory.createClient();
            client.createConnection("localhost", m_config.port(0));
            client.callProcedure("Insert", 13);
            client.close();

            String sql = "select count(*) from T WHERE A1 = ?;";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, 13);
            ResultSet rs = ps.executeQuery();
            rs.next();
            int cnt = rs.getInt(1);
            assertEquals(cnt, 1);
            cnt = 0;

            m_config.killSingleHost(3);
            Thread.sleep(500); // give repair a chance to settle
            ps = conn.prepareStatement(sql);
            ps.setInt(1, 13);
            rs = ps.executeQuery();
            rs.next();
            cnt = rs.getInt(1);
            assertEquals(cnt, 1);
            cnt = 0;

            m_config.killSingleHost(2);
            Thread.sleep(500); // give repair a chance to settle
            ps = conn.prepareStatement(sql);
            ps.setInt(1, 13);
            rs = ps.executeQuery();
            rs.next();
            cnt = rs.getInt(1);
            assertEquals(cnt, 1);
            cnt = 0;
        } catch (Exception ex) {
            //We could get here if the transaction was submitted while the
            //cluster is failing.  Handle the mastership change case, but hope
            //it doesn't happen...
            if (ex.getMessage().contains("change in mastership")) {
                System.out.println("Test quit early due to transaction during node failure.  Bail out.");
            }
            else {
                ex.printStackTrace();
                fail();
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    static public Test suite() throws IOException {
        // the suite made here will all be using the tests from this class
        MultiConfigSuiteBuilder builder = new MultiConfigSuiteBuilder(TestJDBCMultiNodeConnection.class);

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

