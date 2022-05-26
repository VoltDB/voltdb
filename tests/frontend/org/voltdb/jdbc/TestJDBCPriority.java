/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestJDBCPriority {

    static ServerThread server;
    static String testjar;

    @BeforeClass
    public static void setup() throws Exception {
        Class.forName("org.voltdb.jdbc.Driver"); // voodoo
        startServer();
    }

    @AfterClass
    public static void teardown() throws Exception {
        stopServer();
        File jar = new File(testjar);
        jar.delete();
    }

    static void startServer() throws Exception {
        VoltProjectBuilder pb = getBuilderForTest();
        boolean success = pb.compile(Configuration.getPathToCatalogForTest("jdbcpriotest.jar"), 3, 1, 0);
        assertTrue(success);

        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest("jdbcpriotest.xml"));
        testjar = Configuration.getPathToCatalogForTest("jdbcpriotest.jar");

        server = new ServerThread(testjar, pb.getPathToDeployment(), BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();
    }

    static void stopServer() throws Exception {
        if (server != null) {
            try {
                server.shutdown();
            }
            catch (InterruptedException ex) {
                // ignored
            }
            server = null;
        }
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

    /*
     * Not a very good test, since we have no way to know if
     * the procedure call really got marshalled with the given
     * priority.  But it at least exercises the code path.
     */
    @Test
    public void testPriority() throws Exception {
        System.out.println("=-=-= testPriority =-=-=");

        String url = "jdbc:voltdb://localhost:21212?priority=3";
        System.out.println("Connecting to JDBC URL: " + url    );

        System.out.println("Get JDBC connection");
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Fail in connection setup: " + ex.getMessage());
        }

        System.out.println("Initialize table");
        try {
            Client client = ClientFactory.createClient();
            client.createConnection("localhost");
            client.callProcedure("Insert", 13);
            client.close();
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Fail in table insert: " + ex.getMessage());
        }

        System.out.println("Issue JDBC query");
        int cnt = -99;
        try {
            String sql = "select count(*) from T WHERE A1 = ?;";
            PreparedStatement ps = conn.prepareStatement(sql);
            ps.setInt(1, 13);
            ResultSet rs = ps.executeQuery();
            rs.next();
            cnt = rs.getInt(1);
        }
        catch (Exception ex) {
            ex.printStackTrace();
            fail("Fail in SQL execution: " + ex.getMessage());
        }
        assertEquals(cnt, 1);

        System.out.println("Done");
    }
}
