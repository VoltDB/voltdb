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

package org.voltdb.jdbc;

import static org.junit.Assert.*;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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

public class TestJDBCAutoReconnectOnLoss {

    static String TEST_JAR;
    static ServerThread SERVER;
    static Connection CONNECTION;
    static VoltProjectBuilder BUILDER;
    static final String  DRIVER_URL="jdbc:voltdb://localhost:21212?autoreconnect=true";
    static final String  TEST_SQL = "select  count(*) from TT";

    @BeforeClass
    public static void setUp() throws Exception {

        String ddl =
            "CREATE TABLE TT(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE ORDERS(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE UNIQUE INDEX UNIQUE_ORDERS_HASH ON ORDERS (A1, A2_ID); " +
            "CREATE INDEX IDX_ORDERS_HASH ON ORDERS (A2_ID);";


        BUILDER = new VoltProjectBuilder();
        BUILDER.addLiteralSchema(ddl);
        BUILDER.addSchema(TestClientFeatures.class.getResource("clientfeatures.sql"));
        BUILDER.addProcedures(ArbitraryDurationProc.class);
        BUILDER.addPartitionInfo("TT", "A1");
        BUILDER.addPartitionInfo("ORDERS", "A1");
        BUILDER.addStmtProcedure("InsertA", "INSERT INTO TT VALUES(?,?);", "TT.A1: 0");
        BUILDER.addStmtProcedure("SelectB", "SELECT * FROM TT;");
        boolean success = BUILDER.compile(Configuration.getPathToCatalogForTest("jdbcreconnecttest.jar"), 3, 1, 0);
        assert(success);
        MiscUtils.copyFile(BUILDER.getPathToDeployment(), Configuration.getPathToCatalogForTest("jdbcreconnecttest.xml"));
        TEST_JAR = Configuration.getPathToCatalogForTest("jdbcreconnecttest.jar");

        startServer();
        connect();
    }


    @AfterClass
    public static void tearDown() throws Exception {
        stopServer();
        File f = new File(TEST_JAR);
        f.delete();
    }

    private static void stopServer() throws SQLException {

        if (SERVER != null) {
            try { SERVER.shutdown(); } catch (InterruptedException e) { }
            SERVER = null;
        }
    }

    private static void startServer()
    {
        SERVER = new ServerThread(TEST_JAR, BUILDER.getPathToDeployment(),
                                  BackendTarget.NATIVE_EE_JNI);
        SERVER.start();
        SERVER.waitForInitialization();
    }

    private static void connect() throws ClassNotFoundException, SQLException
    {
        Class.forName("org.voltdb.jdbc.Driver");
        CONNECTION =  DriverManager.getConnection(DRIVER_URL, new Properties());
    }

    @Test
    public void testAutoReconnect() throws Exception {

        Statement query = CONNECTION.createStatement();
        ResultSet results = query.executeQuery(TEST_SQL);
        assertTrue(results.next());
        results.close();
        query.close();

        stopServer();
        Thread.sleep(4000);

        try{
            query = CONNECTION.createStatement();
            query.executeQuery(TEST_SQL);
            assertTrue(false);
        } catch (SQLException e){
            assertTrue(true);
        }

        startServer();

        query = CONNECTION.createStatement();
        results = query.executeQuery(TEST_SQL);
        assertTrue(results.next());

        results.close();
        query.close();
        CONNECTION.close();
        tearDown();
    }
}
