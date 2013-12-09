/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestJDBCParameters {
    static String testjar;
    static ServerThread server;
    static Connection conn;
    static Connection myconn;
    static VoltProjectBuilder pb;

    static class Data
    {
        final String typename;
        final int dimension;
        final String tablename;
        final String typedecl;
        final String sgood;
        final String sbad;

        Data(String typename, int dimension, String sgood, String sbad)
        {
            this.typename = typename;
            this.dimension = dimension;
            this.sgood = sgood;
            this.sbad = sbad;
            this.tablename = String.format("T_%s", this.typename);
            if (dimension > 0) {
                this.typedecl = String.format("%s(%d)", this.typename, this.dimension);
            }
            else {
                this.typedecl = this.typename;
            }
        }
    };

    static Data[] data = new Data[] {
            new Data("TINYINT", 0, "11", "abc"),
            new Data("SMALLINT", 0, "-11", "3.2"),
            new Data("INTEGER", 0, "0", ""),
            new Data("BIGINT", 0, "9999999999999", "Jan 23 2011"),
            new Data("FLOAT", 0, "3.1415926", "x"),
            new Data("DECIMAL", 0, "", ""),
            new Data("VARCHAR", 100, "", ""),
            //new Data("VARBINARY", 100, "", ""),
            new Data("TIMESTAMP", 0, "", ""),
    };

    @BeforeClass
    public static void setUp() throws Exception {
        // Add one T_<type> table for each data type.
        String ddl = "";
        for (Data d : data) {
            ddl += String.format("CREATE TABLE %s(ID %s NOT NULL, S1 VARCHAR(255), PRIMARY KEY(ID)); ",
                                 d.tablename, d.typedecl);
        }

        pb = new VoltProjectBuilder();
        pb.addLiteralSchema(ddl);
        boolean success = pb.compile(Configuration.getPathToCatalogForTest("jdbcparameterstest.jar"), 3, 1, 0);
        assert(success);
        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest("jdbcparameterstest.xml"));
        testjar = Configuration.getPathToCatalogForTest("jdbcparameterstest.jar");

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

    @Test
    public void testSetString() throws Exception
    {
        for (Data d : data) {
            String q = String.format("select * from %s where id=?", d.tablename);
            PreparedStatement sel = conn.prepareStatement(q);
            sel.setString(1, d.sgood);
            sel.execute();
        }
    }
}
