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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.MiscUtils;

public class TestJDBCQueries {
    private static final String TEST_XML = "jdbcparameterstest.xml";
    private static final String TEST_JAR = "jdbcparameterstest.jar";
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
        final String[] good;
        final String[] bad;

        Data(String typename, int dimension, String[] good, String[] bad)
        {
            this.typename = typename;
            this.dimension = dimension;
            this.good = new String[good.length];
            for (int i = 0; i < this.good.length; ++i) {
                this.good[i] = good[i];
            }
            if (bad != null) {
                this.bad = new String[bad.length];
                for (int i = 0; i < this.bad.length; ++i) {
                    this.bad[i] = bad[i];
                }
            }
            else {
                this.bad = null;
            }
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
            new Data("TINYINT", 0,
                        new String[] {"11", "22", "33"},
                        new String[] {"abc"}),
            new Data("SMALLINT", 0,
                        new String[] {"-11", "-22", "-33"},
                        new String[] {"3.2", "blah"}),
            new Data("INTEGER", 0,
                        new String[] {"0", "1", "2"},
                        new String[] {""}),
            new Data("BIGINT", 0,
                        new String[] {"9999999999999", "8888888888888", "7777777777777"},
                        new String[] {"Jan 23 2011"}),
            new Data("FLOAT", 0,
                        new String[] {"3.1415926", "2.81828", "-9.0"},
                        new String[] {"x"}),
            new Data("DECIMAL", 0,
                        new String[] {"1111.2222", "-3333.4444", "+5555.6666"},
                        new String[] {""}),
            new Data("VARCHAR", 100,
                        new String[] {"abcdefg", "hijklmn", "opqrstu"},
                        null),
            new Data("VARBINARY", 100,
                        new String[] {"deadbeef01234567", "aaaa", "12341234"},
                        new String[] {"xxx"}),
            new Data("TIMESTAMP", 0,
                        new String[] {"9999999999999", "0", "1"},
                        new String[] {""}),
    };

    @BeforeClass
    public static void setUp() throws Exception {
        // Add one T_<type> table for each data type.
        String ddl = "";
        for (Data d : data) {
            ddl += String.format("CREATE TABLE %s(ID %s, VALUE VARCHAR(255)); ",
                                 d.tablename, d.typedecl);
        }

        pb = new VoltProjectBuilder();
        pb.addLiteralSchema(ddl);
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

    @Before
    public void populateTables()
    {
        // Populate tables.
        for (Data d : data) {
            String q = String.format("insert into %s values(?, ?)", d.tablename);
            for (String id : d.good) {
                try {
                    PreparedStatement sel = conn.prepareStatement(q);
                    sel.setString(1, id);
                    sel.setString(2, String.format("VALUE:%s:%s", d.tablename, id));
                    sel.execute();
                    int count = sel.getUpdateCount();
                    assertTrue(count==1);
                }
                catch(SQLException e) {
                    System.err.printf("ERROR(INSERT): %s value='%s': %s\n",
                            d.typename, d.good[0], e.getMessage());
                    fail();
                }
            }
        }
    }

    @After
    public void clearTables()
    {
        for (Data d : data) {
            try {
                PreparedStatement sel =
                        conn.prepareStatement(String.format("delete from %s", d.tablename));
                sel.execute();
            } catch (SQLException e) {
                System.err.printf("ERROR(DELETE): %s: %s\n", d.tablename, e.getMessage());
                fail();
            }
        }
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
    public void testSimpleStatement() throws Exception
    {
        for (Data d : data) {
            try {
                String q = String.format("select * from %s", d.tablename);
                Statement sel = conn.createStatement();
                sel.execute(q);
                ResultSet rs = sel.getResultSet();
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                assertEquals(d.good.length, rowCount);
            }
            catch(SQLException e) {
                System.err.printf("ERROR(SELECT): %s: %s\n", d.typename, e.getMessage());
                fail();
            }
        }
    }

    @Test
    public void testFloatDoubleVarcharColumn() throws Exception {
        for (Data d : data) {
            try {
                String q = String.format("insert into %s values(?, ?)", d.tablename);
                PreparedStatement ins = conn.prepareStatement(q);
                ins.setString(1, d.good[0]);
                ins.setFloat(2, (float) 1.0);
                if (ins.executeUpdate() != 1) {
                    fail();
                }
                q = String.format("select * from %s", d.tablename);
                Statement sel = conn.createStatement();
                sel.execute(q);
                ResultSet rs = sel.getResultSet();
                int rowCount = 0;
                boolean found = false;
                while (rs.next()) {
                    if (rs.getString(2).equals("1.0")) {
                        found = true;
                    }
                    rowCount++;
                }
                assertTrue(found);
                assertEquals(4, rowCount);

                //Do double
                q = String.format("insert into %s values(?, ?)", d.tablename);
                ins = conn.prepareStatement(q);
                ins.setString(1, d.good[0]);
                ins.setDouble(2, 9.999999);
                if (ins.executeUpdate() != 1) {
                    fail();
                }
                q = String.format("select * from %s", d.tablename);
                sel = conn.createStatement();
                sel.execute(q);
                rs = sel.getResultSet();
                rowCount = 0;
                found = false;
                while (rs.next()) {
                    if (rs.getString(2).equals("9.999999")) {
                        found = true;
                    }
                    rowCount++;
                }
                assertTrue(found);
                assertEquals(5, rowCount);

                //Do int
                q = String.format("insert into %s values(?, ?)", d.tablename);
                ins = conn.prepareStatement(q);
                ins.setString(1, d.good[0]);
                ins.setInt(2, 9);
                if (ins.executeUpdate() != 1) {
                    fail();
                }
                q = String.format("select * from %s", d.tablename);
                sel = conn.createStatement();
                sel.execute(q);
                rs = sel.getResultSet();
                rowCount = 0;
                found = false;
                while (rs.next()) {
                    if (rs.getString(2).equals("9")) {
                        found = true;
                    }
                    rowCount++;
                }
                assertTrue(found);
                assertEquals(6, rowCount);

            } catch (SQLException e) {
                e.printStackTrace();
                System.err.printf("ERROR(SELECT): %s: %s\n", d.typename, e.getMessage());
                fail();
            }
        }
    }

    @Test
    public void testQueryBatch() throws Exception
    {
        Statement batch = conn.createStatement();
        for (Data d : data) {
            String q = String.format("update %s set value='%s'", d.tablename, "whatever");
            batch.addBatch(q);
        }
        try {
            int[] resultCodes = batch.executeBatch();
            assertEquals(data.length, resultCodes.length);
            int total_cnt = 0;
            for (int i = 0; i < data.length; ++i) {
                assertEquals(data[i].good.length, resultCodes[i]);
                total_cnt += data[i].good.length;
            }
            //Test update count
            assertEquals(total_cnt, batch.getUpdateCount());
        }
        catch(SQLException e) {
            System.err.printf("ERROR: %s\n", e.getMessage());
            fail();
        }
    }

    @Test
    public void testParameterizedQueries() throws Exception
    {
        for (Data d : data) {
            String q = String.format("select * from %s where id != ?", d.tablename);
            try {
                PreparedStatement sel = conn.prepareStatement(q);
                sel.setString(1, d.good[0]);
                sel.execute();
                ResultSet rs = sel.getResultSet();
                int rowCount = 0;
                while (rs.next()) {
                    rowCount++;
                }
                assertEquals(d.good.length-1, rowCount);
            }
            catch(SQLException e) {
                System.err.printf("ERROR(SELECT): %s value='%s': %s\n",
                                  d.typename, d.good[0], e.getMessage());
                fail();
            }
            if (d.bad != null) {
                for (String value : d.bad) {
                    boolean exceptionReceived = false;
                    try {
                        PreparedStatement sel = conn.prepareStatement(q);
                        sel.setString(1, value);
                        sel.execute();
                        System.err.printf("ERROR(SELECT): %s value='%s': * should have failed *\n",
                                          d.typename, value);
                    }
                    catch(SQLException e) {
                        exceptionReceived = true;
                    }
                    assertTrue(exceptionReceived);
                }
            }
        }
    }

    @Test
    public void testVarbinarySetBytes() throws Exception
    {
        // Verify that setBytes() works to set VARBINARY with byte[] input
        PreparedStatement ps = conn.prepareStatement("insert into T_VARBINARY values (?, ?)");
        byte[] data = {'a', 'b', 'g', '0', 0, 1, 127, -128};
        ps.setBytes(1, data);
        ps.setString(2, "bytes");
        ps.executeUpdate();
        ps = conn.prepareStatement("select ID from T_VARBINARY where VALUE='bytes'");
        ps.executeQuery();
        ResultSet rs = ps.getResultSet();
        while (rs.next()) {
            byte[] data2 = rs.getBytes(1);
            assertEquals(data.length, data2.length);
            for (int i = 0; i < data.length; i++) {
                assertEquals(data[i], data2[i]);
            }
        }

        // Also verify that setString() with a hex-encoded string works to set VARBINARY
        ps = conn.prepareStatement("insert into T_VARBINARY values (?, ?)");
        String stringdata = "000102030405060708090a";
        ps.setString(1, stringdata);
        ps.setString(2, "string");
        ps.executeUpdate();
        ps = conn.prepareStatement("select ID from T_VARBINARY where VALUE='string'");
        ps.executeQuery();
        rs = ps.getResultSet();
        while (rs.next()) {
            byte[] data2 = rs.getBytes(1);
            assertEquals(stringdata.length()/2, data2.length);
            for (int i = 0; i < data2.length; i++) {
                assertEquals(i, data2[i]);
            }
        }
    }

    @Test
    public void testGetTimestamp() throws Exception
    {
        PreparedStatement ins = conn.prepareStatement("insert into T_TIMESTAMP values (?, ?)");
        // Bad reported input
        Timestamp ts = Timestamp.valueOf("2014-03-23 05:12:08.156000");
        ins.setTimestamp(1, ts);
        ins.setString(2, "badinput");
        ins.executeUpdate();
        PreparedStatement ps = conn.prepareStatement("select ID from T_TIMESTAMP where VALUE='badinput'");
        ps.executeQuery();
        ResultSet rs = ps.getResultSet();
        while (rs.next()) {
            assertEquals(ts, rs.getTimestamp(1));
            assertEquals(new Date(ts.getTime()), rs.getDate(1));
            assertEquals(new Time(ts.getTime()), rs.getTime(1));
        }
        // Bad round-trip
        ts = new Timestamp(System.currentTimeMillis());
        ins.setTimestamp(1, ts);
        ins.setString(2, "timestamp");
        ins.executeUpdate();
        ps = conn.prepareStatement("select ID from T_TIMESTAMP where VALUE='timestamp'");
        ps.executeQuery();
        rs = ps.getResultSet();
        while (rs.next()) {
            assertEquals(ts, rs.getTimestamp(1));
            assertEquals(new Date(ts.getTime()), rs.getDate(1));
            assertEquals(new Time(ts.getTime()), rs.getTime(1));
        }
        // Crashy null
        ins.setTimestamp(1, null);
        ins.setString(2, "crashy");
        ins.executeUpdate();
        ps = conn.prepareStatement("select ID from T_TIMESTAMP where VALUE='crashy'");
        ps.executeQuery();
        rs = ps.getResultSet();
        while (rs.next()) {
            assertEquals(null, rs.getTimestamp(1));
            assertEquals(null, rs.getDate(1));
            assertEquals(null, rs.getTime(1));
        }
        // THE TIMESTAMP BEFORE TIME
        //Timestamp ts = new Timestamp(-10000);
        //ts.setNanos(999999000);
        //System.out.println("BEFORE TIME: " + ts.toString());
        //ins.setTimestamp(1, ts);
        //ins.setString(2, "beforetime");
        //ins.executeUpdate();
        //PreparedStatement ps = conn.prepareStatement("select ID from T_TIMESTAMP where VALUE='beforetime'");
        //ps.executeQuery();
        //ResultSet rs = ps.getResultSet();
        //while (rs.next()) {
        //    Timestamp ts1 = rs.getTimestamp(1);
        //    assertEquals(ts, ts1);
        //}
    }

}
