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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

public class TestJDBCResultSet {

    static Connection HsqlConn;
    PreparedStatement hsqlEmptyStmt;
    PreparedStatement hsql3RowStmt;
    ResultSet hsqlEmptyRS;
    ResultSet hsql3RowRS;

    static ServerThread voltDBServer;
    static Connection VoltDBConn;
    static VoltProjectBuilder pb;
    static String testjar;
    Statement voltEmptyStmt;
    Statement volt3RowStmt;
    ResultSet voltEmptyRS;
    ResultSet volt3RowRS;

    @BeforeClass
    public static void setUp() throws Exception {
        final String TEST_XML = "jdbcresultsettest.xml";
        final String TEST_JAR = "jdbcresultsettest.jar";
        final String table1Stmt = "CREATE TABLE HSQLTABLE (COL1 INTEGER PRIMARY KEY, COL2 VARCHAR(20))";
        final String table2Stmt = "CREATE TABLE HSQLEMPTY (COLEM1 INTEGER PRIMARY KEY, COLEM2 VARCHAR(20))";
        final String insert1Stmt = "INSERT INTO HSQLTABLE(COL1, COL2) VALUES(1,'FirstRow')";
        final String insert2Stmt = "INSERT INTO HSQLTABLE(COL1, COL2) VALUES(2,'SecondRow')";
        final String insert3Stmt = "INSERT INTO HSQLTABLE(COL1, COL2) VALUES(3,'ThirdRow')";

        try {
            // HSQL setup
            Class.forName("org.hsqldb_voltpatches.jdbcDriver");
            HsqlConn = DriverManager.getConnection("jdbc:hsqldb:mem:temptest",
                    "sa", "");
            Statement hsqlStmt = null;

            hsqlStmt = HsqlConn.createStatement();

            int i = hsqlStmt.executeUpdate(table1Stmt);
            assertFalse(i == -1);
            i = hsqlStmt.executeUpdate(table2Stmt);
            assertFalse(i == -1);
            i = hsqlStmt.executeUpdate(insert1Stmt);
            assertFalse(i == -1);
            i = hsqlStmt.executeUpdate(insert2Stmt);
            assertFalse(i == -1);
            i = hsqlStmt.executeUpdate(insert3Stmt);
            assertFalse(i == -1);
            hsqlStmt.close();

            // VoltDB Setup
            String ddl = table1Stmt + ";" + table2Stmt + ";";

            pb = new VoltProjectBuilder();
            pb.addLiteralSchema(ddl);
            boolean success = pb.compile(
                    Configuration.getPathToCatalogForTest(TEST_JAR), 3, 1, 0);
            assert (success);
            MiscUtils.copyFile(pb.getPathToDeployment(),
                    Configuration.getPathToCatalogForTest(TEST_XML));
            testjar = Configuration.getPathToCatalogForTest(TEST_JAR);

            // Set up ServerThread and Connection
            startServer();
            Statement VoltDBStmt = VoltDBConn.createStatement();

            i = VoltDBStmt.executeUpdate(insert1Stmt);
            assertFalse(i == -1);
            i = VoltDBStmt.executeUpdate(insert2Stmt);
            assertFalse(i == -1);
            i = VoltDBStmt.executeUpdate(insert3Stmt);
            assertFalse(i == -1);
            VoltDBStmt.close();
        } catch (SQLException ex) {
            fail();
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        try {
            Statement st = HsqlConn.createStatement();
            st.execute("SHUTDOWN");
            st.close();
            HsqlConn.close();

            stopServer();
        } catch (SQLException ex) {
            fail();
        }
    }

    private static void startServer() throws ClassNotFoundException,
            SQLException {
        voltDBServer = new ServerThread(testjar, pb.getPathToDeployment(),
                BackendTarget.NATIVE_EE_JNI);
        voltDBServer.start();
        voltDBServer.waitForInitialization();

        Class.forName("org.voltdb.jdbc.Driver");
        VoltDBConn = DriverManager
                .getConnection("jdbc:voltdb://localhost:21212");
    }

    private static void stopServer() throws SQLException {
        if (VoltDBConn != null) {
            VoltDBConn.close();
            VoltDBConn = null;
        }
        if (voltDBServer != null) {
            try {
                voltDBServer.shutdown();
            } catch (InterruptedException e) { /* empty */
            }
            voltDBServer = null;
        }
    }

    @Before
    public void populateResult() {
        try {
            hsqlEmptyStmt = HsqlConn.prepareStatement(
                    "SELECT * FROM HSQLEMPTY",
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            hsql3RowStmt = HsqlConn.prepareStatement("SELECT * FROM HSQLTABLE ORDER BY COL1", ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            hsqlEmptyRS = hsqlEmptyStmt.executeQuery();
            hsql3RowRS = hsql3RowStmt.executeQuery();

            voltEmptyStmt = VoltDBConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
            volt3RowStmt = VoltDBConn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

            voltEmptyRS = voltEmptyStmt.executeQuery("SELECT * FROM HSQLEMPTY");
            volt3RowRS = volt3RowStmt.executeQuery("SELECT * FROM HSQLTABLE ORDER BY COL1");
        } catch (SQLException ex) {
            fail();
        }
    }

    @After
    public void clearResult() {
        try {
            hsqlEmptyRS.close();
            hsqlEmptyRS = null;
            hsql3RowRS.close();
            hsql3RowRS = null;
            hsqlEmptyStmt.close();
            hsqlEmptyStmt = null;
            hsql3RowStmt.close();
            hsql3RowStmt = null;
        } catch (SQLException e) {

        }

        try {
            voltEmptyRS.close();
            voltEmptyRS = null;
            volt3RowRS.close();
            volt3RowRS = null;
            voltEmptyStmt.close();
            voltEmptyStmt = null;
            volt3RowStmt.close();
            volt3RowStmt = null;
        } catch (SQLException e) {

        }
    }

    // Tests For Table with data
    @Test
    public void testFirst() throws Exception {

        assertEquals(hsql3RowRS.absolute(2), volt3RowRS.absolute(2));
        assertTrue(hsql3RowRS.first() == volt3RowRS.first());
        assertTrue(hsql3RowRS.getRow() == volt3RowRS.getRow());
        assertEquals(hsql3RowRS.getInt(1), volt3RowRS.getInt(1));
    }

    @Test
    public void testLast() throws Exception {

        assertTrue(hsql3RowRS.last() == volt3RowRS.last());
        assertTrue(hsql3RowRS.getRow() == volt3RowRS.getRow());
        assertEquals(hsql3RowRS.getInt(1), volt3RowRS.getInt(1));
    }

    @Test
    public void testBeforeFirst() throws Exception {

        hsql3RowRS.beforeFirst();
        volt3RowRS.beforeFirst();
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        assertEquals(hsql3RowRS.next(), volt3RowRS.next());
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());
    }

    @Test
    public void testAfterLast() throws Exception {

        assertEquals(hsql3RowRS.next(), volt3RowRS.next());
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        hsql3RowRS.afterLast();
        volt3RowRS.afterLast();
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        assertEquals(hsql3RowRS.previous(), volt3RowRS.previous());
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());
    }

    @Test
    public void testIsBeforeFirst() throws Exception {

        hsql3RowRS.beforeFirst();
        volt3RowRS.beforeFirst();
        assertEquals(hsql3RowRS.isBeforeFirst(), volt3RowRS.isBeforeFirst());
    }

    @Test
    public void testIsAfterLast() throws Exception {

        hsql3RowRS.afterLast();
        volt3RowRS.afterLast();
        assertEquals(hsql3RowRS.isAfterLast(), volt3RowRS.isAfterLast());
    }

    @Test
    public void testIsFirst() throws Exception {

        hsql3RowRS.beforeFirst();
        volt3RowRS.beforeFirst();
        assertEquals( hsql3RowRS.isFirst(), volt3RowRS.isFirst());
    }

    @Test
    public void testIsLast() throws Exception {

        hsql3RowRS.afterLast();
        volt3RowRS.afterLast();
        assertEquals(hsql3RowRS.isLast(), volt3RowRS.isLast());
    }

    @Test
    public void testAbsolute() throws Exception {

        assertEquals(hsql3RowRS.previous(), volt3RowRS.previous());
        assertEquals(hsql3RowRS.absolute(2), volt3RowRS.absolute(2));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        // afterLast position
        assertEquals(hsql3RowRS.absolute(4), volt3RowRS.absolute(4));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        // random afterLast position
        assertEquals(hsql3RowRS.absolute(10), volt3RowRS.absolute(10));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        // Absolute 0 position
        assertEquals(hsql3RowRS.absolute(0), volt3RowRS.absolute(0));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        // negative position
        assertEquals(hsql3RowRS.absolute(-3), volt3RowRS.absolute(-3));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());
    }

    @Test
    public void testRelative() throws Exception {

        assertEquals(hsql3RowRS.next(), volt3RowRS.next());
        assertEquals(hsql3RowRS.relative(2), volt3RowRS.relative(2));

        hsql3RowRS.beforeFirst();
        volt3RowRS.beforeFirst();
        assertEquals(hsql3RowRS.relative(0), volt3RowRS.relative(0));

        assertEquals(hsql3RowRS.relative(-3), volt3RowRS.relative(-3));
        assertEquals(hsql3RowRS.relative(2), volt3RowRS.relative(2));

        assertEquals(hsql3RowRS.previous(), volt3RowRS.previous());
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        assertEquals(hsql3RowRS.relative(2), volt3RowRS.relative(2));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        assertEquals(hsql3RowRS.absolute(4), volt3RowRS.absolute(4));
        assertEquals(hsql3RowRS.relative(-3), volt3RowRS.relative(-3));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        assertEquals(hsql3RowRS.relative(-10), volt3RowRS.relative(-10));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        hsql3RowRS.afterLast();
        volt3RowRS.afterLast();
        assertEquals(hsql3RowRS.relative(1), volt3RowRS.relative(1));

        hsql3RowRS.absolute(3);
        volt3RowRS.absolute(3);
        assertEquals(hsql3RowRS.absolute(3), volt3RowRS.absolute(3));
        assertEquals(hsql3RowRS.relative(-3), volt3RowRS.relative(-3));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        assertEquals(hsql3RowRS.absolute(1), volt3RowRS.absolute(1));
        hsql3RowRS.afterLast();
        volt3RowRS.afterLast();
        assertEquals(hsql3RowRS.relative(-4), volt3RowRS.relative(-4));
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());
    }

    @Test
    public void testNextBoundaryRows() throws SQLException {

        assertEquals(hsql3RowRS.last(), volt3RowRS.last());
        assertEquals(hsql3RowRS.next(), volt3RowRS.next());
        assertEquals(hsql3RowRS.getRow(), volt3RowRS.getRow());

        assertEquals(hsql3RowRS.absolute(4), volt3RowRS.absolute(4));
        assertTrue(hsql3RowRS.getRow() == volt3RowRS.getRow());
    }

    @Test
    public void testPreviousBoundaryRows() throws SQLException {

        assertEquals(hsql3RowRS.previous(), volt3RowRS.previous());
        assertTrue(hsql3RowRS.getRow() == volt3RowRS.getRow());
    }

    // Tests for empty table
    @Test
    public void testEmptyFirst() throws Exception {

        assertTrue(hsqlEmptyRS.first() == voltEmptyRS.first());
        assertTrue(hsqlEmptyRS.getRow() == voltEmptyRS.getRow());
    }

    @Test
    public void testEmptyLast() throws Exception {

        assertTrue(hsqlEmptyRS.last() == voltEmptyRS.last());
        assertTrue(hsqlEmptyRS.getRow() == voltEmptyRS.getRow());
    }

    @Test
    public void testEmptyBeforeFirst() throws Exception {

        hsql3RowRS.beforeFirst();
        volt3RowRS.beforeFirst();
        assertEquals(hsqlEmptyRS.getRow(), voltEmptyRS.getRow());

        assertEquals(hsql3RowRS.next(), volt3RowRS.next());
        assertEquals(hsqlEmptyRS.getRow(), voltEmptyRS.getRow());
    }

    @Test
    public void testEmptyAfterLast() throws Exception {

        hsql3RowRS.afterLast();
        volt3RowRS.afterLast();
        assertEquals(hsqlEmptyRS.getRow(), voltEmptyRS.getRow());

        assertEquals(hsql3RowRS.previous(), volt3RowRS.previous());
        assertEquals(hsqlEmptyRS.getRow(), voltEmptyRS.getRow());
    }

    @Test
    public void testEmptyAbsolute() throws Exception {

        assertTrue(hsqlEmptyRS.absolute(-1) == voltEmptyRS.absolute(-1));
        assertTrue(hsqlEmptyRS.getRow() == voltEmptyRS.getRow());

        assertTrue(hsqlEmptyRS.absolute(0) == voltEmptyRS.absolute(0));
        assertTrue(hsqlEmptyRS.getRow() == voltEmptyRS.getRow());

        assertTrue(hsqlEmptyRS.absolute(1) == voltEmptyRS.absolute(1));
        assertTrue(hsqlEmptyRS.getRow() == voltEmptyRS.getRow());
    }

    @Test
    public void testEmptyRelative() throws Exception {

        assertTrue(hsqlEmptyRS.relative(-1) == voltEmptyRS.relative(-1));
        assertTrue(hsqlEmptyRS.getRow() == voltEmptyRS.getRow());

        assertTrue(hsqlEmptyRS.relative(0) == voltEmptyRS.relative(0));
        assertTrue(hsqlEmptyRS.getRow() == voltEmptyRS.getRow());

        assertTrue(hsqlEmptyRS.relative(1) == voltEmptyRS.relative(1));
        assertTrue(hsqlEmptyRS.getRow() == voltEmptyRS.getRow());
    }
}