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

import static junit.framework.Assert.assertFalse;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ProcedurePartitionData;
import org.voltdb.ServerThread;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltType;
import org.voltdb.client.ArbitraryDurationProc;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.TestClientFeatures;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.types.VoltDecimalHelper;
import org.voltdb.utils.MiscUtils;

public class TestJDBCDriver {
    static String testjar;
    static ServerThread server;
    static Connection conn;
    static Connection myconn;
    static VoltProjectBuilder pb;



    @BeforeClass
    public static void setUp() throws Exception {
        // Fake out the constraints that were previously written against the
        // TPCC schema
        String ddl =
            "CREATE TABLE TT(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE ORDERS(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE ORDER_THIS(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE LAST(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE STEAL_THIS_TABLE(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE BLAST_IT(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE ROBBIE_MUSTOE(A1 INTEGER NOT NULL, A2_ID INTEGER, PRIMARY KEY(A1));" +
            "CREATE TABLE CUSTOMER(A1 INTEGER NOT NULL, A2_ID INTEGER, A3 INTEGER, A4 INTEGER, " +
                             "A5_0 INTEGER, A6 INTEGER, A7_ID INTEGER, A8 INTEGER, A9 INTEGER, " +
                             "A10 INTEGER, A11 INTEGER, A12_ID INTEGER, A13 INTEGER, A14 INTEGER, " +
                             "A15 INTEGER, A16 INTEGER, A17_ID INTEGER, A18 INTEGER, A19 INTEGER, " +
                             "A20 INTEGER, A21 INTEGER, A22_ID INTEGER, A23 INTEGER, A24_ID INTEGER, " +
                             "A25 INTEGER, A26_MIDDLE INTEGER, " +
                             "PRIMARY KEY(A1));" +
            "CREATE TABLE NUMBER_NINE(A1 INTEGER NOT NULL, A2 INTEGER, A3_BIN VARBINARY, PRIMARY KEY(A1));" +
            "CREATE TABLE WAREHOUSE(A1 INTEGER NOT NULL, A2 INTEGER, A3 INTEGER, A4_ID INTEGER, " +
                             "A5 INTEGER, A6 INTEGER, A7 INTEGER, A8 INTEGER, W_ID INTEGER, " +
                             "PRIMARY KEY(A1));" +
            "CREATE TABLE ALL_TYPES("
                             + "A1 TINYINT NOT NULL, "
                             + "A2 TINYINT, "
                             + "A3 SMALLINT, "
                             + "A4 INTEGER, "
                             + "A5 BIGINT, "
                             + "A6 FLOAT, "
                             + "A7 VARCHAR(10), "
                             + "A8 VARBINARY(10), "
                             + "A9 TIMESTAMP, "
                             + "A10 DECIMAL, "
                             + "A11 GEOGRAPHY_POINT, "
                             + "A12 GEOGRAPHY(2048), "
                             + "PRIMARY KEY(A1));" +
            "CREATE UNIQUE INDEX UNIQUE_ORDERS_HASH ON ORDERS (A1, A2_ID); " +
            "CREATE INDEX IDX_ORDERS_HASH ON ORDERS (A2_ID);";


        pb = new VoltProjectBuilder();
        pb.addLiteralSchema(ddl);
        pb.addSchema(TestClientFeatures.class.getResource("clientfeatures.sql"));
        pb.addProcedure(ArbitraryDurationProc.class);
        pb.addPartitionInfo("TT", "A1");
        pb.addPartitionInfo("ORDERS", "A1");
        pb.addPartitionInfo("LAST", "A1");
        pb.addPartitionInfo("BLAST_IT", "A1");
        pb.addPartitionInfo("ROBBIE_MUSTOE", "A1");
        pb.addPartitionInfo("CUSTOMER", "A1");
        pb.addPartitionInfo("NUMBER_NINE", "A1");
        pb.addPartitionInfo("ALL_TYPES", "A1");
        pb.addStmtProcedure("InsertAllTypes", "INSERT INTO ALL_TYPES VALUES(?,?,?,?,?,?,?,?,?,?,?,?);",
                new ProcedurePartitionData("ALL_TYPES", "A1"));
        pb.addStmtProcedure("InsertA", "INSERT INTO TT VALUES(?,?);",
                new ProcedurePartitionData("TT", "A1"));
        pb.addStmtProcedure("SelectB", "SELECT * FROM TT;");
        pb.addStmtProcedure("SelectC", "SELECT * FROM ALL_TYPES;");
        boolean success = pb.compile(Configuration.getPathToCatalogForTest("jdbcdrivertest.jar"), 3, 1, 0);
        assert(success);
        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest("jdbcdrivertest.xml"));
        testjar = Configuration.getPathToCatalogForTest("jdbcdrivertest.jar");

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
        if(ClientConfig.ENABLE_SSL_FOR_TEST) {
            conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212?" + JDBCTestCommons.SSL_URL_SUFFIX);
        }
        else {
            conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
        }

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
    public void testURLParsing() throws Exception
    {
        String url = "jdbc:voltdb://server1:21212,server2?prop1=true&prop2=false";
        String[] servers = Driver.getServersFromURL(url);
        assertEquals("server1:21212", servers[0]);
        assertEquals("server2", servers[1]);
        Map<String, String> props = Driver.getPropsFromURL(url);
        assertEquals(2, props.size());
        assertEquals("true", props.get("prop1"));
        assertEquals("false", props.get("prop2"));
    }

    @Test
    public void testTableTypes() throws SQLException {
        ResultSet types = conn.getMetaData().getTableTypes();
        int count = 0;
        List<String> typeList = Arrays.asList(JDBC4DatabaseMetaData.tableTypes);
        while (types.next()) {
            assertTrue(typeList.contains(types.getString("TABLE_TYPE")));
            count++;
        }
        assertEquals(count, typeList.size());
    }

    /**
     * Retrieve table of the given types and check if the count matches the
     * expected values
     *
     * @param types The table type
     * @param expected Expected total count
     * @throws SQLException
     */
    private void tableTest(String[] types, String pattern, int expected) throws SQLException {
        ResultSet tables = conn.getMetaData().getTables("blah", "blah", pattern,
                                                        types);
        int count = 0;
        List<String> typeList = Arrays.asList(JDBC4DatabaseMetaData.tableTypes);
        if (types != null) {
            typeList = Arrays.asList(types);
        }

        while (tables.next()) {
            assertFalse(tables.getString("TABLE_NAME").isEmpty());
            assertTrue(typeList.contains(tables.getString("TABLE_TYPE")));
            count++;
        }
        assertEquals(expected, count);
    }

    @Test
    public void testAllTables() throws SQLException {
        // TPCC has 10 tables
        tableTest(null, "%", 13);
    }

    @Test
    public void testFilterTableByType() throws SQLException {
        for (String type : JDBC4DatabaseMetaData.tableTypes) {
            int expected = 0;
            // TPCC has 10 tables and no views
            if (type.equals("TABLE")) {
                expected = 13;
            }
            tableTest(new String[] {type}, "%", expected);
        }
    }

    @Test
    public void testFilterTableByName() throws SQLException {
        // schema has 1 "ORDERS" tables
        tableTest(null, "ORDERS", 1);
         // schema has 1 "ORDER_" table
        tableTest(null, "ORDER_", 1);
         // schema has 2 tables that start with "O"
        tableTest(null, "O%", 2);
         // schema has 5 tables with names containing "ST"
        tableTest(null, "%ST%", 5);
        // schema has 13 tables
        tableTest(null, "", 13);
        // schema has 13 tables, but won't match the types array
        tableTest(new String[] {""}, "", 0);
    }

    @Test
    public void testFilterTableByNameNoMatch() throws SQLException {
        // No matches
        tableTest(null, "%xyzzy", 0);
        tableTest(null, "_", 0);
        tableTest(null, "gobbly_gook", 0);
        tableTest(null, "noname", 0);
    }

    /**
     * Retrieve columns of a given table and check if the count is expected.
     *
     * @param table Table name
     * @param column Column name or null to get all
     * @param expected Expected number of columns
     * @throws SQLException
     */
    private void tableColumnTest(String table, String column, int expected)
    throws SQLException {
        ResultSet columns = conn.getMetaData().getColumns("blah", "blah",
                                                          table, column);
        int count = 0;
        while (columns.next()) {
            assertFalse(columns.getString("COLUMN_NAME").isEmpty());
            count++;
        }
        assertEquals(expected, count);
    }

    @Test
    public void testAllColumns() throws SQLException {
        tableColumnTest("WAREHOUSE", null, 9);
        tableColumnTest("WAREHOUSE", "%", 9);
    }

    @Test
    public void testFilterColumnByName() throws SQLException {
        tableColumnTest("WAREHOUSE", "W_ID", 1);
    }

    @Test
    public void testFilterColumnByWildcard() throws SQLException {
        tableColumnTest("CUSTOMER%", null, 26); // columns of tables starting with "CUSTOMER"
        tableColumnTest("CUSTOMER%", "", 26);
        tableColumnTest("CUSTOMER%", "%MIDDLE", 1);
        tableColumnTest("CUSTOMER", "____", 1);
        tableColumnTest("%", "%ID", 13);
        tableColumnTest(null, "%ID", 13);
        tableColumnTest(null, "", 76); // all the columns of all the tables
    }

    /**
     * Retrieve index info of a table and check the count.
     *
     * @param table
     *            Table name
     * @param unique
     *            Unique or not
     * @param expected
     *            Expected count
     * @throws SQLException
     */
    private void indexInfoTest(String table, boolean unique, int expected)
    throws SQLException {
        ResultSet indexes = conn.getMetaData().getIndexInfo("blah", "blah",
                                                            table, unique,
                                                            false);
        int count = 0;
        while (indexes.next()) {
            assertEquals(table, indexes.getString("TABLE_NAME"));
            if (unique) {
                assertEquals(false, indexes.getBoolean("NON_UNIQUE"));
            }
            count++;
        }
        assertEquals(expected, count);
    }

    @Test
    public void testAllIndexes() throws SQLException {
        indexInfoTest("ORDERS", false, 4);
    }

    @Test
    public void testFilterIndexByUnique() throws SQLException {
        indexInfoTest("ORDERS", true, 3);
    }

    @Test
    public void testAllPrimaryKeys() throws SQLException {
        ResultSet keys = conn.getMetaData().getPrimaryKeys("blah", "blah",
                                                           "ORDERS");
        int count = 0;
        while (keys.next()) {
            assertEquals("ORDERS", keys.getString("TABLE_NAME"));
            count++;
        }
        assertEquals(1, count);
    }

    @Test
    public void testAllProcedures() throws SQLException {
        ResultSet procedures =
                conn.getMetaData().getProcedures("blah", "blah", "%");
        int count = 0;
        List<String> names = Arrays.asList(new String[] {"InsertA", "InsertAllTypes",
            "SelectB", "SelectC", "ArbitraryDurationProc"});
        while (procedures.next()) {
            String procedure = procedures.getString("PROCEDURE_NAME");
            if (procedure.contains(".")) {
                // auto-generated CRUD
            } else {
                assertTrue(names.contains(procedure));
            }
            count++;
        }
        System.out.println("Procedure count is: " + count);
        // After adding .upsert stored procedure
        // 9 tables * 5 CRUD/table + 4 procedures +
        // 4 tables * 4 for replicated crud
        assertEquals(10 * 5 + 4 + 3 * 4, count);
    }

    @Test
    public void testFilterProcedureByName() {
        try {
            conn.getMetaData().getProcedures("blah", "blah", "InsertA");
        } catch (SQLException e) {
            return;
        }
        fail("Should fail, we don't support procedure filtering by name");
    }

    /**
     * Retrieve columns of a given procedure and check if the count is expected.
     *
     * @param procedure Procedure name
     * @param column Column name or null to get all
     * @param expected Expected number of columns
     * @throws SQLException
     */
    private void procedureColumnTest(String procedure, String column,
                                     int expected)
    throws SQLException {
        ResultSet columns =
                conn.getMetaData().getProcedureColumns("blah", "blah",
                                                       procedure, column);
        int count = 0;
        while (columns.next()) {
            assertEquals(procedure, columns.getString("PROCEDURE_NAME"));
            assertFalse(columns.getString("COLUMN_NAME").isEmpty());
            count++;
        }
        assertEquals(expected, count);
    }

    @Test
    public void testAllProcedureColumns() throws SQLException {
        procedureColumnTest("InsertA", null, 2);
        procedureColumnTest("InsertA", "%", 2);
    }

    @Test
    public void testFilterProcedureColumnsByName() throws SQLException {
        ResultSet procedures =
                conn.getMetaData().getProcedures("blah", "blah", "%");
        int count = 0;
        while (procedures.next()) {
            String proc = procedures.getString("PROCEDURE_NAME");
            // Skip CRUD
            if (proc.contains(".")) {
                continue;
            }

            ResultSet columns = conn.getMetaData().getProcedureColumns("b", "b",
                                                                       proc,
                                                                       null);
            while (columns.next()) {
                String column = columns.getString("COLUMN_NAME");
                procedureColumnTest(proc, column, 1);
                count++;
            }
        }
        assertEquals(15, count);
    }

    @Test
    public void testResultSetMetaData() throws SQLException {
        CallableStatement cs = conn.prepareCall("{call SelectC}");
        ResultSet results = cs.executeQuery();
        ResultSetMetaData meta = results.getMetaData();
        assertEquals(12, meta.getColumnCount());
        // JDBC index starts at 1!!!!!!!!!!!!!!!!!!!!!!!
        assertEquals(Byte.class.getName(), meta.getColumnClassName(1));
        assertEquals(java.sql.Types.TINYINT, meta.getColumnType(1));
        assertEquals("TINYINT", meta.getColumnTypeName(1));
        assertEquals(7, meta.getPrecision(1));
        assertEquals(0, meta.getScale(1));
        assertFalse(meta.isCaseSensitive(1));
        assertTrue(meta.isSigned(1));
        assertEquals(4, meta.getColumnDisplaySize(1));

        assertEquals(Short.class.getName(), meta.getColumnClassName(3));
        assertEquals(java.sql.Types.SMALLINT, meta.getColumnType(3));
        assertEquals("SMALLINT", meta.getColumnTypeName(3));
        assertEquals(15, meta.getPrecision(3));
        assertEquals(0, meta.getScale(3));
        assertFalse(meta.isCaseSensitive(3));
        assertTrue(meta.isSigned(3));
        assertEquals(6, meta.getColumnDisplaySize(3));

        assertEquals(Integer.class.getName(), meta.getColumnClassName(4));
        assertEquals(java.sql.Types.INTEGER, meta.getColumnType(4));
        assertEquals("INTEGER", meta.getColumnTypeName(4));
        assertEquals(31, meta.getPrecision(4));
        assertEquals(0, meta.getScale(4));
        assertFalse(meta.isCaseSensitive(4));
        assertTrue(meta.isSigned(4));
        assertEquals(11, meta.getColumnDisplaySize(4));

        assertEquals(Long.class.getName(), meta.getColumnClassName(5));
        assertEquals(java.sql.Types.BIGINT, meta.getColumnType(5));
        assertEquals("BIGINT", meta.getColumnTypeName(5));
        assertEquals(63, meta.getPrecision(5));
        assertEquals(0, meta.getScale(5));
        assertFalse(meta.isCaseSensitive(5));
        assertTrue(meta.isSigned(5));
        assertEquals(20, meta.getColumnDisplaySize(5));

        assertEquals(Double.class.getName(), meta.getColumnClassName(6));
        assertEquals(java.sql.Types.FLOAT, meta.getColumnType(6));
        assertEquals("FLOAT", meta.getColumnTypeName(6));
        assertEquals(53, meta.getPrecision(6));
        assertEquals(0, meta.getScale(6));
        assertFalse(meta.isCaseSensitive(6));
        assertTrue(meta.isSigned(6));
        assertEquals(8, meta.getColumnDisplaySize(6));

        assertEquals(String.class.getName(), meta.getColumnClassName(7));
        assertEquals(java.sql.Types.VARCHAR, meta.getColumnType(7));
        assertEquals("VARCHAR", meta.getColumnTypeName(7));
        assertEquals(VoltType.MAX_VALUE_LENGTH, meta.getPrecision(7));
        assertEquals(0, meta.getScale(7));
        assertTrue(meta.isCaseSensitive(7));
        assertFalse(meta.isSigned(7));
        assertEquals(128, meta.getColumnDisplaySize(7));

        assertEquals(Byte[].class.getCanonicalName(), meta.getColumnClassName(8));
        assertEquals(java.sql.Types.VARBINARY, meta.getColumnType(8));
        assertEquals("VARBINARY", meta.getColumnTypeName(8));
        assertEquals(VoltType.MAX_VALUE_LENGTH, meta.getPrecision(8));
        assertEquals(0, meta.getScale(8));
        assertFalse(meta.isCaseSensitive(8));
        assertFalse(meta.isSigned(8));
        assertEquals(128, meta.getColumnDisplaySize(8));

        assertEquals(Timestamp.class.getName(), meta.getColumnClassName(9));
        assertEquals(java.sql.Types.TIMESTAMP, meta.getColumnType(9));
        assertEquals("TIMESTAMP", meta.getColumnTypeName(9));
        assertEquals(63, meta.getPrecision(9));
        assertEquals(0, meta.getScale(9));
        assertFalse(meta.isCaseSensitive(9));
        assertFalse(meta.isSigned(9));
        assertEquals(32, meta.getColumnDisplaySize(9));

        assertEquals(BigDecimal.class.getName(), meta.getColumnClassName(10));
        assertEquals(java.sql.Types.DECIMAL, meta.getColumnType(10));
        assertEquals("DECIMAL", meta.getColumnTypeName(10));
        assertEquals(VoltDecimalHelper.kDefaultPrecision, meta.getPrecision(10));
        assertEquals(12, meta.getScale(10));
        assertFalse(meta.isCaseSensitive(10));
        assertTrue(meta.isSigned(10));
        assertEquals(40, meta.getColumnDisplaySize(10));

        assertEquals(org.voltdb.types.GeographyPointValue.class.getName(), meta.getColumnClassName(11));
        assertEquals(java.sql.Types.OTHER, meta.getColumnType(11));
        assertEquals("GEOGRAPHY_POINT", meta.getColumnTypeName(11));
        assertEquals(0, meta.getPrecision(11));
        assertEquals(0, meta.getScale(11));
        assertFalse(meta.isCaseSensitive(11));
        assertFalse(meta.isSigned(11));
        assertEquals(42, meta.getColumnDisplaySize(11));

        assertEquals(org.voltdb.types.GeographyValue.class.getName(), meta.getColumnClassName(12));
        assertEquals(java.sql.Types.OTHER, meta.getColumnType(12));
        assertEquals("GEOGRAPHY", meta.getColumnTypeName(12));
        assertEquals(1048576, meta.getPrecision(12));
        assertEquals(0, meta.getScale(12));
        assertFalse(meta.isCaseSensitive(12));
        assertFalse(meta.isSigned(12));
        assertEquals(128, meta.getColumnDisplaySize(12));
    }

    @Test
    public void testBadProcedureName() throws SQLException {
        CallableStatement cs = conn.prepareCall("{call Oopsy(?)}");
        cs.setLong(1, 99);
        try {
            cs.execute();
        } catch (SQLException e) {
            // Since it's a GENERAL_ERROR we need to look for a string by pattern.
            assertEquals(e.getSQLState(), SQLError.GENERAL_ERROR);
            assertTrue(Pattern.matches(".*Procedure .* not found.*", e.getMessage()));
        }
    }

    @Test
    public void testDoubleInsert() throws SQLException {
        // long i_id, long i_im_id, String i_name, double i_price, String i_data
        CallableStatement cs = conn.prepareCall("{call InsertA(?, ?)}");
        cs.setInt(1, 55);
        cs.setInt(2, 66);
        cs.execute();
        try {
            cs.setInt(1, 55);
            cs.setInt(2, 66);
            cs.execute();
        } catch (SQLException e) {
            // Since it's a GENERAL_ERROR we need to look for a string by pattern.
            assertEquals(e.getSQLState(), SQLError.GENERAL_ERROR);
            assertTrue(e.getMessage().contains("violation of constraint"));
        }
    }

    // Check that the null type is handled the same way as specifying the correct type
    // this is for spring framework compatibility
    @Test
    public void testInsertNulls() throws SQLException {
        // First inserted row contains all null values (this will cause VoltType.NULL_STRING_OR_VARBINARY to be inserted in every field)
        CallableStatement cs = conn.prepareCall("{call InsertAllTypes(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
        cs.setInt(1, 0);
        cs.setNull(2, Types.NULL);
        cs.setNull(3, Types.NULL);
        cs.setNull(4, Types.NULL);
        cs.setNull(5, Types.NULL);
        cs.setNull(6, Types.NULL);
        cs.setNull(7, Types.NULL);
        cs.setNull(8, Types.NULL);
        cs.setNull(9, Types.NULL);
        cs.setNull(10, Types.NULL);
        cs.setNull(11, Types.NULL);
        cs.setNull(12, Types.NULL);
        cs.execute();
        // Second inserted row contains the specific type of the field causing the typed nulls to be inserted
        cs = conn.prepareCall("{call InsertAllTypes(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
        cs.setInt(1, 1);
        cs.setNull(2, Types.TINYINT);
        cs.setNull(3, Types.SMALLINT);
        cs.setNull(4, Types.INTEGER);
        cs.setNull(5, Types.BIGINT);
        cs.setNull(6, Types.DOUBLE);
        cs.setNull(7, Types.VARCHAR);
        cs.setNull(8, Types.VARBINARY);
        cs.setNull(9, Types.TIMESTAMP);
        cs.setNull(10, Types.DECIMAL);
        cs.setNull(11, Types.OTHER);
        cs.setNull(12, Types.OTHER);
        cs.execute();
        // Call SelectC (select * from ALL_TYPES)
        cs = conn.prepareCall("{call SelectC}");
        ResultSet results = cs.executeQuery();

        // Retrieve the values for the first row
        results.next();
        byte a2 = results.getByte(2);
        short a3 = results.getShort(3);
        int a4 = results.getInt(4);
        long a5 = results.getLong(5);
        double a6 = results.getDouble(6);
        String a7 = results.getString(7);
        byte[] a8 = results.getBytes(8);
        Timestamp a9 = results.getTimestamp(9);
        BigDecimal a10 = results.getBigDecimal(10);

        // Compare the second row values with the first row
        results.next();
        assertEquals(results.getByte(2), a2);
        assertEquals(results.getShort(3), a3);
        assertEquals(results.getInt(4), a4);
        assertEquals(results.getLong(5), a5);
        assertEquals(results.getDouble(6), a6, 0);
        assertEquals(results.getString(7), a7);
        assertEquals(results.getBytes(8), a8);
        assertEquals(results.getTimestamp(9), a9);
        assertEquals(results.getBigDecimal(10), a10);
    }

    public void testVersionMetadata() throws SQLException {
        int major = conn.getMetaData().getDatabaseMajorVersion();
        int minor = conn.getMetaData().getDatabaseMinorVersion();
        assertTrue(major >= 2);
        assertTrue(minor >= 0);
    }

    @Test
    public void testLostConnection() throws SQLException, ClassNotFoundException {
        // Break the current connection and try to execute a procedure call.
        CallableStatement cs = conn.prepareCall("{call Oopsy(?)}");
        stopServer();
        cs.setLong(1, 99);
        try {
            cs.execute();
        } catch (SQLException e) {
            assertEquals(e.getSQLState(), SQLError.CONNECTION_FAILURE);
        }
        // Restore a working connection for any remaining tests
        startServer();
    }

    @Test
    public void testSetMaxRows() throws SQLException    {
        // Add 10 rows
        PreparedStatement ins = conn.prepareCall("{call InsertA(?, ?)}");
        for (int i = 0; i < 10; i++) {
            ins.setInt(1, i);
            ins.setInt(2, i + 50);
            ins.execute();
        }

        // check for our 10 rows
        PreparedStatement cs = conn.prepareCall("{call SelectB}");
        ResultSet rs = cs.executeQuery();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(10, count);

        // constrain to 5 and try again.
        cs.setMaxRows(5);
        assertEquals(5, cs.getMaxRows());
        rs = cs.executeQuery();
        count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(5, count);

        // Verify 0 gets us everything again
        cs.setMaxRows(0);
        assertEquals(0, cs.getMaxRows());
        rs = cs.executeQuery();
        count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(10, count);

        // Go for spot-on
        cs.setMaxRows(10);
        assertEquals(10, cs.getMaxRows());
        rs = cs.executeQuery();
        count = 0;
        while (rs.next()) {
            count++;
        }
        assertEquals(10, count);
    }

    // Test to check Query without Timeout
    @Test
    public  void testQueryNotTimeout() throws SQLException {
        PreparedStatement stmt = conn.prepareCall("{call ArbitraryDurationProc(?)}");

        // Make the proc wait 30 seconds before returning.
        stmt.setLong(1, 30000);
        try {
            stmt.execute();
        } catch (SQLException ex) {
            fail("Query threw exception when not expected to: " + ex.getSQLState());
        }
    }

    // execute a Query with a Timeout set
    // return true if timeout (excecptionCalled)
    public Boolean runQueryWithTimeout(int timeQuery, int timeout)
            throws SQLException {
        boolean exceptionCalled = false;
        PreparedStatement stmt = myconn
                .prepareCall("{call ArbitraryDurationProc(?)}");

        // Now make it timeout
        stmt.setLong(1, timeQuery);
        try {
            stmt.setQueryTimeout(timeout);
            stmt.execute();
        } catch (SQLException ex) {
            System.out.println("Query timed out: " + ex.getSQLState());
            exceptionCalled = true;
        }
        return exceptionCalled;
    }

    // Test to check Query Timeout with Time Unit being setup
    @Test
    public void testQueryTimeout() throws Exception {
        Properties props = new Properties();
        // Check default setting, timeout unit should be TimeUnit.SECONDS
        myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        assertTrue(runQueryWithTimeout(4000, 1));
        assertFalse(runQueryWithTimeout(4000, 30));
        assertTrue(runQueryWithTimeout(4000, -1));
        myconn.close();

        // Check set time unit to MILLISECONDS
        // through url
        myconn = JDBCTestCommons.getJdbcConnection(
                "jdbc:voltdb://localhost:21212?jdbc.querytimeout.unit=milliseconds",
                props);
        assertTrue(runQueryWithTimeout(4000, 1));
        myconn.close();

        // Check set time unit to MILLISECONDS
        // through Java Propeties
        props.setProperty(JDBC4Connection.QUERYTIMEOUT_UNIT, "milliseconds");
        myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        assertTrue(runQueryWithTimeout(4000, 1));
        myconn.close();

        // Check set time unit to other unsupported unit, should by default
        // still use TimeUnit.SECONDS
        props.setProperty(JDBC4Connection.QUERYTIMEOUT_UNIT, "nanoseconds");
        myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        assertTrue(runQueryWithTimeout(4000, 1));
        myconn.close();
    }

    private void checkSafeMode(Connection myconn)
    {
        boolean threw = false;
        try {
            myconn.commit();
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertTrue(threw);
        threw = false;
        // autocommit true should never throw
        try {
            myconn.setAutoCommit(true);
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
        threw = false;
        try {
            myconn.setAutoCommit(false);
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertTrue(threw);
        threw = false;
        try {
            myconn.rollback();
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertTrue(threw);
    }

    private void checkCarlosDanger(Connection myconn)
    {
        boolean threw = false;
        try {
            myconn.commit();
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
        threw = false;
        // autocommit true should never throw
        try {
            myconn.setAutoCommit(true);
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
        threw = false;
        try {
            myconn.setAutoCommit(false);
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
        threw = false;
        try {
            myconn.rollback();
        }
        catch (SQLException bleh) {
            threw = true;
        }
        assertFalse(threw);
    }

    @Test
    public void testSafetyOffThroughProperties() throws Exception
    {
        Properties props = new Properties();
        // Check default behavior
        myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        checkSafeMode(myconn);
        myconn.close();

        // Check commit and setAutoCommit
        props.setProperty(JDBC4Connection.COMMIT_THROW_EXCEPTION, "true");
        props.setProperty(JDBC4Connection.ROLLBACK_THROW_EXCEPTION, "true");
        myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        checkSafeMode(myconn);
        myconn.close();

        props.setProperty(JDBC4Connection.COMMIT_THROW_EXCEPTION, "false");
        props.setProperty(JDBC4Connection.ROLLBACK_THROW_EXCEPTION, "false");
        myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        checkCarlosDanger(myconn);
        myconn.close();
    }

    @Test
    public void testSafetyOffThroughURL() throws Exception
    {
        Properties props = new Properties();
        // Check default behavior
        myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212", props);
        checkSafeMode(myconn);
        myconn.close();

        // Check commit and setAutoCommit
        myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212?" +
                JDBC4Connection.COMMIT_THROW_EXCEPTION + "=true" + "&" +
                JDBC4Connection.ROLLBACK_THROW_EXCEPTION + "=true", props);
        checkSafeMode(myconn);
        myconn.close();

        myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212?" +
                JDBC4Connection.COMMIT_THROW_EXCEPTION + "=false" + "&" +
                JDBC4Connection.ROLLBACK_THROW_EXCEPTION + "=false", props);
        checkCarlosDanger(myconn);
        myconn.close();
    }

    @Test
    public void testSafetyOffThroughSystemProp() throws Exception {
        String tmppath = "/tmp/" + System.getProperty("user.name");
        String propfile = tmppath + "/voltdb.properties";
        // start clean
        File tmp = new File(propfile);
        if (tmp.exists()) {
            tmp.delete();
        }
        try {
            Properties props = new Properties();
            props.setProperty(JDBC4Connection.COMMIT_THROW_EXCEPTION, "false");
            props.setProperty(JDBC4Connection.ROLLBACK_THROW_EXCEPTION, "false");
            FileOutputStream out = null;
            try {
                out = new FileOutputStream(propfile);
                props.store(out, "");
            } catch (FileNotFoundException e) {
                fail();
            } catch (IOException e) {
                fail();
            }        finally {
                if (out != null) {
                    try {
                        out.close();
                    } catch (IOException e) { }
                }
            }

            System.setProperty(Driver.JDBC_PROP_FILE_PROP, propfile);
            props = new Properties();
            myconn = JDBCTestCommons.getJdbcConnection("jdbc:voltdb://localhost:21212", props);
            checkCarlosDanger(myconn);
            myconn.close();
        }
        finally {
            // end clean
            if (tmp.exists()) {
                tmp.delete();
            }
        }
    }

    @Test
    public void testSSLPropertiesFromURL() {
        String url = "jdbc:voltdb://server1:21212,server2?"
                + "ssl=true&truststore=/tmp/xyz&truststorepassword=password";
        String[] servers = Driver.getServersFromURL(url);
        assertEquals("server1:21212", servers[0]);
        assertEquals("server2", servers[1]);
        Map<String, String> propMap = Driver.getPropsFromURL(url);
        assertEquals(3, propMap.size());
        assertEquals("true", propMap.get(Driver.SSL_PROP));
        assertEquals("/tmp/xyz", propMap.get(Driver.TRUSTSTORE_CONFIG_PROP));
        assertEquals("password", propMap.get(Driver.TRUSTSTORE_PASSWORD_PROP));
    }
}
