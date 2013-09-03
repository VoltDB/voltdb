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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ServerThread;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.InsertNewOrder;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb_testprocs.regressionsuites.multipartitionprocs.MultiSiteSelect;

public class TestJDBCDriver {
    static String testjar;
    static ServerThread server;
    static Connection conn;
    static TPCCProjectBuilder pb;

    @BeforeClass
    public static void setUp() throws ClassNotFoundException, SQLException {
        testjar = BuildDirectoryUtils.getBuildDirectoryPath() + File.separator
                + "jdbcdrivertest.jar";

        // compile a catalog
        pb = new TPCCProjectBuilder();
        pb.addDefaultSchema();
        pb.addDefaultPartitioning();
        pb.addProcedures(MultiSiteSelect.class, InsertNewOrder.class);
        pb.compile(testjar, 2, 0);

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
    }

    private static void stopServer() throws SQLException {
        if (conn != null) {
            conn.close();
            conn = null;
        }
        if (server != null) {
            try { server.shutdown(); } catch (InterruptedException e) { /*empty*/ }
            server = null;
        }
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
        tableTest(null, "%", 10);
    }

    @Test
    public void testFilterTableByType() throws SQLException {
        for (String type : JDBC4DatabaseMetaData.tableTypes) {
            int expected = 0;
            // TPCC has 10 tables and no views
            if (type.equals("TABLE")) {
                expected = 10;
            }
            tableTest(new String[] {type}, "%", expected);
        }
    }

    @Test
    public void testFilterTableByName() throws SQLException {
        // TPCC has 1 "ORDERS" tables
        tableTest(null, "ORDERS", 1);
         // TPCC has 1 "ORDER_" table
        tableTest(null, "ORDER_", 1);
         // TPCC has 2 tables that start with "O"
        tableTest(null, "O%", 2);
         // TPCC has 5 tables with names containing "ST"
        tableTest(null, "%ST%", 5);
        // TPCC has 10 tables
        tableTest(null, "", 10);
        // TPCC has 10 tables, but won't match the types array
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
        tableColumnTest("CUSTOMER%", null, 26);
        tableColumnTest("CUSTOMER%", "", 26);
        tableColumnTest("CUSTOMER%", "%MIDDLE", 1);
        tableColumnTest("CUSTOMER", "____", 1);
        tableColumnTest("%", "%ID", 32);
        tableColumnTest(null, "%ID", 32);
        tableColumnTest(null, "", 97);
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
        indexInfoTest("ORDERS", false, 10);
    }

    @Test
    public void testFilterIndexByUnique() throws SQLException {
        indexInfoTest("ORDERS", true, 7);
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
        assertEquals(3, count);
    }

    @Test
    public void testAllProcedures() throws SQLException {
        ResultSet procedures =
                conn.getMetaData().getProcedures("blah", "blah", "%");
        int count = 0;
        List<String> names = Arrays.asList(new String[] {"MultiSiteSelect",
                                                         "InsertNewOrder"});
        while (procedures.next()) {
            String procedure = procedures.getString("PROCEDURE_NAME");
            if (procedure.contains(".")) {
                // auto-generated CRUD
            } else {
                assertTrue(names.contains(procedure));
            }
            count++;
        }
        // 7 tables * 4 CRUD/table + 2 procedures +
        // 3 for replicated crud and 2 for insert where partition key !in primary
        assertEquals(7 * 4 + 2 + 3 + 2, count);
    }

    @Test
    public void testFilterProcedureByName() {
        try {
            conn.getMetaData().getProcedures("blah", "blah", "InsertNewOrder");
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
        procedureColumnTest("InsertNewOrder", null, 3);
        procedureColumnTest("InsertNewOrder", "%", 3);
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
        assertEquals(3, count);
    }

    @Test
    public void testResultSetMetaData() throws SQLException {
        ResultSetMetaData meta = conn.getMetaData().getTableTypes().getMetaData();
        // JDBC index starts at 1!!!!!!!!!!!!!!!!!!!!!!!
        assertEquals(String.class.getName(), meta.getColumnClassName(1));
        assertTrue(meta.getColumnDisplaySize(1) > 0);
        assertEquals(Types.VARCHAR, meta.getColumnType(1));
        assertEquals("VARCHAR", meta.getColumnTypeName(1));
        assertTrue(meta.getPrecision(1) > 0);
        assertEquals(0, meta.getScale(1));
        assertTrue(meta.isCaseSensitive(1));
        assertFalse(meta.isSigned(1));
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
        CallableStatement cs = conn.prepareCall("{call InsertNewOrder(?, ?, ?)}");
        cs.setInt(1, 55);
        cs.setInt(2, 66);
        cs.setInt(3, 77);
        cs.execute();
        try {
            cs.setInt(1, 55);
            cs.setInt(2, 66);
            cs.setInt(3, 77);
            cs.execute();
        } catch (SQLException e) {
            // Since it's a GENERAL_ERROR we need to look for a string by pattern.
            assertEquals(e.getSQLState(), SQLError.GENERAL_ERROR);
            assertTrue(e.getMessage().contains("violation of constraint"));
        }
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
}
