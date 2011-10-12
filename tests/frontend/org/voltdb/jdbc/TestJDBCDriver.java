/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;

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

    @BeforeClass
    public static void setUp() throws Exception {
        testjar = BuildDirectoryUtils.getBuildDirectoryPath() + File.separator
                + "jdbcdrivertest.jar";

        // compile a catalog
        TPCCProjectBuilder pb = new TPCCProjectBuilder();
        pb.addDefaultSchema();
        pb.addDefaultPartitioning();
        pb.addProcedures(MultiSiteSelect.class, InsertNewOrder.class);
        pb.compile(testjar, 2, 0);

        server = new ServerThread(testjar, pb.getPathToDeployment(),
                                  BackendTarget.NATIVE_EE_JNI);
        server.start();
        server.waitForInitialization();

        Class.forName("org.voltdb.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:voltdb://localhost:21212");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        conn.close();
        server.shutdown();
        File f = new File(testjar);
        f.delete();
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
    private void tableTest(String[] types, int expected) throws SQLException {
        ResultSet tables = conn.getMetaData().getTables("blah", "blah", "%",
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
        tableTest(null, 10);
    }

    @Test
    public void testFilterTableByType() throws SQLException {
        for (String type : JDBC4DatabaseMetaData.tableTypes) {
            int expected = 0;
            // TPCC has 10 tables and no views
            if (type.equals("TABLE")) {
                expected = 10;
            }
            tableTest(new String[] {type}, expected);
        }
    }

    @Test
    public void testFilterTableByName() {
        try {
            conn.getMetaData().getTables("blah", "blah", "ORDERS", null);
        } catch (SQLException e) {
            return;
        }
        fail("Should fail, we don't support filtering by table name");
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
            assertEquals(table, columns.getString("TABLE_NAME"));
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
        // 7 tables * 4 CRUD/table + 2 procedures
        assertEquals(7 * 4 + 2, count);
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
}
