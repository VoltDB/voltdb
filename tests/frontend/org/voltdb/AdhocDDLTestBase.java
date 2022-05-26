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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.utils.MiscUtils;

import java.util.ArrayList;
import java.util.List;

public class AdhocDDLTestBase extends JUnit4LocalClusterTest {

    protected ServerThread m_localServer;
    protected Client m_client;

    protected void startSystem(VoltDB.Configuration config) throws Exception
    {
        startServer(config);
        startClient(null);
    }

    protected void teardownSystem() throws Exception
    {
        stopClient();
        stopServer();
    }

    protected void startServer(VoltDB.Configuration config) throws Exception
    {
        m_localServer = new ServerThread(config);
        m_localServer.start();
        m_localServer.waitForInitialization();
    }

    protected void startClient(ClientConfig clientConfig) throws Exception
    {
        if (clientConfig != null) {
            m_client = ClientFactory.createClient(clientConfig);
        }
        else {
            m_client = ClientFactory.createClient();
        }
        m_client.createConnection("localhost");
    }

    protected void stopServer() throws Exception
    {
        if (m_localServer != null) {
            m_localServer.shutdown();
            m_localServer.join();
            m_localServer = null;
        }
    }

    protected void stopClient() throws Exception
    {
        if (m_client != null) {
            m_client.close();
            m_client = null;
        }
    }

    protected boolean findTableInSystemCatalogResults(String table) throws Exception
    {
        VoltTable tables = m_client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingRow(tables, "TABLE_NAME", table);
        return found;
    }

    protected boolean findIndexInSystemCatalogResults(String index) throws Exception
    {
        VoltTable indexinfo = m_client.callProcedure("@SystemCatalog", "INDEXINFO").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingRow(indexinfo, "INDEX_NAME", index);
        return found;
    }

    protected boolean findClassInSystemCatalog(String classname) throws Exception
    {
        VoltTable classes = m_client.callProcedure("@SystemCatalog", "CLASSES").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingRow(classes, "CLASS_NAME", classname);
        return found;
    }

    protected boolean findFunctionInSystemCatalog(String function) throws Exception
    {
        VoltTable functions = m_client.callProcedure("@SystemCatalog", "FUNCTIONS").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingRow(functions, "FUNCTION_NAME", function);
        return found;
    }

    protected boolean findTaskInSystemCatalog(String task) throws Exception
    {
        VoltTable tasks = m_client.callProcedure("@SystemCatalog", "TASKS").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingRow(tasks, "TASK_NAME", task);
        return found;
    }

    protected boolean findProcedureInSystemCatalog(String proc) throws Exception
    {
        VoltTable procedures = m_client.callProcedure("@SystemCatalog", "PROCEDURES").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingRow(procedures, "PROCEDURE_NAME", proc);
        return found;
    }

    protected boolean verifySinglePartitionProcedure(String proc) throws Exception
    {
        VoltTable procedures = m_client.callProcedure("@SystemCatalog", "PROCEDURES").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingRow(procedures, "PROCEDURE_NAME", proc);
        boolean verified = false;
        if (found) {
            String remarks = procedures.getString("REMARKS");
            System.out.println("REMARKS: " + remarks);
            JSONObject jsObj = new JSONObject(remarks);
            verified = (jsObj.getBoolean(Constants.JSON_SINGLE_PARTITION));
        }
        return verified;
    }

    protected boolean verifyIndexUniqueness(String index, boolean expectedUniq) throws Exception
    {
        VoltTable indexinfo = m_client.callProcedure("@SystemCatalog", "INDEXINFO").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingRow(indexinfo, "INDEX_NAME", index);
        boolean verified = false;
        if (found) {
            int thisval = (int)indexinfo.getLong("NON_UNIQUE");
            int expectedVal = expectedUniq ? 0 : 1;
            if (thisval == expectedVal) {
                verified = true;
            }
        }
        return verified;
    }

    protected boolean doesColumnExist(String table, String column) throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
        return found;
    }

    protected boolean verifyTableColumnType(String table, String column, String type)
        throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
        boolean verified = false;
        if (found) {
            String thistype = columns.getString("TYPE_NAME");
            if (thistype.equalsIgnoreCase(type)) {
                verified = true;
            }
        }
        return verified;
    }

    protected boolean verifyTableColumnSize(String table, String column, int size)
        throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
        boolean verified = false;
        if (found) {
            int thissize = (int)columns.getLong("COLUMN_SIZE");
            if (thissize == size) {
                verified = true;
            }
        }
        return verified;
    }

    protected boolean verifyTableColumnDefault(String table, String column, String value)
        throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean found = VoltTableTestHelpers.moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
        boolean verified = false;
        if (found) {
            String thisdefault = columns.getString("COLUMN_DEF");
            if ((thisdefault == null && value == null) || (thisdefault.equals(value))) {
                verified = true;
            }
        }
        return verified;
    }

    // Can be misleading, assumes someone has already checked whether the column actually
    // exists.
    protected boolean isColumnNullable(String table, String column) throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean nullable = false;
        boolean found = VoltTableTestHelpers.moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
        if (found) {
            nullable = columns.getString("IS_NULLABLE").equalsIgnoreCase("YES");
        }
        return nullable;
    }

    protected boolean isColumnPartitionColumn(String table, String column) throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean partitioncol = false;
        boolean found = VoltTableTestHelpers.moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
        if (found) {
            String remarks = columns.getString("REMARKS");
            if (remarks != null) {
                partitioncol = columns.getString("REMARKS").equalsIgnoreCase("PARTITION_COLUMN");
            }
        }
        return partitioncol;
    }

    protected boolean isDRedTable(String table) throws Exception
    {
        VoltTable tableinfo = m_client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        for(int i = 0; i < tableinfo.m_rowCount; i++) {
            tableinfo.advanceToRow(i);
            String tablename = (String) tableinfo.get(2, VoltType.STRING);
            if(tablename.equals(table)) {
                try {
                    String remarks = (String)tableinfo.get(4, VoltType.STRING);
                    if (remarks == null) {
                        return false;
                    }
                    JSONObject jsEntry = new JSONObject(remarks);
                    return Boolean.valueOf(jsEntry.getString(JdbcDatabaseMetaDataGenerator.JSON_DRED_TABLE));
                } catch (JSONException e) {
                    return false;
                }

            }
        }
        return false;
    }

    protected int indexedColumnCount(String table) throws Exception
    {
        VoltTable indexinfo = m_client.callProcedure("@SystemCatalog", "INDEXINFO").getResults()[0];

        int count = 0;
        for(int i = 0; i < indexinfo.m_rowCount; i++)
        {
            indexinfo.advanceToRow(i);
            String name = (String) indexinfo.get(2, VoltType.STRING);
            if(name.equals(table))
            {
                count++;
            }
        }
        return count;
    }

    protected String getTableType(String table) throws Exception
    {
        VoltTable tableinfo = m_client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        for(int i = 0; i < tableinfo.m_rowCount; i++)
        {
            tableinfo.advanceToRow(i);
            String tablename = (String) tableinfo.get(2, VoltType.STRING);
            if(tablename.equals(table))
            {
                return (String) tableinfo.get(3, VoltType.STRING);
            }
        }
        return null;
    }

    protected VoltTable getStatWaitOnRowCount(String selector, int expected) throws Exception
    {
        // Stats are polled out of EE, so we have to poll and wait for a change
        VoltTable stats = null;
        do {
            stats = m_client.callProcedure("@Statistics", selector, 0).getResults()[0];
        } while (stats.getRowCount() != expected);
        return stats;
    }

    /**
     * Asserts that the specified column in the specified table exists.
     * @param table String: the table name in which the column is defined
     * @param column String: the column name whose existence will be tested
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    protected void assertColumnExists(String table, String column) throws Exception
    {
        assertTrue("Column "+table+"."+column+" should exist", doesColumnExist(table, column));
    }

    /**
     * Asserts that the the specified column in the specified table does NOT exist.
     * @param table String: the table name in which the column is defined
     * @param column String: the column name whose existence will be tested
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    protected void assertColumnDoesNotExist(String table, String column) throws Exception
    {
        assertFalse("Column "+table+"."+column+" should NOT exist", doesColumnExist(table, column));
    }

    /**
     * Asserts that the type of the specified column in the specified table
     * equals the expectedType.
     * @param table String: the table name in which the column is defined
     * @param column String: the column name whose type will be tested
     * @param expectedType String: the expected type of table.column
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    protected void assertColumnTypeEquals(String table, String column, String expectedType) throws Exception
    {
        assertEquals("Column "+table+"."+column+" has wrong type", expectedType,
                     getSystemCatalogColumnsString(table, column, "TYPE_NAME"));
    }

    /**
     * Asserts that the size of the specified column in the specified table
     * equals the expectedSize.
     * @param table String: the table name in which the column is defined
     * @param column String: the column name whose size will be tested
     * @param expectedSize long: the expected size of table.column
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    protected void assertColumnSizeEquals(String table, String column, long expectedSize) throws Exception
    {
        assertEquals("Column "+table+"."+column+" has wrong size", expectedSize,
                     getSystemCatalogColumnsLong(table, column, "COLUMN_SIZE"));
    }

    /**
     * Asserts that the specified column in the specified table is nullable.
     * @param table String: the table name in which the column is defined
     * @param column String: the column name whose existence will be tested
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    protected void assertColumnIsNullable(String table, String column) throws Exception
    {
        assertTrue("Column "+table+"."+column+" should be nullable", isColumnNullable(table, column));
    }

    /**
     * Asserts that the specified column in the specified table is NOT nullable,
     * that is, it has been declared as NOT NULL.
     * @param table String: the table name in which the column is defined
     * @param column String: the column name whose existence will be tested
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    protected void assertColumnIsNotNullable(String table, String column) throws Exception
    {
        assertFalse("Column "+table+"."+column+" should NOT be nullable", isColumnNullable(table, column));
    }

    /**
     * Asserts that the default value of the specified column in the specified
     * table equals the expectedDefault.
     * @param table String: the table name in which the column is defined
     * @param column String: the column name whose default value will be tested
     * @param expectedDefault String: the expected default value of table.column;
     * may be null, indicating that no default value is expected; default String
     * values must be contained in single quotes, e.g., if the expected default
     * value is 'ABC', then "'ABC'" must be passed.
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    protected void assertColumnDefaultValueEquals(String table, String column, String expectedDefault) throws Exception
    {
        assertEquals("Column "+table+"."+column+" has wrong default value", expectedDefault,
                getSystemCatalogColumnsString(table, column, "COLUMN_DEF"));
    }

    /**
     * Asserts that the ordinal position of the specified column within the
     * specified table equals the expectedPosition. Note that this uses a
     * 1-based index, so the first column has ordinal position 1, the second, 2,
     * etc.; there is no ordinal position 0.
     * @param table String: the table name in which the column is defined
     * @param column String: the column name whose ordinal position will be tested
     * @param expectedPosition long: the expected ordinal position of table.column
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    protected void assertColumnOrdinalPositionEquals(String table, String column, long expectedPosition) throws Exception
    {
        assertEquals("Column "+table+"."+column+" has wrong ordinal position", expectedPosition,
                     getSystemCatalogColumnsLong(table, column, "ORDINAL_POSITION"));
    }

    /**
     * Returns a particular String from the table returned by a call to
     * @SystemCatalog "COLUMNS". The value returned is from the row whose
     * TABLE_NAME is the specified tableName, and whose COLUMN_NAME is the
     * specified columnName; from that row, it returns the value from the
     * systemCatalogColumn column.
     * @param tableName String: the TABLE_NAME used to find the desired row
     *        from the @SystemCatalog "COLUMNS" table
     * @param columnName String: the COLUMN_NAME used to find the desired row
     *        from the @SystemCatalog "COLUMNS" table
     * @param systemCatalogColumn String: used to find the desired column from
     *        the @SystemCatalog "COLUMNS" table
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    private String getSystemCatalogColumnsString(String tableName, String columnName,
            String systemCatalogColumn) throws Exception
    {
        String result = "@SystemCatalog COLUMNS value not found, for TABLE_NAME " + tableName
                      + ", COLUMN_NAME " + columnName + ", column " + systemCatalogColumn;
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        if (VoltTableTestHelpers.moveToMatchingTupleRow(columns, "TABLE_NAME", tableName, "COLUMN_NAME", columnName)) {
            result = columns.getString(systemCatalogColumn);
        }
        return result;
    }

    /**
     * Returns a particular long value from the table returned by a call to
     * @SystemCatalog "COLUMNS". The value returned is from the row whose
     * TABLE_NAME is the specified tableName, and whose COLUMN_NAME is the
     * specified columnName; from that row, it returns the value from the
     * systemCatalogColumn column.
     * @param tableName String: the TABLE_NAME used to find the desired row
     *        from the @SystemCatalog "COLUMNS" table
     * @param columnName String: the COLUMN_NAME used to find the desired row
     *        from the @SystemCatalog "COLUMNS" table
     * @param systemCatalogColumn String: used to find the desired column from
     *        the @SystemCatalog "COLUMNS" table
     * @throws Exception if the call to @SystemCatalog "COLUMNS" does not work
     */
    private long getSystemCatalogColumnsLong(String tableName, String columnName,
            String systemCatalogColumn) throws Exception
    {
        long result = -1;
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        if (VoltTableTestHelpers.moveToMatchingTupleRow(columns, "TABLE_NAME", tableName, "COLUMN_NAME", columnName)) {
            result = columns.getLong(systemCatalogColumn);
        }
        return result;
    }

    protected static List<Object> getColumn(VoltTable tbl, int columnIndex, VoltType vt) {
        final List<Object> result = new ArrayList<>();
        while (tbl.advanceRow()) {
            result.add(tbl.get(columnIndex, vt));
        }
        return result;
    }

    protected void createSchema(
            VoltDB.Configuration config, String ddl, final int sitesPerHost, final int hostCount,
            final int replication) throws Exception {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(ddl);
        builder.setUseDDLSchema(true);
        config.m_pathToCatalog = VoltDB.Configuration.getPathToCatalogForTest("adhocddl.jar");
        boolean success = builder.compile(config.m_pathToCatalog, sitesPerHost, hostCount, replication);
        assertTrue("Schema compilation failed", success);
        config.m_pathToDeployment = VoltDB.Configuration.getPathToCatalogForTest("adhocddl.xml");
        MiscUtils.copyFile(builder.getPathToDeployment(), config.m_pathToDeployment);
    }
}
