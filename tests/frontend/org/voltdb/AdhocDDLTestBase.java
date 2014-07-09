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

package org.voltdb;

import junit.framework.TestCase;

import org.json_voltpatches.JSONObject;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.common.Constants;

public class AdhocDDLTestBase extends TestCase {

    // Still need to write these tests somewhere --izzy
    //adHocQuery = "CREATE INDEX IDX_PROJ_PNAME ON PROJ(PNAME);";
    //adHocQuery = "CREATE PROCEDURE AS SELECT 1 FROM PROJ;";
    //adHocQuery = "CREATE PROCEDURE FROM CLASS bar.Foo;";

    protected ServerThread m_localServer;
    protected Client m_client;

    protected void startSystem(VoltDB.Configuration config) throws Exception
    {
        m_localServer = new ServerThread(config);
        m_localServer.start();
        m_localServer.waitForInitialization();
        m_client = ClientFactory.createClient();
        m_client.createConnection("localhost");
    }

    protected void teardownSystem() throws Exception
    {
        if (m_client != null) { m_client.close(); }
        m_client = null;

        if (m_localServer != null) {
            m_localServer.shutdown();
            m_localServer.join();
        }
        m_localServer = null;
    }

    protected boolean moveToMatchingRow(VoltTable table, String columnName,
                              String columnValue)
    {
        boolean found = false;
        table.resetRowPosition();
        while (table.advanceRow())
        {
            if (((String)table.get(columnName, VoltType.STRING)).
                    equalsIgnoreCase(columnValue.toUpperCase()))
            {
                found = true;
                break;
            }
        }
        return found;
    }

    protected boolean moveToMatchingTupleRow(VoltTable table, String column1Name,
                              String column1Value, String column2Name,
                              String column2Value)
    {
        boolean found = false;
        table.resetRowPosition();
        while (table.advanceRow())
        {
            if (((String)table.get(column1Name, VoltType.STRING)).
                    equalsIgnoreCase(column1Value.toUpperCase()) &&
                    ((String)table.get(column2Name, VoltType.STRING)).
                    equalsIgnoreCase(column2Value.toUpperCase()))
            {
                found = true;
                break;
            }
        }
        return found;
    }

    protected boolean findTableInSystemCatalogResults(String table) throws Exception
    {
        VoltTable tables = m_client.callProcedure("@SystemCatalog", "TABLES").getResults()[0];
        boolean found = moveToMatchingRow(tables, "TABLE_NAME", table);
        return found;
    }

    protected boolean findIndexInSystemCatalogResults(String index) throws Exception
    {
        VoltTable indexinfo = m_client.callProcedure("@SystemCatalog", "INDEXINFO").getResults()[0];
        boolean found = moveToMatchingRow(indexinfo, "INDEX_NAME", index);
        return found;
    }

    protected boolean findProcedureInSystemCatalog(String proc) throws Exception
    {
        VoltTable procedures = m_client.callProcedure("@SystemCatalog", "PROCEDURES").getResults()[0];
        boolean found = moveToMatchingRow(procedures, "PROCEDURE_NAME", proc);
        return found;
    }

    protected boolean verifySinglePartitionProcedure(String proc) throws Exception
    {
        VoltTable procedures = m_client.callProcedure("@SystemCatalog", "PROCEDURES").getResults()[0];
        boolean found = moveToMatchingRow(procedures, "PROCEDURE_NAME", proc);
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
        boolean found = moveToMatchingRow(indexinfo, "INDEX_NAME", index);
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
        boolean found = moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
        return found;
    }

    protected boolean verifyTableColumnType(String table, String column, String type)
        throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean found = moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
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
        boolean found = moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
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
        boolean found = moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
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
        boolean found = moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
        if (found) {
            nullable = columns.getString("IS_NULLABLE").equalsIgnoreCase("YES");
        }
        return nullable;
    }

    protected boolean isColumnPartitionColumn(String table, String column) throws Exception
    {
        VoltTable columns = m_client.callProcedure("@SystemCatalog", "COLUMNS").getResults()[0];
        boolean partitioncol = false;
        boolean found = moveToMatchingTupleRow(columns, "TABLE_NAME", table, "COLUMN_NAME", column);
        if (found) {
            String remarks = columns.getString("REMARKS");
            if (remarks != null) {
                partitioncol = columns.getString("REMARKS").equalsIgnoreCase("PARTITION_COLUMN");
            }
        }
        return partitioncol;
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
}
