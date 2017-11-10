/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.sysprocs;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.sysprocs.NibbleDeleteSP.ComparisonConstant;
import org.voltdb.types.TimestampType;

public class TestNibbleDelete {

    LocalCluster m_cluster = null;
    Client m_client = null;

    @Before
    public void setUp() throws IOException {
        String testSchema =
                "CREATE TABLE part (\n"
              + "    id BIGINT not null, \n"
              + "    ts TIMESTAMP not null, \n"
              + "    description VARCHAR(200), "
              + "    PRIMARY KEY (id) \n"
              + " ); \n"
              + "PARTITION TABLE part ON COLUMN id;"
              + "CREATE INDEX partindex ON part (ts);"

              + "CREATE TABLE rep (\n"
              + "    id BIGINT not null, \n"
              + "    ts TIMESTAMP not null, \n"
              + "    description VARCHAR(200), "
              + "    PRIMARY KEY (id) \n"
              + " ); \n"
              + "CREATE INDEX repindex ON rep (ts);";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(testSchema);
        builder.addPartitionInfo("part", "id");
        builder.addStmtProcedure("partcount", "select count(*) from part;");
        builder.addStmtProcedure("repcount", "select count(*) from rep;");
        m_cluster = new LocalCluster("foo.jar", 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setHasLocalServer(true);
        m_cluster.compile(builder);
        m_cluster.startUp();
    }

    @After
    public void tearDown() throws InterruptedException {
        if (m_client != null) {
            m_client.close();
        }
        if (m_cluster != null) {
            m_cluster.shutDown();
            m_cluster = null;
        }
    }

    private VoltTable createTable(int numberOfItems, int indexBase)
    {
        VoltTable partition_table =
            new VoltTable(new ColumnInfo("ID", VoltType.BIGINT),
            new ColumnInfo("TS", VoltType.TIMESTAMP),
            new ColumnInfo("DESCRIPTION", VoltType.STRING)
        );

        for (int i = indexBase; i < numberOfItems + indexBase; i++)
        {
            Object[] row = new Object[] { i,
                                          new TimestampType(i),
                                          "name_" + i };
            partition_table.addRow(row);
        }
        return partition_table;
    }


    private VoltTable[] loadTable(Client client, String tableName, boolean replicated,
                                  VoltTable table)
    {
        VoltTable[] results = null;
        try
        {
            if (replicated) {
                client.callProcedure("@LoadMultipartitionTable", tableName,
                      (byte) 0, table); // using insert
            } else {
                ArrayList<SyncCallback> callbacks = new ArrayList<>();
                VoltType columnTypes[] = new VoltType[table.getColumnCount()];
                for (int ii = 0; ii < columnTypes.length; ii++) {
                    columnTypes[ii] = table.getColumnType(ii);
                }
                while (table.advanceRow()) {
                    SyncCallback cb = new SyncCallback();
                    callbacks.add(cb);
                    Object params[] = new Object[table.getColumnCount()];
                    for (int ii = 0; ii < columnTypes.length; ii++) {
                        params[ii] = table.get(ii, columnTypes[ii]);
                    }
                    client.callProcedure(cb, tableName + ".insert", params);
                }
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("loadTable exception: " + ex.getMessage());
        }
        return results;
    }


    @Test
    public void testPartitionedTable() throws IOException, InterruptedException {

        m_client = ClientFactory.createClient();
        m_client.createConnection(m_cluster.getListenerAddress(0));

        int numberOfItems = 10000;
        VoltTable part_table = createTable(numberOfItems, 0);
        loadTable(m_client, "part", false, part_table);

        VoltTable partitionKeys = null;
        try {
            partitionKeys = m_client.callProcedure("@GetPartitionKeys", "INTEGER").getResults()[0];
        } catch (ProcCallException e1) {
            fail("Failed to get partition keys.");
        }
        TimestampType value = new TimestampType(8888);
        VoltTable table = new VoltTable(new ColumnInfo[] {
                new ColumnInfo("col1", VoltType.typeFromObject(value)),
        });
        table.addRow(value);
        long totalRowsDeleted = 0;
        while (partitionKeys.advanceRow()) {
            int partitionKey = (int)partitionKeys.getLong("PARTITION_KEY");
            try {
                ClientResponse response = m_client.callProcedure("@NibbleDeleteSP",
                        partitionKey,
                        "part",
                        "ts",
                        ComparisonConstant.LESS_THAN.ordinal(),
                        table,
                        1000);
                VoltTable result = response.getResults()[0];
                assertEquals(1, result.getRowCount());
                result.advanceRow();
                long deletedRows = result.getLong("DELETED_ROWS");
                totalRowsDeleted += deletedRows;
                assertEquals(1000, deletedRows);
                long leftoverRows = result.getLong("LEFTOVER_ROWS");
            } catch (ProcCallException e) {
                fail("Failed to run NibbleDeleteSP: " + e.getMessage());
            }
        }
        try {
            long rowCount = m_client.callProcedure("partcount").getResults()[0].asScalarLong();
            assertEquals(numberOfItems - totalRowsDeleted, rowCount);
        } catch (ProcCallException e) {
            fail("Failed to get row count from Table part");
        }
    }

    @Test
    public void testReplicatedTable() throws NoConnectionsException, IOException {

        m_client = ClientFactory.createClient();
        m_client.createConnection(m_cluster.getListenerAddress(0));

        int numberOfItems = 10000;
        VoltTable rep_table = createTable(numberOfItems, 0);
        loadTable(m_client, "rep", true, rep_table);

        TimestampType value = new TimestampType(8888);
        VoltTable table = new VoltTable(new ColumnInfo[] {
                new ColumnInfo("col1", VoltType.typeFromObject(value)),
        });
        table.addRow(value);
        long deletedRows = 0;
        try {
            ClientResponse response = m_client.callProcedure("@NibbleDeleteMP",
                    "rep",
                    "ts",
                    ComparisonConstant.LESS_THAN.ordinal(),
                    table,
                    1000);
            VoltTable result = response.getResults()[0];
            assertEquals(1, result.getRowCount());
            result.advanceRow();
            deletedRows = result.getLong("DELETED_ROWS");
            assertEquals(1000, deletedRows);
            long leftoverRows = result.getLong("LEFTOVER_ROWS");
        } catch (ProcCallException e) {
            fail("Fail to run NibbleDeleteSP: " + e.getMessage());
        }
        try {
            long rowCount = m_client.callProcedure("repcount").getResults()[0].asScalarLong();
            assertEquals(numberOfItems - deletedRows, rowCount);
        } catch (ProcCallException e) {
            fail("Failed to get row count from Table part");
        }
    }

}
