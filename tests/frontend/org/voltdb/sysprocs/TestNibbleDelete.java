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

package org.voltdb.sysprocs;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.utils.Pair;
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
import org.voltdb.types.TimestampType;

public class TestNibbleDelete {

    LocalCluster m_cluster = null;
    Client m_client = null;
    static int SPH = 2;
    static int HOSTCOUNT = 3;
    static int KFACTOR = 1;

    @Before
    public void setUp() throws IOException {
        String testSchema =
                "CREATE TABLE part (\n"
              + "    id BIGINT not null, \n"
              + "    ts TIMESTAMP, \n"
              + "    description VARCHAR(200), "
              + "    PRIMARY KEY (id) \n"
              + " ); \n"
              + "PARTITION TABLE part ON COLUMN id;"
              + "CREATE INDEX partindex ON part (ts);"

              + "CREATE TABLE rep (\n"
              + "    id BIGINT not null, \n"
              + "    ts TIMESTAMP, \n"
              + "    description VARCHAR(200), "
              + "    PRIMARY KEY (id) \n"
              + " ); \n"
              + "CREATE INDEX repindex ON rep (ts);";

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(testSchema);
        builder.addPartitionInfo("part", "id");
        builder.addStmtProcedure("partcount", "select count(*) from part;");
        builder.addStmtProcedure("repcount", "select count(*) from rep;");
        m_cluster = new LocalCluster("foo.jar", SPH, HOSTCOUNT, KFACTOR, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setHasLocalServer(false);
        m_cluster.compile(builder);
        m_cluster.startUp();

        m_client = ClientFactory.createClient();
        m_client.createConnection(m_cluster.getListenerAddress(0));
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

    /**
     * nullRatio, zeroRatio and dupValRatio can't be set to non-zero at the same time. They are mutual exclusive.
     */
    private VoltTable createTable(long numberOfItems, int indexBase,
            double nullRatio, double zeroRatio, double dupValRatio)
    {
        assert ((nullRatio != 0 && zeroRatio == 0 && dupValRatio == 0) ||
                (nullRatio == 0 && zeroRatio != 0 && dupValRatio == 0) ||
                (nullRatio == 0 && zeroRatio == 0 && dupValRatio != 0) ||
                (nullRatio == 0 && zeroRatio == 0 && dupValRatio == 0));

        VoltTable table =
                new VoltTable(new ColumnInfo("ID", VoltType.BIGINT),
                new ColumnInfo("TS", VoltType.TIMESTAMP),
                new ColumnInfo("DESCRIPTION", VoltType.STRING)
            );

        for (int i = indexBase; i < numberOfItems + indexBase; i++)
        {
            TimestampType ts;
            if (nullRatio != 0) {
                int numberOfNulls = (int)(numberOfItems * nullRatio);
                if (i < numberOfNulls) {
                    ts = null;
                } else {
                    ts = new TimestampType(i);
                }
            } else if (zeroRatio != 0) {
                int numberOfZeros = (int)(numberOfItems * zeroRatio);
                if (i < numberOfZeros) {
                    ts = new TimestampType(0);
                } else {
                    ts = new TimestampType(i);
                }
            } else if (dupValRatio != 0) {
                int numberOfSameValues = (int)(numberOfItems * dupValRatio);
                int val = (i / numberOfSameValues) * numberOfSameValues;
                ts = new TimestampType(val);
            } else {
                ts = new TimestampType(i);
            }

            Object[] row = new Object[] { i,
                                          ts,
                                          "name_" + i };
            table.addRow(row);
        }
        return table;
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

    private Pair<Long, Long> nibbleDeletePartitioned(String opStr,
            int ts, long numberOfItems)
            throws NoConnectionsException, IOException
    {
        TimestampType value = new TimestampType(ts);
        VoltTable parameter = new VoltTable(new ColumnInfo[] {
                new ColumnInfo("col1", VoltType.typeFromObject(value)),
        });
        parameter.addRow(value);

        VoltTable partitionKeys = null;
        try {
            partitionKeys = m_client.callProcedure("@GetPartitionKeys", "INTEGER").getResults()[0];
        } catch (ProcCallException e1) {
            fail("Failed to get partition keys.");
        }

        long start = System.currentTimeMillis();

        long deleted = 0;
        long toBeDeleted = 0;
        while (partitionKeys.advanceRow()) {
            int partitionKey = (int)partitionKeys.getLong("PARTITION_KEY");
            try {
                ClientResponse response = m_client.callProcedure("@NibbleDeleteSP",
                        partitionKey,
                        "part",
                        "ts",
                        opStr,
                        parameter,
                        500);
                VoltTable result = response.getResults()[0];
                assertEquals(1, result.getRowCount());
                result.advanceRow();
                long deletedRows = result.getLong("DELETED_ROWS");
                deleted += deletedRows;
                long leftoverRows = result.getLong("LEFT_ROWS");
                toBeDeleted += leftoverRows;
            } catch (ProcCallException e) {
                fail("Failed to run NibbleDeleteSP: " + e.getMessage());
            }
        }
        System.out.println("Delete " + deleted +
                " rows on partitioned table, " + toBeDeleted + " rows pending, cost " +
                (System.currentTimeMillis() - start) + " ms");
        try {
            long rowCount = m_client.callProcedure("partcount").getResults()[0].asScalarLong();
            assertEquals(numberOfItems - deleted, rowCount);
        } catch (ProcCallException e) {
            fail("Failed to get row count from Table part");
        }
        return new Pair<>(deleted, toBeDeleted);
    }

    private Pair<Long, Long> nibbleDeleteReplicated(String opStr, int ts, long numberOfItems)
            throws NoConnectionsException, IOException
    {
        TimestampType value = new TimestampType(ts);
        VoltTable table = new VoltTable(new ColumnInfo[] {
                new ColumnInfo("col1", VoltType.typeFromObject(value)),
        });
        table.addRow(value);
        long deleted = 0;
        long toBeDeleted = 0;
        long start = System.currentTimeMillis();
        try {
            ClientResponse response = m_client.callProcedure("@NibbleDeleteMP",
                    "rep",
                    "ts",
                    opStr,
                    table,
                    500);
            VoltTable result = response.getResults()[0];
            assertEquals(1, result.getRowCount());
            result.advanceRow();
            deleted = result.getLong("DELETED_ROWS");
            long leftoverRows = result.getLong("LEFT_ROWS");
            toBeDeleted += leftoverRows;
        } catch (ProcCallException e) {
            fail("Fail to run NibbleDeleteMP: " + e.getMessage());
        }
        System.out.println("Delete " + deleted +
                " rows on replicated table, " + toBeDeleted + " rows pending, cost " +
                (System.currentTimeMillis() - start) + " ms");
        try {
            long rowCount = m_client.callProcedure("repcount").getResults()[0].asScalarLong();
            assertEquals(numberOfItems - deleted, rowCount);
        } catch (ProcCallException e) {
            fail("Failed to get row count from Table part");
        }
        return new Pair<>(deleted, toBeDeleted);
    }

    private void runTester(long numberOfItems, String opStr, int ts)
            throws NoConnectionsException, IOException
    {
        Pair<Long, Long> pair = new Pair<>(0l, Long.MAX_VALUE); // <deleted, toBeDeleted>
        int loop = 0;
        long existingRows = numberOfItems;
        while (pair.getSecond() > 0) {
            pair = nibbleDeletePartitioned(opStr, ts, existingRows);
            existingRows -= pair.getFirst();
            if (++loop > 100) {
                fail("Make no progress on delete, something wrong happens in @NibbleDeleteSP or @NibbleDeleteMP");
            }
        }

        pair = new Pair<>(0l, Long.MAX_VALUE); // <deleted, toBeDeleted>
        loop = 0;
        existingRows = numberOfItems;
        while (pair.getSecond() > 0) {
            pair = nibbleDeleteReplicated(opStr, ts, existingRows);
            existingRows -= pair.getFirst();
            if (++loop > 100) {
                fail("Make no progress on delete, something wrong happens in @NibbleDeleteSP or @NibbleDeleteMP");
            }
        }
    }

    /**
     * Delete on column with non-unique index, data in uniform distribution.
     */
    @Test
    public void testBasic() throws NoConnectionsException, IOException
    {
        System.out.println("testBasic");

        long numberOfItems = 10000;
        VoltTable inputTable = createTable(numberOfItems, 0, 0, 0, 0);
        loadTable(m_client, "part", false, inputTable);
        loadTable(m_client, "rep", true, inputTable);
        runTester(numberOfItems, "<=", 9000);
    }

    /**
     * Delete table on column with non-unique index, data has lots of NULLs.
     */
    @Test
    public void testDeleteTableHasLotsNulls() throws NoConnectionsException, IOException
    {
        System.out.println("testDeleteTableHasLotsNulls");

        int numberOfItems = 10000;
        VoltTable table = createTable(numberOfItems, 0, 0.5f, 0, 0);
        loadTable(m_client, "part", false, table);
        loadTable(m_client, "rep", true, table);
        runTester(numberOfItems, "<=", 6000);
    }

    @Test
    public void testDeleteTableHasLotsZeros() throws NoConnectionsException, IOException
    {
        System.out.println("testDeleteTableHasLotsZeros");

        int numberOfItems = 10000;
        VoltTable table = createTable(numberOfItems, 0, 0, 0.5f, 0);
        loadTable(m_client, "part", false, table);
        loadTable(m_client, "rep", true, table);
        runTester(numberOfItems, "<=", 6000);
    }

    @Test
    public void testDeleteTableHasLotsDuplicates() throws NoConnectionsException, IOException
    {
        System.out.println("testDeleteTableHasLotsDuplicates");

        int numberOfItems = 10000;
        VoltTable table = createTable(numberOfItems, 0, 0, 0, 0.3f);
        loadTable(m_client, "part", false, table);
        loadTable(m_client, "rep", true, table);
        runTester(numberOfItems, ">=", 0);
    }
}
