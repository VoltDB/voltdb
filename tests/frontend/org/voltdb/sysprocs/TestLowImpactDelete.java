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

import java.io.IOException;
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
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.types.TimestampType;

import junit.framework.TestCase;

public class TestLowImpactDelete extends TestCase {

    LocalCluster m_cluster = null;
    Client m_client = null;
    static int SPH = 2;
    static int HOSTCOUNT = 3;
    static int KFACTOR = 1;
    static int MAX_FREQUENCEY = 1;
    static int INTERVAL = 1;
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
              + "CREATE INDEX partindex ON part (ts);\n"

              + "CREATE TABLE ttl (\n"
              + "    id BIGINT not null, \n"
              + "    ts TIMESTAMP not null, "
              + "    PRIMARY KEY (id) \n"
              + " ) USING TTL 30 SECONDS ON COLUMN TS BATCH_SIZE 10 MAX_FREQUENCY 3; \n"
              + "PARTITION TABLE ttl ON COLUMN id;"
              + "CREATE INDEX ttlindex ON ttl (ts);"

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
        builder.addStmtProcedure("ttlcount", "select count(*) from ttl;");
        builder.setUseDDLSchema(true);
        m_cluster = new LocalCluster("fooxx.jar", SPH, HOSTCOUNT, KFACTOR, BackendTarget.NATIVE_EE_JNI);
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


    private void loadTable(Client client, String tableName, boolean replicated,
                                  VoltTable table) {
        try{
            if (replicated) {
                client.callProcedure("@LoadMultipartitionTable", tableName,
                      (byte) 0, table); // using insert
            } else {
                VoltType columnTypes[] = new VoltType[table.getColumnCount()];
                for (int ii = 0; ii < columnTypes.length; ii++) {
                    columnTypes[ii] = table.getColumnType(ii);
                }
                while (table.advanceRow()) {
                    Object params[] = new Object[table.getColumnCount()];
                    for (int ii = 0; ii < columnTypes.length; ii++) {
                        params[ii] = table.get(ii, columnTypes[ii]);
                    }
                    client.callProcedure(tableName + ".insert", params);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("loadTable exception: " + ex.getMessage());
        }
    }

    // make sure LowImpactDelete complains if the table is missing
    // also doubles as a smoke test to make sure it can be called at all
    public void testMissingTable() throws Exception {
        // fail on missing table
        try {
            m_client.callProcedure("@LowImpactDeleteNT", "notable", "nocolumn", "75", "<", 1000, 2000, MAX_FREQUENCEY, INTERVAL);
            fail();
        }
        catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Table \"notable\" not"));
        }

        // add a table
        m_client.callProcedure("@AdHoc", "create table foo (a integer, b varchar(255), c integer, primary key (a));");

        // fail on missing column
        try {
            m_client.callProcedure("@LowImpactDeleteNT", "foo", "nocolumn", "75", "<", 1000, 2000, MAX_FREQUENCEY, INTERVAL);
            fail();
        }
        catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Column \"nocolumn\" not"));
        }

        // fail on improper type
        try {
            m_client.callProcedure("@LowImpactDeleteNT", "foo", "a", "stringdata", "<", 1000, 2000, MAX_FREQUENCEY, INTERVAL);
            fail();
        }
        catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Unable to convert"));
        }
    }

    @Test
    public void testLowImpactDelete() throws NoConnectionsException, IOException, ProcCallException {
        System.out.println("testLowImpactDelete");

        long numberOfItems = 10000;
        VoltTable inputTable = createTable(numberOfItems, 0, 0, 0, 0);
        loadTable(m_client, "part", false, inputTable);
        loadTable(m_client, "rep", true, inputTable);
        ClientResponse response = m_client.callProcedure("@LowImpactDeleteNT", "part", "ts", "9000", "<", 500, 1000 * 1000, MAX_FREQUENCEY, INTERVAL);
        VoltTable result = response.getResults()[0];
        assertEquals(1, result.getRowCount());
        result.advanceRow();
        long deleted = result.getLong("ROWS_DELETED");
        assertTrue (deleted == 1500);

        response = m_client.callProcedure("@LowImpactDeleteNT", "rep", "ts", "9000", "<", 500, 1000 * 1000, MAX_FREQUENCEY, INTERVAL);
        result = response.getResults()[0];
        assertEquals(1, result.getRowCount());
        result.advanceRow();
        deleted = result.getLong("ROWS_DELETED");
        assertTrue (deleted == 500);
    }

    @Test
    public void testLongRunningNibbleDelete() throws InterruptedException {
        System.out.println("testLongRunningNibbleDelete");

        Thread t1 = new Thread(() -> {
            long numberOfItems = 10000;
            int duration = 60 * 1000;
            long start = System.currentTimeMillis();
            long now = start;
            long end = start + duration;
            VoltTable inputTable;
            int offset = 0;
            while (now < end) {
                inputTable = createTable(numberOfItems, offset, 0, 0, 0);
                offset += numberOfItems;
                loadTable(m_client, "part", false, inputTable);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                now = System.currentTimeMillis();
            }
        });

        Thread t2 = new Thread(()-> {
            long start = System.currentTimeMillis();
            int duration = 100 * 1000;
            long now = start;
            long end = start + duration;
            while (now < end) {
                ClientResponse response = null;
                try {
                    response = m_client.callProcedure("@LowImpactDeleteNT", "part", "ts", "1000000000", "<", 10000, 1000 * 1000, 4, INTERVAL);
                } catch (NoConnectionsException e) {
                    e.printStackTrace();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ProcCallException e) {
                    e.printStackTrace();
                }

                VoltTable result = response.getResults()[0];
                assertEquals(1, result.getRowCount());
                result.advanceRow();
                long rowsLeft = result.getLong("ROWS_LEFT");
                assertTrue (rowsLeft == 0);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                now = System.currentTimeMillis();
            }
        });
        t1.start();
        Thread.sleep(3000);
        t2.start();

        t1.join();
        t2.join();
        try {
            long rowCount = m_client.callProcedure("partcount").getResults()[0].asScalarLong();
            assertEquals(0, rowCount);
        } catch (Exception e) {
            fail("Failed to get row count from Table part");
        }
    }

    @Test
    public void testTimeToLive() throws InterruptedException {
        //load 500 rows
        for (int i = 0; i < 500; i++) {
            try {
                m_client.callProcedure("@AdHoc", "INSERT INTO TTL VALUES(" + i + ",CURRENT_TIMESTAMP())");
            } catch (IOException | ProcCallException e) {
                fail("fail to insert data for TTL testing.");
            }
        }
        //allow TTL to work, the inserted rows should be deleted after 10 seconds
        try {
            Thread.sleep(90*1000);
            VoltTable vt = m_client.callProcedure("@Statistics", "TTL").getResults()[0];
            System.out.println(vt.toFormattedString());
            vt = m_client.callProcedure("@AdHoc", "select count(*) from TTL").getResults()[0];
            assertEquals(0, vt.asScalarLong());

            for (int i = 500; i < 1000; i++) {
                try {
                    m_client.callProcedure("@AdHoc", "INSERT INTO TTL VALUES(" + i + ",CURRENT_TIMESTAMP())");
                } catch (IOException | ProcCallException e) {
                    fail("fail to insert data for TTL testing.");
                }
            }
            Thread.sleep(90*1000);
            vt = m_client.callProcedure("@Statistics", "TTL").getResults()[0];
            System.out.println(vt.toFormattedString());
            vt = m_client.callProcedure("@AdHoc", "select count(*) from TTL").getResults()[0];
            assertEquals(0, vt.asScalarLong());
        } catch (Exception e) {
            fail("Failed to get row count from Table ttl:" + e.getMessage());
        }
    }
}
