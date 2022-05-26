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

package org.voltdb.regressionsuites;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyListOf;
import static org.mockito.Mockito.doAnswer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.BackendTarget;
import org.voltdb.RealVoltDB;
import org.voltdb.TableStreamType;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.jni.ExecutionEngine;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.utils.SnapshotVerifier;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures
 */
public class TestSaveRestoreSerializationFailures extends SaveRestoreBase {

    public TestSaveRestoreSerializationFailures(String name) {
        super(name);
    }

    private VoltTable createReplicatedTable(int numberOfItems,
                                            int indexBase,
                                            Set<String> expectedText,
                                            boolean generateCSV)
    {
        VoltTable repl_table =
            new VoltTable(new ColumnInfo("RT_ID", VoltType.INTEGER),
                          new ColumnInfo("RT_NAME", VoltType.STRING),
                          new ColumnInfo("RT_INTVAL", VoltType.INTEGER),
                          new ColumnInfo("RT_FLOATVAL", VoltType.FLOAT),
                          new ColumnInfo("RT_POINTVAL", VoltType.GEOGRAPHY_POINT),
                          new ColumnInfo("RT_GEOGVAL", VoltType.GEOGRAPHY));
        char delimiter = generateCSV ? ',' : '\t';
        for (int i = indexBase; i < numberOfItems + indexBase; i++) {
            String stringVal = null;
            String escapedVal = null;
            if (expectedText != null) {
                if (generateCSV) {
                    int escapable = i % 5;
                    switch (escapable) {
                    case 0:
                        stringVal = "name_" + i;
                        escapedVal = "\"name_" + i + "\"";
                        break;
                    case 1:
                        stringVal = "na,me_" + i;
                        escapedVal = "\"na,me_" + i + "\"";
                        break;
                    case 2:
                        stringVal = "na\"me_" + i;
                        escapedVal = "\"na\"\"me_" + i + "\"";
                        break;
                    case 3:
                        stringVal = "na\rme_" + i;
                        escapedVal = "\"na\rme_" + i + "\"";
                        break;
                    case 4:
                        stringVal = "na\nme_" + i;
                        escapedVal = "\"na\nme_" + i + "\"";
                        break;
                    }
                } else {
                    stringVal = "name_" + i;
                    escapedVal = "name_" + i;
                }
            } else {
                stringVal = "name_" + i;
            }

            GeographyPointValue gpv = getGeographyPointValue(i);
            GeographyValue gv = getGeographyValue(i);
            Object[] row = new Object[] {i,
                                         stringVal,
                                         i,
                                         new Double(i),
                                         gpv,
                                         gv};
            if (expectedText != null) {
                StringBuilder sb = new StringBuilder(64);
                if (generateCSV) {
                    sb.append('"').append(i).append('"').append(delimiter);
                    sb.append(escapedVal).append(delimiter);
                    sb.append('"').append(i).append('"').append(delimiter);
                    sb.append('"').append(new Double(i).toString()).append('"').append(delimiter);
                    sb.append('"').append(gpv.toString()).append('"').append(delimiter);
                    sb.append('"').append(gpv.toString()).append('"');
                } else {
                    sb.append(i).append(delimiter);
                    sb.append(escapedVal).append(delimiter);
                    sb.append(i).append(delimiter);
                    sb.append(new Double(i).toString()).append(delimiter);
                    sb.append(gpv.toString()).append(delimiter);
                    sb.append(gpv.toString());

                }
                expectedText.add(sb.toString());
            }
            repl_table.addRow(row);
        }
        return repl_table;
    }

    private VoltTable createPartitionedTable(int numberOfItems,
                                             int indexBase)
    {
        VoltTable partition_table =
                new VoltTable(new ColumnInfo("PT_ID", VoltType.INTEGER),
                              new ColumnInfo("PT_NAME", VoltType.STRING),
                              new ColumnInfo("PT_INTVAL", VoltType.INTEGER),
                              new ColumnInfo("PT_FLOATVAL", VoltType.FLOAT),
                              new ColumnInfo("PT_POINTVAL", VoltType.GEOGRAPHY_POINT),
                              new ColumnInfo("PT_GEOGVAL", VoltType.GEOGRAPHY));

        for (int i = indexBase; i < numberOfItems + indexBase; i++)
        {
            Object[] row = new Object[] {i,
                                         "name_" + i,
                                         i,
                                         new Double(i),
                                         getGeographyPointValue(i),
                                         getGeographyValue(i)};
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
                ArrayList<SyncCallback> callbacks = new ArrayList<SyncCallback>();
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

    private void loadLargeReplicatedTable(Client client, String tableName,
            int itemsPerChunk, int numChunks) {
        loadLargeReplicatedTable(client, tableName, itemsPerChunk, numChunks, false, null);
    }

    private void loadLargeReplicatedTable(Client client, String tableName,
                                          int itemsPerChunk, int numChunks, boolean generateCSV, Set<String> expectedText)
    {
        for (int i = 0; i < numChunks; i++)
        {
            VoltTable repl_table =
                createReplicatedTable(itemsPerChunk, i * itemsPerChunk, expectedText, generateCSV);
            loadTable(client, tableName, true, repl_table);
        }
    }

    private void loadLargePartitionedTable(Client client, String tableName,
                                          int itemsPerChunk, int numChunks)
    {
        for (int i = 0; i < numChunks; i++)
        {
            VoltTable part_table =
                createPartitionedTable(itemsPerChunk, i * itemsPerChunk);
            loadTable(client, tableName, false, part_table);
        }
    }

    private VoltTable[] saveTablesWithDefaultOptions(Client client, String nonce)
    {
        return saveTables(client, TMPDIR, nonce, true, false);
    }

    private VoltTable[] saveTables(Client client, String dir, String nonce, boolean block, boolean csv)
    {
        VoltTable[] results = null;
        try
        {
            // For complete coverage test with JSON for CSV saves and legacy args otherwise.
            if (csv) {
                JSONObject jsObj = new JSONObject();
                try {
                    jsObj.put(SnapshotUtil.JSON_URIPATH, String.format("file://%s", dir));
                    jsObj.put(SnapshotUtil.JSON_NONCE, nonce);
                    jsObj.put(SnapshotUtil.JSON_BLOCK, block);
                    jsObj.put(SnapshotUtil.JSON_FORMAT, "csv");
                } catch (JSONException e) {
                    fail("JSON exception" + e.getMessage());
                }
                results = client.callProcedure("@SnapshotSave", jsObj.toString()).getResults();
            }
            else {
                results = client.callProcedure("@SnapshotSave", dir, nonce, (byte)(block ? 1 : 0))
                                    .getResults();
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        return results;
    }

    private void checkTable(Client client, String tableName, String orderByCol,
                            int expectedRows) throws Exception
    {
        if (expectedRows > 200000)
        {
            System.out.println("Table too large to retrieve with select *");
            System.out.println("Skipping integrity check");
        }

        VoltTable result = client.callProcedure("SaveRestoreSelect", tableName).getResults()[0];

        final int rowCount = result.getRowCount();
        assertEquals(expectedRows, rowCount);

        int i = 0;
        while (result.advanceRow())
        {
            assertEquals(i, result.getLong(0));
            assertEquals("name_" + i, result.getString(1));
            assertEquals(i, result.getLong(2));
            assertEquals(new Double(i), result.getDouble(3));
            ++i;
        }
    }

    private void validateSnapshot(boolean expectSuccess, String nonce) {
        validateSnapshot(expectSuccess, false, nonce);
    }

    private boolean validateSnapshot(boolean expectSuccess,
            boolean onlyReportSuccess, String nonce) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream original = System.out;
        try {
            System.setOut(ps);
            List<String> directories = new ArrayList<String>();
            directories.add(TMPDIR);
            Set<String> snapshotNames = new HashSet<String>();
            snapshotNames.add(nonce);
            SnapshotVerifier.verifySnapshots(directories, snapshotNames);
            ps.flush();
            String reportString = baos.toString("UTF-8");
            boolean success = false;
            if (expectSuccess) {
                success = reportString.startsWith("Snapshot valid\n");
            } else {
                success = reportString.startsWith("Snapshot corrupted\n");
            }
            if (!onlyReportSuccess) {
                if (!success) {
                    fail(reportString);
                }
            }
            return success;
        } catch (UnsupportedEncodingException e) {}
          finally {
            System.setOut(original);
        }
          return false;
    }

    private void checkStatistics(VoltTable[] results, Client client, long maxTupleCount) throws Exception{
        long tend = System.currentTimeMillis() + 60000;
        long tupleCount = 0;
        while (tupleCount < maxTupleCount) {
            tupleCount = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
                {
                    tupleCount += results[0].getLong("TUPLE_COUNT");
                }
            }
            assertTrue("Time out when checking statistics", System.currentTimeMillis() < tend);
        }
        assertEquals(maxTupleCount, tupleCount);
    }

    public void testSaveAndRestorePartitionedTable()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testSaveAndRestorePartitionedTable");
        int num_partitioned_items_per_chunk = 120; // divisible by 3
        int num_partitioned_chunks = 10;
        int num_replicated_items_per_chunk = 200;
        int num_replicated_chunks = 10;
        Client client = getClient();
        long cluster1CreateTime = 0;
        long cluster1InstanceId = 0;
        long cluster2CreateTime = 0;
        long cluster2InstanceId = 0;

        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);
        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks);

        VoltTable[] results = null;

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

        RealVoltDB rvdb = (RealVoltDB)VoltDB.instance();
        ExecutionEngine ee0 = rvdb.debugGetSpiedEE(0);
        doAnswer(new Answer<Pair<Long, int[]>>() {
        @Override
            public Pair<Long, int[]> answer(InvocationOnMock invocation) throws Throwable {
                @SuppressWarnings("unchecked")
                Pair<Long,int[]> realResult = (Pair<Long, int[]>) invocation.callRealMethod();
                if (realResult.getFirst() <= 0) {
                    // There is no more data to stream. We can now safely insert an error.
                    return new Pair<Long, int[]>(-1l, null);
                }
                else {
                    return realResult;
                }
            }
        }).when(ee0).tableStreamSerializeMore(anyInt(), any(TableStreamType.class), anyListOf(BBContainer.class));
        VoltTable orig_mem = null;
        try
        {
            orig_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
            System.out.println("STATS: " + orig_mem.toString());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("Statistics exception: " + ex.getMessage());
        }

        results = saveTablesWithDefaultOptions(client, "first");

        validateSnapshot(false, "first");

        while (results[0].advanceRow()) {
            if (!results[0].getString("RESULT").equals("SUCCESS")) {
                System.out.println(results[0].getString("ERR_MSG"));
            }
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        try
        {
            checkSnapshotStatus(client, TMPDIR, "first", null, "FAILURE", 1 /*first*/);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }
        Mockito.reset(ee0);

        results = saveTablesWithDefaultOptions(client, "second");

        validateSnapshot(true, "second");

        while (results[0].advanceRow()) {
            if (!results[0].getString("RESULT").equals("SUCCESS")) {
                System.out.println(results[0].getString("ERR_MSG"));
            }
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        try
        {
            checkSnapshotStatus(client, TMPDIR, "second", null, "SUCCESS", 2 /* first and second */);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }


        cluster1CreateTime = VoltDB.instance().getClusterCreateTime();
        cluster1InstanceId = VoltDB.instance().getHostMessenger().getInstanceId().getTimestamp();
        assertTrue(cluster1CreateTime == cluster1InstanceId);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();
        cluster2CreateTime = VoltDB.instance().getClusterCreateTime();
        cluster2InstanceId = VoltDB.instance().getHostMessenger().getInstanceId().getTimestamp();
        assertTrue(cluster2CreateTime == cluster2InstanceId);
        assertFalse(cluster1CreateTime == cluster2CreateTime);

        client = getClient();

        boolean threwException = false;
        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           "first").getResults();

            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }
        }
        catch (Exception ex)
        {
            threwException = true;
        }
        assertTrue(threwException);

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           "second").getResults();

            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        // Make sure the cluster time remains unchanged after a restore
        assertTrue(VoltDB.instance().getClusterCreateTime() == cluster2CreateTime);
        checkTable(client, "PARTITION_TESTER", "PT_ID",
                num_partitioned_items_per_chunk * num_partitioned_chunks);
        checkStatistics(results, client, num_partitioned_items_per_chunk * num_partitioned_chunks);

        // Test a duplicated restore
        try{
            results = doDupRestore(client, "second");
        } catch (Exception ex) {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }
        // Make sure the cluster time has been reset with the restore
        assertTrue(VoltDB.instance().getClusterCreateTime() == cluster1CreateTime);
        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);
        checkStatistics(results, client, num_partitioned_items_per_chunk * num_partitioned_chunks);

        deleteTestFiles("first");
        deleteTestFiles("second");
    }

    private VoltTable[] doDupRestore(Client client, String nonce) throws Exception {
        return doDupRestore(client, VoltDB.instance().getHostMessenger().getZK(), nonce);
    }

    private VoltTable[] doDupRestore(Client client, ZooKeeper zk, String nonce) throws Exception {
        VoltTable[] results;

        // Now check that doing a restore and logging duplicates works.

        JSONObject jsObj = new JSONObject();
        jsObj.put(SnapshotUtil.JSON_NONCE, nonce);
        jsObj.put(SnapshotUtil.JSON_PATH, TMPDIR);
        // Set isRecover = true so we won't get errors in restore result.
        jsObj.put(SnapshotUtil.JSON_IS_RECOVER, true);

        results = client.callProcedure("@SnapshotRestore", jsObj.toString()).getResults();

        while (results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                fail(results[0].getString("ERR_MSG"));
            }
        }
        return results;
    }

    public static class SnapshotResult {
        Long hostID;
        String table;
        String path;
        String filename;
        String nonce;
        Long txnID;
        Long endTime;
        String result;
    }

    public static SnapshotResult[] checkSnapshotStatus(Client client, String path, String nonce, Integer endTime,
            String result, Integer rowCount)
            throws NoConnectionsException, IOException, ProcCallException {

        // Execute @SnapshotSummary to get raw results.
        VoltTable statusResults[] = client.callProcedure("@Statistics", "SnapshotSummary", 0).getResults();
        assertNotNull(statusResults);
        assertEquals( 1, statusResults.length);

        // Validate row count if requested.
        Integer resultRowCount = statusResults[0].getRowCount();
        if (rowCount != null) {
            assertEquals(rowCount, resultRowCount);
        }

        // Populate status data object list.
        SnapshotResult[] results = new SnapshotResult[resultRowCount];
        for (int i = 0; i < resultRowCount; i++) {
            assertTrue(statusResults[0].advanceRow());
            results[i] = new SnapshotResult();
            results[i].nonce = statusResults[0].getString("NONCE");
            results[i].txnID = statusResults[0].getLong("TXNID");
            results[i].path = statusResults[0].getString("PATH");
            results[i].endTime = statusResults[0].getLong("END_TIME");
            results[i].result = statusResults[0].getString("RESULT");

            if (nonce.equals(results[i].nonce)) {
                // Perform requested validation.
                if (path != null) {
                    assertEquals(path, results[i].path);
                }
                if (endTime != null) {
                    assertEquals(endTime, results[i].endTime);
                }
                if (result != null) {
                    assertEquals(result, results[i].result);
                }
            }
        }

        return results;
    }

    /**
     * Build a list of the tests to be run. Use the regression suite
     * helpers to allow multiple back ends.
     * JUnit magic that uses the regression suite helper classes.
     */
    static public junit.framework.Test suite() {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestSaveRestoreSerializationFailures.class);

        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();

        config =
            new CatalogChangeSingleProcessServer(JAR_NAME, 3,
                                                 BackendTarget.NATIVE_EE_SPY_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
