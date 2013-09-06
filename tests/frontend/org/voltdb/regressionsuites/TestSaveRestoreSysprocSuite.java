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

package org.voltdb.regressionsuites;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.zip.GZIPInputStream;

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.zk.ZKUtil;
import org.voltdb.BackendTarget;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.VoltZK;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.sysprocs.SnapshotRestore;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.utils.SnapshotConverter;
import org.voltdb.utils.SnapshotVerifier;
import org.voltdb.utils.VoltFile;
import org.voltdb_testprocs.regressionsuites.SaveRestoreBase;
import org.voltdb_testprocs.regressionsuites.saverestore.CatalogChangeSingleProcessServer;
import org.voltdb_testprocs.regressionsuites.saverestore.SaveRestoreTestProjectBuilder;

import com.google.common.collect.ImmutableSet;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures
 */
public class TestSaveRestoreSysprocSuite extends SaveRestoreBase {
    private final static VoltLogger LOG = new VoltLogger("CONSOLE");

    public TestSaveRestoreSysprocSuite(String name) {
        super(name);
    }

    private void corruptTestFiles(java.util.Random r) throws Exception
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String file)
            {
                // NOTE: at some point we will be prepared to corrupt
                // the catalog.  At that point, get rid of the
                // .jar exclusion.
                return file.startsWith(TESTNONCE) && !file.endsWith(".jar");
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        int tmpIndex = r.nextInt(tmp_files.length);
        byte corruptValue[] = new byte[1];
        r.nextBytes(corruptValue);
        java.io.RandomAccessFile raf = new java.io.RandomAccessFile( tmp_files[tmpIndex], "rw");
        int corruptPosition = r.nextInt((int)raf.length());
        raf.seek(corruptPosition);
        byte currentValue = raf.readByte();
        while (currentValue == corruptValue[0]) {
            r.nextBytes(corruptValue);
        }
        System.out.println("Corrupting file " + tmp_files[tmpIndex].getName() +
                " at byte " + corruptPosition + " with value " + corruptValue[0]);
        raf.seek(corruptPosition);
        raf.write(corruptValue);
        raf.close();
    }

    private VoltTable createReplicatedTable(int numberOfItems,
            int indexBase,
            Set<String> expectedText) {
        return createReplicatedTable(numberOfItems, indexBase, expectedText, false);
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
                          new ColumnInfo("RT_FLOATVAL", VoltType.FLOAT));
        char delimeter = generateCSV ? ',' : '\t';
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

            Object[] row = new Object[] {i,
                                         stringVal,
                                         i,
                                         new Double(i)};
            if (expectedText != null) {
                StringBuilder sb = new StringBuilder(64);
                if (generateCSV) {
                    sb.append('"').append(i).append('"').append(delimeter).append(escapedVal).append(delimeter);
                    sb.append('"').append(i).append('"').append(delimeter);
                    sb.append('"').append(new Double(i).toString()).append('"');
                } else {
                    sb.append(i).append(delimeter).append(escapedVal).append(delimeter);
                    sb.append(i).append(delimeter);
                    sb.append(new Double(i).toString());
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
                              new ColumnInfo("PT_FLOATVAL", VoltType.FLOAT));

        for (int i = indexBase; i < numberOfItems + indexBase; i++)
        {
            Object[] row = new Object[] {i,
                                         "name_" + i,
                                         i,
                                         new Double(i)};
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
                            table);
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

    private VoltTable[] saveTablesWithDefaultOptions(Client client)
    {
        return saveTables(client, TMPDIR, TESTNONCE, true, false);
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
                    jsObj.put("uripath", String.format("file://%s", dir));
                    jsObj.put("nonce", nonce);
                    jsObj.put("block", block);
                    jsObj.put("format", "csv");
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

    private void validateSnapshot(boolean expectSuccess) {
        validateSnapshot(expectSuccess, false,  TESTNONCE);
    }

    private boolean validateSnapshot(boolean expectSuccess, boolean onlyReportSuccess, String nonce) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream original = System.out;
        try {
            System.setOut(ps);
            String args[] = new String[] {
                    nonce,
                    "--dir",
                    TMPDIR
            };
            SnapshotVerifier.main(args);
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

    /*
     * Test that IV2 transaction ids for inactive partitions are propagated during snapshot restore. This
     * test is sufficient to test restore as well because the transactions ids are published
     * to ZK and retrieved by the snapshot daemon for each @SnapshotSave invocation.
     */
    @SuppressWarnings("unchecked")
    public void testPropagateIV2TransactionIds()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        if (!VoltDB.instance().isIV2Enabled()) return;


        System.out.println("Starting testPropagateIV2TransactionIds");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();
        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
        // make a TPCC warehouse table
        VoltTable partition_table =
            createPartitionedTable(num_partitioned_items, 0);

        loadTable(client, "REPLICATED_TESTER", true, repl_table);
        loadTable(client, "PARTITION_TESTER", false, partition_table);

        saveTablesWithDefaultOptions(client);

        JSONObject digest = SnapshotUtil.CRCCheck(new VoltFile(TMPDIR, TESTNONCE + "-host_0.digest"), LOG);
        JSONObject transactionIds = digest.getJSONObject("partitionTransactionIds");
        System.out.println("TRANSACTION IDS: " + transactionIds.toString());
        assertEquals( 4, transactionIds.length());

        Set<Integer> partitions = new HashSet<Integer>();
        Iterator<String> keys = transactionIds.keys();
        while (keys.hasNext()) {
            final long foundTxnId = transactionIds.getLong(keys.next());
            //The txnids should be non-zero and there should be one for each partition
            partitions.add(TxnEgo.getPartitionId(foundTxnId));
            assertTrue(foundTxnId > TxnEgo.makeZero(TxnEgo.getPartitionId(foundTxnId)).getTxnId());
        }
        assertTrue(partitions.contains(0));
        assertTrue(partitions.contains(1));
        assertTrue(partitions.contains(2));
        assertTrue(partitions.contains(MpInitiator.MP_INIT_PID));

        m_config.shutDown();

        CatalogChangeSingleProcessServer config =
                (CatalogChangeSingleProcessServer) m_config;
        config.recompile(1);
        try {
            m_config.startUp(false);
            client = getClient();

            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();

            saveTables(client, TMPDIR, TESTNONCE + 2, true, false);

            digest = SnapshotUtil.CRCCheck(new VoltFile(TMPDIR, TESTNONCE + "2-host_0.digest"), LOG);
            JSONObject newTransactionIds = digest.getJSONObject("partitionTransactionIds");
            assertEquals(transactionIds.length(), newTransactionIds.length());

            keys = transactionIds.keys();
            while (keys.hasNext()) {
                String partitionId = keys.next();
                final long txnid = newTransactionIds.getLong(partitionId);

                //Because these are no longer part of the cluster they should be unchanged
                if (partitionId.equals("2") || partitionId.equals("1")) {
                    assertEquals(txnid, transactionIds.getLong(partitionId));
                } else if (partitionId.equals(Integer.toString(MpInitiator.MP_INIT_PID)) || partitionId.equals(1)) {
                    //These should be > than the one from the other snapshot
                    //because it picked up where it left off on restore and did more work
                    assertTrue(txnid > transactionIds.getLong(partitionId));
                }
            }
        } finally {
            config.revertCompile();
        }
    }

    //
    // Test that a replicated table can be distributed correctly
    //
    public void testDistributeReplicatedTable()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testDistributeReplicatedTable");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        LocalCluster lc = new LocalCluster( JAR_NAME, 2, 3, 0, BackendTarget.NATIVE_EE_JNI);
        lc.setHasLocalServer(false);
        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        lc.compile(project);
        lc.startUp();
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                // make a TPCC warehouse table
                VoltTable partition_table =
                    createPartitionedTable(num_partitioned_items, 0);

                loadTable(client, "REPLICATED_TESTER", true, repl_table);
                loadTable(client, "PARTITION_TESTER", false, partition_table);
                saveTablesWithDefaultOptions(client);

                boolean skipFirst = true;
                int deletedFiles = 0;
                for (File f : lc.listFiles(new File(TMPDIR))) {
                    if (f.getName().startsWith(TESTNONCE + "-REPLICATED")) {
                        if (skipFirst) {
                            skipFirst = false;
                            continue;
                        }
                        assertTrue(f.delete());
                        deletedFiles++;
                    }
                }
                assertEquals(deletedFiles, 2);
            } finally {
                client.close();
            }
            lc.shutDown();
            lc.startUp(false);

            client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));

            try {
                ClientResponse cr = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);
                assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

                checkTable(client, "REPLICATED_TESTER", "RT_ID", num_replicated_items);
                checkTable(client, "PARTITION_TESTER", "PT_ID", num_partitioned_items);

                /*
                 * Test that the cluster goes down if you do a restore with dups
                 */
                ZooKeeper zk = ZKUtil.getClient(lc.zkinterface(0), 5000, ImmutableSet.<Long>of());
                doDupRestore(client, false, zk);
                long start = System.currentTimeMillis();
                while(!lc.areAllNonLocalProcessesDead()) {
                    Thread.sleep(1);
                    long now = System.currentTimeMillis();
                    long delta = now - start;
                    if (delta > 10000) break;
                }
                assertTrue(lc.areAllNonLocalProcessesDead());
            } finally {
                client.close();
            }
        } finally {
            lc.shutDown();
        }
    }

    public void testQueueUserSnapshot() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Staring testQueueUserSnapshot.");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new HashSet<String>();

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks,
                                 false,
                                 expectedText);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        //
        // Take a snapshot that will block snapshots in the system
        //
        DefaultSnapshotDataTarget.m_simulateBlockedWrite = new CountDownLatch(1);
        client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)0);

        org.voltdb.SnapshotDaemon.m_userSnapshotRetryInterval = 1;

        //
        // Make sure we can queue a snapshot
        //
        ClientResponse r =
            client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE + "2",  (byte)0);
        VoltTable result = r.getResults()[0];
        assertTrue(result.advanceRow());
        assertTrue(
                result.getString("ERR_MSG").startsWith("SNAPSHOT REQUEST QUEUED"));

        //Let it reattempt and fail a few times
        Thread.sleep(2000);

        //
        // Make sure that attempting to queue a second snapshot save request results
        // in a snapshot in progress message
        //
        r =
            client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE + "2",  (byte)0);
        result = r.getResults()[0];
        assertTrue(result.advanceRow());
        assertTrue(
                result.getString("ERR_MSG").startsWith("SNAPSHOT IN PROGRESS"));

        //
        // Now make sure it is reattempted and works
        //
        DefaultSnapshotDataTarget.m_simulateBlockedWrite.countDown();
        DefaultSnapshotDataTarget.m_simulateBlockedWrite = null;

        boolean hadSuccess = false;
        for (int ii = 0; ii < 5; ii++) {
            Thread.sleep(2000);
            hadSuccess = validateSnapshot(true, true, TESTNONCE + "2");
            if (hadSuccess) break;
        }
        assertTrue(hadSuccess);

        //
        // Make sure errors are properly forwarded, this is one code path to handle errors,
        // there is another for errors that don't occur right off the bat
        //
        r =
            client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE + "2",  (byte)1);
        result = r.getResults()[0];
        assertTrue(result.advanceRow());
        assertTrue(result.getString("ERR_MSG").startsWith("SAVE FILE ALREADY EXISTS"));
    }

    //
    // Test specific case where a user snapshot is queued
    // and then fails while queued. It shouldn't block future snapshots
    //
    public void testQueueFailedUserSnapshot() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Staring testQueueFailedUserSnapshot.");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new HashSet<String>();

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks,
                                 false,
                                 expectedText);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        //
        // Take a snapshot that will block snapshots in the system
        //
        DefaultSnapshotDataTarget.m_simulateBlockedWrite = new CountDownLatch(1);
        client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)0);

        org.voltdb.SnapshotDaemon.m_userSnapshotRetryInterval = 1;

        //
        // Make sure we can queue a snapshot
        //
        ClientResponse r =
            client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte)0);
        VoltTable result = r.getResults()[0];
        assertTrue(result.advanceRow());
        assertTrue(
                result.getString("ERR_MSG").startsWith("SNAPSHOT REQUEST QUEUED"));

        //Let it reattempt a few times
        Thread.sleep(2000);

        //
        // Now make sure it is reattempted, it will fail,
        // because it has the name of an existing snapshot.
        // No way to tell other then that new snapshots continue to work
        //
        DefaultSnapshotDataTarget.m_simulateBlockedWrite.countDown();
        DefaultSnapshotDataTarget.m_simulateBlockedWrite = null;

        Thread.sleep(2000);

        //
        // Make sure errors are properly forwarded, this is one code path to handle errors,
        // there is another for errors that don't occur right off the bat
        //
        r =
            client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE + "2",  (byte)1);
        result = r.getResults()[0];
        while (result.advanceRow()) {
            assertTrue(result.getString("RESULT").equals("SUCCESS"));
        }
        validateSnapshot(true, false, TESTNONCE + "2");
    }

    public void testRestore12Snapshot()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        Client client = getClient();
        byte snapshotTarBytes[] = new byte[1024 * 1024 * 3];
        InputStream is =
            org.voltdb_testprocs.regressionsuites.saverestore.MatView.class.
            getResource("voltdb_1.2_snapshot.tar.gz").openConnection().getInputStream();
        GZIPInputStream gis = new GZIPInputStream(is);
        int totalRead = 0;
        int readLastTime = 0;
        while (readLastTime != -1 && totalRead != snapshotTarBytes.length) {
            readLastTime = gis.read(snapshotTarBytes, totalRead, snapshotTarBytes.length - totalRead);
            if (readLastTime == -1) {
                break;
            }
            totalRead += readLastTime;
        }
        assertTrue(totalRead > 0);
        assertFalse(totalRead == snapshotTarBytes.length);

        ProcessBuilder pb = new ProcessBuilder(new String[]{ "tar", "--directory", TMPDIR, "-x"});
        Process proc = pb.start();
        OutputStream os = proc.getOutputStream();
        os.write(snapshotTarBytes, 0, totalRead);
        os.close();
        assertEquals(0, proc.waitFor());
        validateSnapshot(true);

        byte firstStringBytes[] = new byte[1048576];
        java.util.Arrays.fill(firstStringBytes, (byte)'c');
        byte secondStringBytes[] = new byte[1048564];
        java.util.Arrays.fill(secondStringBytes, (byte)'a');

        client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);

        VoltTable results[] = client.callProcedure("JumboSelect", 0).getResults();
        assertEquals(results.length, 1);
        assertTrue(results[0].advanceRow());
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(1), firstStringBytes));
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(2), secondStringBytes));
    }

    public void testRestore2dot8dot4dot1Snapshot()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        Client client = getClient();
        byte snapshotTarBytes[] = new byte[1024 * 1024 * 3];
        InputStream is =
            org.voltdb_testprocs.regressionsuites.saverestore.MatView.class.
            getResource("voltdb_2.8.4.1_snapshot.tar.gz").openConnection().getInputStream();
        GZIPInputStream gis = new GZIPInputStream(is);
        int totalRead = 0;
        int readLastTime = 0;
        while (readLastTime != -1 && totalRead != snapshotTarBytes.length) {
            readLastTime = gis.read(snapshotTarBytes, totalRead, snapshotTarBytes.length - totalRead);
            if (readLastTime == -1) {
                break;
            }
            totalRead += readLastTime;
        }
        assertTrue(totalRead > 0);
        assertFalse(totalRead == snapshotTarBytes.length);

        ProcessBuilder pb = new ProcessBuilder(new String[]{ "tar", "--directory", TMPDIR, "-x"});
        Process proc = pb.start();
        OutputStream os = proc.getOutputStream();
        os.write(snapshotTarBytes, 0, totalRead);
        os.close();
        assertEquals(0, proc.waitFor());
        validateSnapshot(true);

        byte firstStringBytes[] = new byte[1048576];
        java.util.Arrays.fill(firstStringBytes, (byte)'c');
        byte secondStringBytes[] = new byte[1048564];
        java.util.Arrays.fill(secondStringBytes, (byte)'a');

        client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);

        VoltTable results[] = client.callProcedure("JumboSelect", 0).getResults();
        assertEquals(results.length, 1);
        assertTrue(results[0].advanceRow());
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(1), firstStringBytes));
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(2), secondStringBytes));
    }

    public void testSaveRestoreJumboRows()
    throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSaveRestoreJumboRows.");
        Client client = getClient();
        byte firstStringBytes[] = new byte[1048576];
        java.util.Arrays.fill(firstStringBytes, (byte)'c');
        String firstString = new String(firstStringBytes, "UTF-8");
        byte secondStringBytes[] = new byte[1048564];
        java.util.Arrays.fill(secondStringBytes, (byte)'a');
        String secondString = new String(secondStringBytes, "UTF-8");

        VoltTable results[] = client.callProcedure("JumboInsert", 0, firstString, secondString).getResults();
        firstString = null;
        secondString = null;

        assertEquals(results.length, 1);
        assertEquals( 1, results[0].asScalarLong());

        results = client.callProcedure("JumboSelect", 0).getResults();
        assertEquals(results.length, 1);
        assertTrue(results[0].advanceRow());
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(1), firstStringBytes));
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(2), secondStringBytes));

        saveTablesWithDefaultOptions(client);
        validateSnapshot(true);

        releaseClient(client);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

        client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);

        results = client.callProcedure("JumboSelect", 0).getResults();
        assertEquals(results.length, 1);
        assertTrue(results[0].advanceRow());
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(1), firstStringBytes));
        assertTrue(java.util.Arrays.equals( results[0].getStringAsBytes(2), secondStringBytes));
    }

    public void testTSVConversion() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Staring testTSVConversion.");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new TreeSet<String>();

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks,
                                 false,
                                 expectedText);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)1);

        validateSnapshot(true);
        generateAndValidateTextFile( expectedText, false);
    }

    public void testCSVConversion() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testCSVConversion");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new TreeSet<String>();

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks,
                                 true,
                                 expectedText);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)1);

        validateSnapshot(true);
        generateAndValidateTextFile( new TreeSet<String>(expectedText), true);

        deleteTestFiles();

        client.callProcedure("@SnapshotSave",
                "{ uripath:\"file://" + TMPDIR +
                "\", nonce:\"" + TESTNONCE + "\", block:true, format:\"csv\" }");

        FileInputStream fis = new FileInputStream(
                TMPDIR + File.separator + TESTNONCE + "-REPLICATED_TESTER" + ".csv");
        validateTextFile(expectedText, true, fis);
    }

    public void testBadSnapshotParams() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testBadSnapshotParams");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new TreeSet<String>();

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks,
                                 true,
                                 expectedText);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        boolean threwexception = false;
        try {
            client.callProcedure("@SnapshotSave",
                    "{ }");
        } catch (Exception e) {
            threwexception = true;
        }
        assertTrue(threwexception);

        threwexception = false;
        try {
            client.callProcedure("@SnapshotSave",
                    new Object[] {null});
        } catch (Exception e) {
            threwexception = true;
        }
        assertTrue(threwexception);

        threwexception = false;
        try {
            client.callProcedure("@SnapshotSave",
                    "{ uripath:\"file:///tmp\", nonce:\"\", block:true }");
        } catch (Exception e) {
            threwexception = true;
        }
        assertTrue(threwexception);

        threwexception = false;
        try {
            client.callProcedure("@SnapshotSave",
                    "{ uripath:\"file:///tmp\", nonce:\"-\", block:true }");
        } catch (Exception e) {
            threwexception = true;
        }
        assertTrue(threwexception);

        threwexception = false;
        try {
            client.callProcedure("@SnapshotSave",
                    "{ uripath:\"file:///tmp\", nonce:\",\", block:true }");
        } catch (Exception e) {
            threwexception = true;
        }
        assertTrue(threwexception);

        threwexception = false;
        try {
            client.callProcedure("@SnapshotSave",
                    "{ uripath:\"hdfs:///tmp\", nonce:\"foo\", block:true }");
        } catch (Exception e) {
            threwexception = true;
        }
        assertTrue(threwexception);

        threwexception = false;
        try {
            client.callProcedure("@SnapshotSave",
                    "{ uripath:\"/tmp\", nonce:\"foo\", block:true }");
        } catch (Exception e) {
            threwexception = true;
        }
        assertTrue(threwexception);

        threwexception = false;
        try {
            client.callProcedure("@SnapshotSave",
                    "{ uripath:true, nonce:\"foo\", block:true }");
        } catch (Exception e) {
            threwexception = true;
        }
        assertTrue(threwexception);

        client.callProcedure("@SnapshotSave",
                "{ uripath:\"file://" + TMPDIR +
                "\", nonce:\"" + TESTNONCE + "\", block:true, format:\"csv\" }");

        FileInputStream fis = new FileInputStream(
                TMPDIR + File.separator + TESTNONCE + "-REPLICATED_TESTER" + ".csv");
        validateTextFile(expectedText, true, fis);
    }

    //
    // Also does some basic smoke tests
    // of @SnapshotStatus, @SnapshotScan and @SnapshotDelete
    //
    public void testSnapshotSave() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSnapshotSave");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        VoltTable[] results = null;

        results = client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)1).getResults();

        validateSnapshot(true);

        //
        // Check that snapshot status returns a reasonable result
        //
        checkSnapshotStatus(client, TMPDIR, TESTNONCE, null, "SUCCESS", 8);

        VoltTable scanResults[] = client.callProcedure("@SnapshotScan", new Object[] { null }).getResults();
        assertNotNull(scanResults);
        assertEquals( 1, scanResults.length);
        assertEquals( 1, scanResults[0].getColumnCount());
        assertEquals( 1, scanResults[0].getRowCount());
        assertTrue( scanResults[0].advanceRow());
        assertTrue( "ERR_MSG".equals(scanResults[0].getColumnName(0)));

        scanResults = client.callProcedure("@SnapshotScan", "/doesntexist").getResults();
        assertNotNull(scanResults);
        assertEquals( 1, scanResults[1].getRowCount());
        assertTrue( scanResults[1].advanceRow());
        assertTrue( "FAILURE".equals(scanResults[1].getString("RESULT")));

        scanResults = client.callProcedure("@SnapshotScan", TMPDIR).getResults();
        assertNotNull(scanResults);
        assertEquals( 3, scanResults.length);
        assertEquals( 9, scanResults[0].getColumnCount());
        assertTrue(scanResults[0].getRowCount() >= 1);
        assertTrue(scanResults[0].advanceRow());
        //
        // We can't assert that all snapshot files are generated by this test.
        // There might be leftover snapshot files from other runs.
        //
        int count = 0;
        String completeStatus = null;
        do {
            if (TESTNONCE.equals(scanResults[0].getString("NONCE"))) {
                assertTrue(TMPDIR.equals(scanResults[0].getString("PATH")));
                count++;
                completeStatus = scanResults[0].getString("COMPLETE");
            }
        } while (scanResults[0].advanceRow());
        assertEquals(1, count);
        assertNotNull(completeStatus);
        assertTrue("TRUE".equals(completeStatus));

        FilenameFilter cleaner = new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE) && file.endsWith("vpt");
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        tmp_files[0].delete();

        scanResults = client.callProcedure("@SnapshotScan", TMPDIR).getResults();
        assertNotNull(scanResults);
        assertEquals( 3, scanResults.length);
        assertEquals( 9, scanResults[0].getColumnCount());
        assertTrue(scanResults[0].getRowCount() >= 1);
        assertTrue(scanResults[0].advanceRow());
        count = 0;
        String missingTableName = null;
        do {
            if (TESTNONCE.equals(scanResults[0].getString("NONCE"))
                && "FALSE".equals(scanResults[0].getString("COMPLETE"))) {
                assertTrue(TMPDIR.equals(scanResults[0].getString("PATH")));
                count++;
                missingTableName = scanResults[0].getString("TABLES_MISSING");
            }
        } while (scanResults[0].advanceRow());
        assertEquals(1, count);
        assertNotNull(missingTableName);
        assertTrue(tmp_files[0].getName().contains(missingTableName));

        // Instead of something exhaustive, let's just make sure that we get
        // the number of result rows corresponding to the number of ExecutionSites
        // that did save work
        Cluster cluster = VoltDB.instance().getCatalogContext().cluster;
        Database database = cluster.getDatabases().get("database");
        CatalogMap<Table> tables = database.getTables();
        int num_hosts = 1;
        int replicated = 0;
        int total_tables = 0;
        int expected_entries = 3;

        for (Table table : tables)
        {
            // Ignore materialized tables
            if (table.getMaterializer() == null)
            {
                total_tables++;
                if (table.getIsreplicated())
                {
                    replicated++;
                }
            }
        }
        assertEquals(expected_entries, results[0].getRowCount());

        while (results[0].advanceRow())
        {
            assertEquals(results[0].getString("RESULT"), "SUCCESS");
        }

        // Now, try the save again and verify that we fail (since all the save
        // files will still exist. This will return one entry per table
        // per host
        expected_entries =
            ((total_tables - replicated) * num_hosts) + replicated;
        try
        {
            results = client.callProcedure("@SnapshotSave", TMPDIR,
                                           TESTNONCE, (byte)1).getResults();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        assertEquals(expected_entries, results[0].getRowCount());
        while (results[0].advanceRow())
        {
            if (!tmp_files[0].getName().contains(results[0].getString("TABLE"))) {
                assertEquals(results[0].getString("RESULT"), "FAILURE");
                assertTrue(results[0].getString("ERR_MSG").contains("SAVE FILE ALREADY EXISTS"));
            }
        }

        VoltTable deleteResults[] =
            client.callProcedure(
                "@SnapshotDelete",
                new String[] {TMPDIR},
                new String[]{TESTNONCE}).getResults();
        assertNotNull(deleteResults);
        assertEquals( 1, deleteResults.length);
        assertEquals( 9, deleteResults[0].getColumnCount());
        //No rows returned right now, because the delete is done in a separate thread
        assertEquals( 0, deleteResults[0].getRowCount());
        //Give the async thread time to delete the files
        boolean hadZeroFiles = false;
        for (int ii = 0; ii < 20; ii++) {
            Thread.sleep(100);
            tmp_files = tmp_dir.listFiles(cleaner);
            if (tmp_files.length == 0) {
                hadZeroFiles = true;
                break;
            }
        }
        assertTrue( hadZeroFiles);

        validateSnapshot(false);

        try
        {
            results = client.callProcedure(
                    "@SnapshotSave",
                    "{ uripath:\"file://" + TMPDIR +
                    "\", nonce:\"" + TESTNONCE + "\", block:true, format:\"csv\" }").getResults();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        System.out.println("Created CSV snapshot");
    }

    private void generateAndValidateTextFile(Set<String> expectedText, boolean csv) throws Exception {
        String args[] = new String[] {
                TESTNONCE,
               "--dir",
               TMPDIR,
               "--table",
               "REPLICATED_TESTER",
               "--type",
               csv ? "CSV" : "TSV",
               "--outdir",
               TMPDIR
        };
        SnapshotConverter.main(args);
        FileInputStream fis = new FileInputStream(
                TMPDIR + File.separator + "REPLICATED_TESTER" + (csv ? ".csv" : ".tsv"));
        validateTextFile( expectedText, csv, fis);
    }

    private void validateTextFile(Set<String> expectedText, boolean csv, FileInputStream fis) throws Exception {
        try {
            InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
            BufferedReader br = new BufferedReader(isr);
            StringBuffer sb = new StringBuffer();
            int nextCharInt;
            while ((nextCharInt = br.read()) != -1) {
                char nextChar = (char)nextCharInt;
                if (csv) {
                    if (nextChar == '"') {
                        sb.append(nextChar);
                        int nextNextCharInt = -1;
                        char prevChar = nextChar;
                        while ((nextNextCharInt = br.read()) != -1) {
                            char nextNextChar = (char)nextNextCharInt;
                            if (nextNextChar == '"') {
                                if (prevChar == '"') {
                                    sb.append(nextNextChar);
                                } else {
                                    sb.append(nextNextChar);
                                    break;
                                }
                            } else {
                                sb.append(nextNextChar);
                            }
                            prevChar = nextNextChar;
                        }
                    } else if (nextChar == '\n' || nextChar == '\r') {
                        if (!expectedText.contains(sb.toString())) {
                            System.out.println("Missing string is " + sb);
                        }
                        assertTrue(expectedText.remove(sb.toString()));
                        sb = new StringBuffer();
                    } else {
                        sb.append(nextChar);
                    }
                } else {
                    if (nextChar == '\\') {
                        sb.append(nextChar);
                        int nextNextCharInt = br.read();
                        char nextNextChar = (char)nextNextCharInt;
                        sb.append(nextNextChar);
                    } else if (nextChar == '\n' || nextChar == '\r') {
                        if (!expectedText.contains(sb.toString())) {
                            System.out.println("Missing string is " + sb);
                        }
                        assertTrue(expectedText.remove(sb.toString()));
                        sb = new StringBuffer();
                    } else {
                        sb.append(nextChar);
                    }
                }
            }
            assertTrue(expectedText.isEmpty());
        } finally {
            fis.close();
        }
    }

    public void testIdleOnlineSnapshot() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testIdleOnlineSnapshot");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)0);

        //
        // Increased timeout from .7 to 1.2 seconds for the mini. It might not
        // finished the non-blocking snapshot in time.  Later increased to 2.0
        // to get memcheck to stop timing out ENG-1800
        //
        Thread.sleep(2000);

        //
        // Check that snapshot status returns a reasonable result
        //
        checkSnapshotStatus(client, TMPDIR, TESTNONCE, null, "SUCCESS", 8);

        validateSnapshot(true);
    }

    public void testSaveReplicatedAndRestorePartitionedTable()
    throws Exception
    {
        if (isValgrind()) return;

        System.out.println("Starting testSaveReplicatedAndRestorePartitionedTable");

        int num_replicated_items_per_chunk = 200;
        int num_replicated_chunks = 10;

        Client client = getClient();

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks);

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

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

        VoltTable[] results = null;
        results = saveTablesWithDefaultOptions(client);

        // Kill and restart all the execution sites after changing the catalog
        m_config.shutDown();
        CatalogChangeSingleProcessServer config =
            (CatalogChangeSingleProcessServer) m_config;
        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        // partition the original replicated table on its primary key
        project.addPartitionInfo("REPLICATED_TESTER", "RT_ID");
        project.addSchema(SaveRestoreTestProjectBuilder.class.
                          getResource("saverestore-ddl.sql"));
        config.recompile(project);
        m_config.startUp();

        client = getClient();

        try
        {
            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);

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

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

        VoltTable final_mem = null;
        try
        {
            final_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
            System.out.println("STATS: " + final_mem.toString());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("Statistics exception: " + ex.getMessage());
        }

        checkTable(client, "REPLICATED_TESTER", "RT_ID",
                   num_replicated_items_per_chunk * num_replicated_chunks);

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        System.out.println("@Statistics after restore:");
        System.out.println(results[0]);

        boolean ok = false;
        int foundItem = 0;
        long tupleCounts[] = null;
        while (!ok) {
            ok = true;
            foundItem = 0;
            tupleCounts = new long[3];
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("REPLICATED_TESTER"))
                {
                    tupleCounts[foundItem] = results[0].getLong("TUPLE_COUNT");
                    ++foundItem;
                }
            }
            ok = ok & (foundItem == 3);
        }

        long totalTupleCount = 0;
        for (long c : tupleCounts) {
            totalTupleCount += c;
        }
        assertEquals((num_replicated_chunks * num_replicated_items_per_chunk), totalTupleCount);

        // make sure all sites were loaded
        validateSnapshot(true);

        // revert back to replicated table
        config.revertCompile();

    }

    public void testSavePartitionedAndRestoreReplicatedTable()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSaveAndRestorePartitionedTable");
        int num_partitioned_items_per_chunk = 120; // divisible by 3
        int num_partitioned_chunks = 10;
        Client client = getClient();

        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);
        VoltTable[] results = null;

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

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

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = false;

        validateSnapshot(false);

        results = saveTablesWithDefaultOptions(client);

        validateSnapshot(true);

        while (results[0].advanceRow()) {
            if (!results[0].getString("RESULT").equals("SUCCESS")) {
                System.out.println(results[0].getString("ERR_MSG"));
            }
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        try
        {
            checkSnapshotStatus(client, TMPDIR, TESTNONCE, null, "SUCCESS", 8);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        // Kill and restart all the execution sites after removing partitioning column
        m_config.shutDown();
        CatalogChangeSingleProcessServer config =
            (CatalogChangeSingleProcessServer) m_config;
        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addDefaultProcedures();
        project.addSchema(SaveRestoreTestProjectBuilder.class.
                          getResource("saverestore-ddl.sql"));
        config.recompile(project);
        m_config.startUp();

        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE).getResults();

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

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);

        boolean ok = false;
        int foundItem = 0;
        while (!ok) {
            ok = true;
            foundItem = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
                {
                    long tupleCount = results[0].getLong("TUPLE_COUNT");
                    ok = (ok & (tupleCount == (num_partitioned_items_per_chunk * num_partitioned_chunks)));
                    ++foundItem;
                }
            }
            ok = ok & (foundItem == 3);
        }

        results = client.callProcedure("@Statistics", "table", 0).getResults();
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
            {
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks),
                        results[0].getLong("TUPLE_COUNT"));
            }
        }

        doDupRestore(client);

        /*
         * Assert a non-empty CSV file containing duplicates was created
         */
        boolean havePartitionedCSVFile = false;
        for (File f : new File(TMPDIR).listFiles()) {
          final String name = f.getName();
          if (name.startsWith("PARTITION_TESTER") && name.endsWith(".csv")) {
              havePartitionedCSVFile = true;
              if (!(f.length() > 30000)) {
                  //It should be about 37k, make sure it isn't unusually small
                  fail("Duplicates file is not as large as expected " + f.length());
              }
          }
        }
        assertTrue(havePartitionedCSVFile);

        config.revertCompile();
    }


    public void testSaveAndRestoreReplicatedTable()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSaveAndRestoreReplicatedTable");
        int num_replicated_items_per_chunk = 200;
        int num_replicated_chunks = 10;

        Client client = getClient();

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks);

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

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

        VoltTable[] results = null;
        results = saveTablesWithDefaultOptions(client);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

        try
        {
            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);

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

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

        VoltTable final_mem = null;
        try
        {
            final_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
            System.out.println("STATS: " + final_mem.toString());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("Statistics exception: " + ex.getMessage());
        }

        checkTable(client, "REPLICATED_TESTER", "RT_ID",
                   num_replicated_items_per_chunk * num_replicated_chunks);

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        System.out.println("@Statistics after restore:");
        System.out.println(results[0]);

        boolean ok = false;
        int foundItem = 0;
        while (!ok) {
            ok = true;
            foundItem = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("REPLICATED_TESTER"))
                {
                    long tupleCount = results[0].getLong("TUPLE_COUNT");
                    ok = (ok & (tupleCount == (num_replicated_items_per_chunk * num_replicated_chunks)));
                    ++foundItem;
                }
            }
            ok = ok & (foundItem == 3);
        }

        results = client.callProcedure("@Statistics", "table", 0).getResults();
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("REPLICATED_TESTER"))
            {
                assertEquals((num_replicated_chunks * num_replicated_items_per_chunk),
                        results[0].getLong("TUPLE_COUNT"));
            }
        }

        // make sure all sites were loaded

        validateSnapshot(true);
    }

    public void testSaveAndRestorePartitionedTable()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testSaveAndRestorePartitionedTable");
        int num_partitioned_items_per_chunk = 120; // divisible by 3
        int num_partitioned_chunks = 10;
        int num_replicated_items_per_chunk = 200;
        int num_replicated_chunks = 10;
        Client client = getClient();

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

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = true;
        results = saveTablesWithDefaultOptions(client);
        deleteTestFiles();

        while (results[0].advanceRow()) {
            assertTrue(results[0].getString("RESULT").equals("FAILURE"));
        }

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = false;

        validateSnapshot(false);

        results = saveTablesWithDefaultOptions(client);

        validateSnapshot(true);

        while (results[0].advanceRow()) {
            if (!results[0].getString("RESULT").equals("SUCCESS")) {
                System.out.println(results[0].getString("ERR_MSG"));
            }
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        try
        {
            checkSnapshotStatus(client, TMPDIR, TESTNONCE, null, "SUCCESS", 8);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE).getResults();

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

        boolean threwException = false;
        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE).getResults();

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

        doDupRestore(client);

        /*
         * Assert a non-empty CSV file containing duplicates was created
         */
        boolean havePartitionedCSVFile = false;
        boolean haveReplicatedCSVFile = false;
        for (File f : new File(TMPDIR).listFiles()) {
          final String name = f.getName();
          if (name.startsWith("PARTITION_TESTER") && name.endsWith(".csv")) {
              havePartitionedCSVFile = true;
              if (!(f.length() > 30000)) {
                  //It should be about 37k, make sure it isn't unusually small
                  fail("Duplicates file is not as large as expected " + f.length());
              }
          }
          if (name.startsWith("REPLICATED_TESTER") && name.endsWith(".csv")) {
              haveReplicatedCSVFile = true;
              if (!(f.length() > 60000)) {
                  //It should be about 37k, make sure it isn't unusually small
                  fail("Duplicates file is not as large as expected " + f.length());
              }
          }
        }
        assertTrue(havePartitionedCSVFile);
        assertTrue(haveReplicatedCSVFile);

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);

        boolean ok = false;
        int foundItem = 0;
        while (!ok) {
            ok = true;
            foundItem = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
                {
                    long tupleCount = results[0].getLong("TUPLE_COUNT");
                    ok = (ok & (tupleCount == ((num_partitioned_items_per_chunk * num_partitioned_chunks) / 3)));
                    ++foundItem;
                }
            }
            ok = ok & (foundItem == 3);
        }

        results = client.callProcedure("@Statistics", "table", 0).getResults();
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
            {
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 3,
                        results[0].getLong("TUPLE_COUNT"));
            }
        }

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();
        deleteTestFiles();

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingChunk = true;

        org.voltdb.sysprocs.SnapshotRegistry.clear();
        client = getClient();

        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        results = saveTablesWithDefaultOptions(client);

        validateSnapshot(false);

        try
        {
            results = client.callProcedure("@SnapshotStatus").getResults();
            boolean hasFailure = false;
            while (results[0].advanceRow())
                hasFailure |= results[0].getString("RESULT").equals("FAILURE");
            assertTrue(hasFailure);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingChunk = false;
        deleteTestFiles();
        results = saveTablesWithDefaultOptions(client);

        validateSnapshot(true);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE).getResults();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

        VoltTable final_mem = null;
        try
        {
            final_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
            System.out.println("STATS: " + final_mem.toString());
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("Statistics exception: " + ex.getMessage());
        }

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        ok = false;
        foundItem = 0;
        while (!ok) {
            ok = true;
            foundItem = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
                {
                    long tupleCount = results[0].getLong("TUPLE_COUNT");
                    ok = (ok & (tupleCount == ((num_partitioned_items_per_chunk * num_partitioned_chunks) / 3)));
                    ++foundItem;
                }
            }
            ok = ok & (foundItem == 3);
        }
        assertEquals(3, foundItem);

        results = client.callProcedure("@Statistics", "table", 0).getResults();
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
            {
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 3,
                        results[0].getLong("TUPLE_COUNT"));
            }
        }
    }

    private void doDupRestore(Client client) throws Exception {
        doDupRestore(client, true, VoltDB.instance().getHostMessenger().getZK());
    }

    private void doDupRestore(Client client, boolean allowDupes, ZooKeeper zk) throws Exception {
        VoltTable[] results;
        boolean threwException;

        /*
         * Now check that doing a restore and logging duplicates works.
         * Delete the ZK nodes created by the restore already done and
         * it will go again.
         */
        zk.delete(VoltZK.restoreMarker, -1);
        zk.delete(VoltZK.perPartitionTxnIds, -1);
        threwException = false;
        try
        {
            JSONObject jsObj = new JSONObject();
            jsObj.put(SnapshotRestore.JSON_NONCE, TESTNONCE);
            jsObj.put(SnapshotRestore.JSON_PATH, TMPDIR);
            if (allowDupes) {
                jsObj.put(SnapshotRestore.JSON_DUPLICATES_PATH, TMPDIR);
            }

            results = client.callProcedure("@SnapshotRestore", jsObj.toString()).getResults();

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
        assertTrue(allowDupes == !threwException);
    }

    // Test that we fail properly when there are no savefiles available
    public void testRestoreMissingFiles()
    throws IOException, InterruptedException
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testRestoreMissingFile");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();

        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
        // make a TPCC warehouse table
        VoltTable partition_table =
            createPartitionedTable(num_partitioned_items, 0);

        loadTable(client, "REPLICATED_TESTER", true, repl_table);
        loadTable(client, "PARTITION_TESTER", false, partition_table);
        saveTablesWithDefaultOptions(client);

        validateSnapshot(true);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        deleteTestFiles();
        m_config.startUp();

        client = getClient();

        try {
            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("No savefile state to restore"));
            return;
        }
        assertTrue(false);
    }

    // Test that we fail properly when the save files are corrupted
    public void testCorruptedFiles()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testCorruptedFiles");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;
        java.util.Random r = new java.util.Random(0);
        final int iterations = isValgrind() ? 5 : 5;

        for (int ii = 0; ii < iterations; ii++) {
            Client client = getClient();
            VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
            // make a TPCC warehouse table
            VoltTable partition_table =
                createPartitionedTable(num_partitioned_items, 0);

            loadTable(client, "REPLICATED_TESTER", true, repl_table);
            loadTable(client, "PARTITION_TESTER", false, partition_table);
            VoltTable results[] = saveTablesWithDefaultOptions(client);
            validateSnapshot(true);
            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    System.out.println(results[0].getString("ERR_MSG"));
                }
                assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
            }

            corruptTestFiles(r);
            validateSnapshot(false);
            releaseClient(client);
            // Kill and restart all the execution sites.
            m_config.shutDown();
            m_config.startUp();

            client = getClient();

            results = null;
            try {
                client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);
                fail(); // expect fail
            }
            catch (ProcCallException e) {
                assertEquals(ClientResponse.OPERATIONAL_FAILURE, e.getClientResponse().getStatus());
                results = e.getClientResponse().getResults();
            }
            assertNotNull(results);
            assertNotNull(results[0]);
            boolean haveFailure = false;
            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    haveFailure = true;
                    break;
                }
            }
            assertTrue(haveFailure);

            deleteTestFiles();
            releaseClient(client);

            // Kill and restart all the execution sites.
            m_config.shutDown();
            m_config.startUp();
        }
    }

    // Test that a random corruption doesn't mess up the table. Not reproducible but useful for detecting
    // stuff we won't normally find
    public void testCorruptedFilesRandom()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testCorruptedFilesRandom");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;
        java.util.Random r = new java.util.Random();
        final int iterations = isValgrind() ? 5 : 5;

        for (int ii = 0; ii < iterations; ii++) {
            Client client = getClient();

            VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
            // make a TPCC warehouse table
            VoltTable partition_table =
                createPartitionedTable(num_partitioned_items, 0);

            loadTable(client, "REPLICATED_TESTER", true, repl_table);
            loadTable(client, "PARTITION_TESTER", false, partition_table);
            saveTablesWithDefaultOptions(client);
            validateSnapshot(true);
            releaseClient(client);
            // Kill and restart all the execution sites.
            m_config.shutDown();
            corruptTestFiles(r);
            validateSnapshot(false);
            m_config.startUp();

            client = getClient();

            VoltTable results[] = null;
            try {
                client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();
                fail(); // expect fail
            }
            catch (ProcCallException e) {
                assertEquals(ClientResponse.OPERATIONAL_FAILURE, e.getClientResponse().getStatus());
                results = e.getClientResponse().getResults();
            }
            assertNotNull(results);
            assertNotNull(results[0]);
            boolean haveFailure = false;
            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    haveFailure = true;
                    break;
                }
            }
            if (!haveFailure) {
                System.out.println("foo");
            }
            assertTrue(haveFailure);

            deleteTestFiles();
            releaseClient(client);

            // Kill and restart all the execution sites.
            m_config.shutDown();
            m_config.startUp();
        }
    }


    public void testRestoreMissingPartitionFile()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();

        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
        // make a TPCC warehouse table
        VoltTable partition_table =
            createPartitionedTable(num_partitioned_items, 0);

        loadTable(client, "REPLICATED_TESTER", true, repl_table);
        loadTable(client, "PARTITION_TESTER", false, partition_table);
        saveTablesWithDefaultOptions(client);

        // Kill and restart all the execution sites.
        m_config.shutDown();

        String filename = TESTNONCE + "-PARTITION_TESTER-host_0.vpt";
        File item_file = new File(TMPDIR, filename);
        assertTrue(item_file.delete());

        m_config.startUp();
        client = getClient();

        VoltTable resultTable = null;
        try {
            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);
        }
        catch (ProcCallException e) {
            resultTable = e.getClientResponse().getResults()[0];
            assertEquals(ClientResponse.OPERATIONAL_FAILURE, e.getClientResponse().getStatus());
        }
        assertTrue(resultTable.advanceRow());
        assertTrue(resultTable.getString("ERR_MSG").equals("Save data contains no information for table PARTITION_TESTER"));
    }

    public void testRepartition()
    throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testRepartition");
        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120; // divisible by 3 and 4
        int num_partitioned_chunks = 10;
        Client client = getClient();

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_partitioned_chunks);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);
        VoltTable[] results = null;
        results = saveTablesWithDefaultOptions(client);
        validateSnapshot(true);
        // Kill and restart all the execution sites.
        m_config.shutDown();

        CatalogChangeSingleProcessServer config =
            (CatalogChangeSingleProcessServer) m_config;
        config.recompile(4);

        m_config.startUp();

        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE).getResults();
            // XXX Should check previous results for success but meh for now
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);
        checkTable(client, "REPLICATED_TESTER", "RT_ID",
                   num_replicated_items_per_chunk * num_replicated_chunks);

        // Spin until the stats look complete
        boolean ok = false;
        int foundItem = 0;
        while (!ok) {
            ok = true;
            foundItem = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
                {
                    long tupleCount = results[0].getLong("TUPLE_COUNT");
                    ok = (ok & (tupleCount == ((num_partitioned_items_per_chunk * num_partitioned_chunks) / 4)));
                    ++foundItem;
                }
            }
            ok = ok & (foundItem == 4);
        }

        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
            {
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 4,
                        results[0].getLong("TUPLE_COUNT"));
            }
        }

        config.revertCompile();
    }

    public void testChangeDDL()
    throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testChangeDDL");
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;
        Client client = getClient();

        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        // Store something in the table which will change columns
        VoltTable change_table =
            new VoltTable(new ColumnInfo("ID", VoltType.INTEGER),
                          new ColumnInfo("BYEBYE", VoltType.INTEGER));
        VoltTable eng_2025_table =
            new VoltTable(new ColumnInfo("key", VoltType.STRING),
                    new ColumnInfo("value", VoltType.VARBINARY));
        for (int i = 0; i < 10; i++)
        {
            Object[] row = new Object[] {i, i};
            change_table.addRow(row);
            eng_2025_table.addRow(new Object[] {Integer.toString(i), new byte[64]});
        }

        loadTable(client, "CHANGE_COLUMNS", false, change_table);
        loadTable(client, "ENG_2025", true, eng_2025_table);

        VoltTable[] results = null;
        results = saveTablesWithDefaultOptions(client);
        validateSnapshot(true);

        // Kill and restart all the execution sites.
        m_config.shutDown();

        CatalogChangeSingleProcessServer config =
            (CatalogChangeSingleProcessServer) m_config;

        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addSchema(SaveRestoreTestProjectBuilder.class.
                          getResource("saverestore-altered-ddl.sql"));
        config.recompile(project);

        m_config.startUp();

        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE).getResults();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        // because stats are not synchronous :(
        Thread.sleep(5000);

        // XXX consider adding a check that the newly materialized table is
        // not loaded
        results = client.callProcedure("@Statistics", "table", 0).getResults();

        boolean found_gets_created = false;
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("GETS_REMOVED"))
            {
                fail("Table GETS_REMOVED got reloaded");
            }
            if (results[0].getString("TABLE_NAME").equals("GETS_CREATED"))
            {
                found_gets_created = true;
            }
        }

        // Check the table which changed columns
        VoltTable[] change_results =
            client.callProcedure("SaveRestoreSelect", "CHANGE_COLUMNS").getResults();

        assertEquals(3, change_results[0].getColumnCount());
        for (int i = 0; i < 10; i++)
        {
            VoltTableRow row = change_results[0].fetchRow(i);
            assertEquals(i, row.getLong("ID"));
            assertEquals(1234, row.getLong("HASDEFAULT"));
            row.getLong("HASNULL");
            assertTrue(row.wasNull());
        }

        assertTrue(found_gets_created);
        config.revertCompile();
    }

    public void testGoodChangeAttributeTypes()
    throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testGoodChangeAttributeTypes");
        Client client = getClient();

        // Store something in the table which will change columns
        VoltTable change_types =
            new VoltTable(new ColumnInfo("ID", VoltType.INTEGER),
                          new ColumnInfo("BECOMES_INT", VoltType.TINYINT),
                          new ColumnInfo("BECOMES_FLOAT", VoltType.INTEGER),
                          new ColumnInfo("BECOMES_TINY", VoltType.INTEGER));

        change_types.addRow(0, 100, 100, 100);
        change_types.addRow(1, VoltType.NULL_TINYINT, VoltType.NULL_INTEGER,
                            VoltType.NULL_INTEGER);

        loadTable(client, "CHANGE_TYPES", true, change_types);

        saveTablesWithDefaultOptions(client);
        validateSnapshot(true);

        // Kill and restart all the execution sites.
        m_config.shutDown();

        CatalogChangeSingleProcessServer config =
            (CatalogChangeSingleProcessServer) m_config;

        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addSchema(SaveRestoreTestProjectBuilder.class.
                          getResource("saverestore-altered-ddl.sql"));
        config.recompile(project);

        m_config.startUp();

        client = getClient();

        try
        {
            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        client.callProcedure("@Statistics", "table", 0);

        VoltTable[] change_results =
            client.callProcedure("SaveRestoreSelect", "CHANGE_TYPES").getResults();

        VoltTableRow row = change_results[0].fetchRow(0);
        assertEquals(100, row.getLong(1));
        assertEquals(100.0, row.getDouble(2));
        assertEquals(100, row.getLong(3));
        row = change_results[0].fetchRow(1);
        row.getLong(1);
        assertTrue(row.wasNull());
        row.getDouble(2);
        assertTrue(row.wasNull());
        row.getLong(3);
        assertTrue(row.wasNull());

        config.revertCompile();
    }

    public void testBadChangeAttributeTypes()
    throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("Starting testBadChangeAttributeTypes");
        Client client = getClient();

        // Store something in the table which will change columns
        VoltTable change_types =
            new VoltTable(new ColumnInfo("ID", VoltType.INTEGER),
                          new ColumnInfo("BECOMES_INT", VoltType.TINYINT),
                          new ColumnInfo("BECOMES_FLOAT", VoltType.INTEGER),
                          new ColumnInfo("BECOMES_TINY", VoltType.INTEGER));

        change_types.addRow(0, 100, 100, 100000);

        loadTable(client, "CHANGE_TYPES", true, change_types);

        VoltTable[] results = null;
        results = saveTablesWithDefaultOptions(client);
        validateSnapshot(true);

        // Kill and restart all the execution sites.
        m_config.shutDown();

        CatalogChangeSingleProcessServer config =
            (CatalogChangeSingleProcessServer) m_config;

        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addDefaultProcedures();
        project.addDefaultPartitioning();
        project.addSchema(SaveRestoreTestProjectBuilder.class.
                          getResource("saverestore-altered-ddl.sql"));
        config.recompile(project);

        m_config.startUp();

        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE).getResults();
            fail(); // expect failure
        }
        catch (ProcCallException ex) {
            assertEquals(ClientResponse.OPERATIONAL_FAILURE, ex.getClientResponse().getStatus());
            results = ex.getClientResponse().getResults();
        }

        boolean type_failure = false;
        while (results[0].advanceRow())
        {
            if (results[0].getString("RESULT").equals("FAILURE"))
            {
                if (results[0].getString("ERR_MSG").contains("would overflow"))
                {
                    type_failure = true;
                }
            }
        }
        assertTrue(type_failure);

        config.revertCompile();
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

        // Execute @SnapshotStatus to get raw results.
        VoltTable statusResults[] = client.callProcedure("@SnapshotStatus").getResults();
        assertNotNull(statusResults);
        assertEquals( 1, statusResults.length);
        assertEquals( 14, statusResults[0].getColumnCount());

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
            results[i].hostID = statusResults[0].getLong("HOST_ID");
            results[i].table = statusResults[0].getString("TABLE");
            results[i].path = statusResults[0].getString("PATH");
            results[i].filename = statusResults[0].getString("FILENAME");
            results[i].nonce = statusResults[0].getString("NONCE");
            results[i].txnID = statusResults[0].getLong("TXNID");
            results[i].endTime = statusResults[0].getLong("END_TIME");
            results[i].result = statusResults[0].getString("RESULT");

            // Perform requested validation.
            if (path != null) {
                assertEquals(path, results[i].path);
            }
            if (nonce != null) {
                assertEquals(nonce, results[i].nonce);
            }
            if (endTime != null) {
                assertEquals(endTime, results[i].endTime);
            }
            if (result != null) {
                assertEquals(result, results[i].result);
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
            new MultiConfigSuiteBuilder(TestSaveRestoreSysprocSuite.class);

        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();

        config =
            new CatalogChangeSingleProcessServer(JAR_NAME, 3,
                                                 BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
