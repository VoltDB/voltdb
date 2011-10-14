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
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.voltdb.BackendTarget;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.utils.SnapshotConverter;
import org.voltdb.utils.SnapshotVerifier;
import org.voltdb_testprocs.regressionsuites.saverestore.CatalogChangeSingleProcessServer;
import org.voltdb_testprocs.regressionsuites.saverestore.SaveRestoreTestProjectBuilder;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures
 */
public class TestSaveRestoreSysprocSuite extends RegressionSuite {

    private static final String TMPDIR = "/tmp";
    private static final String TESTNONCE = "testnonce";

    public TestSaveRestoreSysprocSuite(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception
    {
        deleteTestFiles();
        super.setUp();
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingChunk = false;
        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = false;
        org.voltdb.sysprocs.SnapshotRegistry.clear();
    }

    @Override
    public void tearDown() throws Exception
    {
        deleteTestFiles();
        super.tearDown();
    }

    private void deleteTestFiles()
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE) ||
                file.endsWith(".vpt") ||
                file.endsWith(".digest") ||
                file.endsWith(".tsv") ||
                file.endsWith(".csv");
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files)
        {
            tmp_file.delete();
        }
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
            Set<String>[] expectedText) {
        return createReplicatedTable(numberOfItems, indexBase, expectedText, false);
    }

    private VoltTable createReplicatedTable(int numberOfItems,
                                            int indexBase,
                                            Set<?>[] expectedText,
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
            StringBuilder sb = null;
            if (expectedText != null) {
                sb = new StringBuilder(64);
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
                    int escapable = i % 5;
                    switch (escapable) {
                    case 0:
                        stringVal = "name_" + i;
                        escapedVal = "name_" + i;
                        break;
                    case 1:
                        stringVal = "na\tme_" + i;
                        escapedVal = "na\\tme_" + i;
                        break;
                    case 2:
                        stringVal = "na\nme_" + i;
                        escapedVal = "na\\nme_" + i;
                        break;
                    case 3:
                        stringVal = "na\rme_" + i;
                        escapedVal = "na\\rme_" + i;
                        break;
                    case 4:
                        stringVal = "na\\me_" + i;
                        escapedVal = "na\\\\me_" + i;
                        break;
                    }
                }
            } else {
                stringVal = "name_" + i;
            }

            Object[] row = new Object[] {i,
                                         stringVal,
                                         i,
                                         new Double(i)};
            if (expectedText != null) {
                sb.append(i).append(delimeter).append(escapedVal).append(delimeter);
                sb.append(i).append(delimeter).append(new Double(i).toString());
                //expectedText.add(sb.toString());
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

    private VoltTable[] loadTable(Client client, String tableName,
                                  VoltTable table)
    {
        VoltTable[] results = null;
        try
        {
            client.callProcedure("@LoadMultipartitionTable", tableName,
                                 table);
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
                                          int itemsPerChunk, int numChunks, boolean generateCSV, Set<?>[] expectedText)
    {
        for (int i = 0; i < numChunks; i++)
        {
            VoltTable repl_table =
                createReplicatedTable(itemsPerChunk, i * itemsPerChunk, expectedText, generateCSV);
            loadTable(client, tableName, repl_table);
        }
    }

    private void loadLargePartitionedTable(Client client, String tableName,
                                          int itemsPerChunk, int numChunks)
    {
        for (int i = 0; i < numChunks; i++)
        {
            VoltTable part_table =
                createPartitionedTable(itemsPerChunk, i * itemsPerChunk);
            loadTable(client, tableName, part_table);
        }
    }

    private VoltTable[] saveTables(Client client)
    {
        VoltTable[] results = null;
        try
        {
            results = client.callProcedure("@SnapshotSave", TMPDIR,
                                           TESTNONCE,
                                           (byte)1).getResults();
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        return results;
    }

    private void checkTable(Client client, String tableName, String orderByCol,
                            int expectedRows)
    {
        if (expectedRows > 200000)
        {
            System.out.println("Table too large to retrieve with select *");
            System.out.println("Skipping integrity check");
        }
        VoltTable result = null;
        try
        {
            result = client.callProcedure("SaveRestoreSelect", tableName).getResults()[0];
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
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
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream original = System.out;
        try {
            System.setOut(ps);
            String args[] = new String[] {
                    TESTNONCE,
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
            if (!success) {
                fail(reportString);
            }
        } catch (UnsupportedEncodingException e) {}
          finally {
            System.setOut(original);
        }
    }

    public void testRestore12Snapshot()
    throws Exception
    {
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

        ProcessBuilder pb = new ProcessBuilder(new String[]{ "tar", "--directory", "/tmp", "-x"});
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

        saveTables(client);
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
        System.out.println("Staring testTSVConversion.");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<?>[] expectedText = new Set<?>[4];
        for (int i = 0; i < 4; i++)
            expectedText[i] = new HashSet<String>();

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
        System.out.println("Starting testCSVConversion");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<?>[] expectedText = new Set<?>[4];
        for (int i = 0; i < 4; i++)
            expectedText[i] = new HashSet<String>();

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
        generateAndValidateTextFile( expectedText, true);
    }

    /*
     * Also does some basic smoke tests
     * of @SnapshotStatus, @SnapshotScan and @SnapshotDelete
     */
    public void testSnapshotSave() throws Exception
    {
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

        /*
         * Check that snapshot status returns a reasonable result
         */
        VoltTable statusResults[] = client.callProcedure("@SnapshotStatus").getResults();
        assertNotNull(statusResults);
        assertEquals( 1, statusResults.length);
        assertEquals( 14, statusResults[0].getColumnCount());
        assertTrue(statusResults[0].advanceRow());
        assertTrue(TMPDIR.equals(statusResults[0].getString("PATH")));
        assertTrue(TESTNONCE.equals(statusResults[0].getString("NONCE")));
        assertFalse( 0 == statusResults[0].getLong("END_TIME"));
        assertTrue("SUCCESS".equals(statusResults[0].getString("RESULT")));

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
        /*
         * We can't assert that all snapshot files are generated by this test.
         * There might be leftover snapshot files from other runs.
         */
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
        CatalogMap<Site> sites = cluster.getSites();
        int num_hosts = cluster.getHosts().size();
        int replicated = 0;
        int total_tables = 0;
        int expected_entries = 0;

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

        for (Site s : sites) {
            if (s.getIsexec()) {
                expected_entries++;
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
        Thread.sleep(100);
        tmp_files = tmp_dir.listFiles(cleaner);
        assertEquals( 0, tmp_files.length);

        validateSnapshot(false);
    }

    private void generateAndValidateTextFile(Set<?>[] expectedText, boolean csv) throws Exception {
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
                        //assertTrue(expectedText.remove(sb.toString()));
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
                        //assertTrue(expectedText.remove(sb.toString()));
                        sb = new StringBuffer();
                    } else {
                        sb.append(nextChar);
                    }
                }
            }
            //assertTrue(expectedText.isEmpty());
        } finally {
            fis.close();
        }
    }

    public void testIdleOnlineSnapshot() throws Exception
    {
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

        /*
         * Increased timeout from .7 to 1.2 seconds for the mini. It might not
         * finished the non-blocking snapshot in time.  Later increased to 2.0
         * to get memcheck to stop timing out ENG-1800
         */
        Thread.sleep(2000);

        /*
         * Check that snapshot status returns a reasonable result
         */
        VoltTable statusResults[] = client.callProcedure("@SnapshotStatus").getResults();
        assertNotNull(statusResults);
        assertEquals( 1, statusResults.length);
        assertEquals( 14, statusResults[0].getColumnCount());
        assertEquals( 7, statusResults[0].getRowCount());
        assertTrue(statusResults[0].advanceRow());
        assertTrue(TMPDIR.equals(statusResults[0].getString("PATH")));
        assertTrue(TESTNONCE.equals(statusResults[0].getString("NONCE")));
        assertFalse( 0 == statusResults[0].getLong("END_TIME"));
        assertTrue("SUCCESS".equals(statusResults[0].getString("RESULT")));

        validateSnapshot(true);
    }

    private void checkBeforeAndAfterMemory(VoltTable orig_mem,
                                           VoltTable final_mem)
    {
        orig_mem.advanceRow();
        final_mem.advanceRow();
        assertFalse(0 == orig_mem.getLong("TUPLEDATA"));
        assertFalse(0 == orig_mem.getLong("TUPLECOUNT"));
        assertEquals(orig_mem.getLong("TUPLEDATA"), final_mem.getLong("TUPLEDATA"));
        assertEquals(orig_mem.getLong("TUPLEALLOCATED"), final_mem.getLong("TUPLEALLOCATED"));
        assertEquals(orig_mem.getLong("INDEXMEMORY"), final_mem.getLong("INDEXMEMORY"));
        assertEquals(orig_mem.getLong("STRINGMEMORY"), final_mem.getLong("STRINGMEMORY"));
        assertEquals(orig_mem.getLong("TUPLECOUNT"), final_mem.getLong("TUPLECOUNT"));
        assertEquals(orig_mem.getLong("POOLEDMEMORY"), final_mem.getLong("POOLEDMEMORY"));

        long orig_rss = orig_mem.getLong("RSS");
        long final_rss = final_mem.getLong("RSS");

        assertTrue(Math.abs(orig_rss - final_rss) < orig_rss * .1);
    }

    public void testSaveAndRestoreReplicatedTable()
    throws IOException, InterruptedException, ProcCallException
    {
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
        results = saveTables(client);

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

        // commented out because it fails on macs like crazy-town
        //checkBeforeAndAfterMemory(orig_mem, final_mem);

        checkTable(client, "REPLICATED_TESTER", "RT_ID",
                   num_replicated_items_per_chunk * num_replicated_chunks);

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        System.out.println("@Statistics after restore:");
        System.out.println(results[0]);

        int foundItem = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("REPLICATED_TESTER"))
            {
                ++foundItem;
                assertEquals((num_replicated_chunks * num_replicated_items_per_chunk),
                        results[0].getLong("TUPLE_COUNT"));
            }
        }
        // make sure all sites were loaded
        assertEquals(3, foundItem);

        validateSnapshot(true);
    }

    public void testSaveAndRestorePartitionedTable()
    throws IOException, InterruptedException, ProcCallException
    {
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

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = true;
        results = saveTables(client);
        deleteTestFiles();

        while (results[0].advanceRow()) {
            assertTrue(results[0].getString("RESULT").equals("FAILURE"));
        }

        DefaultSnapshotDataTarget.m_simulateFullDiskWritingHeader = false;

        validateSnapshot(false);

        results = saveTables(client);

        validateSnapshot(true);

        while (results[0].advanceRow()) {
            if (!results[0].getString("RESULT").equals("SUCCESS")) {
                System.out.println(results[0].getString("ERR_MSG"));
            }
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        try
        {
            results = client.callProcedure("@SnapshotStatus").getResults();
            assertTrue(results[0].advanceRow());
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
            assertEquals( 7, results[0].getRowCount());
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

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        int foundItem = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
            {
                ++foundItem;
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 3,
                        results[0].getLong("TUPLE_COUNT"));
            }
        }
        // make sure all sites were loaded
        assertEquals(3, foundItem);

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

        results = saveTables(client);

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
        results = saveTables(client);

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

        // commented out because it fails on macs like crazy-town
        //checkBeforeAndAfterMemory(orig_mem, final_mem);

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        foundItem = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
            {
                ++foundItem;
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 3,
                        results[0].getLong("TUPLE_COUNT"));
            }
        }
        // make sure all sites were loaded
        assertEquals(3, foundItem);
    }

    // Test that we fail properly when there are no savefiles available
    public void testRestoreMissingFiles()
    throws IOException, InterruptedException
    {
        System.out.println("Starting testRestoreMissingFile");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();

        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
        // make a TPCC warehouse table
        VoltTable partition_table =
            createPartitionedTable(num_partitioned_items, 0);

        loadTable(client, "REPLICATED_TESTER", repl_table);
        loadTable(client, "PARTITION_TESTER", partition_table);
        saveTables(client);

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

            loadTable(client, "REPLICATED_TESTER", repl_table);
            loadTable(client, "PARTITION_TESTER", partition_table);
            VoltTable results[] = saveTables(client);
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

            results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();
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

            loadTable(client, "REPLICATED_TESTER", repl_table);
            loadTable(client, "PARTITION_TESTER", partition_table);
            saveTables(client);
            validateSnapshot(true);
            releaseClient(client);
            // Kill and restart all the execution sites.
            m_config.shutDown();
            corruptTestFiles(r);
            validateSnapshot(false);
            m_config.startUp();

            client = getClient();

            VoltTable results[] = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();
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

//
//    public void testRestoreMissingPartitionFile()
//    throws IOException, InterruptedException
//    {
//        int num_replicated_items = 1000;
//        int num_partitioned_items = 126;
//
//        Client client = getClient();
//
//        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0);
//        // make a TPCC warehouse table
//        VoltTable partition_table =
//            createPartitionedTable(num_partitioned_items, 0);
//
//        loadTable(client, "REPLICATED_TESTER", repl_table);
//        loadTable(client, "PARTITION_TESTER", partition_table);
//        saveTables(client);
//
//        // Kill and restart all the execution sites.
//        m_config.shutDown();
//
//        String filename = TESTNONCE + "-PARTITION_TESTER-host_0";
//        File item_file = new File(TMPDIR, filename);
//        item_file.delete();
//
//        m_config.startUp();
//        client = getClient();
//
//        try {
//            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);
//        }
//        catch (Exception e) {
//            assertTrue(e.getMessage().
//                       contains("PARTITION_TESTER has some inconsistency"));
//            return;
//        }
//        assertTrue(false);
//    }

    public void testRepartition()
    throws IOException, InterruptedException, ProcCallException
    {
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
        results = saveTables(client);
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

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        int foundItem = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
            {
                ++foundItem;
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 4,
                        results[0].getLong("TUPLE_COUNT"));
            }
        }
        // make sure all sites were loaded
        assertEquals(4, foundItem);

        config.revertCompile();
    }

    public void testChangeDDL()
    throws IOException, InterruptedException, ProcCallException
    {
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

        for (int i = 0; i < 10; i++)
        {
            Object[] row = new Object[] {i, i};
            change_table.addRow(row);
        }

        loadTable(client, "CHANGE_COLUMNS", change_table);

        VoltTable[] results = null;
        results = saveTables(client);
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

        loadTable(client, "CHANGE_TYPES", change_types);

        saveTables(client);
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
        System.out.println("Starting testBadChangeAttributeTypes");
        Client client = getClient();

        // Store something in the table which will change columns
        VoltTable change_types =
            new VoltTable(new ColumnInfo("ID", VoltType.INTEGER),
                          new ColumnInfo("BECOMES_INT", VoltType.TINYINT),
                          new ColumnInfo("BECOMES_FLOAT", VoltType.INTEGER),
                          new ColumnInfo("BECOMES_TINY", VoltType.INTEGER));

        change_types.addRow(0, 100, 100, 100000);

        loadTable(client, "CHANGE_TYPES", change_types);

        VoltTable[] results = null;
        results = saveTables(client);
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
            fail("Unexpected exception from SnapshotRestore");
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
            new CatalogChangeSingleProcessServer("sysproc-threesites.jar", 3,
                                                 BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config);

        return builder;
    }
}
