/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;

import junit.framework.Test;

import org.voltdb.BackendTarget;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.regressionsuites.saverestore.CatalogChangeSingleProcessServer;
import org.voltdb.regressionsuites.saverestore.SaveRestoreTestProjectBuilder;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures
 */
public class TestSaveRestoreSysprocSuite extends RegressionSuite {

    private static final String TMPDIR = "/tmp";
    private static final String TESTNONCE = "testnonce";
    private static final int ALLOWELT = 0;

    public TestSaveRestoreSysprocSuite(String name) {
        super(name);
    }

    @Override
    public void setUp()
    {
        deleteTestFiles();
        super.setUp();
    }

    @Override
    public void tearDown() throws InterruptedException
    {
        deleteTestFiles();
        super.tearDown();
    }

    private void deleteTestFiles()
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE);
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files)
        {
            tmp_file.delete();
        }
    }

    private void corruptTestFiles(boolean random) throws Exception
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE);
            }
        };

        java.util.Random r;
        if (random) {
            r = new java.util.Random();
        } else {
            r = new java.util.Random(0);
        }

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        int tmpIndex = r.nextInt(tmp_files.length);
        java.io.RandomAccessFile raf = new java.io.RandomAccessFile( tmp_files[tmpIndex], "rw");
        raf.seek(r.nextInt((int)raf.length()));
        raf.writeByte(r.nextInt() % 127);
        raf.close();
    }

    private VoltTable createReplicatedTable(int numberOfItems,
                                            int indexBase)
    {
        VoltTable repl_table =
            new VoltTable(new ColumnInfo("RT_ID", VoltType.INTEGER),
                          new ColumnInfo("RT_NAME", VoltType.STRING),
                          new ColumnInfo("RT_INTVAL", VoltType.INTEGER),
                          new ColumnInfo("RT_FLOATVAL", VoltType.FLOAT));

        for (int i = indexBase; i < numberOfItems + indexBase; i++) {
            Object[] row = new Object[] {i,
                                         "name_" + i,
                                         i,
                                         new Double(i)};
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
        int allowELT = 0;
        try
        {
            client.callProcedure("@LoadMultipartitionTable", tableName,
                                 table, allowELT);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("loadTable exception: " + ex.getMessage());
        }
        return results;
    }

    private void loadLargeReplicatedTable(Client client, String tableName,
                                          int itemsPerChunk, int numChunks)
    {
        for (int i = 0; i < numChunks; i++)
        {
            VoltTable repl_table =
                createReplicatedTable(itemsPerChunk, i * itemsPerChunk);
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
                                           (byte)1);
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
            result = client.callProcedure("SaveRestoreSelect", tableName)[0];
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

    /*
     * Also does some basic smoke tests
     * of @SnapshotStatus, @SnapshotScan and @SnapshotDelete
     */
    public void testSnapshotSave() throws Exception
    {
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
                                       TESTNONCE, (byte)1);

        VoltTable statusResults[] = client.callProcedure("@SnapshotStatus");
        assertNotNull(statusResults);
        assertEquals( 2, statusResults.length);
        assertEquals( 7, statusResults[0].getColumnCount());
        assertEquals( 1, statusResults[0].getRowCount());
        assertTrue(statusResults[0].advanceRow());
        assertTrue(TMPDIR.equals(statusResults[0].getString("PATH")));
        assertTrue(TESTNONCE.equals(statusResults[0].getString("NONCE")));
        assertFalse( 0 == statusResults[0].getLong("END_TIME"));

        VoltTable scanResults[] = client.callProcedure("@SnapshotScan", new Object[] { null });
        assertNotNull(scanResults);
        assertEquals( 1, scanResults.length);
        assertEquals( 1, scanResults[0].getColumnCount());
        assertEquals( 1, scanResults[0].getRowCount());
        assertTrue( scanResults[0].advanceRow());
        assertTrue( "ERR_MSG".equals(scanResults[0].getColumnName(0)));

        scanResults = client.callProcedure("@SnapshotScan", "/doesntexist");
        assertNotNull(scanResults);
        assertEquals( 1, scanResults[1].getRowCount());
        assertTrue( scanResults[1].advanceRow());
        assertTrue( "FAILURE".equals(scanResults[1].getString("RESULT")));

        scanResults = client.callProcedure("@SnapshotScan", TMPDIR);
        assertNotNull(scanResults);
        assertEquals( 3, scanResults.length);
        assertEquals( 8, scanResults[0].getColumnCount());
        assertEquals( 1, scanResults[0].getRowCount());
        assertTrue( scanResults[0].advanceRow());
        assertTrue(TMPDIR.equals(scanResults[0].getString("PATH")));
        assertTrue(TESTNONCE.equals(scanResults[0].getString("NONCE")));
        assertTrue("TRUE".equals(scanResults[0].getString("COMPLETE")));

        FilenameFilter cleaner = new FilenameFilter()
        {
            public boolean accept(File dir, String file)
            {
                return file.startsWith(TESTNONCE);
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        tmp_files[0].delete();

        scanResults = client.callProcedure("@SnapshotScan", TMPDIR);
        assertNotNull(scanResults);
        assertEquals( 3, scanResults.length);
        assertEquals( 8, scanResults[0].getColumnCount());
        assertEquals( 1, scanResults[0].getRowCount());
        assertTrue( scanResults[0].advanceRow());
        assertTrue(TMPDIR.equals(scanResults[0].getString("PATH")));
        assertTrue(TESTNONCE.equals(scanResults[0].getString("NONCE")));
        assertTrue("FALSE".equals(scanResults[0].getString("COMPLETE")));
        assertTrue(tmp_files[0].getName().contains(scanResults[0].getString("TABLES_MISSING")));

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
                                           TESTNONCE, (byte)1);
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
                new String[]{TESTNONCE});
        assertNotNull(deleteResults);
        assertEquals( 1, deleteResults.length);
        assertEquals( 8, deleteResults[0].getColumnCount());
        assertEquals( 7, deleteResults[0].getRowCount());
        tmp_files = tmp_dir.listFiles(cleaner);
        assertEquals( 0, tmp_files.length);
    }

    public void testSaveAndRestoreReplicatedTable()
    throws IOException, InterruptedException, ProcCallException
    {
        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;

        Client client = getClient();

        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                 num_replicated_items_per_chunk,
                                 num_replicated_chunks);

        VoltTable[] results = null;
        results = saveTables(client);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

        try
        {
            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE, ALLOWELT);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        checkTable(client, "REPLICATED_TESTER", "RT_ID",
                   num_replicated_items_per_chunk * num_replicated_chunks);

        results = client.callProcedure("@Statistics", "table");

        int foundItem = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString(2).equals("REPLICATED_TESTER"))
            {
                ++foundItem;
                assertEquals((num_replicated_chunks * num_replicated_items_per_chunk),
                        results[0].getLong(4));
            }
        }
        // make sure all sites were loaded
        assertEquals(3, foundItem);
    }

    public void testSaveAndRestorePartitionedTable()
    throws IOException, InterruptedException, ProcCallException
    {
        int num_partitioned_items_per_chunk = 120; // divisible by 3
        int num_partitioned_chunks = 10;
        Client client = getClient();

        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);
        VoltTable[] results = null;
        results = saveTables(client);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE, ALLOWELT);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);

        results = client.callProcedure("@Statistics", "table");

        int foundItem = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString(2).equals("PARTITION_TESTER"))
            {
                ++foundItem;
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 3,
                        results[0].getLong(4));
            }
        }
        // make sure all sites were loaded
        assertEquals(3, foundItem);
    }

    // Test that we fail properly when there are no savefiles available
    public void testRestoreMissingFiles()
    throws IOException, InterruptedException
    {
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();

        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0);
        // make a TPCC warehouse table
        VoltTable partition_table =
            createPartitionedTable(num_partitioned_items, 0);

        loadTable(client, "REPLICATED_TESTER", repl_table);
        loadTable(client, "PARTITION_TESTER", partition_table);
        saveTables(client);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        deleteTestFiles();
        m_config.startUp();

        client = getClient();

        try {
            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE, ALLOWELT);
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
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();
        for (int ii = 0; ii < 100; ii++) {
            VoltTable repl_table = createReplicatedTable(num_replicated_items, 0);
            // make a TPCC warehouse table
            VoltTable partition_table =
                createPartitionedTable(num_partitioned_items, 0);

            loadTable(client, "REPLICATED_TESTER", repl_table);
            loadTable(client, "PARTITION_TESTER", partition_table);
            saveTables(client);

            // Kill and restart all the execution sites.
            m_config.shutDown();
            corruptTestFiles(false);
            m_config.startUp();

            client = getClient();

            VoltTable results[] = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE, ALLOWELT);
            assertNotNull(results);
            deleteTestFiles();
        }
    }

    // Test that a random corruption doesn't mess up the table. Not reproducible but useful for detecting
    // stuff we won't normally find
    public void testCorruptedFilesRandom()
    throws Exception
    {
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        for (int ii = 0; ii < 30; ii++) {
            Client client = getClient();

            VoltTable repl_table = createReplicatedTable(num_replicated_items, 0);
            // make a TPCC warehouse table
            VoltTable partition_table =
                createPartitionedTable(num_partitioned_items, 0);

            loadTable(client, "REPLICATED_TESTER", repl_table);
            loadTable(client, "PARTITION_TESTER", partition_table);
            saveTables(client);

            // Kill and restart all the execution sites.
            m_config.shutDown();
            corruptTestFiles(true);
            m_config.startUp();

            client = getClient();

            VoltTable results[] = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE, ALLOWELT);
            assertNotNull(results);
            deleteTestFiles();
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
                                           TESTNONCE, ALLOWELT);
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

        results = client.callProcedure("@Statistics", "table");

        int foundItem = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString(2).equals("PARTITION_TESTER"))
            {
                ++foundItem;
                assertEquals((num_partitioned_items_per_chunk * num_partitioned_chunks) / 4,
                        results[0].getLong(4));
            }
        }
        // make sure all sites were loaded
        assertEquals(4, foundItem);

        config.revertCompile();
    }

    public void testChangeDDL()
    throws IOException, InterruptedException, ProcCallException
    {
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
                                           TESTNONCE, ALLOWELT);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        // XXX consider adding a check that the newly materialized table is
        // not loaded
        results = client.callProcedure("@Statistics", "table");

        boolean found_gets_created = false;
        while (results[0].advanceRow())
        {
            if (results[0].getString(2).equals("GETS_REMOVED"))
            {
                fail("Table GETS_REMOVED got reloaded");
            }
            if (results[0].getString(2).equals("GETS_CREATED"))
            {
                found_gets_created = true;
            }
        }

        // Check the table which changed columns
        VoltTable[] change_results =
            client.callProcedure("SaveRestoreSelect", "CHANGE_COLUMNS");

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
            client.callProcedure("@SnapshotRestore", TMPDIR,
                                           TESTNONCE, ALLOWELT);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        client.callProcedure("@Statistics", "table");

        VoltTable[] change_results =
            client.callProcedure("SaveRestoreSelect", "CHANGE_TYPES");

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
                                           TESTNONCE, ALLOWELT);
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
    static public Test suite() {
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
