/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

import static org.voltdb.regressionsuites.TestSaveRestoreSysprocSuite.checkSnapshotStatus;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltdb.BackendTarget;
import org.voltdb.DefaultSnapshotDataTarget;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.SyncCallback;
import org.voltdb.regressionsuites.TestSaveRestoreSysprocSuite.SnapshotResult;
import org.voltdb_testprocs.regressionsuites.saverestore.CatalogChangeSingleProcessServer;
import org.voltdb_testprocs.regressionsuites.saverestore.SaveRestoreTestProjectBuilder;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures
 */
public class TestIV2BlacklistSaveRestoreSysprocSuite extends RegressionSuite {

    private static final String TMPDIR = "/tmp/" + System.getProperty("user.name");
    private static final String TESTNONCE = "testnonce";
    protected static final String JAR_NAME = "sysproc-threesites.jar";

    public TestIV2BlacklistSaveRestoreSysprocSuite(String name) {
        super(name);
    }

    @Override
    public void setUp() throws Exception
    {
        File tempDir = new File(TMPDIR);
        if (!tempDir.exists()) {
            assertTrue(tempDir.mkdirs());
        }
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

    private void deleteRecursively(File f) {
        if (f.isDirectory()) {
            for (File f2 : f.listFiles()) {
                deleteRecursively(f2);
            }
            assertTrue(f.delete());
        } else  {
            assertTrue(f.delete());
        }
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
                file.endsWith(".csv") ||
                file.endsWith(".incomplete") ||
                new File(dir, file).isDirectory();
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files)
        {
            deleteRecursively(tmp_file);
        }
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

    /*
     * ENG-3177 Test that a CSV snapshot of a replicated table provides status
     * on only one node, the one that actually saved the snapshot.
     */
    public void testReplicatedTableCSVSnapshotStatus()
    throws Exception
    {
        m_config.shutDown();

        int host_count = 2;
        int site_count = 1;
        int k_factor = 0;
        LocalCluster lc = new LocalCluster(JAR_NAME, site_count, host_count, k_factor,
                                           BackendTarget.NATIVE_EE_JNI);
        lc.setHasLocalServer(false);
        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        lc.compile(project);
        lc.startUp();
        String replicatedTableName = "REPLICATED_TESTER";
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                VoltTable repl_table = createReplicatedTable(100, 0, null);
                loadTable(client, replicatedTableName, true, repl_table);
                VoltTable[] results = saveTables(client, TMPDIR, TESTNONCE, true, true);
                assertEquals("Wrong host/site count from @SnapshotSave.",
                             host_count * site_count, results[0].getRowCount());
            }
            finally {
                client.close();
            }

            // Connect to each host and check @SnapshotStatus.
            // Only one host should say it saved the replicated table we're watching.
            int nReplSaved = 0;
            for (int iclient = 0; iclient < host_count; iclient++) {
                client = ClientFactory.createClient();
                client.createConnection(lc.getListenerAddresses().get(iclient));
                try {
                    SnapshotResult[] results =
                            checkSnapshotStatus(client, null, TESTNONCE, null, "SUCCESS", null);
                    for (SnapshotResult result : results) {
                        if (result.table.equals(replicatedTableName)) {
                            nReplSaved++;
                        }
                    }
                }
                finally {
                    client.close();
                }
            }
            assertEquals("Replicated table CSV is not saved on exactly one host.", 1, nReplSaved);
        }
        catch (Exception e) {
            fail(String.format("Caught %s: %s", e.getClass().getName(), e.getMessage()));
        }
        finally {
            lc.shutDown();
        }
    }

    /**
     * Build a list of the tests to be run. Use the regression suite
     * helpers to allow multiple back ends.
     * JUnit magic that uses the regression suite helper classes.
     */
    static public junit.framework.Test suite() {
        VoltServerConfig config = null;

        MultiConfigSuiteBuilder builder =
            new MultiConfigSuiteBuilder(TestIV2BlacklistSaveRestoreSysprocSuite.class);

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
