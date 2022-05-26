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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import com.google_voltpatches.common.collect.Sets;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.zk.ZKUtil;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.FlakyTestRule;
import org.voltdb.SnapshotErrorInjectionUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltTable.ColumnInfo;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.iv2.TxnEgo;
import org.voltdb.sysprocs.SnapshotRestoreResultSet;
import org.voltdb.sysprocs.SnapshotRestoreResultSet.RestoreResultValue;
import org.voltdb.sysprocs.saverestore.SnapshotUtil;
import org.voltdb.sysprocs.saverestore.SystemTable;
import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.SnapshotConverter;
import org.voltdb.utils.SnapshotVerifier;
import org.voltdb.utils.VoltSnapshotFile;

import static org.junit.Assert.assertNotEquals;

/**
 * Test the SnapshotSave and SnapshotRestore system procedures
 */
public class TestSaveRestoreSysprocSuite extends SaveRestoreBase {
    @Rule
    public FlakyTestRule ftRule = new FlakyTestRule();

    private final static VoltLogger LOG = new VoltLogger("CONSOLE");
    private final static int SITE_COUNT = 2;
    private final static int TABLE_COUNT = 11 + SystemTable.values().length; // Must match schema used

    protected int getTableCount() {
        return TABLE_COUNT;
    }

    public TestSaveRestoreSysprocSuite(String name) {
        super(name);
    }

    @Override
    public void tearDown() throws Exception {
        ((CatalogChangeSingleProcessServer) m_config).revertCompile();
        super.tearDown();
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
        java.io.RandomAccessFile raf = null;
        // roll dice until find a non-empty file
        do {
            raf = new java.io.RandomAccessFile( tmp_files[tmpIndex], "rw");
            if (raf.length() != 0) {
                break;
            }
            raf.close();
            tmpIndex = r.nextInt(tmp_files.length);
        } while (true);
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

    static VoltTable createReplicatedTable(int numberOfItems,
            int indexBase,
            Set<String> expectedText) {
        return createReplicatedTable(numberOfItems, indexBase, expectedText, false);
    }

    static VoltTable createReplicatedTable(int numberOfItems,
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

            GeographyPointValue pt = getGeographyPointValue(i);
            GeographyValue gv = getGeographyValue(i);
            Object[] row = new Object[] {i,
                                         stringVal,
                                         i,
                                         new Double(i),
                                         pt,
                                         gv};
            if (expectedText != null) {
                StringBuilder sb = new StringBuilder(64);
                if (generateCSV) {
                    sb.append('"').append(i).append('"').append(delimiter);
                    sb.append(escapedVal).append(delimiter);
                    sb.append('"').append(i).append('"').append(delimiter);
                    sb.append('"').append(new Double(i).toString()).append('"').append(delimiter);
                    sb.append('"').append(pt.toString()).append('"').append(delimiter);
                    sb.append('"').append(gv.toString()).append('"');
                } else {
                    sb.append(i).append(delimiter);
                    sb.append(escapedVal).append(delimiter);
                    sb.append(i).append(delimiter);
                    sb.append(new Double(i).toString()).append(delimiter);
                    sb.append(pt.toString()).append(delimiter);
                    sb.append(gv.toString());
                }
                expectedText.add(sb.toString());
            }
            repl_table.addRow(row);
        }
        return repl_table;
    }

    static VoltTable createPartitionedTable(int numberOfItems,
                                             int indexBase)
    {
        VoltTable partition_table =
                new VoltTable(new ColumnInfo("PT_ID", VoltType.INTEGER),
                              new ColumnInfo("PT_NAME", VoltType.STRING),
                              new ColumnInfo("PT_INTVAL", VoltType.INTEGER),
                              new ColumnInfo("PT_FLOATVAL", VoltType.FLOAT),
                              new ColumnInfo("PT_POINTVAL", VoltType.GEOGRAPHY_POINT),
                              new ColumnInfo("PT_GEOGVAL", VoltType.GEOGRAPHY)
                              );

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

    static VoltTable[] loadTable(Client client, String tableName, boolean replicated,
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
                checkAllResponses(callbacks);
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("loadTable exception: " + ex.getMessage());
        }
        return results;
    }

    static void checkAllResponses(ArrayList<SyncCallback> callbacks) {
        // Preserve argument
        ArrayList<SyncCallback> checkList = new ArrayList<>(callbacks);
        while (!checkList.isEmpty()) {
            ListIterator<SyncCallback> lit = checkList.listIterator();
            while (lit.hasNext()) {
                if (lit.next().checkForResponse()) {
                    lit.remove();
                }
            }
        }
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

    static VoltTable[] saveTablesWithDefaultOptions(Client client, String nonce)
    {
        return saveTables(client, TMPDIR, nonce, null, null, true, false);
    }

    private VoltTable[] saveTablesWithPath(Client client, String nonce, String path) {
        return saveTables(client, path, nonce, null, null, true, false);
    }

    private VoltTable[] saveTablesWithDefaultNonceAndPath(Client client) {
        String defaultParam = "{block:false,format:\"native\"}";
        VoltTable[] results = null;
        try {
            results = client.callProcedure("@SnapshotSave", defaultParam).getResults();
        } catch (Exception e) {
            e.printStackTrace();
            fail("SnapshotSave exception: " + e.getMessage());
        }
        return results;
    }

    public static VoltTable[] saveTables(Client client, String dir, String nonce, String[] tables, String[] skiptables,
            boolean block, boolean csv)
    {
        VoltTable[] results = null;
        try
        {
            // For complete coverage test with JSON and legacy args otherwise.
            if (!csv && tables == null && skiptables == null) {
                results = client.callProcedure("@SnapshotSave", dir, nonce, (byte)(block ? 1 : 0))
                        .getResults();
            } else {
                JSONObject jsObj = new JSONObject();
                try {
                    jsObj.put(SnapshotUtil.JSON_URIPATH, String.format("file://%s", dir));
                    jsObj.put(SnapshotUtil.JSON_NONCE, nonce);
                    if (tables != null) {
                        jsObj.put("tables", tables);
                    }
                    if (skiptables != null) {
                        jsObj.put("skiptables", skiptables);
                    }
                    jsObj.put(SnapshotUtil.JSON_BLOCK, block ? 1 : 0);
                    if (csv) {
                        jsObj.put(SnapshotUtil.JSON_FORMAT, "csv");
                    }
                } catch (JSONException e) {
                    fail("JSON exception" + e.getMessage());
                }
                results = client.callProcedure("@SnapshotSave", jsObj.toString()).getResults();
                System.out.println("SnapshotSave Result: " + results[0].toFormattedString());
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotSave exception: " + ex.getMessage());
        }
        return results;
    }

    private void checkTableButSkipGeoColumns(Client client, String tableName, String orderByCol,
            int expectedRows) throws Exception
    {
        checkTable(client, tableName, orderByCol, expectedRows, false);
    }

    private void checkTable(Client client, String tableName, String orderByCol,
            int expectedRows) throws Exception
    {
        checkTable(client, tableName, orderByCol, expectedRows, true);
    }

    private void checkTable(Client client, String tableName, String orderByCol,
                            int expectedRows, boolean checkGeoColumns) throws Exception
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
            if (checkGeoColumns) {
                assertEquals(getGeographyPointValue(i), result.getGeographyPointValue(4));
                assertEquals(getGeographyValue(i), result.getGeographyValue(5));
            }
            ++i;
        }
    }

    static void validateSnapshot(boolean expectSuccess, String nonce) {
        validateSnapshot(expectSuccess, false, nonce);
    }

    static boolean validateSnapshot(boolean expectSuccess,
            boolean onlyReportSuccess,String nonce) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream original = System.out;
        try {
            System.setOut(ps);
            List<String> directories = new ArrayList<>();
            directories.add(TMPDIR);
            Set<String> snapshotNames = new HashSet<>();
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

    private String getRestoreParamsJSON(boolean useHashinator)
    {
        JSONObject jsObj = new JSONObject();
        try {
            jsObj.put(SnapshotUtil.JSON_PATH, TMPDIR);
            jsObj.put(SnapshotUtil.JSON_NONCE, TESTNONCE);
            jsObj.put(SnapshotUtil.JSON_HASHINATOR, useHashinator);
        } catch (JSONException e) {
            fail("JSON exception" + e.getMessage());
        }
        return jsObj.toString();
    }

    public static void doDupRestore(Client client, String nonce) throws Exception {
        doDupRestore(client, TMPDIR, nonce);
    }

    public static void doDupRestore(Client client, String path, String nonce) throws Exception {
        VoltTable[] results;

        // Now check that doing a restore and logging duplicates works.

        JSONObject jsObj = new JSONObject();
        jsObj.put(SnapshotUtil.JSON_NONCE, nonce);
        jsObj.put(SnapshotUtil.JSON_PATH, path);
        // Set isRecover = true so we won't get errors in restore result.
        jsObj.put(SnapshotUtil.JSON_IS_RECOVER, true);

        results = client.callProcedure("@SnapshotRestore", jsObj.toString()).getResults();

        while (results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                  fail(results[0].getString("ERR_MSG"));
            }
        }

    }


    private boolean checkTableNameAppears(Client client, String[] tables) throws Exception {
        final String UNIQUE_TESTNONCE = TESTNONCE + System.currentTimeMillis();
        VoltTable[] results = saveTables(client, TMPDIR, UNIQUE_TESTNONCE, tables, null, false, false);

        //make sure this snapshot is finished.
        while (true) {
            VoltTable[] status = client.callProcedure("@Statistics", "SNAPSHOTSTATUS").getResults();
            boolean finished = false;
            while (status[0].advanceRow()) {
                if (status[0].getString("NONCE").equals(UNIQUE_TESTNONCE)
                        && status[0].getLong("END_TIME") > status[0].getLong("START_TIME")) {
                    finished = true;
                }
            }
            if (finished) {
                break;
            }
            Thread.sleep(100);
        }

        Set<String> tableSet = new HashSet<>();
        while (results[0].advanceRow()) {
            if (!results[0].getString("TABLE").equals("")) {
                tableSet.add(results[0].getString("TABLE"));
            }
        }
        return tableSet.containsAll(Arrays.asList(tables));
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
                        assertTrue("Did not find expected text: -->" + sb + "<--", expectedText.remove(sb.toString()));
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

    @Test
    public void testRestoreWithDifferentTopology()
            throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testRestoreWithDifferentTopology");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        LocalCluster lc = new LocalCluster(JAR_NAME, 1, 2, 0, BackendTarget.NATIVE_EE_JNI);
        lc.setHasLocalServer(false);
        lc.setEnableVoltSnapshotPrefix(true);

        // Save snapshot for 2 site/host cluster.
        lc.compile(project);
        lc.startUp();
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                VoltTable partition_table = createPartitionedTable(num_partitioned_items, 0);

                loadTable(client, "REPLICATED_TESTER", true, repl_table);
                loadTable(client, "PARTITION_TESTER", false, partition_table);
                saveTablesWithDefaultOptions(client, TESTNONCE);
                validateSnapshot(true, true, TESTNONCE);
            }
            finally {
                client.close();
            }
        }
        finally {
            lc.shutDown();
        }

        // Copy over snapshot data from removed node
        String srcDir = lc.getServerSpecificScratchDir("1") + TMPDIR;
        String destDir = lc.getServerSpecificScratchDir("0") + TMPDIR;
        Path destPath = Paths.get(destDir);
        for (Path p : Files.newDirectoryStream(Paths.get(srcDir), TESTNONCE + "*")) {
            Path dest = destPath.resolve(p.getFileName());
            System.out.println("Copying " + p + " to " + dest);
            Files.copy(p, dest, StandardCopyOption.REPLACE_EXISTING);
        }

        // Restore snapshot to 1 nodes 1 sites/host cluster.
        lc.setHostCount(1);
        lc.compile(project);
        lc.startUp(false);
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                ClientResponse cr;
                try {
                    cr = client.callProcedure("@SnapshotRestore", getRestoreParamsJSON(false));
                } catch (ProcCallException e) {
                    System.err.println(e.toString());
                    cr = e.getClientResponse();
                    System.err.printf("%d '%s' %s\n", cr.getStatus(), cr.getStatusString(),
                            cr.getResults()[0].toString());
                }

                assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
            }
            finally {
                client.close();
            }
        }
        finally {
            lc.shutDown();
        }
    }

    @Test
    public void testRestoreWithGhostPartitionAndJoin()
            throws IOException, InterruptedException, ProcCallException
    {
        if (!MiscUtils.isPro()) {
            return; // this is a pro only test, involves elastic join
        }
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testRestoreWithGhostPartitionAndJoin");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        LocalCluster lc = new LocalCluster(JAR_NAME, 1, 2, 0, BackendTarget.NATIVE_EE_JNI);
        lc.setEnableVoltSnapshotPrefix(true);
        lc.setJavaProperty("ELASTIC_TOTAL_TOKENS", "4");
        // Fails if local server flag is true. Collides with m_config.
        lc.setHasLocalServer(false);

        // Save snapshot for 2 site/host cluster.
        {
            lc.compile(project);
            lc.startUp();
            try {
                Client client = ClientFactory.createClient();
                client.createConnection(lc.getListenerAddresses().get(0));
                try {
                    VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                    VoltTable partition_table = createPartitionedTable(num_partitioned_items, 0);

                    System.out.println("testRestoreWithGhostPartitionAndJoin - Load REPLICATED_TESTER and PARTITION_TESTER");
                    loadTable(client, "REPLICATED_TESTER", true, repl_table);
                    loadTable(client, "PARTITION_TESTER", false, partition_table);
                    System.out.println("testRestoreWithGhostPartitionAndJoin - Save snapshot");
                    saveTablesWithDefaultOptions(client, TESTNONCE);
                    System.out.println("testRestoreWithGhostPartitionAndJoin - Validate snapshot");
                    validateSnapshot(true, true, TESTNONCE);
                }
                finally {
                    client.close();
                }
            }
            finally {
                lc.shutDown();
            }
        }

        System.out.println("testRestoreWithGhostPartitionAndJoin - Copy over snapshot data from removed node");
        //Copy over snapshot data from removed node
        String srcDir = lc.getServerSpecificScratchDir("1") + TMPDIR;
        String destDir = lc.getServerSpecificScratchDir("0") + TMPDIR;
        Path destPath = Paths.get(destDir);
        for (Path p : Files.newDirectoryStream(Paths.get(srcDir), TESTNONCE + "*")) {
            Path dest = destPath.resolve(p.getFileName());
            System.out.println("Copying " + p + " to " + dest);
            Files.copy(p, dest, StandardCopyOption.REPLACE_EXISTING);
        }

        // Restore snapshot to 1 nodes 1 sites/host cluster.
        System.out.println("testRestoreWithGhostPartitionAndJoin - Restore snapshot to 1 nodes 1 sites/host cluster.");
        {
            lc.setHostCount(1);
            lc.compile(project);
            lc.startUp(false);
            try {
                Client client = ClientFactory.createClient();
                client.createConnection(lc.getListenerAddresses().get(0));
                try {
                    ClientResponse cr;
                    try {
                        cr = client.callProcedure("@SnapshotRestore", getRestoreParamsJSON(false));
                    } catch(ProcCallException e) {
                        System.err.println(e.toString());
                        cr = e.getClientResponse();
                        System.err.printf("%d '%s' %s\n", cr.getStatus(), cr.getStatusString(), cr.getResults()[0].toString());
                    }

                    assertTrue(cr.getStatus() == ClientResponse.SUCCESS);

                    System.out.println("testRestoreWithGhostPartitionAndJoin - Join the second node.");
                    // Join the second node
                    lc.joinOne(1);
                    Thread.sleep(1000);

                    for (int ii = 0; ii < Integer.MAX_VALUE; ii++) {
                        cr = client.callProcedure("GetTxnId", ii);
                        if (cr.getStatus() != ClientResponse.SUCCESS) {
                            System.out.println(cr.getStatusString());
                        }
                        Long txnid = Long.parseLong(cr.getAppStatusString());
                        if (TxnEgo.getPartitionId(txnid) == 1) {
                            System.out.println("Found " + TxnEgo.txnIdToString(txnid));
                            long sequence = TxnEgo.getSequence(txnid) - TxnEgo.SEQUENCE_ZERO;
                            //If we don't inherit and ID it ends up being 30
                            assertTrue( 30L != sequence);
                            //If things work and we inherit the id we get something larger
                            assertTrue( sequence > 80L);
                            break;
                        }
                    }
                }
                finally {
                    client.close();
                }
            }
            finally {
                lc.shutDown();
            }
        }
    }

    /*
     * Test that a new set of IV2 transaction ids are generated for "Restored" snapshots. This
     * prevents stale txnIds from being propagated into future snapshots of clusters with different
     * site counts.
     */
    @Test
    @FlakyTestRule.Flaky(description="TestSaveRestoreSysprocSuite.testIgnoreTransactionIdsForRestore, for sub-class TestReplicatedSaveRestoreSysprocSuite")
    public void testIgnoreTransactionIdsForRestore()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testIgnoreTransactionIdsForRestore");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();
        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
        // make a TPCC warehouse table
        VoltTable partition_table =
            createPartitionedTable(num_partitioned_items, 0);

        loadTable(client, "REPLICATED_TESTER", true, repl_table);
        loadTable(client, "PARTITION_TESTER", false, partition_table);

        saveTablesWithDefaultOptions(client, TESTNONCE);
        validateSnapshot(true, TESTNONCE);

        JSONObject digest = SnapshotUtil.CRCCheck(new VoltSnapshotFile(TMPDIR, TESTNONCE + "-host_0.digest"), LOG);
        JSONObject transactionIds = digest.getJSONObject("partitionTransactionIds");
        System.out.println("TRANSACTION IDS: " + transactionIds.toString());
        assertEquals( 4, transactionIds.length());

        Set<Integer> partitions = new HashSet<>();
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
        m_config.startUp(false);

        client = getClient();
        client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();

        saveTables(client, TMPDIR, TESTNONCE + 2, null, null, true, false);

        digest = SnapshotUtil.CRCCheck(new VoltSnapshotFile(TMPDIR, TESTNONCE + "2-host_0.digest"), LOG);
        JSONObject newTransactionIds = digest.getJSONObject("partitionTransactionIds");
        assertEquals(newTransactionIds.length(), 2);

        keys = newTransactionIds.keys();
        while (keys.hasNext()) {
            String partitionId = keys.next();
            final long txnid = newTransactionIds.getLong(partitionId);

            // Transaction IDs are reset on restore so check that they are correct for the partition
            if (partitionId.equals(Integer.toString(MpInitiator.MP_INIT_PID))) {
                assertEquals(TxnEgo.makeZero(MpInitiator.MP_INIT_PID).makeNext().makeNext().getTxnId(), txnid);
            } else if (partitionId.equals("0")) {
                assertEquals(TxnEgo.makeZero(0).makeNext().makeNext().makeNext().getTxnId(), txnid);
            } else {
                // No other partitions should exist
                fail("Partition should not exist " + partitionId);
            }
        }
    }

    /*
     * Test that the original transaction ids are retrieved from the "Recovered" snapshot.
     */
    @Test
    public void testPropagateTransactionIdsForRecover()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testPropagateTransactionIdsForRecover");
        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();
        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
        // make a TPCC warehouse table
        VoltTable partition_table =
            createPartitionedTable(num_partitioned_items, 0);

        loadTable(client, "REPLICATED_TESTER", true, repl_table);
        loadTable(client, "PARTITION_TESTER", false, partition_table);

        saveTablesWithDefaultOptions(client, TESTNONCE);
        validateSnapshot(true, TESTNONCE);

        JSONObject digest = SnapshotUtil.CRCCheck(new VoltSnapshotFile(TMPDIR, TESTNONCE + "-host_0.digest"), LOG);
        JSONObject transactionIds = digest.getJSONObject("partitionTransactionIds");
        System.out.println("TRANSACTION IDS: " + transactionIds.toString());
        assertEquals( 4, transactionIds.length());

        Set<Integer> partitions = new HashSet<>();
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
        m_config.startUp(false);

        client = getClient();
        doDupRestore(client, TESTNONCE);

        saveTables(client, TMPDIR, TESTNONCE + 2, null, null, true, false);

        digest = SnapshotUtil.CRCCheck(new VoltSnapshotFile(TMPDIR, TESTNONCE + "2-host_0.digest"), LOG);
        JSONObject newTransactionIds = digest.getJSONObject("partitionTransactionIds");
        assertEquals(transactionIds.length(), newTransactionIds.length());

        keys = transactionIds.keys();
        while (keys.hasNext()) {
            String partitionId = keys.next();
            final long txnid = newTransactionIds.getLong(partitionId);

            //These should be > than the one from the other snapshot
            //because it picked up where it left off on restore and did more work
            assertTrue(txnid >= transactionIds.getLong(partitionId));
        }
    }

    //
    // Test that a replicated table can be distributed correctly
    //
    @Test
    public void testDistributeReplicatedTable()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testDistributeReplicatedTable");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        LocalCluster lc = new LocalCluster( JAR_NAME, 2, 3, 0, BackendTarget.NATIVE_EE_JNI);
        lc.setHasLocalServer(false);
        lc.setEnableVoltSnapshotPrefix(true);
        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
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
                saveTablesWithDefaultOptions(client, TESTNONCE);

                int deletedFiles = 0;
                // Delete REPLICATED tables files from first 2 nodes.
                for (int i = 0; i < lc.m_hostCount -1; i++) {
                    String scratch = lc.getServerSpecificScratchDir(String.valueOf(i));
                    for (File f : (new File(scratch + TMPDIR).listFiles())) {
                        if (f.getName().startsWith(TESTNONCE + "-REPLICATED")) {
                            assertTrue(f.delete());
                            deletedFiles++;
                        }
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
                 * Test that the cluster doesn't goes down if you do a restore with dups
                 */
                ZooKeeper zk = ZKUtil.getClient(lc.zkInterface(0), lc.zkPort(0), 5000, Sets.<Long>newHashSet());
                doDupRestore(client, TESTNONCE);
                lc.shutDownExternal();
                long start = System.currentTimeMillis();
                while(!lc.areAllNonLocalProcessesDead()) {
                    Thread.sleep(1);
                    long now = System.currentTimeMillis();
                    long delta = now - start;
                    if (delta > 10000) {
                        break;
                    }
                }
                assertTrue(lc.areAllNonLocalProcessesDead());
            } finally {
                client.close();
            }
        } finally {
            lc.shutDown();
        }
    }

    @Test
    public void testQueueUserSnapshot() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Staring testQueueUserSnapshot.");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new HashSet<>();

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
        CountDownLatch latch = new CountDownLatch(1);
        SnapshotErrorInjectionUtils.blockOn(latch);
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
        latch.countDown();

        boolean hadSuccess = false;
        for (int ii = 0; ii < 5; ii++) {
            Thread.sleep(2000);
            hadSuccess = validateSnapshot(true, true, TESTNONCE + "2");
            if (hadSuccess) {
                break;
            }
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
        assertEquals(result.getString("ERR_MSG"), "SNAPSHOT FILE WITH SAME NONCE ALREADY EXISTS");
    }

    //
    // Test specific case where a user snapshot is queued
    // and then fails while queued. It shouldn't block future snapshots
    //
    @Test
    public void testQueueFailedUserSnapshot() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Staring testQueueFailedUserSnapshot.");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new HashSet<>();

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
        CountDownLatch latch = new CountDownLatch(1);
        SnapshotErrorInjectionUtils.blockOn(latch);
        client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)0);

        org.voltdb.SnapshotDaemon.m_userSnapshotRetryInterval = 1;

        //
        // Make sure we can queue a snapshot
        //
        ClientResponse r =
            client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE + "1", (byte)0);
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
        latch.countDown();

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

    @Test
    public void testRestore12Snapshot()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
        validateSnapshot(true, false, TESTNONCE);

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

    @Test
    public void testRestoreFutureSnapshot()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        /*
         * The "future" snapshot here was created by writing to REPLICATED_TESTER and
         * PARTITION_TESTER with the values described above, but was created using a
         * modified VoltDB with the following properties:
         * 1. Snapshots contain a ".nonsense" file
         * 2. DR timestamp is the second hidden column; the first contains a VARCHAR reading "whatever"
         * 3. Digests have an opinion on whether or not they are, in fact, penguins, and DR sequence numbers
         *    have opinions on whether or not they have penguins
         *
         * These modifications should conform fairly well to the ways in which we expect our snapshot
         * format to change in the future
         */
        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Client client = getClient();
        byte snapshotTarBytes[] = new byte[1024 * 1024 * 3];
        InputStream is =
            org.voltdb_testprocs.regressionsuites.saverestore.MatView.class.
            getResource("the_future_snapshot.tar.gz").openConnection().getInputStream();
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
        validateSnapshot(true, true, TESTNONCE);

        byte firstStringBytes[] = new byte[1048576];
        java.util.Arrays.fill(firstStringBytes, (byte)'c');
        byte secondStringBytes[] = new byte[1048564];
        java.util.Arrays.fill(secondStringBytes, (byte)'a');

        client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);

        checkTableButSkipGeoColumns(client, "REPLICATED_TESTER", "RT_ID", num_replicated_items_per_chunk * num_replicated_chunks);
        checkTableButSkipGeoColumns(client, "PARTITION_TESTER", "PT_ID", num_partitioned_items_per_chunk * num_partitioned_chunks);

        /*
         * TODO: Validate the following information when/if APIs are available to validate them:
         *
         * DR timestamps on rows
         * TableTuple(REPLICATED_TESTER) ->(integer::0)(varchar::[6]"name_0"[@4813176893])(integer::0)(double::0) hidden->(varchar::[8]"whatever"[@5002215450])(<NULL>) @5002215424 DR timestamp: 128899524933120
         * TableTuple(REPLICATED_TESTER) ->(integer::1)(varchar::[6]"name_1"[@4813176913])(integer::1)(double::1) hidden->(varchar::[8]"whatever"[@5002215516])(<NULL>) @5002215490 DR timestamp: 128899524933120
         * TableTuple(REPLICATED_TESTER) ->(integer::2)(varchar::[6]"name_2"[@4813176933])(integer::2)(double::2) hidden->(varchar::[8]"whatever"[@5002215582])(<NULL>) @5002215556 DR timestamp: 128899524933120
         * TableTuple(REPLICATED_TESTER) ->(integer::3)(varchar::[6]"name_3"[@4813176953])(integer::3)(double::3) hidden->(varchar::[8]"whatever"[@5002215648])(<NULL>) @5002215622 DR timestamp: 128899524933120
         * TableTuple(REPLICATED_TESTER) ->(integer::4)(varchar::[6]"name_4"[@4813176973])(integer::4)(double::4) hidden->(varchar::[8]"whatever"[@5002215714])(<NULL>) @5002215688 DR timestamp: 128899524933120
         * TableTuple(REPLICATED_TESTER) ->(integer::5)(varchar::[6]"name_5"[@4813176993])(integer::5)(double::5) hidden->(varchar::[8]"whatever"[@5002215780])(<NULL>) @5002215754 DR timestamp: 128899524933120
         * TableTuple(REPLICATED_TESTER) ->(integer::6)(varchar::[6]"name_6"[@4813177013])(integer::6)(double::6) hidden->(varchar::[8]"whatever"[@5002215846])(<NULL>) @5002215820 DR timestamp: 128899524933120
         * TableTuple(REPLICATED_TESTER) ->(integer::7)(varchar::[6]"name_7"[@4813177033])(integer::7)(double::7) hidden->(varchar::[8]"whatever"[@5002215912])(<NULL>) @5002215886 DR timestamp: 128899524933120
         * TableTuple(REPLICATED_TESTER) ->(integer::8)(varchar::[6]"name_8"[@4813177053])(integer::8)(double::8) hidden->(varchar::[8]"whatever"[@5002215978])(<NULL>) @5002215952 DR timestamp: 128899524933120
         * TableTuple(REPLICATED_TESTER) ->(integer::9)(varchar::[6]"name_9"[@4813177073])(integer::9)(double::9) hidden->(varchar::[8]"whatever"[@5002216044])(<NULL>) @5002216018 DR timestamp: 128899524933120
         * TableTuple(PARTITION_TESTER) ->(integer::3)(varchar::[6]"name_3"[@4813178893])(integer::3)(double::3) hidden->(varchar::[8]"whatever"[@5117194266])(<NULL>) @5117194240 DR timestamp: 128899525052416
         * TableTuple(PARTITION_TESTER) ->(integer::7)(varchar::[6]"name_7"[@4813178913])(integer::7)(double::7) hidden->(varchar::[8]"whatever"[@5117194332])(<NULL>) @5117194306 DR timestamp: 128899525052417
         * TableTuple(PARTITION_TESTER) ->(integer::8)(varchar::[6]"name_8"[@4813178933])(integer::8)(double::8) hidden->(varchar::[8]"whatever"[@5117194398])(<NULL>) @5117194372 DR timestamp: 128899525052418
         * TableTuple(PARTITION_TESTER) ->(integer::12)(varchar::[7]"name_12"[@4813178953])(integer::12)(double::12) hidden->(varchar::[8]"whatever"[@5117194464])(<NULL>) @5117194438 DR timestamp: 128899525052419
         * TableTuple(PARTITION_TESTER) ->(integer::15)(varchar::[7]"name_15"[@4813178973])(integer::15)(double::15) hidden->(varchar::[8]"whatever"[@5117194530])(<NULL>) @5117194504 DR timestamp: 128899525053440
         * TableTuple(PARTITION_TESTER) ->(integer::17)(varchar::[7]"name_17"[@4813178993])(integer::17)(double::17) hidden->(varchar::[8]"whatever"[@5117194596])(<NULL>) @5117194570 DR timestamp: 128899525053952
         * TableTuple(PARTITION_TESTER) ->(integer::18)(varchar::[7]"name_18"[@4813179013])(integer::18)(double::18) hidden->(varchar::[8]"whatever"[@5117194662])(<NULL>) @5117194636 DR timestamp: 128899525053953
         * TableTuple(PARTITION_TESTER) ->(integer::21)(varchar::[7]"name_21"[@4813179033])(integer::21)(double::21) hidden->(varchar::[8]"whatever"[@5117194728])(<NULL>) @5117194702 DR timestamp: 128899525053954
         * TableTuple(PARTITION_TESTER) ->(integer::28)(varchar::[7]"name_28"[@4813179053])(integer::28)(double::28) hidden->(varchar::[8]"whatever"[@5117194794])(<NULL>) @5117194768 DR timestamp: 128899525053955
         * TableTuple(PARTITION_TESTER) ->(integer::31)(varchar::[7]"name_31"[@4813179073])(integer::31)(double::31) hidden->(varchar::[8]"whatever"[@5117194860])(<NULL>) @5117194834 DR timestamp: 128899525053956
         *
         * From the digest:
         * p=0
         * sequenceNumber=416
         * p=1
         * sequenceNumber=408
         * p=2
         * sequenceNumber=373
         * p=16383
         * sequenceNumber=9
         */
    }

    @Test
    public void testRestoreWithFailures() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
        validateSnapshot(true, false, TESTNONCE);

        try {
            client.callProcedure("@SnapshotRestore", TMPDIR + "x", TESTNONCE);
        } catch (ProcCallException ex) {
            System.err.println(((ClientResponseImpl) ex.getClientResponse()).toJSONString());
            assertEquals("Restore failed to complete. See response table for additional info.", ex.getMessage());
            VoltTable table = ex.getClientResponse().getResults()[0];
            String tableString = table.toString();
            assertTrue(tableString, table.advanceRow());
            assertTrue(tableString, table.getString(1).contains("No snapshot related digests files found"));
            assertTrue(tableString, table.advanceRow());
            assertTrue(tableString, table.getString(1).contains("does not exist"));
        }
    }

    @Test
    public void testSaveRestoreJumboRows()
    throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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

        StringBuilder sb = new StringBuilder();
        sb.appendCodePoint(0x24B62);
        int length = 1;
        while (length < 524288) {
            String halfString = sb.toString();
            length = halfString.length() * 2;
            sb.append(halfString);
        }
        String thirdString = sb.toString();
        assert(thirdString.length() == 524288);
        String fourthString = sb.substring(524288 - 524282);
        assert(fourthString.length() == 524282);

        results = client.callProcedure("JumboInsertChars", 0, thirdString, fourthString).getResults();

        assertEquals(results.length, 1);
        assertEquals( 1, results[0].asScalarLong());

        results = client.callProcedure("JumboSelectChars", 0).getResults();
        assertEquals(results.length, 1);
        assertTrue(results[0].advanceRow());
        assertTrue(thirdString.equals(results[0].getString(1)));
        assertTrue(fourthString.equals(results[0].getString(2)));

        saveTablesWithDefaultOptions(client, TESTNONCE);
        validateSnapshot(true, TESTNONCE);

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

        results = client.callProcedure("JumboSelectChars", 0).getResults();
        assertEquals(results.length, 1);
        assertTrue(results[0].advanceRow());
        assertTrue(thirdString.equals(results[0].getString(1)));
        assertTrue(fourthString.equals(results[0].getString(2)));
    }

    @Test
    public void testTSVConversion() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Staring testTSVConversion.");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new TreeSet<>();

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

        validateSnapshot(true, TESTNONCE);
        generateAndValidateTextFile( expectedText, false);
    }

    @Test
    public void testCSVConversion() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testCSVConversion");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new TreeSet<>();

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

        validateSnapshot(true, TESTNONCE);
        generateAndValidateTextFile( new TreeSet<>(expectedText), true);

        client.callProcedure("@SnapshotSave",
                "{ uripath:\"file://" + TMPDIR +
                "\", nonce:\"" + TESTNONCE + "\", block:true, format:\"csv\" }");
        File f = new File(TMPDIR + File.separator + TESTNONCE + "-REPLICATED_TESTER" + ".csv");
        long endTime = System.currentTimeMillis() + 10000;
        while (true) {
            if (f.exists()) {
                break;
            }
            Thread.sleep(1000);
            if (System.currentTimeMillis() > endTime) {
                break;
            }
        }
        FileInputStream fis = new FileInputStream(
                TMPDIR + File.separator + TESTNONCE + "-REPLICATED_TESTER" + ".csv");
        validateTextFile(expectedText, true, fis);
    }

    @Test
    public void testBadSnapshotParams() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testBadSnapshotParams");
        Client client = getClient();

        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Set<String> expectedText = new TreeSet<>();

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
                    "{ block: true }");
        } catch (Exception e) {
            threwexception = true;
        }
        assertFalse(threwexception);

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
                    "{ uripath:\"file:///tmp\", nonce:\"MANUAL\", block:true }");
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
        File f = new File(TMPDIR + File.separator + TESTNONCE + "-REPLICATED_TESTER" + ".csv");
        long endTime = System.currentTimeMillis() + 10000;
        while (true) {
            if (f.exists()) {
                break;
            }
            Thread.sleep(1000);
            if (System.currentTimeMillis() > endTime) {
                break;
            }
        }
        FileInputStream fis = new FileInputStream(
                TMPDIR + File.separator + TESTNONCE + "-REPLICATED_TESTER" + ".csv");
        validateTextFile(expectedText, true, fis);
        client.close();
    }

    //
    // Also does some basic smoke tests
    // of @Statistics SnapshotStatus, @SnapshotScan and @SnapshotDelete
    //
    @Test
    @FlakyTestRule.Flaky(description="TestSaveRestoreSysprocSuite.testSnapshotSave, for sub-class TestReplicatedSaveRestoreSysprocSuite")
    public void testSnapshotSave() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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

        ClientResponse cr = client.callProcedure("@SnapshotSave", TMPDIR,
                                       TESTNONCE, (byte)1);
        if (cr.getStatus() != ClientResponse.SUCCESS) {
            System.out.println(cr.getStatusString());
        }
        results = cr.getResults();
        System.out.println(results[0]);


        validateSnapshot(true, TESTNONCE);

        //
        // Check that snapshot status returns a reasonable result
        //
        checkSnapshotStatus(client, TMPDIR, TESTNONCE, null, "SUCCESS", getTableCount());

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
        assertEquals( 10, scanResults[0].getColumnCount());
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
        assertEquals( 10, scanResults[0].getColumnCount());
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
        int expected_entries = 3;

        //Make this failing test debuggable, why do we get the wrong number sometimes?
        if (results[0].getRowCount() != expected_entries) {
            System.out.println(results[0]);
        }
        assertEquals(expected_entries, results[0].getRowCount());

        while (results[0].advanceRow())
        {
            assertEquals(results[0].getString("RESULT"), "SUCCESS");
        }

        // Now, try the save again and verify that we fail (since all the save
        // files will still exist. This will return one entry per table
        // per host
        results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1).getResults();
        assertEquals(getTableCount(), results[0].getRowCount());
        while (results[0].advanceRow())
        {
            if (!tmp_files[0].getName().contains(results[0].getString("TABLE"))) {
                assertEquals(results[0].getString("RESULT"), "FAILURE");
                assertEquals(results[0].getString("ERR_MSG"), "SNAPSHOT FILE WITH SAME NONCE ALREADY EXISTS");
            }
        }

        VoltTable deleteResults[] =
            client.callProcedure(
                "@SnapshotDelete",
                new String[] {TMPDIR},
                new String[]{TESTNONCE}).getResults();
        assertNotNull(deleteResults);
        assertEquals( 1, deleteResults.length);
        assertEquals( 10, deleteResults[0].getColumnCount());
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

        validateSnapshot(false, TESTNONCE);

        results = client.callProcedure("@SnapshotSave",
                "{ uripath:\"file://" + TMPDIR + "\", nonce:\"" + TESTNONCE + "\", block:true, format:\"csv\" }")
                .getResults();
        System.out.println("Created CSV snapshot");
    }

    @Test
    public void testSnapshotSaveNonExistingTable() throws Exception {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }
        System.out.println("Starting testSnapshotSaveNonExistingTable");
        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        Client client = getClient();
        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                num_replicated_items_per_chunk,
                num_replicated_chunks);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                num_partitioned_items_per_chunk,
                num_partitioned_chunks);

        // Test saving a non-existing table
        VoltTable[] results = saveTables(client, TMPDIR, TESTNONCE + "NonExistingTable", new String[]{"DUMMYTABLE"},
                null, false, false);
        assertTrue(results[0].getRowCount() == 1);
        while (results[0].advanceRow()) {
            assertEquals("FAILURE", results[0].getString("RESULT"));
            assertEquals("The following tables were specified to include in the snapshot, but are not present in the database: DUMMYTABLE",
                    results[0].getString("ERR_MSG"));
        }

        // Test saving a mix of existing and non-existing table
        results = saveTables(client, TMPDIR, TESTNONCE + "MixedTable", new String[]{"REPLICATED_TESTER","PARTITION_TESTER","DUMMYTABLE"},
                null, false, false);
        assertTrue(results[0].getRowCount() == 1);
        while (results[0].advanceRow()) {
            assertEquals("FAILURE", results[0].getString("RESULT"));
            assertEquals("The following tables were specified to include in the snapshot, but are not present in the database: DUMMYTABLE",
                    results[0].getString("ERR_MSG"));
        }
    }

    @Test
    public void testPartialSnapshotSave() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testPartialSnapshotSave");
        int num_replicated_items_per_chunk = 100;
        int num_replicated_chunks = 10;
        int num_partitioned_items_per_chunk = 120;
        int num_partitioned_chunks = 10;

        //saving replicated table
        Client client = getClient();
        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                                num_replicated_items_per_chunk,
                                num_replicated_chunks);
        if (!checkTableNameAppears(client, new String[]{"REPLICATED_TESTER"})) {
            fail("Missing replicated table name in snapshot result");
        }
        //A restart is needed to make sure next snapshot in checkTableNameAppears() will not be queued.
        //This is because even if checkTableNameAppears() ensures each snapshot is finished before it returns,
        //a new snapshot may still considers the previous one is running due to thread racing.
        m_config.shutDown();
        m_config.startUp();

        //saving partitioned table
        client = getClient();
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                num_partitioned_items_per_chunk,
                num_partitioned_chunks);
        if (!checkTableNameAppears(client, new String[]{"PARTITION_TESTER"})) {
            fail("Missing partitioned table name in snapshot result");
        }
        m_config.shutDown();
        m_config.startUp();

        //saving both replicated and partitioned table
        client = getClient();
        loadLargeReplicatedTable(client, "REPLICATED_TESTER",
                num_replicated_items_per_chunk,
                num_replicated_chunks);
        loadLargePartitionedTable(client, "PARTITION_TESTER",
                num_partitioned_items_per_chunk,
                num_partitioned_chunks);
        if (!checkTableNameAppears(client, new String[]{"PARTITION_TESTER", "REPLICATED_TESTER"})) {
            fail("Missing replicated or partitioned table name in snapshot result");
        }
    }

    @Test
    public void testIdleOnlineSnapshot() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
        checkSnapshotStatus(client, TMPDIR, TESTNONCE, null, "SUCCESS", getTableCount());

        validateSnapshot(true, TESTNONCE);
    }

    @Test
    @FlakyTestRule.Flaky(description="TestSaveRestoreSysprocSuite.testSaveReplicatedAndRestorePartitionedTable, for sub-class TestReplicatedSaveRestoreSysprocSuite")
    public void testSaveReplicatedAndRestorePartitionedTable()
    throws Exception
    {
        if (isValgrind()) {
            return;
        }

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
        orig_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
        System.out.println("STATS: " + orig_mem.toString());

        VoltTable[] results = null;
        results = saveTablesWithDefaultOptions(client, TESTNONCE);

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
        project.addSchema(
                org.voltdb_testprocs.regressionsuites.saverestore.MatView.class.
                getResource("saverestore-ddl.sql"));
        config.recompile(project);
        m_config.startUp();

        client = getClient();

        try {
            client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);
        } catch (ProcCallException e) {
            fail(e.getClientResponse().getResults()[0].toFormattedString());
        }

        while (results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                fail(results[0].getString("ERR_MSG"));
            }
        }

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

        VoltTable final_mem = null;
        final_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
        System.out.println("STATS: " + final_mem.toString());

        checkTable(client, "REPLICATED_TESTER", "RT_ID", num_replicated_items_per_chunk * num_replicated_chunks);

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
            while (results[0].advanceRow()) {
                if (results[0].getString("TABLE_NAME").equals("REPLICATED_TESTER")) {
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
        validateSnapshot(true, TESTNONCE);
    }

    @Test
    @FlakyTestRule.Flaky(description="TestSaveRestoreSysprocSuite.testSavePartitionedAndRestoreReplicatedTable, for sub-class TestReplicatedSaveRestoreSysprocSuite")
    public void testSavePartitionedAndRestoreReplicatedTable()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testSavePartitionedAndRestoreReplicatedTable");
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
        orig_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
        System.out.println("STATS: " + orig_mem.toString());

        validateSnapshot(false, TESTNONCE);

        results = saveTablesWithDefaultOptions(client, TESTNONCE);

        validateSnapshot(true, TESTNONCE);

        while (results[0].advanceRow()) {
            if (!results[0].getString("RESULT").equals("SUCCESS")) {
                System.out.println(results[0].getString("ERR_MSG"));
            }
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        checkSnapshotStatus(client, TMPDIR, TESTNONCE, null, "SUCCESS", getTableCount());

        // Kill and restart all the execution sites after removing partitioning column
        m_config.shutDown();
        CatalogChangeSingleProcessServer config =
            (CatalogChangeSingleProcessServer) m_config;
        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addDefaultProceduresNoPartitioning();
        project.addSchema(
                org.voltdb_testprocs.regressionsuites.saverestore.MatView.class.
                getResource("saverestore-ddl.sql"));
        assertTrue(config.recompile(project));
        m_config.startUp();

        client = getClient();

        results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();

        while (results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                fail(results[0].getString("ERR_MSG"));
            }
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

        // Test doing a duplicate restore and validate the CSV file containing duplicates
        doDupRestore(client, TESTNONCE);

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
    }

    @Test
    public void testSaveAndRestoreReplicatedTable()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
        orig_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
        System.out.println("STATS: " + orig_mem.toString());

        VoltTable[] results = null;
        results = saveTablesWithDefaultOptions(client, TESTNONCE);
        //test saving results
        while (results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                fail(results[0].getString("ERR_MSG"));
            }
        }

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();

        client = getClient();

        client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

        VoltTable final_mem = null;
        final_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
        System.out.println("STATS: " + final_mem.toString());

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

        validateSnapshot(true, TESTNONCE);
    }

    @Test
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
        long cluster3CreateTime = 0;
        long cluster3InstanceId = 0;
        long cluster4CreateTime = 0;
        long cluster4InstanceId = 0;

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

        SnapshotErrorInjectionUtils.failFirstWrite();
        results = saveTablesWithDefaultOptions(client, TESTNONCE);
        deleteTestFiles(TESTNONCE);

        // Make sure that either the response to the snapshot indicates failure or the status
        boolean failed = true;
        while (results[0].advanceRow()) {
            failed &= results[0].getString("RESULT").equals("FAILURE");
        }

        if (!failed) {
            results = client.callProcedure("@Statistics", "SNAPSHOTSTATUS", 0).getResults();

            while (results[0].advanceRow()) {
                assertEquals("FAILURE", results[0].getString("RESULT"));
            }
        }

        validateSnapshot(false, TESTNONCE);

        results = saveTablesWithDefaultOptions(client, "first");

        validateSnapshot(true, "first");

        while (results[0].advanceRow()) {
            if (!results[0].getString("RESULT").equals("SUCCESS")) {
                System.out.println(results[0].getString("ERR_MSG"));
            }
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        checkSnapshotStatus(client, TMPDIR, "first", null, "SUCCESS", getTableCount() * SITE_COUNT);

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

        results = client.callProcedure("@SnapshotRestore", TMPDIR, "first").getResults();

        while (results[0].advanceRow()) {
            if (results[0].getString("RESULT").equals("FAILURE")) {
                fail(results[0].getString("ERR_MSG"));
            }
        }

        try {
            results = client.callProcedure("@SnapshotRestore", TMPDIR,
                                           "first").getResults();

            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }
            fail("Should have thrown an exception");
        }
        catch (Exception ex) {}

        // Make sure the cluster create time has not been altered by the restore
        assertTrue(VoltDB.instance().getClusterCreateTime() == cluster2CreateTime);

        // Test doing a duplicate restore and validate the CSV file containing duplicates
        doDupRestore(client, "first");

        // Make sure the cluster time has been reset with the recover
        assertTrue(VoltDB.instance().getClusterCreateTime() == cluster1CreateTime);

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

        long tend = System.currentTimeMillis() + 60000;
        long tupleCount = 0;
        while (tupleCount < num_partitioned_items_per_chunk * num_partitioned_chunks) {
            tupleCount = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
                {
                    tupleCount += results[0].getLong("TUPLE_COUNT");
                }
            }
            assertTrue(System.currentTimeMillis() < tend);
        }
        assertEquals(num_partitioned_items_per_chunk * num_partitioned_chunks, tupleCount);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();
        //deleteTestFiles(TESTNONCE);
        cluster3CreateTime = VoltDB.instance().getClusterCreateTime();
        cluster3InstanceId = VoltDB.instance().getHostMessenger().getInstanceId().getTimestamp();
        assertTrue(cluster3CreateTime == cluster3InstanceId);
        assertFalse(cluster1CreateTime == cluster3CreateTime);
        assertFalse(cluster2CreateTime == cluster3CreateTime);

        SnapshotErrorInjectionUtils.failSecondWrite();

        org.voltdb.sysprocs.SnapshotRegistry.clear();
        client = getClient();

        loadLargePartitionedTable(client, "PARTITION_TESTER",
                                  num_partitioned_items_per_chunk,
                                  num_partitioned_chunks);

        results = saveTablesWithDefaultOptions(client, TESTNONCE);

        validateSnapshot(false, TESTNONCE);

        results = client.callProcedure("@Statistics", "SnapshotStatus", 0).getResults();
        boolean hasFailure = false;
        while (results[0].advanceRow())
        {
            if (results[0].getString("NONCE").contains(TESTNONCE))
            {
                hasFailure |= results[0].getString("RESULT").equals("FAILURE");
            }
        }
        assertTrue(hasFailure);

        //deleteTestFiles(TESTNONCE);
        results = saveTablesWithDefaultOptions(client, "second");

        validateSnapshot(true, "second");

        // Kill and restart all the execution sites.
        m_config.shutDown();
        m_config.startUp();
        cluster4CreateTime = VoltDB.instance().getClusterCreateTime();
        cluster4InstanceId = VoltDB.instance().getHostMessenger().getInstanceId().getTimestamp();
        assertTrue(cluster4CreateTime == cluster4InstanceId);
        assertFalse(cluster1CreateTime == cluster4CreateTime);
        assertFalse(cluster2CreateTime == cluster4CreateTime);
        assertFalse(cluster3CreateTime == cluster4CreateTime);

        client = getClient();

        results = client.callProcedure("@SnapshotRestore", TMPDIR, "second").getResults();
        // Make sure the cluster time has been reset with the restore
        assertTrue(VoltDB.instance().getClusterCreateTime() == cluster4CreateTime);

        // hacky, need to sleep long enough so the internal server tick
        // updates the memory stats
        Thread.sleep(1000);

        VoltTable final_mem = null;
        final_mem = client.callProcedure("@Statistics", "memory", 0).getResults()[0];
        System.out.println("STATS: " + final_mem.toString());

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);

        results = client.callProcedure("@Statistics", "table", 0).getResults();

        tend = System.currentTimeMillis() + 60000;
        tupleCount = 0;
        while (tupleCount < num_partitioned_items_per_chunk * num_partitioned_chunks) {
            tupleCount = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
                {
                    tupleCount += results[0].getLong("TUPLE_COUNT");
                }
            }
            assertTrue(System.currentTimeMillis() < tend);
        }
        assertEquals(num_partitioned_items_per_chunk * num_partitioned_chunks, tupleCount);

        // checking SnapshotScan order, second is the most recent nonce so it should be first
        results = client.callProcedure("@SnapshotScan", TMPDIR).getResults();
        String[] nonceSet = {"second", "first"};
        int marker = 0;
        while (results[0].advanceRow())
        {
            if (results[0].getString("NONCE").contains(nonceSet[marker]))
            {
                marker++;
            }
            if (marker == 2) {
                break;
            }
        }
        assertEquals(marker, 2);
        marker = 0;
        while (results[2].advanceRow())
        {
            if (results[2].getString("NAME").contains(nonceSet[marker]))
            {
                marker++;
            }
            if (marker == 2) {
                break;
            }
        }
        assertEquals(marker, 2);
        deleteTestFiles("first");
        deleteTestFiles("second");
    }

    // Test that we fail properly when there are no savefiles available
    @Test
    @FlakyTestRule.Flaky(description="TestSaveRestoreSysprocSuite.testRestoreMissingFiles, for sub-class TestReplicatedSaveRestoreSysprocSuite")
    public void testRestoreMissingFiles()
    throws IOException, InterruptedException
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
        saveTablesWithDefaultOptions(client, TESTNONCE);

        validateSnapshot(true, TESTNONCE);

        // Kill and restart all the execution sites.
        m_config.shutDown();
        deleteTestFiles(TESTNONCE);
        m_config.startUp();

        client = getClient();

        try {
            client.callProcedure("@SnapshotRestore", getRestoreParamsJSON(false));
        }
        catch (ProcCallException e) {
            VoltTable vt[] = e.getClientResponse().getResults();
            assertTrue(e.getMessage().contains("Restore failed to complete"));
            assertTrue(vt.length > 0);
            boolean noDigestsFound = false;
            while (vt[0].advanceRow()) {
                if (vt[0].getString("ERR_MSG").contains("No snapshot related digests files found")) {
                    noDigestsFound = true;
                }
            }
            assertTrue(noDigestsFound);
            return;
        }
        assertTrue(false);
    }

    // Test that we fail properly when the save files are corrupted
    @Test
    public void testCorruptedFiles()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
            VoltTable results[] = saveTablesWithDefaultOptions(client, TESTNONCE);
            Thread.sleep(10000);
            validateSnapshot(true, TESTNONCE);
            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    System.out.println(results[0].getString("ERR_MSG"));
                }
                assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
            }

            corruptTestFiles(r);
            validateSnapshot(false, TESTNONCE);
            releaseClient(client);
            // Kill and restart all the execution sites.
            m_config.shutDown();
            m_config.startUp();

            client = getClient();

            results = null;
            try {
                client.callProcedure("@SnapshotRestore", getRestoreParamsJSON(true));
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

            deleteTestFiles(TESTNONCE);
            releaseClient(client);

            // Kill and restart all the execution sites.
            m_config.shutDown();
            m_config.startUp();
        }
    }

    // Test that a random corruption doesn't mess up the table. Not reproducible but useful for detecting
    // stuff we won't normally find
    @Test
    public void testCorruptedFilesRandom()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
            saveTablesWithDefaultOptions(client, TESTNONCE);
            validateSnapshot(true, TESTNONCE);
            releaseClient(client);
            // Kill and restart all the execution sites.
            m_config.shutDown();
            corruptTestFiles(r);
            validateSnapshot(false, TESTNONCE);
            m_config.startUp();

            client = getClient();

            VoltTable results[] = null;
            try {
                client.callProcedure("@SnapshotRestore", getRestoreParamsJSON(true)).getResults();
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

            deleteTestFiles(TESTNONCE);
            releaseClient(client);

            // Kill and restart all the execution sites.
            m_config.shutDown();
            m_config.startUp();
        }
    }

    @Test
    public void testRestoreMissingPartitionFile()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        Client client = getClient();

        VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
        // make a TPCC warehouse table
        VoltTable partition_table =
            createPartitionedTable(num_partitioned_items, 0);

        loadTable(client, "REPLICATED_TESTER", true, repl_table);
        loadTable(client, "PARTITION_TESTER", false, partition_table);
        saveTablesWithDefaultOptions(client, TESTNONCE);

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

    @Test
    public void testRepartition()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
        results = saveTablesWithDefaultOptions(client, TESTNONCE);
        validateSnapshot(true, TESTNONCE);
        // Kill and restart all the execution sites.
        m_config.shutDown();

        CatalogChangeSingleProcessServer config =
            (CatalogChangeSingleProcessServer) m_config;
        config.recompile(4);

        m_config.startUp();

        client = getClient();

        results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();
        // XXX Should check previous results for success but meh for now

        checkTable(client, "PARTITION_TESTER", "PT_ID",
                   num_partitioned_items_per_chunk * num_partitioned_chunks);
        checkTable(client, "REPLICATED_TESTER", "RT_ID",
                   num_replicated_items_per_chunk * num_replicated_chunks);

        // Spin until the stats look complete
        long tend = System.currentTimeMillis() + 60000;
        long tupleCount = 0;
        while (tupleCount < num_partitioned_items_per_chunk * num_partitioned_chunks) {
            tupleCount = 0;
            results = client.callProcedure("@Statistics", "table", 0).getResults();
            while (results[0].advanceRow())
            {
                if (results[0].getString("TABLE_NAME").equals("PARTITION_TESTER"))
                {
                    tupleCount += results[0].getLong("TUPLE_COUNT");
                }
            }
            assertTrue(System.currentTimeMillis() < tend);
        }
        assertEquals(num_partitioned_items_per_chunk * num_partitioned_chunks, tupleCount);
    }

    @Test
    public void testChangeDDL()
    throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
        results = saveTablesWithDefaultOptions(client, TESTNONCE);
        validateSnapshot(true, TESTNONCE);

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

        results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();

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
    }

    @Test
    public void testGoodChangeAttributeTypes()
    throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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

        saveTablesWithDefaultOptions(client, TESTNONCE);
        validateSnapshot(true, TESTNONCE);

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

        client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE);

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
    }

    @Test
    public void testBadChangeAttributeTypes()
    throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind())
         {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

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
        results = saveTablesWithDefaultOptions(client, TESTNONCE);
        validateSnapshot(true, TESTNONCE);

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
                if (results[0].getString("ERR_MSG").contains("out of range"))
                {
                    type_failure = true;
                }
            }
        }
        assertTrue(type_failure);
    }

    @Test
    public void testRestoreHashinatorWithAddedPartition()
            throws IOException, InterruptedException, ProcCallException
    {
        if (isValgrind())
         {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testRestoreHashinatorWithAddedPartition");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        LocalCluster lc = new LocalCluster(JAR_NAME, 1, 1, 0, BackendTarget.NATIVE_EE_JNI);
        // Fails if local server flag is true. Collides with m_config.
        lc.setHasLocalServer(false);
        lc.setEnableVoltSnapshotPrefix(true);

        // Save snapshot for 1 site/host cluster.
        lc.compile(project);
        lc.startUp();
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                VoltTable partition_table = createPartitionedTable(num_partitioned_items, 0);

                loadTable(client, "REPLICATED_TESTER", true, repl_table);
                loadTable(client, "PARTITION_TESTER", false, partition_table);
                saveTablesWithDefaultOptions(client, TESTNONCE);
                validateSnapshot(true, true, TESTNONCE);
            }
            finally {
                client.close();
            }
        }
        finally {
            lc.shutDown();
        }

        // Restore snapshot to 2 nodes 1 sites/host cluster.
        lc.setHostCount(2);
        lc.compile(project);
        // Init second node.
        lc.initOne(1, false, false);
        lc.startUp(false);
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                ClientResponse cr;
                try {
                    cr = client.callProcedure("@SnapshotRestore", getRestoreParamsJSON(true));
                } catch (ProcCallException e) {
                    System.err.println(e.toString());
                    cr = e.getClientResponse();
                    System.err.printf("%d '%s' %s\n", cr.getStatus(), cr.getStatusString(),
                            cr.getResults()[0].toString());
                }
                assertTrue(cr.getStatus() == ClientResponse.SUCCESS);
                // Poll statistics until they appear.
                while (true) {
                    VoltTable results = client.callProcedure("@Statistics", "table", 0).getResults()[0];

                    if (results != null && results.getRowCount() > 0) {
                        long maxPartitionId = -1;
                        boolean done = true;
                        while (results.advanceRow()) {
                            String tableName = results.getString("TABLE_NAME");
                            long partitionId = results.getLong("PARTITION_ID");
                            long tupleCount = results.getLong("TUPLE_COUNT");
                            maxPartitionId = Math.max(partitionId, maxPartitionId);
                            if (tableName.equals("REPLICATED_TESTER") && tupleCount == 0) {
                                done = false;
                            } else if (tableName.equals("PARTITION_TESTER")) {
                                // The second partition should have no rows.
                                if (partitionId == 0 && tupleCount == 0) {
                                    done = false;
                                }
                                else if (partitionId != 0) {
                                    assertTrue(tupleCount == 0);
                                }
                            }
                        }

                        if (maxPartitionId != 1) {
                            done = false;
                        }

                        if (done) {
                            break;
                        } else {
                            Thread.sleep(1);
                        }
                    }
                }
            }
            finally {
                client.close();
            }
        }
        finally {
            lc.shutDown();
        }
    }

    //
    // Test that system always pick up the latest complete snapshot to recover
    //
    @Test
    public void testRecoverPickupLatestSnapshot()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testNewCliRecoverPickupLatestSnapshot");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        LocalCluster lc = new LocalCluster( JAR_NAME, 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        lc.setHasLocalServer(false);
        // We are going to use snapshots directory for restoring snapshots inside of volt
        lc.setEnableVoltSnapshotPrefix(false);
        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        lc.compile(project);
        lc.startUp();
        String snapshotsPath = getSnapshotPath(lc, 0);
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                // make a TPCC warehouse table
                VoltTable partition_table = createPartitionedTable(num_partitioned_items, 0);

                loadTable(client, "REPLICATED_TESTER", true, repl_table);
                loadTable(client, "PARTITION_TESTER", false, partition_table);
                // now reptable has 1000 rows and parttable has 126 rows
                saveTablesWithPath(client, TESTNONCE, snapshotsPath);

                // Load more data
                VoltTable another_repl_table = createReplicatedTable(num_replicated_items, num_replicated_items, null);
                VoltTable another_partition_table = createPartitionedTable(num_partitioned_items, num_partitioned_items);

                loadTable(client, "REPLICATED_TESTER", true, another_repl_table);
                loadTable(client, "PARTITION_TESTER", false, another_partition_table);
                // now reptable has 2000 rows and parttable has 252 rows
                saveTablesWithPath(client, TESTNONCE + "2", snapshotsPath);
            } finally {
                client.close();
            }

            lc.shutDown();
            boolean clearLocalDataDirectories = false;
            lc.startUp(clearLocalDataDirectories);

            client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                long reptableRows = client.callProcedure("@AdHoc", "select count(*) from REPLICATED_TESTER").getResults()[0].asScalarLong();
                assertEquals(2000, reptableRows);
                long parttableRows = client.callProcedure("@AdHoc", "select count(*) from PARTITION_TESTER").getResults()[0].asScalarLong();
                assertEquals(252, parttableRows);
            } finally {
                client.close();
            }
        } finally {
            lc.shutDown();
        }
    }

    //
    // Test that system always pick up the latest complete snapshot to recover
    //
    @Test
    public void testRecoverFromShutdownSnapshot()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testRecoverFromShutdownSnapshot");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        LocalCluster lc = new LocalCluster( JAR_NAME, 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        lc.setEnableVoltSnapshotPrefix(false);
        lc.setHasLocalServer(false);
        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        lc.compile(project);
        lc.startUp();
        String snapshotsPath = getSnapshotPath(lc, 0);
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                // make a TPCC warehouse table
                VoltTable partition_table = createPartitionedTable(num_partitioned_items, 0);

                loadTable(client, "REPLICATED_TESTER", true, repl_table);
                loadTable(client, "PARTITION_TESTER", false, partition_table);
                // now reptable has 1000 rows and parttable has 126 rows
                saveTablesWithPath(client, TESTNONCE, snapshotsPath);

                // Load more data
                VoltTable another_repl_table = createReplicatedTable(num_replicated_items, num_replicated_items, null);
                VoltTable another_partition_table = createPartitionedTable(num_partitioned_items, num_partitioned_items);

                loadTable(client, "REPLICATED_TESTER", true, another_repl_table);
                loadTable(client, "PARTITION_TESTER", false, another_partition_table);
                // now reptable has 2000 rows and parttable has 252 rows
            } finally {
                client.close();
            }

            // make a terminal snapshot
            Client adminClient = ClientFactory.createClient();
            adminClient.createConnection(lc.getAdminAddress(0));
            try {
                lc.shutdownSave(adminClient);
                lc.waitForNodesToShutdown();
            } finally {
                adminClient.close();
            }
            // recover cluster
            boolean clearLocalDataDirectories = false;
            lc.startUp(clearLocalDataDirectories);

            // make sure cluster use terminal snapshot to recover
            client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                long reptableRows = client.callProcedure("@AdHoc", "select count(*) from REPLICATED_TESTER").getResults()[0].asScalarLong();
                assertEquals(2000, reptableRows);
                long parttableRows = client.callProcedure("@AdHoc", "select count(*) from PARTITION_TESTER").getResults()[0].asScalarLong();
                assertEquals(252, parttableRows);
            } finally {
                client.close();
            }

            // kill and recover again to test if terminal snapshot still there (should be)
            lc.shutDown();
            lc.startUp(false);
            // make sure cluster use terminal snapshot to recover
            client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                long reptableRows = client.callProcedure("@AdHoc", "select count(*) from REPLICATED_TESTER").getResults()[0].asScalarLong();
                assertEquals(2000, reptableRows);
                long parttableRows = client.callProcedure("@AdHoc", "select count(*) from PARTITION_TESTER").getResults()[0].asScalarLong();
                assertEquals(252, parttableRows);

                saveTablesWithPath(client, TESTNONCE + 2, snapshotsPath);
            } finally {
                client.close();
            }

            // ENG-16548 Validate that the digest is the same size for all snapshots
            Path snapshotDir = Paths.get(snapshotsPath);
            long referenceSize = -1;
            for (Path file : Files.newDirectoryStream(snapshotDir, "*.digest")) {
                long size = Files.size(file);
                if (referenceSize < 0) {
                    referenceSize = size;
                } else {
                    assertEquals(referenceSize, size);
                }
            }
            assertNotEquals(-1, referenceSize);

        } finally {
            lc.shutDown();
        }
    }

    //
    // Test that system always pick up the latest complete snapshot to recover,
    // whether it is terminal snapshot or not
    //
    @Test
    public void testRecoverShutdownSnapshotIsNotLatest()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testRecoverShutdownSnapshotIsNotLatest");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        LocalCluster lc = new LocalCluster( JAR_NAME, 2, 1, 0, BackendTarget.NATIVE_EE_JNI);
        lc.setEnableVoltSnapshotPrefix(false);
        lc.setHasLocalServer(false);
        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        lc.compile(project);
        lc.startUp();
        String snapshotsPath = getSnapshotPath(lc, 0);
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                // make a TPCC warehouse table
                VoltTable partition_table = createPartitionedTable(num_partitioned_items, 0);

                loadTable(client, "REPLICATED_TESTER", true, repl_table);
                loadTable(client, "PARTITION_TESTER", false, partition_table);
                // now reptable has 1000 rows and parttable has 126 rows
            } finally {
                client.close();
            }

            // make a terminal snapshot
            Client adminClient = ClientFactory.createClient();
            adminClient.createConnection(lc.getAdminAddress(0));
            try {
                lc.shutdownSave(adminClient);
                lc.waitForNodesToShutdown();
            } finally {
                adminClient.close();
            }
            // recover cluster
            boolean clearLocalDataDirectories = false;
            lc.startUp(clearLocalDataDirectories);

            client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                // Load more data
                VoltTable another_repl_table = createReplicatedTable(num_replicated_items, num_replicated_items, null);
                VoltTable another_partition_table = createPartitionedTable(num_partitioned_items, num_partitioned_items);

                loadTable(client, "REPLICATED_TESTER", true, another_repl_table);
                loadTable(client, "PARTITION_TESTER", false, another_partition_table);
                // now reptable has 2000 rows and parttable has 252 rows
                saveTablesWithPath(client, TESTNONCE + "2", snapshotsPath);
            } finally {
                client.close();
            }

            // Kill the cluster and recover again.
            // Snapshots directory now has an older terminal snapshot and a
            // newer non-terminal snapshot, system should pick up the latest one.
            lc.shutDown();
            lc.startUp(clearLocalDataDirectories);
            client = ClientFactory.createClient();
            try {
                client.createConnection(lc.getListenerAddresses().get(0));
                long reptableRows = client.callProcedure("@AdHoc", "select count(*) from REPLICATED_TESTER")
                        .getResults()[0].asScalarLong();
                assertEquals(2000, reptableRows);
                long parttableRows = client.callProcedure("@AdHoc", "select count(*) from PARTITION_TESTER")
                        .getResults()[0].asScalarLong();
                assertEquals(252, parttableRows);
            } finally {
                client.close();
            }
        } finally {
            lc.shutDown();
        }
    }

    //
    // Test that 2-node cluster with one node missing the latest snapshot,
    // system restarts with the second latest snapshot. This is simulated by
    // write two snapshots to the disk, manually deletes the second snapshot
    // on one node, then do the recover, verify that system picks up the latest
    // snapshot.
    //
    @Test
    public void testMinorityOfClusterMissingSnapshot()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testMinorityOfClusterMissingSnapshot");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        // 2 node k=1 cluster
        LocalCluster lc = new LocalCluster( JAR_NAME, 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        lc.setEnableVoltSnapshotPrefix(false);
        lc.setHasLocalServer(false);
        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        lc.compile(project);
        lc.startUp();
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                // make a TPCC warehouse table
                VoltTable partition_table = createPartitionedTable(num_partitioned_items, 0);

                loadTable(client, "REPLICATED_TESTER", true, repl_table);
                loadTable(client, "PARTITION_TESTER", false, partition_table);
                // now reptable has 1000 rows and parttable has 126 rows
                saveTablesWithDefaultNonceAndPath(client);

                waitForSnapshotToFinish(client);
                // Load more data
                VoltTable another_repl_table = createReplicatedTable(num_replicated_items, num_replicated_items, null);
                VoltTable another_partition_table = createPartitionedTable(num_partitioned_items, num_partitioned_items);

                loadTable(client, "REPLICATED_TESTER", true, another_repl_table);
                loadTable(client, "PARTITION_TESTER", false, another_partition_table);
                // now reptable has 2000 rows and parttable has 252 rows
                Thread.sleep(250);
                saveTablesWithDefaultNonceAndPath(client);
                waitForSnapshotToFinish(client);

                // Delete the second snapshot on node 0
                Pattern pat = Pattern.compile(MAGICNONCE + "(\\d+)-.+");
                File snapshotPath = new File(getSnapshotPath(lc, 0));
                TreeMap<Long, String> snapshotNonces = new TreeMap<>();
                for (File child : snapshotPath.listFiles()) {
                    Matcher matcher = pat.matcher(child.getName());
                    if (matcher.matches()) {
                        String nonce = matcher.group(1);
                        snapshotNonces.put(Long.parseLong(nonce), MAGICNONCE + nonce);
                    }
                }
                String nonce = snapshotNonces.lastEntry().getValue();
                int deleted = 0;
                for (File child : snapshotPath.listFiles()) {
                    if (child.getName().startsWith(nonce)) {
                        if (MiscUtils.deleteRecursively(child)) {
                            deleted++;
                        }
                    }
                }
                assertTrue("We must have some files deleted.", (deleted > 0));
                // Two nodes have different snapshots, system recover should pick up the latest snapshot on node 1

            } finally {
                client.close();
            }

            lc.shutDown();
            boolean clearLocalDataDirectories = false;
            lc.startUp(clearLocalDataDirectories);

            client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                long reptableRows = client.callProcedure("@AdHoc", "select count(*) from REPLICATED_TESTER").getResults()[0].asScalarLong();
                assertEquals(2000, reptableRows);
                long parttableRows = client.callProcedure("@AdHoc", "select count(*) from PARTITION_TESTER").getResults()[0].asScalarLong();
                assertEquals(252, parttableRows);
            } finally {
                client.close();
            }
        } finally {
            lc.shutDown();
        }
    }


    //
    // Test that 3-node cluster with 2 of them missing the latest snapshot,
    // system restarts with the second latest snapshot. This is simulated by
    // write two snapshots to the disk, manually deletes the second snapshot
    // on two nodes, then do the recover, verify that system picks up the first
    // snapshot.
    //
    @Test
    public void testMajorityOfClusterMissingSnapshot()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("Starting testMajorityOfClusterMissingSnapshot");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        // 2 node k=1 cluster
        LocalCluster lc = new LocalCluster( JAR_NAME, 2, 3, 1, BackendTarget.NATIVE_EE_JNI);
        lc.setEnableVoltSnapshotPrefix(false);
        lc.setHasLocalServer(false);
        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();
        lc.compile(project);
        lc.startUp();
        try {
            Client client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(0));
            try {
                VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                // make a TPCC warehouse table
                VoltTable partition_table = createPartitionedTable(num_partitioned_items, 0);

                loadTable(client, "REPLICATED_TESTER", true, repl_table);
                loadTable(client, "PARTITION_TESTER", false, partition_table);
                // now reptable has 1000 rows and parttable has 126 rows
                saveTablesWithDefaultNonceAndPath(client);
                waitForSnapshotToFinish(client);

                // Load more data
                VoltTable another_repl_table = createReplicatedTable(num_replicated_items, num_replicated_items, null);
                VoltTable another_partition_table = createPartitionedTable(num_partitioned_items, num_partitioned_items);

                loadTable(client, "REPLICATED_TESTER", true, another_repl_table);
                loadTable(client, "PARTITION_TESTER", false, another_partition_table);
                // now reptable has 2000 rows and parttable has 252 rows
                Thread.sleep(250);
                saveTablesWithDefaultNonceAndPath(client);
                waitForSnapshotToFinish(client);

                // Delete the second snapshot on node 0
                Pattern pat = Pattern.compile(MAGICNONCE + "(\\d+)-.+");
                File snapshotPath = new File(getSnapshotPath(lc, 0));
                TreeMap<Long, String> snapshotNonces = new TreeMap<>();
                for (File child : snapshotPath.listFiles()) {
                    Matcher matcher = pat.matcher(child.getName());
                    if (matcher.matches()) {
                        String nonce = matcher.group(1);
                        snapshotNonces.put(Long.parseLong(nonce), MAGICNONCE + nonce);
                    }
                }
                String latestNonce = snapshotNonces.lastEntry().getValue();
                for (File child : snapshotPath.listFiles()) {
                    if (child.getName().startsWith(latestNonce)) {
                        MiscUtils.deleteRecursively(child);
                    }
                }
                snapshotPath = new File(getSnapshotPath(lc, 1));
                for (File child : snapshotPath.listFiles()) {
                    if (child.getName().startsWith(latestNonce)) {
                        MiscUtils.deleteRecursively(child);
                    }
                }
                // Three nodes have different snapshots, system recover should pick up the latest snapshot on node 2

            } finally {
                client.close();
            }

            lc.shutDown();
            boolean clearLocalDataDirectories = false;
            lc.startUp(clearLocalDataDirectories);

            client = ClientFactory.createClient();
            client.createConnection(lc.getListenerAddresses().get(2));
            try {
                long reptableRows = client.callProcedure("@AdHoc", "select count(*) from REPLICATED_TESTER").getResults()[0].asScalarLong();
                assertEquals(1000, reptableRows);
                long parttableRows = client.callProcedure("@AdHoc", "select count(*) from PARTITION_TESTER").getResults()[0].asScalarLong();
                assertEquals(126, parttableRows);
            } finally {
                client.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            lc.shutDown();
        }
    }

    @Test
    public void testRestoreResults()
    throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        final int SAVE_HOST_COUNT = 1;
        final int RESTORE_HOST_COUNT = 1;

        System.out.println("Starting testRestoreResults");
        m_config.shutDown();

        int num_replicated_items = 1000;
        int num_partitioned_items = 126;

        SaveRestoreTestProjectBuilder project = new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();

        LocalCluster lc = new LocalCluster(JAR_NAME, SITE_COUNT, SAVE_HOST_COUNT, 0, BackendTarget.NATIVE_EE_JNI);
        lc.setHasLocalServer(false);
        lc.compile(project);
        lc.startUp();

        // Save snapshot
        {
            try {
                Client client = ClientFactory.createClient();
                client.createConnection(lc.getListenerAddresses().get(0));
                try {
                    VoltTable repl_table = createReplicatedTable(num_replicated_items, 0, null);
                    VoltTable partition_table = createPartitionedTable(num_partitioned_items, 0);

                    loadTable(client, "REPLICATED_TESTER", true, repl_table);
                    loadTable(client, "PARTITION_TESTER", false, partition_table);
                    saveTablesWithDefaultOptions(client, TESTNONCE);
                    validateSnapshot(true, true, TESTNONCE);
                }
                finally {
                    client.close();
                }
            }
            finally {
                lc.shutDown();
            }
        }

        // Restore snapshot and check results.
        {
            lc.setHostCount(RESTORE_HOST_COUNT);
            lc.compile(project);
            lc.startUp(false);
            try {
                Client client = ClientFactory.createClient();
                client.createConnection(lc.getListenerAddresses().get(0));
                try {
                    SnapshotRestoreResultSet results = restoreTablesWithDefaultOptions(client, TESTNONCE);
                    validateRestoreResults(lc.m_siteCount, lc.m_kfactor, results, getTableCount(), true);
                }
                finally {
                    client.close();
                }
            }
            finally {
                lc.shutDown();
            }
        }
    }

    @Test
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
        lc.setEnableVoltSnapshotPrefix(true);
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
                VoltTable[] results = saveTables(client, TMPDIR, TESTNONCE, null, null, true, true);
                assertEquals("Wrong host/site count from @SnapshotSave.",
                             host_count * site_count, results[0].getRowCount());
            }
            finally {
                client.close();
            }

            // Connect to each host and check @Statistics SnapshotStatus.
            // Only one host should say it saved the replicated table we're watching.
            Set<Long> hostIds = new HashSet<>();
            for (int iclient = 0; iclient < host_count; iclient++) {
                client = ClientFactory.createClient();
                client.createConnection(lc.getListenerAddresses().get(iclient));
                try {
                    SnapshotResult[] results =
                            checkSnapshotStatus(client, null, TESTNONCE, null, "SUCCESS", null);
                    for (SnapshotResult result : results) {
                        if (result.table.equals(replicatedTableName)) {
                            hostIds.add(result.hostID);
                        }
                    }
                }
                finally {
                    client.close();
                }
            }
            assertEquals("Replicated table CSV is not saved on exactly one host.", 1, hostIds.size());
        } finally {
            lc.shutDown();
        }
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

        // Execute @Statistics SnapshotStatus to get raw results.
        VoltTable statusResults[] = client.callProcedure("@Statistics", "SnapshotStatus", 0).getResults();
        assertNotNull(statusResults);
        assertEquals( 1, statusResults.length);
        assertEquals( 15, statusResults[0].getColumnCount());

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

    static private SnapshotRestoreResultSet restoreTablesWithDefaultOptions(Client client, String nonce)
    {
        return restoreTables(client, TMPDIR, nonce);
    }

    static private SnapshotRestoreResultSet restoreTables(Client client, String dir, String nonce)
    {
        SnapshotRestoreResultSet results = new SnapshotRestoreResultSet();
        try
        {
            VoltTable[] vt = client.callProcedure("@SnapshotRestore", dir, nonce).getResults();

            while (vt[0].advanceRow()) {
                results.parseRestoreResultRow(vt[0]);
            }
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            results = null;
        }
        return results;
    }

    static private void validateRestoreResults(
            int siteCount,
            int kFactor,
            SnapshotRestoreResultSet results,
            int tableCount,
            boolean expectSuccess)
    {
        if (results == null) {
            assertFalse(expectSuccess);
            return;
        }
        int nfailures = 0;
        Set<String> tablesReceived = new HashSet<>();
        for (Entry<SnapshotRestoreResultSet.RestoreResultKey, RestoreResultValue> entry : results.entrySet()) {
            tablesReceived.add(entry.getKey().m_table);
            if (!entry.getValue().mergeSuccess()) {
                nfailures++;
            }
            if (entry.getKey().m_partitionId != -1) {
                // Each host/partition/table repetition count should match kfactor+1.
                assertEquals(kFactor + 1, entry.getValue().getCount());
            }
            else {
                // Replicated table entries should repeat for each site.
                assertEquals(siteCount, entry.getValue().getCount());
            }
        }
        // Make sure we saw every table.
        assertEquals(tableCount, tablesReceived.size());
        if (nfailures > 0) {
            assertFalse(expectSuccess);
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
                new MultiConfigSuiteBuilder(TestSaveRestoreSysprocSuite.class);

        SaveRestoreTestProjectBuilder project =
            new SaveRestoreTestProjectBuilder();
        project.addAllDefaults();

        config =
            new CatalogChangeSingleProcessServer(JAR_NAME, 3,
                                                 BackendTarget.NATIVE_EE_JNI);
        boolean success = config.compile(project);
        assert(success);
        builder.addServerConfig(config, MultiConfigSuiteBuilder.ReuseServer.NEVER);

        return builder;
    }

    static void waitForSnapshotToFinish(Client client) throws IOException, ProcCallException, InterruptedException {
        for (int i = 0; i < 20; ++i) {
            if (isSnapshotFinished(client)) {
                return;
            }
            Thread.sleep(100);
        }
        fail("Snapshot did not complete before timeout");
    }

    static boolean isSnapshotFinished(Client client) throws IOException, ProcCallException {
        ClientResponse cr = client.callProcedure("@Statistics", "SNAPSHOTSTATUS");
        assertEquals(ClientResponse.SUCCESS, cr.getStatus());
        VoltTable table = cr.getResults()[0];
        while (table.advanceRow()) {
            if (table.getLong("END_TIME") < table.getLong("START_TIME")) {
                return false;
            }
        }
        return true;
    }

    static String getSnapshotPath(LocalCluster cluster, int hostId) {
        return cluster.getServerSpecificRoot(Integer.toString(hostId)) + File.separator + "snapshots";
    }
}
