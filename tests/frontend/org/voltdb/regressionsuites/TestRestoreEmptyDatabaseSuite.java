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

import static org.junit.Assert.assertNotEquals;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;

import org.apache.commons.io.FileUtils;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.sysprocs.saverestore.SystemTable;

public class TestRestoreEmptyDatabaseSuite extends SaveRestoreBase {

    private static final int SITE_COUNT = 4;
    private static final int HOST_COUNT = 3;

    static LocalCluster m_nonEmptyConfig;
    static LocalCluster m_emptyConfig;
    static LocalCluster m_emptyCatalogConfig;

    @Override
    public void setUp() throws Exception
    {
        FileUtils.deleteDirectory(new File(TMPDIR));
        File f = new File(TMPDIR);
        f.mkdirs();
        super.setUp();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    protected Object[] convertValsToParams(final int i,
            final Object[] rowdata)
    {
        final Object[] params = new Object[rowdata.length + 1];
        params[0] = i;
        for (int ii=0; ii < rowdata.length; ++ii) {
            params[ii+1] = rowdata[ii];
        }
        return params;
    }


    private static void moveSnapshotFiles(final String nonce, final LocalCluster srcCluster, final LocalCluster destCluster)
    {
        FilenameFilter cleaner = new FilenameFilter()
        {
            @Override
            public boolean accept(File dir, String file)
            {
                return file.startsWith(nonce);
            }
        };

        assertTrue(srcCluster.m_hostCount == destCluster.m_hostCount);
        for (int i=0; i<srcCluster.m_hostCount; i++) {
            File src_dir = new File(srcCluster.getServerSpecificScratchDir(String.valueOf(i)) + TMPDIR);
            String destPath = destCluster.getServerSpecificScratchDir(String.valueOf(i)) + TMPDIR;
            File destDir = new File(destPath);
            if(!destDir.exists()){
                try {
                    FileUtils.forceMkdir(destDir);
                } catch (IOException e) {
                    fail("fail to move snapshot file:" + e.getMessage());
                }
            }
            File[] tmp_files = src_dir.listFiles(cleaner);
            for (File tmp_file : tmp_files)
            {
                File targetFile = new File(destPath + "/" + tmp_file.getName());

                try {
                    Files.move(tmp_file.toPath(), targetFile.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException ex) {
                    fail("fail to move snapshot file:" + ex.getMessage());
                }
            }
        }
    }

    @Test
    public void testEmptySnapshotSaveRestore() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("testEmptySnapshotSaveRestore");
        m_nonEmptyConfig.shutDown();
        m_emptyConfig.startUp();
        setConfig(m_emptyConfig);
        Client client = getClient();

        VoltTable[] results = null;
        results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1)
                .getResults();

        while (results[0].advanceRow()) {
            assertEquals("SUCCESS", results[0].getString("RESULT"));
        }

        // Kill and restart all the execution sites.
        m_emptyConfig.shutDown();
        m_emptyConfig.startUp(false);

        client = getClient();

        try {
            results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();

            assertEquals(getSystemTableRowCount(), results[0].getRowCount());
        } catch (ProcCallException e) {
            System.err.println(Arrays.toString(e.getClientResponse().getResults()));
            throw e;
        }

        System.out.println("finished testEmptySnapshotSaveRestore");
    }

    @Test
    public void testSaveAndRestoreFromEmptyDatabase() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("testSaveAndRestoreFromEmptyDatabase");
        Client client = getClient();

        final Object[] rowdata = TestSQLTypesSuite.m_midValues;
        for (int i = 0; i < 10; i++) {
            final Object[] params = convertValsToParams(i, rowdata);
            client.callProcedure("ALLOW_NULLS.insert", params);
        }
        client.drain();

        VoltTable[] results = null;
        results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1)
                .getResults();

        while (results[0].advanceRow()) {
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        // Kill and restart all the execution sites.
        switchRunningConfig(m_emptyConfig);
        client = getClient();

        try {
            results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();

            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }
            // 6 tables with 12 rows each in the result == 72 + system table rows
            assertEquals(72 + getSystemTableRowCount(), results[0].getRowCount());
        } catch (ProcCallException e) {
            System.err.println(Arrays.toString(e.getClientResponse().getResults()));
            throw e;
        }

        System.out.println("finished testSaveAndRestoreFromEmptyDatabase");
    }

    @Test
    public void testSaveAndRestoreFromNonEmptyDatabase() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("testSaveAndRestoreFromNonEmptyDatabase");
        Client client = getClient();

        final Object[] rowdata = TestSQLTypesSuite.m_midValues;
        for (int i = 0; i < 10; i++) {
            final Object[] params = convertValsToParams(i, rowdata);
            client.callProcedure("ALLOW_NULLS.insert", params);
        }
        client.drain();

        VoltTable[] results = null;
        results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1)
                .getResults();

        while (results[0].advanceRow()) {
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        // Kill and restart all the execution sites.
        switchRunningConfig(m_emptyConfig);

        client = getClient();

        client.callProcedure("@AdHoc", "CREATE TABLE foo (a integer)");

        try {
            results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();

            while (results[0].advanceRow()) {
                assertNotEquals(results[0].getString("ERR_MSG"), "FAILURE", results[0].getString("RESULT"));
            }
            assertEquals(getSystemTableRowCount(), results[0].getRowCount());
        } catch (ProcCallException e) {
            System.err.println(Arrays.toString(e.getClientResponse().getResults()));
            throw e;
        }
        System.out.println("finished testSaveAndRestoreFromNonEmptyDatabase");
    }

    @Test
    public void testSaveAndRestoreFromCatalogDatabase() throws Exception
    {
        if (isValgrind()) {
            return; // snapshot doesn't run in valgrind ENG-4034
        }

        System.out.println("testSaveAndRestoreFromCatalogDatabase");
        Client client = getClient();

        final Object[] rowdata = TestSQLTypesSuite.m_midValues;
        for (int i = 0; i < 10; i++) {
            final Object[] params = convertValsToParams(i, rowdata);
            client.callProcedure("ALLOW_NULLS.insert", params);
        }
        client.drain();

        VoltTable[] results = null;
        results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1)
                .getResults();

        while (results[0].advanceRow()) {
            assertEquals("SUCCESS", results[0].getString("RESULT"));
        }

        // Kill and restart all the execution sites.
        switchRunningConfig(m_emptyCatalogConfig);
        client = getClient();

        try
        {
            results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults();
            fail("Should have thrown ProcCallException");
        }
        catch (ProcCallException ex)
        {
            assertEquals(ex.getMessage(), "Cannot restore catalog from snapshot when schema is set to catalog in the deployment.");
        }

        System.out.println("finished testSaveAndRestoreFromCatalogDatabase");
    }

    /**
     * Switch the running config from {@link #m_nonEmptyConfig} to {@code newConfig}
     *
     * @param newConfig New configuration to be started
     * @throws InterruptedException
     */
    private void switchRunningConfig(LocalCluster newConfig) throws InterruptedException {
        m_nonEmptyConfig.shutDown();
        newConfig.setCallingMethodName(m_methodName);
        newConfig.startUp();

        moveSnapshotFiles(TESTNONCE, m_nonEmptyConfig, newConfig);

        setConfig(newConfig);
    }

    /**
     * @return the number of rows expected in the restore result which are from system tables
     */
    private static int getSystemTableRowCount() {
        // One row for each table for each partition
        return SystemTable.values().length * SITE_COUNT * HOST_COUNT;
    }

    public TestRestoreEmptyDatabaseSuite(final String name) {
        super(name);
    }

    static public junit.framework.Test suite() throws Exception
    {
        final MultiConfigSuiteBuilder builder =
                new MultiConfigSuiteBuilder(TestRestoreEmptyDatabaseSuite.class);

        VoltProjectBuilder project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        project.addSchema(TestSQLTypesSuite.class.getResource("sqltypessuite-ddl.sql"));

        m_nonEmptyConfig = new LocalCluster("non-empty-database.jar", SITE_COUNT, HOST_COUNT, 0, BackendTarget.NATIVE_EE_JNI);
        m_nonEmptyConfig.setHasLocalServer(false);
        m_nonEmptyConfig.setEnableVoltSnapshotPrefix(true);
        boolean compile = m_nonEmptyConfig.compile(project);
        assertTrue(compile);
        builder.addServerConfig(m_nonEmptyConfig, MultiConfigSuiteBuilder.ReuseServer.NEVER);


        m_emptyConfig = new LocalCluster("empty-database.jar", SITE_COUNT, HOST_COUNT, 0, BackendTarget.NATIVE_EE_JNI);
        m_emptyConfig.setHasLocalServer(false);
        m_emptyConfig.setEnableVoltSnapshotPrefix(true);
        project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        compile = m_emptyConfig.compile(project);
        assertTrue(compile);


        m_emptyCatalogConfig = new LocalCluster("empty-catalog-database.jar", SITE_COUNT, HOST_COUNT, 0, BackendTarget.NATIVE_EE_JNI);
        m_emptyCatalogConfig.setHasLocalServer(false);
        m_emptyCatalogConfig.setEnableVoltSnapshotPrefix(true);
        project = new VoltProjectBuilder();
        compile = m_emptyCatalogConfig.compile(project);
        assertTrue(compile);

        return builder;
    }
}
