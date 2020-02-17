/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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
import java.nio.file.Files;

import org.apache.commons.io.FileUtils;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.VoltFile;

public class TestRestoreEmptyDatabaseSuite extends SaveRestoreBase {

    static LocalCluster m_nonEmptyConfig;
    static LocalCluster m_emptyConfig;
    static LocalCluster m_emptyCatalogConfig;

    @Override
    public void setUp() throws Exception
    {
        VoltFile.recursivelyDelete(new File(TMPDIR));
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
        for (int ii=0; ii < rowdata.length; ++ii)
            params[ii+1] = rowdata[ii];
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
            File src_dir = new File(srcCluster.getSubRoots().get(i).getAbsolutePath() + TMPDIR);
            String destPath = destCluster.getSubRoots().get(i).getAbsolutePath() + TMPDIR;
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

    public void testEmptySnapshotSaveRestore() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("testEmptySnapshotSaveRestore");
        m_nonEmptyConfig.shutDown();
        m_emptyConfig.startUp();
        setConfig(m_emptyConfig);
        Client client = getClient();

        VoltTable[] results = null;
        results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1)
                .getResults();

        while (results[0].advanceRow()) {
            assertTrue(results[0].getString("RESULT").equals("SUCCESS"));
        }

        // Kill and restart all the execution sites.
        m_emptyConfig.shutDown();
        m_emptyConfig.startUp(false);

        client = getClient();

        try
        {
            String necPath = m_emptyConfig.getSubRoots().get(0).getAbsolutePath();
            results = client.callProcedure("@SnapshotRestore", necPath + TMPDIR,
                    TESTNONCE).getResults();

            assertEquals(results[0].getRowCount(), 0);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        deleteTestFiles(TESTNONCE);
        System.out.println("finished testEmptySnapshotSaveRestore");
    }

    public void testSaveAndRestoreFromEmptyDatabase() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("testSaveAndRestoreFromEmptyDatabase");
        setConfig(m_nonEmptyConfig);
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
        m_nonEmptyConfig.shutDown();

        m_emptyConfig.getSubRoots().clear();
        m_emptyConfig.startUp(false);

        moveSnapshotFiles(TESTNONCE, m_nonEmptyConfig, m_emptyConfig);

        setConfig(m_emptyConfig);
        client = getClient();

        try
        {
            String necPath = m_emptyConfig.getSubRoots().get(0).getAbsolutePath();
            results = client.callProcedure("@SnapshotRestore", necPath + TMPDIR,
                    TESTNONCE).getResults();

            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }
            // 6 tables with 12 rows each in the result == 72
            assertEquals(results[0].getRowCount(), 72);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        deleteTestFiles(TESTNONCE);
        System.out.println("finished testSaveAndRestoreFromEmptyDatabase");
    }

    public void testSaveAndRestoreFromNonEmptyDatabase() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("testSaveAndRestoreFromNonEmptyDatabase");
        setConfig(m_nonEmptyConfig);
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
        m_nonEmptyConfig.shutDown();
        m_emptyConfig.startUp(false);

        moveSnapshotFiles(TESTNONCE, m_nonEmptyConfig, m_emptyConfig);

        setConfig(m_emptyConfig);
        client = getClient();

        client.callProcedure("@AdHoc", "CREATE TABLE foo (a integer)");

        try
        {
            String necPath = m_emptyConfig.getSubRoots().get(0).getAbsolutePath();
            results = client.callProcedure("@SnapshotRestore", necPath + TMPDIR,
                    TESTNONCE).getResults();

            while (results[0].advanceRow()) {
                if (results[0].getString("RESULT").equals("FAILURE")) {
                    fail(results[0].getString("ERR_MSG"));
                }
            }
            assertEquals(results[0].getRowCount(), 0);
        }
        catch (Exception ex)
        {
            ex.printStackTrace();
            fail("SnapshotRestore exception: " + ex.getMessage());
        }

        deleteTestFiles(TESTNONCE);
        System.out.println("finished testSaveAndRestoreFromNonEmptyDatabase");
    }

    public void testSaveAndRestoreFromCatalogDatabase() throws Exception
    {
        if (isValgrind()) return; // snapshot doesn't run in valgrind ENG-4034

        System.out.println("testSaveAndRestoreFromCatalogDatabase");
        setConfig(m_nonEmptyConfig);
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
        m_nonEmptyConfig.shutDown();
        m_emptyCatalogConfig.startUp(false);

        moveSnapshotFiles(TESTNONCE, m_nonEmptyConfig, m_emptyCatalogConfig);

        setConfig(m_emptyCatalogConfig);
        client = getClient();

        try
        {
            String necPath = m_emptyCatalogConfig.getSubRoots().get(0).getAbsolutePath();
            results = client.callProcedure("@SnapshotRestore", necPath + TMPDIR,
                    TESTNONCE).getResults();
        }
        catch (ProcCallException ex)
        {
            assertEquals(ex.getMessage(), "Cannot restore catalog from snapshot when schema is set to catalog in the deployment.");
        }

        deleteTestFiles(TESTNONCE);
        System.out.println("finished testSaveAndRestoreFromCatalogDatabase");
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

        m_nonEmptyConfig = new LocalCluster("non-empty-database.jar", 4, 3, 0, BackendTarget.NATIVE_EE_JNI);
        //TODO: Migrate to new cli
        m_nonEmptyConfig.setNewCli(false);
        boolean compile = m_nonEmptyConfig.compile(project);
        assertTrue(compile);
        builder.addServerConfig(m_nonEmptyConfig, MultiConfigSuiteBuilder.ReuseServer.NEVER);


        m_emptyConfig = new LocalCluster("empty-database.jar", 4, 3, 0, BackendTarget.NATIVE_EE_JNI);
        //TODO: Migrate to new cli
        m_emptyConfig.setNewCli(false);
        project = new VoltProjectBuilder();
        project.setUseDDLSchema(true);
        compile = m_emptyConfig.compile(project);
        assertTrue(compile);


        m_emptyCatalogConfig = new LocalCluster("empty-catalog-database.jar", 4, 3, 0, BackendTarget.NATIVE_EE_JNI);
        //TODO: Migrate to new cli
        m_emptyCatalogConfig.setNewCli(false);
        project = new VoltProjectBuilder();
        compile = m_emptyCatalogConfig.compile(project);
        assertTrue(compile);


        return builder;
    }
}
