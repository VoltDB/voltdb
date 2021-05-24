/* This file is part of VoltDB.
 * Copyright (C) 2008-2021 VoltDB Inc.
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

package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.voltdb.utils.SnapshotComparer.STATUS_INVALID_INPUT;
import static org.voltdb.utils.SnapshotComparer.STATUS_OK;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;

public class TestSnapshotComparer extends JUnit4LocalClusterTest {
    protected static final String TMPDIR = "/tmp/Backup";
    protected static final String TESTNONCE = "testnonce";
    private static LocalCluster m_server;
    private static Client m_client;

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        m_server = new LocalCluster("testsnapshotcomparer.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        m_server.setHasLocalServer(false);
        m_server.setOldCli();
        VoltProjectBuilder project = new VoltProjectBuilder();
        project.addLiteralSchema(
                "CREATE TABLE T_SP(A2 VARCHAR(128), A1 INTEGER NOT NULL, A3 VARCHAR(64), A4 VARCHAR(64));" +

                        "CREATE TABLE T_MP(A2 VARCHAR(128), A1 INTEGER NOT NULL, A3 VARCHAR(64), A4 VARCHAR(64));");
        project.addPartitionInfo("T_SP", "A1");
        VoltFile.resetSubrootForThisProcess();
        assertTrue(m_server.compile(project));
        File tempDir = new File(TMPDIR);
        if (!tempDir.exists()) {
            assertTrue(tempDir.mkdirs());
        }
        deleteTestFiles(TESTNONCE);
    }

    public void startCluster() throws Exception {
        m_server.startUp();
        m_client = ClientFactory.createClient();
        for (String s : m_server.getListenerAddresses()) {
            m_client.createConnection(s);
        }
    }

    @After
    public void tearDown() throws InterruptedException {
        if (m_client != null) {
            try {
                m_client.close();
            } catch (InterruptedException e) {
            }
        }
        if (m_server != null) {
            try {
                m_server.shutDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    // simple functional negative test
    // wrong parameter yield failure result
    @Test
    public void testSimpleFaiure() {
        exit.expectSystemExitWithStatus(STATUS_INVALID_INPUT);
        String[] wrongArg = {"--self", "--unknow"};
        SnapshotComparer.main(wrongArg);
    }

    // positive test
    // Cluster with no rejoin/snapshot should always has same row order
    @Test
    public void testSimpleSuccess() throws Exception {
        if (LocalCluster.isMemcheckDefined()) {
            return;
        }
        startCluster();
        exit.expectSystemExitWithStatus(STATUS_OK);
        int expectedLines = 10;
        Random r = new Random(Calendar.getInstance().getTimeInMillis());
        for (int i = 0; i < expectedLines; i++) {
            int id = r.nextInt();
            m_client.callProcedure("T_SP.insert", String.format("Test String %s:%d", "SP", i), id, "blab", "blab");
            m_client.callProcedure("T_MP.insert", String.format("Test String %s:%d", "MP", i), id, "blab", "blab");
        }

        VoltTable[] results = null;
        try {
            results = m_client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, 1).getResults();
        } catch(Exception ex) {
            ex.printStackTrace();
            fail();
        }
        System.out.println(results[0]);

        try {
            results = m_client.callProcedure("@SnapshotScan", TMPDIR).getResults();
        } catch (NoConnectionsException e) {
            e.printStackTrace();
        } catch (Exception ex) {
            fail();
        }
        assertEquals(2, results[0].getRowCount());
        System.out.println(results[0]);
        VoltTable vt = results[0];
        List<String> subdirs = new ArrayList<>();
        while (vt.advanceRow()) {
            subdirs.add(vt.getString("PATH"));
        }

        // start convert to MP snapshot to csv
        String[] localSnapshots = {"--self", "--nonce", TESTNONCE, "--dirs", String.join(",", subdirs)};
        SnapshotComparer.main(localSnapshots);
    }

    protected static void deleteTestFiles(final String nonce) {
        FilenameFilter cleaner = new FilenameFilter() {
            @Override
            public boolean accept(File dir, String file) {
                return file.startsWith(nonce) || file.endsWith(".vpt") || file.endsWith(".digest")
                        || file.endsWith(".tsv") || file.endsWith(".csv") || file.endsWith(".incomplete")
                        || new File(dir, file).isDirectory();
            }
        };

        File tmp_dir = new File(TMPDIR);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        for (File tmp_file : tmp_files) {
            deleteRecursively(tmp_file);
        }
    }

    private static void deleteRecursively(File f) {
        if (f.isDirectory()) {
            for (File f2 : f.listFiles()) {
                deleteRecursively(f2);
            }
            boolean deleted = f.delete();
            if (!deleted) {
                if (!f.exists()) {
                    return;
                }
                System.err.println("Couldn't delete " + f.getPath());
                System.err.println("Remaining files are:");
                for (File f2 : f.listFiles()) {
                    System.err.println("    " + f2.getPath());
                }
                //Recurse until stack overflow trying to delete, y not rite?
                deleteRecursively(f);
            }
        } else {
            boolean deleted = f.delete();
            if (!deleted) {
                if (!f.exists()) {
                    return;
                }
                System.err.println("Couldn't delete " + f.getPath());
            }
            assertTrue(deleted);
        }
    }
}
