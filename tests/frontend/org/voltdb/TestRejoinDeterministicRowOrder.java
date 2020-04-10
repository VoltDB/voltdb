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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import org.junit.contrib.java.lang.system.ExpectedSystemExit;
import org.junit.contrib.java.lang.system.internal.CheckExitCalled;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.SnapshotComparer;
import static org.voltdb.utils.SnapshotComparer.STATUS_OK;

public class TestRejoinDeterministicRowOrder extends RejoinTestBase {
    private static String snapshotDir = "/tmp/voltdb/backup/";
    private static String snapshotNonce = "POST_REJOIN";
    private static String testLongString1 =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + // 80
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + // 160
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + // 240
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + // 320
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + // 400
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" + // 480
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ; // 560
    private static String testLongString2 =
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + // 80
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + // 160
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + // 240
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + // 320
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + // 400
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" + // 480
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb" ; // 560

    static class TableMutator {
        final Client m_client;
        final String m_tableName;
        final Random m_rand = new Random(0);
        final Semaphore m_rateLimit;
        final int m_numTuples;
        final int m_maxStringLen;
//        final ArrayList<Integer> m_serverValues = new ArrayList<Integer>();
        final HashSet<Integer> m_deletedValues = new HashSet<Integer>();

        TableMutator(Client client, String tableName, Semaphore rateLimit,
                int numTuples, int maxStringLen)
                throws InterruptedException, NoConnectionsException, IOException {
            m_client = client;
            m_tableName = tableName;
            m_rateLimit = rateLimit;
            m_numTuples = numTuples;
            m_maxStringLen = maxStringLen;
            m_loadThread.setName("Mutate Table " + m_tableName);

            for (int ii = 0; ii < m_numTuples; ii++) {
                m_rateLimit.acquire();
                int value = m_rand.nextInt(1000000);
                Object[] params = new Object[m_maxStringLen == 0 ? 2 : 3];
                params[0] = ii;
                params[1] = value;
                if (m_maxStringLen > 0) {
                    params[2] = testLongString1.substring(0, m_rand.nextInt(m_maxStringLen));
                }
//                m_serverValues.add(value);
                m_client.callProcedure( new ProcedureCallback() {

                    @Override
                    public void clientCallback(ClientResponse clientResponse)
                            throws Exception {
                        m_rateLimit.release();
                        if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                            System.err.println(clientResponse.getStatusString());
                            return;
                        }
                        if (clientResponse.getResults()[0].asScalarLong() != 1) {
                            System.err.println("Update didn't happen");
                            return;
                        }
                    }

                }, "Insert" + m_tableName, params);
            }
            m_client.drain();
        }

        final AtomicBoolean m_shouldContinue = new AtomicBoolean(true);
        final Thread m_loadThread = new Thread() {
            @Override
            public void run() {
                try {
                    int addedTuples = 0;
                    while (m_shouldContinue.get()) {
                        for (int ii = 0; ii < 1000 && m_shouldContinue.get(); ii++) {
                            m_rateLimit.acquire();
                            final int updateKey = m_rand.nextInt(m_numTuples + addedTuples);
                            if (m_deletedValues.contains(updateKey)) continue;
                            final int action = m_rand.nextInt(4);
                            if (action == 0) {
                                m_deletedValues.add(updateKey);
                                m_client.callProcedure( new ProcedureCallback() {

                                    @Override
                                    public void clientCallback(ClientResponse clientResponse)
                                            throws Exception {
                                        m_rateLimit.release();
                                        if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                                            System.err.println("Delete failed for values ("
                                                + updateKey + ") " + clientResponse.getStatusString());
                                            return;
                                        }
                                        if (clientResponse.getResults()[0].asScalarLong() != 1) {
                                            System.err.println("Delete row count error for values ("
                                                + updateKey + ") ");
                                        }
                                    }

                                }, "Delete" + m_tableName, updateKey);
                            } else if (action == 1) {
                                final int newKey = m_numTuples + addedTuples++;
                                int value = m_rand.nextInt(1000000);
                                Object[] params = new Object[m_maxStringLen == 0 ? 2 : 3];
                                params[0] = newKey;
                                params[1] = value;
                                if (m_maxStringLen > 0) {
                                    params[2] = testLongString1.substring(0, m_rand.nextInt(m_maxStringLen));
                                }
//                                m_serverValues.add(value);
                                m_client.callProcedure( new ProcedureCallback() {

                                    @Override
                                    public void clientCallback(ClientResponse clientResponse)
                                            throws Exception {
                                        m_rateLimit.release();
                                        if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                                            System.err.println(clientResponse.getStatusString());
                                            return;
                                        }
                                        if (clientResponse.getResults()[0].asScalarLong() != 1) {
                                            System.err.println("Insert didn't happen");
                                            return;
                                        }
                                    }

                                }, "Insert" + m_tableName, params);

                            } else {
                                final int updateValue = m_rand.nextInt(1000000);
                                Object[] params = new Object[2];
                                params[1] = updateKey;
                                if (m_maxStringLen > 0) {
                                    params[0] = testLongString2.substring(0, m_rand.nextInt(m_maxStringLen));
                                }
                                else {
                                    params[0] = updateValue;
//                                  m_serverValues.set(updateKey, updateValue);
                                }
                                m_client.callProcedure( new ProcedureCallback() {

                                    @Override
                                    public void clientCallback(ClientResponse clientResponse)
                                            throws Exception {
                                        m_rateLimit.release();
                                        if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                                            System.err.println("Update failed for values ("
                                                + updateKey + "," + updateValue +
                                                ") " + clientResponse.getStatusString());
                                            return;
                                        }
                                        if (clientResponse.getResults()[0].asScalarLong() != 1) {
                                            System.err.println("Update row count error for values ("
                                                + updateKey + "," + updateValue +
                                                ") ");
                                        }
                                    }

                                }, "Update" + m_tableName, params);
                            }
                        }
                    }
                    m_client.drain();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        void stopMutations() {
            m_shouldContinue.set(false);
        }

        void start() {
            m_loadThread.start();
        }

        void join() throws InterruptedException {
            m_loadThread.join();
        }
    }

    static class TableVerifier {
        final Client m_client;

        TableVerifier(Client client)
                throws InterruptedException, NoConnectionsException, IOException {
            m_client = client;
        }

        Thread m_scanThread = new Thread("Snapshot Compare thread") {
            @Override
            public void run() {
                ClientResponse response;
                try {
                    response = m_client.callProcedure("@SnapshotSave", snapshotDir, snapshotNonce, 1);
                    assertEquals(ClientResponse.SUCCESS, response.getStatus());
                    int monitorLoop = 0;
                    VoltTable vt = null;
                    while (monitorLoop < 10) {
                        vt = m_client.callProcedure("@Statistics", "SNAPSHOTSTATUS").getResults()[0];
                        if (vt.getRowCount() == SNAPSHOT_TABLE_COUNT*2) {
                            break;
                        }
                        Thread.sleep(1000);
                        monitorLoop++;
                    }
                    assertEquals(vt.getRowCount(), SNAPSHOT_TABLE_COUNT*2);
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
                try {
                    String[] localSnapshots = {"--self", "--nonce", snapshotNonce, "--dirs", snapshotDir, "--ignoreOrder"};
                    SnapshotComparer.main(localSnapshots);
                    fail();
                }
                catch (CheckExitCalled e) {
                    assert(e.getStatus() == STATUS_OK);
                }
                catch (Exception e) {
                    fail();
                }
            };
        };

        void start() {
            m_scanThread.start();
        }

        void join() throws InterruptedException {
            m_scanThread.join();
        }

    }

    @Rule
    public final ExpectedSystemExit exit = ExpectedSystemExit.none();

    @Before
    public void setUp() throws Exception {
        deleteTestFiles(snapshotNonce);
    }

    //
    // Load some stuff, kill some stuff, rejoin some stuff, update some stuff, rejoin some stuff, drain,
    // verify the updates occurred. Lather, rinse, and repeat.
    //
    @Test
    public void testRejoinWithOrderedRowChecks() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);
        final int numTuples = 500;
        final int numHosts = 2;
        final int kfactor = 1;
        final int sitesPerHost = 1;
        final LocalCluster cluster =
            new LocalCluster(
                    "rejoin.jar",
                    sitesPerHost,
                    numHosts,
                    kfactor,
                    BackendTarget.NATIVE_EE_JNI,
                    LocalCluster.FailureState.ALL_RUNNING,
                    true);
        cluster.setMaxHeap(256);
        cluster.overrideAnyRequestForValgrind();
        if (cluster.isValgrind()) {
            return;
        }

        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);
        final java.util.concurrent.atomic.AtomicBoolean rejoinFailed = new AtomicBoolean(false);

        Thread recoveryThread = new Thread("Recovery thread") {
            @Override
            public void run() {
                int attempts = 0;
                while (true) {
                    if (attempts == 6) {
                        rejoinFailed.set(true);
                        break;
                    }
                    if (cluster.recoverOne(1, 0, "")) {
                        break;
                    }
                    attempts++;
                }
            }
        };

        cluster.startUp();

        Client client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        final Semaphore rateLimit = new Semaphore(25);

        TableMutator loadThread = new TableMutator(client, "Partitioned", rateLimit, numTuples, 0);

        try {
            cluster.killSingleHost(1);

            //
            // This version doesn't work. It causes concurrent failures during the rejoin sysproc
            // that aren't handled correctly
            //
            recoveryThread.start();
            Thread.sleep(2000);
            loadThread.start();
            recoveryThread.join();

            if (rejoinFailed.get()) {
                fail("Exception in killer thread");
            }

            loadThread.stopMutations();
            rateLimit.release();
            loadThread.join();

            TableVerifier verifier = new TableVerifier(client);
            verifier.start();
            verifier.join();
        }
        finally {
            client.close();
            cluster.shutDown();
            exit.expectSystemExitWithStatus(STATUS_OK);
        }
    }

    @Test
    public void testRejoinWithMultipleVarchars() throws Exception {
        VoltProjectBuilder builder = getBuilderForTest();
        builder.setSecurityEnabled(true, true);
        final int numTuples = 1000;
        final int numHosts = 2;
        final int kfactor = 1;
        final int sitesPerHost = 1;
        final LocalCluster cluster =
            new LocalCluster(
                    "rejoin.jar",
                    sitesPerHost,
                    numHosts,
                    kfactor,
                    BackendTarget.NATIVE_EE_JNI,
                    LocalCluster.FailureState.ALL_RUNNING,
                    true);
        cluster.setMaxHeap(256);
        cluster.overrideAnyRequestForValgrind();
        if (cluster.isValgrind()) {
            return;
        }

        boolean success = cluster.compile(builder);
        assertTrue(success);
        MiscUtils.copyFile(builder.getPathToDeployment(), Configuration.getPathToCatalogForTest("rejoin.xml"));
        cluster.setHasLocalServer(false);
        final java.util.concurrent.atomic.AtomicBoolean rejoinFailed = new AtomicBoolean(false);

        Thread recoveryThread = new Thread("Recovery thread") {
            @Override
            public void run() {
                int attempts = 0;
                while (true) {
                    if (attempts == 6) {
                        rejoinFailed.set(true);
                        break;
                    }
                    if (cluster.recoverOne(1, 0, "")) {
                        break;
                    }
                    attempts++;
                }
            }
        };

        cluster.startUp();
        Client client = ClientFactory.createClient(m_cconfig);
        client.createConnection("localhost", cluster.port(0));

        final Semaphore rateLimit = new Semaphore(25);

        TableMutator loadThread = new TableMutator(client, "PartitionedLarge", rateLimit, numTuples, 200);

        try {
            cluster.killSingleHost(1);

            //
            // This version doesn't work. It causes concurrent failures during the rejoin sysproc
            // that aren't handled correctly
            //
            recoveryThread.start();
            Thread.sleep(2000);
            loadThread.start();
            recoveryThread.join();

            if (rejoinFailed.get()) {
                fail("Rejoin failed");
            }

            loadThread.stopMutations();
            rateLimit.release();
            loadThread.join();

            TableVerifier verifier = new TableVerifier(client);
            verifier.start();
            verifier.join();
        }
        finally {
            client.close();
            cluster.shutDown();
            exit.expectSystemExitWithStatus(STATUS_OK);
        }
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

        File tmp_dir = new File(snapshotDir);
        File[] tmp_files = tmp_dir.listFiles(cleaner);
        if (tmp_files != null) {
            for (File tmp_file : tmp_files) {
                deleteRecursively(tmp_file);
            }
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
