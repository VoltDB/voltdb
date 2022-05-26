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

package org.voltdb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Before;
import org.junit.Test;
import org.voltcore.utils.Bits;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.sysprocs.saverestore.SystemTable;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.SnapshotVerifier;

import com.google_voltpatches.common.collect.Sets;

public class TestSnapshotsWithFailures extends JUnit4LocalClusterTest {

    protected static final String TMPDIR = "/tmp/" + System.getProperty("user.name");
    protected static final String TESTNONCE = "testnonce";
    private static final int fillerSize = 515 * 512;
    private static final byte[] fillerBytes = new byte[fillerSize];

    LocalCluster cluster = null;
    Client c = null;

    @Before
    public void setup() throws IOException {
        File tempDir = new File(TMPDIR);
        if (!tempDir.exists()) {
            assertTrue(tempDir.mkdirs());
        }
        deleteTestFiles(TESTNONCE);
        VoltDB.wasCrashCalled = false;
        SnapshotSiteProcessor.ExecutionSitesCurrentlySnapshotting.clear();
        SnapshotSiteProcessor.m_tasksOnSnapshotCompletion.clear();
        org.voltdb.sysprocs.SnapshotRegistry.clear();
    }

    @Test
    public void testTruncationSnapshotWithDataFailure() throws Exception {
        if (!MiscUtils.isPro()) {
            return;
        }

        System.out.println("Starting testTruncationSnapshotWithDataFailure");
        try {
            //4 sph, 3 host count, k = 1
            cluster = createLocalCluster("testRestore.jar", 4, 3, 1);
            c = ClientFactory.createClient();
            c.createConnection("", cluster.port(0));
            verifyPartitionCount(c, 6);
            verifyLiveHostCount(c, 3);

            SnapshotErrorInjectionUtils.failSecondWrite();
            for (int ii = 0; ii < 500; ii++) {
                c.callProcedure("P1.insert", ii, fillerBytes);
            }

            c.drain();
            verifySnapshotStatus(c, "COMMANDLOG", 3, true);
            validateSnapshot(cluster.getServerSpecificRoot("0") + "/command_log_snapshot", null, true);
            assertTrue(VoltDB.wasCrashCalled);
        } finally {
            VoltDB.wasCrashCalled = false;
            cleanup();
        }
    }

    @Test
    public void testTruncationSnapshotWithHeaderFailure() throws Exception {
        if (!MiscUtils.isPro()) {
            return;
        }

        System.out.println("Starting testTruncationSnapshotWithHeaderFailure");
        try {
            //4 sph, 3 host count, k = 1
            cluster = createLocalCluster("testRestore.jar", 4, 3, 1);
            c = ClientFactory.createClient();
            c.createConnection("", cluster.port(0));
            verifyPartitionCount(c, 6);
            verifyLiveHostCount(c, 3);

            SnapshotErrorInjectionUtils.failFirstWrite();
            for (int ii = 0; ii < 500; ii++) {
                c.callProcedure("P1.insert", ii, fillerBytes);
            }

            c.drain();
            verifySnapshotStatus(c, "COMMANDLOG", 3, true);
            validateSnapshot(cluster.getServerSpecificRoot("0") + "/command_log_snapshot", null, false);
            assertTrue(VoltDB.wasCrashCalled);
        } finally {
            VoltDB.wasCrashCalled = false;
            cleanup();
        }
    }

    @Test
    public void testNonTruncationSnapshotWithDataFailure() throws Exception {
        System.out.println("Starting testNonTruncationSnapshotWithDataFailure");
        try {
            //4 sph, 3 host count, k = 1
            cluster = createLocalCluster("testRestore.jar", 4, 3, 1);
            c = ClientFactory.createClient();
            c.createConnection("", cluster.port(0));
            verifyPartitionCount(c, 6);
            verifyLiveHostCount(c, 3);
            SnapshotErrorInjectionUtils.failSecondWrite();
            for (int ii = 0; ii < 2; ii++) {
                c.callProcedure("P1.insert", ii, fillerBytes);
            }

            c.drain();

            //save snapshot
            VoltTable results = c.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1).getResults()[0];
            while (results.advanceRow()) {
                assertEquals("SUCCESS", results.getString("RESULT"));
            }

            verifySnapshotStatus(c, "MANUAL", 3, false);
            validateSnapshot(TMPDIR, TESTNONCE, false);
            assertFalse(VoltDB.wasCrashCalled);
        } finally {
            cleanup();
        }
        deleteTestFiles(TESTNONCE);
    }

    @Test
    public void testNonTruncationSnapshotWithHeaderFailure() throws Exception {
        System.out.println("Starting testNonTruncationSnapshotWithHeaderFailure");
        try {
            //4 sph, 3 host count, k = 1
            cluster = createLocalCluster("testRestore.jar", 4, 3, 1);
            c = ClientFactory.createClient();
            c.createConnection("", cluster.port(0));
            verifyPartitionCount(c, 6);
            verifyLiveHostCount(c, 3);
            SnapshotErrorInjectionUtils.failFirstWrite();
            for (int ii = 0; ii < 2; ii++) {
                c.callProcedure("P1.insert", ii, fillerBytes);
            }

            c.drain();

            //save snapshot
            assertSnapshotFailed(c, c.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1).getResults()[0]);

            verifySnapshotStatus(c, "MANUAL", 3, false);
            validateSnapshot(TMPDIR, TESTNONCE, false);
            assertFalse(VoltDB.wasCrashCalled);
        } finally {
            cleanup();
        }
        deleteTestFiles(TESTNONCE);
    }

    /**
     * Assert that either the result from starting the snapshot failed or the status reports failure
     */
    private void assertSnapshotFailed(Client client, VoltTable snapshotResult)
            throws NoConnectionsException, IOException, ProcCallException {
        boolean failed = true;
        while (snapshotResult.advanceRow()) {
            failed &= snapshotResult.getLong("HOST_ID") != 0 || snapshotResult.getString("RESULT").equals("FAILURE");
        }

        if (!failed) {
            VoltTable results = client.callProcedure("@Statistics", "SNAPSHOTSTATUS", 0).getResults()[0];

            while (results.advanceRow()) {
                assertTrue(results.getLong("HOST_ID") != 0 || results.getString("RESULT").equals("FAILURE"));
            }
        }
    }

    protected void deleteTestFiles(final String nonce) {
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

    private void deleteRecursively(File f) {
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

    private LocalCluster createLocalCluster(String jarName, int sph, int hostCount, int kfactor)
            throws IOException {
        final String schema = "CREATE TABLE P1 (ID BIGINT DEFAULT '0' NOT NULL,"
                + " FILLER VARCHAR(" + fillerSize + " BYTES) NOT NULL,"
                + " PRIMARY KEY (ID)); PARTITION TABLE P1 ON COLUMN ID;";
        LocalCluster cluster = new LocalCluster(jarName, sph, hostCount, kfactor, BackendTarget.NATIVE_EE_JNI);
        cluster.overrideAnyRequestForValgrind();
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema(schema);
        if (MiscUtils.isPro()) {
            builder.configureLogging(true, true, 2, 2, 64);
        }
        cluster.setHasLocalServer(true);
        cluster.setJavaProperty("CL_STARTING_BUFFER_SIZE", Integer.toString(Bits.pageSize()));
        cluster.setJavaProperty("LOG_SEGMENTS", Integer.toString(2));
        boolean success = cluster.compile(builder);
        assertTrue(success);
        cluster.startUp();
        return cluster;
    }

    private void verifyLiveHostCount(Client client, int expectedCount) throws NoConnectionsException, IOException, ProcCallException {
        VoltTable vt = c.callProcedure("@SystemInformation", "overview").getResults()[0];
        Set<Long> hosts = Sets.newHashSet();
        while (vt.advanceRow()) {
            hosts.add(vt.getLong(0));
        }
        assertEquals(expectedCount, hosts.size());
    }

    private void verifyPartitionCount(Client client, int expectedCount)
            throws NoConnectionsException, IOException, ProcCallException {
        long sleep = 100;
        //allow time to get the stats
        final long maxSleep = 1800000;
        int partitionKeyCount;
        while (true) {
            try {
                VoltTable vt = client.callProcedure("@GetPartitionKeys", "INTEGER").getResults()[0];
                partitionKeyCount = vt.getRowCount();
                if (expectedCount == partitionKeyCount) {
                    break;
                }
                try {
                    Thread.sleep(sleep);
                } catch (Exception ignored) {
                }
                if (sleep < maxSleep) {
                    sleep = Math.min(sleep + sleep, maxSleep);
                } else {
                    break;
                }
            } catch (Exception e) {
            }
        }
        assertEquals(expectedCount, partitionKeyCount);
        VoltTable vt = client.callProcedure("@GetHashinatorConfig").getResults()[0];
        assertTrue(vt.advanceRow());
        HashSet<Integer> partitionIds = new HashSet<Integer>();
        ByteBuffer buf = ByteBuffer.wrap(vt.getVarbinary(1));
        int tokens = buf.getInt();
        for (int ii = 0; ii < tokens; ii++) {
            buf.getInt();
            partitionIds.add(buf.getInt());
        }
        assertEquals(expectedCount, partitionIds.size());
    }

    private void cleanup() throws InterruptedException {
        if (c != null) {
            c.close();
        }
        if (cluster != null) {
            cluster.shutDown();
        }
    }

    private void verifySnapshotStatus(Client client, String expectedType, int hostCount, boolean checkVoltDBCrashed)
            throws Exception {
        boolean success = true;
        int cnt;
        VoltTable rslt = null;
        int snapshotFiles = 0;
        // For command log snapshot there is a gather period of 10 seconds between
        // scheduling and actual initiation, so 60 seconds should be a very safe timeout
        // For manual snapshot, most of the time it will get failure on the first try
        // so it's not a problem
        for (cnt = 0; cnt < 600; ++cnt) {
            rslt = client.callProcedure("@Statistics", "SNAPSHOTSTATUS", 0).getResults()[0];
            TreeSet<Long> interestedSnapshotTxnIds = new TreeSet<>();
            long interestedSnapshotTxnId;
            while (rslt.advanceRow()) {
                if (rslt.getString("TYPE").equals(expectedType)) {
                    interestedSnapshotTxnIds.add(rslt.getLong("TXNID"));
                }
            }
            if (expectedType.equals("COMMANDLOG")) {
                // We are only interested in the second truncation snapshot (if there is),
                // the first truncation snapshot is an empty snapshot that always succeeds.
                if (interestedSnapshotTxnIds.size() < 2) {
                    Thread.sleep(100);
                    continue;
                }
                interestedSnapshotTxnIds.pollFirst();
                interestedSnapshotTxnId = interestedSnapshotTxnIds.pollFirst();
            } else {
                // For other types, for now MANUAL, we are only interested in the first one.
                // For MANUAL there will be only one manual snapshot requested.
                if (interestedSnapshotTxnIds.isEmpty()) {
                    Thread.sleep(100);
                    continue;
                }
                interestedSnapshotTxnId = interestedSnapshotTxnIds.pollFirst();
            }
            rslt.resetRowPosition();
            snapshotFiles = 0;
            while (rslt.advanceRow()) {
                if (rslt.getLong("TXNID") == interestedSnapshotTxnId) {
                    snapshotFiles++;
                    success &= rslt.getString("RESULT").equals("SUCCESS");
                }
            }
            if (!success && !(checkVoltDBCrashed && !VoltDB.wasCrashCalled)) {
                break;
            }
            Thread.sleep(100);
        }
        assertTrue(cnt < 600);
        assertEquals(hostCount * (1 + SystemTable.values().length), snapshotFiles);
    }

    private void validateSnapshot(String ssPath, String nonce, boolean checkCorrupted) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream original = System.out;
        try {
            System.setOut(ps);
            List<String> directories = new ArrayList<>();
            directories.add(ssPath);
            Set<String> snapshotNames = new HashSet<>();
            if (nonce != null) {
                snapshotNames.add(nonce);
            }
            SnapshotVerifier.verifySnapshots(directories, snapshotNames);
            ps.flush();
            String reportString = baos.toString("UTF-8");
            // If headers are bad in the table files, SnapshotVerifier will not report the file as corrupt
            if (checkCorrupted) {
                if (nonce == null) {
                    // For truncation snapshots the initial snapshot will be good but the second is bad, but since
                    // we don't know the names, just scan them all and search for any corruption
                    assertTrue(reportString.contains("Snapshot corrupted\n"));
                }
                else {
                    assertTrue(reportString.startsWith("Snapshot corrupted\n"));
                }
            }
        } catch (UnsupportedEncodingException e) {}
          finally {
            System.setOut(original);
        }
    }
}
