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
package org.voltdb.dtxn;

import static org.junit.Assert.*;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltTable;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.iv2.DeterminismHash;
import org.voltdb.iv2.DuplicateCounter;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;

public class TestHashMismatches extends JUnit4LocalClusterTest {

    protected static final String TMPDIR = "/tmp/" + System.getProperty("user.name");
    protected static final String TESTNONCE = "testnonce";
    static final String SCHEMA =
            "CREATE TABLE kv (" +
                    "key bigint not null, " +
                    "nondetval bigint not null, " +  // non-deterministic value (host ID)
                    "PRIMARY KEY(key)" +
                    "); " +
                    "PARTITION TABLE kv ON COLUMN key;" +
                    "CREATE INDEX idx_kv ON kv(nondetval);" +
                    "CREATE TABLE mp(key bigint not null, nondetval bigint not null);";

    static final String SCHEMA_UPDATE =
            "CREATE TABLE kv (" +
                    "key bigint not null, " +
                    "nondetval bigint not null, " +
                    "PRIMARY KEY(key)" +
                    "); " +
                    "PARTITION TABLE kv ON COLUMN key;" +
                    "CREATE INDEX idx_kv ON kv(nondetval);" +
                    "CREATE TABLE mp_update(key bigint not null, nondetval bigint not null);";

    LocalCluster server = null;
    Client client;
    final int sitesPerHost = 2;
    final int hostCount = 2;
    final int kfactor = 1;
    static String expectedLogMessage = "Hash mismatch";
    static String expectHashDetectionMessage = "Hash mismatch is detected";
    static String expectedHashNotIncludeMessage = "after " + (DeterminismHash.MAX_HASHES_COUNT / 2 - DeterminismHash.HEADER_OFFSET + 1) + " statements";
    static String expectedSysProcMessage = "is system procedure. Please Contact VoltDB Support.";


    void createCluster(String method) throws IOException {
        createCluster(method, kfactor, hostCount, sitesPerHost);
    }
    void createCluster(String method, int k, int hosts, int sph) throws IOException {
        createCluster(method, k, hosts, sph, false);
    }
    void createCluster(String method, int k, int hostcount, int sph, boolean clEnabled) throws IOException {
        File tempDir = new File(TMPDIR);
        if (!tempDir.exists()) {
            assertTrue(tempDir.mkdirs());
        }
        deleteTestFiles(TESTNONCE);
        System.out.println("*** executing *** " + method);
        try {
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(SCHEMA);
            builder.addProcedure(NonDeterministicSPProc.class, "kv.key: 0");
            builder.addProcedure(NonDeterministic_RO_SP.class, "kv.key: 0");
            builder.addProcedure(Deterministic_RO_SP.class, "kv.key: 0");
            builder.addProcedure(NonDeterministic_RO_MP.class);
            builder.addProcedure(Deterministic_RO_MP.class);
            server = new LocalCluster(method + ".jar", sph, hostcount, k, BackendTarget.NATIVE_EE_JNI);
            server.overrideAnyRequestForValgrind();
            server.setCallingClassName(method);
            if (clEnabled) {
                builder.configureLogging(null, null, true, true, 200, Integer.MAX_VALUE, 300);
            }
            assertTrue("Catalog compilation failed", server.compile(builder));

            server.setHasLocalServer(false);
            server.setEnableVoltSnapshotPrefix(true);
            List<String> logSearchPatterns = new ArrayList<>(1);
            logSearchPatterns.add(expectedLogMessage);
            logSearchPatterns.add(expectHashDetectionMessage);
            logSearchPatterns.add(DuplicateCounter.MISMATCH_RESPONSE_MSG);
            logSearchPatterns.add(DuplicateCounter.MISMATCH_HASH_MSG);
            logSearchPatterns.add(CatalogUtil.MISMATCHED_STATEMENTS);
            logSearchPatterns.add(CatalogUtil.MISMATCHED_PARAMETERS);
            logSearchPatterns.add(expectedHashNotIncludeMessage);
            logSearchPatterns.add(expectedSysProcMessage);
            server.setLogSearchPatterns(logSearchPatterns);

            client = ClientFactory.createClient();
            server.startUp();
            for (String s : server.getListenerAddresses()) {
                client.createConnection(s);
            }
        } catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @After
    public void shutDown() {
        if ( client != null) {
            try {
                client.close();
                client = null;
            } catch (InterruptedException e) {
            }
        }
        if (server != null) {
            try {
                server.shutDown();
            } catch (Exception e) {
                e.printStackTrace();
            }
            server = null;
        }
    }

    /**
     * Do a non-deterministic insertion
     */
    @Test(timeout = 60_000)
    public void testNonDeterministicInsert() throws Exception {
        createCluster("testNonDeterministicInsert");
        try {
            client.callProcedure(
                    "NonDeterministicSPProc",
                    0,
                    0,
                    NonDeterministicSPProc.MISMATCH_INSERTION);
            assertTrue(server.anyHostHasLogMessage(CatalogUtil.MISMATCHED_PARAMETERS));
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch();
                System.out.println("Stopped replicas.");
                insertMoreNormalData(1000, 1100);
            } else {
                fail("Mismatch insertion failed");
            }
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database") ||
                    e.getMessage().contains("Transaction dropped"));
            // make sure every host witnessed the hash mismatch
            if (!MiscUtils.isPro()) {
                assertTrue(server.verifyLogMessage(expectedLogMessage));
            }
        }
    }

    /**
     * Do a non-deterministic insertion followed by a single partition read-only operation.
     * ENG-3288 - Expect non-deterministic read-only queries to succeed.
     */
    @Test(timeout = 60_000)
    public void testNonDeterministic_RO_SP() throws Exception {
        createCluster("testNonDeterministic_RO_SP");
        try {
            insertMoreNormalData(1, 100);
            client.callProcedure("NonDeterministic_RO_SP", 0);
        } catch (ProcCallException e) {
            fail("R/O SP mismatch failed?! " + e.toString());
        }
    }

    /**
     * Test that different whitespace fails the determinism CRC check on SQL
     */
    @Test(timeout = 60_000)
    public void testWhitespaceChanges() throws Exception {
        createCluster("testWhitespaceChanges");
        try {
            client.callProcedure(
                    "NonDeterministicSPProc",
                    0,
                    0,
                    NonDeterministicSPProc.MISMATCH_WHITESPACE_IN_SQL);
            assertTrue(server.anyHostHasLogMessage(CatalogUtil.MISMATCHED_STATEMENTS));
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch();
                System.out.println("Stopped replicas.");
                insertMoreNormalData(10001, 10100);
            } else {
                fail("Whitespace changes not picked up by determinism CRC");
            }
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database") ||
                    e.getMessage().contains("Transaction dropped"));
            // make sure every host witnessed the hash mismatch
            if (!MiscUtils.isPro()) {
                assertTrue(server.verifyLogMessage(expectedLogMessage));
            }
        }
    }

    @Test(timeout = 60_000)
    public void testMultistatementNonDeterministicProc() throws Exception {
        createCluster("testMultistatementNonDeterministicProc");

        try {
            for (int i = 0; i < 10000; i++) {
                client.callProcedure("KV.insert", i, 999);
            }
        } catch (Exception e) {
            fail("Failed to insert data");
            return;
        }

        try {
            client.callProcedure("NonDeterministicSPProc",
                    1234, //not use
                    999,
                    NonDeterministicSPProc.MULTI_STATEMENT_MISMATCH);
            assertTrue(server.anyHostHasLogMessage(expectedHashNotIncludeMessage));
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch();
                System.out.println("Stopped replicas.");
                insertMoreNormalData(10001, 10100);
            } else {
                fail("Multi-statement hash mismatch update failed");
            }
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database") ||
                    e.getMessage().contains("Transaction dropped"));
            // make sure every host witnessed the hash mismatch
            if (!MiscUtils.isPro()) {
                assertTrue(server.verifyLogMessage(expectedLogMessage));
            }
        }
    }

    @Test(timeout = 60_000)
    public void testPartialstatementNonDeterministicProc() throws Exception {
        createCluster("testPartialstatementNonDeterministicProc");
        try {
            for (int i = 0; i < 10000; i++) {
                client.callProcedure("KV.insert", i, 999);
            }
        } catch (Exception e) {
            fail("Failed to insert data");
            return;
        }

        try {
            client.callProcedure("NonDeterministicSPProc",
                    1234, //not use
                    999,
                    NonDeterministicSPProc.PARTIAL_STATEMENT_MISMATCH);
            assertTrue(server.anyHostHasLogMessage(CatalogUtil.MISMATCHED_PARAMETERS));
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch();
                System.out.println("Stopped replicas.");
                insertMoreNormalData(10001, 10100);
            } else {
                fail("Partial-statement hash mismatch update failed");
            }
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database") ||
                    e.getMessage().contains("Transaction dropped"));
            // make sure every host witnessed the hash mismatch
            if (!MiscUtils.isPro()) {
                assertTrue(server.verifyLogMessage(expectedLogMessage));
            }
        }
    }

    @Test(timeout = 60_000)
    public void testBuggyNonDeterministicProc() throws Exception {
        createCluster("testBuggyNonDeterministicProc");
        try {
            client.callProcedure(
                    "NonDeterministicSPProc",
                    1234,
                    999,
                    NonDeterministicSPProc.TXN_ABORT);
            assertTrue(server.anyHostHasLogMessage(DuplicateCounter.MISMATCH_RESPONSE_MSG));
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch();
                System.out.println("Stopped replicas.");
                insertMoreNormalData(10001, 10100);
            } else {
                fail("testBuggyNonDeterministicProc failed");
            }
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database") ||
                    e.getMessage().contains("Crash deliberately"));
            // make sure every host witnessed the hash mismatch
            if (!MiscUtils.isPro()) {
                assertTrue(server.verifyLogMessage(expectedLogMessage));
            }
        }
    }

    @Test(timeout = 60_000)
    public void testSnapshotSaveRestoreWithoutCL() throws Exception {
        if (!MiscUtils.isPro() || LocalCluster.isMemcheckDefined()) {
            return;
        }
        deleteTestFiles(TESTNONCE);
        createCluster("testSnapshotSaveRestoreWithoutCL", 2, 3, 18);
        VoltTable vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
        System.out.println(vt.toFormattedString());
        try {
            for (int i = 5000; i < 5091; i++) {
                client.callProcedure(
                        "NonDeterministicSPProc",
                        i,
                        i,
                        NonDeterministicSPProc.MISMATCH_INSERTION);
            }
            vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            for (int i = 0; i < 200; i++) {
                client.callProcedure("mp.insert",i,i);
            }
            verifyTopologyAfterHashMismatch();
            System.out.println("Stopped replicas.");
            insertMoreNormalData(10001, 10092);
            for (int i = 200; i < 300; i++) {
                client.callProcedure("mp.insert",i,i);
            }
            client.drain();
            System.out.println("Saving snapshot...");
            ClientResponse resp  = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1);
            vt = resp.getResults()[0];
            System.out.println(vt.toFormattedString());
            while (vt.advanceRow()) {
                assertTrue(vt.getString("RESULT").equals("SUCCESS"));
            }

            vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            long rows = vt.asScalarLong();
            vt = client.callProcedure("@AdHoc", "select count(*) from MP").getResults()[0];
            long mprows = vt.asScalarLong();
            client.callProcedure("@AdHoc", "delete from KV");
            client.callProcedure("@AdHoc", "delete from MP");

            System.out.println("Saved snapshot with " + rows + ", reloading snapshot...");
            vt = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults()[0];
            System.out.println(vt.toFormattedString());
            while (vt.advanceRow()) {
                if (vt.getString("RESULT").equals("FAILURE")) {
                    fail(vt.getString("ERR_MSG"));
                }
            }
            System.out.println("snapshot reloaded");
            vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            assert(rows == vt.asScalarLong());
            vt = client.callProcedure("@AdHoc", "select count(*) from MP").getResults()[0];
            assert(mprows == vt.asScalarLong());
        } catch (ProcCallException e) {
            fail("testSnapshotSaveRestoreWithoutCL failed");
        }
    }

    @Test(timeout = 60_000)
    public void testUacAfterHashMismatch() throws Exception {
        if (!MiscUtils.isPro()) {
            return;
        }
        createCluster("testUacAfterHashMismatch", 2, 3, 18);
        try {
            for (int i = 5000; i < 5091; i++) {
                client.callProcedure(
                        "NonDeterministicSPProc",
                        i,
                        i,
                        NonDeterministicSPProc.MISMATCH_INSERTION);
            }
            for (int i = 0; i < 200; i++) {
                client.callProcedure("mp.insert",i,i);
            }
            verifyTopologyAfterHashMismatch();
            System.out.println("Stopped replicas.");
            insertMoreNormalData(10001, 10092);
            for (int i = 200; i < 300; i++) {
                client.callProcedure("mp.insert",i,i);
            }
            client.drain();
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(SCHEMA);
            builder.addProcedure(NonDeterministicSPProc.class, "kv.key: 0");
            builder.addProcedure(NonDeterministic_RO_SP.class, "kv.key: 0");
            builder.addProcedure(Deterministic_RO_SP.class, "kv.key: 0");
            builder.addProcedure(NonDeterministic_RO_MP.class);
            LocalCluster localserver = new LocalCluster("update.jar", 18, 3, 2, BackendTarget.NATIVE_EE_JNI);
            boolean compile = localserver.compile(builder);
            assertTrue(compile);

            String newCatalogURL = Configuration.getPathToCatalogForTest("update.jar");
            byte[] catBytes2 = MiscUtils.fileToBytes(new File(newCatalogURL));
            ClientResponseImpl response = (ClientResponseImpl) client.callProcedure(
                    "@UpdateApplicationCatalog", catBytes2, null);
            assert(response.getStatus() == ClientResponse.SUCCESS);
            insertMoreNormalData(10092, 10098);
        } catch (ProcCallException e) {
            e.printStackTrace();
            fail("testUacAfterHashMismatch failed");
        }
    }

    @Test(timeout = 180_000)
    public void testShutdownRecoverWithoutCL() throws Exception {
        if (!MiscUtils.isPro() || LocalCluster.isMemcheckDefined()) {
            return;
        }
        deleteTestFiles(TESTNONCE);
        createCluster("testShutdownRecoverWithoutCL", 2, 3, 18);
        VoltTable vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
        System.out.println(vt.toFormattedString());
        try {
            for (int i = 5000; i < 5091; i++) {
                client.callProcedure(
                        "NonDeterministicSPProc",
                        i,
                        i,
                        NonDeterministicSPProc.MISMATCH_INSERTION);
            }
            for (int i = 0; i < 200; i++) {
                client.callProcedure("mp.insert",i,i);
            }

            verifyTopologyAfterHashMismatch();
            System.out.println("Stopped replicas.");
            insertMoreNormalData(10001, 10092);
            for (int i = 200; i < 300; i++) {
                client.callProcedure("mp.insert",i,i);
            }
            vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            long rows = vt.asScalarLong();
            vt = client.callProcedure("@AdHoc", "select count(*) from MP").getResults()[0];
            long mprows = vt.asScalarLong();
            client.drain();
            System.out.println("Saving snapshot...");
            ClientResponse resp  = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE + "2", (byte) 1);
            vt = resp.getResults()[0];
            System.out.println(vt.toFormattedString());
            while (vt.advanceRow()) {
                assertTrue(vt.getString("RESULT").equals("SUCCESS"));
            }
            client.close();
            server.shutDown();
            Thread.sleep(2000);
            System.out.println("Shutdown cluster and recover");
            server.startUp(false);
            System.out.println("cluster recovered!");
            client = ClientFactory.createClient();
            client.createConnection("", server.port(0));
            vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
            System.out.println("recovered topo:\n" + vt.toFormattedString());

            System.out.println("Saved snapshot with " + rows + ", reloading snapshot...");
            vt = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE + "2").getResults()[0];
            System.out.println(vt.toFormattedString());
            while (vt.advanceRow()) {
                if (vt.getString("RESULT").equals("FAILURE")) {
                    fail(vt.getString("ERR_MSG"));
                }
            }
            System.out.println("snapshot reloaded");
            vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            System.out.println("rows+" + rows + " recovered:" + vt.asScalarLong());
            assert(rows == vt.asScalarLong());
            vt = client.callProcedure("@AdHoc", "select count(*) from MP").getResults()[0];
            assert(mprows == vt.asScalarLong());
        } catch (ProcCallException e) {
            fail("testShutdownRecoverWithoutCL failed");
        }
    }

    @Test(timeout = 180_000)
    public void testShutdownRecoverToMasterOnlyModeWithCL() throws Exception {
        testShutdownRecoverWithCL("testShutdownRecoverToMasterOnlyModeWithCL", false);
    }

    @Test(timeout = 180_000)
    public void testShutdownRecoverToFullClusterWithCL() throws Exception {
        testShutdownRecoverWithCL("testShutdownRecoverToFullClusterWithCL", true);
    }

    private void testShutdownRecoverWithCL(String testCase, boolean correctStoreProc) throws Exception {
        if (!MiscUtils.isPro()) {
            return;
        }
        createCluster(testCase, 2, 3, 18, true);
        VoltTable vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
        System.out.println(vt.toFormattedString());
        try {
            for (int i = 5000; i < 5091; i++) {
                client.callProcedure(
                        "NonDeterministicSPProc",
                        i,
                        i,
                        NonDeterministicSPProc.MISMATCH_INSERTION);
            }
            for (int i = 0; i < 10; i++) {
                client.callProcedure("mp.insert",i,i);
            }

            verifyTopologyAfterHashMismatch();
            System.out.println("Stopped replicas.");
            insertMoreNormalData(10001, 10092);
            for (int i = 10; i < 20; i++) {
                client.callProcedure("mp.insert",i,i);
            }
            vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            long rows = vt.asScalarLong();
            vt = client.callProcedure("@AdHoc", "select count(*) from MP").getResults()[0];
            long mprows = vt.asScalarLong();
            client.drain();

            //Can do an UAC here to correct the problem stored procedure
            //hack here to correct the stored procedure!
            if (correctStoreProc) {
                server.setJavaProperty("DISABLE_HASH_MISMATCH_TEST", "true");
            }
            client.close();
            // make a terminal snapshot
            Client adminClient = ClientFactory.createClient();
            adminClient.createConnection(server.getAdminAddress(0));
            try {
                server.shutdownSave(adminClient);
                server.waitForNodesToShutdown();
            } finally {
                adminClient.close();
            }
            Thread.sleep(2000);
            System.out.println("Shutdown cluster and recover");
            server.startUp(false);
            System.out.println("cluster recovered!");
            client = ClientFactory.createClient();
            client.createConnection("", server.port(0));
            vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
            System.out.println("recovered topo:\n" + vt.toFormattedString());
            vt = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
            System.out.println("rows+" + rows + " recovered:" + vt.asScalarLong());
            assert(rows == vt.asScalarLong());
            vt = client.callProcedure("@AdHoc", "select count(*) from MP").getResults()[0];
            assert(mprows == vt.asScalarLong());
            insertMoreNormalData(10100, 10110);
            for (int i = 20; i < 25; i++) {
                client.callProcedure("mp.insert",i,i);
            }
        } catch (ProcCallException e) {
            fail(testCase + " failed");
        }
    }
    private void insertMoreNormalData(int start, int end) throws NoConnectionsException, IOException, ProcCallException {
        for (int i = start; i < end; i++) {
            client.callProcedure("NonDeterministicSPProc", i, i, NonDeterministicSPProc.NO_PROBLEM);
        }
    }

    private void verifyTopologyAfterHashMismatch() {
        //allow time to get the stats
        final long maxSleep = TimeUnit.MINUTES.toMillis(5);
        boolean done = false;
        long start = System.currentTimeMillis();
        while (!done) {
            boolean inprogress = false;
            try {
                Thread.sleep(5000);
                VoltTable vt = client.callProcedure("@Statistics", "TOPO").getResults()[0];
                System.out.println(vt.toFormattedString());
                vt.resetRowPosition();
                while (vt.advanceRow()) {
                    long pid = vt.getLong(0);
                    if (pid != MpInitiator.MP_INIT_PID) {
                        String replicasStr = vt.getString(1);
                        String[] replicas = replicasStr.split(",");
                        if (replicas.length != 1) {
                            inprogress = true;
                            break;
                        }
                    }
                }
                if (!inprogress) {
                   return;
                }

                if (maxSleep < (System.currentTimeMillis() - start)) {
                    break;
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        assert(done);
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
}
