/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Test;
import org.voltdb.BackendTarget;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.iv2.MpInitiator;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltFile;

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
                    "CREATE INDEX idx_kv ON kv(nondetval);";

    Client client;
    final int sitesPerHost = 1;
    final int hostCount = 2;
    final int kfactor = 1;
    static String expectedLogMessage = "Hash mismatch";
    static String expectHashDetectionMessage = "Hash mismatch is detected";

    LocalCluster createCluster(String method) {
        return createCluster(method, kfactor, hostCount, sitesPerHost);
    }

    LocalCluster createCluster(String method, int k, int hostcount, int sph) {
        LocalCluster server = null;
        try {
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(SCHEMA);
            builder.addProcedure(NonDeterministicSPProc.class, "kv.key: 0");
            builder.addProcedure(NonDeterministic_RO_SP.class, "kv.key: 0");
            builder.addProcedure(Deterministic_RO_SP.class, "kv.key: 0");
            builder.addProcedure(NonDeterministic_RO_MP.class);
            server = new LocalCluster(method + ".jar", sph, hostcount, k, BackendTarget.NATIVE_EE_JNI);
            server.overrideAnyRequestForValgrind();
            server.setCallingClassName(method);
            assertTrue("Catalog compilation failed", server.compile(builder));

            server.setHasLocalServer(false);
            List<String> logSearchPatterns = new ArrayList<>(1);
            logSearchPatterns.add(expectedLogMessage);
            logSearchPatterns.add(expectHashDetectionMessage);
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
        return server;
    }

    private void shutDown(LocalCluster cluster) {
        if ( client != null) {
            try {
                client.close();
            } catch (InterruptedException e) {
            }
        }
        if (cluster != null) {
            try {
                cluster.shutDown();
            } catch (InterruptedException e) {
            }
        }
    }

    /**
     * Do a non-deterministic insertion
     */
    @Test
    public void testNonDeterministicInsert() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster("testNonDeterministicInsert");
        try {
            client.callProcedure(
                    "NonDeterministicSPProc",
                    0,
                    0,
                    NonDeterministicSPProc.MISMATCH_INSERTION);
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch(server);
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
        } finally {
            shutDown(server);
        }
    }

    /**
     * Do a non-deterministic insertion followed by a single partition read-only operation.
     * ENG-3288 - Expect non-deterministic read-only queries to succeed.
     */
    @Test
    public void testNonDeterministic_RO_SP() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster("testNonDeterministic_RO_SP");
        try {
            insertMoreNormalData(1, 100);
            client.callProcedure("NonDeterministic_RO_SP", 0);
        } catch (ProcCallException e) {
            fail("R/O SP mismatch failed?! " + e.toString());
        } finally {
            shutDown(server);
        }
    }

    /**
     * Negative test that expects a deterministic proc to fail due to mismatched results.
     */
    @Test
    public void testDeterministicProc() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster("testDeterministicProc");
        try {
            insertMoreNormalData(1, 100);
            client.callProcedure("Deterministic_RO_MP", 0);
            fail("Deterministic procedure succeeded for non-deterministic results?");
        } catch (ProcCallException e) {
            // I don't quite understand what it try to test for.
        } finally {
            shutDown(server);
        }
    }

    /**
     * Test that different whitespace fails the determinism CRC check on SQL
     */
    @Test
    public void testWhitespaceChanges() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster("testWhitespaceChanges");
        try {
            client.callProcedure(
                    "NonDeterministicSPProc",
                    0,
                    0,
                    NonDeterministicSPProc.MISMATCH_WHITESPACE_IN_SQL);
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch(server);
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
        } finally {
            shutDown(server);
        }
    }

    @Test
    public void testMultistatementNonDeterministicProc() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster("testMultistatementNonDeterministicProc");
        // preload some data
        try {
            for (int i = 0; i < 10000; i++) {
                client.callProcedure("KV.insert", i, 999);
            }
        } catch (Exception e) {
            shutDown(server);
            fail("Failed to insert data");
            return;
        }

        try {
            client.callProcedure("NonDeterministicSPProc",
                    1234, //not use
                    999,
                    NonDeterministicSPProc.MULTI_STATEMENT_MISMATCH);
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch(server);
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
        } finally {
            shutDown(server);
        }
    }

    @Test
    public void testPartialstatementNonDeterministicProc() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster("testPartialstatementNonDeterministicProc");
        // preload some data
        try {
            for (int i = 0; i < 10000; i++) {
                client.callProcedure("KV.insert", i, 999);
            }
        } catch (Exception e) {
            shutDown(server);
            fail("Failed to insert data");
            return;
        }

        try {
            client.callProcedure("NonDeterministicSPProc",
                    1234, //not use
                    999,
                    NonDeterministicSPProc.PARTIAL_STATEMENT_MISMATCH);
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch(server);
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
        } finally {
            shutDown(server);
        }
    }

    @Test
    public void testBuggyNonDeterministicProc() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster("testBuggyNonDeterministicProc");
        try {
            client.callProcedure(
                    "NonDeterministicSPProc",
                    1234,
                    999,
                    NonDeterministicSPProc.TXN_ABORT);
            if (MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectHashDetectionMessage));
                verifyTopologyAfterHashMismatch(server);
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
        } finally {
            shutDown(server);
        }
    }

    @Test
    public void testOnLargeCluster() throws Exception {
        VoltFile.resetSubrootForThisProcess();
        LocalCluster server = createCluster("testOnLargeCluster", 2, 3, 18);
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
            if (MiscUtils.isPro()) {
                verifyTopologyAfterHashMismatch(server);
                System.out.println("Stopped replicas.");
                insertMoreNormalData(10001, 10092);
                client.drain();

//                System.out.println("Saving snapshot...");
//                VoltTable results = client.callProcedure("@SnapshotSave", TMPDIR, TESTNONCE, (byte) 1).getResults()[0];
//                while (results.advanceRow()) {
//                    assertTrue(results.getString("RESULT").equals("SUCCESS"));
//                }
//
//                results = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
//                long rows = results.asScalarLong();
//                System.out.println("Saved snapshot with " + rows + ", reloading snapshot...");
//                results = client.callProcedure("@SnapshotRestore", TMPDIR, TESTNONCE).getResults()[0];
//                while (results.advanceRow()) {
//                    if (results.getString("RESULT").equals("FAILURE")) {
//                        fail(results.getString("ERR_MSG"));
//                    }
//                }
//                System.out.println("snapshot reloaded");
//                results = client.callProcedure("@AdHoc", "select count(*) from KV").getResults()[0];
//                assert(rows == results.asScalarLong());
            } else {
                fail("testOnLargeCluster failed");
            }
        } catch (ProcCallException e) {
            assertTrue(e.getMessage().contains("Connection to database") ||
                    e.getMessage().contains("Crash deliberately"));
            // make sure every host witnessed the hash mismatch
            if (!MiscUtils.isPro()) {
                assertTrue(server.anyHostHasLogMessage(expectedLogMessage));
            }
        } finally {
            shutDown(server);
        }
    }

    private void insertMoreNormalData(int start, int end) throws NoConnectionsException, IOException, ProcCallException {
        for (int i = start; i < end; i++) {
            client.callProcedure("NonDeterministicSPProc", i, i, NonDeterministicSPProc.NO_PROBLEM);
        }
    }

    private void verifyTopologyAfterHashMismatch(LocalCluster server) {
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
}
