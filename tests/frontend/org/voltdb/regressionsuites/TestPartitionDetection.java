/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB Inc.
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
import junit.framework.TestCase;

import java.io.*;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.BackendTarget;
import org.voltdb.client.*;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.SnapshotVerifier;

public class TestPartitionDetection extends TestCase
{
    private static final String TMPDIR = "/tmp";
    private static final String TESTNONCE = "ppd_nonce";

    public TestPartitionDetection(String name) {
        super(name);
    }

    VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE T(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));");
        builder.addPartitionInfo("T", "A1");
        builder.addStmtProcedure("InsertA", "INSERT INTO T VALUES(?,?);", "T.A1: 0");
        return builder;
    }

    // stolen from TestSaveRestoreSysproc
    private void validateSnapshot(boolean expectSuccess) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(baos);
        PrintStream original = System.out;
        try {
            System.setOut(ps);
            String args[] = new String[] {
                    TESTNONCE,
                    "--dir",
                    TMPDIR
            };
            SnapshotVerifier.main(args);
            ps.flush();
            String reportString = baos.toString("UTF-8");
            if (expectSuccess) {
                assertTrue(reportString.startsWith("Snapshot valid\n"));
            } else {
                assertTrue(reportString.startsWith("Snapshot corrupted\n"));
            }
        } catch (UnsupportedEncodingException e) {}
          finally {
            System.setOut(original);
        }
    }



    static class Callback implements ProcedureCallback {
        private Semaphore m_rateLimit;
        public Callback(Semaphore rateLimit) {
            m_rateLimit = rateLimit;
        }
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
    }

    static class CallbackGood implements ProcedureCallback {
        private Semaphore m_rateLimit;
        public static AtomicBoolean allOk = new AtomicBoolean(true);

        public CallbackGood(Semaphore rateLimit) {
            m_rateLimit = rateLimit;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse)
                throws Exception {
            m_rateLimit.release();

            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                System.err.println(clientResponse.getStatusString());
                allOk.set(false);
                return;
            }
            if (clientResponse.getResults()[0].asScalarLong() != 1) {
                allOk.set(false);
                return;
            }
        }
    }


    public void testPartitionAndVerifySnapshot() throws Exception {
        final Semaphore rateLimit = new Semaphore(10);
        final Client client = ClientFactory.createClient();

        try {
            VoltProjectBuilder builder = getBuilderForTest();
            // choose a partitionable cluster: 2 sites / 2 hosts / k-factor 1.
            // use a separate process for each host.
            LocalCluster cluster = new LocalCluster("partition-detection1.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
            cluster.setHasLocalServer(false);
            boolean success = cluster.compileWithPartitiondDetection(builder, TMPDIR, TESTNONCE);
            assertTrue(success);
            cluster.startUp();
            client.createConnection("localhost");

            // add several tuples
            for (int i=0; i < 100; i++) {
                rateLimit.acquire();
                client.callProcedure(new CallbackGood(rateLimit), "InsertA", i, 1000+i);
            }
            client.drain();
            client.close();
            assertTrue(CallbackGood.allOk.get());


            // kill the blessed host (leaving the non-blessed partitioned)
            int blessed = cluster.getBlessedPartitionDetectionProcId();
            cluster.shutDownSingleHost(blessed);

            /* add several tuples without blocking the test.
            final Client client2 = ClientFactory.createClient();
            client2.createConnection("localhost", Client.VOLTDB_SERVER_PORT + blessed);
            Thread cltthread = new Thread("TestPartitionDetectionClientThread") {
                @Override
                public void run() {
                    for (int i=100; i < 200; i++) {
                        try {
                            rateLimit.acquire();
                            client2.callProcedure(new Callback(rateLimit), "InsertA", i, 1000+i);
                        } catch (Exception ex) {
                            // this is allowed - the cluster is in the process of dying.
                            rateLimit.release();
                            break;
                        }
                    }
                }
            };
            cltthread.setDaemon(true);
            cltthread.start();
            */

            while (!cluster.areAllNonLocalProcessesDead()) {
                System.err.println("Waiting for cluster to stop execution");
                Thread.sleep(5000);
            }

            // now verify the snapshot.
            validateSnapshot(true);
        }
        finally {
            client.close();
        }
    }

    public void testBlessedHost() throws Exception {
        final Semaphore rateLimit = new Semaphore(10);
        Client client = ClientFactory.createClient();

        VoltProjectBuilder builder = getBuilderForTest();
        // choose a partitionable cluster: 2 sites / 2 hosts / k-factor 1.
        // use a separate process for each host.
        LocalCluster cluster = new LocalCluster("partition-detection2.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.setHasLocalServer(false);
        boolean success = cluster.compileWithPartitiondDetection(builder, TMPDIR, TESTNONCE);
        assertTrue(success);
        cluster.startUp();
        client.createConnection("localhost");

        // add several tuples
        for (int i=0; i < 100; i++) {
            rateLimit.acquire();
            client.callProcedure(new Callback(rateLimit), "InsertA", i, 1000+i);
        }
        client.drain();
        client.close();

        // kill the non-blessed. the blessed host should continue.
        int blessed = cluster.getBlessedPartitionDetectionProcId();
        if (blessed == 0) {
            cluster.shutDownSingleHost(1);
        } else {
            cluster.shutDownSingleHost(0);
        }

        client = ClientFactory.createClient();
        client.createConnection("localhost", Client.VOLTDB_SERVER_PORT + blessed);
        for (int i=100; i < 200; i++) {
            rateLimit.acquire();
            client.callProcedure(new Callback(rateLimit), "InsertA", i, 1000+i);
        }
        client.drain();
        client.close();

        // all callbacks succeeded
        assertTrue(CallbackGood.allOk.get());
    }
}
