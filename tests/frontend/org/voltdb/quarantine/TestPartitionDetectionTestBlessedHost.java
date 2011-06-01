/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.quarantine;
import java.io.IOException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.voltdb.BackendTarget;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;

public class TestPartitionDetectionTestBlessedHost extends TestCase
{
    private static final String TMPDIR = "/tmp";
    private static final String TESTNONCE = "ppd_nonce";

    public TestPartitionDetectionTestBlessedHost(String name) {
        super(name);
    }

    VoltProjectBuilder getBuilderForTest() throws IOException {
        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("CREATE TABLE T(A1 INTEGER NOT NULL, A2 INTEGER, PRIMARY KEY(A1));");
        builder.addPartitionInfo("T", "A1");
        builder.addStmtProcedure("InsertA", "INSERT INTO T VALUES(?,?);", "T.A1: 0");
        return builder;
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

    public void testBlessedHost() throws Exception {
        final Semaphore rateLimit = new Semaphore(10);
        Client client = ClientFactory.createClient();

        VoltProjectBuilder builder = getBuilderForTest();
        // choose a partitionable cluster: 2 sites / 2 hosts / k-factor 1.
        // use a separate process for each host.
        LocalCluster cluster = new LocalCluster("partition-detection2.jar", 2, 2, 1, BackendTarget.NATIVE_EE_JNI);
        cluster.setHasLocalServer(false);
        boolean success = cluster.compileWithPartitionDetection(builder, TMPDIR, TESTNONCE);
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
