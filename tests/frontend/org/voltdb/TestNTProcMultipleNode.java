/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.JUnit4LocalClusterTest;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.VoltFile;

public class TestNTProcMultipleNode extends JUnit4LocalClusterTest {

    static final String SCHEMA =
            "create table part (" +
                    "key bigint not null, " +
                    "value bigint not null, " +
                    "PRIMARY KEY(key)" +
             "); " +
            "PARTITION TABLE part ON COLUMN key;" +
             "create table rep (" +
                     "wow bigint not null, " +
                     "huh bigint not null);" +
            "create procedure from class org.voltdb.TestNTProcMultipleNode$RunEverywhereNTProc;";

    private LocalCluster cluster;
    private static AtomicInteger s_expectedResponses = new AtomicInteger(0);
    private static AtomicInteger s_totalResponses = new AtomicInteger(0);

    private static VoltLogger hostLog = new VoltLogger("HOST");

    @Before
    public void setUp() {
        try {
            VoltProjectBuilder builder = new VoltProjectBuilder();
            builder.addLiteralSchema(SCHEMA);

            cluster = new LocalCluster("ntproc.jar", 8, 3, 2, BackendTarget.NATIVE_EE_JNI);
            cluster.overrideAnyRequestForValgrind();
            assertTrue("Catalog compilation failed", cluster.compile(builder));

            cluster.setHasLocalServer(false);

            cluster.startUp();

            // Previous Test thread could still have VoltPrefix assigned
            VoltFile.resetSubrootForThisProcess();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @After
    public void tearDown() throws Exception {
        cluster.shutDown();
        assertTrue(cluster.areAllNonLocalProcessesDead());
        s_expectedResponses.set(0);
        s_totalResponses.set(0);
    }

    class AsyncCallback implements ProcedureCallback {
        int m_id;
        String m_query;

        public AsyncCallback(int id, String query) {
            m_id = id;
            m_query = query;
        }

        public void clientCallback(ClientResponse r) throws Exception {
            // Might get RESPONSE_UNKNOWN because mastership changes
            if (r.getStatus() == ClientResponse.SUCCESS || r.getStatus() == ClientResponse.RESPONSE_UNKNOWN) {
                s_expectedResponses.incrementAndGet();
            } else {
                hostLog.error("Client " + m_id + " sent query " + m_query + " received unexpected response: " + r.getStatus());
            }
            s_totalResponses.incrementAndGet();
        }
    }

    public static class RunEverywhereNTProc extends VoltNTSystemProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Running on one!");
            CompletableFuture<Map<Integer,ClientResponse>> pf = callNTProcedureOnAllHosts("TestNTProcs$TrivialNTProcPriority");
            pf.get();
            System.out.println("Got responses!");
            return -1;
        }
    }

    @Test
    public void TestAdHocNTProcedureWithNodeFailure() throws Exception {
        int poolSize = 2;
        int CLIENTS = 10;
        int PER_CLIENT_TXN = 200;
        int MP_RATIO = 5;
        int READONLY_RATIO = 2;
        int STOPNODE_RATIO = 4;
        ExecutorService es = Executors.newFixedThreadPool(poolSize);
        List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
        for (int i = 1; i <= CLIENTS; i++) {
            final boolean isMp = (i % MP_RATIO == 0) ? true : false;
            final boolean isReadOnly = (i % READONLY_RATIO == 0) ? true : false;
            final boolean isStopNode = (i % STOPNODE_RATIO == 0) ? true : false;
            final int id = i;
            Future<Boolean> ret = es.submit(() -> {
                Client client = ClientFactory.createClient();
                try {
                    client.createConnection("localhost", cluster.port(0));
                    if (isStopNode) {
                        client.callProcedure(new AsyncCallback(id, "@StopNode " + (id / STOPNODE_RATIO)), "@StopNode", (id / STOPNODE_RATIO));
                    } else {
                        String query;
                        for (int j = 0; j < PER_CLIENT_TXN; j++) {
                            if (isMp) {
                                if (isReadOnly) {
                                    query = "select * from rep";
                                } else {
                                    query = "insert into rep values (" + (id * 10000 + j) + "," + id + ")";
                                }
                            } else {
                                if (isReadOnly) {
                                    query = "select * from part where key = " + id;
                                } else {
                                    query = "insert into part values (" + (id * 10000 + j) + "," + id + ")";
                                }
                            }
                            client.callProcedure(new AsyncCallback(id, query), "@AdHoc", query);
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
                return true;
            });
            results.add(ret);
        }
        Thread.sleep(2000); // enough time for all responses to come back
        for (Future<Boolean> ret : results) {
            assertTrue(ret.get(1, TimeUnit.SECONDS));
        }

        // wait for everything to be done and check status
        es.shutdown();
        if (!es.awaitTermination(60, TimeUnit.SECONDS)) {
            fail("Worker threads should have finished execution by now");
        }
        int numOfStopNode = CLIENTS / STOPNODE_RATIO;
        int totalResponses = (CLIENTS - numOfStopNode) * PER_CLIENT_TXN + numOfStopNode;
        assertEquals(totalResponses, s_totalResponses.get()); // Do we miss a response?
        assertEquals(totalResponses, s_expectedResponses.get()); // Do all responses have expected status code?
    }

    @Test
    public void TestRunEverywhereNTProcWithNodeFailure() throws Exception {
        int poolSize = 2;
        int CLIENTS = 2;
        int PER_CLIENT_TXN = 200;
        int STOPNODE_RATIO = 2;
        ExecutorService es = Executors.newFixedThreadPool(poolSize);
        List<Future<Boolean>> results = new ArrayList<Future<Boolean>>();
        for (int i = 1; i <= CLIENTS; i++) {
            final boolean isStopNode = (i % STOPNODE_RATIO == 0) ? true : false;
            final int id = i;
            Future<Boolean> ret = es.submit(() -> {
                Client client = ClientFactory.createClient();
                try {
                    client.createConnection("localhost", cluster.port(0));
                    client.createConnection("localhost", cluster.port(1));
                    client.createConnection("localhost", cluster.port(2));

                    if (isStopNode) {
                        ClientResponse r = client.callProcedure("@StopNode", (id / STOPNODE_RATIO));
                        s_expectedResponses.incrementAndGet();
                        if (ClientResponse.SUCCESS == r.getStatus()) {
                            s_totalResponses.incrementAndGet();
                        }
                    } else {
                        for (int j = 0; j < PER_CLIENT_TXN; j++) {
                            try {
                                ClientResponse r = client.callProcedure("TestNTProcMultipleNode$RunEverywhereNTProc");
                                s_totalResponses.incrementAndGet();
                                if (ClientResponse.SUCCESS == r.getStatus()) {
                                    s_expectedResponses.incrementAndGet();
                                }
                            } catch (ProcCallException e) {
                                s_totalResponses.incrementAndGet();
                                s_expectedResponses.incrementAndGet();
                                continue;
                            }
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    return false;
                }
                return true;
            });
            results.add(ret);
        }
        for (Future<Boolean> ret : results) {
            assertTrue(ret.get());
        }

        // wait for everything to be done and check status
        es.shutdown();
        if (!es.awaitTermination(60, TimeUnit.SECONDS)) {
            fail("Worker threads should have finished execution by now");
        }
        int numOfStopNode = CLIENTS / STOPNODE_RATIO;
        int totalResponses = (CLIENTS - numOfStopNode) * PER_CLIENT_TXN + numOfStopNode;
        assertEquals(totalResponses, s_totalResponses.get()); // Do we miss a response?
        assertEquals(totalResponses, s_expectedResponses.get()); // Do all responses have expected status code?
    }
}
