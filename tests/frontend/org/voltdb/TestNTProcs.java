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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.utils.MiscUtils;

public class TestNTProcs extends TestCase {

    public static class TrivialNTProc extends VoltNTProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Ran trivial proc!");
            return -1;
        }
    }

    public static class NestedNTProc extends VoltNTProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Did it!");
            CompletableFuture<ClientResponse> pf = callProcedure("@AdHoc", "select * from blah");

            // NB: blocking on a response keeps the thread in the pool wasted.
            // don't do this in prod
            ClientResponseImpl cr = (ClientResponseImpl) pf.get();
            System.out.println("Got response!");
            System.out.println(cr.toJSONString());
            return -1;
        }
    }

    public static class AsyncNTProc extends VoltNTProcedure {
        long nextStep(ClientResponse cr) {
            System.out.printf("AsyncNTProc.nextStep running in thread: %s\n", Thread.currentThread().getName());
            System.out.flush();

            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Got to nextStep!");
            return 0;
        }

        public CompletableFuture<Long> run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Did it!");
            CompletableFuture<ClientResponse> pf = callProcedure("@AdHoc", "select * from blah");
            return pf.thenApply(this::nextStep);
        }
    }

    public static class RunEverywhereNTProc extends VoltNTProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Running on one!");
            CompletableFuture<Map<Integer,ClientResponse>> pf = callAllNodeNTProcedure("TestNTProcs$TrivialNTProc");
            Map<Integer,ClientResponse> cr = pf.get();
            cr.entrySet().stream()
                .forEach(e -> {
                    assertEquals(ClientResponse.SUCCESS, e.getValue().getStatus());
                });
            System.out.println("Got responses!");
            return -1;
        }
    }

    public static class DelayProc extends VoltProcedure {
        public long run(int millis) throws InterruptedException {
            // This comment is left here to remind people this is not an NT proc..
            //assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Starting delay proc");
            System.out.flush();
            Thread.sleep(millis);
            System.out.println("Done with delay proc");
            System.out.flush();
            return -1;
        }
    }

    public static class DelayProcNT extends VoltNTProcedure {
        public long run(int millis) throws InterruptedException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Starting delaynt proc");
            System.out.flush();
            Thread.sleep(millis);
            System.out.println("Done with delaynt proc");
            System.out.flush();
            return -1;
        }
    }

    public static class NTProcWithFutures extends VoltNTProcedure {

        public Long secondPart(ClientResponse response) {
            System.out.printf("NTProcWithFutures.secondPart running in thread: %s\n", Thread.currentThread().getName());
            System.out.flush();

            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Did it NT2!");
            ClientResponseImpl cr = (ClientResponseImpl) response;
            System.out.println(cr.toJSONString());
            return -1L;
        }

        public CompletableFuture<Long> run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Did it NT1!");
            return callProcedure("TestNTProcs$DelayProc", 1).thenApplyAsync(this::secondPart, m_runner.m_executorService);
        }
    }

    public static class RunEverywhereNTProcWithDelay extends VoltNTProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Running on one!");

            // you can't have two of these outstanding so this is expected to fail
            CompletableFuture<Map<Integer,ClientResponse>> pf1 = callAllNodeNTProcedure("TestNTProcs$DelayProc", 100);
            callAllNodeNTProcedure("TestNTProcs$DelayProcNT", 100);

            Map<Integer,ClientResponse> cr = pf1.get();
            cr.entrySet().stream()
                .forEach(e -> {
                    assertEquals(ClientResponse.SUCCESS, e.getValue().getStatus());
                });
            System.out.println("Got responses!");
            return -1;
        }
    }

    // This class returns CompletableFuture<String> from run(), which is both invalid, and hard to
    // check statically. Verify we at least get a runtime error.
    public static class NTProcWithBadTypeFuture extends VoltNTProcedure {

        public String secondPart(ClientResponse response) {
            System.out.printf("NTProcWithBadTypeFuture.secondPart running in thread: %s\n", Thread.currentThread().getName());
            System.out.flush();

            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Did it NT2!");
            return "This is spinal tap!";
        }

        public CompletableFuture<String> run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Did it NT1!");
            return callProcedure("TestNTProcs$DelayProc", 1).thenApplyAsync(this::secondPart, m_runner.m_executorService);
        }
    }

    final String SCHEMA =
            "create table blah (pkey integer not null, strval varchar(200), PRIMARY KEY(pkey));\n" +
            "partition table blah on column pkey;\n" +
            "create procedure from class org.voltdb.TestNTProcs$TrivialNTProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$NestedNTProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$AsyncNTProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$RunEverywhereNTProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$NTProcWithFutures;\n" +
            "create procedure from class org.voltdb.TestNTProcs$DelayProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$DelayProcNT;\n" +
            "create procedure from class org.voltdb.TestNTProcs$RunEverywhereNTProcWithDelay;\n" +
            "create procedure from class org.voltdb.TestNTProcs$NTProcWithBadTypeFuture;\n" +
            "partition table blah on column pkey;\n";

    private void compile() throws Exception {
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(SCHEMA);
        assertTrue(pb.compile(Configuration.getPathToCatalogForTest("compileNT.jar")));
        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest("compileNT.xml"));
    }

    private ServerThread start() throws Exception {
        compile();

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = Configuration.getPathToCatalogForTest("compileNT.jar");
        config.m_pathToDeployment = Configuration.getPathToCatalogForTest("compileNT.xml");
        ServerThread localServer = new ServerThread(config);
        localServer.start();
        localServer.waitForInitialization();

        return localServer;
    }

    public void testNTCompile() throws Exception {
        compile();
    }

    public void testTrivialNTRoundTrip() throws Exception {
        ServerThread localServer = start();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response;

        response = (ClientResponseImpl) client.callProcedure("TestNTProcs$TrivialNTProc");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.println("Client got trivial response");
        System.out.println(response.toJSONString());

        localServer.shutdown();
        localServer.join();
    }

    public void testNestedNTRoundTrip() throws Exception {
        ServerThread localServer = start();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response;

        response = (ClientResponseImpl) client.callProcedure("TestNTProcs$NestedNTProc");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.println("Client got trivial response");
        System.out.println(response.toJSONString());

        localServer.shutdown();
        localServer.join();
    }

    public void testRunEverywhereNTRoundTripOneNode() throws Exception {
        ServerThread localServer = start();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response;

        response = (ClientResponseImpl) client.callProcedure("TestNTProcs$RunEverywhereNTProc");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.println("Client got trivial response");
        System.out.println(response.toJSONString());

        localServer.shutdown();
        localServer.join();
    }

    public void testRunEverywhereNTRoundTripCluster() throws Exception {
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(SCHEMA);

        LocalCluster cluster = new LocalCluster("compileNT.jar", 4, 3, 1, BackendTarget.NATIVE_EE_JNI);

        boolean success = cluster.compile(pb);
        assertTrue(success);

        cluster.startUp();

        Client client = ClientFactory.createClient();
        client.createConnection(cluster.getListenerAddress(0));

        ClientResponseImpl response;

        response = (ClientResponseImpl) client.callProcedure("TestNTProcs$RunEverywhereNTProc");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.println("Client got trivial response");
        System.out.println(response.toJSONString());

        client.close();
        cluster.shutDown();
    }

    //
    // This should stress the callbacks, handles and futures for NT procs
    //
    public void testOverlappingNT() throws Exception {
        ServerThread localServer = start();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        final int CALL_COUNT = 500;

        ClientResponseImpl response;
        SyncCallback[] cb = new SyncCallback[CALL_COUNT];

        for (int i = 0; i < CALL_COUNT; i++) {
            cb[i] = new SyncCallback();
            boolean success = client.callProcedure(cb[i], "TestNTProcs$NTProcWithFutures");
            assert(success);
        }

        response = (ClientResponseImpl) client.callProcedure("TestNTProcs$NTProcWithFutures");
        System.out.println("1: " + response.toJSONString());

        for (int i = 0; i < CALL_COUNT; i++) {
            cb[i].waitForResponse();
            response = (ClientResponseImpl) cb[i].getResponse();
            System.out.println("2: " + response.toJSONString());
        }

        localServer.shutdown();
        localServer.join();
    }

    //
    // Make sure you can only run one all host NT proc at a time from a single calling proc
    // (It's ok to run them from multiple calling procs)
    //
    public void testRunOnAllHostsSerialness() throws Exception {
        ServerThread localServer = start();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response;

        try {
            response = (ClientResponseImpl) client.callProcedure("TestNTProcs$RunEverywhereNTProcWithDelay");
        }
        catch (ProcCallException e) {
            response = (ClientResponseImpl) e.getClientResponse();
        }
        assertEquals(ClientResponse.USER_ABORT, response.getStatus());
        assertTrue(response.getStatusString().contains("can be running at a time"));

        System.out.println("Client got trivial response");
        System.out.println(response.toJSONString());

        localServer.shutdown();
        localServer.join();
    }

    public void testBadFutureType() throws Exception {
        ServerThread localServer = start();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response = null;

        try {
            response = (ClientResponseImpl) client.callProcedure("TestNTProcs$NTProcWithBadTypeFuture");
        }
        catch (ProcCallException e) {
            response = (ClientResponseImpl) e.getClientResponse();
        }
        assertEquals(ClientResponse.GRACEFUL_FAILURE, response.getStatus());
        System.out.println("Client got failure response");
        System.out.println(response.toJSONString());

        localServer.shutdown();
        localServer.join();
    }
}
