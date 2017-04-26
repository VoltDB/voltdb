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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.sysprocs.AdHoc_RO_MP;
import org.voltdb.sysprocs.GC;
import org.voltdb.sysprocs.UpdateApplicationCatalog;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTableUtil;

public class TestNTProcs extends TestCase {

    public static class TrivialNTProc extends VoltNonTransactionalProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            //System.out.println("Ran trivial proc!");
            return -1;
        }
    }

    public static class TrivialNTProcPriority extends VoltNonTransactionalProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));
            assertTrue(Thread.currentThread().getName().contains(NTProcedureService.NTPROC_THREADPOOL_PRIORITY_SUFFIX));

            System.out.println("Ran trivial priority proc!");
            return -1;
        }
    }

    public static class NestedNTProc extends VoltNonTransactionalProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            // call a real transactional proc
            System.out.println("Calling transaction!");
            CompletableFuture<ClientResponse> pf = callProcedure("@AdHoc", "select * from blah");
            // NB: blocking on a response keeps the thread in the pool wasted.
            // don't do this in prod
            ClientResponseImpl cr = (ClientResponseImpl) pf.get();
            System.out.println("Got response 1!");
            System.out.println(cr.toJSONString());

            // call an NT proc (should run on priority exec service)
            System.out.println("Calling nt proc!");
            pf = callProcedure("TestNTProcs$TrivialNTProcPriority");
            // NB: blocking on a response keeps the thread in the pool wasted.
            // don't do this in prod
            cr = (ClientResponseImpl) pf.get();
            System.out.println("Got response 2!");
            System.out.println(cr.toJSONString());

            return -1;
        }
    }

    public static class AsyncNTProc extends VoltNonTransactionalProcedure {
        long nextStep(ClientResponse cr) {
            System.out.printf("AsyncNTProc.nextStep running in thread: %s\n", Thread.currentThread().getName());
            System.out.flush();

            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));
            assertTrue(Thread.currentThread().getName().contains(NTProcedureService.NTPROC_THREADPOOL_PRIORITY_SUFFIX));

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

    public static class RunEverywhereNTProc extends VoltNTSystemProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Running on one!");
            CompletableFuture<Map<Integer,ClientResponse>> pf = callNTProcedureOnAllHosts("TestNTProcs$TrivialNTProcPriority");
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

    public static class DelayProcNT extends VoltNonTransactionalProcedure {
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

    public static class NTProcWithFutures extends VoltNonTransactionalProcedure {

        public Long secondPart(ClientResponse response) {
            System.out.printf("NTProcWithFutures.secondPart running in thread: %s\n", Thread.currentThread().getName());
            System.out.flush();

            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));
            assertTrue(Thread.currentThread().getName().contains(NTProcedureService.NTPROC_THREADPOOL_PRIORITY_SUFFIX));

            System.out.println("Did it NT2!");
            ClientResponseImpl cr = (ClientResponseImpl) response;
            System.out.println(cr.toJSONString());
            return -1L;
        }

        public CompletableFuture<Long> run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Did it NT1!");
            return callProcedure("TestNTProcs$DelayProc", 1).thenApply(this::secondPart);
        }
    }

    public static class RunEverywhereNTProcWithDelay extends VoltNTSystemProcedure {
        public long run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Running on one!");

            // you can't have two of these outstanding so this is expected to fail
            CompletableFuture<Map<Integer,ClientResponse>> pf1 = callNTProcedureOnAllHosts("TestNTProcs$DelayProc", 100);
            callNTProcedureOnAllHosts("TestNTProcs$DelayProcNT", 100);

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
    public static class NTProcWithBadTypeFuture extends VoltNonTransactionalProcedure {

        public String secondPart(ClientResponse response) {
            System.out.printf("NTProcWithBadTypeFuture.secondPart running in thread: %s\n", Thread.currentThread().getName());
            System.out.flush();

            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));
            assertTrue(Thread.currentThread().getName().contains(NTProcedureService.NTPROC_THREADPOOL_PRIORITY_SUFFIX));

            System.out.println("Did it NT2!");
            return "This is spinal tap!";
        }

        public CompletableFuture<String> run() throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.println("Did it NT1!");
            return callProcedure("TestNTProcs$DelayProc", 1).thenApply(this::secondPart);
        }
    }





    public static class NTProcThatSlams extends VoltNonTransactionalProcedure {

        final static int COLLECT_BLOCK = 0;
        final static int COLLECT_RETURN = 1;
        final static int COLLECT_ASYNC = 2;

        public ClientResponse secondPart(ClientResponse response) {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));
            assertTrue(Thread.currentThread().getName().contains(NTProcedureService.NTPROC_THREADPOOL_PRIORITY_SUFFIX));

            return response;
        }

        public CompletableFuture<ClientResponse> run(String whatToCall, byte[] serializedParams, int howToCollect, int timeToSleep) {
            CompletableFuture<ClientResponse> cf = null;

            // sleep for specified time
            try { Thread.sleep(timeToSleep); } catch (InterruptedException e) {}

            if (whatToCall == null) {
                cf = new CompletableFuture<ClientResponse>();
                cf.complete(new ClientResponseImpl(
                        ClientResponse.SUCCESS,
                        new VoltTable[0],
                        null,
                        0));
                return cf;
            }

            // get parameter sets
            Object[] params = new Object[0];
            if ((serializedParams != null) && (serializedParams.length > 0)) {
                ByteBuffer paramBuf = ByteBuffer.wrap(serializedParams);
                ParameterSet pset = null;
                try {
                    pset = ParameterSet.fromByteBuffer(paramBuf);
                } catch (IOException e) {
                    e.printStackTrace();
                    fail();
                }
                params = pset.toArray();
            }

            cf = callProcedure(whatToCall, params);

            switch (howToCollect) {
            case COLLECT_BLOCK:
                ClientResponse r = null;
                try {
                    r = cf.get();
                } catch (InterruptedException | ExecutionException e) {
                    e.printStackTrace();
                    fail();
                }
                cf = new CompletableFuture<ClientResponse>();
                cf.complete(r);
                return cf;
            case COLLECT_RETURN:
                return cf;
            case COLLECT_ASYNC:
                return cf.thenApply(this::secondPart);
            default:
                fail();
            }
            fail();
            return null;
        }

    }

    // get the first stats table for any selector
    final VoltTable getStats(Client client, String selector) {
        ClientResponse response = null;
        try {
            response = client.callProcedure("@Statistics", selector);
        } catch (IOException | ProcCallException e) {
            fail();
        }
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        return response.getResults()[0];
    }

    // get the first stats table for any selector
    // Note this needs to be the FULL proc name from class.getName
    final Map<String, Long> aggregateProcRow(Client client, final String procName) {
        VoltTable raw = getStats(client, "PROCEDURE");

        long[] rawResult = VoltTableUtil.stream(raw)
            // find matching rows
            .filter(r -> r.getString("PROCEDURE").equals(procName))
            // filter to six columns of interest
            .map(r -> new long[] {
                    r.getLong("INVOCATIONS"),
                    r.getLong("MAX_RESULT_SIZE"),
                    r.getLong("MAX_PARAMETER_SET_SIZE"),
                    r.getLong("ABORTS"),
                    r.getLong("FAILURES"),
                    r.getLong("TRANSACTIONAL")
                    })
            // aggregate (sum, max, max, sum, sum, identity)
            .reduce(new long[] {0, 0, 0, 0, 0, 0}, (a, b) ->
                new long[] {
                        a[0] + b[0],
                        Math.max(a[1], b[1]),
                        Math.max(a[2], b[2]),
                        a[3] + b[3],
                        a[4] + b[4],
                        b[5]
                        }
            );

        Map<String, Long> retval = new HashMap<>();
        retval.put("INVOCATIONS", rawResult[0]);
        retval.put("MAX_RESULT_SIZE", rawResult[1]);
        retval.put("MAX_PARAMETER_SET_SIZE", rawResult[2]);
        retval.put("ABORTS", rawResult[3]);
        retval.put("FAILURES", rawResult[4]);
        retval.put("TRANSACTIONAL", rawResult[5]);

        return retval;
    }

    final String SCHEMA =
            "create table blah (pkey integer not null, strval varchar(200), PRIMARY KEY(pkey));\n" +
            "partition table blah on column pkey;\n" +
            "create procedure from class org.voltdb.TestNTProcs$TrivialNTProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$TrivialNTProcPriority;\n" +
            "create procedure from class org.voltdb.TestNTProcs$NestedNTProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$AsyncNTProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$RunEverywhereNTProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$NTProcWithFutures;\n" +
            "create procedure from class org.voltdb.TestNTProcs$DelayProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$DelayProcNT;\n" +
            "create procedure from class org.voltdb.TestNTProcs$RunEverywhereNTProcWithDelay;\n" +
            "create procedure from class org.voltdb.TestNTProcs$NTProcWithBadTypeFuture;\n" +
            "create procedure from class org.voltdb.TestNTProcs$NTProcThatSlams;\n" +
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

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        assertTrue(VoltTableUtil.tableContainsString(statsT, "TrivialNTProc", true));
        Map<String, Long> stats = aggregateProcRow(client, TrivialNTProc.class.getName());
        assertEquals(1, stats.get("INVOCATIONS").longValue());

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

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        System.out.println("STATS: " + statsT.toFormattedString());

        assertTrue(VoltTableUtil.tableContainsString(statsT, "NestedNTProc", true));
        assertTrue(VoltTableUtil.tableContainsString(statsT, "adhoc", false));
        Map<String, Long> stats = aggregateProcRow(client, NestedNTProc.class.getName());
        assertEquals(1, stats.get("INVOCATIONS").longValue());
        stats = aggregateProcRow(client, AdHoc_RO_MP.class.getName());
        assertEquals(1, stats.get("INVOCATIONS").longValue());

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

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        assertTrue(VoltTableUtil.tableContainsString(statsT, "RunEverywhereNTProc", true));
        assertTrue(VoltTableUtil.tableContainsString(statsT, "TrivialNTProcPriority", true));
        Map<String, Long> stats = aggregateProcRow(client, RunEverywhereNTProc.class.getName());
        assertEquals(1, stats.get("INVOCATIONS").longValue());
        stats = aggregateProcRow(client, TrivialNTProcPriority.class.getName());
        assertEquals(1, stats.get("INVOCATIONS").longValue());

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

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        assertTrue(VoltTableUtil.tableContainsString(statsT, "RunEverywhereNTProc", true));
        assertTrue(VoltTableUtil.tableContainsString(statsT, "TrivialNTProcPriority", true));
        Map<String, Long> stats = aggregateProcRow(client, RunEverywhereNTProc.class.getName());
        assertEquals(1, stats.get("INVOCATIONS").longValue());
        stats = aggregateProcRow(client, TrivialNTProcPriority.class.getName());
        assertEquals(3, stats.get("INVOCATIONS").longValue());

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

        final int CALL_COUNT = 400;

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

        Thread.sleep(3000);

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        System.out.println("STATS: " + statsT.toFormattedString());

        assertTrue(VoltTableUtil.tableContainsString(statsT, "NTProcWithFutures", true));

        System.out.println(statsT.toFormattedString());

        // repeatedly call stats until they match
        long start = System.currentTimeMillis();
        boolean found = false;
        while (System.currentTimeMillis() - start < 20000) {
            Map<String, Long> stats = aggregateProcRow(client, NTProcWithFutures.class.getName());
            if ((CALL_COUNT + 1) == stats.get("INVOCATIONS").longValue()) {
                found = true;
                break;
            }
            Thread.sleep(1000);
        }

        localServer.shutdown();
        localServer.join();

        if (!found) {
            fail();
        }
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
        assertTrue(response.getStatusString().contains("was not an acceptible VoltDB return type"));
        System.out.println("Client got failure response");
        System.out.println(response.toJSONString());

        localServer.shutdown();
        localServer.join();
    }

    // @GC is the fist (to be coded) NT sysproc
    public void testGCSysproc() throws Exception {
        ServerThread localServer = start();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response = (ClientResponseImpl) client.callProcedure("@GC");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.println(response.getResults()[0].toFormattedString());

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        System.out.println("STATS: " + statsT.toFormattedString());

        assertTrue(VoltTableUtil.tableContainsString(statsT, "GC", true));

        System.out.println(statsT.toFormattedString());

        Map<String, Long> stats = aggregateProcRow(client, GC.class.getName());
        assertEquals(1, stats.get("INVOCATIONS").longValue());

        localServer.shutdown();
        localServer.join();
    }

    public void testUAC() throws Exception {
        String pathToCatalog = Configuration.getPathToCatalogForTest("adhocddl.jar");
        String pathToDeployment = Configuration.getPathToCatalogForTest("adhocddl.xml");

        VoltProjectBuilder builder = new VoltProjectBuilder();
        builder.addLiteralSchema("--dont care");
        builder.setUseDDLSchema(true);
        boolean success = builder.compile(pathToCatalog, 2, 1, 0);
        assertTrue("Schema compilation failed", success);
        MiscUtils.copyFile(builder.getPathToDeployment(), pathToDeployment);

        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        ServerThread localServer = new ServerThread(config);

        localServer.start();
        localServer.waitForInitialization();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response = (ClientResponseImpl) client.callProcedure("@AdHoc", "create table blah (pkey integer not null, strval varchar(200), PRIMARY KEY(pkey));");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        System.out.println("STATS: " + statsT.toFormattedString());

        assertTrue(VoltTableUtil.tableContainsString(statsT, "UpdateApplicationCatalog", true));

        Map<String, Long> stats = aggregateProcRow(client, UpdateApplicationCatalog.class.getName());
        assertEquals(1, stats.get("INVOCATIONS").longValue());

        localServer.shutdown();
        localServer.join();
    }

    /*
    final static int CALL_NOTHING = 0;
    final static int CALL_ADHOC = 1;
    final static int CALL_WRITE_PROC = 2;
    final static int CALL_TRIVIALNT = 3;
    final static int CALL_UAC = 4;
    final static int CALL_STATS = 5;
    */

    public void testSlamNTProcs() throws Exception {
        ServerThread localServer = start();

        final AtomicLong called = new AtomicLong(0);
        final AtomicLong responded = new AtomicLong(0);

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        final Client firehoseClient = ClientFactory.createClient();
        firehoseClient.createConnection("localhost");
        final AtomicBoolean keepFirehosing = new AtomicBoolean(true);

        final ProcedureCallback firehoseCallback = new ProcedureCallback() {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                responded.incrementAndGet();
                if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                    ClientResponseImpl cri = (ClientResponseImpl) clientResponse;
                    System.err.println(cri.toJSONString());
                    System.err.flush();
                    fail();
                }
            }
        };

        Thread firehoseThread = new Thread() {
            @Override
            public void run() {
                while (keepFirehosing.get()) {
                    try {
                        firehoseClient.callProcedure(firehoseCallback,
                                                     "TestNTProcs$NTProcThatSlams",
                                                     "TestNTProcs$TrivialNTProc",
                                                     new byte[0], // params
                                                     NTProcThatSlams.COLLECT_ASYNC,
                                                     1);
                        called.incrementAndGet();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
                try {
                    firehoseClient.drain();
                } catch (NoConnectionsException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        };

        firehoseThread.start();

        long nanoTime1 = System.nanoTime();
        Thread.sleep(5000);
        long nanoTime2 = System.nanoTime();
        long nowCalled = called.get(); long nowResponsed = responded.get();
        System.out.printf("Ran for %.2f seconds. Called %d procs with %d responded and %d outstanding.\n",
                (nanoTime2 - nanoTime1) / 1000000000.0, nowCalled, nowResponsed, nowCalled - nowResponsed);
        keepFirehosing.set(false);
        firehoseThread.join();
        long nanoTime3 = System.nanoTime();
        System.out.printf("Drained for %.2f seconds. %d responded.\n",
                (nanoTime3 - nanoTime2) / 1000000000.0, nowCalled - nowResponsed);

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        System.out.println("STATS: " + statsT.toFormattedString());

        assertTrue(VoltTableUtil.tableContainsString(statsT, "NTProcThatSlams", true));

        client.close();
        firehoseClient.close();
        localServer.shutdown();
        localServer.join();
    }
}
