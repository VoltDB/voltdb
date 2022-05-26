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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.voltdb.VoltDB.Configuration;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ClientResponseWithPartitionKey;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcCallException;
import org.voltdb.client.ProcedureCallback;
import org.voltdb.client.SyncCallback;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.regressionsuites.LocalCluster;
import org.voltdb.sysprocs.AdHoc_RO_MP;
import org.voltdb.sysprocs.GC;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTableUtil;

import junit.framework.AssertionFailedError;
import junit.framework.TestCase;

public class TestNTProcs extends TestCase {

    public static class TrivialNTProc extends VoltNonTransactionalProcedure {

        final static AtomicLong m_runCount = new AtomicLong(0);
        final static AtomicLong m_returnCount = new AtomicLong(0);

        public long run() throws InterruptedException, ExecutionException {
            m_runCount.incrementAndGet();
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            m_returnCount.incrementAndGet();
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

            String threadName = Thread.currentThread().getName();
            assertTrue(threadName.startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

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

        final static AtomicLong m_runCount = new AtomicLong(0);
        final static AtomicLong m_returnCount = new AtomicLong(0);

        final static int COLLECT_BLOCK = 0;
        final static int COLLECT_RETURN = 1;
        final static int COLLECT_ASYNC = 2;

        public ClientResponse secondPart(ClientResponse response) {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            m_returnCount.incrementAndGet();
            assert(response != null);
            assert(response.getStatus() == ClientResponse.SUCCESS);
            return response;
        }

        public CompletableFuture<ClientResponse> run(String whatToCall, byte[] serializedParams, int howToCollect, int timeToSleep) {
            m_runCount.incrementAndGet();

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
            try {
                ClientResponse cr = cf.get();
                assert(cr.getStatus() == ClientResponse.SUCCESS);
            } catch (InterruptedException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                fail();
            } catch (ExecutionException e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
                fail();
            }

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
                //m_runCount.incrementAndGet();
                return cf.thenApply(this::secondPart);
            default:
                fail();
            }
            fail();
            return null;
        }

    }

    public static class RegularProcedureSP extends VoltProcedure {

        final SQLStmt select = new SQLStmt("select * from blah;");

        public VoltTable[] run(int pkey, int shouldThrow) {
            System.out.printf("Running on a site and shouldThrow = %d!\n", shouldThrow);
            if (shouldThrow != 0) {
                // divide by zero. live dangerously.
                shouldThrow /= 0;
                // I like to use a var just to be sure the compiler doesn't optimize it out
                // This is probably unnecessary in java
                System.out.printf("We just divided by zero and shouldThrow is now: %d\n", shouldThrow);
            }

            voltQueueSQL(select);
            return voltExecuteSQL(true);
        }

    }

    public static class RunOnAllPartitionsNTProc extends VoltNonTransactionalProcedure {

        final static int NORMAL = 0;
        final static int SP_PROC_SHOULD_THROW = 1;
        final static int NT_PROC_SHOULD_THROW = 2;
        final static int CALL_MISSING_PROC = 3;

        public long run(int mode) throws InterruptedException, ExecutionException {
            assertTrue(Thread.currentThread().getName().startsWith(NTProcedureService.NTPROC_THREADPOOL_NAMEPREFIX));

            System.out.printf("Running on one with mode = %d!\n", mode);

            CompletableFuture<ClientResponseWithPartitionKey[]> pf = null;

            if (mode == NORMAL) {
                pf = callAllPartitionProcedure("TestNTProcs$RegularProcedureSP", 0);
            }
            if (mode == SP_PROC_SHOULD_THROW) {
                pf = callAllPartitionProcedure("TestNTProcs$RegularProcedureSP", 1);
            }
            if (mode == NT_PROC_SHOULD_THROW) {
                // send off async work then throw an exception and bail
                pf = callAllPartitionProcedure("TestNTProcs$RegularProcedureSP", 0);
                int x = mode / 0;
                // I like to use a var just to be sure the compiler doesn't optimize it out
                // This is probably unnecessary in java
                System.out.printf("Divide by zero y'all: %d\n", x);
            }
            if (mode == CALL_MISSING_PROC) {
                pf = callAllPartitionProcedure("RyanLikesTheYankees", 0);
            }

            final byte expectedCode = (mode == SP_PROC_SHOULD_THROW) ? ClientResponse.UNEXPECTED_FAILURE : ClientResponse.SUCCESS;

            ClientResponseWithPartitionKey[] crs = pf.get();

            final AtomicReference<String> throwIntentionalAssertion = new AtomicReference<>(new String(""));
            try {
                Arrays.stream(crs)
                    .forEach(crwp -> {
                        short status = crwp.response.getStatus();
                        if (status != expectedCode) {
                            if (status == ClientResponse.RESPONSE_UNKNOWN
                                || status == ClientResponse.SERVER_UNAVAILABLE) {
                                // nothing to do here I guess
                            }
                            else {
                                ClientResponseImpl cri = (ClientResponseImpl) crwp.response;
                                if (mode != CALL_MISSING_PROC) {
                                    System.err.println(cri.toJSONString());
                                    System.err.flush();
                                    System.exit(-1);
                                }
                                else {
                                    throwIntentionalAssertion.set(cri.toJSONString());
                                }
                            }
                        }
                    });
                if (!throwIntentionalAssertion.get().isEmpty()) {
                    throw new AssertionFailedError(throwIntentionalAssertion.get());
                }
                System.out.println("Got responses!");
            }
            catch (Exception e) {
                e.printStackTrace();
            }
            return crs.length;
        }
    }

    private ServerThread m_serverThread;
    private LocalCluster m_cluster;

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
            "create procedure from class org.voltdb.TestNTProcs$RunOnAllPartitionsNTProc;\n" +
            "create procedure from class org.voltdb.TestNTProcs$RegularProcedureSP;\n" +
            "partition procedure TestNTProcs$RegularProcedureSP on table blah column pkey;\n";

    private void compile() throws Exception {
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(SCHEMA);
        assertTrue(pb.compile(Configuration.getPathToCatalogForTest("compileNT.jar"), 5, 0));
        MiscUtils.copyFile(pb.getPathToDeployment(), Configuration.getPathToCatalogForTest("compileNT.xml"));
    }

    private void start() throws Exception {
        compile();

        start(Configuration.getPathToCatalogForTest("compileNT.jar"),
                Configuration.getPathToCatalogForTest("compileNT.xml"));
    }

    private void start(String pathToCatalog, String pathToDeployment) throws Exception {
        VoltDB.Configuration config = new VoltDB.Configuration();
        config.m_pathToCatalog = pathToCatalog;
        config.m_pathToDeployment = pathToDeployment;

        m_serverThread = new ServerThread(config);
        m_serverThread.start();
        m_serverThread.waitForInitialization();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (m_serverThread != null) {
            System.out.println("Stopping server thread");
            m_serverThread.shutdown();
            m_serverThread.join();
            m_serverThread = null;
        }

        if (m_cluster != null) {
            System.out.println("Stopping local cluster");
            m_cluster.shutDown();
            m_cluster = null;
        }
    }

    public void testNTCompile() throws Exception {
        compile();
    }

    public void testTrivialNTRoundTrip() throws Exception {
        start();

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
    }

    public void testNestedNTRoundTrip() throws Exception {
        start();

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
    }

    public void testRunEverywhereNTRoundTripOneNode() throws Exception {
        start();

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
    }

    public void testRunEverywhereNTRoundTripCluster() throws Exception {
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(SCHEMA);

        m_cluster = new LocalCluster("compileNT.jar", 4, 3, 1, BackendTarget.NATIVE_EE_JNI);

        boolean success = m_cluster.compile(pb);
        assertTrue(success);

        m_cluster.startUp();

        Client client = ClientFactory.createClient();
        client.createConnection(m_cluster.getListenerAddress(0));

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
    }

    //
    // This should stress the callbacks, handles and futures for NT procs
    //
    public void testOverlappingNT() throws Exception {
        start();

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
        while ((System.currentTimeMillis() - start) < 30000) {
            Map<String, Long> stats = aggregateProcRow(client, NTProcWithFutures.class.getName());
            if ((CALL_COUNT + 1) == stats.get("INVOCATIONS").longValue()) {
                found = true;
                break;
            }
            Thread.sleep(1000);
        }

        if (!found) {
            fail();
        }
    }

    //
    // Make sure you can only run one all host NT proc at a time from a single calling proc
    // (It's ok to run them from multiple calling procs)
    //
    public void testRunOnAllHostsSerialness() throws Exception {
        start();

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
    }

    public void testRunOnAllHostsAPI() throws Exception {
        start();

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
    }

    public void testBadFutureType() throws Exception {
        start();

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
    }

    // @GC is the fist (to be coded) NT sysproc
    public void testGCSysproc() throws Exception {
        start();

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

        start(pathToCatalog, pathToDeployment);

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response = (ClientResponseImpl) client.callProcedure("@AdHoc", "create table blah (pkey integer not null, strval varchar(200), PRIMARY KEY(pkey));");
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        System.out.println("STATS: " + statsT.toFormattedString());
        assertEquals(0, statsT.getRowCount());
    }

    /*
    final static int CALL_NOTHING = 0;
    final static int CALL_ADHOC = 1;
    final static int CALL_WRITE_PROC = 2;
    final static int CALL_TRIVIALNT = 3;
    final static int CALL_UAC = 4;
    final static int CALL_STATS = 5;
    */

    static class SlamCallback implements ProcedureCallback {
        final Set<Long> m_outstanding;
        final long m_id;
        final AtomicBoolean m_done = new AtomicBoolean(false);
        final static AtomicLong m_count = new AtomicLong(0);

        SlamCallback(long id, Set<Long> outstanding) {
            m_id = id; m_outstanding = outstanding;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            m_count.incrementAndGet();

            if (!m_done.compareAndSet(false, true)) {
                assert(false);
                fail();
            }

            boolean removed = m_outstanding.remove(m_id);
            assertTrue(removed);
            if (clientResponse.getStatus() != ClientResponse.SUCCESS) {
                ClientResponseImpl cri = (ClientResponseImpl) clientResponse;
                System.err.println(cri.toJSONString());
                System.err.flush();
                fail();
            }
        }
    }

    public void testSlamNTProcs() throws Exception {

        final Set<Long> outstanding = Collections.synchronizedSet(new HashSet<Long>());

        start();

        final AtomicLong called = new AtomicLong(0);

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        final Client firehoseClient = ClientFactory.createClient();
        firehoseClient.createConnection("localhost");
        final AtomicBoolean keepFirehosing = new AtomicBoolean(true);
        final AtomicBoolean keepChecking = new AtomicBoolean(true);

        Thread firehoseThread = new Thread() {
            @Override
            public void run() {
                long id = 0;

                while (keepFirehosing.get()) {

                    SlamCallback scb = new SlamCallback(++id, outstanding);
                    outstanding.add(scb.m_id);

                    try {
                        boolean status = firehoseClient.callProcedure(scb,
                                                                      "TestNTProcs$NTProcThatSlams",
                                                                      "TestNTProcs$TrivialNTProc",
                                                                      new byte[0], // params
                                                                      NTProcThatSlams.COLLECT_ASYNC,
                                                                      1);
                        assertTrue(status);
                        called.incrementAndGet();
                    } catch (IOException e) {
                        e.printStackTrace();
                        fail();
                    }
                }
                try {
                    firehoseClient.drain();
                } catch (NoConnectionsException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    fail();
                }
            }
        };

        firehoseThread.start();

        long nanoTime1 = System.nanoTime();
        Thread.sleep(5000);
        long nanoTime2 = System.nanoTime();
        long nowCalled = called.get(); long leftToRespond = outstanding.size();
        System.out.printf("Ran for %.2f seconds. Called %d procs with %d outstanding.\n",
                (nanoTime2 - nanoTime1) / 1000000000.0, nowCalled, leftToRespond);
        System.out.printf("NTProcThatSlams reports %d starts and %d returns.\n",
                NTProcThatSlams.m_runCount.get(), NTProcThatSlams.m_returnCount.get());
        System.out.printf("TrivialNTProc reports %d starts and %d returns.\n",
                TrivialNTProc.m_runCount.get(), TrivialNTProc.m_returnCount.get());
        keepFirehosing.set(false);

        Thread statsThread = new Thread() {
            @Override
            public void run() {
                while (keepChecking.get()) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    long nowCalled = called.get(); long leftToRespond = outstanding.size();
                    System.out.printf("Update: Called %d procs with %d outstanding.\n",
                            nowCalled, leftToRespond);
                    System.out.printf("  NTProcThatSlams reports %d starts and %d returns.\n",
                            NTProcThatSlams.m_runCount.get(), NTProcThatSlams.m_returnCount.get());
                    System.out.printf("  TrivialNTProc reports %d starts and %d returns.\n",
                            TrivialNTProc.m_runCount.get(), TrivialNTProc.m_returnCount.get());
                    if (outstanding.size() > 0) {
                        Set<Long> copySet = new HashSet<>();
                        copySet.addAll(outstanding);
                        System.out.print("  Outstanding: ");
                        copySet.stream().forEach(l -> System.out.printf("%d, ", l));
                        System.out.println();
                    }
                    System.out.printf("  SlamCallback reports %d processed.\n",
                            SlamCallback.m_count.get());
                }
            }
        };

        statsThread.start();

        firehoseThread.join();
        keepChecking.set(false);
        long nanoTime3 = System.nanoTime();
        System.out.printf("Drained for %.2f seconds. %d outstanding.\n",
                (nanoTime3 - nanoTime2) / 1000000000.0, outstanding.size());
        System.out.printf("NTProcThatSlams reports %d starts and %d returns.\n",
                NTProcThatSlams.m_runCount.get(), NTProcThatSlams.m_returnCount.get());
        System.out.printf("TrivialNTProc reports %d starts and %d returns.\n",
                TrivialNTProc.m_runCount.get(), TrivialNTProc.m_returnCount.get());

        // CHECK STATS
        VoltTable statsT = getStats(client, "PROCEDURE");
        System.out.println("STATS: " + statsT.toFormattedString());

        assertTrue(VoltTableUtil.tableContainsString(statsT, "NTProcThatSlams", true));

        client.close();
        firehoseClient.close();
    }

    class VerifySuccessCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            assertTrue(clientResponse.getStatus() == ClientResponse.SUCCESS);
        }
    }

    public void testRunOnAllPartitionsProcedure() throws Exception {
        start();

        Client client = ClientFactory.createClient();
        client.createConnection("localhost");

        ClientResponseImpl response = null;

        // regular run
        System.out.println("regular run"); System.out.flush();
        response = (ClientResponseImpl) client.callProcedure("TestNTProcs$RunOnAllPartitionsNTProc", RunOnAllPartitionsNTProc.NORMAL);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.println("Client got trivial response");
        System.out.println(response.toJSONString());

        // nested proc will div by zero
        System.out.println("nested proc will div by zero"); System.out.flush();
        response = (ClientResponseImpl) client.callProcedure("TestNTProcs$RunOnAllPartitionsNTProc", RunOnAllPartitionsNTProc.SP_PROC_SHOULD_THROW);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        System.out.println("Client got trivial response");
        System.out.println(response.toJSONString());

        // nt proc will div by zero after initiating async work (scary)
        System.out.println("nt proc will div by zero after initiating async work (scary)\n"); System.out.flush();
        try {
            client.callProcedure("TestNTProcs$RunOnAllPartitionsNTProc", RunOnAllPartitionsNTProc.NT_PROC_SHOULD_THROW);
            fail();
        }
        catch (ProcCallException e) {
            response = (ClientResponseImpl) e.getClientResponse();
            assertEquals(ClientResponse.UNEXPECTED_FAILURE, response.getStatus());
            System.out.println("Client got trivial response");
            System.out.println(response.toJSONString());
        }

        // make sure system is still working and do a minor stress test by blasting 1000 healthy procs at it
        for (int i = 0; i < 1000; i++) {
            boolean success = client.callProcedure(new VerifySuccessCallback(), "TestNTProcs$RunOnAllPartitionsNTProc", RunOnAllPartitionsNTProc.NORMAL);
            assertTrue(success);
        }
        client.drain();

        // nt proc will call a proc that doesn't exist
        System.out.println("nt proc will call a proc that doesn't exist"); System.out.flush();
        try {
            ClientResponseImpl cri = (ClientResponseImpl) client.callProcedure("TestNTProcs$RunOnAllPartitionsNTProc", RunOnAllPartitionsNTProc.CALL_MISSING_PROC);
            System.err.println(cri.toJSONString());
            fail();
        }
        catch (ProcCallException e) {
            response = (ClientResponseImpl) e.getClientResponse();
            assertEquals(ClientResponse.UNEXPECTED_FAILURE, response.getStatus());
            System.out.println("Client got trivial response");
            System.out.println(response.toJSONString());
        }
    }

    class AvoidCatastropheCallback implements ProcedureCallback {

        final long index;
        final long timestamp;
        final Map<Long, AvoidCatastropheCallback> outstandingCallbacks;

        AvoidCatastropheCallback(long index, Map<Long, AvoidCatastropheCallback> outstandingCallbacks) {
            this.index = index;
            timestamp = System.currentTimeMillis();
            this.outstandingCallbacks = outstandingCallbacks;
        }

        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            outstandingCallbacks.remove(index);

            switch (clientResponse.getStatus()) {
            case ClientResponse.SUCCESS:
            case ClientResponse.CONNECTION_LOST:
            case ClientResponse.SERVER_UNAVAILABLE:
            case ClientResponse.RESPONSE_UNKNOWN:
                return;
            }

            // now we fail!
            System.err.println(clientResponse.getStatusString());
            ClientResponseImpl cri = (ClientResponseImpl) clientResponse;
            System.err.println(cri.toJSONString());
            fail();
        }

        @Override
        public String toString() {
            return String.format("CB(%d, %s)", index, new Date(timestamp).toString());
        }
    }

    public void testRunOnAllPartitionsWithNodeFailure() throws Exception {
        VoltProjectBuilder pb = new VoltProjectBuilder();
        pb.addLiteralSchema(SCHEMA);

        m_cluster = new LocalCluster("compileNT.jar", 6, 4, 2, BackendTarget.NATIVE_EE_JNI);
        m_cluster.setHasLocalServer(true);

        boolean success = m_cluster.compile(pb);
        assertTrue(success);

        m_cluster.startUp();

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setTopologyChangeAware(true);
        clientConfig.setConnectionResponseTimeout(5000);
        clientConfig.setConnectionResponseTimeout(200);
        clientConfig.setMaxOutstandingTxns(500);
        final Client firehoseClient = ClientFactory.createClient();
        firehoseClient.createConnection(m_cluster.getListenerAddress(0));

        System.out.println("Client connected");

        Map<Long, AvoidCatastropheCallback> outstandingCallbacks = new ConcurrentHashMap<>();


        AtomicBoolean keepPrintingCallbacksOutstanding = new AtomicBoolean(true);
        AtomicBoolean printDetail = new AtomicBoolean(false);
        Thread callbackStatusThread = new Thread() {
            @Override
            public void run() {
                while (keepPrintingCallbacksOutstanding.get()) {
                    try {
                        Thread.sleep(5000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    SortedMap<Long, AvoidCatastropheCallback> sortedCallbacks = new TreeMap<>(outstandingCallbacks);

                    if (sortedCallbacks.size() == 0) {
                        System.out.printf("TestNTProcs is tracking ZERO outstanding callbacks\n");
                        continue;
                    }

                    AvoidCatastropheCallback oldestCallback = sortedCallbacks.get(sortedCallbacks.firstKey());
                    assert(oldestCallback != null);

                    System.out.printf("TestNTProcs is tracking %d outstanding callbacks and the oldest is %s\n", outstandingCallbacks.size(), oldestCallback.toString());
                    if (printDetail.get()) {
                        for (AvoidCatastropheCallback callback : sortedCallbacks.values()) {
                            System.out.printf("  TestNTProcs is waiting for %s\n", callback.toString());
                        }
                    }
                }
            }
        };
        System.out.println("Starting status printer");
        callbackStatusThread.start();

        AtomicBoolean keepFirehosing = new AtomicBoolean(true);
        Thread firehoseThread = new Thread() {
            @Override
            public void run() {
                long index = 0;

                while (keepFirehosing.get()) {

                    try {
                        AvoidCatastropheCallback cb = new AvoidCatastropheCallback(index++, outstandingCallbacks);
                        outstandingCallbacks.put(cb.index, cb);

                        boolean status = firehoseClient.callProcedure(cb,
                                                                      "TestNTProcs$RunOnAllPartitionsNTProc",
                                                                      RunOnAllPartitionsNTProc.NORMAL);
                        assertTrue(status);
                    }
                    catch (NoConnectionsException e) {
                        // i'm cool with this exception
                    }
                    catch (IOException e) {
                        e.printStackTrace();
                        fail();
                    }
                }
                try {
                    firehoseClient.drain();
                } catch (NoConnectionsException | InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                    fail();
                }
            }
        };

        System.out.println("Starting firehose");
        firehoseThread.start();

        AtomicBoolean keepKilling = new AtomicBoolean(true);
        Random rand = new Random(0);

        Thread killRejoinThread = new Thread() {
            @Override
            public void run() {
                while (keepKilling.get()) {
                    try {
                        int nodeCount = m_cluster.getNodeCount();
                        int killNode = rand.nextInt(nodeCount);
                        m_cluster.killSingleHost(killNode);
                        m_cluster.rejoinOne(killNode);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        fail();
                    }
                }

            }
        };

        System.out.println("Starting kill/rejoin thread");
        killRejoinThread.start();

        Thread.sleep(10000);

        keepKilling.set(false);
        System.out.println("Stopping kill/rejoin thread");
        killRejoinThread.join(20000);

        keepFirehosing.set(false);
        Thread.sleep(1000);
        printDetail.set(false);
        System.out.println("Stopping firehose thread");
        firehoseThread.join(10000);

        System.out.println("Draining client");
        firehoseClient.drain();
        firehoseClient.close();

        System.out.println("Stopping callback status thread");
        keepPrintingCallbacksOutstanding.set(false);
        callbackStatusThread.join(10000);
    }
}
