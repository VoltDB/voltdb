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

package org.voltdb.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.ServerThread;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;

import junit.framework.TestCase;

public class TestClientFeatures extends TestCase {

    ServerThread localServer;
    DeploymentBuilder depBuilder;

    @Override
    public void setUp()
    {
        try {
            CatalogBuilder catBuilder = new CatalogBuilder();
            catBuilder.addSchema(getClass().getResource("clientfeatures.sql"));
            catBuilder.addProcedures(ArbitraryDurationProc.class);

            boolean success = catBuilder.compile(Configuration.getPathToCatalogForTest("timeouts.jar"));
            assert(success);

            depBuilder = new DeploymentBuilder(1, 1, 0);
            depBuilder.writeXML(Configuration.getPathToCatalogForTest("timeouts.xml"));

            VoltDB.Configuration config = new VoltDB.Configuration();
            config.m_pathToCatalog = Configuration.getPathToCatalogForTest("timeouts.jar");
            config.m_pathToDeployment = Configuration.getPathToCatalogForTest("timeouts.xml");
            localServer = new ServerThread(config);
            localServer.start();
            localServer.waitForInitialization();
        }
        catch (Exception e) {
            e.printStackTrace();
            fail();
        }
    }

    @Override
    public void tearDown() throws Exception {
        localServer.shutdown();
    }

    class CSL extends ClientStatusListenerExt {

        AtomicBoolean m_gotCall = new AtomicBoolean(false);

        @Override
        public synchronized void lateProcedureResponse(ClientResponse r, String hostname, int port) {
            m_gotCall.set(true);
        }

        public boolean waitForCall(long timeout) {
            long start = System.currentTimeMillis();
            long now = start;
            while ((m_gotCall.get() == false) && ((now - start) < timeout)) {
                Thread.yield();
                now = System.currentTimeMillis();
            }
            return m_gotCall.get();
        }
    }

    public void testPerCallTimeout() throws Exception {
        CSL csl = new CSL();

        ClientConfig config = new ClientConfig(null, null, csl, ClientAuthScheme.HASH_SHA1);
        config.setProcedureCallTimeout(500);
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");

        ClientResponse response = client.callProcedure("ArbitraryDurationProc", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        try {
            client.callProcedure("ArbitraryDurationProc", 3000);
            fail();
        }
        catch (ProcCallException e) {
            assertTrue(e.getMessage().startsWith("No response received in the allotted time"));
        }
        // make sure the callback gets called
        assertTrue(csl.waitForCall(6000));

        //
        // From here down test special exception for slow snapshots or catalogs updates
        // - Both features are pro only
        //

        // build a catalog with a ton of indexes so catalog update will be slow
        CatalogBuilder builder = new CatalogBuilder();
        builder.addSchema(getClass().getResource("clientfeatures-wellindexed.sql"));
        builder.addProcedures(ArbitraryDurationProc.class);
        byte[] catalogToUpdate = builder.compileToBytes();
        assert(catalogToUpdate != null);

        // make a copy of the table from ddl for loading
        // (shouldn't have to do this, but for now, the table loader requires
        //  a VoltTable, and can't read schema. Could fix by using this VoltTable
        //  to generate schema or by teaching to loader how to discover tables)
        TableHelper.Configuration helperConfig = new TableHelper.Configuration();
        helperConfig.rand = new Random();
        TableHelper helper = new TableHelper(helperConfig);
        VoltTable t = TableHelper.quickTable("indexme (pkey:bigint, " +
                                             "c01:varchar63, " +
                                             "c02:varchar63, " +
                                             "c03:varchar63, " +
                                             "c04:varchar63, " +
                                             "c05:varchar63, " +
                                             "c06:varchar63, " +
                                             "c07:varchar63, " +
                                             "c08:varchar63, " +
                                             "c09:varchar63, " +
                                             "c10:varchar63) " +
                                             "PKEY(pkey)");
        // get a client with a normal timout
        Client client2 = ClientFactory.createClient();
        client2.createConnection("localhost");
        helper.fillTableWithBigintPkey(t, 400, 0, client2, 0, 1);

        long start;
        double duration;

        // run a catalog update that *might* normally timeout
        start = System.nanoTime();
        response = client.callProcedure("@UpdateApplicationCatalog", catalogToUpdate, depBuilder.getXML());
        duration = (System.nanoTime() - start) / 1000000000.0;
        System.out.printf("Catalog update duration in seconds: %.2f\n", duration);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        // run a blocking snapshot that *might* normally timeout
        start = System.nanoTime();
        response = client.callProcedure("@SnapshotSave", Configuration.getPathToCatalogForTest(""), "slow", 1);
        duration = (System.nanoTime() - start) / 1000000000.0;
        System.out.printf("Snapshot save duration in seconds: %.2f\n", duration);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }

    public void testMaxTimeout() throws NoConnectionsException, IOException, ProcCallException {
        CSL csl = new CSL();

        ClientConfig config = new ClientConfig(null, null, csl);
        config.setProcedureCallTimeout(0);
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");

        ClientResponse response = client.callProcedure("ArbitraryDurationProc", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());

        client.callProcedure("ArbitraryDurationProc", 3000);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
    }

    public void testQueryTimeout() throws NoConnectionsException, IOException, ProcCallException {
        CSL csl = new CSL();

        ClientConfig config = new ClientConfig(null, null, csl);
        config.setProcedureCallTimeout(0);
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");

        ClientResponse response = client.callProcedure("ArbitraryDurationProc", 0);
        assertEquals(ClientResponse.SUCCESS, response.getStatus());
        boolean exceptionCalled = false;
        try {
            // Query timeout is in seconds second arg.
            client.callProcedureWithClientTimeout(BatchTimeoutOverrideType.NO_TIMEOUT,
                    "ArbitraryDurationProc", 3, TimeUnit.SECONDS, 6000);
        } catch (ProcCallException ex) {
            assertEquals(ClientResponse.CONNECTION_TIMEOUT, ex.m_response.getStatus());
            exceptionCalled = true;
        }
        assertTrue(exceptionCalled);

        //larger timeout than proc wait duration
        exceptionCalled = false;
        try {
            // Query timeout is in seconds second arg.
            client.callProcedureWithClientTimeout(BatchTimeoutOverrideType.NO_TIMEOUT,
                    "ArbitraryDurationProc", 30, TimeUnit.SECONDS, 6000);
        } catch (ProcCallException ex) {
            exceptionCalled = true;
        }
        assertFalse(exceptionCalled);

        //no timeout of 0
        try {
            // Query timeout is in seconds second arg.
            client.callProcedureWithClientTimeout(BatchTimeoutOverrideType.NO_TIMEOUT,
                    "ArbitraryDurationProc", 0, TimeUnit.SECONDS, 2000);
        } catch (ProcCallException ex) {
            exceptionCalled = true;
        }
        assertFalse(exceptionCalled);

        final CountDownLatch latch = new CountDownLatch(1);
        class MyCallback implements ProcedureCallback {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                assert (clientResponse.getStatus() == ClientResponse.CONNECTION_TIMEOUT);
                System.out.println("Async Query timeout called..");
                latch.countDown();
            }
        }
        // Query timeout is in seconds third arg.
        //Async versions
        client.callProcedureWithClientTimeout(new MyCallback(), BatchTimeoutOverrideType.NO_TIMEOUT,
                "ArbitraryDurationProc", 3, TimeUnit.SECONDS, 6000);
        try {
            latch.await();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            fail();
        }

        //Async versions - does not timeout.
        final CountDownLatch latch2 = new CountDownLatch(1);
        class MyCallback2 implements ProcedureCallback {

            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                assert (clientResponse.getStatus() == ClientResponse.SUCCESS);
                latch2.countDown();
            }
        }
        // Query timeout is in seconds third arg.
        client.callProcedureWithClientTimeout(new MyCallback2(), BatchTimeoutOverrideType.NO_TIMEOUT,
                "ArbitraryDurationProc", 30, TimeUnit.SECONDS, 6000);
        try {
            latch2.await();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            fail();
        }

        //Async versions - 0 timeout.
        final CountDownLatch latch3 = new CountDownLatch(1);
        class MyCallback3 implements ProcedureCallback {

            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                assert (clientResponse.getStatus() == ClientResponse.SUCCESS);
                latch3.countDown();
            }
        }
        // Query timeout is in seconds third arg.
        client.callProcedureWithClientTimeout(new MyCallback3(), BatchTimeoutOverrideType.NO_TIMEOUT,
                "ArbitraryDurationProc", 0, TimeUnit.SECONDS, 6000);
        try {
            latch3.await();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            fail();
        }

        final CountDownLatch latch4 = new CountDownLatch(1);
        class MyCallback4 implements ProcedureCallback {

            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                assert (clientResponse.getStatus() == ClientResponse.CONNECTION_TIMEOUT);
                latch4.countDown();
            }
        }

        /*
         * Check that a super tiny timeout triggers fast
         */
        client.callProcedureWithClientTimeout(new MyCallback4(), BatchTimeoutOverrideType.NO_TIMEOUT,
                "ArbitraryDurationProc", 50, TimeUnit.NANOSECONDS, 6000);
        final long start = System.nanoTime();
        try {
            latch4.await();
        } catch (InterruptedException ex) {
            ex.printStackTrace();
            fail();
        }
        final long delta = System.nanoTime() - start;
        assertTrue(TimeUnit.NANOSECONDS.toSeconds(delta) < 1);
    }

    /**
     * Nonblocking async mode. This test forces backpressure based on
     * hitting the max outstanding transactions limit.
     */
    Object backpressureEvent = new Object();
    boolean backpressureOn = false;

    class NbCSL extends ClientStatusListenerExt {
        @Override
        public void backpressure(boolean status) {
            synchronized (backpressureEvent) {
                System.out.printf("Nonblocking async test: backpressure: %s -> %s%n", backpressureOn, status);
                backpressureOn = status;
                if (!status) { // backpressure removed
                    backpressureEvent.notify();
                }
            }
        }
    }

    CountDownLatch nbReady = new CountDownLatch(1);

    class NbCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) throws Exception {
            assertEquals(ClientResponse.SUCCESS, clientResponse.getStatus());
            nbReady.countDown();
        }
    }

    public void testNonblockingAsync() throws Exception {
        NbCSL csl = new NbCSL();
        NbCallback nbcb = new NbCallback();

        ClientConfig config = new ClientConfig(null, null, csl);
        config.setNonblockingAsync(); // default, 500 uS
        config.setMaxOutstandingTxns(1); // ridiculously low for testing
        config.setBackpressureQueueThresholds(1, 100_000); // 1 request, 100,000 bytes

        // Validate that setters are setting
        assertEquals(500_000, config.getNonblockingAsync());
        assertEquals(1, config.getMaxOutstandingTxns());
        assertEquals(1, config.getBackpressureQueueThresholds()[0]);
        assertEquals(100_000, config.getBackpressureQueueThresholds()[1]);

        // Connect is still synchronous
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");

        // Some common timeouts
        final int shortCall = 10;
        final int longCall = 1000;
        final int waitTmo = 2 * longCall;

        // We may be backpressured from connection setup; this is an artifact
        // of testing with a low limit on outstanding requests. Use a throwaway
        // request to clear it (we have to make a request because of how the
        // implementation reports backpressure)
        boolean b0 = client.callProcedure(nbcb, "ArbitraryDurationProc", shortCall);
        if (b0) { // it was accepted, wait for it to finish
            b0 = nbReady.await(waitTmo, TimeUnit.MILLISECONDS);
            assertTrue("initial call did not complete", b0);
        } else { // it was refused, wait for backpressure to clear
            synchronized (backpressureEvent) {
                if (backpressureOn) {
                    backpressureEvent.wait(waitTmo);
                }
                assertFalse("initial backpressure did not clear", backpressureOn);
            }
        }

        // Based on 1 outstanding txn max, expect 2nd call to be refused
        // since 1st call takes 1 sec to complete
        boolean b1 = client.callProcedure(nbcb, "ArbitraryDurationProc", longCall);
        assertTrue("first call refused", b1);
        boolean b2 = client.callProcedure(nbcb, "ArbitraryDurationProc", shortCall);
        assertFalse("second call accepted", b2);

        // Wait for no backpressure (first call completion) with time limit
        // Note the test for backpressure initially being on assumes we can
        // get here before the first call completes after 1 second.
        synchronized (backpressureEvent) {
            assertTrue("backpressure not on", backpressureOn);
            backpressureEvent.wait(waitTmo); // no loop used; should be no spurious wakeups
            assertFalse("backpressure still on", backpressureOn);
        }

        // Now we can send the second request
        boolean b2a = client.callProcedure(nbcb, "ArbitraryDurationProc", shortCall);
        assertTrue("second call refused", b2a);

        // Wait on completion
        client.drain();
        client.close();
    }

    /**
     * Nonblocking async mode incompatibility checks
     */
    public void testNonblockingVersusRateLimiting() throws Exception {

        boolean gotExc1 = false;
        ClientConfig config1 = new ClientConfig();
        config1.setMaxTransactionsPerSecond(100_000);
        try {
            config1.setNonblockingAsync(250_000);
        } catch (IllegalStateException ex) {
            System.out.printf("setNonblockingAsync got expected exception: %s\n", ex.getMessage());
            gotExc1 = true;
        }
        assertTrue("setNonblockingAsync succeeded, should have failed", gotExc1);
        assertEquals(-1, config1.getNonblockingAsync());

        boolean gotExc2 = false;
        ClientConfig config2 = new ClientConfig();
        config2.setNonblockingAsync(250_000);
        try {
            config2.setMaxTransactionsPerSecond(100_000);
        } catch (IllegalStateException ex) {
            System.out.printf("setMaxTransactionsPerSecond got expected exception: %s\n", ex.getMessage());
            gotExc2 = true;
        }
        assertTrue("setMaxTransactionsPerSecond succeeded, should have failed", gotExc2);
        assertEquals(250_000, config2.getNonblockingAsync());
    }

    /**
     * Verify a client can reconnect to a cluster that has been restarted.
     */
    public void testReconnect() throws Exception {
        ClientConfig config = new ClientConfig();
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");

        tearDown();

        for (int i = 0; (i < 40) && (client.getConnectedHostList().size() > 0); i++) {
            Thread.sleep(500);
        }
        assertTrue(client.getConnectedHostList().isEmpty());

        setUp();

        client.createConnection("localhost");

        assertFalse(client.getConnectedHostList().isEmpty());
    }

    /**
     * Verify a client can reconnect automatically if reconnect on connection loss feature is turned on
     *
     * Then verify that nothing is still going when all it shutdown
     */
    public void testAutoReconnect() throws Exception {
        ClientConfig config = new ClientConfig();
        config.setReconnectOnConnectionLoss(true);
        Client client = ClientFactory.createClient(config);
        client.createConnection("localhost");

        tearDown();

        for (int i = 0; (i < 40) && (client.getConnectedHostList().size() > 0); i++) {
            Thread.sleep(500);
        }
        assertTrue(client.getConnectedHostList().isEmpty());

        // sleep before server restart to force some reconnect failures
        Thread.sleep(2000);

        setUp();

        boolean failed = true;
        for (int i = 0; i < 40; i++) {
            if (client.getConnectedHostList().size() > 0) {
                failed = false;
                break;
            }
            Thread.sleep(500);
        }
        if (failed) {
            fail("Client should have been reconnected");
        }

        tearDown();

        for (int i = 0; (i < 40) && (client.getConnectedHostList().size() > 0); i++) {
            Thread.sleep(500);
        }
        assertTrue(client.getConnectedHostList().isEmpty());

        client.close();

        // hunt for reconnect thread to make sure it's gone
        Map<Thread, StackTraceElement[]> stMap = Thread.getAllStackTraces();
        for (Entry<Thread, StackTraceElement[]> e : stMap.entrySet()) {
            StackTraceElement[] st = e.getValue();
            Thread t = e.getKey();

            // skip the current thread
            if (t == Thread.currentThread()) {
                continue;
            }

            for (StackTraceElement ste : st) {
                if (ste.getClassName().toLowerCase().contains("voltdb.client")) {
                    System.err.println(ste.getClassName().toLowerCase());
                    fail("Something failed to clean up.");
                }
            }
        }
    }

    public void testGetAddressList() throws UnknownHostException, IOException, InterruptedException {
        CSL csl = new CSL();

        ClientConfig config = new ClientConfig(null, null, csl);
        config.setProcedureCallTimeout(0);
        Client client = ClientFactory.createClient(config);

        List<InetSocketAddress> addrs = client.getConnectedHostList();
        assertEquals(0, addrs.size());
        client.createConnection("localhost");
        addrs = client.getConnectedHostList();
        assertEquals(1, addrs.size());
        assertEquals(VoltDB.DEFAULT_PORT, addrs.get(0).getPort());
        client.close();
        addrs = client.getConnectedHostList();
        assertEquals(0, addrs.size());
    }

    public void testBackpressureTimeout() throws Exception {
        final ClientImpl client = (ClientImpl)ClientFactory.createClient();
        client.createConnection("localhost");
        client.backpressureBarrier(System.nanoTime(), TimeUnit.DAYS.toNanos(1));
        client.m_listener.backpressure(true);

        // Backpressure is on. Wait with a timeout of 200 mS.
        // Expect completion somewhere in the interval (200 mS, 1 min)
        // and backpressure to remain on: we timed out.
        long start = System.nanoTime();
        assertTrue(client.backpressureBarrier(System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(200)));
        long delta = System.nanoTime() - start;
        assertTrue(delta > TimeUnit.MILLISECONDS.toNanos(200));
        assertTrue(delta < TimeUnit.MINUTES.toNanos(1));

        // Backpressure is on. Arrange to turn it off in 20 mS.
        // Wait with a timeout of 1 min. Expect completion somewhere
        // in the interval (20 mS, 1 min) and backpressure to now
        // be off: we did not time out.
        start = System.nanoTime();
        new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(20);
                    client.m_listener.backpressure(false);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
        assertFalse(client.backpressureBarrier(System.nanoTime(), TimeUnit.MINUTES.toNanos(1)));
        delta = System.nanoTime() - start;
        assertTrue(delta < TimeUnit.MINUTES.toNanos(1));
        assertTrue(delta > TimeUnit.MILLISECONDS.toNanos(20));
    }

    public void testDefaultConfigValues() {
        final ClientConfig dut = new ClientConfig();
        assertEquals(ClientAuthScheme.HASH_SHA256, dut.m_hashScheme);
        assertTrue(dut.m_username.isEmpty());
        assertTrue(dut.m_password.isEmpty());
        assertTrue(dut.m_cleartext);
        assertFalse(dut.m_heavyweight);
        assertEquals(3000, dut.m_maxOutstandingTxns);
        assertEquals(Integer.MAX_VALUE, dut.m_maxTransactionsPerSecond);
        assertEquals(TimeUnit.MINUTES.toNanos(2), dut.m_procedureCallTimeoutNanos);
        assertEquals(TimeUnit.MINUTES.toMillis(2), dut.m_connectionResponseTimeoutMS);
        assertFalse(dut.m_reconnectOnConnectionLoss);
        assertEquals(TimeUnit.SECONDS.toMillis(1), dut.m_initialConnectionRetryIntervalMS);
        assertEquals(TimeUnit.SECONDS.toMillis(8), dut.m_maxConnectionRetryIntervalMS);
    }
}
