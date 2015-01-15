/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.voltdb.ServerThread;
import org.voltdb.TableHelper;
import org.voltdb.VoltDB;
import org.voltdb.VoltDB.Configuration;
import org.voltdb.VoltTable;
import org.voltdb.compiler.CatalogBuilder;
import org.voltdb.compiler.DeploymentBuilder;
import org.voltdb.utils.MiscUtils;

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

        ClientConfig config = new ClientConfig(null, null, csl);
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

        if (MiscUtils.isPro()) {
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
            ((ClientImpl) client).callProcedureWithTimeout("ArbitraryDurationProc", 3, TimeUnit.SECONDS, 6000);
        } catch (ProcCallException ex) {
            assertEquals(ClientResponse.CONNECTION_TIMEOUT, ex.m_response.getStatus());
            exceptionCalled = true;
        }
        assertTrue(exceptionCalled);

        //larger timeout than proc wait duration
        exceptionCalled = false;
        try {
            // Query timeout is in seconds second arg.
            ((ClientImpl) client).callProcedureWithTimeout("ArbitraryDurationProc", 30, TimeUnit.SECONDS, 6000);
        } catch (ProcCallException ex) {
            exceptionCalled = true;
        }
        assertFalse(exceptionCalled);

        //no timeout of 0
        try {
            // Query timeout is in seconds second arg.
            ((ClientImpl) client).callProcedureWithTimeout("ArbitraryDurationProc", 0, TimeUnit.SECONDS, 2000);
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
        ((ClientImpl) client).callProcedureWithTimeout(new MyCallback(), "ArbitraryDurationProc", 3, TimeUnit.SECONDS, 6000);
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
        ((ClientImpl) client).callProcedureWithTimeout(new MyCallback2(), "ArbitraryDurationProc", 30, TimeUnit.SECONDS, 6000);
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
        ((ClientImpl) client).callProcedureWithTimeout(new MyCallback3(), "ArbitraryDurationProc", 0, TimeUnit.SECONDS, 6000);
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
        ((ClientImpl) client).callProcedureWithTimeout(new MyCallback4(), "ArbitraryDurationProc", 50, TimeUnit.NANOSECONDS, 6000);
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

        for (int i = 0; i < 40; i++) {
            if (client.getConnectedHostList().size() > 0) {
                return;
            }
            Thread.sleep(500);
        }

        fail("Client should have been reconnected");
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

        long start = System.nanoTime();
        assertTrue(client.backpressureBarrier(System.nanoTime(), TimeUnit.MILLISECONDS.toNanos(200)));
        long delta = System.nanoTime() - start;
        assertTrue(delta > TimeUnit.MILLISECONDS.toNanos(200));
        assertTrue(delta < TimeUnit.MINUTES.toNanos(1));

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
        assertTrue(delta < TimeUnit.MINUTES.toNanos(1));
        assertTrue(delta > TimeUnit.MILLISECONDS.toNanos(20));
    }
}
