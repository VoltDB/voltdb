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
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.voltcore.network.Connection;
import org.voltcore.network.QueueMonitor;
import org.voltcore.network.ReverseDNSCache;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.network.VoltProtocolHandler;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import junit.framework.TestCase;

public class TestDistributer extends TestCase {

    class MockInputHandler extends VoltProtocolHandler {

        volatile boolean gotPing = false;
        AtomicBoolean sendResponses = new AtomicBoolean(true);
        AtomicBoolean sendProcTimeout = new AtomicBoolean(false);
        volatile Semaphore invokedSubscribe = new Semaphore(0);
        volatile Semaphore invokedTopology = new Semaphore(0);
        volatile Semaphore invokedSystemCatalog = new Semaphore(0);

        @Override
        public int getMaxRead() {
            return 8192;
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) {
            try {
                StoredProcedureInvocation spi = new StoredProcedureInvocation();
                spi.initFromBuffer(message);

                final String proc = spi.getProcName();

                // record if we got a ping
                if (proc.equals("@Ping"))
                    gotPing = true;

                if (sendResponses.get()) {
                    VoltTable vt[] = new VoltTable[0];
                    if (proc.equals("@Subscribe")) {
                        invokedSubscribe.release();
                    } else if (proc.equals("@Statistics")) {
                        invokedTopology.release();
                    } else if (proc.equals("@SystemCatalog")) {
                        invokedSystemCatalog.release();
                    } else {
                        vt = new VoltTable[1];
                        vt[0] = new VoltTable(new VoltTable.ColumnInfo("Foo", VoltType.BIGINT));
                        vt[0].addRow(1);
                    }
                    ClientResponseImpl response;
                    if (sendProcTimeout.get()) {
                        response = new ClientResponseImpl(ClientResponseImpl.CONNECTION_TIMEOUT, vt,
                                "Timeout String", spi.getClientHandle());
                    } else {
                        response = new ClientResponseImpl(ClientResponseImpl.SUCCESS, vt,
                                "Extra String", spi.getClientHandle());
                    }
                    ByteBuffer buf = ByteBuffer.allocate(4 + response.getSerializedSize());
                    buf.putInt(buf.capacity() - 4);
                    response.flattenToBuffer(buf);
                    buf.clear();
                    c.writeStream().enqueue(buf);
                    roundTrips.incrementAndGet();
                    System.err.printf("Sending response for %s.%n", proc);
                }
                else {
                    System.err.printf("Witholding response for %s.%n", proc);
                }
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public void started(Connection c) {
            // TODO Auto-generated method stub

        }

        @Override
        public void starting(Connection c) {
            // TODO Auto-generated method stub

        }

        @Override
        public void stopped(Connection c) {
            // TODO Auto-generated method stub

        }

        @Override
        public void stopping(Connection c) {
            // TODO Auto-generated method stub

        }
        AtomicInteger roundTrips = new AtomicInteger();

        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {}
            };
        }

        @Override
        public Runnable onBackPressure() {
            return new Runnable() {
                @Override
                public void run() {}
            };
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return null;
        }

        public void stopResponding() {

        }
    }

    // A fake server.
    class MockVolt extends Thread {
        boolean handleConnection = true;
        MockVolt(int port) throws IOException {
            network = new VoltNetworkPool();
            network.start();
            socket = ServerSocketChannel.open();
            socket.configureBlocking(false);
            socket.socket().bind(new InetSocketAddress(port));
        }

        @Override
        public void run() {
            try {
                while (shutdown.get() == false) {
                    SocketChannel client = socket.accept();
                    if (client != null) {
                        client.configureBlocking(true);
                        final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);//Extra byte for version also
                        client.read(lengthBuffer);
                        final ByteBuffer versionBuffer = ByteBuffer.allocate(1);//Extra byte for version also
                        client.read(versionBuffer);
                        versionBuffer.flip();
                        final ByteBuffer schemeBuffer = ByteBuffer.allocate(1);//Extra byte for scheme also
                        client.read(schemeBuffer);
                        schemeBuffer.flip();
                        ClientAuthScheme scheme = ClientAuthScheme.get(schemeBuffer.get());

                        final ByteBuffer serviceLengthBuffer = ByteBuffer.allocate(4);
                        while (serviceLengthBuffer.remaining() > 0)
                            client.read(serviceLengthBuffer);
                        serviceLengthBuffer.flip();
                        ByteBuffer serviceBuffer = ByteBuffer.allocate(serviceLengthBuffer.getInt());
                        while (serviceBuffer.remaining() > 0)
                            client.read(serviceBuffer);
                        serviceBuffer.flip();

                        final ByteBuffer usernameLengthBuffer = ByteBuffer.allocate(4);
                        while (usernameLengthBuffer.remaining() > 0)
                            client.read(usernameLengthBuffer);
                        usernameLengthBuffer.flip();
                        final int usernameLength = usernameLengthBuffer.getInt();
                        final ByteBuffer usernameBuffer = ByteBuffer.allocate(usernameLength);
                        while (usernameBuffer.remaining() > 0)
                            client.read(usernameBuffer);
                        usernameBuffer.flip();

                        final ByteBuffer passwordBuffer = ByteBuffer.allocate(ClientAuthScheme.getDigestLength(scheme));
                        while (passwordBuffer.remaining() > 0)
                            client.read(passwordBuffer);
                        passwordBuffer.flip();

                        final byte usernameBytes[] = new byte[usernameLength];
                        final byte passwordBytes[] = new byte[ClientAuthScheme.getDigestLength(scheme)];
                        usernameBuffer.get(usernameBytes);
                        passwordBuffer.get(passwordBytes);

                        @SuppressWarnings("unused")
                        final String username = new String(usernameBytes);

                        final ByteBuffer responseBuffer = ByteBuffer.allocate(34);
                        responseBuffer.putInt(30);
                        responseBuffer.put((byte)0);//version
                        responseBuffer.put((byte)0);//success response
                        responseBuffer.putInt(0);//hostId
                        responseBuffer.putLong(0);//connectionId
                        responseBuffer.putLong(0);//instanceId
                        responseBuffer.putInt(0);//instanceId pt 2
                        responseBuffer.putInt(0);
                        responseBuffer.flip();
                        handler = new MockInputHandler();
                        client.write(responseBuffer);

                        client.configureBlocking(false);
                        channels.add(client);
                        if (handleConnection) {
                            network.registerChannel( client, handler, null, null);
                        }
                    }
                    Thread.yield();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        public void shutdown() throws InterruptedException {
            shutdown.set(true);
            join();

            try {
                network.shutdown();
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            try {
                socket.close();
            }
            catch (IOException ignored) {
            }
            for (SocketChannel sc : channels) {
                try {
                    sc.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        private AtomicBoolean shutdown = new AtomicBoolean(false);
        volatile ServerSocketChannel socket = null;
        volatile MockInputHandler handler = null;
        volatile VoltNetworkPool network;
        List<SocketChannel> channels = new ArrayList<>();
    }

    private static class CSL extends ClientStatusListenerExt {
        private volatile boolean m_exceptionHandled = false;
        @Override
        public void uncaughtException(ProcedureCallback callback,
                ClientResponse r, Throwable e) {
            m_exceptionHandled = true;
        }
    }

    public class ProcCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            System.err.println("Ran callback.");
        }
    }

    public class ThrowingCallback implements ProcedureCallback {
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            throw new RuntimeException();
        }
    }

    @Override
    public void setUp()
    {
        System.out.printf("=-=-=-=-=-=-= Starting test %s =-=-=-=-=-=-=\n", getName());
        // Reset client factory state to original
        ClientFactory.m_preserveResources = false;
        while (ClientFactory.m_activeClientCount > 0) {
            try {
                ClientFactory.decreaseClientNum();
            }
            catch (InterruptedException e) {}
        }
        // The DNS cache is always initialized in the started state
        ReverseDNSCache.start();
    }

    @Override
    public void tearDown() throws Exception {
        System.out.printf("=-=-=-=-=-=-= End of test %s =-=-=-=-=-=-=\n", getName());
    }

    @Test
    public void testCreateConnection() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        MockVolt volt0 = null;
        MockVolt volt1 = null;
        try {
            // create a fake server and connect to it.
            volt0 = new MockVolt(20000);
            volt0.start();

            volt1 = new MockVolt(20001);
            volt1.start();

            assertTrue(volt1.socket.isOpen());
            assertTrue(volt0.socket.isOpen());

            // And a distributer
            Distributer dist = new Distributer();
            dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA1);
            dist.createConnection("localhost", "", "", 20001, ClientAuthScheme.HASH_SHA1);

            Thread.sleep(1000);
            assertTrue(volt1.handler != null);
            assertTrue(volt0.handler != null);
        }
        finally {
            if (volt0 != null) {
                volt0.shutdown();
            }
            if (volt1 != null) {
                volt1.shutdown();
            }
        }
    }

    @Test
    public void testAuthenticationTimeout() throws Exception {
        MockVolt volt0 = null;
        try {
            // create a fake server but don't start it.
            // It will not read anything from the socket.
            volt0 = new MockVolt(20000);

            // Create a new distributor with a short connection timeout.
            // Authentication should time out because the server is not
            // reading anything.
            Distributer dist = new Distributer(
                    false,
                    ClientConfig.DEFAULT_PROCEDURE_TIMEOUT_NANOS,
                    10,
                    null,
                    null);
            dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA1);

            fail("Should have timed out");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("timed out"));
        } finally {
            if (volt0 != null) {
                volt0.shutdown();
            }
        }
    }

    @Test
    public void testCreateConnectionSha256() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        MockVolt volt0 = null;
        MockVolt volt1 = null;
        try {
            // create a fake server and connect to it.
            volt0 = new MockVolt(20000);
            volt0.start();

            volt1 = new MockVolt(20001);
            volt1.start();

            assertTrue(volt1.socket.isOpen());
            assertTrue(volt0.socket.isOpen());

            // And a distributer
            Distributer dist = new Distributer();
            dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA256);
            dist.createConnection("localhost", "", "", 20001, ClientAuthScheme.HASH_SHA256);

            Thread.sleep(1000);
            assertTrue(volt1.handler != null);
            assertTrue(volt0.handler != null);
        }
        finally {
            if (volt0 != null) {
                volt0.shutdown();
            }
            if (volt1 != null) {
                volt1.shutdown();
            }
        }
    }

    @Test
    public void testCreateConnectionMixHashed() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        MockVolt volt0 = null;
        MockVolt volt1 = null;
        try {
            // create a fake server and connect to it.
            volt0 = new MockVolt(20000);
            volt0.start();

            volt1 = new MockVolt(20001);
            volt1.start();

            assertTrue(volt1.socket.isOpen());
            assertTrue(volt0.socket.isOpen());

            // And a distributer
            Distributer dist = new Distributer();
            dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA1);
            dist.createConnection("localhost", "", "", 20001, ClientAuthScheme.HASH_SHA256);

            Thread.sleep(1000);
            assertTrue(volt1.handler != null);
            assertTrue(volt0.handler != null);
        }
        finally {
            if (volt0 != null) {
                volt0.shutdown();
            }
            if (volt1 != null) {
                volt1.shutdown();
            }
        }
    }

    @Test
    public void testQueue() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        // Uncongested connections get round-robin use.
        MockVolt volt0, volt1, volt2;
        int handle = 0;
        volt0 = volt1 = volt2 = null;
        try {
            volt0 = new MockVolt(20000);
            volt0.start();
            volt1 = new MockVolt(20001);
            volt1.start();
            volt2 = new MockVolt(20002);
            volt2.start();

            CSL csl = new CSL();

            Distributer dist = new Distributer(false,
                    ClientConfig.DEFAULT_PROCEDURE_TIMEOUT_NANOS,
                    ClientConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                    null /* subject */, null);
            dist.addClientStatusListener(csl);
            dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA1);
            dist.createConnection("localhost", "", "", 20001, ClientAuthScheme.HASH_SHA1);
            dist.createConnection("localhost", "", "", 20002, ClientAuthScheme.HASH_SHA1);

            assertTrue(volt1.handler != null);
            assertTrue(volt0.handler != null);
            assertTrue(volt2.handler != null);

            ProcedureInvocation pi1 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi2 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi3 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi4 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi5 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi6 = new ProcedureInvocation(++handle, "i1", new Integer(1));

            dist.queue(pi1, new ThrowingCallback(), true, System.nanoTime(), 0);
            dist.drain();
            assertTrue(csl.m_exceptionHandled);
            dist.queue(pi2, new ProcCallback(), true, System.nanoTime(), 0);
            dist.queue(pi3, new ProcCallback(), true, System.nanoTime(), 0);
            dist.queue(pi4, new ProcCallback(), true, System.nanoTime(), 0);
            dist.queue(pi5, new ProcCallback(), true, System.nanoTime(), 0);
            dist.queue(pi6, new ProcCallback(), true, System.nanoTime(), 0);

            dist.drain();
            System.err.println("Finished drain.");

            // Invocations are round-robin because no hashinator is present
            // However node 0 gets 4 more invocations to subscribe to topology updates
            assertEquals(6, volt0.handler.roundTrips.get());
            assertEquals(2, volt1.handler.roundTrips.get());
            assertEquals(2, volt2.handler.roundTrips.get());


        }
        finally {
            if (volt0 != null) {
                volt0.shutdown();
            }
            if (volt1 != null) {
                volt1.shutdown();
            }
            if (volt2 != null) {
                volt2.shutdown();
            }
        }
    }

    @Test
    public void testQueueMixed() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        // Uncongested connections get round-robin use.
        MockVolt volt0, volt1, volt2;
        int handle = 0;
        volt0 = volt1 = volt2 = null;
        try {
            volt0 = new MockVolt(20000);
            volt0.start();
            volt1 = new MockVolt(20001);
            volt1.start();
            volt2 = new MockVolt(20002);
            volt2.start();

            CSL csl = new CSL();

            Distributer dist = new Distributer(false,
                    ClientConfig.DEFAULT_PROCEDURE_TIMEOUT_NANOS,
                    ClientConfig.DEFAULT_CONNECTION_TIMEOUT_MS,
                    null /* subject */, null);
            dist.addClientStatusListener(csl);
            dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA1);
            dist.createConnection("localhost", "", "", 20001, ClientAuthScheme.HASH_SHA256);
            dist.createConnection("localhost", "", "", 20002, ClientAuthScheme.HASH_SHA1);

            assertTrue(volt1.handler != null);
            assertTrue(volt0.handler != null);
            assertTrue(volt2.handler != null);

            ProcedureInvocation pi1 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi2 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi3 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi4 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi5 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi6 = new ProcedureInvocation(++handle, "i1", new Integer(1));

            dist.queue(pi1, new ThrowingCallback(), true, System.nanoTime(), 0);
            dist.drain();
            assertTrue(csl.m_exceptionHandled);
            dist.queue(pi2, new ProcCallback(), true, System.nanoTime(), 0);
            dist.queue(pi3, new ProcCallback(), true, System.nanoTime(), 0);
            dist.queue(pi4, new ProcCallback(), true, System.nanoTime(), 0);
            dist.queue(pi5, new ProcCallback(), true, System.nanoTime(), 0);
            dist.queue(pi6, new ProcCallback(), true, System.nanoTime(), 0);

            dist.drain();
            System.err.println("Finished drain.");

            // Invocations are round-robin because no hashinator is present
            // However node 0 gets 4 more invocations to subscribe to topology updates
            assertEquals(6, volt0.handler.roundTrips.get());
            assertEquals(2, volt1.handler.roundTrips.get());
            assertEquals(2, volt2.handler.roundTrips.get());


        }
        finally {
            if (volt0 != null) {
                volt0.shutdown();
            }
            if (volt1 != null) {
                volt1.shutdown();
            }
            if (volt2 != null) {
                volt2.shutdown();
            }
        }
    }


    /**
     * Test connection timeouts.
     * Create a fake voltdb that runs all happy for a while, but
     * then can be told to shut up if it knows what's good for it.
     * Wait for the connection timeout to kill the connection and
     * call the appropriate callbacks.
     */
    @Test
    public void testResponseTimeout() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;


        final CountDownLatch latch = new CountDownLatch(2);

        class TimeoutMonitorPCB implements ProcedureCallback {
            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                assert(clientResponse.getStatus() == ClientResponse.CONNECTION_LOST);
                latch.countDown();
            }
        }

        class TimeoutMonitorCSL extends ClientStatusListenerExt {
            @Override
            public void connectionLost(String hostname, int port, int connectionsLeft,
                    ClientStatusListenerExt.DisconnectCause cause) {
                latch.countDown();
            }
        }

        // create a fake server and connect to it.
        MockVolt volt = new MockVolt(20000);
        volt.start();

        // create distributer and connect it to the client
        Distributer dist = new Distributer(false,
                ClientConfig.DEFAULT_PROCEDURE_TIMEOUT_NANOS,
                1000 /* One second connection timeout */,
                null /* subject */, null);
        dist.addClientStatusListener(new TimeoutMonitorCSL());
        dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA1);

        // make sure it connected
        assertTrue(volt.handler != null);

        // run fine for long enough to send some pings
        Thread.sleep(3000);
        assertTrue(volt.handler.gotPing);

        //Check that we can send a ping and get a response ourselves
        SyncCallback sc = new SyncCallback();
        dist.queue(new ProcedureInvocation(88, "@Ping"), sc, true, System.nanoTime(), 0);
        sc.waitForResponse();
        assertEquals(ClientResponse.SUCCESS, sc.getResponse().getStatus());

        // tell the mock voltdb to stop responding
        volt.handler.sendResponses.set(false);

        try {
            // this call should hang until the connection is closed,
            // then will be called with CONNECTION_LOST
            ProcedureInvocation invocation = new ProcedureInvocation(44, "@Ping");
            dist.queue(invocation, new TimeoutMonitorPCB(), true, System.nanoTime(), 0);
        } catch (NoConnectionsException e) {
            //Ok this is a little odd scheduling wise, would expect to at least be able to submit
            //the transaction before reaching a multi-second timeout, but such is life
            //The callback won't be invoked so count the latch down for it
            latch.countDown();
        }

        // wait for both callbacks
        latch.await();

        // clean up
        dist.shutdown();
        volt.shutdown();
    }

    /**
     * Test premature connection timeouts.
     */
    @Test
    public void testPrematureTimeout() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        final AtomicBoolean failed = new AtomicBoolean(false);
        class TimeoutMonitorCSL extends ClientStatusListenerExt {
            @Override
            public void connectionLost(String hostname, int port, int connectionsLeft,
                                       ClientStatusListenerExt.DisconnectCause cause) {
                if (cause.equals(DisconnectCause.TIMEOUT)) {
                    failed.set(true);
                }
            }
        }

        // create a fake server and connect to it.
        MockVolt volt = new MockVolt(20000);
        volt.start();

        // create distributer and connect it to the client
        Distributer dist = new Distributer(false,
                ClientConfig.DEFAULT_PROCEDURE_TIMEOUT_NANOS,
                2000 /* Two seconds connection timeout */,
                null /* subject */, null);
        dist.addClientStatusListener(new TimeoutMonitorCSL());
        dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA1);

        // make sure it connected
        assertTrue(volt.handler != null);

        // run fine for long enough to send some pings
        long start = System.currentTimeMillis();
        while ((System.currentTimeMillis() - start) < 3000) {
            Thread.yield();
        }

        start = System.currentTimeMillis();

        // tell the mock voltdb to stop responding
        volt.handler.sendResponses.set(false);

        // Should not timeout unless 2 seconds has passed
        while (!failed.get()) {
            if ((System.currentTimeMillis() - start) > 2000) {
                break;
            } else {
                Thread.yield();
            }
        }

        // If the actual elapsed time is smaller than the timeout value,
        // but the connection was closed due to a timeout, fail.
        // Only check if the duration is within a range, because the timer may not be accurate.
        if ((System.currentTimeMillis() - start) < 1900 &&
            (System.currentTimeMillis() - start) > 2100) {
            fail("Premature timeout occurred " + (System.currentTimeMillis() - start));
        }

        // clean up
        dist.shutdown();
        volt.shutdown();
    }

    /**
     * Test query timeouts. Create a fake voltdb that runs all happy for a while, but then can be told to shut up if it
     * knows what's good for it. Wait for the query timeout.
     */
    @Test
    public void testQueryTimeout() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;


        final CountDownLatch latch = new CountDownLatch(1);

        class QueryTimeoutMonitor implements ProcedureCallback {

            @Override
            public void clientCallback(ClientResponse clientResponse) throws Exception {
                assert (clientResponse.getStatus() == ClientResponse.CONNECTION_TIMEOUT);
                System.out.println("Query timeout called..: " + clientResponse.getStatusString());
                latch.countDown();
            }
        }

        // create a fake server and connect to it.
        MockVolt volt = new MockVolt(20000);
        volt.start();

        // create distributer and connect it to the client
        Distributer dist = new Distributer(false,
                ClientConfig.DEFAULT_PROCEDURE_TIMEOUT_NANOS,
                30000 /* thirty second connection timeout */,
                null /* subject */, null);
        dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA1);

        // make sure it connected
        assertTrue(volt.handler != null);

        // run fine for long enough to send some pings
        Thread.sleep(3000);

        // tell the mock voltdb to stop responding
        volt.handler.sendProcTimeout.set(true);

        // this call should hang until the connection is closed,
        // then will be called with CONNECTION_LOST
        ProcedureInvocation invocation = new ProcedureInvocation(45, "@Ping");
        dist.queue(invocation, new QueryTimeoutMonitor(), true, System.nanoTime(), 10);

        // wait for callback
        latch.await();

        // clean up
        dist.shutdown();
        volt.shutdown();
    }

    /**
     * Test that a connection actually times out when it should timeout,
     * rather than sooner. Also check pings aren't sent super duper early.
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testResponseNoEarlyTimeout() throws IOException, InterruptedException {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        final CountDownLatch latch = new CountDownLatch(1);

        final long CONNECTION_TIMEOUT = 6000;

        class TimeoutMonitorCSL extends ClientStatusListenerExt {
            @Override
            public void connectionLost(String hostname, int port, int connectionsLeft,
                    ClientStatusListenerExt.DisconnectCause cause) {
                latch.countDown();
            }
        }

        // create a fake server and connect to it.
        MockVolt volt = new MockVolt(20000);
        volt.start();

        // create distributer and connect it to the client
        Distributer dist = new Distributer( false,
                ClientConfig.DEFAULT_PROCEDURE_TIMEOUT_NANOS,
                CONNECTION_TIMEOUT /* six second connection timeout */,
                null /* subject */, null);
        dist.addClientStatusListener(new TimeoutMonitorCSL());
        long start = System.currentTimeMillis();
        dist.createConnection("localhost", "", "", 20000, ClientAuthScheme.HASH_SHA1);

        // don't respond to pings
        volt.handler.sendResponses.set(false);

        // make sure it connected
        assertTrue(volt.handler != null);

        // make sure the ping takes more than 1s
        Thread.sleep(1000);
        assertFalse(volt.handler.gotPing);

        // verify that a ping was sent after 4s
        Thread.sleep(3000);
        assertTrue(volt.handler.gotPing);

        // wait for callback
        latch.await();

        long duration = System.currentTimeMillis() - start;
        assertTrue(duration > CONNECTION_TIMEOUT);

        // clean up
        dist.shutdown();
        volt.shutdown();
    }

    public void testClient() throws Exception {
       if (ClientConfig.ENABLE_SSL_FOR_TEST) return;
       // TODO: write a mock server that can grock ssl
       MockVolt volt = null;

       try {
           // create a fake server and connect to it.
           volt = new MockVolt(21212);
           volt.start();

           Client clt = ClientFactory.createClient();
           clt.createConnection("localhost");

           // this call blocks for a result!
           clt.callProcedure("Foo", new Integer(1));
           assertEquals(5, volt.handler.roundTrips.get());

           // this call doesn't block! (use drain)
           clt.callProcedure(new ProcCallback(), "Bar", new Integer(2));
           clt.drain();
           assertEquals(6, volt.handler.roundTrips.get());
       }
       finally {
           if (volt != null) {
               volt.shutdown();
           }
       }
    }

    @Test
    public void testClientBlockedOnMaxOutstanding() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        // create a fake server and connect to it.
        MockVolt volt0 = new MockVolt(20000);
        volt0.handleConnection = false;

        Client clientPtr = null;
        try {
            volt0.start();

            ClientConfig config = new ClientConfig();
            config.setMaxOutstandingTxns(5);
            config.setConnectionResponseTimeout(2000);

            final Client client = ClientFactory.createClient(config);
            client.createConnection("localhost", 20000);
            clientPtr = client;

            final AtomicInteger counter = new AtomicInteger(0);
            final Thread loadThread = new Thread() {
                @Override
                public void run() {
                    try {
                        for (int ii = 0; ii < 6; ii++) {
                            client.callProcedure(new NullCallback(), "foo");
                            System.out.println(counter.incrementAndGet());
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };
            loadThread.start();

            final long start = System.currentTimeMillis();
            loadThread.join(300);
            final long finish = System.currentTimeMillis();
            assertTrue(finish - start >= 300);
            System.out.println("Counter " + counter.get());
            assertTrue(counter.get() == 5);
            loadThread.join();
        }
        finally {
            if (clientPtr != null) clientPtr.close();
            volt0.shutdown();
        }
    }

    public void testUnresolvedHost() throws IOException {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        final String hostname = "doesnotexist";
        boolean threwException = false;
        try {
            ConnectionUtil.getAuthenticatedConnection(hostname, "", new byte[0], 32, null, ClientAuthScheme.HASH_SHA1, 0);
        } catch (java.net.UnknownHostException e) {
            threwException = true;
            assertTrue(e.getMessage().equals(hostname));
        }
        assertTrue(threwException);
    }

    public void testSubscription() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        Distributer.RESUBSCRIPTION_DELAY_MS = 1;
        MockVolt volt0 = new MockVolt(20000);
        MockVolt volt1 = new MockVolt(20001);
        volt0.start();
        volt1.start();

        try {
        Client c = ClientFactory.createClient();
        c.createConnection("localhost", 20000);

        //Test that metadata was retrieved
        assertTrue(volt0.handler.invokedSubscribe.tryAcquire(10, TimeUnit.SECONDS));
        assertTrue(volt0.handler.invokedSystemCatalog.tryAcquire(10, TimeUnit.SECONDS));
        assertTrue(volt0.handler.invokedTopology.tryAcquire( 10, TimeUnit.SECONDS));

        c.createConnection("localhost", 20001);

        Thread.sleep(50);
        //Should not have invoked anything
        assertFalse(volt1.handler.invokedSubscribe.tryAcquire());
        assertFalse(volt1.handler.invokedSystemCatalog.tryAcquire());
        assertFalse(volt1.handler.invokedTopology.tryAcquire());

        volt0.shutdown();

        Thread.sleep(50);
        //Test that topology is retrieved and re-subscribed
        assertTrue(volt1.handler.invokedSubscribe.tryAcquire(10, TimeUnit.SECONDS));
        assertTrue(volt1.handler.invokedTopology.tryAcquire( 10, TimeUnit.SECONDS));
        //Don't need to get the catalog again due to node failure
        assertFalse(volt1.handler.invokedSystemCatalog.tryAcquire());
        } finally {
            volt0.shutdown();
            volt1.shutdown();
        }
    }

    public void testSubscribeConnectionLost() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        Distributer.RESUBSCRIPTION_DELAY_MS = 1;
        MockVolt volt0 = new MockVolt(20000);
        volt0.handleConnection = false;
        MockVolt volt1 = new MockVolt(20001);
        volt0.start();
        volt1.start();

        try {
            Client c = ClientFactory.createClient();
            c.createConnection("localhost", 20000);

            c.createConnection("localhost", 20001);

            Thread.sleep(50);
            //Should not have invoked anything
            assertFalse(volt1.handler.invokedSubscribe.tryAcquire());
            assertFalse(volt1.handler.invokedSystemCatalog.tryAcquire());
            assertFalse(volt1.handler.invokedTopology.tryAcquire());

            volt0.shutdown();

            Thread.sleep(50);
            //Test that topology is retrieved and re-subscribed
            assertTrue("not invokedSubscribe", volt1.handler.invokedSubscribe.tryAcquire(10, TimeUnit.SECONDS));
            assertTrue("not invokedTopology", volt1.handler.invokedTopology.tryAcquire(10, TimeUnit.SECONDS));
            assertTrue("not invokedSystemCatalog", volt1.handler.invokedSystemCatalog.tryAcquire(10, TimeUnit.SECONDS));
        } finally {
            volt0.shutdown();
            volt1.shutdown();
        }
    }

    static public class threadedClient extends Thread {
        Client c = null;
        public static CountDownLatch establishConnection = new CountDownLatch(1);
        public static CountDownLatch teardownConnection = new CountDownLatch(1);
        public static AtomicInteger gracefulExits = new AtomicInteger(0);

        public static ClientConfig config = new ClientConfig();
        static {
            config.setMaxOutstandingTxns(5);
            config.setConnectionResponseTimeout(2000);
        }

        static Client createClient() throws UnknownHostException, IOException {
            Client client = ClientFactory.createClient(config);
            client.createConnection("localhost", 20000);
            return client;
        }

        @Override
        public void run() {
            try {
                establishConnection.await();
                c = createClient();
                teardownConnection.await();
                gracefulExits.incrementAndGet();
            }
            catch (Exception ex) {
                System.out.printf("Thread %s ungraceful exit: %s%n", Thread.currentThread().getName(), ex);
            }
            finally {
                if (c != null) {
                    try {
                        c.close();
                    }
                    catch (InterruptedException e) {
                    }
                }
            }
        }
    }

    public void testParallelClientConnections() throws Exception {
        // TODO: write a mock server that can grock ssl
        if (ClientConfig.ENABLE_SSL_FOR_TEST) return;

        Thread t1 = new threadedClient();
        t1.start();
        Thread t2 = new threadedClient();
        t2.start();
        Thread t3 = new threadedClient();
        t3.start();

        MockVolt volt0 = new MockVolt(20000);
        volt0.handleConnection = false;

        boolean connected = false;
        try {
            volt0.start();
            stopRevDNS(); // I don't know why we do this but...
            System.out.println("Signalling connection establishment");
            connected = true;
            threadedClient.establishConnection.countDown();
        }
        catch (Exception ex) {
            System.out.printf("Unexpected exception starting MockVolt: %s", ex);
            fail("Unexpected exception");
        }
        finally {
            if (connected) {
                System.out.println("Signalling connection teardown");
                threadedClient.teardownConnection.countDown();
            }
            else {
                t1.interrupt();
                t2.interrupt();
                t3.interrupt();
            }
            t1.join();
            t2.join();
            t3.join();
            assertEquals(3, threadedClient.gracefulExits.get());
            System.out.println("Shutdown");
            volt0.shutdown();
        }
    }

    private void stopRevDNS() {
        try {
            Client forceRemoveDNSThread = threadedClient.createClient();
            forceRemoveDNSThread.close();
            boolean dnsThreadUp = true;
            while (dnsThreadUp) {
                try {
                    ReverseDNSCache.submit(new Runnable() {
                        @Override
                        public void run() { }
                    });
                }
                catch (IllegalStateException e) {
                    dnsThreadUp = false;
                }
                Thread.sleep(100);
            }
        }
        catch (Exception ex) {
            System.out.printf("Unexpected exception stopping ReverseDNSCache: %s%n", ex);
            // but keep going
        }
    }
}
