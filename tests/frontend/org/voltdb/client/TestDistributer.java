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

package org.voltdb.client;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.junit.Test;
import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.network.Connection;
import org.voltdb.network.QueueMonitor;
import org.voltdb.network.VoltNetwork;
import org.voltdb.network.VoltProtocolHandler;

public class TestDistributer extends TestCase {

    class MockInputHandler extends VoltProtocolHandler {

        @Override
        public int getMaxRead() {
            return 8192;
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) {
            try {
                FastDeserializer fds = new FastDeserializer(message);
                StoredProcedureInvocation spi = fds.readObject(StoredProcedureInvocation.class);

                VoltTable vt[] = new VoltTable[1];
                vt[0] = new VoltTable(new VoltTable.ColumnInfo("Foo", VoltType.BIGINT));
                vt[0].addRow(1);
                ClientResponseImpl response =
                    new ClientResponseImpl(ClientResponseImpl.SUCCESS, vt, "Extra String", spi.getClientHandle());
                c.writeStream().enqueue(response);
                roundTrips.incrementAndGet();
                System.err.println("Sending response.");
            }
            catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        @Override
        public int getExpectedOutgoingMessageSize() {
            return 2048;
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
    }

    // A fake server.
    class MockVolt extends Thread {
        boolean handleConnection = true;
        MockVolt(int port) throws IOException {
            network = new VoltNetwork();
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
                        final ByteBuffer lengthBuffer = ByteBuffer.allocate(5);//Extra byte for version also
                        client.read(lengthBuffer);

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

                        final ByteBuffer passwordBuffer = ByteBuffer.allocate(20);
                        while (passwordBuffer.remaining() > 0)
                            client.read(passwordBuffer);
                        passwordBuffer.flip();

                        final byte usernameBytes[] = new byte[usernameLength];
                        final byte passwordBytes[] = new byte[20];
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
                        if (handleConnection) {
                            network.registerChannel( client, handler);
                        }
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
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
        }

        public void shutdown() {
            shutdown.set(true);
        }

        AtomicBoolean shutdown = new AtomicBoolean(false);
        volatile ServerSocketChannel socket = null;
        volatile MockInputHandler handler = null;
        volatile VoltNetwork network;
    }

    private static class CSL implements ClientStatusListener {
        private volatile boolean m_exceptionHandled = false;
        @Override
        public void uncaughtException(ProcedureCallback callback,
                ClientResponse r, Throwable e) {
            m_exceptionHandled = true;
        }
        @Override
        public void backpressure(boolean status) {
            // TODO Auto-generated method stub

        }
        @Override
        public void connectionLost(String hostname, int connectionsLeft) {
            // TODO Auto-generated method stub

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


    @Test
    public void testCreateConnection() throws Exception {
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
            dist.createConnection("localhost", "", "", 20000);
            dist.createConnection("localhost", "", "", 20001);

            Thread.sleep(1000);
            assertTrue(volt1.handler != null);
            assertTrue(volt0.handler != null);
        }
        finally {
            if (volt0 != null) {
                volt0.shutdown();
                volt0.join();
            }
            if (volt1 != null) {
                volt1.shutdown();
                volt1.join();
            }
        }
    }

    @Test
    public void testQueue() throws Exception {

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

            Distributer dist = new Distributer(128, null, false, null);
            dist.addClientStatusListener(csl);
            dist.createConnection("localhost", "", "", 20000);
            dist.createConnection("localhost", "", "", 20001);
            dist.createConnection("localhost", "", "", 20002);

            assertTrue(volt1.handler != null);
            assertTrue(volt0.handler != null);
            assertTrue(volt2.handler != null);

            ProcedureInvocation pi1 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi2 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi3 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi4 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi5 = new ProcedureInvocation(++handle, "i1", new Integer(1));
            ProcedureInvocation pi6 = new ProcedureInvocation(++handle, "i1", new Integer(1));

            dist.queue(pi1, new ThrowingCallback(), 128, true);
            dist.drain();
            assertTrue(csl.m_exceptionHandled);
            dist.queue(pi2, new ProcCallback(), 128, true);
            dist.queue(pi3, new ProcCallback(), 128, true);
            dist.queue(pi4, new ProcCallback(), 128, true);
            dist.queue(pi5, new ProcCallback(), 128, true);
            dist.queue(pi6, new ProcCallback(), 128, true);

            dist.drain();
            System.err.println("Finished drain.");

            assertEquals(2, volt0.handler.roundTrips.get());
            assertEquals(2, volt1.handler.roundTrips.get());
            assertEquals(2, volt2.handler.roundTrips.get());


        }
        finally {
            if (volt0 != null) {
                volt0.shutdown();
                volt0.join();
            }
            if (volt1 != null) {
                volt1.shutdown();
                volt1.join();
            }
            if (volt2 != null) {
                volt2.shutdown();
                volt2.join();
            }
        }
    }

    public void testClient() throws Exception {
       MockVolt volt = null;

       try {
           // create a fake server and connect to it.
           volt = new MockVolt(21212);
           volt.start();

           Client clt = ClientFactory.createClient();
           clt.createConnection("localhost");

           // this call blocks for a result!
           clt.callProcedure("Foo", new Integer(1));
           assertEquals(1, volt.handler.roundTrips.get());

           // this call doesn't block! (use drain)
           clt.callProcedure(new ProcCallback(), "Bar", new Integer(2));
           clt.drain();
           assertEquals(2, volt.handler.roundTrips.get());
       }
       finally {
           if (volt != null) {
               volt.shutdown();
               volt.join();
           }
       }
    }

    @Test
    public void testClientBlockedOnMaxOutstanding() throws Exception {
        // create a fake server and connect to it.
        MockVolt volt0 = new MockVolt(20000);
        volt0.handleConnection = false;
        try {
            volt0.start();

            ClientConfig config = new ClientConfig();
            config.setMaxOutstandingTxns(5);

            final Client client = ClientFactory.createClient(config);
            client.createConnection("localhost", 20000);

            final java.util.concurrent.atomic.AtomicInteger counter = new java.util.concurrent.atomic.AtomicInteger(0);
            final Thread loadThread = new Thread() {
                @Override
                public void run() {
                    try {
                        for (int ii = 0; ii < 6; ii++) {
                            client.callProcedure(new NullCallback(), "foo");
                            counter.incrementAndGet();
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
            assert(finish - start >= 300);
            assert(counter.get() == 5);
            loadThread.interrupt();
            loadThread.join();
        }
        finally {
            volt0.shutdown();
        }
    }

    public void testUnresolvedHost() throws IOException {
        final String hostname = "doesnotexist";
        boolean threwException = false;
        try {
            ConnectionUtil.getAuthenticatedConnection(hostname, "", new byte[0], 32);
        } catch (java.net.UnknownHostException e) {
            threwException = true;
            assertTrue(e.getMessage().equals(hostname));
        }
        assertTrue(threwException);
    }

}

