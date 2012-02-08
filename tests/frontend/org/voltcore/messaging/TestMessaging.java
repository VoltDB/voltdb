/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltcore.messaging;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase;

import org.mockito.ArgumentCaptor;
import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.client.ConnectionUtil;
import org.voltcore.network.VoltNetworkPool;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.messaging.SocketJoiner.JoinHandler;
import static org.mockito.Mockito.*;

public class TestMessaging extends TestCase {
    public static class MsgTest extends VoltMessage {

        public static final byte MSG_TEST_ID = 127;

        static byte[] globalValue;
        byte[] m_localValue;
        int m_length;

        static void initWithSize(int size) {
            Random r = new Random();
            globalValue = new byte[size];
            for (int i = 0; i < size; i++)
                globalValue[i] = (byte) r.nextInt(Byte.MAX_VALUE);
        }

        @Override
        public void initFromBuffer(ByteBuffer buf) {
            m_length = buf.limit() - buf.position();
            m_localValue = new byte[m_length];
            buf.get(m_localValue);
        }

        @Override
        protected void flattenToBuffer(ByteBuffer buf) {
            buf.put(MSG_TEST_ID);
            buf.put(m_localValue);
        }

        public void setValues() {
            m_localValue = new byte[globalValue.length];
            for (int i = 0; i < globalValue.length; i++)
                m_localValue[i] = globalValue[i];
        }

        public boolean verify() {
            if (globalValue.length != m_localValue.length)
                return false;
            for (int i = 0; i < globalValue.length; i++) {
                if (globalValue[i] != m_localValue[i]) {
                    System.err.printf("MsgTst.verify() failed at byte: %d\n", i);
                    return false;
                }
            }
            return true;
        }
    }

    public static class MessageFactory extends org.voltcore.messaging.VoltMessageFactory {
        final public static byte DUMMY_ID = VOLTCORE_MESSAGE_ID_MAX + 1;

        @Override
        protected VoltMessage instantiate_local(byte messageType)
        {
            // instantiate a new message instance according to the id
            VoltMessage message = null;

            switch (messageType) {
            case DUMMY_ID:
               message = new MsgTest();
               break;
            }
            return message;
        }
    }

    public static class MsgTestEndpoint extends Thread {
        static final int msgCount = 1024;
        static final int hostCount = 3;
        static final long mailboxCount = 5;

        static Lock hostMessengerLock = new ReentrantLock();
        static HostMessenger[] messengers = new HostMessenger[hostCount];
        static AtomicInteger sitesDone = new AtomicInteger(0);

        static AtomicInteger sentCount = new AtomicInteger(0);
        static AtomicInteger recvCount = new AtomicInteger(0);
        static Random rand = new Random();
        static AtomicInteger siteCount = new AtomicInteger(0);

        int mySiteId;

        public MsgTestEndpoint() {
            mySiteId = siteCount.getAndIncrement();
        }

        @Override
        public void run() {
            // create a site
            int hostId = mySiteId % hostCount;
            InetAddress leader = null;
            HostMessenger currentMessenger = null;
            Mailbox[] mbox = new Mailbox[(int)mailboxCount];

            System.out.printf("Starting up host: %d, site: %d\n", hostId, mySiteId);

            hostMessengerLock.lock();
            {
                // get the host if possible
                currentMessenger = messengers[hostId];

                // create the host if needed
                if (currentMessenger == null) {
                    boolean isPrimary = hostId == 0;
                    if (isPrimary)
                    {
                        System.out.printf("Host/Site %d/%d is initializing the HostMessenger class.\n", hostId, mySiteId);
                        leader = ConnectionUtil.getLocalAddress();
                    }
                    System.out.printf("Host/Site %d/%d is creating a new HostMessenger.\n", hostId, mySiteId);
                    HostMessenger.Config config = new HostMessenger.Config();
                    config.internalPort += mySiteId;
                    final HostMessenger messenger = new HostMessenger(config);
                    currentMessenger = messenger;
                    messengers[hostId] = currentMessenger;
                    new Thread() {
                        @Override
                        public void run() {
                            try {
                                messenger.start();
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    };
                }
                else
                    System.out.printf("Host/Site %d/%d found existing HostMessenger.\n", hostId, mySiteId);
            }
            hostMessengerLock.unlock();

            HostMessenger messenger = currentMessenger;
            messenger.waitForGroupJoin();
            // create the mailboxes
            for (int i = 0; i < mailboxCount; i++) {
                mbox[i] = currentMessenger.createMailbox();
            }
            // claim this site is done
            sitesDone.incrementAndGet();
            System.out.printf("Host/Site %d/%d has joined and created sites.\n", hostId, mySiteId);

            // spin until all sites are done
            while (sitesDone.get() < siteCount.get()) {
            }

            System.out.printf("Host/Site %d/%d thinks all threads are ready.\n", hostId, mySiteId);

            // begin loop
            while(recvCount.get() < msgCount) {
                // figure out which message to send
                int msgIndex = sentCount.getAndIncrement();

                // send a message
                if (msgIndex < msgCount) {
                    int siteId = rand.nextInt(siteCount.get());
                    long mailboxId = rand.nextLong() % mailboxCount;
                    System.out.printf("Host/Site %d/%d is sending message %d/%d to site/mailbox %d/%d.\n",
                            hostId, mySiteId, msgIndex, msgCount, siteId, mailboxId);
                    MsgTest mt = new MsgTest();
                    mt.setValues();
                    try {
                        (currentMessenger).send((mailboxCount << 32) + siteId, mt);
                    } catch (MessagingException e) {
                        e.printStackTrace();
                    }
                }

                // try to recv a message
                for (int i = 0; i < mailboxCount; i++) {
                    MsgTest mt = (MsgTest) mbox[i].recv();
                    if (mt != null) {
                        int recvCountTemp = recvCount.incrementAndGet();
                        System.out.printf("Host/Site %d/%d is receiving message %d/%d.\n",
                                hostId, mySiteId, recvCountTemp, msgCount);
                        assertTrue(mt.verify());
                    }
                }
            }
            try {
                messenger.shutdown();
            } catch (InterruptedException e1) {
                e1.printStackTrace();
            }
        }
    }

    public void testJoiner() {
        try {
            JoinHandler jh1 = mock(JoinHandler.class);
            SocketJoiner joiner1 = new SocketJoiner(new InetSocketAddress("127.0.0.1", 3212), "", 3212, null, jh1);
            JoinHandler jh2 = mock(JoinHandler.class);
            SocketJoiner joiner2 = new SocketJoiner(new InetSocketAddress("127.0.0.1", 3212), "", 3213, null, jh2);
            JoinHandler jh3 = mock(JoinHandler.class);
            SocketJoiner joiner3 = new SocketJoiner(new InetSocketAddress("127.0.0.1", 3212), "", 3214, null, jh3);

            CountDownLatch cdl = new CountDownLatch(1);
            joiner1.start(cdl);
            joiner2.start(null);
            verify(jh1, never()).requestJoin(any(SocketChannel.class), any(InetSocketAddress.class));
            verify(jh1).requestJoin(isNotNull(SocketChannel.class), eq(new InetSocketAddress("127.0.0.1", 3213)));
            joiner3.start(null);

            cdl.countDown();
            jh1.


            joiner1.shutdown();
            joiner2.shutdown();
            joiner3.shutdown();
            assertTrue(true);
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertTrue(false);
    }

    public void testSimple() throws MessagingException, UnknownHostException, InterruptedException {
        try {
            Selector.open();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        VoltNetworkPool network = new VoltNetworkPool();
        network.start();
        HostMessenger msg1 = new HostMessenger(network, ConnectionUtil.getLocalAddress(), 2, 0, 0, null);
        Thread.sleep(20);
        HostMessenger msg2 = new HostMessenger(network, ConnectionUtil.getLocalAddress(), 2, 0, 0, null);

        System.out.println("Waiting for socketjoiners...");
        msg1.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg1");
        msg2.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg2");

        assertEquals(msg1.getHostId(), 0);
        assertEquals(msg2.getHostId(), 1);

        int siteId1 = msg1.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 1;
        int siteId2 = msg2.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 2;

        msg1.createLocalSite(siteId1);
        msg2.createLocalSite(siteId2);

        Mailbox mb1 = msg1.createMailbox(siteId1, 1, false);
        Mailbox mb2 = msg2.createMailbox(siteId2, 1, false);

        MsgTest.initWithSize(16);
        MsgTest mt = (MsgTest) VoltMessage.createNewMessage(MsgTest.MSG_TEST_ID);
        mt.setValues();
        mb1.send(siteId2, 1, mt);
        MsgTest mt2 = null;
        while (mt2 == null) {
            mt2 = (MsgTest) mb2.recv();
        }
        assertTrue(mt2.verify());

        // Do it again
        MsgTest.initWithSize(32);
        mt = (MsgTest) VoltMessage.createNewMessage(MsgTest.MSG_TEST_ID);
        mt.setValues();
        mb1.send(siteId2, 1, mt);
        mt2 = null;
        while (mt2 == null) {
            mt2 = (MsgTest) mb2.recv();
        }
        assertTrue(mt2.verify());

        // Do it a final time with a message that should block on write.
        // spin on a complete network message here - maybe better to write
        // the above cases this way too?
        for (int i=0; i < 3; ++i) {
            System.out.println("running iteration: " + i);

            MsgTest.initWithSize(4280034);
            mt = (MsgTest) VoltMessage.createNewMessage(MsgTest.MSG_TEST_ID);
            mt.setValues();
            mb1.send(siteId2, 1, mt);
            mt2 = null;
            while (mt2 == null) {
                mt2 = (MsgTest) mb2.recv();
            }
            assertTrue(mt.verify());
        }
        msg1.shutdown();
        msg2.shutdown();
        network.shutdown();
    }

    public void testMultiMailbox() throws MessagingException, UnknownHostException, InterruptedException {
        VoltNetworkPool network = new VoltNetworkPool();
        network.start();
        HostMessenger msg1 = new HostMessenger(network, ConnectionUtil.getLocalAddress(), 3, 0, 0, null);
        Thread.sleep(20);
        HostMessenger msg2 = new HostMessenger(network, ConnectionUtil.getLocalAddress(), 3, 0, 0, null);
        Thread.sleep(20);
        HostMessenger msg3 = new HostMessenger(network, ConnectionUtil.getLocalAddress(), 3, 0, 0, null);

        System.out.println("Waiting for socketjoiners...");
        msg1.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg1");
        msg2.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg2");
        msg3.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg3");

        assertTrue(msg1.getHostId() != msg2.getHostId() && msg2.getHostId() != msg3.getHostId());
        //assertTrue(msg2.getHostId() == 1);
        //assertTrue(msg3.getHostId() == 2);

        int siteId1 = msg1.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 1;
        int siteId5 = msg1.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 5;
        msg1.createLocalSite(siteId1);
        msg1.createLocalSite(siteId5);

        int siteId2 = msg2.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 2;
        msg2.createLocalSite(siteId2);

        int siteId3 = msg3.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 3;
        int siteId4 = msg3.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 4;
        msg3.createLocalSite(siteId3);
        msg3.createLocalSite(siteId4);

        Mailbox mb1 = msg1.createMailbox(siteId1, 1, false);
        Mailbox mb2 = msg2.createMailbox(siteId2, 1, false);
        Mailbox mb3 = msg3.createMailbox(siteId3, 1, false);
        Mailbox mb4 = msg3.createMailbox(siteId4, 1, false);
        Mailbox mb5 = msg1.createMailbox(siteId5, 1, false);

        MsgTest.initWithSize(16);
        MsgTest mt = (MsgTest) VoltMessage.createNewMessage(MsgTest.MSG_TEST_ID);
        mt.setValues();

        int msgCount = 0;

        mb1.send(new int[] {siteId2,siteId3,siteId5,siteId4}, 1, mt);
        long now = System.currentTimeMillis();
        MsgTest mt2 = null, mt3 = null, mt4 = null, mt5 = null;

        // run (for no more than 5s) until all 4 messages have arrived
        // this code is really weird, but it is more accurate than just
        // running until you get 4 messages. It actually makes sure they
        // are the right messages.
        while (msgCount < 4) {
            assertTrue((System.currentTimeMillis() - now) < 5000);

            if (mt2 == null) {
                mt2 = (MsgTest) mb2.recv();
                if (mt2 != null) {
                    assertTrue(mt2.verify());
                    msgCount++;
                }
            }
            if (mt3 == null) {
                mt3 = (MsgTest) mb3.recv();
                if (mt3 != null) {
                    assertTrue(mt3.verify());
                    msgCount++;
                }
            }
            if (mt4 == null) {
                mt4 = (MsgTest) mb4.recv();
                if (mt4 != null) {
                    assertTrue(mt4.verify());
                    msgCount++;
                }
            }
            if (mt5 == null) {
                mt5 = (MsgTest) mb5.recv();
                if (mt5 != null) {
                    assertTrue(mt5.verify());
                    msgCount++;
                }
            }
        }

        mb3.send(new int[] {siteId5}, 1, mt);

        assertEquals(((SiteMailbox)mb2).getWaitingCount(), 0);
        assertEquals(((SiteMailbox)mb3).getWaitingCount(), 0);
        assertEquals(((SiteMailbox)mb4).getWaitingCount(), 0);

        // check that there is a single message for mb5
        // again, weird code, but I think it's right (jhugg)
        int wc = 0;
        now = System.currentTimeMillis();
        while (wc != 1) {
            assertTrue((System.currentTimeMillis() - now) < 5000);
            wc = ((SiteMailbox)mb5).getWaitingCount();
            if (wc == 0)
            assertTrue(wc < 2);
        }
        msg1.shutdown();
        msg2.shutdown();
        msg3.shutdown();
        network.shutdown();
    }

    /*public void testForStress1() {
        final int siteCount = 3;

        MsgTest.initWithSize(64);

        MsgTestEndpoint[] endpoints = new MsgTestEndpoint[siteCount];

        for (int i = 0; i < siteCount; i++)
            endpoints[i] = new MsgTestEndpoint();

        for (int i = 0; i < siteCount; i++)
            endpoints[i].start();

        for (int i = 0; i < siteCount; i++)
            try {
                endpoints[i].join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
    }*/

    class MockNewNode extends Thread {
        final int m_port;
        AtomicBoolean m_ready = new AtomicBoolean(false);

        MockNewNode(int port) {
            m_port = port;
        }

        void waitUntilReady() {
            while (!m_ready.get())
                Thread.yield();
        }

        @Override
        public void run() {
            try {
                ServerSocketChannel listener = ServerSocketChannel.open();
                listener.socket().bind(new InetSocketAddress(m_port));

                VoltNetworkPool network = new VoltNetworkPool();
                network.start();
                HostMessenger msg = new HostMessenger(network, listener, 2, 0, 0, null);
                m_ready.set(true);
                msg.waitForGroupJoin();

            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    public void testFailAndRejoin() throws Exception {
        MockVoltDB mockvolt = new MockVoltDB();
        VoltDB.replaceVoltDBInstanceForTest(mockvolt);

        // get config info (mostly the port)
        int internalPort = new VoltDB.Configuration().m_internalPort;

        try {
            Selector.open();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        VoltNetworkPool network = new VoltNetworkPool();
        network.start();
        HostMessenger msg1 = new HostMessenger(network, ConnectionUtil.getLocalAddress(), 2, 0, 0, null);
        Thread.sleep(20);
        HostMessenger msg2 = new HostMessenger(network, ConnectionUtil.getLocalAddress(), 2, 0, 0, null);

        System.out.println("Waiting for socketjoiners...");
        msg1.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg1");
        msg2.waitForGroupJoin();
        System.out.println("Finished socket joiner for msg2");

        int siteId1 = msg1.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 1;
        int siteId2 = msg2.getHostId() * VoltDB.SITES_TO_HOST_DIVISOR + 2;
        int msg2hostId = msg2.getHostId();

        msg1.createLocalSite(siteId1);
        msg2.createLocalSite(siteId2);

        // kill host #2
        // triggers the fault manager
        msg2.closeForeignHostSocket(msg1.getHostId());
        msg2.shutdown();
        // this is just to wait for the fault manager to kick in
        Thread.sleep(50);

        // wait until the fault manager has kicked in
        for (int i = 0; msg1.countForeignHosts() > 0; i++) {
            if (i > 10) fail();
            Thread.sleep(50);
        }
        assertEquals(0, msg1.countForeignHosts());

        // rejoin the network in a new thread
        MockNewNode newnode = new MockNewNode(internalPort);
        newnode.start();
        newnode.waitUntilReady();
        // this is just for extra safety
        Thread.sleep(50);

        HashSet<Integer> liveHosts = new HashSet<Integer>();
        liveHosts.add(msg1.getHostId());
        // the last byte array below is the bytes of a dummy catalog
        msg1.rejoinForeignHostPrepare(msg2hostId, new InetSocketAddress(internalPort), 0, 0, liveHosts, 0, 1, 0, new byte[] { 'a' });
        msg1.rejoinForeignHostCommit();

        // this timeout is rather lousy, but neither is it exception safe!
        newnode.join(1000);
        if (newnode.isAlive()) fail();

        msg1.shutdown();
        network.shutdown();
        mockvolt.shutdown(null);
    }
}
