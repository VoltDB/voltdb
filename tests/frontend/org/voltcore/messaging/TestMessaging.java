/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import junit.framework.TestCase;

import org.voltcore.utils.PortGenerator;

public class TestMessaging extends TestCase {

    static final PortGenerator m_portGenerator = new PortGenerator();

    public static class MsgTest extends VoltMessage {
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
        public int getSerializedSize() {
            return super.getSerializedSize() + m_localValue.length;
        }

        @Override
        public void initFromBuffer(ByteBuffer buf) {
            m_length = buf.limit() - buf.position();
            m_localValue = new byte[m_length];
            buf.get(m_localValue);
        }

        @Override
        public void flattenToBuffer(ByteBuffer buf) {
            buf.put(MessageFactory.DUMMY_ID);
            buf.put(m_localValue);
            assert(buf.position() == buf.capacity());
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
                    }
                    System.out.printf("Host/Site %d/%d is creating a new HostMessenger.\n", hostId, mySiteId);
                    HostMessenger.Config config = new HostMessenger.Config(m_portGenerator);
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
            messenger.waitForGroupJoin(hostCount);
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
                    (currentMessenger).send((mailboxCount << 32) + siteId, mt);
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

    /*
     * Socket joiner isn't easily unit testable. It's interaction with host messenger is too complex.
     */
//    public void testJoiner() throws Exception {
//        try {
//            final JoinHandler jh1 = mock(JoinHandler.class);
//            final SocketJoiner joiner1 = new SocketJoiner(new InetSocketAddress("127.0.0.1", 3212), "", 3212, null, jh1);
//            doAnswer( new Answer() {
//                private final int m_count = 0;
//                @Override
//                public Object answer(InvocationOnMock invocation) {
//                    Object[] args = invocation.getArguments();
//                    SocketChannel sc = (SocketChannel)args[0];
//                    InetSocketAddress address = (InetSocketAddress)args[1];
//                    Object mock = invocation.getMock();
//                    return null;
//                }
//            }).when(jh1).requestJoin(any(SocketChannel.class), any(InetSocketAddress.class));
//            final JoinHandler jh2 = mock(JoinHandler.class);
//            final SocketJoiner joiner2 = new SocketJoiner(new InetSocketAddress("127.0.0.1", 3212), "", 3213, null, jh2);
//            final JoinHandler jh3 = mock(JoinHandler.class);
//            final SocketJoiner joiner3 = new SocketJoiner(new InetSocketAddress("127.0.0.1", 3212), "", 3214, null, jh3);
//
//            CountDownLatch cdl = new CountDownLatch(1);
//            joiner1.start(cdl);
//            Thread t1 = new Thread() {
//                @Override
//                public void run() {
//                    try {
//                        joiner2.start(null);
//                    } catch (Exception e) {
//                        e.printStackTrace();
//                    }
//                }
//            };
//            t1.start();
//            Thread.sleep(10);
//            verify(jh1, never()).requestJoin(any(SocketChannel.class), any(InetSocketAddress.class));
//            verify(jh2, never()).notifyOfHosts(
//                    anyInt(), any(int[].class), any(SocketChannel[].class), any(InetSocketAddress[].class));
//            verify(jh3, never()).notifyOfHosts(
//                    anyInt(), any(int[].class), any(SocketChannel[].class), any(InetSocketAddress[].class));
//            cdl.countDown();
//            verify(jh1, timeout(1000)).requestJoin(
//                    isNotNull(SocketChannel.class),
//                    eq(new InetSocketAddress("127.0.0.1", 3213)));
//            verify(jh2, timeout(1000)).notifyOfHosts(
//                    eq(1), any(int[].class),
//                    any(SocketChannel[].class),
//                    eq(new InetSocketAddress[] { new InetSocketAddress("127.0.0.1", 3212) }));
//            verify(jh3, never()).notifyOfHosts(
//                    anyInt(), any(int[].class), any(SocketChannel[].class), any(InetSocketAddress[].class));
//            joiner3.start(null);
//            verify(jh1, timeout(1000)).requestJoin(
//                    isNotNull(SocketChannel.class),
//                    eq(new InetSocketAddress("127.0.0.1", 3214)));
//            verify(jh2, timeout(1000)).notifyOfJoin(
//                    eq(2), any(SocketChannel.class), new InetSocketAddress("127.0.0.1", 3214));
//            verify(jh3, never()).notifyOfHosts(
//                    anyInt(), any(int[].class), any(SocketChannel[].class),
//                    eq(new InetSocketAddress[] {
//                        new InetSocketAddress("127.0.0.1", 3212), new InetSocketAddress("127.0.0.1", 3213) }));
//
//            joiner1.shutdown();
//            joiner2.shutdown();
//            joiner3.shutdown();
//            assertTrue(true);
//            return;
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        assertTrue(false);
//    }

    private static HostMessenger.Config getConfig() {
        HostMessenger.Config config = new HostMessenger.Config(m_portGenerator);
        config.factory = new MessageFactory();
        return config;
    }

    public void testSimple() throws Exception {
        HostMessenger msg1 = new HostMessenger(getConfig());
        msg1.start();
        HostMessenger msg2 = new HostMessenger(getConfig());
        msg2.start();

        System.out.println("Waiting for socketjoiners...");
        msg1.waitForGroupJoin(2);
        System.out.println("Finished socket joiner for msg1");
        msg2.waitForGroupJoin(2);
        System.out.println("Finished socket joiner for msg2");

        assertEquals(msg1.getHostId(), 0);
        assertEquals(msg2.getHostId(), 1);

        Mailbox mb1 = msg1.createMailbox();
        Mailbox mb2 = msg2.createMailbox();
        long siteId2 = mb2.getHSId();

        MsgTest.initWithSize(16);
        MsgTest mt = new MsgTest();
        mt.setValues();
        mb1.send(siteId2, mt);
        MsgTest mt2 = null;
        while (mt2 == null) {
            mt2 = (MsgTest) mb2.recv();
        }
        assertTrue(mt2.verify());

        // Do it again
        MsgTest.initWithSize(32);
        mt = new MsgTest();
        mt.setValues();
        mb1.send(siteId2, mt);
        mt2 = null;
        while (mt2 == null) {
            mt2 = (MsgTest) mb2.recv();
        }
        assertTrue(mt2.verify());

        // Do it a final time with a message that should block on write.
        // spin on a complete network message here - maybe better to write
        // the above cases this way too?
        for (int i=0; i < 3; ++i) {
            MsgTest.initWithSize(4280034);
            mt = new MsgTest();
            mt.setValues();
            mb1.send(siteId2, mt);
            mt2 = null;
            while (mt2 == null) {
                mt2 = (MsgTest) mb2.recv();
            }
            assertTrue(mt.verify());
        }
        msg1.shutdown();
        msg2.shutdown();
    }
}
