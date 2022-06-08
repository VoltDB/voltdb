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

package org.voltcore.messaging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Selector;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang3.RandomStringUtils;
import org.voltcore.utils.PortGenerator;

import junit.framework.TestCase;

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

    private static List<HostMessenger.Config> getConfigs(int hostCount) {
        List<HostMessenger.Config> configs = HostMessenger.Config.generate(m_portGenerator, hostCount);
        for (HostMessenger.Config config: configs) {
            config.factory = new MessageFactory();
        }
        return configs;
    }

    public void testSimple() throws Exception {
        List<HostMessenger.Config> configs = getConfigs(2);
        HostMessenger msg1 = createHostMessenger(configs.get(0));
        msg1.start();
        HostMessenger msg2 = createHostMessenger(configs.get(1));
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

    public void testMultiMailbox() throws Exception {
        List<HostMessenger.Config> configs = getConfigs(3);
        HostMessenger msg1 = createHostMessenger(configs.get(0));
        msg1.start();
        HostMessenger msg2 = createHostMessenger(configs.get(1));
        msg2.start();
        HostMessenger msg3 = createHostMessenger(configs.get(2));
        msg3.start();

        System.out.println("Waiting for socketjoiners...");
        msg1.waitForGroupJoin(3);
        System.out.println("Finished socket joiner for msg1");
        msg2.waitForGroupJoin(3);
        System.out.println("Finished socket joiner for msg2");
        msg3.waitForGroupJoin(3);
        System.out.println("Finished socket joiner for msg3");

        assertTrue(msg1.getHostId() != msg2.getHostId() && msg2.getHostId() != msg3.getHostId());
        //assertTrue(msg2.getHostId() == 1);
        //assertTrue(msg3.getHostId() == 2);

        Mailbox mb1 = msg1.createMailbox();
        Mailbox mb2 = msg2.createMailbox();
        Mailbox mb3 = msg3.createMailbox();
        Mailbox mb4 = msg3.createMailbox();
        Mailbox mb5 = msg1.createMailbox();

        long siteId5 = mb5.getHSId();

        long siteId2 = mb2.getHSId();

        long siteId3 = mb3.getHSId();
        long siteId4 = mb4.getHSId();



        MsgTest.initWithSize(16);
        MsgTest mt = new MsgTest();
        mt.setValues();

        int msgCount = 0;

        mb1.send(new long[] {siteId2,siteId3,siteId5,siteId4}, mt);
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

        mb3.send(new long[] {siteId5}, mt);

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
    }

    class MockNewNode extends Thread {
        AtomicBoolean m_ready = new AtomicBoolean(false);
        final HostMessenger.Config config;

        MockNewNode(HostMessenger.Config config) {
            this.config = config;
        }

        void waitUntilReady() {
            while (!m_ready.get())
                Thread.yield();
        }

        @Override
        public void run() {
            try {
                HostMessenger msg = createHostMessenger(config);
                msg.start();
                m_ready.set(true);
                msg.waitForGroupJoin(2);
            } catch (Exception e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

    public void testFailAndRejoin() throws Exception {
        /* Why is throwing away a selector interesting !? */
        try {
            Selector.open();
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
        List<HostMessenger.Config> configs = getConfigs(2);

        HostMessenger msg1 = createHostMessenger(configs.get(0));
        msg1.start();
        HostMessenger msg2 = createHostMessenger(configs.get(1));
        msg2.start();
        System.out.println("Waiting for socketjoiners...");
        msg1.waitForGroupJoin(2);
        System.out.println("Finished socket joiner for msg1");
        msg2.waitForGroupJoin(2);
        System.out.println("Finished socket joiner for msg2");

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
        MockNewNode newnode = new MockNewNode(configs.get(1));
        newnode.start();
        newnode.waitUntilReady();
        // this is just for extra safety
        Thread.sleep(50);

        // this timeout is rather lousy, but neither is it exception safe!
        newnode.join(1000);
        if (newnode.isAlive()) fail();

        msg1.shutdown();
    }

    private HostMessenger createHostMessenger(HostMessenger.Config config) {
        return new HostMessenger(config, null, RandomStringUtils.random(20));
    }
}
