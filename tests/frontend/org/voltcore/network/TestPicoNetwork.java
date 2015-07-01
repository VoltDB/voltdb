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

package org.voltcore.network;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestPicoNetwork extends TestCase {

    ServerSocketChannel ssc;
    SocketChannel networkChannel;
    SocketChannel rawChannel;
    PicoNetwork pn;
    MockInputHandler handler = new MockInputHandler();

    BlockingQueue<ByteBuffer> messages = new LinkedBlockingQueue<ByteBuffer>();

    private class MockInputHandler extends VoltProtocolHandler {
        volatile boolean startedCalled;
        volatile boolean startingCalled;
        volatile boolean stoppedCalled;
        volatile boolean stoppingCalled;
        Exception e;

        @Override
        public int getMaxRead() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public void handleMessage(ByteBuffer message, Connection c) {
            messages.offer(message);
        }

        @Override
        public void started(Connection c) {
            if (startedCalled) e = new Exception("Started called twice");
            startedCalled = true;
        }

        @Override
        public void starting(Connection c) {
            if (startingCalled) e = new Exception("Starting called twice");
            startingCalled = true;
        }

        @Override
        public void stopped(Connection c) {
            if (stoppedCalled) e = new Exception("Stopped called twice");
            stoppedCalled = true;
        }

        @Override
        public void stopping(Connection c) {
            if (stoppingCalled) e = new Exception("Stopping called twice");
            stoppingCalled = true;
        }

        @Override
        public Runnable offBackPressure() {
            return new Runnable() {
                @Override
                public void run() {
                    new Throwable("SHould never happen").printStackTrace();
                    System.exit(-1);
                }
            };
        }

        @Override
        public Runnable onBackPressure() {
            return new Runnable() {
                @Override
                public void run() {
                    new Throwable("SHould never happen").printStackTrace();
                    System.exit(-1);
                }
            };
        }

        @Override
        public QueueMonitor writestreamMonitor() {
            return null;
        }

        @Override
        public long connectionId() {
            return 0;
        }
    }
    @Override
    @Before
    public void setUp() throws Exception {
        ssc = ServerSocketChannel.open();
        InetSocketAddress addr = new InetSocketAddress(InetAddress.getLoopbackAddress(), 21212);
        ssc.bind(addr);
        rawChannel = SocketChannel.open();
        rawChannel.configureBlocking(false);
        rawChannel.connect(addr);
        networkChannel = ssc.accept();
        rawChannel.finishConnect();
        rawChannel.configureBlocking(true);
        pn = new PicoNetwork(networkChannel);
        handler = new MockInputHandler();
        pn.start(handler, new HashSet<Long>());
    }

    @Override
    @After
    public void tearDown() throws Exception {
        ssc.close();
        networkChannel.close();
        rawChannel.close();
        pn.shutdownAsync();
        messages.clear();
        handler = null;
        System.gc();
        System.runFinalization();
    }

    @Test
    public void testBasic() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(5);
        buf.putInt(1);
        buf.position(0);

        rawChannel.write(buf);

        buf = messages.take();

        assertTrue(handler.startedCalled);
        assertTrue(handler.startingCalled);

        rawChannel.close();

        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < 60000) {
            if (handler.stoppedCalled == true && handler.stoppingCalled == true) break;
        }
        if (handler.e != null) throw new Exception(handler.e);
        assertTrue(handler.stoppedCalled);
        assertTrue(handler.stoppingCalled);
    }

    @Test
    public void testLargeMessages() throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(1024 * 1024 * 30);
        buf.putInt(buf.capacity() - 4);
        buf.position(0);
        while (buf.hasRemaining()) {
            rawChannel.write(buf);
        }

        ByteBuffer receipt = messages.take();
        assertEquals(receipt.capacity(), buf.capacity() - 4);

        buf.clear();
        pn.enqueue(buf.duplicate());

        buf.clear();
        while (buf.hasRemaining()) {
            rawChannel.read(buf);
        }
    }
}
