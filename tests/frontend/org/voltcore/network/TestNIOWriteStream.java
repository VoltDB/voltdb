/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB Inc. are licensed under the following
 * terms and conditions:
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
/* Copyright (C) 2008
 * Evan Jones
 * Massachusetts Institute of Technology
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
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package org.voltcore.network;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SelectionKey;
import java.util.concurrent.atomic.AtomicLong;

import junit.framework.TestCase;

import org.voltcore.utils.EstTime;
import org.voltcore.utils.EstTimeUpdater;

public class TestNIOWriteStream extends TestCase {

    private class MockPort extends VoltPort {

        @Override
        public String toString() {
            return null;
        }

        public MockPort() throws UnknownHostException {
            super(null, null, new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 21212), pool);
        }

        @Override
        public void setInterests(int opsToAdd, int opsToRemove) {
            this.opsToAdd = opsToAdd;
        }

        public boolean checkWriteSet() {
            if (opsToAdd == SelectionKey.OP_WRITE) {
                opsToAdd = 0;
                return true;
            }
            return false;
        }

        int opsToAdd;
    }

    NetworkDBBPool pool;

    @Override
    public void setUp() {
        pool = new NetworkDBBPool();
    }

    @Override
    public void tearDown() {
        pool.clear();
    }

    /**
     * Mock channel that will either consume all, some or no
     * bytes from the buffer.
     */
    private static class MockChannel implements GatheringByteChannel {
        MockChannel(int behavior) {
            m_behavior = behavior;
        }

        private boolean wroteSizeZero = false;
        private boolean didOversizeWrite = false;
        private boolean wrotePartial = false;

        @Override
        public int write(ByteBuffer src) throws IOException {
            if (!m_open) throw new IOException();

            if (!src.hasRemaining()) {
                wroteSizeZero = true;
            }

            if (!src.isDirect()) {
                if (src.remaining() > 1024 * 256) {
                    didOversizeWrite = true;
                }
            }
            if (m_behavior == SINK) {
                int remaining = src.remaining();
                src.position(src.limit());
                return remaining;
            }
            else if (m_behavior == FULL) {
                return 0;
            }
            else if (m_behavior == PARTIAL) {
                if (wrotePartial) {
                    return 0;
                } else {
                    wrotePartial = true;
                }
                ByteBuffer copy = ByteBuffer.allocate(src.remaining());
                src.get(copy.array(), 0, src.remaining()/2);
                return src.remaining();
            }
            assert(false);
            return -1;
        }

        @Override
        public long write(ByteBuffer src[]) throws IOException {
            if (!m_open) throw new IOException();

            if (m_behavior == SINK) {
                int remaining = src[0].remaining();
                src[0].position(src[0].limit());
                return remaining;
            }
            else if (m_behavior == FULL) {
                return 0;
            }
            else if (m_behavior == PARTIAL) {
                if (wrotePartial) {
                    return 0;
                } else {
                    wrotePartial = true;
                }
                ByteBuffer copy = ByteBuffer.allocate(src[0].remaining());
                src[0].get(copy.array(), 0, src[0].remaining()/2);
                return src[0].remaining();
            }
            assert(false);
            return -1;
        }

        @Override
        public void close() throws IOException {
            // TODO Auto-generated method stub
        }

        @Override
        public boolean isOpen() {
            return m_open;
        }

        public boolean m_open = true;

        public int m_behavior;
        public static int SINK = 0;     // accept all data
        public static int FULL = 1;     // accept no data
        public static int PARTIAL = 2;  // accept some data
        @Override
        public long write(ByteBuffer[] srcs, int offset, int length)
                throws IOException {
            // TODO Auto-generated method stub
            return 0;
        }
    }


    public void testSink() throws IOException {
        MockChannel channel = new MockChannel(MockChannel.SINK);
        MockPort port = new MockPort();
        NIOWriteStream wstream = new NIOWriteStream(port);
        assertTrue(wstream.isEmpty());

        ByteBuffer tmp = ByteBuffer.allocate(2);
        tmp.put((byte) 1);
        tmp.put((byte) 2);
        tmp.flip();
        wstream.enqueue(tmp);
        assertTrue(port.checkWriteSet());
        assertEquals(1, wstream.getOutstandingMessageCount());
        wstream.swapAndSerializeQueuedWrites(pool);
        assertEquals(2, wstream.drainTo(channel));
        assertTrue(wstream.isEmpty());
        assertEquals(0, wstream.getOutstandingMessageCount());
        wstream.shutdown();
        port.toString();
    }

    public void testFull() throws IOException {
        MockChannel channel = new MockChannel(MockChannel.FULL);
        MockPort port = new MockPort();
        NIOWriteStream wstream = new NIOWriteStream(port);
        assertTrue(wstream.isEmpty());

        ByteBuffer tmp = ByteBuffer.allocate(4);
        tmp.put((byte)1);
        tmp.put((byte)2);
        tmp.put((byte)3);
        tmp.put((byte)4);
        tmp.flip();
        wstream.enqueue(tmp);
        assertTrue(port.checkWriteSet());
        assertEquals(1, wstream.getOutstandingMessageCount());
        wstream.swapAndSerializeQueuedWrites(pool);
        wstream.drainTo(channel);
        assertFalse(wstream.isEmpty());

        channel.m_behavior = MockChannel.SINK;
        wstream.swapAndSerializeQueuedWrites(pool);
        int wrote = wstream.drainTo(channel);
        assertEquals(4, wrote);
        assertTrue(wstream.isEmpty());
        assertEquals(0, wstream.getOutstandingMessageCount());
        wstream.shutdown();
    }

    public void testPartial() throws IOException {
        MockChannel channel = new MockChannel(MockChannel.PARTIAL);
        MockPort port = new MockPort();
        NIOWriteStream wstream = new NIOWriteStream(port);
        assertTrue(wstream.isEmpty());

        ByteBuffer tmp = ByteBuffer.allocate(4);
        tmp.put((byte)1);
        tmp.put((byte)2);
        tmp.put((byte)3);
        tmp.put((byte)4);
        tmp.flip();
        wstream.enqueue(tmp);
        assertTrue(port.checkWriteSet());
        wstream.swapAndSerializeQueuedWrites(pool);
        int wrote = wstream.drainTo(channel);
        assertFalse(wstream.isEmpty());
        assertEquals(2, wrote);

        channel.wrotePartial = false;

        ByteBuffer tmp2 = ByteBuffer.allocate(4);
        tmp2.put((byte)5);
        tmp2.put((byte)6);
        tmp2.put((byte)7);
        tmp2.put((byte)8);
        tmp2.flip();
        wstream.enqueue(tmp2);
        wstream.swapAndSerializeQueuedWrites(pool);
        wrote += wstream.drainTo( channel);
        assertFalse(wstream.isEmpty());
        // wrote half of half of the first buffer (note +=)
        assertEquals(3, wrote);

        channel.m_behavior = MockChannel.SINK;
        wstream.swapAndSerializeQueuedWrites(pool);
        wrote += wstream.drainTo( channel);
        assertEquals(8, wrote);
        assertTrue(wstream.isEmpty());
        wstream.shutdown();
    }

    public void testClosed() throws IOException {
        MockChannel channel = new MockChannel(MockChannel.FULL);
        MockPort port = new MockPort();
        NIOWriteStream wstream = new NIOWriteStream(port);

        ByteBuffer tmp = ByteBuffer.allocate(4);
        tmp.put((byte)1);
        tmp.put((byte)2);
        tmp.put((byte)3);
        tmp.put((byte)4);
        tmp.flip();
        wstream.enqueue(tmp);
        assertTrue(port.checkWriteSet());
        wstream.swapAndSerializeQueuedWrites(pool);
        int closed = wstream.drainTo(channel);
        assertEquals(closed, 0);

        channel.m_open = false;

        boolean threwException = false;
        try {
            wstream.swapAndSerializeQueuedWrites(pool);
            assertEquals( -1, wstream.drainTo( channel));
        } catch (IOException e) {
            threwException = true;
        }
        assertTrue(threwException);
        wstream.shutdown();

    }

    public void testLargeNonDirectWrite() throws IOException {
        MockChannel channel = new MockChannel(MockChannel.SINK);
        MockPort port = new MockPort();
        NIOWriteStream wstream = new NIOWriteStream(port);

        ByteBuffer tmp = ByteBuffer.allocate(262144 * 3);
        wstream.enqueue(tmp);
        assertTrue(port.checkWriteSet());
        wstream.swapAndSerializeQueuedWrites(pool);
        int written = wstream.drainTo( channel);
        assertEquals( 262144 * 3, written);
        assertFalse(channel.didOversizeWrite);
        wstream.shutdown();
    }

    public void testLastWriteDelta() throws Exception {
        final MockChannel channel = new MockChannel(MockChannel.SINK);
        MockPort port = new MockPort();
        NIOWriteStream wstream = new NIOWriteStream(port);

        assertEquals( 0, wstream.calculatePendingWriteDelta(999));

        EstTimeUpdater.update(System.currentTimeMillis());

        /**
         * Test the basic write and drain
         */
        final ByteBuffer b = ByteBuffer.allocate(5);
        wstream.enqueue(b.duplicate());
        assertEquals( 5, wstream.calculatePendingWriteDelta(EstTime.currentTimeMillis() + 5));
        wstream.swapAndSerializeQueuedWrites(pool);
        wstream.drainTo( channel);
        assertEquals( 0, wstream.calculatePendingWriteDelta(EstTime.currentTimeMillis() + 5));

        Thread.sleep(20);
        EstTimeUpdater.update(System.currentTimeMillis());

        wstream.enqueue(b.duplicate());
        assertEquals( 5, wstream.calculatePendingWriteDelta(EstTime.currentTimeMillis() + 5));
        wstream.enqueue(b.duplicate());
        assertEquals( 5, wstream.calculatePendingWriteDelta(EstTime.currentTimeMillis() + 5));
        channel.m_behavior = MockChannel.PARTIAL;
        wstream.swapAndSerializeQueuedWrites(pool);
        wstream.drainTo( channel );
        assertEquals( 5, wstream.calculatePendingWriteDelta(EstTime.currentTimeMillis() + 5));
        wstream.shutdown();
    }

    public void testQueueMonitor() throws Exception {
        final MockChannel channel = new MockChannel(MockChannel.FULL);
        MockPort port = new MockPort();
        final AtomicLong queue = new AtomicLong();
        NIOWriteStream wstream = new NIOWriteStream(port, null, null, new QueueMonitor() {

            @Override
            public boolean queue(int bytes) {
                queue.addAndGet(bytes);
                return false;
            }
        });
        wstream.enqueue(ByteBuffer.allocate(32));
        wstream.swapAndSerializeQueuedWrites(pool);
        assertEquals(32, queue.get());
        wstream.shutdown();
        assertEquals(0, queue.get());
    }

}
