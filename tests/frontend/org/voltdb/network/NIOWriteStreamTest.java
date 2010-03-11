/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by VoltDB L.L.C. are licensed under the following
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

package org.voltdb.network;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import junit.framework.*;
import java.nio.channels.SelectionKey;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.EstTimeUpdater;

public class NIOWriteStreamTest extends TestCase {

    private static class MockPort extends VoltPort {

        @Override
        public String toString() {
            return null;
        }

        public MockPort() {
            super(null, null, 2048, "");
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

    DBBPool pool;

    @Override
    public void setUp() {
        pool = new DBBPool();
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
                if (src.remaining() > NIOWriteStream.MAX_GATHERING_WRITE) {
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

        ByteBuffer tmp = ByteBuffer.allocate(5);
        tmp.put((byte) 1);
        tmp.put((byte) 2);
        tmp.flip();
        assertTrue(wstream.enqueue(tmp));
        assertTrue(port.checkWriteSet());
        assertEquals(2, wstream.drainTo(channel, wstream.swapAndSerializeQueuedWrites(pool)));
        assertTrue(wstream.isEmpty());
        wstream.shutdown();
        port.toString();
    }

    public void testFull() throws IOException {
        MockChannel channel = new MockChannel(MockChannel.FULL);
        MockPort port = new MockPort();
        NIOWriteStream wstream = new NIOWriteStream(port);
        assertTrue(wstream.isEmpty());

        ByteBuffer tmp = ByteBuffer.allocate(5);
        tmp.put((byte)1);
        tmp.put((byte)2);
        tmp.put((byte)3);
        tmp.put((byte)4);
        tmp.flip();
        assertTrue(wstream.enqueue(tmp));
        assertTrue(port.checkWriteSet());
        assertEquals(0, wstream.drainTo(channel, wstream.swapAndSerializeQueuedWrites(pool)));
        assertFalse(wstream.isEmpty());

        channel.m_behavior = MockChannel.SINK;
        int wrote = wstream.drainTo(channel, wstream.swapAndSerializeQueuedWrites(pool));
        assertEquals(4, wrote);
        assertTrue(wstream.isEmpty());
        wstream.shutdown();
    }

    public void testPartial() throws IOException {
        MockChannel channel = new MockChannel(MockChannel.PARTIAL);
        MockPort port = new MockPort();
        NIOWriteStream wstream = new NIOWriteStream(port);
        assertTrue(wstream.isEmpty());

        ByteBuffer tmp = ByteBuffer.allocate(5);
        tmp.put((byte)1);
        tmp.put((byte)2);
        tmp.put((byte)3);
        tmp.put((byte)4);
        tmp.flip();
        assertTrue(wstream.enqueue(tmp));
        assertTrue(port.checkWriteSet());
        int wrote = wstream.drainTo(channel, wstream.swapAndSerializeQueuedWrites(pool));
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
        org.voltdb.utils.DBBPool.BBContainer containers[] = wstream.swapAndSerializeQueuedWrites(pool);
        wrote += wstream.drainTo( channel, containers);
        assertFalse(wstream.isEmpty());
        // wrote half of half of the first buffer (note +=)
        assertEquals(3, wrote);

        channel.m_behavior = MockChannel.SINK;
        wrote += wstream.drainTo( channel, wstream.swapAndSerializeQueuedWrites(pool));
        assertEquals(8, wrote);
        assertTrue(wstream.isEmpty());
        wstream.shutdown();
    }

    public void testClosed() throws IOException {
        MockChannel channel = new MockChannel(MockChannel.FULL);
        MockPort port = new MockPort();
        NIOWriteStream wstream = new NIOWriteStream(port);

        ByteBuffer tmp = ByteBuffer.allocate(5);
        tmp.put((byte)1);
        tmp.put((byte)2);
        tmp.put((byte)3);
        tmp.put((byte)4);
        tmp.flip();
        assertTrue(wstream.enqueue(tmp));
        assertTrue(port.checkWriteSet());
        int closed = wstream.drainTo(channel, wstream.swapAndSerializeQueuedWrites(pool));
        assertEquals(closed, 0);

        channel.m_open = false;

        boolean threwException = false;
        try {
            assertEquals( -1, wstream.drainTo( channel, wstream.swapAndSerializeQueuedWrites(pool)));
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

        ByteBuffer tmp = ByteBuffer.allocate(NIOWriteStream.MAX_GATHERING_WRITE * 2);

        assertTrue(wstream.enqueue(tmp));
        assertTrue(port.checkWriteSet());
        int written = wstream.drainTo( channel, wstream.swapAndSerializeQueuedWrites(pool));
        assertEquals( NIOWriteStream.MAX_GATHERING_WRITE * 2, written);
        assertFalse(channel.didOversizeWrite);
        assertTrue(channel.wroteSizeZero);
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
        wstream.drainTo( channel, wstream.swapAndSerializeQueuedWrites(pool));
        assertEquals( 0, wstream.calculatePendingWriteDelta(EstTime.currentTimeMillis() + 5));

        Thread.sleep(20);
        EstTimeUpdater.update(System.currentTimeMillis());

        wstream.enqueue(b.duplicate());
        assertEquals( 5, wstream.calculatePendingWriteDelta(EstTime.currentTimeMillis() + 5));
        wstream.enqueue(b.duplicate());
        assertEquals( 5, wstream.calculatePendingWriteDelta(EstTime.currentTimeMillis() + 5));
        channel.m_behavior = MockChannel.PARTIAL;
        wstream.drainTo( channel, wstream.swapAndSerializeQueuedWrites(pool));
        assertEquals( 5, wstream.calculatePendingWriteDelta(EstTime.currentTimeMillis() + 5));

        wstream.shutdown();
    }

}
