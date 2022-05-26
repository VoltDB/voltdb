/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
 *
 * This file contains original code and/or modifications of original code.
 * Any modifications made by Volt Active Data Inc. are licensed under the following
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
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import junit.framework.TestCase;

public class TestNIOReadStream extends TestCase {
    MockReadableByteChannel channel;
    NIOReadStream stream;
    NetworkDBBPool pool;

    private static class MockReadableByteChannel implements ReadableByteChannel {
        @Override
        public int read(ByteBuffer buffer) {
            if (nextRead == null) {
                if (end) return -1;
                return 0;
            }

            byte[] target = nextRead;
            nextRead = null;
            int length = target.length;

            // If we have too much data, split the nextRead array
            if (length > buffer.remaining()) {
                length = buffer.remaining();
                nextRead = new byte[target.length-length];
                for (int i = 0; i < nextRead.length; ++i) {
                    nextRead[i] = target[i + length];
                }
            }

            buffer.put(target, 0, length);
            return length;
        }

        @Override
        public boolean isOpen() { return !closed; }
        @Override
        public void close() {
            assert !closed;
            closed = true;
            end = true;
        }

        public byte[] nextRead;
        public boolean end = false;
        private boolean closed = false;
    }

    @Override
    public void setUp() {
        channel = new MockReadableByteChannel();
        stream = new NIOReadStream();
        pool = new NetworkDBBPool();
    }

    @Override
    public void tearDown() {
        stream.shutdown();

    }

    public void testZeroLength() {
        assertEquals(0, stream.dataAvailable());
        byte[] empty = new byte[0];
        stream.getBytes(empty);
    }

    public void testEmpty() {
        byte[] foo = new byte[1];
        try {
            stream.getBytes(foo);
            fail("expected IllegalStateException");
        } catch (IllegalStateException e) {}
    }

    public void testMultipleOneRead() throws IOException {
        channel.nextRead = new byte[]{0, 1, 2, 3,};
        assertEquals(4, stream.read(channel, 1, pool));
        assertEquals(4, stream.dataAvailable());
        byte[] single = new byte[1];
        for (int i = 0; i < 4; ++i) {
            stream.getBytes(single);
            assertEquals(i, single[0]);
        }
    }

    public void testSpanRead() throws IOException {
        // Write a block that spans multiple buffers
        final int SIZE = 4096*10;
        channel.nextRead = new byte[SIZE];
        channel.nextRead[0] = 42;
        channel.nextRead[SIZE-1] = 79;

        // Read a byte off the beginning
        assertTrue(stream.read(channel, 1, pool) >= 1);
        assertTrue(1 <= stream.dataAvailable() && stream.dataAvailable() < SIZE);
        byte[] single = new byte[1];
        stream.getBytes(single);
        assertEquals(42, single[0]);

        // Read most of the block
        byte[] most = new byte[SIZE-2];
        final int remainingToRead = (SIZE - 1) - stream.dataAvailable();
        assertEquals(remainingToRead, stream.read(channel, most.length, pool));
        assertEquals(SIZE-1, stream.dataAvailable());
        stream.getBytes(most);

        // Read the byte off the end
        stream.getBytes(single);
        assertEquals(79, single[0]);
        assertStreamIsEmpty();
    }

    public void testPeekBytes() throws Exception {
        final int SIZE = 4096*10;
        ByteBuffer netbb = ByteBuffer.allocate(SIZE);
        final int BORDER = 32 * 1024;

        // Write a block that spans multiple buffers
        channel.nextRead = netbb.array();
        netbb.putInt(2121);
        netbb.putInt(5555);

        netbb.position(BORDER - 2);
        netbb.putInt(2121);
        netbb.putInt(5555);

        assertEquals(SIZE, stream.read(channel, SIZE, pool));
        ByteBuffer intbb = ByteBuffer.allocate(Integer.BYTES);
        intbb.limit(Integer.BYTES);

        stream.peekBytes(intbb.array());
        assertEquals(2121, intbb.getInt(0));

        stream.peekBytes(intbb.array());
        assertEquals(2121, intbb.getInt(0));

        stream.getBytes(intbb.array());
        assertEquals(2121, intbb.getInt(0));

        stream.getBytes(intbb.array());
        assertEquals(5555, intbb.getInt(0));

        byte [] fillah = new byte[BORDER - 10];
        stream.getBytes(fillah);

        stream.peekBytes(intbb.array());
        assertEquals(2121, intbb.getInt(0));

        stream.peekBytes(intbb.array());
        assertEquals(2121, intbb.getInt(0));

        stream.getBytes(intbb.array());
        assertEquals(2121, intbb.getInt(0));

        stream.getBytes(intbb.array());
        assertEquals(5555, intbb.getInt(0));
    }

    public void testGetSlice() throws Exception {
        final int SIZE = 4096*10;
        ByteBuffer netbb = ByteBuffer.allocate(SIZE);
        final int BORDER = 32 * 1024;

        // Write a block that spans multiple buffers
        channel.nextRead = netbb.array();
        netbb.putInt(2121);
        netbb.putInt(5555);

        netbb.position(BORDER - 2);
        netbb.putInt(2121);
        netbb.putInt(5555);

        assertEquals(SIZE, stream.read(channel, SIZE, pool));

        NIOReadStream.Slice slc = stream.getSlice(8);
        assertEquals(8, slc.bb.writerIndex());
        assertEquals(0, slc.bb.readerIndex());
        assertEquals(2121, slc.bb.readInt());
        assertEquals(5555, slc.bb.readInt());
        assertEquals(8, slc.bb.readerIndex());

        assertEquals(0, slc.discard());

        int sliceSize = BORDER - 10;
        slc = stream.getSlice(sliceSize);
        assertEquals(sliceSize, slc.bb.writerIndex());
        assertEquals(0, slc.bb.readerIndex());
        slc.bb.readerIndex(sliceSize);
        assertFalse(slc.bb.isReadable());

        assertEquals(0, slc.discard());

        slc = stream.getSlice(8);
        assertEquals(8, slc.bb.writerIndex());
        assertEquals(0, slc.bb.readerIndex());
        assertEquals(2121, slc.bb.readInt());
        assertEquals(5555, slc.bb.readInt());
        assertEquals(8, slc.bb.readerIndex());

        assertEquals(1, slc.discard());
        assertEquals(0, slc.discard());

        sliceSize = 8186;
        slc = stream.getSlice(sliceSize);
        assertEquals(sliceSize, slc.bb.writerIndex());
        assertEquals(0, slc.bb.readerIndex());
        slc.bb.readerIndex(sliceSize);
        assertFalse(slc.bb.isReadable());

        assertEquals(1, slc.discard());
        assertEquals(0, slc.discard());
    }

    public void testMultipleReadsOneValue() throws IOException {
        final int HUGE_SIZE = 4096*16;
        byte[] huge = new byte[HUGE_SIZE];
        channel.nextRead = huge;
        assertEquals(HUGE_SIZE, stream.read(channel, HUGE_SIZE, pool));
        assertEquals(HUGE_SIZE, stream.dataAvailable());
        stream.getBytes(huge);
        assertStreamIsEmpty();
    }

    public void testIncompleteReads() throws IOException {
        channel.nextRead = new byte[1024 * 32 + 1500];
        assertEquals(1024 * 32, stream.read(channel, 1500, pool));
        assertEquals(1024 * 32, stream.dataAvailable());
        channel.nextRead = new byte[500];
        assertEquals(500, stream.read(channel, 1500, pool));
        assertEquals(1024 * 32 + 500, stream.dataAvailable());
    }

    public void testReadInt() throws IOException {
        channel.nextRead = new byte[]{1, 2, 3, 4};
        assertEquals(4, stream.read( channel, 4, pool));
        int value = stream.getInt();
        assertEquals(0x01020304, value);

        channel.nextRead = new byte[]{-1, -1, -1, -4};
        assertEquals(4, stream.read( channel, 4, pool));
        assertEquals(-4, stream.getInt());

        channel.nextRead = new byte[]{0, 0, 0, -4};
        assertEquals(4, stream.read( channel, 4, pool));
        assertEquals(252, stream.getInt());
    }

    public void testEndReadComplete() throws IOException {
        channel.nextRead = new byte[]{1, 2, 3,4 };
        channel.end = true;
        assertEquals(4, stream.read(channel, 1, pool));
        assertEquals(-1, stream.read(channel, 42, pool));
    }

   /* public void testEndReadIncomplete() throws IOException {
        channel.nextRead = new byte[]{1, 2, 3,4 };
        channel.end = true;
        assertEquals(-1, stream.fillFrom(channel, 42));
    }*/

    private void assertStreamIsEmpty() throws IOException {
        assertEquals(0, stream.read(channel, Integer.MAX_VALUE, pool));
        assertEquals(0, stream.dataAvailable());
    }
}
