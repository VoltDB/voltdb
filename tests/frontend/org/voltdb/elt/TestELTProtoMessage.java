/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.elt;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.DBBPool.BBContainer;

import junit.framework.TestCase;

public class TestELTProtoMessage extends TestCase {

    private void assertMsgType(ELTProtoMessage m, int skip) {
        if ((skip & ELTProtoMessage.kOpen) == 0)
            assertFalse(m.isOpen());
        if ((skip & ELTProtoMessage.kOpenResponse) == 0)
            assertFalse(m.isOpenResponse());
        if ((skip & ELTProtoMessage.kPoll) == 0)
            assertFalse(m.isPoll());
        if ((skip & ELTProtoMessage.kPollResponse) == 0)
            assertFalse(m.isPollResponse());
        if ((skip & ELTProtoMessage.kAck) == 0)
            assertFalse(m.isAck());
        if ((skip & ELTProtoMessage.kClose) == 0)
            assertFalse(m.isClose());
        if ((skip & ELTProtoMessage.kError) == 0)
            assertFalse(m.isError());
    }

    public void testIsOpen() {
        ELTProtoMessage m = new ELTProtoMessage(1,2);
        m.open();
        assertTrue(m.isOpen());
        assertMsgType(m, ELTProtoMessage.kOpen);
    }

    public void testIsOpenResponse() {
        ELTProtoMessage m = new ELTProtoMessage(1,2);
        m.openResponse(null);
        assertTrue(m.isOpenResponse());
        assertMsgType(m, ELTProtoMessage.kOpenResponse);
    }

    public void testIsPoll() {
        ELTProtoMessage m = new ELTProtoMessage(1,2);
        m.poll();
        assertTrue(m.isPoll());
        assertMsgType(m, ELTProtoMessage.kPoll);
    }

    public void testIsPollResponse() {
        ELTProtoMessage m = new ELTProtoMessage(1,2);
        ByteBuffer bb = ByteBuffer.allocate(10);
        m.pollResponse(1024, bb);
        assertTrue(m.isPollResponse());
        assertTrue(m.getData() == bb);
        assertTrue(m.getAckOffset() == 1024);
        assertMsgType(m, ELTProtoMessage.kPollResponse);
    }

    public void testIsAck() {
        ELTProtoMessage m = new ELTProtoMessage(1,2);
        m.ack(2048);
        assertTrue(m.isAck());
        assertTrue(m.getAckOffset() == 2048);
        assertMsgType(m, ELTProtoMessage.kAck);
    }

    public void testIsClose() {
        ELTProtoMessage m = new ELTProtoMessage(1,2);
        m.close();
        assertTrue(m.isClose());
        assertMsgType(m, ELTProtoMessage.kClose);
    }

    public void testIsError() {
        ELTProtoMessage m = new ELTProtoMessage(1,2);
        m.error();
        assertTrue(m.isError());
        assertMsgType(m, ELTProtoMessage.kError);
    }

    // also tests toBuffer, serializableBytes()
    // serialize with bytebuffer data
    public void testReadExternal1() throws IOException {
        ELTProtoMessage m1, m2;

        m1 = new ELTProtoMessage(1,2);
        ByteBuffer b = ByteBuffer.allocate(100);
        b.putInt(100);
        b.putInt(200);
        b.flip();
        m1.pollResponse(1000, b);

        ByteBuffer bm1 = m1.toBuffer();
        FastDeserializer fdsm1 = new FastDeserializer(bm1);

        // test the length prefix, which isn't consumed by readExternal
        assertEquals(m1.serializableBytes(), fdsm1.readInt());
        m2 = ELTProtoMessage.readExternal(fdsm1);

        assertTrue(m2.isPollResponse());
        assertMsgType(m2, ELTProtoMessage.kPollResponse);
        assertEquals(1000, m2.getAckOffset());
        assertEquals(100, m2.getData().getInt());
        assertEquals(200, m2.getData().getInt());
        assertEquals(1, m2.getPartitionId());
        assertEquals(2, m2.getTableId());
    }

    // also tests toBuffer, serializableBytes()
    // with null data payload
    public void testReadExternal2() throws IOException {
        ELTProtoMessage m1, m2;

        m1 = new ELTProtoMessage(1,2);
        m1.ack(2000);

        ByteBuffer bm1 = m1.toBuffer();
        FastDeserializer fdsm1 = new FastDeserializer(bm1);

        // test the length prefix, which isn't consumed by readExternal
        assertEquals(m1.serializableBytes(), fdsm1.readInt());
        m2 = ELTProtoMessage.readExternal(fdsm1);

        assertTrue(m2.isAck());
        assertMsgType(m2, ELTProtoMessage.kAck);
        assertEquals(2000, m2.getAckOffset());
        assertEquals(0, m2.getData().capacity());
        assertEquals(1, m2.getPartitionId());
        assertEquals(2, m2.getTableId());
    }

    // also tests toBuffer, serializableBytes()
    // serialize with 0 capacity bytebuffer data
    public void testReadExternal3() throws IOException {
        ELTProtoMessage m1, m2;

        m1 = new ELTProtoMessage(1,2);
        ByteBuffer b = ByteBuffer.allocate(0);
        m1.pollResponse(1000, b);

        ByteBuffer bm1 = m1.toBuffer();
        FastDeserializer fdsm1 = new FastDeserializer(bm1);

        // test the length prefix, which isn't consumed by readExternal
        assertEquals(m1.serializableBytes(), fdsm1.readInt());
        m2 = ELTProtoMessage.readExternal(fdsm1);

        assertTrue(m2.isPollResponse());
        assertMsgType(m2, ELTProtoMessage.kPollResponse);
        assertEquals(1000, m2.getAckOffset());
        assertEquals(0, m2.getData().capacity());
        assertEquals(1, m2.getPartitionId());
        assertEquals(2, m2.getTableId());
    }

    // mimic what raw processor and the el poller do to each other
    public void testELPollerPattern() throws IOException {
        final ELTProtoMessage r = new ELTProtoMessage(1, 5);
        ByteBuffer data = ByteBuffer.allocate(8);
        data.putInt(100);
        data.putInt(200);
        data.flip();
        r.pollResponse(1000, data);
        final DBBPool p = new DBBPool();
        BBContainer bb =
            new DeferredSerialization() {
                @Override
                public BBContainer serialize(DBBPool p) throws IOException {
                    FastSerializer fs = new FastSerializer(p, r.serializableBytes() + 4);
                    r.writeToFastSerializer(fs);
                    return fs.getBBContainer();
                }
                @Override
                public void cancel() {
                    // TODO Auto-generated method stub
                }
            }.serialize(p);

        ByteBuffer b = bb.b;
        assertEquals(28, b.getInt());
        FastDeserializer fds = new FastDeserializer(b);
        ELTProtoMessage m = ELTProtoMessage.readExternal(fds);
        assertEquals(1, m.m_partitionId);
        assertEquals(5, m.getTableId());
        assertTrue(m.isPollResponse());
        assertTrue(m.getData().remaining() == 8);
        assertMsgType(m, ELTProtoMessage.kPollResponse);

    }

}
