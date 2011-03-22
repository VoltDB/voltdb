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

package org.voltdb.export;

import java.io.IOException;
import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DeferredSerialization;

public class TestExportProtoMessage extends TestCase {

    private void assertMsgType(ExportProtoMessage m, int skip) {
        if ((skip & ExportProtoMessage.kOpen) == 0)
            assertFalse(m.isOpen());
        if ((skip & ExportProtoMessage.kOpenResponse) == 0)
            assertFalse(m.isOpenResponse());
        if ((skip & ExportProtoMessage.kPoll) == 0)
            assertFalse(m.isPoll());
        if ((skip & ExportProtoMessage.kPollResponse) == 0)
            assertFalse(m.isPollResponse());
        if ((skip & ExportProtoMessage.kAck) == 0)
            assertFalse(m.isAck());
        if ((skip & ExportProtoMessage.kClose) == 0)
            assertFalse(m.isClose());
        if ((skip & ExportProtoMessage.kError) == 0)
            assertFalse(m.isError());
    }

    public void testIsOpen() {
        ExportProtoMessage m = new ExportProtoMessage( 0, 1, "");
        m.open();
        assertTrue(m.isOpen());
        assertMsgType(m, ExportProtoMessage.kOpen);
    }

    public void testIsOpenResponse() {
        ExportProtoMessage m = new ExportProtoMessage( 0, 1, "");
        m.openResponse(null);
        assertTrue(m.isOpenResponse());
        assertMsgType(m, ExportProtoMessage.kOpenResponse);
    }

    public void testIsPoll() {
        ExportProtoMessage m = new ExportProtoMessage( 0, 1, "");
        m.poll();
        assertTrue(m.isPoll());
        assertMsgType(m, ExportProtoMessage.kPoll);
    }

    public void testIsPollResponse() {
        ExportProtoMessage m = new ExportProtoMessage( 0, 1, "");
        ByteBuffer bb = ByteBuffer.allocate(10);
        m.pollResponse(1024, bb);
        assertTrue(m.isPollResponse());
        assertTrue(m.getData() == bb);
        assertTrue(m.getAckOffset() == 1024);
        assertMsgType(m, ExportProtoMessage.kPollResponse);
    }

    public void testIsAck() {
        ExportProtoMessage m = new ExportProtoMessage( -1, 1, "");
        m.ack(2048);
        assertTrue(m.isAck());
        assertTrue(m.getAckOffset() == 2048);
        assertMsgType(m, ExportProtoMessage.kAck);
    }

    public void testIsClose() {
        ExportProtoMessage m = new ExportProtoMessage( -1, 1, "");
        m.close();
        assertTrue(m.isClose());
        assertMsgType(m, ExportProtoMessage.kClose);
    }

    public void testIsError() {
        ExportProtoMessage m = new ExportProtoMessage( -1, 1, "");
        m.error();
        assertTrue(m.isError());
        assertMsgType(m, ExportProtoMessage.kError);
    }

    // also tests toBuffer, serializableBytes()
    // serialize with bytebuffer data
    public void testReadExternal1() throws IOException {
        ExportProtoMessage m1, m2;

        m1 = new ExportProtoMessage( -1, 1, "foo");
        ByteBuffer b = ByteBuffer.allocate(100);
        b.putInt(100);
        b.putInt(200);
        b.flip();
        m1.pollResponse(1000, b);

        ByteBuffer bm1 = m1.toBuffer();
        FastDeserializer fdsm1 = new FastDeserializer(bm1);

        // test the length prefix, which isn't consumed by readExternal
        assertEquals(m1.serializableBytes(), fdsm1.readInt());
        m2 = ExportProtoMessage.readExternal(fdsm1);

        assertTrue(m2.isPollResponse());
        assertMsgType(m2, ExportProtoMessage.kPollResponse);
        assertEquals(1000, m2.getAckOffset());
        assertEquals(100, m2.getData().getInt());
        assertEquals(200, m2.getData().getInt());
        assertEquals(1, m2.getPartitionId());
        assertEquals("foo", m2.getSignature());
    }

    // also tests toBuffer, serializableBytes()
    // with null data payload
    public void testReadExternal2() throws IOException {
        ExportProtoMessage m1, m2;

        m1 = new ExportProtoMessage( -1, 1, "foo");
        m1.ack(2000);

        ByteBuffer bm1 = m1.toBuffer();
        FastDeserializer fdsm1 = new FastDeserializer(bm1);

        // test the length prefix, which isn't consumed by readExternal
        assertEquals(m1.serializableBytes(), fdsm1.readInt());
        m2 = ExportProtoMessage.readExternal(fdsm1);

        assertTrue(m2.isAck());
        assertMsgType(m2, ExportProtoMessage.kAck);
        assertEquals(2000, m2.getAckOffset());
        assertEquals(0, m2.getData().capacity());
        assertEquals(1, m2.getPartitionId());
        assertEquals("foo", m2.getSignature());
    }

    // also tests toBuffer, serializableBytes()
    // serialize with 0 capacity bytebuffer data
    public void testReadExternal3() throws IOException {
        ExportProtoMessage m1, m2;

        m1 = new ExportProtoMessage( -1, 1, "foo");
        ByteBuffer b = ByteBuffer.allocate(0);
        m1.pollResponse(1000, b);

        ByteBuffer bm1 = m1.toBuffer();
        FastDeserializer fdsm1 = new FastDeserializer(bm1);

        // test the length prefix, which isn't consumed by readExternal
        assertEquals(m1.serializableBytes(), fdsm1.readInt());
        m2 = ExportProtoMessage.readExternal(fdsm1);

        assertTrue(m2.isPollResponse());
        assertMsgType(m2, ExportProtoMessage.kPollResponse);
        assertEquals(1000, m2.getAckOffset());
        assertEquals(0, m2.getData().capacity());
        assertEquals(1, m2.getPartitionId());
        assertEquals("foo", m2.getSignature());
    }

    // mimic what raw processor and the el poller do to each other
    public void testELPollerPattern() throws IOException {
        final ExportProtoMessage r = new ExportProtoMessage( -1, 1, "bar");
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
        assertEquals(39, b.getInt());
        FastDeserializer fds = new FastDeserializer(b);
        ExportProtoMessage m = ExportProtoMessage.readExternal(fds);
        assertEquals(1, m.m_partitionId);
        assertEquals("bar", m.getSignature());
        assertTrue(m.isPollResponse());
        assertTrue(m.getData().remaining() == 8);
        assertMsgType(m, ExportProtoMessage.kPollResponse);

    }

}
