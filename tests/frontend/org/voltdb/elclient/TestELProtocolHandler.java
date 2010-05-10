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
package org.voltdb.elclient;

import java.nio.ByteBuffer;
import java.util.ArrayList;

import org.voltdb.VoltType;
import org.voltdb.elt.ELTProtoMessage;
import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;

import junit.framework.TestCase;

public class TestELProtocolHandler extends TestCase {

    static final int TABLE_ID = 1;
    static final int PARTITION_ID = 2;

    class TestELTDecoder extends ELTDecoderBase
    {
        public TestELTDecoder(AdvertisedDataSource source)
        {
            super(source);
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) {
            return true;
        }
    }

    ByteBuffer makeFakePollData(int length)
    {
        ByteBuffer retval = ByteBuffer.allocate(length + 4);
        retval.putInt(length);
        retval.flip();
        return retval;
    }

    public void testBasicStuff()
    {
        ELDataSink dut =
            new ELDataSink(PARTITION_ID, TABLE_ID, "coffeetable",
                           new TestELTDecoder(new AdvertisedDataSource(PARTITION_ID,
                                                                       TABLE_ID,
                                                                       "coffeetable",
                                                                       null, null)));
        assertNull(dut.getTxQueue().peek());
        dut.work();
        assertNotNull(dut.getTxQueue().peek());
        ELTProtoMessage m = dut.getTxQueue().poll();
        assertTrue(m.isPoll());
        assertFalse(m.isAck());
        // move the offset along a bit
        m = new ELTProtoMessage(PARTITION_ID, TABLE_ID);
        m.pollResponse(0, makeFakePollData(10));
        dut.getRxQueue().offer(m);
        dut.work();
        assertNotNull(dut.getTxQueue().peek());
        m = dut.getTxQueue().poll();
        assertTrue(m.isPoll());
        assertTrue(m.isAck());
        assertEquals(0, m.getAckOffset());
        m = new ELTProtoMessage(PARTITION_ID, TABLE_ID);
        m.pollResponse(10, makeFakePollData(20));
        dut.getRxQueue().offer(m);
        dut.work();
        assertNotNull(dut.getTxQueue().peek());
        m = dut.getTxQueue().poll();
        assertTrue(m.isPoll());
        assertTrue(m.isAck());
        assertEquals(10, m.getAckOffset());
        // stall the poll and verify the incoming message is just a poll
        m = new ELTProtoMessage(PARTITION_ID, TABLE_ID);
        m.pollResponse(20, makeFakePollData(0));
        dut.getRxQueue().offer(m);
        dut.work();
        assertNotNull(dut.getTxQueue().peek());
        m = dut.getTxQueue().poll();
        assertTrue(m.isPoll());
        assertFalse(m.isAck());
    }
}
