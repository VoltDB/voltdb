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
package org.voltdb.exportclient;

import java.nio.ByteBuffer;

import junit.framework.TestCase;

import org.voltdb.export.ExportProtoMessage;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;

public class TestExportDataSink extends TestCase {

    static final String TABLE_SIGNATURE = "foo";
    static final int PARTITION_ID = 2;

    class TestExportDecoder extends ExportDecoderBase
    {
        public TestExportDecoder(AdvertisedDataSource source)
        {
            super(source);
        }

        @Override
        public boolean processRow(int rowSize, byte[] rowData) {
            return true;
        }

        @Override
        public void sourceNoLongerAdvertised(AdvertisedDataSource source) {
            // TODO Auto-generated method stub

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
        String CONN_NAME = "ryanlovestheyankees";
        ExportDataSink dut =
            new ExportDataSink( 0, PARTITION_ID, TABLE_SIGNATURE, "coffeetable",
                           new TestExportDecoder(new AdvertisedDataSource(PARTITION_ID,
                                                                          TABLE_SIGNATURE,
                                                                          "coffeetable", 0, 32,
                                                                          null, null)));
        dut.addExportConnection(CONN_NAME, 0);
        assertNull(dut.getTxQueue(CONN_NAME).peek());
        dut.work();
        assertNotNull(dut.getTxQueue(CONN_NAME).peek());
        ExportProtoMessage m = dut.getTxQueue(CONN_NAME).poll();
        assertTrue(m.isPoll());
        assertEquals(Long.MIN_VALUE, m.getAckOffset());
        assertTrue(m.isAck());
        // move the offset along a bit
        m = new ExportProtoMessage( 0,PARTITION_ID, TABLE_SIGNATURE);
        m.pollResponse(0, makeFakePollData(10));
        dut.getRxQueue(CONN_NAME).offer(m);
        dut.work();
        assertNotNull(dut.getTxQueue(CONN_NAME).peek());
        m = dut.getTxQueue(CONN_NAME).poll();
        assertTrue(m.isPoll());
        assertTrue(m.isAck());
        assertEquals(0, m.getAckOffset());
        m = new ExportProtoMessage( 0, PARTITION_ID, TABLE_SIGNATURE);
        m.pollResponse(10, makeFakePollData(20));
        dut.getRxQueue(CONN_NAME).offer(m);
        dut.work();
        assertNotNull(dut.getTxQueue(CONN_NAME).peek());
        m = dut.getTxQueue(CONN_NAME).poll();
        assertTrue(m.isPoll());
        assertTrue(m.isAck());
        assertEquals(10, m.getAckOffset());
        // stall the poll and verify the incoming message is just a poll
        m = new ExportProtoMessage( 0, PARTITION_ID, TABLE_SIGNATURE);
        m.pollResponse(20, makeFakePollData(0));
        dut.getRxQueue(CONN_NAME).offer(m);
        dut.work();
        assertNotNull(dut.getTxQueue(CONN_NAME).peek());
        m = dut.getTxQueue(CONN_NAME).poll();
        assertTrue(m.isPoll());
        assertTrue(m.isAck());
        assertEquals(m.getAckOffset(), 10);
    }
}
