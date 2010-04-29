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

package org.voltdb.elt.processors;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingDeque;

import org.voltdb.elt.*;
import org.voltdb.elt.processors.RawProcessor.ProtoStateBlock;
import org.voltdb.messaging.*;
import org.voltdb.network.*;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.DBBPool.BBContainer;

import junit.framework.TestCase;

public class TestRawProcessor extends TestCase {

    static class MockWriteStream implements WriteStream {
        DBBPool pool = new DBBPool();

        LinkedBlockingDeque<ELTProtoMessage> writequeue =
            new LinkedBlockingDeque<ELTProtoMessage>();

        @Override
        public int calculatePendingWriteDelta(long now) {
            return 0;
        }

        @Override
        public boolean enqueue(BBContainer c) {
            return false;
        }

        @Override
        public boolean enqueue(FastSerializable f) {
            return false;
        }

        @Override
        public boolean enqueue(FastSerializable f, int expectedSize) {
            return false;
        }

        @Override
        public boolean enqueue(DeferredSerialization ds) {
            try {
                ByteBuffer b = ds.serialize(pool).b;
                b.getInt(); // eat the length prefix
                FastDeserializer fds = new FastDeserializer(b);
                ELTProtoMessage m = ELTProtoMessage.readExternal(fds);
                writequeue.add(m);
            }
            catch (IOException e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        @Override
        public boolean enqueue(ByteBuffer b) {
            return false;
        }

        @Override
        public boolean hadBackPressure() {
            return false;
        }

        @Override
        public boolean isEmpty() {
            return false;
        }
    }

    static class MockConnection implements Connection {
        MockWriteStream m_writeStream = new MockWriteStream();

        @Override
        public void disableReadSelection() {
        }

        @Override
        public void enableReadSelection() {
        }

        @Override
        public String getHostname() {
            return null;
        }

        @Override
        public NIOReadStream readStream() {
            return null;
        }

        @Override
        public WriteStream writeStream() {
            return m_writeStream;
        }

        public ELTProtoMessage pollWriteStream() {
            return m_writeStream.writequeue.poll();
        }
    }

    static class MockELTDataSource extends ELTDataSource {
        LinkedBlockingDeque<ELTProtoMessage> eequeue =
            new LinkedBlockingDeque<ELTProtoMessage>();

        public MockELTDataSource(String db, String tableName, int partitionId,
                int siteId, int tableId) {
            super(db, tableName, partitionId, siteId, tableId);
        }

        @Override
        public void poll(RawProcessor.ELTInternalMessage m) throws MessagingException {
            // Simulate what ExecutionEngineJNI and ExecutionSite do.
            if (m.m_m.isPoll()) {
                ELTProtoMessage r =
                    new ELTProtoMessage(m.m_m.getPartitionId(), m.m_m.getTableId());
                ByteBuffer data = ByteBuffer.allocate(8);
                data.putInt(100);
                data.putInt(200);
                r.pollResponse(2000, data);
                eequeue.add(r);
            }
        }
    }


    RawProcessor rp;
    MockConnection c;
    MockELTDataSource ds;
    ProtoStateBlock sb;
    ELTProtoMessage m, bad_m1, bad_m2;

    @Override
    public void setUp() {
        rp = new RawProcessor();
        ds = new MockELTDataSource("db", "table", 1, 3, 2);
        rp.addDataSource(ds);

        c = new MockConnection();
        sb = rp.new ProtoStateBlock(c);
        m = new ELTProtoMessage(1, 2);

        // partition, site do not align match datasource
        bad_m1 = new ELTProtoMessage(10, 2);
        bad_m2 = new ELTProtoMessage(1, 10);
    }

    public void assertErrResponse() {
        ELTProtoMessage r = c.pollWriteStream();
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        assertTrue(r.isError());
    }

    private ELTProtoMessage pollDataSource() {
        return ds.eequeue.poll();
    }

    private ELTProtoMessage pollWriteStream() {
        return c.pollWriteStream();
    }

    // closed state

    public void testFSM_closed_open() {
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        sb.event(m.open());
        ELTProtoMessage r = pollWriteStream();
        assertEquals(RawProcessor.CONNECTED, sb.m_state);
        assertTrue(r.isOpenResponse());
    }

    public void testFSM_closed_ack() {
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        sb.event(m.ack(1000));
        assertErrResponse();
    }

    public void testFSM_closed_poll() {
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        sb.event(m.poll());
        assertErrResponse();
    }

    public void testFSM_closed_ackpoll() {
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        sb.event(m.ack(1000).poll());
        assertErrResponse();
    }

    public void testFSM_closed_close() {
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        sb.event(m.close());
        ELTProtoMessage r = pollWriteStream();
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        assertEquals(null, r);
    }

    // connected state
    public void testFSM_conn_open() {
        sb.m_state = RawProcessor.CONNECTED;
        sb.event(m.open());
        assertErrResponse();
    }

    public void testFSM_conn_ack() {
        sb.m_state = RawProcessor.CONNECTED;
        sb.event(m.ack(1000));
        ELTProtoMessage r = pollDataSource();
        assertEquals(RawProcessor.CONNECTED, sb.m_state);
        assertEquals(null, r);
    }

    public void testFSM_conn_poll() {
        sb.m_state = RawProcessor.CONNECTED;
        sb.event(m.poll());
        ELTProtoMessage r = pollDataSource();
        assertEquals(RawProcessor.CONNECTED, sb.m_state);
        assertTrue(r.isPollResponse());
    }

    public void testFSM_conn_ackpoll() {
        sb.m_state = RawProcessor.CONNECTED;
        sb.event(m.poll().ack(1000));
        ELTProtoMessage r = pollDataSource();
        assertEquals(RawProcessor.CONNECTED, sb.m_state);
        assertTrue(r.isPollResponse());
    }


}
