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

package org.voltdb.export.processors;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.LinkedBlockingDeque;

import junit.framework.TestCase;

import org.voltdb.MockVoltDB;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.export.*;
import org.voltdb.export.processors.RawProcessor.ProtoStateBlock;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.MessagingException;
import org.voltdb.network.Connection;
import org.voltdb.network.NIOReadStream;
import org.voltdb.network.WriteStream;
import org.voltdb.utils.DBBPool;
import org.voltdb.utils.VoltFile;
import org.voltdb.utils.DBBPool.BBContainer;
import org.voltdb.utils.DeferredSerialization;

public class TestRawProcessor extends TestCase {

    static class MockWriteStream implements WriteStream {
        DBBPool pool = new DBBPool();

        LinkedBlockingDeque<ExportProtoMessage> writequeue =
            new LinkedBlockingDeque<ExportProtoMessage>();

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
                ExportProtoMessage m = ExportProtoMessage.readExternal(fds);
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

        @Override
        public int getOutstandingMessageCount()
        {
            return writequeue.size();
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
        public String getHostnameOrIP() {
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

        public ExportProtoMessage pollWriteStream() {
            return m_writeStream.writequeue.poll();
        }

        @Override
        public void scheduleRunnable(Runnable r) {
        }

        @Override
        public void unregister() {
            // TODO Auto-generated method stub

        }

        @Override
        public long connectionId()
        {
            // TODO Auto-generated method stub
            return -1;
        }
    }

    static class MockExportDataSource extends ExportDataSource {
        LinkedBlockingDeque<ExportProtoMessage> eequeue =
            new LinkedBlockingDeque<ExportProtoMessage>();


        // not really sure how much of this is needed, but pass at least
        // a mostly real set of CatalogMaps to the datasource ctor.
        public static MockVoltDB m_mockVoltDB = new MockVoltDB();
        public static int m_host = 0;
        public static int m_site = 1;
        public static int m_part = 2;

        static {
            m_mockVoltDB.addHost(m_host);
            m_mockVoltDB.addPartition(m_part);
            m_mockVoltDB.addSite(m_site, m_host, m_part, true);
            m_mockVoltDB.addTable("TableName", false);
            m_mockVoltDB.addColumnToTable("TableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
            m_mockVoltDB.addColumnToTable("TableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);
            VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        }

        public MockExportDataSource(String db, String tableName,
                                 int partitionId, int siteId, String tableSignature) throws Exception
        {
            super(null, db, tableName, partitionId, siteId, tableSignature, 0,
                  m_mockVoltDB.getCatalogContext().database.getTables().get("TableName").getColumns(),
                  "/tmp/" + System.getProperty("user.name"));
        }

        @Override
        public void exportAction(RawProcessor.ExportInternalMessage m) throws MessagingException {
            // Simulate what ExecutionEngineJNI and ExecutionSite do.
            if (m.m_m.isPoll()) {
                ExportProtoMessage r =
                    new ExportProtoMessage( m.m_m.getGeneration(), m.m_m.getPartitionId(), m.m_m.getSignature());
                ByteBuffer data = ByteBuffer.allocate(8);
                data.putInt(100); // some fake poll data
                data.putInt(200); // more fake poll data
                r.pollResponse(2000, data);
                eequeue.add(r);
            }
        }
    }


    RawProcessor rp;
    MockConnection c;
    MockExportDataSource ds;
    ProtoStateBlock sb;
    ExportProtoMessage m, bad_m1, bad_m2, m_postadmin;

    @Override
    public void setUp() throws Exception {
        File directory = new File("/tmp/" + System.getProperty("user.name"));
        VoltFile.recursivelyDelete(directory);
        if (!directory.exists()) {
            directory.mkdirs();
        }
        for (File f : directory.listFiles()) {
            if (f.getName().endsWith(".pbd") || f.getName().endsWith(".ad")) {
                f.delete();
            }
        }

        rp = new RawProcessor();
        ds = new MockExportDataSource("db", "table", 1, 3, "foo");
        ExportGeneration generation = new ExportGeneration( 0, null, directory);
        generation.addDataSource(ds);
        rp.setExportGeneration(generation);

        c = new MockConnection();
        sb = rp.new ProtoStateBlock(c, false);
        m = new ExportProtoMessage( -1, 1, "foo");
        m_postadmin = new ExportProtoMessage( -1, 1, "foo");

        // partition, site do not align match datasource
        bad_m1 = new ExportProtoMessage( -1, 10, "foo");
        bad_m2 = new ExportProtoMessage( -1, 1, "bar");
    }

    public void assertErrResponse() {
        ExportProtoMessage r = c.pollWriteStream();
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        assertTrue(r.isError());
    }

    private ExportProtoMessage pollDataSource() {
        return ds.eequeue.poll();
    }

    private ExportProtoMessage pollWriteStream() {
        return c.pollWriteStream();
    }

    // closed state

    public void testFSM_closed_open() {
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        sb.event(m.open());
        ExportProtoMessage r = pollWriteStream();
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
        ExportProtoMessage r = pollWriteStream();
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
        ExportProtoMessage r = pollDataSource();
        assertEquals(RawProcessor.CONNECTED, sb.m_state);
        assertEquals(null, r);
    }

    public void testFSM_conn_poll() {
        sb.m_state = RawProcessor.CONNECTED;
        sb.event(m.poll());
        ExportProtoMessage r = pollDataSource();
        assertEquals(RawProcessor.CONNECTED, sb.m_state);
        assertTrue(r.isPollResponse());
    }

    public void testFSM_conn_ackpoll() {
        sb.m_state = RawProcessor.CONNECTED;
        sb.event(m.poll().ack(1000));
        ExportProtoMessage r = pollDataSource();
        assertEquals(RawProcessor.CONNECTED, sb.m_state);
        assertTrue(r.isPollResponse());
    }

    // ProtoStateBlock created is non-admin, so swapping and trying
    // to use it in admin mode should result in error.  Should be re-openable
    // when admin mode switches off
    public void testFSM_adminmode()
    {
        MockExportDataSource.m_mockVoltDB.setMode(OperationMode.PAUSED);
        sb.m_state = RawProcessor.CONNECTED;
        sb.event(m.poll().ack(1000));
        assertErrResponse();
        assertEquals(RawProcessor.CLOSED, sb.m_state);
        sb.event(m_postadmin.open());
        assertErrResponse();
        MockExportDataSource.m_mockVoltDB.setMode(OperationMode.RUNNING);
        sb.event(m_postadmin.open());
        ExportProtoMessage r = pollWriteStream();
        assertTrue(r.isOpenResponse());
        assertEquals(RawProcessor.CONNECTED, sb.m_state);
        sb.event(m.poll().ack(1000));
        r = pollDataSource();
        assertEquals(RawProcessor.CONNECTED, sb.m_state);
        assertTrue(r.isPollResponse());
    }
}
