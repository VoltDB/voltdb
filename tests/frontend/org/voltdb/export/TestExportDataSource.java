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

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicReference;

import junit.framework.TestCase;

import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.export.processors.RawProcessor.ExportInternalMessage;
import org.voltdb.export.processors.RawProcessor.ExportStateBlock;

public class TestExportDataSource extends TestCase {

    static {
        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
    }

    private static final int MAGIC_TUPLE_SIZE = 94;
    MockVoltDB m_mockVoltDB;
    int m_host = 0;
    int m_site = 1;
    int m_part = 2;

    @Override
    public void setUp() {
        m_mockVoltDB = new MockVoltDB();
        m_mockVoltDB.addHost(m_host);
        m_mockVoltDB.addPartition(m_part);
        m_mockVoltDB.addSite(m_site, m_host, m_part, true);
        m_mockVoltDB.addTable("TableName", false);
        m_mockVoltDB.addColumnToTable("TableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("TableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);
        m_mockVoltDB.addTable("RepTableName", false);
        m_mockVoltDB.addColumnToTable("RepTableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("RepTableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);

        File directory = new File("/tmp");
        for (File f : directory.listFiles()) {
            if (f.getName().endsWith(".pbd") || f.getName().endsWith(".ad")) {
                f.delete();
            }
        }
    }

    @Override
    public void tearDown() throws Exception {
        m_mockVoltDB.shutdown(null);
    }

    public void testExportDataSource() throws Exception
    {
        String[] tables = {"TableName", "RepTableName"};
        for (String table_name : tables)
        {
            Table table = m_mockVoltDB.getCatalogContext().database.getTables().get(table_name);
            ExportDataSource s = new ExportDataSource( null, "database",
                                                table.getTypeName(),
                                                m_part,
                                                m_site,
                                                table.getSignature(),
                                                0,
                                                table.getColumns(),
                                                "/tmp");

            assertEquals("database", s.getDatabase());
            assertEquals(table_name, s.getTableName());
            assertEquals(m_part, s.getPartitionId());
            assertEquals(m_site, s.getSiteId());
            assertEquals(table.getSignature(), s.getSignature());
            // There are 6 additional Export columns added
            assertEquals(2 + 6, s.m_columnNames.size());
            assertEquals(2 + 6, s.m_columnTypes.size());
            assertEquals("VOLT_TRANSACTION_ID", s.m_columnNames.get(0));
            assertEquals("VOLT_EXPORT_OPERATION", s.m_columnNames.get(5));
            assertEquals("COL1", s.m_columnNames.get(6));
            assertEquals("COL2", s.m_columnNames.get(7));
            assertEquals(VoltType.INTEGER.ordinal(), s.m_columnTypes.get(6).intValue());
            assertEquals(VoltType.STRING.ordinal(), s.m_columnTypes.get(7).intValue());
        }
    }

    public void testPoll() throws Exception{
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new ExportDataSource( null,
                                            "database",
                                            table.getTypeName(),
                                            m_part,
                                            m_site,
                                            table.getSignature(),
                                            0,
                                            table.getColumns(),
                                            "/tmp");
        ByteBuffer foo = ByteBuffer.allocate(20);
        s.pushExportBuffer(23, 0, foo.duplicate(), false, false);
        assertEquals(s.sizeInBytes(), 20 );

        //Push it twice more to check stats calc
        s.pushExportBuffer(43, 0, foo.duplicate(), false, false);
        assertEquals(s.sizeInBytes(), 40 );
        s.pushExportBuffer(63, 0, foo.duplicate(), false, false);

        //Only two are kept in memory, flattening the third takes 12 bytes extra so 72 instead of 60
        assertEquals(s.sizeInBytes(), 72);

        //Sync which flattens them all
        s.pushExportBuffer(63, 0, null, true, false);

        //flattened size with 60 + (12 * 3)
        assertEquals( 96, s.sizeInBytes());

        ExportProtoMessage m = new ExportProtoMessage( 0, m_part, table.getSignature());
        final AtomicReference<ExportProtoMessage> ref = new AtomicReference<ExportProtoMessage>();
        ExportStateBlock esb = new ExportStateBlock() {
            @Override
            public void event(ExportProtoMessage m) {
                ref.set(m);
            }
        };
        ExportInternalMessage pair = new ExportInternalMessage(esb, m);

        m.poll();
        s.exportAction(pair);
        //No change in size because the buffers are flattened to disk, until the whole
        //file is polled/acked it won't shrink
        assertEquals( 96, s.sizeInBytes());
        m = ref.get();

        assertEquals(m_part, m.m_partitionId);
        assertEquals(table.getSignature(), m.m_signature);
        assertEquals( 43, m.m_offset);
        foo = ByteBuffer.allocate(24);
        foo.order(ByteOrder.LITTLE_ENDIAN);
        foo.putInt(20);
        foo.position(0);
        assertEquals( foo, m.m_data);

        m.ack(43);
        m.poll();
        pair = new ExportInternalMessage(esb, m);

        s.exportAction(pair);
        //No change in size because the buffers are flattened to disk, until the whole
        //file is polled/acked it won't shrink
        assertEquals( 96, s.sizeInBytes());
        m = ref.get();

        assertEquals(m_part, m.m_partitionId);
        assertEquals(table.getSignature(), m.m_signature);
        assertEquals( 63, m.m_offset);
        assertEquals( foo, m.m_data);

        m.ack(63);
        m.poll();

        pair = new ExportInternalMessage(esb, m);
        s.exportAction(pair);
        //The two buffers pushed into a file at the head are now gone
        //One file with a single buffer remains.
        assertEquals( 32, s.sizeInBytes());
        m = ref.get();

        assertEquals(m_part, m.m_partitionId);
        assertEquals(table.getSignature(), m.m_signature);
        assertEquals( 83, m.m_offset);
        assertEquals( foo, m.m_data);

        m.ack(83);
        m.poll();
        pair = new ExportInternalMessage(esb, m);
        s.exportAction(pair);
        //4-bytes remain, this is the number of entries in the write segment
        assertEquals( 0, s.sizeInBytes());
        System.out.println(s.sizeInBytes());
        m = ref.get();

        assertEquals(m_part, m.m_partitionId);
        assertEquals(table.getSignature(), m.m_signature);
        assertEquals( 83, m.m_offset);
        assertEquals( 0, m.m_data.getInt());
    }

    /**
     * Test basic release.  create two buffers, release the first one, and
     * ensure that our next poll returns the second one.
     */
    public void testSimpleRelease() throws Exception
    {
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new ExportDataSource(
                                            null,
                                            "database",
                                            table.getTypeName(),
                                            m_part,
                                            m_site,
                                            table.getSignature(),
                                            0,
                                            table.getColumns(),
                                            "/tmp");

        // we get nothing with no data
        ExportProtoMessage m = new ExportProtoMessage( 0, m_part, table.getSignature());
        final AtomicReference<ExportProtoMessage> ref = new AtomicReference<ExportProtoMessage>();
        ExportStateBlock esb = new ExportStateBlock() {
            @Override
            public void event(ExportProtoMessage m) {
                ref.set(m);
            }
        };
        ExportInternalMessage pair = new ExportInternalMessage(esb, m);

        m.poll();
        s.exportAction(pair);

        m = ref.get();
        assertEquals(m.getAckOffset(), 0);
        assertEquals(m.m_data.getInt(), 0);

        ByteBuffer firstBuffer = ByteBuffer.allocate(MAGIC_TUPLE_SIZE * 9);
        s.pushExportBuffer(0, 0, firstBuffer, false, false);

        ByteBuffer secondBuffer = ByteBuffer.allocate(MAGIC_TUPLE_SIZE * 10);
        s.pushExportBuffer(MAGIC_TUPLE_SIZE * 9, 0, secondBuffer, false, false);

        // release the first buffer
        m.ack(MAGIC_TUPLE_SIZE * 9);
        m.poll();
        ref.set(null);
        pair = new ExportInternalMessage(esb, m);
        s.exportAction(pair);

        // now get the second
        m = ref.get();
        assertEquals(MAGIC_TUPLE_SIZE * 19, m.getAckOffset());
        assertEquals(MAGIC_TUPLE_SIZE * 10, m.m_data.remaining() - 4);
    }

    /**
     * Test that attempting to release uncommitted bytes only releases what
     * is committed
     */
    public void testReleaseUncommitted() throws Exception
    {
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new ExportDataSource(
                                            null,
                                            "database",
                                            table.getTypeName(),
                                            m_part,
                                            m_site,
                                            table.getSignature(),
                                            0,
                                            table.getColumns(),
                                            "/tmp");

        // we get nothing with no data
        ExportProtoMessage m = new ExportProtoMessage( 0, m_part, table.getSignature());
        final AtomicReference<ExportProtoMessage> ref = new AtomicReference<ExportProtoMessage>();
        ExportStateBlock esb = new ExportStateBlock() {
            @Override
            public void event(ExportProtoMessage m) {
                ref.set(m);
            }
        };
        ExportInternalMessage pair = new ExportInternalMessage(esb, m);

        m.poll();
        s.exportAction(pair);

        m = ref.get();
        assertEquals(m.getAckOffset(), 0);
        assertEquals(m.m_data.getInt(), 0);

        ByteBuffer firstBuffer = ByteBuffer.allocate(MAGIC_TUPLE_SIZE * 3);
        s.pushExportBuffer(0, 0, firstBuffer, false, false);

        // release part of the committed data
        m.ack(MAGIC_TUPLE_SIZE * 2);
        m.poll();
        pair = new ExportInternalMessage(esb, m);
        s.exportAction(pair);

        //Verify the release was done correctly, should only get one tuple back
        m.close();
        pair = new ExportInternalMessage(esb, m);
        s.exportAction(pair);
        m = ref.get();
        assertEquals(m.getAckOffset(), MAGIC_TUPLE_SIZE * 3);
        m.m_data.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        assertEquals(m.m_data.getInt(), MAGIC_TUPLE_SIZE);

        //Reset close flag
        m = new ExportProtoMessage( 0, m_part, table.getSignature());
        m.poll();
        pair = new ExportInternalMessage(esb, m);

        // now try to release past committed data
        m.ack(MAGIC_TUPLE_SIZE * 10);
        s.exportAction(pair);

        //verify that we have moved to the end of the committed data
        m = ref.get();
        assertEquals(m.getAckOffset(), MAGIC_TUPLE_SIZE * 3);
        assertEquals(m.m_data.getInt(), 0);

        ByteBuffer secondBuffer = ByteBuffer.allocate(MAGIC_TUPLE_SIZE * 6);
        s.pushExportBuffer(MAGIC_TUPLE_SIZE * 3, 0, secondBuffer, false, false);

        m.ack(MAGIC_TUPLE_SIZE * 3);
        m.poll();
        pair = new ExportInternalMessage(esb, m);
        s.exportAction(pair);

        // now, more data and make sure we get the all of it
        m = ref.get();
        assertEquals(m.getAckOffset(), MAGIC_TUPLE_SIZE * 9);
        m.m_data.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        assertEquals(m.m_data.getInt(), MAGIC_TUPLE_SIZE * 6);
    }

    /**
     * Test that attempting to release on a non-buffer boundary will return
     * the remaining un-acked partial buffer when we poll
     */
    public void testReleaseOnNonBoundary() throws Exception
    {
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new ExportDataSource(
                                            null,
                                            "database",
                                            table.getTypeName(),
                                            m_part,
                                            m_site,
                                            table.getSignature(),
                                            0,
                                            table.getColumns(),
                                            "/tmp");

        ExportProtoMessage m = new ExportProtoMessage( 0, m_part, table.getSignature());
        final AtomicReference<ExportProtoMessage> ref = new AtomicReference<ExportProtoMessage>();
        ExportStateBlock esb = new ExportStateBlock() {
            @Override
            public void event(ExportProtoMessage m) {
                ref.set(m);
            }
        };
        ExportInternalMessage pair = new ExportInternalMessage(esb, m);
        m.poll();

        ByteBuffer firstBuffer = ByteBuffer.allocate(MAGIC_TUPLE_SIZE * 9);
        s.pushExportBuffer(0, 0, firstBuffer, false, false);

        ByteBuffer secondBuffer = ByteBuffer.allocate(MAGIC_TUPLE_SIZE * 10);
        s.pushExportBuffer(MAGIC_TUPLE_SIZE * 9, 0, secondBuffer, false, false);

        // release part of the first buffer
        m.ack(MAGIC_TUPLE_SIZE * 4);
        s.exportAction(pair);

        m = ref.get();

        // get the first buffer and make sure we get the remainder
        assertEquals(m.getAckOffset(), MAGIC_TUPLE_SIZE * 9);
        m.m_data.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        assertEquals(m.m_data.getInt(), MAGIC_TUPLE_SIZE * 5);

        //Now get the second buffer
        m.ack(MAGIC_TUPLE_SIZE * 9);
        m.poll();
        pair = new ExportInternalMessage(esb, m);
        s.exportAction(pair);

        m = ref.get();
        assertEquals(m.getAckOffset(), MAGIC_TUPLE_SIZE * 19);
        m.m_data.order(java.nio.ByteOrder.LITTLE_ENDIAN);
        assertEquals(m.m_data.getInt(), MAGIC_TUPLE_SIZE * 10);
    }

//    /**
//     * Test that releasing everything in steps and then polling results in
//     * the right StreamBlock
//     */
//    public void testReleaseAllInAlignedSteps() throws Exception
//    {
//        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
//        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
//        ExportDataSource s = new ExportDataSource("database",
//                                            table.getTypeName(),
//                                            table.getIsreplicated(),
//                                            m_part,
//                                            m_site,
//                                            table.getRelativeIndex(),
//                                            table.getColumns());
//
//        // we get nothing with no data
//        ExportProtoMessage m = new ExportProtoMessage(m_part, table.getRelativeIndex());
//        RawProcessor.ExportInternalMessage pair = new RawProcessor.ExportInternalMessage(null, m);
//
//        final AtomicReference<ExportInternalMessage> ref = new AtomicReference<ExportInternalMessage>();
//        ExportManager.setInstanceForTest(new ExportManager() {
//           @Override
//           public void queueMessage(ExportInternalMessage mbp) {
//               ref.set(mbp);
//           }
//        });
//
//        ByteBuffer firstBuffer = ByteBuffer.allocate(4 + MAGIC_TUPLE_SIZE * 9);
//        s.pushExportBuffer(0, 0, firstBuffer);
//
//        ByteBuffer secondBuffer = ByteBuffer.allocate(4 + MAGIC_TUPLE_SIZE * 10);
//        s.pushExportBuffer(MAGIC_TUPLE_SIZE * 9, 0, secondBuffer);
//
//        // release the first buffer
//        m.ack(MAGIC_TUPLE_SIZE * 9);
//        s.exportAction(pair);
//
//        ExportInternalMessage mbp = ref.get();
//        bool released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 9));
//        EXPECT_TRUE(released);
//
//
//        // release the second buffer
//        released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 19));
//        EXPECT_TRUE(released);
//
//        // now get the current state
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 19));
//        EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 19));
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedSize(), 0);
//    }
//
//    /**
//     * Test that releasing multiple blocks at once and then polling results in
//     * the right StreamBlock
//     */
//    TEST_F(TupleStreamWrapperTest, ReleaseAllAtOnce)
//    {
//        // we get nothing with no data
//        StreamBlock* results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), 0);
//        EXPECT_EQ(results->unreleasedUso(), 0);
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedSize(), 0);
//
//        for (int i = 1; i < 10; i++)
//        {
//            appendTuple(i-1, i);
//        }
//        m_wrapper->periodicFlush(-1, 0, 9, 9);
//
//        for (int i = 10; i < 20; i++)
//        {
//            appendTuple(i-1, i);
//        }
//        m_wrapper->periodicFlush(-1, 0, 19, 19);
//
//        // release everything
//        bool released;
//        released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 19));
//        EXPECT_TRUE(released);
//
//        // now get the current state
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 19));
//        EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 19));
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedSize(), 0);
//    }
//
//    /**
//     * Test that releasing bytes earlier than recorded history just succeeds
//     */
//    TEST_F(TupleStreamWrapperTest, ReleasePreHistory)
//    {
//        // we get nothing with no data
//        StreamBlock* results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), 0);
//        EXPECT_EQ(results->unreleasedUso(), 0);
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedSize(), 0);
//
//        for (int i = 1; i < 10; i++)
//        {
//            appendTuple(i-1, i);
//        }
//        m_wrapper->periodicFlush(-1, 0, 9, 9);
//
//        for (int i = 10; i < 20; i++)
//        {
//            appendTuple(i-1, i);
//        }
//        m_wrapper->periodicFlush(-1, 0, 19, 19);
//
//        // release everything
//        bool released;
//        released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 19));
//        EXPECT_TRUE(released);
//
//        // now release something early in what just got released
//        released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 4));
//        EXPECT_TRUE(released);
//
//        // now get the current state
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 19));
//        EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 19));
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedSize(), 0);
//    }
//
//    /**
//     * Test that releasing at a point in the current stream block
//     * works correctly
//     */
//    TEST_F(TupleStreamWrapperTest, ReleaseInCurrentBlock)
//    {
//        // we get nothing with no data
//        StreamBlock* results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), 0);
//        EXPECT_EQ(results->unreleasedUso(), 0);
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedSize(), 0);
//
//        // Fill the current buffer with some stuff
//        for (int i = 1; i < 10; i++)
//        {
//            appendTuple(i-1, i);
//        }
//
//        // release part of the way into the current buffer
//        bool released;
//        released = m_wrapper->releaseExportBytes((MAGIC_TUPLE_SIZE * 4));
//        EXPECT_TRUE(released);
//
//        // Poll and verify that we get a StreamBlock that indicates that
//        // there's no data available at the new release point
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 4));
//        EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 4));
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedSize(), 0);
//
//        // Now, flush the buffer and then verify that the next poll gets
//        // the right partial result
//        m_wrapper->periodicFlush(-1, 0, 9, 9);
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), 0);
//        EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 4));
//        EXPECT_EQ(results->offset(), (MAGIC_TUPLE_SIZE * 9));
//        EXPECT_EQ(results->unreleasedSize(), (MAGIC_TUPLE_SIZE * 5));
//    }
//
//    /**
//     * Test that reset allows re-polling data
//     */
//    TEST_F(TupleStreamWrapperTest, ResetInFirstBlock)
//    {
//        // Fill the current buffer with some stuff
//        for (int i = 1; i < 10; i++)
//        {
//            appendTuple(i-1, i);
//        }
//
//        // Flush all data
//        m_wrapper->periodicFlush(-1, 0, 10, 10);
//
//        // Poll and verify that data is returned
//        StreamBlock* results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), 0);
//        EXPECT_EQ(results->unreleasedUso(), 0);
//        EXPECT_EQ(results->offset(), MAGIC_TUPLE_SIZE * 9);
//        EXPECT_EQ(results->unreleasedSize(), MAGIC_TUPLE_SIZE * 9);
//
//        // Poll again and see that an empty block is returned
//        // (Not enough data to require more than one block)
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), MAGIC_TUPLE_SIZE * 9);
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedUso(), results->uso());
//
//        // Reset the stream and get the first poll again
//        m_wrapper->resetPollMarker();
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), 0);
//        EXPECT_EQ(results->unreleasedUso(), 0);
//        EXPECT_EQ(results->offset(), MAGIC_TUPLE_SIZE * 9);
//        EXPECT_EQ(results->unreleasedSize(), MAGIC_TUPLE_SIZE * 9);
//    }
//
//    TEST_F(TupleStreamWrapperTest, ResetInPartiallyAckedBlock)
//    {
//        // Fill the current buffer with some stuff
//        for (int i = 1; i < 10; i++) {
//            appendTuple(i-1, i);
//        }
//
//        // Ack the first 4 tuples.
//        bool released = m_wrapper->releaseExportBytes(MAGIC_TUPLE_SIZE * 4);
//        EXPECT_TRUE(released);
//
//        // Poll and verify that we get a StreamBlock that indicates that
//        // there's no data available at the new release point
//        // (because the full block is not committed)
//        StreamBlock* results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 4));
//        EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 4));
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedSize(), 0);
//
//        // reset the poll point; this should not change anything.
//        m_wrapper->resetPollMarker();
//
//        // Same verification as above.
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), (MAGIC_TUPLE_SIZE * 4));
//        EXPECT_EQ(results->unreleasedUso(), (MAGIC_TUPLE_SIZE * 4));
//        EXPECT_EQ(results->offset(), 0);
//        EXPECT_EQ(results->unreleasedSize(), 0);
//    }
//
//    TEST_F(TupleStreamWrapperTest, ResetInPartiallyAckedCommittedBlock)
//    {
//        // write some, committing as tuples are added
//        int i = 0;  // keep track of the current txnid
//        for (i = 1; i < 10; i++) {
//            appendTuple(i-1, i);
//        }
//
//        // partially ack the buffer
//        bool released = m_wrapper->releaseExportBytes(MAGIC_TUPLE_SIZE * 4);
//        EXPECT_TRUE(released);
//
//        // wrap and require a new buffer
//        int tuples_to_fill = BUFFER_SIZE / MAGIC_TUPLE_SIZE + 10;
//        for (int j = 0; j < tuples_to_fill; j++, i++) {
//            appendTuple(i, i+1);
//        }
//
//        // poll - should get the content post release (in the old buffer)
//        StreamBlock* results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), 0);
//        EXPECT_EQ(results->unreleasedUso(), MAGIC_TUPLE_SIZE * 4);
//        EXPECT_TRUE(results->offset() > 0);
//
//        // poll again.
//         m_wrapper->getCommittedExportBytes();
//
//        // reset. Aftwards, should be able to get original block back
//        m_wrapper->resetPollMarker();
//
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), 0);
//        EXPECT_EQ(results->unreleasedUso(), MAGIC_TUPLE_SIZE * 4);
//        EXPECT_TRUE(results->offset() > 0);
//
//        // flush should also not change the reset base poll point
//        m_wrapper->periodicFlush(-1, 0, i, i);
//        m_wrapper->resetPollMarker();
//
//        results = m_wrapper->getCommittedExportBytes();
//        EXPECT_EQ(results->uso(), 0);
//        EXPECT_EQ(results->unreleasedUso(), MAGIC_TUPLE_SIZE * 4);
//        EXPECT_TRUE(results->offset() > 0);
//    }
}
