/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.voltdb.export.ExportMatchers.ackPayloadIs;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.MockVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.export.ExportDataSource.AckingContainer;
import org.voltdb.utils.VoltFile;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import junit.framework.TestCase;

public class TestExportDataSource extends TestCase {

    static {
        org.voltdb.EELibraryLoader.loadExecutionEngineLibrary(true);
    }
    private final static File TEST_DIR = new File("/tmp/" + System.getProperty("user.name"));

    MockVoltDB m_mockVoltDB;
    int m_host = 0;
    int m_site = 1;
    int m_part = 2;
    TestGeneration m_generation;

    private static TreeSet<String> getSortedDirectoryListingSegments() {
        TreeSet<String> names = new TreeSet<String>();
        for (File f : TEST_DIR.listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                if (pathname.getName().endsWith(".pbd")) {
                    return true;
                }
                return false;
            }
        })) {
            names.add(f.getName());
        }
        return names;
    }
    class TestGeneration implements Generation {

        @Override
        public void acceptMastership(int partitionId) {
        }

        @Override
        public void close(HostMessenger messenger) {
        }

        @Override
        public long getQueuedExportBytes(int partitionId, String signature) {
            return 0;
        }

        @Override
        public void onSourceDone(int partitionId, String signature) {
        }

        @Override
        public void pushExportBuffer(int partitionId, String signature, long uso, ByteBuffer buffer, boolean sync) {
        }

        @Override
        public void pushEndOfStream(int partitionId, String signature) {
        }

        @Override
        public void truncateExportToTxnId(long snapshotTxnId, long[] perPartitionTxnIds) {
        }

        @Override
        public Map<Integer, Map<String, ExportDataSource>> getDataSourceByPartition() {
            throw new UnsupportedOperationException("Not supported yet.");
        }
    }

    @Override
    public void setUp() throws IOException {
        m_mockVoltDB = new MockVoltDB();
        m_mockVoltDB.addSite(CoreUtils.getHSIdFromHostAndSite(m_host, m_site), m_part);
        m_mockVoltDB.addTable("TableName", false);
        m_mockVoltDB.addColumnToTable("TableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("TableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);
        m_mockVoltDB.addTable("RepTableName", false);
        m_mockVoltDB.addColumnToTable("RepTableName", "COL1", VoltType.INTEGER, false, null, VoltType.INTEGER);
        m_mockVoltDB.addColumnToTable("RepTableName", "COL2", VoltType.STRING, false, null, VoltType.STRING);

        if (TEST_DIR.exists()) {
            for (File f : TEST_DIR.listFiles()) {
                VoltFile.recursivelyDelete(f);
            }
            TEST_DIR.delete();
        }
        TEST_DIR.mkdir();
        m_generation = new TestGeneration();
    }

    @Override
    public void tearDown() throws Exception {
        m_mockVoltDB.shutdown(null);
        System.gc();
        System.runFinalization();
        Thread.sleep(200);
    }

    public void testExportDataSource() throws Exception {
        System.out.println("Running testExportDataSource");
        String[] tables = {"TableName", "RepTableName"};
        for (String table_name : tables) {
            Table table = m_mockVoltDB.getCatalogContext().database.getTables().get(table_name);
            ExportDataSource s = new ExportDataSource(null, "database",
                    table.getTypeName(),
                    m_part,
                    table.getSignature(),
                    table.getColumns(),
                    table.getPartitioncolumn(),
                    TEST_DIR.getAbsolutePath());
            try {
                assertEquals("database", s.getDatabase());
                assertEquals(table_name, s.getTableName());
                assertEquals(m_part, s.getPartitionId());
                assertEquals(table.getSignature(), s.getSignature());
            } finally {
                s.close();
            }
        }
    }

    public void testPollV2() throws Exception{
        System.out.println("Running testPollV2");
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new ExportDataSource(null, "database",
                table.getTypeName(),
                m_part,
                table.getSignature(),
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
            final CountDownLatch cdl = new CountDownLatch(1);
            Runnable cdlWaiter = new Runnable() {

                @Override
                public void run() {
                    cdl.countDown();
                }
            };
            s.setOnMastership(cdlWaiter, false);
            s.acceptMastership();
            cdl.await();

            ByteBuffer foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
            foo.duplicate().put(new byte[28]);
            s.pushExportBuffer(23, foo, false);
            assertEquals(s.sizeInBytes(), 20 );

            //Push it twice more to check stats calc
            foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
            foo.duplicate().put(new byte[28]);
            s.pushExportBuffer(43, foo, false);
            assertEquals(s.sizeInBytes(), 40);
            foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
            foo.duplicate().put(new byte[28]);
            s.pushExportBuffer(63, foo, false);

            assertEquals(s.sizeInBytes(), 60);

            //Sync which flattens them all, but then pulls the first two back in memory
            //resulting in no change
            s.pushExportBuffer(63, null, true);

            assertEquals( 60, s.sizeInBytes());

            AckingContainer cont = (AckingContainer)s.poll().get();
            //No change in size because the buffers are flattened to disk, until the whole
            //file is polled/acked it won't shrink
            assertEquals( 60, s.sizeInBytes());
            assertEquals( 42, cont.m_uso);

            foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
            foo.duplicate().put(new byte[28]);
            foo.position(StreamBlock.HEADER_SIZE);
            foo = foo.slice();
            foo.order(ByteOrder.LITTLE_ENDIAN);
            assertTrue( foo.equals(cont.b()) );

            cont.discard();
            cont = null;
            System.gc(); System.runFinalization(); Thread.sleep(200);
            cont = (AckingContainer)s.poll().get();

            //Should lose 20 bytes for the stuff in memory
            assertEquals( 40, s.sizeInBytes());

            assertEquals( 62, cont.m_uso);
            assertTrue( foo.equals(cont.b()));

            cont.discard();
            cont = null;
            System.gc(); System.runFinalization(); Thread.sleep(200);
            cont = (AckingContainer)s.poll().get();

            //No more buffers on disk, so the + 8 is gone, just the last one pulled in memory
            assertEquals( 20, s.sizeInBytes());
            assertEquals( 82, cont.m_uso);
            assertEquals( foo, cont.b());

            cont.discard();
            cont = null;
            System.gc(); System.runFinalization(); Thread.sleep(200);
            ListenableFuture<BBContainer> fut = s.poll();
            try {
                cont = (AckingContainer)fut.get(100,TimeUnit.MILLISECONDS);
                fail("did not get expected timeout");
            }
            catch( TimeoutException ignoreIt) {}
//            s.pushExportBuffer(83, null, true);
//            assertNull(fut.get());
        } finally {
            s.close();
        }
    }

    public void testReplicatedPoll() throws Exception {
        System.out.println("Running testReplicatedPoll");
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new ExportDataSource(null, "database",
                table.getTypeName(),
                m_part,
                table.getSignature(),
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
        final CountDownLatch cdl = new CountDownLatch(1);
        Runnable cdlWaiter = new Runnable() {
            @Override
            public void run() {
                cdl.countDown();
            }
        };
        Mailbox mockedMbox = Mockito.mock(Mailbox.class);
        final AtomicReference<CountDownLatch> refSendCdl = new AtomicReference<CountDownLatch>(new CountDownLatch(1));
        doAnswer(new Answer<Void>() {
            @Override
            public Void answer(InvocationOnMock invocation) throws Throwable {
                refSendCdl.get().countDown();
                return null;
            }
        }).when(mockedMbox).send(eq(42L), any(BinaryPayloadMessage.class));

        s.updateAckMailboxes(Pair.<Mailbox,ImmutableList<Long>>of(mockedMbox, ImmutableList.<Long>of(42L)));

        s.setOnMastership(cdlWaiter, false);
        s.acceptMastership();
        cdl.await();

        ByteBuffer foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
        foo.duplicate().put(new byte[20]);
        // we are not purposely starting at 0, because on rejoin
        // we may start at non zero offsets
        s.pushExportBuffer(23, foo, false);
        assertEquals(s.sizeInBytes(), 20 );

        //Push it twice more to check stats calc
        foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
        foo.duplicate().put(new byte[20]);
        s.pushExportBuffer(43, foo, false);
        assertEquals(s.sizeInBytes(), 40 );
        foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
        foo.duplicate().put(new byte[20]);
        s.pushExportBuffer(63, foo, false);

        assertEquals(s.sizeInBytes(), 60);

        //Sync which flattens them all
        s.pushExportBuffer(63, null, true);

        //flattened size
        assertEquals( 60, s.sizeInBytes());

        AckingContainer cont = (AckingContainer)s.poll().get();
        //No change in size because the buffers are flattened to disk, until the whole
        //file is polled/acked it won't shrink
        assertEquals( 60, s.sizeInBytes());
        assertEquals( 42, cont.m_uso);

        foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
        foo.duplicate().put(new byte[20]);
        foo.order(ByteOrder.LITTLE_ENDIAN);
        foo.position(StreamBlock.HEADER_SIZE);
        foo = foo.slice();
        assertTrue( foo.equals(cont.b()));

        cont.discard();

        assertTrue("timeout while wating for ack to be sent",refSendCdl.get().await(5,TimeUnit.SECONDS));

        verify(mockedMbox, times(1)).send(
                eq(42L),
                argThat(ackPayloadIs(m_part, table.getSignature(), 42))
                );

        // Poll and discard buffer 63, too
        s.poll().get().discard();

        int i = 1000;
        while( i > 0 && s.sizeInBytes() > 32) {
            --i; Thread.sleep(2);
        }
        // 20, no overhead because it was pulled back in
        assertEquals( 20, s.sizeInBytes());

        cont = (AckingContainer)s.poll().get();
        assertEquals(s.sizeInBytes(), 20);
        assertEquals(82, cont.m_uso);
        cont.discard();

        } finally {
            s.close();
        }
    }

    public void testReleaseExportBytes() throws Exception {
        System.out.println("Running testReleaseExportBytes");
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new ExportDataSource(null, "database",
                table.getTypeName(),
                m_part,
                table.getSignature(),
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
            //Ack before push
            s.ack(100);
            TreeSet<String> listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            //Push and sync
            ByteBuffer foo = ByteBuffer.allocateDirect(200 + StreamBlock.HEADER_SIZE);
            foo.duplicate().put(new byte[200]);
            s.pushExportBuffer(203, foo, true);
            long sz = s.sizeInBytes();
            assertEquals(sz, 200);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            //Ack after push beyond size...last segment kept.
            s.ack(1000);
            sz = s.sizeInBytes();
            assertEquals(sz, 0);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            //Push again and sync to test files.
            ByteBuffer foo2 = ByteBuffer.allocateDirect(900 + StreamBlock.HEADER_SIZE);
            foo2.duplicate().put(new byte[900]);
            s.pushExportBuffer(903, foo2, true);
            sz = s.sizeInBytes();
            assertEquals(sz, 802);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            //Low ack should have no effect.
            s.ack(100);
            sz = s.sizeInBytes();
            assertEquals(sz, 802);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            final CountDownLatch cdl = new CountDownLatch(1);
            Runnable cdlWaiter = new Runnable() {
                @Override
                public void run() {
                    cdl.countDown();
                }
            };
            s.setOnMastership(cdlWaiter, false);
            s.acceptMastership();
            cdl.await();

            //Poll and check before and after discard segment files.
            AckingContainer cont = (AckingContainer) s.poll().get();
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);
            cont.discard();
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            s.unacceptMastership();

            //Last segment is always kept.
            s.ack(2000);
            sz = s.sizeInBytes();
            assertEquals(sz, 0);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

        } finally {
            s.close();
        }
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
