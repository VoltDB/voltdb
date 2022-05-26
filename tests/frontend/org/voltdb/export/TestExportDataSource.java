/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;
import static org.voltdb.export.ExportMatchers.ackPayloadIs;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.voltcore.messaging.BinaryPayloadMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.ExportStatsBase.ExportStatsRow;
import org.voltdb.MockVoltDB;
import org.voltdb.SnapshotCompletionMonitor.ExportSnapshotTuple;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.export.ExportDataSource.ReentrantPollException;
import org.voltdb.export.ExportDataSource.StreamStartAction;
import org.voltdb.export.processors.GuestProcessor;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import org.apache.commons.io.FileUtils;

import junit.framework.TestCase;

public class TestExportDataSource extends TestCase {

    static {
        org.voltdb.NativeLibraryLoader.loadVoltDB();
    }
    private final static File TEST_DIR = new File("/tmp/" + System.getProperty("user.name"));

    MockVoltDB m_mockVoltDB;
    int m_host = 0;
    int m_site = 1;
    int m_part = 2;
    TestGeneration m_generation;
    ExportDataProcessor m_processor;

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
        public void becomeLeader(int partitionId) {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public List<ExportStatsRow> getStats(boolean interval) {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public void onSourceDrained(int partitionId, String tableName) {

        }

        @Override
        public void pushExportBuffer(int partitionId, String signature,
                long seqNo, long committedSeqNo, int tupleCount,
                long uniqueId, BBContainer cont) {
        }

        @Override
        public void updateInitialExportStateToSeqNo(int partitionId,
                String signature, StreamStartAction action,
                Map<Integer, ExportSnapshotTuple> sequenceNumberPerPartition) {
        }

        @Override
        public void updateDanglingExportStates(StreamStartAction action,
                Map<String, Map<Integer, ExportSnapshotTuple>> exportSequenceNumbers) {

        }

        @Override
        public void sync() {
            throw new UnsupportedOperationException("Not supported yet.");
        }

        @Override
        public int getCatalogVersion() {
            return 0;
        }

        @Override
        public void updateGenerationId(long genId) {
        }

        @Override
        public Map<Integer, Map<String, ExportDataSource>> getDataSourceByPartition() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    // Define a class derived from {@code ExportDataSource} allowing mocking
    // the {@code ExportCoordinator}
    class MockExportDataSource extends ExportDataSource {

        public MockExportDataSource(Generation generation, ExportDataProcessor processor, String db, String tableName,
                int partitionId, int siteId, long genId, CatalogMap<Column> catalogMap, Column partitionColumn,
                String overflowPath) throws IOException {
            super(generation, processor, tableName, partitionId, siteId, genId, catalogMap, partitionColumn,
                    overflowPath);
            setMockCoordination();
        }

        void setMockCoordination() {
            m_coordinator = mock(ExportCoordinator.class);
            when(m_coordinator.isCoordinatorInitialized()).thenReturn(true);
            when(m_coordinator.isPartitionLeader()).thenReturn(true);
            when(m_coordinator.isMaster()).thenReturn(true);
            when(m_coordinator.isSafePoint(anyLong())).thenReturn(true);
            when(m_coordinator.isExportMaster(anyLong())).thenReturn(true);
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
        VoltDB.replaceVoltDBInstanceForTest(m_mockVoltDB);
        FileUtils.deleteDirectory(TEST_DIR);
        TEST_DIR.mkdir();
        m_generation = new TestGeneration();
        m_processor = getProcessor();
    }

    @Override
    public void tearDown() throws Exception {
        m_mockVoltDB.shutdown(null);
        m_generation = null;
        m_processor = null;
        System.gc();
        System.runFinalization();
        Thread.sleep(200);
    }

    private ExportDataProcessor getProcessor() {
        Map<String, Pair<Properties, Set<String>>> config = new HashMap<>();
        Properties props = new Properties();
        props.put("nonce", "mynonce");
        props.put("type", "csv");
        props.put("__EXPORT_TO_TYPE__", "org.voltdb.exportclient.ExportToFileClient");
        props.put("replicated", false);
        props.put("outdir", TEST_DIR + "/my_exports");
        Set<String> tables = new HashSet<>();
        tables.add("TableName");
        tables.add("RepTableName");
        config.put("LOG", new Pair<>(props, tables));
        ExportDataProcessor processor = new GuestProcessor();
        processor.setProcessorConfig(config);
        return processor;
    }

    private void waitForMaster(ExportDataSource s) throws InterruptedException {
        int i = 0;
        while (!s.isMaster()) {
            assertFalse(++i > 2);
            Thread.sleep(500);
        }
    }

    private BBContainer getBuffer(int buffSize) {
        ByteBuffer foo = ByteBuffer.allocateDirect(buffSize);
        foo.duplicate().put(new byte[buffSize]);
        return DBBPool.wrapBB(foo);
    }

    public void testExportDataSource() throws Exception {
        System.out.println("Running testExportDataSource");
        String[] tables = {"TableName", "RepTableName"};
        for (String table_name : tables) {
            Table table = m_mockVoltDB.getCatalogContext().database.getTables().get(table_name);
            ExportDataSource s = new MockExportDataSource(null, m_processor, "database",
                    table.getTypeName(),
                    m_part,
                    CoreUtils.getSiteIdFromHSId(m_site),
                    0,
                    table.getColumns(),
                    table.getPartitioncolumn(),
                    TEST_DIR.getAbsolutePath());
            try {
                assertEquals(table_name, s.getTableName());
                assertEquals(m_part, s.getPartitionId());
            } finally {
                s.shutdown();
            }
        }
    }

    public void testPollV2() throws Exception{
        System.out.println("Running testPollV2");
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new MockExportDataSource(null, m_processor, "database",
                table.getTypeName(),
                m_part,
                CoreUtils.getSiteIdFromHSId(m_site),
                0,
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
            s.setReadyForPolling(true);
            s.becomeLeader();
            waitForMaster(s);

            int buffSize = 20 + StreamBlock.HEADER_SIZE;
            s.pushExportBuffer(1, 1, 1, 0, getBuffer(buffSize));
            assertEquals(s.sizeInBytes(), 20 );

            //Push it twice more to check stats calc
            s.pushExportBuffer(2, 2, 1, 0, getBuffer(buffSize));
            assertEquals(s.sizeInBytes(), 40);
            s.pushExportBuffer(3, 3, 1, 0, getBuffer(buffSize));

            assertEquals(s.sizeInBytes(), 60);

            //Sync which flattens them all, but then pulls the first two back in memory
            //resulting in no change
            s.pushExportBuffer(3, -1L, 1, 0, null);

            assertEquals( 60, s.sizeInBytes());

            AckingContainer cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            //No change in size because the buffers are flattened to disk, until the whole
            //file is polled/acked it won't shrink
            assertEquals( 60, s.sizeInBytes());
            assertEquals( 1, cont.m_lastSeqNo);

            ByteBuffer foo = ByteBuffer.allocateDirect(buffSize);
            foo.duplicate().put(new byte[buffSize]);
            foo.position(StreamBlock.HEADER_SIZE);
            foo = foo.slice();
            foo.order(ByteOrder.LITTLE_ENDIAN);
            assertTrue( foo.equals(cont.b()) );

            cont.discard();
            cont = null;
            System.gc(); System.runFinalization(); Thread.sleep(200);
            cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());

            //Should lose 20 bytes for the stuff in memory
            assertEquals( 40, s.sizeInBytes());

            assertEquals( 2, cont.m_lastSeqNo);
            assertTrue( foo.equals(cont.b()));

            cont.discard();
            cont = null;
            System.gc(); System.runFinalization(); Thread.sleep(200);
            cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());

            //No more buffers on disk, so the + 8 is gone, just the last one pulled in memory
            assertEquals( 20, s.sizeInBytes());
            assertEquals( 3, cont.m_lastSeqNo);
            assertEquals( foo, cont.b());

            cont.discard();
            cont = null;
            System.gc(); System.runFinalization(); Thread.sleep(200);
            ListenableFuture<AckingContainer> fut = s.poll();
            try {
                cont = fut.get(100,TimeUnit.MILLISECONDS);
                fail("did not get expected timeout");
            }
            catch( TimeoutException ignoreIt) {}
        } finally {
            s.shutdown();
        }
    }

    public void testDoublePoll() throws Exception{
        System.out.println("Running testDoublePoll");
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new MockExportDataSource(null, m_processor, "database",
                table.getTypeName(),
                m_part,
                CoreUtils.getSiteIdFromHSId(m_site),
                0,
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
            s.setReadyForPolling(true);
            s.becomeLeader();
            waitForMaster(s);

            // Set ready for polling to enable satisfying fut on push
            s.setReadyForPolling(true);

            int buffSize = 20 + StreamBlock.HEADER_SIZE;

            // Push a first buffer - and read it back
            s.pushExportBuffer(1, 1, 1, 0, getBuffer(buffSize));

            AckingContainer cont0 = s.poll().get();
            cont0.updateStartTime(System.currentTimeMillis());

            cont0.discard();
            cont0 = null;
            System.gc(); System.runFinalization(); Thread.sleep(200);

            // Poll once with no data left - EDS sould set m_pollFuture
            ListenableFuture<AckingContainer> fut1 = s.poll();
            assertFalse(fut1.isDone());

            // Do a reentrant poll - the returned fut should have the expected exception
            ListenableFuture<AckingContainer> fut2 = s.poll();
            try {
                fut2.get();
                fail("Did not get expected exception");
            }
            catch (Exception e) {
                if (e.getCause() instanceof ReentrantPollException) {
                    System.out.println("Got expected exception: " + e);

                } else {
                    fail("Got unexpected exception: " + e);
                }
            }

            // Push a buffer - should satisfy fut1
            s.pushExportBuffer(2, 2, 1, 0, getBuffer(buffSize));

            // Verify the pushed buffer can be got
            AckingContainer cont1 = fut1.get();
            cont1.updateStartTime(System.currentTimeMillis());

            cont1.discard();
            cont1 = null;
            System.gc(); System.runFinalization(); Thread.sleep(200);

        } finally {
            s.shutdown();
        }
    }

    public void testOutOfOrderDiscards() throws Exception{
        System.out.println("Running testOutOfOrderDiscards");
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new MockExportDataSource(null, m_processor, "database",
                table.getTypeName(),
                m_part,
                CoreUtils.getSiteIdFromHSId(m_site),
                0,
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
            s.setReadyForPolling(true);
            s.becomeLeader();
            waitForMaster(s);

            // Set ready for polling to enable satisfying fut on push
            s.setReadyForPolling(true);

            int buffSize = 20 + StreamBlock.HEADER_SIZE;
            s.pushExportBuffer(1, 1, 1, 0, getBuffer(buffSize));

            AckingContainer cont0 = s.poll().get();
            cont0.updateStartTime(System.currentTimeMillis());

            // Push a buffer - should satisfy fut1
            s.pushExportBuffer(2, 2, 1, 0, getBuffer(buffSize));

            // Verify the pushed buffer can be got
            AckingContainer cont1 = s.poll().get();
            cont1.updateStartTime(System.currentTimeMillis());

            // Discard out of order
            cont1.discard();
            cont0.discard();

        } finally {
            s.shutdown();
        }
    }

    public void testOutOfOrderAck() throws Exception{
        System.out.println("Running testOutOfOrderAck");
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new MockExportDataSource(null, m_processor, "database",
                table.getTypeName(),
                m_part,
                CoreUtils.getSiteIdFromHSId(m_site),
                0,
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
            s.setReadyForPolling(true);
            s.becomeLeader();
            waitForMaster(s);

            // Set ready for polling to enable satisfying fut on push
            s.setReadyForPolling(true);

            int buffSize = 20 + StreamBlock.HEADER_SIZE;

            // Push 4 buffers with seqNo [1 .. 4]
            s.pushExportBuffer(1, 1, 1, 0, getBuffer(buffSize));
            s.pushExportBuffer(2, 2, 1, 0, getBuffer(buffSize));
            s.pushExportBuffer(3, 3, 1, 0, getBuffer(buffSize));
            s.pushExportBuffer(4, 4, 1, 0, getBuffer(buffSize));

            // Poll the first buffer: creates a read-only copy and
            // increments refcount on StreamBlock
            AckingContainer cont1 = s.poll().get();
            cont1.updateStartTime(System.currentTimeMillis());

            // Simulate a remote ack of the second buffer
            // The log file should show:
            // INFO  [main] EXPORT: Unable to release buffers to seqNo: 2, buffer [1, 1] is still in use
            s.localAck(2, 2);

            // Verify we can poll past the ack point
            AckingContainer cont3 = s.poll().get();
            cont3.updateStartTime(System.currentTimeMillis());
            assertEquals(3, cont3.m_lastSeqNo);

            // Discard first polled buffers
            cont1.discard();
            cont3.discard();

            // Verify we can poll the last buffer
            AckingContainer cont4 = s.poll().get();
            cont4.updateStartTime(System.currentTimeMillis());
            assertEquals(4, cont4.m_lastSeqNo);

            cont4.discard();

        } finally {
            s.shutdown();
        }
    }

    public void testReplicatedPoll() throws Exception {
        System.out.println("Running testReplicatedPoll");
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new MockExportDataSource(null, m_processor, "database",
                table.getTypeName(),
                m_part,
                CoreUtils.getSiteIdFromHSId(m_site),
                0,
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
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

        s.setReadyForPolling(true);
        s.becomeLeader();
        waitForMaster(s);

        ByteBuffer foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
        foo.duplicate().put(new byte[20]);
        // we are not purposely starting at 0, because on rejoin
        // we may start at non zero offsets
        s.pushExportBuffer(1, 1, 1, 0, DBBPool.wrapBB(foo));
        assertEquals(s.sizeInBytes(), 20 );

        //Push it twice more to check stats calc
        foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
        foo.duplicate().put(new byte[20]);
        s.pushExportBuffer(2, 2, 1, 0, DBBPool.wrapBB(foo));
        assertEquals(s.sizeInBytes(), 40 );
        foo = ByteBuffer.allocateDirect(20 + StreamBlock.HEADER_SIZE);
        foo.duplicate().put(new byte[20]);
        s.pushExportBuffer(3, 3, 1, 0, DBBPool.wrapBB(foo));

        assertEquals(s.sizeInBytes(), 60);

        //Sync which flattens them all
        s.pushExportBuffer(3, -1L, 1, 0, null);

        //flattened size
        assertEquals( 60, s.sizeInBytes());

        AckingContainer cont = s.poll().get();
        cont.updateStartTime(System.currentTimeMillis());
        //No change in size because the buffers are flattened to disk, until the whole
        //file is polled/acked it won't shrink
        assertEquals( 60, s.sizeInBytes());
        assertEquals( 1, cont.m_lastSeqNo);

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
                argThat(ackPayloadIs(m_part, table.getSignature(), 1, s.getGenerationIdCreated()))
                );

        // Poll and discard buffer 2, too
        cont = s.poll().get();
        cont.updateStartTime(System.currentTimeMillis());
        cont.discard();

        int i = 1000;
        while( i > 0 && s.sizeInBytes() > 32) {
            --i; Thread.sleep(2);
        }
        // 20, no overhead because it was pulled back in
        assertEquals( 20, s.sizeInBytes());

        cont = s.poll().get();
        cont.updateStartTime(System.currentTimeMillis());
        assertEquals(s.sizeInBytes(), 20);
        assertEquals(3, cont.m_lastSeqNo);
        cont.discard();

        } finally {
            s.shutdown();
        }
    }

    public void testReleaseExportBytes() throws Exception {
        System.out.println("Running testReleaseExportBytes");
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new MockExportDataSource(null, m_processor, "database",
                table.getTypeName(),
                m_part,
                CoreUtils.getSiteIdFromHSId(m_site),
                0,
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
            s.setReadyForPolling(true);

            //Ack before push
            s.remoteAck(100);
            TreeSet<String> listing = getSortedDirectoryListingSegments();
            assertEquals(0, listing.size());

            //Push and sync
            ByteBuffer foo = ByteBuffer.allocateDirect(200 + StreamBlock.HEADER_SIZE);
            foo.duplicate().put(new byte[200]);
            s.pushExportBuffer(101, 101, 1, 0, DBBPool.wrapBB(foo));
            long sz = s.sizeInBytes();
            assertEquals(200, sz);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            //Ack after push beyond size...last segment kept.
            s.remoteAck(110);
            sz = s.sizeInBytes();
            assertEquals(0, sz);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            //Push again and sync to test files.
            ByteBuffer foo2 = ByteBuffer.allocateDirect(900 + StreamBlock.HEADER_SIZE);
            foo2.duplicate().put(new byte[900]);
            s.pushExportBuffer(111, 111, 1, 0, DBBPool.wrapBB(foo2));
            sz = s.sizeInBytes();
            assertEquals(900, sz);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            //Low ack should have no effect.
            s.remoteAck(100);
            sz = s.sizeInBytes();
            assertEquals(900, sz);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            s.becomeLeader();
            waitForMaster(s);

            //Poll and check before and after discard segment files.
            AckingContainer cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);
            cont.discard();
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

            //Last segment is always kept.
            s.remoteAck(2000);
            sz = s.sizeInBytes();
            assertEquals(sz, 0);
            listing = getSortedDirectoryListingSegments();
            assertEquals(listing.size(), 1);

        } finally {
            s.shutdown();
        }
    }

    // Test that gap release operates even when no further export rows are inserted
    public void testGapRelease() throws Exception{
        System.out.println("Running testGapRelease");
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new MockExportDataSource(null, m_processor, "database",
                table.getTypeName(),
                m_part,
                CoreUtils.getSiteIdFromHSId(m_site),
                0,
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
            s.setReadyForPolling(true);
            s.becomeLeader();
            waitForMaster(s);

            // Push 2 buffers with contiguous sequence numbers
            int buffSize = 20 + StreamBlock.HEADER_SIZE;
            s.pushExportBuffer(1, 1, 1, 0, getBuffer(buffSize));
            s.pushExportBuffer(2, 2, 1, 0, getBuffer(buffSize));

            // Push 2 more creating a gap
            s.pushExportBuffer(10, 10, 1, 0, getBuffer(buffSize));
            s.pushExportBuffer(11, 11, 1, 0, getBuffer(buffSize));

            // Poll the 2 first buffers
            AckingContainer cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            assertEquals( 1, cont.m_lastSeqNo);
            cont.discard();

            cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            assertEquals( 2, cont.m_lastSeqNo);
            cont.discard();

            // Poll once more, should hit the gap (wait for the state to be changed)
            ListenableFuture<AckingContainer> fut1 = s.poll();

            Thread.sleep(1000);
            assertFalse(fut1.isDone());
            assertEquals(ExportDataSource.StreamStatus.BLOCKED, s.getStatus());

            // Do a release
            assertTrue(s.processStreamControl(StreamControlOperation.RELEASE));

            // Verify we can poll the 2 buffers past the gap,
            // but wait for long enough to allow the runnable to get
            // past the gap.
            try {
                cont = fut1.get(100, TimeUnit.MILLISECONDS);
            }
            catch( TimeoutException to) {
                fail("did not expect timeout");
            }
            cont.updateStartTime(System.currentTimeMillis());
            assertEquals(10, cont.m_lastSeqNo);
            cont.discard();

            cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            assertEquals(11, cont.m_lastSeqNo);
            cont.discard();

        } finally {
            s.shutdown();
        }
    }

    public void testPendingContainer() throws Exception{
        System.out.println("Running testPendingContainer");
        Table table = m_mockVoltDB.getCatalogContext().database.getTables().get("TableName");
        ExportDataSource s = new MockExportDataSource(null, m_processor, "database",
                table.getTypeName(),
                m_part,
                CoreUtils.getSiteIdFromHSId(m_site),
                0,
                table.getColumns(),
                table.getPartitioncolumn(),
                TEST_DIR.getAbsolutePath());
        try {
            s.setReadyForPolling(true);
            s.becomeLeader();
            waitForMaster(s);

            // Push 4 buffers with contiguous sequence numbers
            int buffSize = 20 + StreamBlock.HEADER_SIZE;
            s.pushExportBuffer(1, 1, 1, 0, getBuffer(buffSize));
            s.pushExportBuffer(2, 2, 1, 0, getBuffer(buffSize));
            s.pushExportBuffer(3, 3, 1, 0, getBuffer(buffSize));
            s.pushExportBuffer(4, 4, 1, 0, getBuffer(buffSize));

            // Poll the 2 first buffers
            AckingContainer cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            assertEquals( 1, cont.m_lastSeqNo);
            cont.discard();

            cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            assertEquals( 2, cont.m_lastSeqNo);
            cont.discard();

            // Poll 3rd buffer but set it pending
            cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            assertEquals( 3, cont.m_lastSeqNo);
            s.setPendingContainer(cont);

            // Poll 3rd buffer once more
            cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            assertEquals( 3, cont.m_lastSeqNo);
            cont.discard();

            // Poll last buffer
            cont = s.poll().get();
            cont.updateStartTime(System.currentTimeMillis());
            assertEquals( 4, cont.m_lastSeqNo);
            cont.discard();

        } finally {
            s.shutdown();
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
