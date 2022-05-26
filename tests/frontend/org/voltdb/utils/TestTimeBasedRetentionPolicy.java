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
package org.voltdb.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.e3.topics.TopicRetention;
import org.voltdb.utils.TestPersistentBinaryDeque.ExtraHeaderMetadata;

import com.google.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;


public class TestTimeBasedRetentionPolicy {
    private static final VoltLogger s_logger = new VoltLogger("TestRetention");
    private static final int s_retainMillis = 500;

    @Rule
    public final TestName m_name = new TestName();

    PersistentBinaryDeque<ExtraHeaderMetadata> m_pbd;

    @Test(timeout=60_000)
    public void testNoReadersOneSegment() throws Exception {
        PersistentBinaryDeque.setupRetentionPolicyMgr(1, 1);
        // In this test, retention policy has only one segment to delete at a time.
        int segmentsCount = 5;

        // Write initial  segment, should never be deleted
        writeBuffers(2);
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        // Run common test with default retention
        commonNoReaderSegmentTest(segmentsCount, s_retainMillis);

        // Stop the current enforcement and start a new one with twice the previous retention
        int newRetention = 2 * s_retainMillis;
        m_pbd.stopRetentionPolicyEnforcement();
        m_pbd.setRetentionPolicy(BinaryDeque.RetentionPolicyType.TIME_MS, Long.valueOf(newRetention));
        m_pbd.startRetentionPolicyEnforcement();

        // Run common test with new retention
        commonNoReaderSegmentTest(segmentsCount, newRetention);

        // last segment shouldn't get deleted
        Thread.sleep(s_retainMillis + 250);
    }

    private void commonNoReaderSegmentTest(int segmentsCount, int retainMs) throws Exception {
        for (int i=1; i<segmentsCount; i++) {
            // force new segment creation.
            m_pbd.updateExtraHeader(null);
            writeBuffers(2);
            assertEquals(2, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
            long fileTime;
            synchronized (m_pbd) {
                fileTime = m_pbd.getSegments().firstEntry().getValue().getTimestamp();
            }

            long now = 0L;
            long end = fileTime + retainMs;
            while(true) {
                Thread.sleep(50);
                now = System.currentTimeMillis();
                if (TestPersistentBinaryDeque.getSortedDirectoryListing().size() == 1) {
                    break;
                }
                assert (now < (end + 250));
            }

            // We must not prune before the expected delay
            now = System.currentTimeMillis();
            assertTrue("Now=" + now + ", end=" + end, now + 250 >= end);
        }
    }

    @Test
    public void testNoReadersMultiSegments() throws Exception {
        PersistentBinaryDeque.setupRetentionPolicyMgr(3, 1);
        // In this test, retention policy may have more than one segment to delete at a time.
        int segmentsCount = 5;
        Random rand = new Random();
        long lastTime = 0;
        for (int i=0; i<segmentsCount; i++) {
            if (i>0) { // force new segment creation.
                // add a random delay, so that all segments won't get deleted one shot
                Thread.sleep(rand.nextInt(s_retainMillis/2));
                m_pbd.updateExtraHeader(null);
                lastTime = System.currentTimeMillis();
            }
            writeBuffers(2);
        }
        Thread.sleep(s_retainMillis - (System.currentTimeMillis() - lastTime) + 100);
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());;

        // last segment shouldn't get deleted
        Thread.sleep(s_retainMillis + 250);
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }

    @Test
    public void testWithOneReader() throws Exception {
        runTestSegmentsDeletedAfterReading(1);
    }

    @Test
    public void testWithMultipleReaders() throws Exception {
        runTestSegmentsDeletedAfterReading(3);
    }

    // This test verifies that the segments are kept around for retention time
    // even though all readers have finished reading.
    @Test
    public void testVerifyRetentionTime() throws Exception {
        // Open a reader
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("reader1");

        // write and read PBD segments
        int numSegments = 3;
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            writeBuffers(2);
            readBuffers(reader, 2);
        }

        assertEquals(numSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        Thread.sleep(s_retainMillis + 250);
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }

    // Verifies that PBD segments are deleted only after all the readers are done reading,
    // even though retention time has elapsed.
    private void runTestSegmentsDeletedAfterReading(int numReaders) throws Exception {
        @SuppressWarnings("unchecked")
        BinaryDequeReader<ExtraHeaderMetadata>[] readers = new BinaryDequeReader[numReaders];
        for (int i=0; i<numReaders; i++) {
            readers[i] = m_pbd.openForRead("reader" + i);
        }

        // write PBD segments
        int numSegments = 5;
        Map<Integer, Long> segmentTimes = new HashMap<>();
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                Thread.sleep(s_retainMillis);
                m_pbd.updateExtraHeader(null);
                segmentTimes.put(i-1, System.currentTimeMillis());
            }
            writeBuffers(2);
        }

        int expected = numSegments;
        boolean first = true;
        for (long time : segmentTimes.values()) {
            long sleepTime = s_retainMillis - (System.currentTimeMillis() - time);
            if (sleepTime > 0) {
                Thread.sleep(s_retainMillis + 100);
            }
            // Readers haven't moved on to next segment, so segment shouldn't be deleted
            assertEquals(expected, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
            for (int i=0; i<readers.length; i++) {
                int numToRead = (first) ? 3 : 2;
                readBuffers(readers[i], numToRead);
                if (i<readers.length-1) {
                    assertEquals(expected, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
                }
            }
            first &= false;
            // Readers have moved on. Segment should be deleted.
            expected--;
            Thread.sleep(200);
            assertEquals(expected, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        }
    }

    @Test(timeout = 30_000)
    public void testWithGapFilling() throws Exception {
        /*
         * This tests gaps filled before the point at which the retention reader is.
         */
        // retention of 2 minutes
        m_pbd.close();
        long retention = 120 *1000;
        m_pbd = PersistentBinaryDeque.builder(m_name.getMethodName(), TestPersistentBinaryDeque.TEST_DIR, s_logger)
                        .compression(true)
                        .requiresId(true)
                        .initialExtraHeader(null, TestPersistentBinaryDeque.SERIALIZER).build();
        m_pbd.setRetentionPolicy(BinaryDeque.RetentionPolicyType.TIME_MS, Long.valueOf(retention));
        m_pbd.startRetentionPolicyEnforcement();

        // segment with current time
        m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), 501, 600, System.currentTimeMillis());
        m_pbd.updateExtraHeader(null);
        m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), 1001, 1100, System.currentTimeMillis()+ 60*1000);

        // fill gap before the current segment with time before retention time
        BinaryDequeGapWriter<ExtraHeaderMetadata> gapWriter = m_pbd.openGapWriter();
        ExtraHeaderMetadata header = new ExtraHeaderMetadata(new Random(System.currentTimeMillis()));
        long gapTime = System.currentTimeMillis() - 5*60*1000;
        gapWriter.updateGapHeader(header);
        // Fill gap segments and verify they are deleted because they are older
        gapWriter.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), 1, 100, gapTime);
        gapWriter.updateGapHeader(header);
        Thread.sleep(250);
        assertEquals(2, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        gapWriter.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), 101, 200, gapTime);
        gapWriter.updateGapHeader(header);
        Thread.sleep(250);
        assertEquals(2, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        gapWriter.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), 201, 300, gapTime);
        gapWriter.updateGapHeader(header);
        Thread.sleep(250);
        assertEquals(2, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        // fill newer gaps
        gapWriter.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), 601, 700, System.currentTimeMillis());
        assertEquals(3, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        gapWriter.updateGapHeader(header);
        gapWriter.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), 701, 800, System.currentTimeMillis());
        gapWriter.close();
        assertEquals(4, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }

    @Test(timeout = 30_000)
    public void testRetentionFrequency() throws Exception {
        // retention of 2 minutes
        m_pbd.close();
        long retention = 120 *1000;
        m_pbd = PersistentBinaryDeque.builder(m_name.getMethodName(), TestPersistentBinaryDeque.TEST_DIR, s_logger)
                        .compression(true)
                        .requiresId(true)
                        .initialExtraHeader(null, TestPersistentBinaryDeque.SERIALIZER).build();
        AtomicInteger callCount = new AtomicInteger(0);
        m_pbd.registerDeferredDeleter((r) -> { callCount.getAndIncrement(); r.run(); } );
        m_pbd.setRetentionPolicy(BinaryDeque.RetentionPolicyType.TIME_MS, Long.valueOf(retention));

        // write a few PBD segments
        long time = System.currentTimeMillis() - 120*1000;
        long startId = 1;
        int numSegments = 2;

        // Write 2 segments with lower time for retention to delete 2 segments
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), startId, startId+100-1, time);
            startId += 100;
            time += 100;
        }
        m_pbd.startRetentionPolicyEnforcement();

        // More segments with current time
        for (int i=0; i<numSegments; i++) {
            m_pbd.updateExtraHeader(null);
            m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), startId, startId+100-1, System.currentTimeMillis());
            startId += 100;
            time += 100;
        }

        // wait for a few seconds. There should have been only be 2 retentions
        Thread.sleep(5000);
        assertEquals(2, callCount.intValue());
    }

    private void readBuffers(BinaryDequeReader<ExtraHeaderMetadata> reader, int numBuffers) throws Exception {
        for (int i=0; i<numBuffers; i++) {
            BBContainer container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            assert(container != null);
            container.discard();
        }
    }

    private void writeBuffers(int numBuffers) throws Exception {
        for (int i=0; i<numBuffers; i++) {
            m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(64)) );
        }
    }

    @BeforeClass
    public static void setUpClass() {
        PersistentBinaryDeque.setupRetentionPolicyMgr(2, 1);
    }

    @Before
    public void setUp() throws Exception {
        TestPersistentBinaryDeque.setupTestDir();
        m_pbd = PersistentBinaryDeque.builder(m_name.getMethodName(), TestPersistentBinaryDeque.TEST_DIR, s_logger)
                        .compression(true)
                        .initialExtraHeader(null, TestPersistentBinaryDeque.SERIALIZER).build();
        m_pbd.setRetentionPolicy(BinaryDeque.RetentionPolicyType.TIME_MS, Long.valueOf(s_retainMillis));
        m_pbd.startRetentionPolicyEnforcement();
    }

    @After
    public void tearDown() throws Exception {
        m_pbd.close();
        TestPersistentBinaryDeque.tearDownTestDir();
    }

    private static final Map<String, Long> s_validLimits;
    static {
        ImmutableMap.Builder<String, Long> bldr = ImmutableMap.builder();
        bldr.put("1mn", 60_000L);
        bldr.put("2 MN", 2L * 60_000L);
        bldr.put("10 Mn", 10L * 60_000L);
        bldr.put("2 hr", 2L * 60L * 60_000L);
        bldr.put("23 HR", 23L * 60L * 60_000L);
        bldr.put("2mo" , 60L * 24L * 60L * 60_000L);
        bldr.put("5 yr", 5 * 365L * 24L * 60L * 60_000L);
        s_validLimits = bldr.build();
    }
    private static final Set<String> s_invalidLimits;
    static {
        ImmutableSet.Builder<String> bldr = ImmutableSet.builder();
        bldr.add("2 mi");
        bldr.add("-2 mn");
        bldr.add("4 days");
        bldr.add("foo 4 dy");
        bldr.add("+3 dy");
        s_invalidLimits = bldr.build();
    }

    @Test
    public void testParsingLimits() throws Exception {
        for (Map.Entry<String, Long> e : s_validLimits.entrySet()) {
            TopicRetention retention = TopicRetention.parse(e.getKey());
            assertEquals(e.getValue().longValue(), retention.getEffectiveLimit());
        }
        for (String limStr : s_invalidLimits) {
            try {
                TopicRetention retention = TopicRetention.parse(limStr);
                assertEquals(retention.getPolicy(), TopicRetention.Policy.TIME);
                fail();
            }
            catch (Exception expected) {
                ; // good
            }
        }
    }
}
