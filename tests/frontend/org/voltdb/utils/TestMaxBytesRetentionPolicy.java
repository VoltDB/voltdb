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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.e3.topics.TopicRetention;
import org.voltdb.utils.TestPersistentBinaryDeque.ExtraHeaderMetadata;

import com.google.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;



public class TestMaxBytesRetentionPolicy {
    private static final VoltLogger s_logger = new VoltLogger("TestRetention");

    private static final long s_maxBytes = 8192;

    PersistentBinaryDeque<ExtraHeaderMetadata> m_pbd;

    @Test
    public void testNoReaders() throws Exception {
        PersistentBinaryDeque.setupRetentionPolicyMgr(1, 1);
        Random random = new Random(System.currentTimeMillis());
        int maxNumBuffers = 100;
        long numWritten = 0;
        int loop = 0;
        // Run until we write 3*maxBytes and we have more than 2 segments
        while (numWritten < s_maxBytes*3 || loop < 2) {
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            numWritten += writeBuffers(numBuffers);
            Thread.sleep(400);
            verifyPBDSegments();
            m_pbd.updateExtraHeader(null);
            loop++;
        }

        // Verify more than 1 segments were written
        assertTrue("loop " + loop, loop >= 2);
    }

    @Test
    public void testWithReaders() throws Exception {
        PersistentBinaryDeque.setupRetentionPolicyMgr(3, 1);
        int numReaders = 2;
        @SuppressWarnings("unchecked")
        BinaryDequeReader<ExtraHeaderMetadata>[] readers = new BinaryDequeReader[numReaders];
        for (int i=0; i<numReaders; i++) {
            readers[i] = m_pbd.openForRead("reader" + i);
        }

        int numSegments = 0;
        int maxNumBuffers = 100;
        Random random = new Random(System.currentTimeMillis());
        long bytesWritten = 0;
        while (bytesWritten < s_maxBytes*3) {
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            bytesWritten += writeBuffers(numBuffers);
            m_pbd.updateExtraHeader(null);
            numSegments++;
        }

        Thread.sleep(250);
        List<File> files = TestPersistentBinaryDeque.getSortedDirectoryListing();
        assertEquals(numSegments, files.size());
        long pbdSize = 0;
        for (File file : files) {
            pbdSize += file.length();
        }

        // To account for the fact that segment gets deleted only after first entry in next segment is discarded
        for (int i=0; i<readers.length; i++) {
            readBuffers(readers[i], 1);
        }

        assertEquals(numSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        int expectedNumSegments = numSegments;
        List<PBDSegment<ExtraHeaderMetadata>> segments = new ArrayList<>(m_pbd.getSegments().values());
        for (PBDSegment<ExtraHeaderMetadata> segment : segments) {
            long segmentSize = segment.m_file.length();
            int numBuffers = segment.getNumEntries();
            for (int i=0; i<numReaders; i++) {
                readBuffers(readers[i], numBuffers);
                Thread.sleep(300);
                if (i == numReaders-1) {
                    if (pbdSize - segmentSize >= s_maxBytes) {
                        expectedNumSegments--;
                    }
                    pbdSize -= segmentSize;
                    assertEquals(expectedNumSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
                } else {
                    assertEquals(expectedNumSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
                }
            }
        }
    }

    @Test
    public void testBytesRetainedAfterReads() throws Exception {
        int numReaders = 2;
        @SuppressWarnings("unchecked")
        BinaryDequeReader<ExtraHeaderMetadata>[] readers = new BinaryDequeReader[numReaders];
        for (int i=0; i<numReaders; i++) {
            readers[i] = m_pbd.openForRead("reader" + i);
        }

        int numSegments = 0;
        int numBuffers = 10;
        List<Long> sizes = new ArrayList<>();
        while (getPbdSize() <= s_maxBytes || numSegments < 2) {
            sizes.add(writeBuffers(numBuffers));
            m_pbd.updateExtraHeader(null);
            numSegments++;
        }

        long sizeToRetain = getPbdSize();
        int toDelete = 0;
        for (long curr : sizes) {
            if (sizeToRetain - curr >= s_maxBytes) {
                toDelete++;
                sizeToRetain -= curr;
            }
        }

        for (int i=0; i<readers.length; i++) {
            for (int j=0; j<=numSegments; j++) {
                readBuffers(readers[i], numBuffers);
            }
        }

        Thread.sleep(1000);
        assertEquals(numSegments-toDelete, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        assertEquals(sizeToRetain, getPbdSize());
    }

    @Test (timeout = 15_000)
    public void testWithGapFilling() throws Exception {
        m_pbd.close();
        m_pbd = PersistentBinaryDeque.builder(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, s_logger)
                        .compression(true)
                        .requiresId(true)
                        .initialExtraHeader(null, TestPersistentBinaryDeque.SERIALIZER).build();
        m_pbd.setRetentionPolicy(BinaryDeque.RetentionPolicyType.MAX_BYTES, s_maxBytes);
        m_pbd.startRetentionPolicyEnforcement();

        // Create a segment with maxBytes
        long startId = 1000;
        while (getPbdSize() < s_maxBytes) {
            long endId = startId + 10 - 1;
            m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), startId, endId, System.currentTimeMillis());
            startId = endId + 1;
        }
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        // fill gap in the beginning. Those should get deleted
        BinaryDequeGapWriter<ExtraHeaderMetadata> gapWriter = m_pbd.openGapWriter();
        ExtraHeaderMetadata header = new ExtraHeaderMetadata(new Random(System.currentTimeMillis()));
        gapWriter.updateGapHeader(header);
        gapWriter.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledBuffer(0)), 1, 500, System.currentTimeMillis() - 60_000);
        assertEquals(2, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        gapWriter.updateGapHeader(header);
        Thread.sleep(250);
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        // Start a new 'normal' segment and write gap segments and verify deletion when size exceeds
        m_pbd.updateExtraHeader(header);
        m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), startId, startId+10-1, System.currentTimeMillis());
        m_pbd.updateExtraHeader(header);
        startId += 10;
        assertEquals(2, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        long gapStartId = startId + 1000;
        long endId = gapStartId + 100 - 1;
        gapWriter.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), gapStartId, endId, System.currentTimeMillis());
        gapWriter.updateGapHeader(header);
        assertEquals(3, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        // Create gap segment in the end
        gapStartId = endId + 1;
        long segmentSize = 0;
        while (segmentSize < s_maxBytes) {
            endId = gapStartId + 10 - 1;
            segmentSize += gapWriter.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), gapStartId, endId, System.currentTimeMillis());
            gapStartId = endId + 1;
        }
        Thread.sleep(250);
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }

    @Test (timeout = 30_000)
    public void testChangeRetentionPolicy() throws Exception {
        // Fill PBD above limit to kick size-based retention at least once and
        // exit with more than 2 segments
        Random random = new Random(System.currentTimeMillis());
        long bytesWritten = 0L;
        int maxNumBuffers = 100;
        while(bytesWritten < s_maxBytes * 3
                || TestPersistentBinaryDeque.getSortedDirectoryListing().size() <= 2) {
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            bytesWritten += writeBuffers(numBuffers);
            Thread.sleep(400);
            verifyPBDSegments();
            m_pbd.updateExtraHeader(null);
        }
        assertTrue(m_pbd.isRetentionPolicyEnforced());
        assertTrue(TestPersistentBinaryDeque.getSortedDirectoryListing().size() > 2);

        // Change to a time-based retention policy and verify effect on segments
        m_pbd.stopRetentionPolicyEnforcement();
        assertFalse(m_pbd.isRetentionPolicyEnforced());
        m_pbd.setRetentionPolicy(BinaryDeque.RetentionPolicyType.TIME_MS, Long.valueOf(500));
        m_pbd.startRetentionPolicyEnforcement();

        // Wait 4 times the retention, only 1 segment should remain
        Thread.sleep(2_000);
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        assertTrue(m_pbd.isRetentionPolicyEnforced());

        // Revert to size-based retention policy
        m_pbd.stopRetentionPolicyEnforcement();
        assertFalse(m_pbd.isRetentionPolicyEnforced());
        m_pbd.setRetentionPolicy(BinaryDeque.RetentionPolicyType.MAX_BYTES, Long.valueOf(s_maxBytes));
        m_pbd.startRetentionPolicyEnforcement();

        bytesWritten = 0L;
        while(bytesWritten < s_maxBytes * 3
                || TestPersistentBinaryDeque.getSortedDirectoryListing().size() <= 2) {
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            bytesWritten += writeBuffers(numBuffers);
            Thread.sleep(400);
            verifyPBDSegments();
            m_pbd.updateExtraHeader(null);
        }
        int nSeg = TestPersistentBinaryDeque.getSortedDirectoryListing().size();
        assertTrue(nSeg > 2);

        // Wait 4 times the time-based retention, and verify no changes
        Thread.sleep(2_000);
        assertEquals(nSeg, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }

    @Test (timeout = 15_000)
    public void testChangeRetentionLimit() throws Exception {
        // Fill PBD above limit to kick size-based retention at least once and
        // exit with more than 2 segments
        Random random = new Random(System.currentTimeMillis());
        long bytesWritten = 0L;
        int maxNumBuffers = 100;
        while(bytesWritten < s_maxBytes * 3
                || TestPersistentBinaryDeque.getSortedDirectoryListing().size() <= 2) {
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            bytesWritten += writeBuffers(numBuffers);
            Thread.sleep(400);
            verifyPBDSegments();
            m_pbd.updateExtraHeader(null);
        }
        assertTrue(TestPersistentBinaryDeque.getSortedDirectoryListing().size() > 2);

        // Change the retention limit and verify effect on segments
        long newSize = s_maxBytes / 2;
        assertTrue(m_pbd.isRetentionPolicyEnforced());
        m_pbd.stopRetentionPolicyEnforcement();
        m_pbd.setRetentionPolicy(BinaryDeque.RetentionPolicyType.MAX_BYTES, Long.valueOf(newSize));
        m_pbd.startRetentionPolicyEnforcement();

        // Add at least 1 segment
        bytesWritten = 0L;
        while(bytesWritten < newSize * 3) {
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            bytesWritten += writeBuffers(numBuffers);
            Thread.sleep(250);
            verifyPBDSegments(newSize);
            m_pbd.updateExtraHeader(null);
        }
    }

    private long getPbdSize() throws IOException {
        List<File> files = TestPersistentBinaryDeque.getSortedDirectoryListing();
        long size = 0;
        for (File file : files) {
            size += file.length();
        }

        return size;
    }

    private void verifyPBDSegments() {
        verifyPBDSegments(s_maxBytes);
    }

    private void verifyPBDSegments(long maxBytes) {
        long firstSize = -1;
        long totalSize = 0;
        LinkedHashSet<Long> sizes = new LinkedHashSet<>();
        for (PBDSegment<ExtraHeaderMetadata> segment: m_pbd.getSegments().values()) {
            long size = segment.getFileSize();
            sizes.add(size);
            if (firstSize == -1) {
                firstSize = size;
            }
            totalSize += size;
        }

        assertTrue("sizes=" + sizes,
                   (totalSize <= maxBytes) || (totalSize - firstSize < maxBytes));
    }

    private void readBuffers(BinaryDequeReader<ExtraHeaderMetadata> reader, int numBuffers) throws Exception {
        for (int i=0; i<numBuffers; i++) {
            BBContainer container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            if (container != null) {
                container.discard();
            } else {
                break;
            }
        }
    }

    private long writeBuffers(int numBuffers) throws Exception {
        long written = 0;
        for (int i=0; i<numBuffers; i++) {
            written += m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(i)));
        }

        return written;
    }

    @BeforeClass
    public static void setUpClass() {
        PersistentBinaryDeque.setupRetentionPolicyMgr(2, 1);
    }

    @Before
    public void setUp() throws Exception {
        TestPersistentBinaryDeque.setupTestDir();
        m_pbd = PersistentBinaryDeque.builder(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, s_logger)
                        .compression(true)
                        .initialExtraHeader(null, TestPersistentBinaryDeque.SERIALIZER).build();
        m_pbd.setRetentionPolicy(BinaryDeque.RetentionPolicyType.MAX_BYTES, s_maxBytes);
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
        bldr.put("64mb", 64 * 1024L * 1024L);
        bldr.put("240 MB", 240L * 1024L * 1024L);
        bldr.put("100 Mb", 100L * 1024L * 1024L);
        bldr.put("2gb", 2L * 1024L * 1024L * 1024L);
        bldr.put("3 GB", 3L * 1024L * 1024L * 1024L);
        s_validLimits = bldr.build();
    }
    private static final Set<String> s_invalidLimits;
    static {
        ImmutableSet.Builder<String> bldr = ImmutableSet.builder();
        bldr.add("2 mb");
        bldr.add("63 mb");
        bldr.add("66 Mx");
        bldr.add("-2 gb");
        bldr.add("4 go");
        bldr.add("foo 4 gb");
        bldr.add("+5 gb");
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
                assertEquals(retention.getPolicy(), TopicRetention.Policy.SIZE);
                System.out.println("Failed = " + limStr);
                fail();
            }
            catch (Exception expected) {
                ; // good
            }
        }
    }
}
