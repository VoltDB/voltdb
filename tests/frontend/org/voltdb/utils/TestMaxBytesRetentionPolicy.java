/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.utils.RetentionPolicyMgr.RetentionLimitException;
import org.voltdb.utils.TestPersistentBinaryDeque.ExtraHeaderMetadata;

import com.google.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;



public class TestMaxBytesRetentionPolicy {
    private static final VoltLogger s_logger = new VoltLogger("TestRetention");

    private static final long s_maxBytes = 8192;

    PersistentBinaryDeque<ExtraHeaderMetadata> m_pbd;

    @Test
    public void testNoReaders() throws Exception {
        Random random = new Random(System.currentTimeMillis());
        int maxNumBuffers = 10;
        int loop = 10;
        for (int i=0; i<loop; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            writeBuffers(numBuffers);
            Thread.sleep(500);
            verifyPBDSegments();
        }
    }

    @Test
    public void testWithReaders() throws Exception {
        int numReaders = 2;
        @SuppressWarnings("unchecked")
        BinaryDequeReader<ExtraHeaderMetadata>[] readers = new BinaryDequeReader[numReaders];
        for (int i=0; i<numReaders; i++) {
            readers[i] = m_pbd.openForRead("reader" + i);
        }

        int numSegments = 10;
        int maxNumBuffers = 10;
        Random random = new Random(System.currentTimeMillis());
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            writeBuffers(numBuffers);
        }
        Thread.sleep(500);
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
        for (PBDSegment<ExtraHeaderMetadata> segment : m_pbd.getSegments().values()) {
            long segmentSize = segment.m_file.length();
            int numBuffers = segment.getNumEntries();
            for (int i=0; i<numReaders; i++) {
                readBuffers(readers[i], numBuffers);
                Thread.sleep(500);
                if (i == numReaders-1) {
                    if (pbdSize - segmentSize >= s_maxBytes) {
                        expectedNumSegments--;
                        pbdSize -= segmentSize;
                    }
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

        int maxNumSegments = 20;
        int numSegments = 0;
        int numBuffers = 0;
        long firstSize = 0;
        long sizeToRetain = 0;
        List<Long> sizes = new ArrayList<>();
        for (int i=0; i<maxNumSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            writeBuffers(10);
            numBuffers += 2;
            sizeToRetain = getPbdSize();
            sizes.add(sizeToRetain);
            numSegments++;
            if (i == 0) {
                firstSize = sizeToRetain;
            }
            if (sizeToRetain-firstSize >= s_maxBytes) { // at least one to delete
                break;
            }

        }

        for (long curr : sizes) {
            if (sizeToRetain - curr >= s_maxBytes) {
                numSegments--;
                sizeToRetain -= curr;
            }
        }

        for (int i=0; i<readers.length; i++) {
            readBuffers(readers[i], numBuffers);
        }

        Thread.sleep(1000);
        assertEquals(numSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        assertEquals(sizeToRetain, getPbdSize());
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
        long firstSize = -1;
        long totalSize = 0;
        for (PBDSegment<ExtraHeaderMetadata> segment: m_pbd.getSegments().values()) {
            long size = segment.getFileSize();
            if (firstSize == -1) {
                firstSize = size;
            }
            totalSize += size;
        }

        assertTrue((totalSize <= s_maxBytes) ||
                   (totalSize - firstSize < s_maxBytes));
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
        bldr.put("+5 gb", 5L * 1024L * 1024L * 1024L);
        s_validLimits = bldr.build();
    }
    private static final Set<String> s_invalidLimits;
    static {
        ImmutableSet.Builder<String> bldr = ImmutableSet.builder();
        bldr.add("2 mb");
        bldr.add("63 mb");
        bldr.add("66 Mo");
        bldr.add("-2 gb");
        bldr.add("4 go");
        bldr.add("foo 4 gb");
        s_invalidLimits = bldr.build();
    }

    @Test
    public void testParsingLimits() throws Exception {
        for (Map.Entry<String, Long> e : s_validLimits.entrySet()) {
            long lim = RetentionPolicyMgr.parseByteLimit(e.getKey());
            assertEquals(lim, e.getValue().longValue());
        }
        for (String limStr : s_invalidLimits) {
            try {
                long lim = RetentionPolicyMgr.parseByteLimit(limStr);
                assertNull(limStr);
            }
            catch (RetentionLimitException expected) {
                ; // good
            }
        }
    }
}
