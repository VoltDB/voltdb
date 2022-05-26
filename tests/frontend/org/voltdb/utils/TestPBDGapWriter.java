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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltdb.test.utils.RandomTestRule;
import org.voltdb.utils.TestPersistentBinaryDeque.ExtraHeaderMetadata;


public class TestPBDGapWriter {
    private static final VoltLogger s_logger = new VoltLogger("TestPBD");

    private PersistentBinaryDeque<ExtraHeaderMetadata> m_pbd;
    private BinaryDequeGapWriter<ExtraHeaderMetadata> m_gapWriter;
    private ExtraHeaderMetadata m_gapHeader;
    private ExtraHeaderMetadata m_metadata;

    @Rule
    public final RandomTestRule m_random = new RandomTestRule();

    @Before
    public void setUp() throws Exception {
        TestPersistentBinaryDeque.setupTestDir();
        m_metadata = new ExtraHeaderMetadata(m_random);
        m_pbd = PersistentBinaryDeque.builder(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, s_logger)
                        .compression(true)
                        .requiresId(true)
                        .initialExtraHeader(m_metadata, TestPersistentBinaryDeque.SERIALIZER).build();
        m_gapWriter = m_pbd.openGapWriter();
        m_gapHeader = new ExtraHeaderMetadata(m_random);
        m_gapWriter.updateGapHeader(m_gapHeader);
    }

    @After
    public void tearDown() throws Exception {
        m_pbd.close();
        TestPersistentBinaryDeque.tearDownTestDir();
    }

    @Test(timeout = 10_000)
    public void testOpenGapWriter() throws Exception {
        //second open should fail
        try {
            m_pbd.openGapWriter();
            fail();
        } catch(IOException e) {
             // Expected
        }

        // second open should succeed after a close
        m_gapWriter.close();
        m_gapWriter = m_pbd.openGapWriter();
        m_gapWriter.updateGapHeader(m_gapHeader);

        // close the pbd. Now gapWriter calls should fail
        m_pbd.close();
        try {
            gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), 1, 10);
            fail();
        } catch(IOException e) {
            //expected
        }
    }

    @Test(timeout = 10_000)
    public void testNegativeOffers() throws Exception {
        int startSeqNo = 1, nextSeqNo = 1;
        for (int i=0; i<3; i++) {
            offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 4);
            nextSeqNo += 5;
            m_pbd.updateExtraHeader(m_metadata);
        }

        negativeGapOffer(startSeqNo, 10);
        negativeGapOffer(-5, 1);
        negativeGapOffer(nextSeqNo-1, nextSeqNo+10);
        negativeGapOffer(nextSeqNo-10, nextSeqNo+10);
        negativeGapOffer(3, 5);
        negativeGapOffer(5, 5);
        negativeGapOffer(6, 6);
        negativeGapOffer(6, 10);
        negativeGapOffer(3, 10);
        negativeGapOffer(10, 13);
        negativeGapOffer(-5, nextSeqNo+10);
    }

    private void negativeGapOffer(long firstSeqNo, long lastSeqNo) throws IOException {
        try {
            gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), firstSeqNo, lastSeqNo);
            fail();
        } catch(IllegalArgumentException e) {
            // expected
        }
    }

    @Test(timeout = 10_000)
    public void testFillAtHead() throws Exception {
        int nextSeqNo = 21;
        for (int i=0; i<3; i++) {
            offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 4);
            nextSeqNo += 5;
        }
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        nextSeqNo = 1;
        for (int i=0; i<4; i++) {
            m_gapWriter.updateGapHeader(m_metadata);
            gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 2);
            gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo + 3, nextSeqNo + 4);
            nextSeqNo += 5;
        }
        assertEquals(5, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        assertEquals(2, getNumActiveSegments());
        nextSeqNo = 1;
        int i = 0;
        for (PBDSegment<ExtraHeaderMetadata> segment : m_pbd.getSegments().values()) {
            if (i++ == 4) {
                break;
            }
            assertEquals(nextSeqNo, segment.m_id);
            assertEquals(nextSeqNo, segment.getStartId());
            assertEquals(nextSeqNo+4, segment.getEndId());
            nextSeqNo += 5;
        }
        m_gapWriter.close();
        assertEquals(1, getNumActiveSegments());
    }

    private int getNumActiveSegments() {
        int count = 0;
        for (PBDSegment<ExtraHeaderMetadata> segment : m_pbd.getSegments().values()) {
            if (segment.isActive()) {
                count++;
            }
        }

        return count;
    }

    @Test(timeout = 10_000)
    public void testFillAtTail() throws Exception {
        long nextSeqNo = 1;
        for (int i=0; i<3; i++) {
            offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 4);
            nextSeqNo += 5;
        }
        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());

        m_pbd.close();
        m_pbd = PersistentBinaryDeque
                .builder(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, s_logger)
                .compression(true).requiresId(true).initialExtraHeader(m_metadata, TestPersistentBinaryDeque.SERIALIZER)
                .build();
        m_gapWriter = m_pbd.openGapWriter();

        long afterGapSeqNo = nextSeqNo + 4 * 5;
        for (int i=0; i<4; i++) {
            m_gapWriter.updateGapHeader(m_gapHeader);
            gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 2);
            gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo + 3, nextSeqNo + 4);
            nextSeqNo += 5;

            // Offer beyond the end of the gap being filled
            offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), afterGapSeqNo, afterGapSeqNo + 4);
            afterGapSeqNo += 5;
        }
        assertEquals(6, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        assertEquals(2, getNumActiveSegments());
        nextSeqNo = 1;
        int i = 0;
        for (PBDSegment<ExtraHeaderMetadata> segment : m_pbd.getSegments().values()) {
            assertEquals(nextSeqNo, segment.m_id);
            assertEquals(nextSeqNo, segment.getStartId());
            if (i++ == 0) {
                assertEquals(15, segment.getEndId());
            } else if (i == 6) {
                assertEquals(55, segment.getEndId());
            } else {
                assertEquals(nextSeqNo+4, segment.getEndId());
            }
            nextSeqNo = segment.getEndId() + 1;
        }
        m_gapWriter.close();
        assertEquals(1, getNumActiveSegments());
    }

    @Test(timeout = 15_000)
    public void testFillMultipleGaps() throws Exception {
        // each gap is 20 long for this test
        TreeSet<Long> gapStarts = new TreeSet<>(Arrays.asList(-9L, 31L, 101L, 151L));
        long nextSeqNo = 11;
        while (nextSeqNo < 150) {
            if (gapStarts.contains(nextSeqNo)) {
                m_pbd.updateExtraHeader(m_metadata);
                nextSeqNo += 20;
            } else {
                offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 9);
                nextSeqNo += 10;
            }
        }
        int numFiles = TestPersistentBinaryDeque.getSortedDirectoryListing().size();

        for (long seqNo : gapStarts) {
            gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), seqNo, seqNo + 19);
        }

        assertEquals(numFiles + gapStarts.size()-1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        for (long seqNo : gapStarts) {
            if (seqNo != 151) { // last gap fill just happens on the then last segment, which will be active
                assertTrue(m_pbd.getSegments().containsKey(seqNo));
            } else {
                assertFalse(m_pbd.getSegments().containsKey(seqNo));
            }
        }

        assertEquals(1, getNumActiveSegments());
    }

    @Test (timeout = 15_000)
    public void testUnorderlyGapFilling() throws Exception {
        // gap from 51-80
        // fill 51-60, 71-80, then 61-70. This should create 3 additional segments
        int nextSeqNo = 1;
        while (nextSeqNo < 100) {
            if (nextSeqNo < 51 || nextSeqNo > 80) {
                offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 9);
            } else {
                m_pbd.updateExtraHeader(m_metadata);
            }
            nextSeqNo += 10;
        }

        int numFiles = TestPersistentBinaryDeque.getSortedDirectoryListing().size();
        gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), 51, 60);
        gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), 71, 80);
        gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), 61, 70);
        assertEquals(numFiles+3, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }

    @Test(timeout = 15_000)
    public void testReverseFillAtHead() throws Exception {
        runReverseFill(-99);
    }

    @Test(timeout = 15_000)
    public void testReverseFillAtTail() throws Exception {
        runReverseFill(1001);
    }

    @Test(timeout = 15_000)
    public void testReverseFillInTheMiddle() throws Exception {
        runReverseFill(601);
    }

    // fill a gap of 100. 50 in the end of the gap first, then the rest
    private void runReverseFill(long gapStart) throws Exception {
        long nextSeqNo = 1;
        while(nextSeqNo < 1000) {
            if (nextSeqNo == gapStart) {
                m_pbd.updateExtraHeader(m_metadata);
            } else {
                offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 99);
            }
            nextSeqNo += 100;
        }
        if (gapStart == 1001) {
            m_pbd.updateExtraHeader(m_metadata);
        }

        int numFiles = TestPersistentBinaryDeque.getSortedDirectoryListing().size();
        long otherKey = gapStart+50;
        gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), otherKey, gapStart + 99);
        gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), gapStart, gapStart + 49);
        assertEquals(numFiles + 2, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
        assertTrue(m_pbd.getSegments().containsKey(otherKey));
        assertTrue(m_pbd.getSegments().containsKey(gapStart));
    }

    @Test(timeout = 15_000)
    public void testWithQuarantined() throws Exception {
        List<Long> quarantined = Arrays.asList(1L, 101L, 501L, 601L, 1001L, 1101L);
        int nextSeqNo = 1;
        while (nextSeqNo < 1200) {
            offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 99);
            nextSeqNo += 100;
            m_pbd.updateExtraHeader(m_metadata);
        }

        for (Map.Entry<Long, PBDSegment<ExtraHeaderMetadata>> curr : m_pbd.getSegments().entrySet()) {
            if (quarantined.contains(curr.getValue().segmentId())) {
                m_pbd.quarantineSegment(curr);
            }
        }
        int numFiles = TestPersistentBinaryDeque.getSortedDirectoryListing().size();

        for (Long seqNo : quarantined) {
            gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), seqNo, seqNo + 99);
        }
        assertEquals(numFiles-3, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }

    @Test(timeout = 15_000)
    public void testGapFillingWithReading() throws Exception {
        // gap from 51-80
        int nextSeqNo = 1;
        while (nextSeqNo < 100) {
            if (nextSeqNo < 51 || nextSeqNo > 80) {
                offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 9);
            } else {
                m_pbd.updateExtraHeader(m_metadata);
            }
            nextSeqNo += 10;
        }

        int readCount = 0;
        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        // read past gap
        for (int i=0; i<6; i++) {
            BBContainer container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            container.discard();
            readCount++;
        }

        // now fill gap while reading
        nextSeqNo = 51;
        while (nextSeqNo < 80) {
            BBContainer container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            if (container != null) {
                readCount++;
                container.discard();
            }
            gapOffer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 9);
            nextSeqNo += 10;
        }

        // do some regular offers
        nextSeqNo = 101;
        while (nextSeqNo < 150) {
            offer(TestPersistentBinaryDeque.getFilledSmallBuffer(0), nextSeqNo, nextSeqNo + 9);
            nextSeqNo += 10;
        }

        BBContainer container = null;
        while ((container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            container.discard();
            readCount++;
        }

        // We make regular offers of 7 first and 5 later.
        assertEquals(12, readCount);

        //Another reader starting from the beginning. It should read all 12 + 3 gap entries
        readCount = 0;
        reader = m_pbd.openForRead("testReader-new");
        while ((container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY)) != null) {
            container.discard();
            readCount++;
        }
        assertEquals(15, readCount);
    }

    private void offer(ByteBuffer buffer, long startId, long endId) throws IOException {
        m_pbd.offer(DBBPool.wrapBB(buffer), startId, endId, System.currentTimeMillis());
    }

    private void gapOffer(ByteBuffer buffer, long startId, long endId) throws IOException {
        m_gapWriter.offer(DBBPool.wrapBB(buffer), startId, endId, System.currentTimeMillis());
    }
}
