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

import static org.junit.Assert.fail;

import java.nio.ByteBuffer;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.Pair;
import org.voltdb.utils.BinaryDeque.BinaryDequeTruncator;
import org.voltdb.utils.BinaryDeque.TruncatorResponse;
import org.voltdb.utils.PersistentBinaryDeque.ByteBufferTruncatorResponse;
import org.voltdb.utils.TestPersistentBinaryDeque.ExtraHeaderMetadata;


public class TestPBDsWithIds {
    private static final VoltLogger s_logger = new VoltLogger("TestPBD");

    PersistentBinaryDeque<ExtraHeaderMetadata> m_pbd;

    @Test
    public void testOfferWithInvalidIds() throws Exception {
        negativeOffer(-1, 10);
        negativeOffer(10, -1);
        negativeOffer(10, 8);

        m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(64)), 1, 10);
        negativeOffer(5, 10);
        try {
            m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)));
            fail("Expected AssertionError");
        } catch(AssertionError e) {
            //expected
        }
        negativeOffer(-1, -1);

        // verify also at new segment boundary
        m_pbd.updateExtraHeader(null);
        negativeOffer(5, 10);
        negativeOffer(-1, -1);
        m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(64)), 11, 20);

        // test offer with no id, then with id
        tearDown();
        setUp();
        m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)));
        negativeOffer(0, 10);
        m_pbd.updateExtraHeader(null);
        negativeOffer(0, 10);
    }

    @Test
    public void testVerifySegmentIds() throws Exception {
        int numSegments = 10;
        int maxNumIds = 100;
        int maxNumBuffers = 10;
        Random random = new Random(System.currentTimeMillis());
        long nextId = Math.abs(random.nextInt());
        @SuppressWarnings("unchecked")
        Pair<Long, Long>[] segmentIds = new Pair[numSegments];
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            long segmentStartId = nextId;
            for (int j=0; j<numBuffers; j++) {
                int numIds = random.nextInt(maxNumIds) + 1;
                m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), nextId, nextId + numIds - 1);
                nextId += numIds;
            }
            segmentIds[i] = new Pair<Long, Long>(segmentStartId, nextId-1);
        }

        int segmentIndex = 0;
        assert(numSegments == m_pbd.getSegments().values().size());
        for (PBDSegment<ExtraHeaderMetadata> segment : m_pbd.getSegments().values()) {
            PBDRegularSegment<ExtraHeaderMetadata> s = (PBDRegularSegment<ExtraHeaderMetadata>) segment;
            assert(segmentIds[segmentIndex].getFirst().longValue() == s.getStartId());
            assert(segmentIds[segmentIndex].getSecond().longValue() == s.getEndId());
            segmentIndex++;
        }
    }

    private enum TruncateIndex {
        START,
        MIDDLE,
        END;
    }

    @Test
    public void testTruncateFirstSegment() throws Exception {
        verifyIdsAfterTruncate(10, 5, 10, 0, TruncateIndex.START);
        tearDown(); setUp();
        verifyIdsAfterTruncate(10, 5, 10, 0, TruncateIndex.MIDDLE);
        tearDown(); setUp();
        verifyIdsAfterTruncate(10, 5, 10, 0, TruncateIndex.END);
    }

    @Test
    public void testTruncateMiddleSegment() throws Exception {
        verifyIdsAfterTruncate(10, 5, 10, 5, TruncateIndex.START);
        tearDown(); setUp();
        verifyIdsAfterTruncate(10, 5, 10, 5, TruncateIndex.MIDDLE);
        tearDown(); setUp();
        verifyIdsAfterTruncate(10, 5, 10, 5, TruncateIndex.END);
    }

    @Test
    public void testTruncateLastSegment() throws Exception {
        verifyIdsAfterTruncate(10, 5, 10, 9, TruncateIndex.START);
        tearDown(); setUp();
        verifyIdsAfterTruncate(10, 5, 10, 9, TruncateIndex.MIDDLE);
        tearDown(); setUp();
        verifyIdsAfterTruncate(10, 5, 10, 9, TruncateIndex.END);
    }

    public void verifyIdsAfterTruncate(int numSegments, int numBuffers, int numIds,
                                       int chosenSegment, TruncateIndex truncPoint) throws Exception {
        long nextId = 0;
        Pair<Long, Long> truncRange = null;
        @SuppressWarnings("unchecked")
        Pair<Long, Long>[] segmentIds = new Pair[numSegments];
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            segmentIds[i] = new Pair<Long, Long>(nextId, nextId + numBuffers* numIds - 1);
            if (i == chosenSegment) {
                truncRange = segmentIds[i];
            }
            for (int j=0; j<numBuffers; j++) {
                m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), nextId, nextId + numIds - 1);
                nextId += numIds;
            }
        }

        // PBD truncation code assumes that we always truncate right after opening, before any writes.
        // So we need some special handling for the truncation beyond last entry written
        if (chosenSegment == numSegments - 1 && truncPoint == TruncateIndex.END) {
            m_pbd.close();
            m_pbd = PersistentBinaryDeque.builder(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, s_logger)
                    .compression(true)
                    .initialExtraHeader(null, TestPersistentBinaryDeque.SERIALIZER).build();
        }

        final long truncId;
        switch(truncPoint) {
        case START : truncId = truncRange.getFirst();
                     break;
        case MIDDLE: truncId = (truncRange.getFirst() + truncRange.getSecond()) / 2;
                     break;
        case END   : truncId = truncRange.getSecond();
                     break;
        default : throw new Exception("Invalid truncPoint: " + truncPoint);
        }

        assert(truncId >= 0) : truncId + " is not >= 0";
        m_pbd.parseAndTruncate(new BinaryDequeTruncator() {
            int m_bufferCount = 0;

            @Override
            public TruncatorResponse parse(BBContainer bbc) {
                ByteBuffer b = bbc.b();
                int segmentIndex = m_bufferCount / numBuffers;
                int bufferIndex = m_bufferCount % numBuffers;
                final long startSequenceNumber = segmentIds[segmentIndex].getFirst() + bufferIndex * numIds;
                m_bufferCount++;
                if (startSequenceNumber >= truncId) {
                    return PersistentBinaryDeque.fullTruncateResponse();
                }
                final long lastSequenceNumber = startSequenceNumber + numIds - 1;
                if (lastSequenceNumber <= truncId) {
                    return new TruncatorResponse(TruncatorResponse.Status.NO_TRUNCATE, lastSequenceNumber);
                } else {
                    return new ByteBufferTruncatorResponse(b, truncId - 1);
                }
            }
        });
    }

    private void negativeOffer(long startId, long endId) throws Exception {
        try {
            m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), startId, endId);
            fail("Expected AssertionError");
        } catch(AssertionError e) {
            //expected
        }
    }

    @Before
    public void setUp() throws Exception {
        TestPersistentBinaryDeque.setupTestDir();
        m_pbd = PersistentBinaryDeque.builder(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, s_logger)
                        .compression(true)
                        .initialExtraHeader(null, TestPersistentBinaryDeque.SERIALIZER).build();
    }

    @After
    public void tearDown() throws Exception {
        m_pbd.close();
        TestPersistentBinaryDeque.tearDownTestDir();
    }
}
