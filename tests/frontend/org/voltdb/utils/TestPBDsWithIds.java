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
import static org.junit.Assert.fail;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
import org.voltdb.utils.BinaryDequeReader.NoSuchOffsetException;
import org.voltdb.utils.BinaryDequeReader.SeekErrorRule;
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
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 100, 10, -1);

        verifySegmentIds(numSegments, segmentIds);
    }

    /*
     * Utility method that creates and populates segments based on given parameters
     * and returns an array with the start and end ids of the segments.
     */
    private Pair<Long, Long>[] createPopulateSegments(int numSegments, int maxNumIds, int maxNumBuffers, int gapSegment)
            throws Exception {
        Random random = new Random(System.currentTimeMillis());
        long nextId = Math.abs(random.nextInt());
        @SuppressWarnings("unchecked")
        Pair<Long, Long>[] segmentIds = new Pair[numSegments];
        for (int i=0; i<numSegments; i++) {
            if (i > 0) {
                m_pbd.updateExtraHeader(null);
            }
            int numBuffers = random.nextInt(maxNumBuffers) + 1;
            if (i == gapSegment) {
                nextId += random.nextInt(500) + 1;
            }
            long segmentStartId = nextId;
            for (int j=0; j<numBuffers; j++) {
                int numIds = random.nextInt(maxNumIds) + 1;
                m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), nextId, nextId + numIds - 1);
                nextId += numIds;
            }
            segmentIds[i] = new Pair<Long, Long>(segmentStartId, nextId-1);
        }

        return segmentIds;
    }

    private void verifySegmentIds(int numSegments, Pair<Long, Long>[] segmentIds) throws Exception {
        int segmentIndex = 0;
        assert(numSegments == m_pbd.getSegments().values().size());
        for (PBDSegment<ExtraHeaderMetadata> segment : m_pbd.getSegments().values()) {
            PBDRegularSegment<ExtraHeaderMetadata> s = (PBDRegularSegment<ExtraHeaderMetadata>) segment;
            assert(segmentIds[segmentIndex].getFirst().longValue() == s.getStartId()) : "For segment index " + segmentIndex + " " +
                                                                                         segmentIds[segmentIndex].getFirst() + "!=" + s.getStartId();
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

    @Test
    public void testIdsAfterRecover() throws Exception {
        int numSegments = 10;
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 100, 10, -1);
        m_pbd.close();
        m_pbd = PersistentBinaryDeque.builder(TestPersistentBinaryDeque.TEST_NONCE, TestPersistentBinaryDeque.TEST_DIR, s_logger)
                        .compression(true)
                        .initialExtraHeader(null, TestPersistentBinaryDeque.SERIALIZER).build();
        // We create a new segment on recover
        numSegments++;
        Pair<Long, Long>[] newSegmentIds = Arrays.copyOf(segmentIds, numSegments);
        newSegmentIds[numSegments-1] = new Pair<Long, Long>(-1L, -1L);
        verifySegmentIds(numSegments, newSegmentIds);

        negativeOffer(-1, -1);
        long nextId = newSegmentIds[numSegments-2].getSecond() + 1;
        long endId = nextId + 50;
        m_pbd.offer(DBBPool.wrapBB(TestPersistentBinaryDeque.getFilledSmallBuffer(0)), nextId, endId);
        newSegmentIds[numSegments-1] = new Pair<Long, Long>(nextId, endId);
        verifySegmentIds(numSegments, newSegmentIds);
    }

    @Test
    public void testSeekEmpty() throws Exception {
        long seekId = 10;
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("testReader", true);
        negativeSeek(reader, seekId, SeekErrorRule.SEEK_AFTER);
        negativeSeek(reader, seekId, SeekErrorRule.SEEK_BEFORE);
        negativeSeek(reader, seekId, SeekErrorRule.THROW);
    }

    @Test
    public void testSingleSegmentSeek() throws Exception {
        int numSegments = 1;
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 10, 10, -1);
        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);

        // seek to beginning
        runValidSeeksInSegment(reader, segmentIds[0]);
        // middle
        runValidSeeksInSegment(reader, segmentIds[numSegments/2]);
        // end
        runValidSeeksInSegment(reader, segmentIds[numSegments-1]);

        assertEquals(1, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }
    @Test
    public void testValidSeeks() throws Exception {
        int numSegments = 5;
        Random rand = new Random(System.currentTimeMillis());
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 10, 10, rand.nextInt(numSegments-1)+1);
        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        List<BBContainer> toDiscard = readBuffers(reader, rand.nextInt(50));

        // seek to beginning
        runValidSeeksInSegment(reader, segmentIds[0]);
        // middle
        runValidSeeksInSegment(reader, segmentIds[numSegments/2]);
        // end
        runValidSeeksInSegment(reader, segmentIds[numSegments-1]);

        for (BBContainer c : toDiscard) {
            c.discard();
        }
        assertEquals(numSegments, TestPersistentBinaryDeque.getSortedDirectoryListing().size());
    }

    private void runValidSeeksInSegment(PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader, Pair<Long, Long> range) throws Exception {
        ArrayList<BBContainer> toDiscard = new ArrayList<>();
        for (SeekErrorRule errorRule : SeekErrorRule.values()) {
            toDiscard.add(verifySeek(reader, range.getFirst(), errorRule, range));
            toDiscard.add(verifySeek(reader, (range.getFirst() + range.getSecond())/2, errorRule, range));
            toDiscard.add(verifySeek(reader, range.getSecond(), errorRule, range));
        }
        for (BBContainer c : toDiscard) {
            c.discard();
        }
    }

    @Test
    public void testSeekEntryBeforeFirst() throws Exception {
        Pair<Long, Long>[] segmentIds = createPopulateSegments(3, 10, 10, -1);
        long firstAvailable = segmentIds[0].getFirst();
        if (firstAvailable == 0) { // cannot run this test in this random run`
            return;
        }
        long seekId = firstAvailable - 1;
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("testReader", true);
        List<BBContainer> toDiscard = readBuffers(reader, (new Random(System.currentTimeMillis())).nextInt(30));
        negativeSeek(reader, seekId, SeekErrorRule.SEEK_BEFORE);
        negativeSeek(reader, seekId, SeekErrorRule.THROW);
        reader.seekToSegment(seekId, SeekErrorRule.SEEK_AFTER);
        BBContainer container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        container.discard();
        PBDSegment<ExtraHeaderMetadata> currSegment = ((PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor) reader).getCurrentSegment();
        assert(currSegment.getStartId() == firstAvailable);

        for (BBContainer c : toDiscard) {
            c.discard();
        }
    }

    @Test
    public void testSeekEntryAfterLast() throws Exception {
        Pair<Long, Long>[] segmentIds = createPopulateSegments(3, 10, 10, -1);
        long lastAvailable = segmentIds[segmentIds.length-1].getSecond();
        long seekId = lastAvailable + 1;
        BinaryDequeReader<ExtraHeaderMetadata> reader = m_pbd.openForRead("testReader", true);
        List<BBContainer> toDiscard = readBuffers(reader, (new Random(System.currentTimeMillis())).nextInt(30));
        negativeSeek(reader, seekId, SeekErrorRule.SEEK_AFTER);
        negativeSeek(reader, seekId, SeekErrorRule.THROW);
        reader.seekToSegment(seekId, SeekErrorRule.SEEK_BEFORE);
        BBContainer container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        container.discard();
        for (BBContainer c : toDiscard) {
            c.discard();
        }
        assertEquals(3, TestPersistentBinaryDeque.getSortedDirectoryListing().size()); // transient readers don't delete segments
        PBDSegment<ExtraHeaderMetadata> currSegment = ((PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor) reader).getCurrentSegment();
        assert(currSegment.getEndId() == lastAvailable);
    }

    @Test
    public void testSeekGapInBetween() throws Exception {
        int numSegments = 5;
        Random rand = new Random(System.currentTimeMillis());
        int gapSegment = rand.nextInt(numSegments-1) + 1; // exclude first segment
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 100, 10, gapSegment);
        long gapStart = segmentIds[gapSegment-1].getSecond()+1;
        long gapEnd = segmentIds[gapSegment].getFirst()-1;

        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        List<BBContainer> toDiscard = readBuffers(reader, rand.nextInt(50));
        negativeSeek(reader, gapStart, SeekErrorRule.THROW);
        negativeSeek(reader, gapEnd, SeekErrorRule.THROW);
        negativeSeek(reader, (gapStart+gapEnd)/2, SeekErrorRule.THROW);

        toDiscard.add(verifySeek(reader, gapStart, SeekErrorRule.SEEK_BEFORE, segmentIds[gapSegment-1]));
        toDiscard.add(verifySeek(reader, gapEnd, SeekErrorRule.SEEK_BEFORE, segmentIds[gapSegment-1]));
        toDiscard.add(verifySeek(reader, (gapStart+gapEnd)/2, SeekErrorRule.SEEK_BEFORE, segmentIds[gapSegment-1]));
        toDiscard.add(verifySeek(reader, gapStart, SeekErrorRule.SEEK_AFTER, segmentIds[gapSegment]));
        toDiscard.add(verifySeek(reader, gapEnd, SeekErrorRule.SEEK_AFTER, segmentIds[gapSegment]));
        toDiscard.add(verifySeek(reader, (gapStart+gapEnd)/2, SeekErrorRule.SEEK_AFTER, segmentIds[gapSegment]));

        for (BBContainer c : toDiscard) {
            c.discard();
        }
    }

    @Test
    public void testSeekAllQuarantined() throws Exception {
        int numSegments = 3;
        createPopulateSegments(numSegments, 10, 10, -1);
        for (Map.Entry<Long, PBDSegment<ExtraHeaderMetadata>> entry : m_pbd.getSegments().entrySet()) {
            m_pbd.quarantineSegment(entry);
        }
        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        negativeSeek(reader, 5, SeekErrorRule.SEEK_AFTER);
        negativeSeek(reader, 5, SeekErrorRule.SEEK_BEFORE);
        negativeSeek(reader, 5, SeekErrorRule.THROW);
    }

    @Test
    public void testSeekFirstQuarantined() throws Exception {
        int numSegments = 3;
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 10, 10, -1);
        m_pbd.quarantineSegment(m_pbd.getSegments().entrySet().iterator().next());
        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        ArrayList<BBContainer> toDiscard = new ArrayList<>();
        for (int i=1; i<numSegments; i++) {
            for (SeekErrorRule errorRule : SeekErrorRule.values()) {
                toDiscard.add(verifySeek(reader, segmentIds[i].getFirst(), errorRule, segmentIds[i]));
            }
        }
        long seekId = 0;
        toDiscard.add(verifySeek(reader, seekId, SeekErrorRule.SEEK_AFTER, segmentIds[1]));
        negativeSeek(reader, 0, SeekErrorRule.SEEK_BEFORE);
        negativeSeek(reader, 0, SeekErrorRule.THROW);
        seekId = segmentIds[numSegments-1].getSecond() + 1;
        toDiscard.add(verifySeek(reader, seekId, SeekErrorRule.SEEK_BEFORE, segmentIds[numSegments-1]));
        negativeSeek(reader, seekId, SeekErrorRule.SEEK_AFTER);
        negativeSeek(reader, seekId, SeekErrorRule.THROW);

        for (BBContainer c : toDiscard) {
            c.discard();
        }
    }

    @Test
    public void testSeekLastQuarantined() throws Exception {
        int numSegments = 3;
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 10, 10, -1);
        for (Map.Entry<Long, PBDSegment<ExtraHeaderMetadata>> entry : m_pbd.getSegments().entrySet()) {
            if (entry.getKey().longValue() == numSegments) {
                m_pbd.quarantineSegment(entry);
            }
        }
        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        ArrayList<BBContainer> toDiscard = new ArrayList<>();
        for (int i=0; i<numSegments-1; i++) {
            for (SeekErrorRule errorRule : SeekErrorRule.values()) {
                toDiscard.add(verifySeek(reader, segmentIds[i].getFirst(), errorRule, segmentIds[i]));
            }
        }
        long seekId = segmentIds[numSegments-2].getSecond() + 1;
        toDiscard.add(verifySeek(reader, seekId, SeekErrorRule.SEEK_BEFORE, segmentIds[numSegments-2]));
        negativeSeek(reader, seekId, SeekErrorRule.SEEK_AFTER);
        negativeSeek(reader, seekId, SeekErrorRule.THROW);

        for (BBContainer c : toDiscard) {
            c.discard();
        }
    }

    @Test
    public void testSeekFirstLastQuarantined() throws Exception {
        int numSegments = 5;
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 10, 10, -1);
        for (Map.Entry<Long, PBDSegment<ExtraHeaderMetadata>> entry : m_pbd.getSegments().entrySet()) {
            long key = entry.getKey().longValue();
            if (key == 1 || key == numSegments) {
                m_pbd.quarantineSegment(entry);
            }
        }
        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        ArrayList<BBContainer> toDiscard = new ArrayList<>();
        for (int i=1; i<numSegments-1; i++) {
            for (SeekErrorRule errorRule : SeekErrorRule.values()) {
                toDiscard.add(verifySeek(reader, segmentIds[i].getFirst(), errorRule, segmentIds[i]));
            }
        }

        long seekId = segmentIds[0].getSecond();
        toDiscard.add(verifySeek(reader, seekId, SeekErrorRule.SEEK_AFTER, segmentIds[1]));
        negativeSeek(reader, seekId, SeekErrorRule.SEEK_BEFORE);
        negativeSeek(reader, seekId, SeekErrorRule.THROW);
        for (BBContainer c : toDiscard) {
            c.discard();
        }
    }

    @Test
    public void testSeekOnlyFirstValid() throws Exception {
        int numSegments = 3;
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 10, 10, -1);
        for (Map.Entry<Long, PBDSegment<ExtraHeaderMetadata>> entry : m_pbd.getSegments().entrySet()) {
            if (entry.getKey().longValue() != 1) {
                m_pbd.quarantineSegment(entry);
            }
        }
        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        ArrayList<BBContainer> toDiscard = new ArrayList<>();
        for (SeekErrorRule errorRule : SeekErrorRule.values()) {
            toDiscard.add(verifySeek(reader, segmentIds[0].getFirst(), errorRule, segmentIds[0]));
            toDiscard.add(verifySeek(reader, segmentIds[0].getSecond(), errorRule, segmentIds[0]));
        }

        long seekId = segmentIds[1].getFirst();
        toDiscard.add(verifySeek(reader, seekId, SeekErrorRule.SEEK_BEFORE, segmentIds[0]));
        negativeSeek(reader, seekId, SeekErrorRule.SEEK_AFTER);
        negativeSeek(reader, seekId, SeekErrorRule.THROW);

        for (BBContainer c : toDiscard) {
            c.discard();
        }
    }

    @Test
    public void testSeekOnlyLastValid() throws Exception {
        int numSegments = 3;
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 10, 10, -1);
        for (Map.Entry<Long, PBDSegment<ExtraHeaderMetadata>> entry : m_pbd.getSegments().entrySet()) {
            if (entry.getKey().longValue() != numSegments) {
                m_pbd.quarantineSegment(entry);
            }
        }
        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        ArrayList<BBContainer> toDiscard = new ArrayList<>();
        for (SeekErrorRule errorRule : SeekErrorRule.values()) {
            toDiscard.add(verifySeek(reader, segmentIds[numSegments-1].getFirst(), errorRule, segmentIds[numSegments-1]));
            toDiscard.add(verifySeek(reader, segmentIds[numSegments-1].getSecond(), errorRule, segmentIds[numSegments-1]));
        }

        long seekId = segmentIds[0].getSecond();
        toDiscard.add(verifySeek(reader, seekId, SeekErrorRule.SEEK_AFTER, segmentIds[numSegments-1]));
        negativeSeek(reader, seekId, SeekErrorRule.SEEK_BEFORE);
        negativeSeek(reader, seekId, SeekErrorRule.THROW);

        for (BBContainer c : toDiscard) {
            c.discard();
        }
    }

    @Test
    public void testSeekRandomQuarantined() throws Exception {
        int numSegments = 10;
        Pair<Long, Long>[] segmentIds = createPopulateSegments(numSegments, 10, 10, -1);
        Random random = new Random(System.currentTimeMillis());
        int numQuarantined = 3;
        HashSet<Long> quarantinedIndexes = new HashSet<>();
        for (int i=0; i<numQuarantined; i++) {
            // quarantine segments other than ones at index 1 and numSegments (2 through numSegments-1)
            quarantinedIndexes.add((long) random.nextInt(numSegments-2)+2);
        }
        for (Map.Entry<Long, PBDSegment<ExtraHeaderMetadata>> entry : m_pbd.getSegments().entrySet()) {
            if (quarantinedIndexes.contains(entry.getKey().longValue())) {
                m_pbd.quarantineSegment(entry);
            }
        }

        PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader = m_pbd.openForRead("testReader", true);
        ArrayList<BBContainer> toDiscard = new ArrayList<>();
        for (int i=0; i<numSegments; i++) {
            if (quarantinedIndexes.contains((long)i+1)) {
                int prev = i-1; // indexes start at 1
                while (quarantinedIndexes.contains(prev+1L)) {
                    prev--;
                }
                int next = i+1;
                while (quarantinedIndexes.contains(next+1L)) {
                    next++;
                }
                toDiscard.add(verifySeek(reader, segmentIds[i].getFirst(), SeekErrorRule.SEEK_AFTER, segmentIds[next]));
                toDiscard.add(verifySeek(reader, segmentIds[i].getFirst(), SeekErrorRule.SEEK_BEFORE, segmentIds[prev]));
                negativeSeek(reader, segmentIds[i].getFirst(), SeekErrorRule.THROW);
                toDiscard.add(verifySeek(reader, segmentIds[i].getSecond(), SeekErrorRule.SEEK_AFTER, segmentIds[next]));
                toDiscard.add(verifySeek(reader, segmentIds[i].getSecond(), SeekErrorRule.SEEK_BEFORE, segmentIds[prev]));
                negativeSeek(reader, segmentIds[i].getSecond(), SeekErrorRule.THROW);
            } else {
                for (SeekErrorRule errorRule : SeekErrorRule.values()) {
                    toDiscard.add(verifySeek(reader, segmentIds[i].getFirst(), errorRule, segmentIds[i]));
                    toDiscard.add(verifySeek(reader, segmentIds[i].getSecond(), errorRule, segmentIds[i]));
                }
            }
        }
    }

    private BBContainer verifySeek(PersistentBinaryDeque<ExtraHeaderMetadata>.ReadCursor reader, long seekId, SeekErrorRule errorRule, Pair<Long, Long> expectedRange)
        throws IOException, NoSuchOffsetException {
        reader.seekToSegment(seekId, errorRule);
        BBContainer container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
        PBDSegment<ExtraHeaderMetadata> currSegment = reader.getCurrentSegment();
        assert(currSegment.getStartId() == expectedRange.getFirst());
        assert(currSegment.getEndId() == expectedRange.getSecond());
        return container;
    }

    private List<BBContainer> readBuffers(BinaryDequeReader<ExtraHeaderMetadata> reader, int numBuffers) throws IOException {
        ArrayList<BBContainer> containers = new ArrayList<>();
        for (int i=0; i<numBuffers; i++) {
            BBContainer container = reader.poll(PersistentBinaryDeque.UNSAFE_CONTAINER_FACTORY);
            if (container == null) {
                break;
            } else {
                containers.add(container);
            }
        }

        return containers;
    }

    private void negativeSeek(BinaryDequeReader<ExtraHeaderMetadata> reader, long seekId, SeekErrorRule errorRule) throws Exception {
        try {
            reader.seekToSegment(seekId, errorRule);
            fail();
        } catch(NoSuchOffsetException e) {
            // expected
        }
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
