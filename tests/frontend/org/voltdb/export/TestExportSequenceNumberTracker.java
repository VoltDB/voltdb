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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.voltcore.utils.Pair;

public class TestExportSequenceNumberTracker {

    private ExportSequenceNumberTracker tracker;

    @Before
    public void setUp() throws IOException {
        tracker = new ExportSequenceNumberTracker();
    }

    @Test
    public void testBasic() throws Exception {
        // Append single to range
        tracker.append(1L, 1L);
        assertEquals(1L, tracker.getFirstSeqNo());
        assertEquals(1L, tracker.getLastSeqNo());
        assertEquals(1L, tracker.getSafePoint());

        int truncated = tracker.truncateBefore(1L);
        assertEquals(1L, tracker.getFirstSeqNo());
        assertEquals(1L, tracker.getLastSeqNo());
        assertEquals(1L, tracker.getSafePoint());
        assertEquals(2L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(ExportSequenceNumberTracker.INFINITE_SEQNO,
                tracker.getFirstGap().getSecond().longValue());
        assertEquals(0, truncated);

        // Append adjacent single to range
        tracker.append(2L, 9L);
        assertEquals(1L, tracker.getFirstSeqNo());
        assertEquals(9L, tracker.getLastSeqNo());
        assertEquals(9L, tracker.getSafePoint());

        // Append single to create a gap
        tracker.append(15L, 20L);
        assertEquals(1L, tracker.getFirstSeqNo());
        assertEquals(20L, tracker.getLastSeqNo());
        assertEquals(9L, tracker.getSafePoint());
        assertEquals(10L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap().getSecond().longValue());

        tracker.append(25L, 30L);
        tracker.append(35L, 40L);

        // Test finding gap by sequence number, checking boundary cases
        // Testing finding gaps on [1, 9] [15, 20] [25, 30] [35, 40]

        assertEquals(10L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap().getSecond().longValue());
        assertEquals(10L, tracker.getFirstGap(9L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(9L).getSecond().longValue());
        assertEquals(10L, tracker.getFirstGap(10L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(10L).getSecond().longValue());
        assertEquals(10L, tracker.getFirstGap(13L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(13L).getSecond().longValue());
        assertEquals(10L, tracker.getFirstGap(14L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(14L).getSecond().longValue());

        assertEquals(21L, tracker.getFirstGap(15L).getFirst().longValue());
        assertEquals(24L, tracker.getFirstGap(15L).getSecond().longValue());
        assertEquals(21L, tracker.getFirstGap(16L).getFirst().longValue());
        assertEquals(24L, tracker.getFirstGap(16L).getSecond().longValue());
        assertEquals(21L, tracker.getFirstGap(20L).getFirst().longValue());
        assertEquals(24L, tracker.getFirstGap(20L).getSecond().longValue());
        assertEquals(21L, tracker.getFirstGap(21L).getFirst().longValue());
        assertEquals(24L, tracker.getFirstGap(21L).getSecond().longValue());
        assertEquals(21L, tracker.getFirstGap(24L).getFirst().longValue());
        assertEquals(24L, tracker.getFirstGap(24L).getSecond().longValue());

        assertEquals(31L, tracker.getFirstGap(25L).getFirst().longValue());
        assertEquals(34L, tracker.getFirstGap(25L).getSecond().longValue());
        assertEquals(31L, tracker.getFirstGap(26L).getFirst().longValue());
        assertEquals(34L, tracker.getFirstGap(26L).getSecond().longValue());
        assertEquals(31L, tracker.getFirstGap(30L).getFirst().longValue());
        assertEquals(34L, tracker.getFirstGap(30L).getSecond().longValue());
        assertEquals(31L, tracker.getFirstGap(31L).getFirst().longValue());
        assertEquals(34L, tracker.getFirstGap(31L).getSecond().longValue());
        assertEquals(31L, tracker.getFirstGap(34L).getFirst().longValue());
        assertEquals(34L, tracker.getFirstGap(34L).getSecond().longValue());

        assertEquals(41L, tracker.getFirstGap(35L).getFirst().longValue());
        assertEquals(ExportSequenceNumberTracker.INFINITE_SEQNO,
                tracker.getFirstGap(35L).getSecond().longValue());
        assertEquals(41L, tracker.getFirstGap(36L).getFirst().longValue());
        assertEquals(ExportSequenceNumberTracker.INFINITE_SEQNO,
                tracker.getFirstGap(36L).getSecond().longValue());
        assertEquals(41L, tracker.getFirstGap(40L).getFirst().longValue());
        assertEquals(ExportSequenceNumberTracker.INFINITE_SEQNO,
                tracker.getFirstGap(40L).getSecond().longValue());
        assertEquals(41L, tracker.getFirstGap(41L).getFirst().longValue());
        assertEquals(ExportSequenceNumberTracker.INFINITE_SEQNO,
                tracker.getFirstGap(41L).getSecond().longValue());
        assertEquals(41L, tracker.getFirstGap(42L).getFirst().longValue());
        assertEquals(ExportSequenceNumberTracker.INFINITE_SEQNO,
                tracker.getFirstGap(42L).getSecond().longValue());

        // try appending an overlapping value
        boolean failed = false;
        try {
            tracker.append(5L, 5L);
        } catch (AssertionError e) {
            failed = true;
        }
        assertTrue(failed);

        // try appending an overlapping value
        failed = false;
        try {
            tracker.append(40L, 45L);
        } catch (AssertionError e) {
            failed = true;
        }
        assertTrue(failed);

        assertEquals(4, tracker.size());

        truncated = tracker.truncateBefore(7L);
        // state of tracker now = [7, 9] [15, 20] [25, 30] [35, 40]
        assertEquals(7L, tracker.getFirstSeqNo());
        assertEquals(40L, tracker.getLastSeqNo());
        assertEquals(9L, tracker.getSafePoint());
        assertEquals(1L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(6L, tracker.getFirstGap().getSecond().longValue());
        assertEquals(10L, tracker.getFirstGap(7L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(7L).getSecond().longValue());
        assertEquals(4, tracker.size());
        assertEquals(6, truncated);
        assertEquals(21, tracker.sizeInSequence());
        System.out.println("Expected: [7, 9] [15, 20] [25, 30] [35, 40]");
        System.out.println("Actual: " + tracker);

        tracker.truncateAfter(22L);
        // state of tracker now = [7, 9] [15, 20]
        assertEquals(7L, tracker.getFirstSeqNo());
        assertEquals(20L, tracker.getLastSeqNo());
        assertEquals(9L, tracker.getSafePoint());
        assertEquals(1L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(6L, tracker.getFirstGap().getSecond().longValue());
        assertEquals(10L, tracker.getFirstGap(7L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(7L).getSecond().longValue());
        assertEquals(2, tracker.size());
        assertEquals(9, tracker.sizeInSequence());
        System.out.println("Expected: [7, 9] [15, 20]");
        System.out.println("Actual: " + tracker);

        // Truncate inside a gap
        truncated = tracker.truncateBefore(11L);
        // state of tracker now = [15, 20]
        assertEquals(15L, tracker.getFirstSeqNo());
        assertEquals(20L, tracker.getLastSeqNo());
        assertEquals(20L, tracker.getSafePoint());
        assertEquals(1L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap().getSecond().longValue());
        assertEquals(1L, tracker.getFirstGap(10L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(10L).getSecond().longValue());
        assertEquals(1L, tracker.getFirstGap(11L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(11L).getSecond().longValue());
        assertEquals(1L, tracker.getFirstGap(12L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(12L).getSecond().longValue());
        System.out.println("Expected: [15, 20]");
        System.out.println("Actual: " + tracker);
        assertEquals(1, tracker.size());
        assertEquals(3, truncated);
        assertEquals(6, tracker.sizeInSequence());
    }

    @Test
    public void testAddRange() {
        long nonOverlapSize = tracker.addRange(5L, 10L);
        assertEquals(6, nonOverlapSize);
        nonOverlapSize = tracker.addRange(8L, 20L);
        assertEquals(10, nonOverlapSize);
        nonOverlapSize = tracker.addRange(20L, 30L);
        assertEquals(10, nonOverlapSize);
        nonOverlapSize = tracker.addRange(32L, 40L);
        assertEquals(9, nonOverlapSize);
        nonOverlapSize = tracker.addRange(4L, 41L);
        assertEquals(3, nonOverlapSize);
    }

    @Test
    public void testAddNotConnectedRange_ENG_15510() {
        tracker.addRange(5L, 10L);
        tracker.addRange(20L, 30L);
        tracker.addRange(40L, 50L);
        tracker.addRange(8L, 12L);
    }

    @Test
    public void testInitialGap1() {
        // Test corner case of initial gap on first row
        tracker.append(2L, 10L);
        assertEquals(1L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(1L, tracker.getFirstGap().getSecond().longValue());
    }

    @Test
    public void intersectionSizeInSequences() {
        tracker.addRange(200, 299);
        tracker.addRange(500, 549);
        tracker.addRange(730, 799);
        ExportSequenceNumberTracker other = new ExportSequenceNumberTracker();
        assertEquals(0, tracker.intersectionSizeInSequences(other));

        // Add range which doesn't overlap
        other.addRange(1, 199);
        assertEquals(0, tracker.intersectionSizeInSequences(other));

        // Add range which intersects with beginning of range
        other.addRange(200, 219);
        assertEquals(20, tracker.intersectionSizeInSequences(other));

        // Add range which intersects with middle of range
        other.addRange(510, 519);
        assertEquals(30, tracker.intersectionSizeInSequences(other));

        // Add range which intersects with end of range
        other.addRange(750, 900);
        assertEquals(80, tracker.intersectionSizeInSequences(other));

        // Complete intersection
        other.addRange(1, 900);
        assertEquals(220, tracker.intersectionSizeInSequences(other));
    }

    @Test
    public void gapOfEmptyTracker() {
        assertEquals(Pair.of(ExportSequenceNumberTracker.MIN_SEQNO, ExportSequenceNumberTracker.INFINITE_SEQNO),
                tracker.getFirstGap(Long.MAX_VALUE / 2));
    }

    /*
     * Test that that imitating looping over gaps will return null if >= INFINTY is requested
     */
    @Test
    public void getGapsFromNonNormalizedTracker() {
        tracker.addRange(500, 999);

        assertEquals(Pair.of(ExportSequenceNumberTracker.MIN_SEQNO, 499L), tracker.getFirstGap(1));
        assertEquals(Pair.of(1000L, ExportSequenceNumberTracker.INFINITE_SEQNO), tracker.getFirstGap(500));
        assertNull(tracker.getFirstGap(ExportSequenceNumberTracker.INFINITE_SEQNO + 1));
    }
}
