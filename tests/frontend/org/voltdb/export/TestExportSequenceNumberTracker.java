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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestExportSequenceNumberTracker {

    private ExportSequenceNumberTracker tracker;

    @Before
    public void setUp() throws IOException {
        tracker = new ExportSequenceNumberTracker();
    }

    @After
    public void tearDown() throws InterruptedException {
    }

    @Test
    public void testBasic() throws Exception {
        // Append single to range
        tracker.append(1L, 1L);
        assertEquals(1L, tracker.getFirstSeqNo());
        assertEquals(1L, tracker.getLastSeqNo());
        assertEquals(1L, tracker.getSafePoint());

        int truncated = tracker.truncate(1L);
        assertEquals(1L, tracker.getFirstSeqNo());
        assertEquals(1L, tracker.getLastSeqNo());
        assertEquals(1L, tracker.getSafePoint());
        assertEquals(2L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(ExportSequenceNumberTracker.INFINITE_SEQNO,
                tracker.getFirstGap().getSecond().longValue());
        assertEquals(1, truncated);

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

        truncated = tracker.truncate(7L);
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
        assertEquals(20, tracker.sizeInSequence());

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
        assertEquals(8, tracker.sizeInSequence());

        // Truncate inside a gap
        truncated = tracker.truncate(11L);
        // state of tracker now = [11, 11] [15, 20]
        assertEquals(11L, tracker.getFirstSeqNo());
        assertEquals(20L, tracker.getLastSeqNo());
        assertEquals(11L, tracker.getSafePoint());
        assertEquals(1L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(10L, tracker.getFirstGap().getSecond().longValue());
        assertEquals(1L, tracker.getFirstGap(10L).getFirst().longValue());
        assertEquals(10L, tracker.getFirstGap(10L).getSecond().longValue());
        assertEquals(12L, tracker.getFirstGap(11L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(11L).getSecond().longValue());
        assertEquals(12L, tracker.getFirstGap(12L).getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap(12L).getSecond().longValue());
        assertEquals(2, tracker.size());
        assertEquals(2, truncated);
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
}
