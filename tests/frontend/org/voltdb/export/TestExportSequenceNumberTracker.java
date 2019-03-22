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
        assertEquals(null, tracker.getFirstGap());
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
        assertEquals(7L, tracker.getFirstSeqNo());
        assertEquals(40L, tracker.getLastSeqNo());
        assertEquals(9L, tracker.getSafePoint());
        assertEquals(10L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap().getSecond().longValue());
        assertEquals(4, tracker.size());
        assertEquals(6, truncated);
        assertEquals(20, tracker.sizeInSequence());

        tracker.truncateAfter(22L);
        assertEquals(7L, tracker.getFirstSeqNo());
        assertEquals(20L, tracker.getLastSeqNo());
        assertEquals(9L, tracker.getSafePoint());
        assertEquals(10L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap().getSecond().longValue());
        assertEquals(2, tracker.size());
        assertEquals(8, tracker.sizeInSequence());

        // Truncate inside a gap
        truncated = tracker.truncate(11L);
        assertEquals(11L, tracker.getFirstSeqNo());
        assertEquals(20L, tracker.getLastSeqNo());
        assertEquals(11L, tracker.getSafePoint());
        assertEquals(12L, tracker.getFirstGap().getFirst().longValue());
        assertEquals(14L, tracker.getFirstGap().getSecond().longValue());
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

}
