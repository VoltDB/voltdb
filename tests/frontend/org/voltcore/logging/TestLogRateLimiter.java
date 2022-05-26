/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

package org.voltcore.logging;

import org.voltcore.utils.EstTime;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest(EstTime.class)
public class TestLogRateLimiter {

    long tick = 10000; // the dawn of time

    void advanceTime(long n) {
        tick += n;
        System.out.printf("    It is %d o'tick and all's well\n", tick);
    }

    long currTime() {
        return tick;
    }

    @Before
    public void setup() {
        PowerMockito.mockStatic(EstTime.class);
        when(EstTime.currentTimeMillis()).thenAnswer(new Answer<Long>() {
                @Override
                public Long answer(InvocationOnMock inv) {
                    return currTime();
                }
            });
    }

    // Tests that rate-limiting actually works
    @Test
    public void testLimiting() throws Exception {

        // Condition for logging is that
        //  time now - time last logged > suppression interval
        LogRateLimiter rl = new LogRateLimiter();

        // Time 10001. All should log, first time.
        advanceTime(1);
        for (int i=1; i<=3; i++) {
            String s = "S" + i;
            assertTrue(s, rl.shouldLog(s, i));
        }

        // Time 10002. None should log
        advanceTime(1);
        for (int i=1; i<=3; i++) {
            String s = "S" + i;
            assertFalse(s, rl.shouldLog(s, i)); // 10002-10001 <= i for all i
        }

        // Time 10004. S1 and S2 should log.
        advanceTime(2);
        assertTrue("S1", rl.shouldLog("S1", 1));  // 10004-10001 > 1
        assertTrue("S2", rl.shouldLog("S2", 2));  // 10004-10001 > 2
        assertFalse("S3", rl.shouldLog("S3", 3)); // 10004-10001 <= 3

        // Time 10006. S1 and S3 should log.
        advanceTime(2);
        assertTrue("S1", rl.shouldLog("S1", 1));  // 10006-10004 > 1
        assertFalse("S2", rl.shouldLog("S2", 2)); // 10006-10004 <= 2
        assertTrue("S3", rl.shouldLog("S3", 3));  // 10006-10004 > 3

        // Time 10010.  All should log.
        advanceTime(4);
        for (int i=1; i<=3; i++) {
            String s = "S" + i;
            assertTrue(s, rl.shouldLog(s, i)); // 10010-10006 > i for all i
        }
    }

    // Ensure we're limiting the cache size
    @Test
    public void testEviction() throws Exception {

        LogRateLimiter rl = new LogRateLimiter();
        final int max = LogRateLimiter.MAXSIZE;
        final int more = 4;

        // Load cache to the max
        // LRU = X1
        for (int i=1; i<=max; i++) {
            String x = "X" + i;
            assertTrue(x, rl.shouldLog(x, i));
        }

        // First entries not yet due
        for (int i=1; i<=more; i++) {
            String x = "X" + i;
            assertFalse(x, rl.shouldLog(x, i));
        }

        // Add 4 more, pushing first entries out
        // LRU = X5
        for (int i=max+1; i<=max+more; i++) {
            String x = "X" + i;
            assertTrue(x, rl.shouldLog(x, 100));
        }

        // First 4 entries gone, so now new, and thus due
        // LRU = X9
        for (int i=1; i<=more; i++) {
            String x = "X" + i;
            assertTrue(x, rl.shouldLog(x, 100));
        }
    }
}
