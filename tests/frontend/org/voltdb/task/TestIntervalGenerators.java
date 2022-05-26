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
package org.voltdb.task;

import static org.junit.Assert.assertTrue;

import java.util.concurrent.TimeUnit;

import org.junit.Test;

public class TestIntervalGenerators {
    @Test
    public void cron() throws Exception {
        CronIntervalGenerator cron = new CronIntervalGenerator();
        cron.initialize("* * * * * *");

        // Cron schedule is for every second so it should be at most a second in the future so check it is 1ms
        Interval interval = cron.getFirstInterval();
        long intervalNs = interval.getInterval(TimeUnit.NANOSECONDS);
        assertTrue(intervalNs > TimeUnit.MILLISECONDS.toNanos(1));
        assertTrue(intervalNs <= TimeUnit.SECONDS.toNanos(1));

        interval = interval.getCallback().apply(null);
        intervalNs = interval.getInterval(TimeUnit.NANOSECONDS);
        assertTrue(intervalNs > TimeUnit.MILLISECONDS.toNanos(1));
        assertTrue(intervalNs <= TimeUnit.SECONDS.toNanos(1));
    }

    @Test
    public void interval() throws Exception {
        IntervalIntervalGenerator intervalGenerator = new IntervalIntervalGenerator();
        intervalGenerator.initialize(null, 5, TimeUnit.MINUTES.name());

        Interval interval = intervalGenerator.getFirstInterval();
        long firstIntervalNs = interval.getInterval(TimeUnit.NANOSECONDS);
        // First scheduled interval will has jitter randomly generated so this is 1/2 actual interval
        assertTrue(firstIntervalNs > TimeUnit.SECONDS.toNanos(148));
        assertTrue(firstIntervalNs <= TimeUnit.MINUTES.toNanos(5));

        interval = interval.getCallback().apply(null);
        long intervalNs = interval.getInterval(TimeUnit.NANOSECONDS);
        assertTrue(intervalNs > firstIntervalNs + TimeUnit.MINUTES.toNanos(5) - TimeUnit.SECONDS.toNanos(2));
        assertTrue(intervalNs < firstIntervalNs + TimeUnit.MINUTES.toNanos(5) + TimeUnit.SECONDS.toNanos(2));
    }

    @Test
    public void delay() throws Exception {
        DelayIntervalGenerator delay = new DelayIntervalGenerator();
        delay.initialize(null, 30, TimeUnit.SECONDS.name());

        Interval interval = delay.getFirstInterval();
        long intervalNs = interval.getInterval(TimeUnit.NANOSECONDS);
        assertTrue(intervalNs > TimeUnit.SECONDS.toNanos(29));
        assertTrue(intervalNs < TimeUnit.SECONDS.toNanos(31));

        interval = interval.getCallback().apply(null);
        intervalNs = interval.getInterval(TimeUnit.NANOSECONDS);
        assertTrue(intervalNs > TimeUnit.SECONDS.toNanos(29));
        assertTrue(intervalNs < TimeUnit.SECONDS.toNanos(31));
    }
}
