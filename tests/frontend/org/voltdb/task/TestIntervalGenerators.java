/* This file is part of VoltDB.
 * Copyright (C) 2021 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
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
