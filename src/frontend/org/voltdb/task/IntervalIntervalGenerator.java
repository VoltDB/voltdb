/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A {@link IntervalGenerator} which tries to execute actions with a fixed amount of time between action start times
 */
public final class IntervalIntervalGenerator extends DurationIntervalGenerator {
    private long m_lastRunNs = System.nanoTime();

    @Override
    public void initialize(TaskHelper helper, int interval, String timeUnit) {
        super.initialize(helper, interval, timeUnit);
        // Introduce some jitter to first start time
        m_lastRunNs = System.nanoTime()
                - ThreadLocalRandom.current().nextLong(Math.min((m_durationNs / 2), TimeUnit.MINUTES.toNanos(5)));
    }

    @Override
    public Interval getFirstInterval() {
        long nextRun = m_lastRunNs + m_durationNs;
        long now = System.nanoTime();
        long interval = nextRun - now;

        if (interval < 0) {
            m_helper.logWarning("Desired execution time is in the past. Resetting interval start to now. "
                    + "Interval might be too short for procedure.");
            m_lastRunNs = now;
            interval = 0;
        } else {
            m_lastRunNs = nextRun;
        }

        return new Interval(interval, TimeUnit.NANOSECONDS, r -> getFirstInterval());
    }
}
