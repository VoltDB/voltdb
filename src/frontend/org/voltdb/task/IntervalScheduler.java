/* This file is part of VoltDB.
 * Copyright (C) 2019 VoltDB Inc.
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
 * A {@link Scheduler} which tries to execute a procedure with a fixed amount of time between procedure start times
 */
public class IntervalScheduler extends DurationScheduler {
    private long m_lastRunNs = System.nanoTime();

    @Override
    public void initialize(TaskHelper helper, int interval, String timeUnit, String procedure,
            String[] procedureParameters) {
        super.initialize(helper, interval, timeUnit, procedure, procedureParameters);
        // Introduce some jitter to first start time
        m_lastRunNs = System.nanoTime()
                - ThreadLocalRandom.current().nextLong(Math.min((m_durationNs / 2), TimeUnit.MINUTES.toNanos(5)));
    }

    @Override
    long getNextDelayNs() {
        long nextRun = m_lastRunNs + m_durationNs;
        long now = System.nanoTime();
        long delay = nextRun - now;

        if (delay < 0) {
            m_helper.logWarning(
                    "Desired execution time is in the past. Resetting interval start to now. Interval might be too short for procedure.");
            m_lastRunNs = now;
            return 0;
        }
        m_lastRunNs = nextRun;
        return delay;
    }
}
