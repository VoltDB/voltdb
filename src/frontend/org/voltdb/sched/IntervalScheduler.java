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

package org.voltdb.sched;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A {@link Scheduler} which tries to execute a procedure with a fixed amount of time between procedure start times
 */
public class IntervalScheduler extends SingleProcScheduler {
    private long m_intervalNs;
    private long m_lastRunNs = System.nanoTime();

    public static String validateParameters(SchedulerHelper helper, int interval, String timeUnit,
            String procedure, String... procedureParameters) {
        SchedulerValidationErrors errors = new SchedulerValidationErrors();
        SingleProcScheduler.validateParameters(errors, helper, procedure, procedureParameters);
        if (interval <= 0) {
            errors.addErrorMessage("Interval must be greater than 0: " + interval);
        }
        switch (timeUnit) {
        default:
            errors.addErrorMessage("Unsupported time unit: " + timeUnit);
            break;
        case "MILLISECONDS":
        case "SECONDS":
        case "MINUTES":
        case "HOURS":
        case "DAYS":
            break;
        }
        return errors.getErrorMessage();
    }

    public void initialize(SchedulerHelper helper, int interval, String timeUnit, String procedure,
            String[] procedureParameters) {
        super.initialize(helper, procedure, procedureParameters);
        m_intervalNs = TimeUnit.valueOf(timeUnit).toNanos(interval);
        // Introduce some jitter to first start time
        m_lastRunNs = System.nanoTime()
                - ThreadLocalRandom.current().nextLong(Math.min((m_intervalNs / 2), TimeUnit.MINUTES.toNanos(5)));
    }

    @Override
    long getNextDelayNs() {
        long nextRun = m_lastRunNs + m_intervalNs;
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
