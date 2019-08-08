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

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.concurrent.TimeUnit;

/**
 * {@link Scheduler} implementation which executes a single procedure with a static set of parameters with a fixed delay
 * between execution times. Delay can either be a number of seconds or {@link Duration} string representation
 */
public class DelayScheduler extends SingleProcScheduler {
    private final long m_delayNs;

    public static String validateParameters(SchedulerValidationHelper helper, String name, String errorHandler,
            String delay, String procedure, String... procedureParameters) {
        SchedulerValidationErrors errors = new SchedulerValidationErrors();
        SingleProcScheduler.validateParameters(errors, helper, errorHandler, procedure, procedureParameters);
        try {
            parseDelay(delay);
        } catch (RuntimeException e) {
            errors.addErrorMessage(e.getMessage());
        }
        return errors.getErrorMessage();
    }

    private static long parseDelay(String delay) {
        try {
            return TimeUnit.SECONDS.toNanos(Integer.parseInt(delay));
        } catch (NumberFormatException e) {
        }

        try {
            Duration duration = Duration.parse(delay);
            return TimeUnit.SECONDS.toNanos(duration.getSeconds()) + duration.getNano();
        } catch (DateTimeParseException e) {
        }

        throw new IllegalArgumentException(
                "Could not parse <" + delay + "> as either an integer or " + Duration.class.getName());
    }

    public DelayScheduler(String name, String errorHandler, String delay, String procedure,
            String[] procedureParameters) {
        super(name, errorHandler, procedure, procedureParameters);
        m_delayNs = parseDelay(delay);
    }

    @Override
    long getNextDelayNs() {
        return m_delayNs;
    }
}
