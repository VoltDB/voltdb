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

import java.util.concurrent.TimeUnit;

import org.voltdb.utils.CompoundErrors;

/**
 * Simple abstract class to handle the common duration parsing and validation of schedulers which use a static duration
 * in the form of &lt;interval&gt; &lt;timeUnit&gt;
 */
abstract class DurationIntervalGenerator implements IntervalGenerator {
    TaskHelper m_helper;
    long m_durationNs = -1;

    public static String validateParameters(TaskHelper helper, int interval, String timeUnit) {
        CompoundErrors errors = new CompoundErrors();
        if (interval <= 0) {
            errors.addErrorMessage("Interval must be greater than 0: " + interval);
        }
        switch (timeUnit) {
        default:
            errors.addErrorMessage("Unsupported time unit: " + timeUnit
                    + ". Must be one of MILLISECONDS, SECONDS, MINUTES, HOURS or DAYS");
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

    void initialize(TaskHelper helper, int interval, String timeUnit) {
        m_helper = helper;
        m_durationNs = TimeUnit.valueOf(timeUnit).toNanos(interval);
    }
}
