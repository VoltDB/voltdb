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

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;

import fc.cron.CronExpression;

/**
 * {@link IntervalGenerator} based upon a cron style configuration schedule
 */
public final class CronIntervalGenerator implements IntervalGenerator {
    private CronExpression m_cronExpression;

    public static String validateParameters(String cronConfiguration) {
        try {
            new CronExpression(cronConfiguration, true);
        } catch (RuntimeException e) {
            return e.getMessage();
        }
        return null;
    }

    public void initialize(String cronConfiguration) {
        m_cronExpression = new CronExpression(cronConfiguration, true);
    }

    @Override
    public String toString() {
        return super.toString() + ' ' + m_cronExpression;
    }

    @Override
    public Interval getFirstInterval() {
        return new Interval(getIntervalNs(), TimeUnit.NANOSECONDS, r -> getFirstInterval());
    }

    private long getIntervalNs() {
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime barrier = now.plusYears(20);
        ZonedDateTime runAt = m_cronExpression.nextTimeAfter(now, barrier);

        return ChronoUnit.NANOS.between(now, runAt);
    }
}
