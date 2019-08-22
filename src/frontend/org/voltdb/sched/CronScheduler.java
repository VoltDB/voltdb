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

import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;

import fc.cron.CronExpression;

/**
 * {@link Scheduler} implementation which executes a single procedure with a static set of parameters based upon a cron
 * style configuration schedule
 */
public class CronScheduler extends SingleProcScheduler {
    private final CronExpression m_cronExpression;

    public static String validateParameters(SchedulerValidationHelper helper, String cronConfiguration,
            String procedure, String... procedureParameters) {
        SchedulerValidationErrors errors = new SchedulerValidationErrors();
        SingleProcScheduler.validateParameters(errors, helper, procedure, procedureParameters);
        try {
            new CronExpression(cronConfiguration, true);
        } catch (RuntimeException e) {
            errors.addErrorMessage(e.getMessage());
        }
        return errors.getErrorMessage();
    }

    public CronScheduler(String cronConfiguration, String procedure, String... procedureParameters) {
        super(procedure, procedureParameters);
        m_cronExpression = new CronExpression(cronConfiguration, true);
    }

    @Override
    public String toString() {
        return super.toString() + ' ' + m_cronExpression;
    }

    @Override
    long getNextDelayNs() {
        ZonedDateTime now = ZonedDateTime.now();
        ZonedDateTime barrier = now.plusYears(20);
        ZonedDateTime runAt = m_cronExpression.nextTimeAfter(now, barrier);

        return ChronoUnit.NANOS.between(runAt, now);
    }
}
