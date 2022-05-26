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

/**
 * {@link IntervalGenerator} implementation which executes actions separated by a fixed delay
 */
public final class DelayIntervalGenerator extends DurationIntervalGenerator {
    private Interval m_delay;

    @Override
    public void initialize(TaskHelper helper, int interval, String timeUnit) {
        super.initialize(helper, interval, timeUnit);
        m_delay = new Interval(m_durationNs, TimeUnit.NANOSECONDS, r -> m_delay);
    }

    @Override
    public Interval getFirstInterval() {
        return m_delay;
    }
}
