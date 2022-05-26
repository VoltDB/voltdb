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
import java.util.function.Function;

/**
 * Class to represent the time interval which should elapse before an action is performed. Includes an interval and a
 * callback to retrieve the next interval.
 */
public class Interval {
    private final long m_intervalNs;
    private final Function<ActionResult, Interval> m_callback;

    public Interval(long interval, TimeUnit timeUnit, Function<ActionResult, Interval> m_callback) {
        this.m_intervalNs = timeUnit.toNanos(interval);
        this.m_callback = m_callback;
    }

    /**
     * @param timeUnit Of the returned interval
     * @return The time interval until the action should be performed
     */
    public long getInterval(TimeUnit timeUnit) {
        return timeUnit == TimeUnit.NANOSECONDS ? m_intervalNs : timeUnit.convert(m_intervalNs, TimeUnit.NANOSECONDS);
    }

    /**
     * @return {@code callback} which should be invoked after the action is performed to get next interval
     */
    public Function<ActionResult, Interval> getCallback() {
        return m_callback;
    }

    @Override
    public String toString() {
        return "Interval [intervalNs=" + m_intervalNs + ", callback=" + m_callback + "]";
    }
}
