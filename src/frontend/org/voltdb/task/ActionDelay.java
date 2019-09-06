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

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Class to represent the delay before an action is performed. Includes a delay and a callback to retrieve the next
 * delay.
 */
public class ActionDelay {
    private final long m_delayNs;
    private final Function<ActionResult, ActionDelay> m_callback;

    public ActionDelay(long delay, TimeUnit timeUnit, Function<ActionResult, ActionDelay> m_callback) {
        this.m_delayNs = timeUnit.toNanos(delay);
        this.m_callback = m_callback;
    }

    /**
     * @param timeUnit Of the returned delay
     * @return The delay until the action should be performed
     */
    public long getDelay(TimeUnit timeUnit) {
        return timeUnit == TimeUnit.NANOSECONDS ? m_delayNs : timeUnit.convert(m_delayNs, TimeUnit.NANOSECONDS);
    }

    /**
     * @return {@code callback} which should be invoked after the action is performed to get next delay
     */
    public Function<ActionResult, ActionDelay> getCallback() {
        return m_callback;
    }

    @Override
    public String toString() {
        return "ActionDelay [delayNs=" + m_delayNs + ", callback=" + m_callback + "]";
    }
}
