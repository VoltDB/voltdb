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

/**
 * {@link ActionSchedule} implementation which executes actions at fixed delay between execution times
 */
public final class DelaySchedule extends DurationSchedule {
    private ActionDelay m_delay;

    @Override
    public void initialize(TaskHelper helper, int interval, String timeUnit) {
        super.initialize(helper, interval, timeUnit);
        m_delay = new ActionDelay(m_durationNs, TimeUnit.NANOSECONDS, r -> m_delay);
    }

    @Override
    public ActionDelay getFirstDelay() {
        return m_delay;
    }
}
