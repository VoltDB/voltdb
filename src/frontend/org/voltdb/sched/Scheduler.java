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

import java.util.Collection;

/**
 * Interface used to create a custom scheduler. All parameters passed to a Scheduler instance are done through the
 * Scheduler's constructor. Only valid column types are allowed as constructor parameters with one exception the last
 * parameter can be either {@code String[]} or {@code Object[]}. If the last parameter is an array then it will be
 * treated as a var args parameter.
 */
public interface Scheduler {
    /**
     * Process the result of the previous run if there was a previous run. Then return a {@link SchedulerResult} which
     * indicates the next procedure to run or that the scheduler should exit because of failure or completed lifecycle.
     *
     * @param previousProcedureRun {@link ScheduledProcedure} of last procedure executed or {@code null} if this is the
     *                             first call to this scheduler
     * @return {@link SchedulerResult} with the result from this scheduler
     */
    SchedulerResult nextRun(ScheduledProcedure previousProcedureRun);

    /**
     * If this method is implemented then the scheduler will only be restarted when it or any classes marked as a
     * dependency are modified. However if this method is not implemented then the Scheduler instance will be restarted
     * any time any class is modified.
     *
     * @return {@link Collection} of {@code classesNames} which this instance depends upon.
     */
    default Collection<String> getDependencies() {
        return null;
    }
}
