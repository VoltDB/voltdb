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
}
