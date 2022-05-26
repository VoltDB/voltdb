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

/**
 * Interface which calculates the time interval which should elapse until the next action should be performed
 */
public interface IntervalGenerator extends Initializable {
    /**
     * This method is invoked only once to obtain the first interval. The first action will be performed after this time
     * interval has elapsed. After the action has been performed {@link Interval#getCallback() callback} will be invoked
     * with the {@link ActionResult result} of the action. The return of the callback will be used as the next interval.
     * <p>
     * If this method throws an exception or returns {@code null} the task instance will halted and put into an error
     * state.
     *
     * @return {@link Interval} with the time interval until the next action should be performed and callback to invoke
     */
    Interval getFirstInterval();
}
