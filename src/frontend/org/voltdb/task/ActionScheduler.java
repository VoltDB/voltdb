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
 * Interface which generates actions to be performed as well as calculating the interval that should elapse before the
 * action is performed.
 */
public interface ActionScheduler extends ActionGeneratorBase {
    /**
     * This method is invoked only once to obtain the first action and interval. The action will be performed after the
     * provided time interval has elapsed. After the action has been performed the {@link ScheduledAction#getCallback()
     * callback} will be invoked with the {@link ActionResult result} of the action. The return of the callback will be
     * used as the next action and interval.
     * <p>
     * If this method throws an exception or returns {@code null} the task instance will halted and put into an error
     * state.
     *
     * @return {@link ScheduledAction} with the action and interval after which the action is performed
     */
    ScheduledAction getFirstScheduledAction();
}
