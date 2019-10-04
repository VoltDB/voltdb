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

/**
 * Interface which generates actions to be performed as well as calculating the delay until that should be performed
 * according to a schedule
 */
public interface ActionScheduler extends ActionGeneratorBase {
    /**
     * This method is invoked for the first action to be performed. All subsequent invocation will be of
     * {@link DelayedAction#getCallback()}. After the delay has elapsed and the action has been performed.
     * <p>
     * If this method throws an exception or returns {@code null} the task instance will halted and put into an error
     * state.
     *
     * @return {@link DelayedAction} with the action and delay of that action
     */
    DelayedAction getFirstDelayedAction();
}
