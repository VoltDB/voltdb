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
 * Interface which generates actions to be be performed as part of a task on a schedule
 */
public interface ActionGenerator extends ActionGeneratorBase {
    /**
     * This method is invoked only once to obtain the first action. The first action will be performed after a time
     * interval has elapsed. After the action has been performed {@link Action#getCallback() callback} will be invoked
     * with the {@link ActionResult result} of this action. The return of the callback will be used as the next action.
     * <p>
     * If this method throws an exception or returns {@code null} the task instance will halted and put into an error
     * state.
     *
     * @return {@link Action} to perform
     */
    Action getFirstAction();
}
