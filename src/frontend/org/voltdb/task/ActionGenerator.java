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
 * Interface which generates actions to be be performed as part of a task on a schedule
 */
public interface ActionGenerator extends Initializable {
    /**
     * This method is invoked for the first action to be performed. All subsequent invocation will be of
     * {@link Action#getCallback()}
     * <p>
     * If this method throws an exception or returns {@code null} the task instance will halted and put into an error
     * state.
     *
     * @return {@link Action} to perform
     */
    Action getFirstAction();

    /**
     * If this method returns {@code true} that means the type of procedure which this scheduler can run is restricted
     * based upon the scope type.
     * <ul>
     * <li>SYSTEM - Cannot run single partition procedures</li>
     * <li>HOSTS - Only NT procedures are allowed</li>
     * <li>PARTITIONS - Only partitioned procedures are allowed</li>
     * </ul>
     * <p>
     * Default return is {@code false}
     *
     * @return {@code true} if the type of procedure this scheduler can run should be restricted based upon scope
     */
    default boolean restrictProcedureByScope() {
        return false;
    }
}
