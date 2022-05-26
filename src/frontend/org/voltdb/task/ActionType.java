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
 * Enum used to describe the type of the {@link Action}.
 */
public enum ActionType {

    /** Schedule a procedure to be executed */
    PROCEDURE(false),
    /** Schedule the provided callback to be invoked */
    CALLBACK(false),
    /** Unexpected error occurred within the scheduler and additional procedures will not be scheduled */
    ERROR(true),
    /** Scheduler has reached an end to its life cycle and is not scheduling any more procedures */
    EXIT(true);

    private final boolean m_stop;

    private ActionType(boolean stop) {
        m_stop = stop;
    }

    /**
     * @return {@code true} if the action is a stop action
     */
    public boolean isStop() {
        return m_stop;
    }
}
