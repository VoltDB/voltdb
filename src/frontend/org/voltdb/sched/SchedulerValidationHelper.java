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

/**
 * Helper class for calling in to the volt system to validate parameters being passed to a {@link Scheduler}
 */
public interface SchedulerValidationHelper {
    /**
     * Validate that a procedure with {@code name} exists and {@code parameters} are valid for that procedure.
     * <p>
     * Note: parameter validation might not work for system procedures
     *
     * @param errors                   {@link SchedulerValidationErrors} instance to collect errors
     * @param restrictProcedureByScope If true type of procedures will be restricted. See
     *                                 {@link Scheduler#restrictProcedureByScope()}
     * @param name                     Name of procedure to validate
     * @param params                   that will be passed to {@code name}
     */
    void validateProcedureAndParams(SchedulerValidationErrors errors, boolean restrictProcedureByScope, String name,
            Object[] params);
}
