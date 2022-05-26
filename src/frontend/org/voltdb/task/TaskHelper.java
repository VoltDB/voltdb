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

import org.voltdb.utils.CompoundErrors;

/**
 * Helper interface passed to {@link IntervalGenerator}, {@link ActionGenerator} and {@link ActionScheduler} instances for
 * calling in to the volt system to perform logging, validation and other operations
 */
public interface TaskHelper {

    /**
     * @return The name of the task
     */
    String getTaskName();

    /**
     * @return The scope in which the task will be executing
     */
    TaskScope getTaskScope();

    /**
     * Returns the ID of the scope when this helper is passed to an {@code instantiate} method otherwise {@code -1}
     * <p>
     * If {@code scope} is {@link TaskScope#PARTITIONS} {@code id} will be a partition ID. If {@code scope} is
     * {@link TaskScope#HOSTS} {@code id} will be a host ID. Otherwise {@code id} will be {@code -1}
     *
     * @return The ID of the scope
     */
    int getScopeId();

    /**
     * @return {@code true} if debug logging is enabled
     */
    boolean isDebugLoggingEnabled();

    /**
     * Log a message in the system log at the debug log level
     *
     * @param message to log
     */
    void logDebug(String message);

    /**
     * Log a message and throwable in the system log at the debug log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    void logDebug(String message, Throwable throwable);

    /**
     * Log a message in the system log at the info log level
     *
     * @param message to log
     */
    void logInfo(String message);

    /**
     * Log a message and throwable in the system log at the info log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    void logInfo(String message, Throwable throwable);

    /**
     * Log a message in the system log at the warning log level
     *
     * @param message to log
     */
    void logWarning(String message);

    /**
     * Log a message and throwable in the system log at the warning log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    void logWarning(String message, Throwable throwable);

    /**
     * Log a message in the system log at the error log level
     *
     * @param message to log
     */
    void logError(String message);

    /**
     * Log a message and throwable in the system log at the error log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    void logError(String message, Throwable throwable);

    /**
     * Validate that a procedure with {@code name} exists and {@code parameters} are valid for that procedure.
     * <p>
     * Note: parameter validation might not work for system procedures
     *
     * @param errors                   {@link CompoundErrors} instance to collect errors
     * @param restrictProcedureByScope If true type of procedures will be restricted. See
     *                                 {@link ActionScheduler#restrictProcedureByScope()}
     * @param procedureName            Name of procedure to validate
     * @param parameters               that will be passed to {@code name}
     */
    void validateProcedure(CompoundErrors errors, boolean restrictProcedureByScope, String procedureName,
            Object[] parameters);

    /**
     * Test if a procedure is read only. If a procedure cannot be found with {@code procedureName} then {@code false} is
     * returned
     *
     * @param procedureName Name of procedure.
     * @return {@code true} if {@code procedureName} is read only
     */
    boolean isProcedureReadOnly(String procedureName);

}