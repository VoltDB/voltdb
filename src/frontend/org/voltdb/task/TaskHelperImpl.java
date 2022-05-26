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

import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientInterface;
import org.voltdb.ParameterConverter;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.CompoundErrors;

/**
 * Helper class passed to {@link IntervalGenerator}, {@link ActionGenerator} and {@link ActionScheduler} instances for
 * calling in to the volt system to perform logging, validation and other operations
 */
final class TaskHelperImpl implements TaskHelper {
    private final VoltLogger m_logger;
    private final UnaryOperator<String> m_generateLogMessage;
    private final String m_taskName;
    private final TaskScope m_scope;
    private final int m_scopeId;
    private final Function<String, Procedure> m_procedureGetter;

    TaskHelperImpl(VoltLogger logger, UnaryOperator<String> generateLogMessage, String taskName, TaskScope scope,
            Function<String, Procedure> procedureMapper) {
        this(logger, generateLogMessage, taskName, scope, -1, procedureMapper);
    }

    TaskHelperImpl(VoltLogger logger, UnaryOperator<String> generateLogMessage, String taskName, TaskScope scope,
            int scopeId, ClientInterface clientInterface) {
        this(logger, generateLogMessage, taskName, scope, scopeId, clientInterface::getProcedureFromName);
    }

    private TaskHelperImpl(VoltLogger logger, UnaryOperator<String> generateLogMessage, String taskName, TaskScope scope,
            int scopeId, Function<String, Procedure> procedureGetter) {
        m_logger = logger;
        m_generateLogMessage = generateLogMessage;
        m_taskName = taskName;
        m_scope = scope;
        m_scopeId = scopeId;
        m_procedureGetter = procedureGetter;
    }

    /**
     * @return The name of the task
     */
    @Override
    public String getTaskName() {
        return m_taskName;
    }

    /**
     * @return The scope in which the task will be executing
     */
    @Override
    public TaskScope getTaskScope() {
        return m_scope;
    }

    /**
     * Returns the ID of the scope when this helper is passed to an {@code instantiate} method otherwise {@code -1}
     * <p>
     * If {@code scope} is {@link TaskScope#PARTITIONS} {@code id} will be a partition ID. If {@code scope} is
     * {@link TaskScope#HOSTS} {@code id} will be a host ID. Otherwise {@code id} will be {@code -1}
     *
     * @return The ID of the scope
     */
    @Override
    public int getScopeId() {
        return m_scopeId;
    }

    /**
     * @return {@code true} if debug logging is enabled
     */
    @Override
    public boolean isDebugLoggingEnabled() {
        return m_logger.isDebugEnabled();
    }

    /**
     * Log a message in the system log at the debug log level
     *
     * @param message to log
     */
    @Override
    public void logDebug(String message) {
        logDebug(message, null);
    }

    /**
     * Log a message and throwable in the system log at the debug log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    @Override
    public void logDebug(String message, Throwable throwable) {
        m_logger.debug(generateLogMessage(message), throwable);
    }

    /**
     * Log a message in the system log at the info log level
     *
     * @param message to log
     */
    @Override
    public void logInfo(String message) {
        logInfo(message, null);
    }

    /**
     * Log a message and throwable in the system log at the info log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    @Override
    public void logInfo(String message, Throwable throwable) {
        m_logger.info(generateLogMessage(message), throwable);
    }

    /**
     * Log a message in the system log at the warning log level
     *
     * @param message to log
     */
    @Override
    public void logWarning(String message) {
        logWarning(message, null);
    }

    /**
     * Log a message and throwable in the system log at the warning log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    @Override
    public void logWarning(String message, Throwable throwable) {
        m_logger.warn(generateLogMessage(message), throwable);
    }

    /**
     * Log a message in the system log at the error log level
     *
     * @param message to log
     */
    @Override
    public void logError(String message) {
        logError(message, null);
    }

    /**
     * Log a message and throwable in the system log at the error log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    @Override
    public void logError(String message, Throwable throwable) {
        m_logger.error(generateLogMessage(message), throwable);
    }

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
    @Override
    public void validateProcedure(CompoundErrors errors, boolean restrictProcedureByScope,
            String procedureName, Object[] parameters) {
        if (m_procedureGetter == null) {
            return;
        }
        Procedure procedure = m_procedureGetter.apply(procedureName);
        if (procedure == null) {
            errors.addErrorMessage("Procedure does not exist: " + procedureName);
            return;
        }

        if (procedure.getSystemproc()) {
            // System procedures do not have parameter types in the procedure definition
            return;
        }

        String error = TaskManager.isProcedureValidForScope(m_scope, procedure, restrictProcedureByScope);
        if (error != null) {
            errors.addErrorMessage(error);
            return;
        }

        CatalogMap<ProcParameter> parameterTypes = procedure.getParameters();

        if (parameterTypes.size() != parameters.length) {
            errors.addErrorMessage(String.format("Procedure %s takes %d parameters but %d were given", procedureName,
                    procedure.getParameters().size(), parameters.length));
            return;
        }

        for (ProcParameter pp : parameterTypes) {
            Class<?> parameterClass = VoltType.classFromByteValue((byte) pp.getType());
            try {
                ParameterConverter.tryToMakeCompatible(parameterClass, parameters[pp.getIndex()]);
            } catch (Exception e) {
                errors.addErrorMessage(
                        String.format("Could not convert parameter %d with the value \"%s\" to type %s: %s",
                                pp.getIndex(), parameters[pp.getIndex()], parameterClass.getName(), e.getMessage()));
            }
        }
    }

    /**
     * Test if a procedure is read only. If a procedure cannot be found with {@code procedureName} then {@code false} is
     * returned
     *
     * @param procedureName Name of procedure.
     * @return {@code true} if {@code procedureName} is read only
     */
    @Override
    public boolean isProcedureReadOnly(String procedureName) {
        if (m_procedureGetter == null) {
            return false;
        }
        Procedure procedure = m_procedureGetter.apply(procedureName);
        return procedure == null ? false : procedure.getReadonly();
    }

    private String generateLogMessage(String body) {
        return m_generateLogMessage.apply(body);
    }
}
