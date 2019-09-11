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

import java.util.function.Function;
import java.util.function.UnaryOperator;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientInterface;
import org.voltdb.DefaultProcedureManager;
import org.voltdb.InvocationDispatcher;
import org.voltdb.ParameterConverter;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

/**
 * Helper class passed to {@link ActionSchedule}, {@link ActionGenerator} and {@link ActionScheduler} instances for
 * calling in to the volt system to perform logging, validation and other operations
 */
public final class TaskHelper {
    private final VoltLogger m_logger;
    private final UnaryOperator<String> m_generateLogMessage;
    private final TaskScope m_scope;
    private final Function<String, Procedure> m_procedureGetter;

    private static Function<String, Procedure> createProcedureFunction(Database database) {
        if (database == null) {
            return null;
        }
        DefaultProcedureManager defaultProcedureManager = new DefaultProcedureManager(database);
        CatalogMap<Procedure> procedures = database.getProcedures();
        return p -> InvocationDispatcher.getProcedureFromName(p, procedures, defaultProcedureManager);
    }

    TaskHelper(VoltLogger logger, UnaryOperator<String> generateLogMessage, TaskScope scope, Database database) {
        this(logger, generateLogMessage, scope, createProcedureFunction(database));
    }

    TaskHelper(VoltLogger logger, UnaryOperator<String> generateLogMessage, TaskScope scope,
            ClientInterface clientInterface) {
        this(logger, generateLogMessage, scope, clientInterface::getProcedureFromName);
    }

    private TaskHelper(VoltLogger logger, UnaryOperator<String> generateLogMessage, TaskScope scope,
            Function<String, Procedure> procedureGetter) {
        m_logger = logger;
        m_generateLogMessage = generateLogMessage;
        m_scope = scope;
        m_procedureGetter = procedureGetter;
    }

    /**
     * @return {@code true} if debug logging is enabled
     */
    public boolean isDebugLoggingEnabled() {
        return m_logger.isDebugEnabled();
    }

    /**
     * Log a message in the system log at the debug log level
     *
     * @param message to log
     */
    public void logDebug(String message) {
        logDebug(message, null);
    }

    /**
     * Log a message and throwable in the system log at the debug log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    public void logDebug(String message, Throwable throwable) {
        m_logger.debug(generateLogMessage(message), throwable);
    }

    /**
     * Log a message in the system log at the info log level
     *
     * @param message to log
     */
    public void logInfo(String message) {
        logInfo(message, null);
    }

    /**
     * Log a message and throwable in the system log at the info log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    public void logInfo(String message, Throwable throwable) {
        m_logger.info(generateLogMessage(message), throwable);
    }

    /**
     * Log a message in the system log at the warning log level
     *
     * @param message to log
     */
    public void logWarning(String message) {
        logWarning(message, null);
    }

    /**
     * Log a message and throwable in the system log at the warning log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    public void logWarning(String message, Throwable throwable) {
        m_logger.warn(generateLogMessage(message), throwable);
    }

    /**
     * Log a message in the system log at the error log level
     *
     * @param message to log
     */
    public void logError(String message) {
        logError(message, null);
    }

    /**
     * Log a message and throwable in the system log at the error log level
     *
     * @param message   to log
     * @param throwable to log along with {@code message}
     */
    public void logError(String message, Throwable throwable) {
        m_logger.error(generateLogMessage(message), throwable);
    }

    /**
     * Validate that a procedure with {@code name} exists and {@code parameters} are valid for that procedure.
     * <p>
     * Note: parameter validation might not work for system procedures
     *
     * @param errors                   {@link TaskValidationErrors} instance to collect errors
     * @param restrictProcedureByScope If true type of procedures will be restricted. See
     *                                 {@link ActionScheduler#restrictProcedureByScope()}
     * @param procedureName            Name of procedure to validate
     * @param parameters               that will be passed to {@code name}
     */
    public void validateProcedure(TaskValidationErrors errors, boolean restrictProcedureByScope,
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

        if (restrictProcedureByScope) {
            String error = TaskManager.isProcedureValidForScope(m_scope, procedure);
            if (error != null) {
                errors.addErrorMessage(error);
                return;
            }
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

    private String generateLogMessage(String body) {
        return m_generateLogMessage.apply(body);
    }
}
