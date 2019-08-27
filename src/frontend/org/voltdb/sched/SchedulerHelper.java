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

import java.util.function.UnaryOperator;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientInterface;
import org.voltdb.ParameterConverter;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;

/**
 * Helper class passed to {@link Scheduler} instances for calling in to the volt system to perform logging, validation
 * and other operations
 */
public final class SchedulerHelper {
    private static final VoltLogger log = new VoltLogger("SCHEDULE");

    private final UnaryOperator<String> m_generateLogMessage;
    private final String m_scope;
    private final ClientInterface m_clientInterface;

    SchedulerHelper(UnaryOperator<String> generateLogMessage, String scope, ClientInterface clientInterface) {
        m_generateLogMessage = generateLogMessage;
        m_scope = scope;
        m_clientInterface = clientInterface;
    }

    /**
     * @return {@code true} if debug logging is enabled
     */
    public boolean isDebugLoggingEnabled() {
        return log.isDebugEnabled();
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
        log.debug(generateLogMessage(message), throwable);
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
        log.info(generateLogMessage(message), throwable);
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
        log.warn(generateLogMessage(message), throwable);
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
        log.error(generateLogMessage(message), throwable);
    }

    /**
     * Validate that a procedure with {@code name} exists and {@code parameters} are valid for that procedure.
     * <p>
     * Note: parameter validation might not work for system procedures
     *
     * @param errors                   {@link SchedulerValidationErrors} instance to collect errors
     * @param restrictProcedureByScope If true type of procedures will be restricted. See
     *                                 {@link Scheduler#restrictProcedureByScope()}
     * @param procedureName            Name of procedure to validate
     * @param parameters               that will be passed to {@code name}
     */
    public void validateProcedure(SchedulerValidationErrors errors, boolean restrictProcedureByScope,
            String procedureName, Object[] parameters) {
        Procedure procedure = m_clientInterface.getProcedureFromName(procedureName);
        if (procedure == null) {
            errors.addErrorMessage("Procedure does not exist: " + procedureName);
            return;
        }

        if (procedure.getSystemproc()) {
            // System procedures do not have parameter types in the procedure definition
            return;
        }

        if (restrictProcedureByScope) {
            String error = SchedulerManager.isProcedureValidForScope(m_scope, procedure);
            if (error != null) {
                errors.addErrorMessage(error);
                return;
            }
        }

        CatalogMap<ProcParameter> parameterTypes = procedure.getParameters();

        if (procedure.getSinglepartition() && parameterTypes.size() == parameters.length + 1) {
            if (procedure.getPartitionparameter() != 0) {
                errors.addErrorMessage(String.format(
                        "Procedure %s is a partitioned procedure but the partition parameter is not the first",
                        procedureName));
                return;
            }

            Object[] newParameters = new Object[parameters.length + 1];
            newParameters[0] = 0;
            System.arraycopy(parameters, 0, newParameters, 1, parameters.length);
            parameters = newParameters;
        }

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
