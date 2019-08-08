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

import java.util.Arrays;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltdb.client.ClientResponse;

import com.google_voltpatches.common.collect.ImmutableSortedMap;

/**
 * Base class of {@link Scheduler} which has different mechanisms for handling errors returned by procedures and
 * schedulers which extend this class.
 */
abstract class ErrorHandlerScheduler implements Scheduler {
    private static final VoltLogger log = new VoltLogger("HOST");

    /** Map of error handler name to error handler implementation */
    private static final Map<String, ErrorHandler> s_errorHandlers = ImmutableSortedMap
            .<String, ErrorHandler>orderedBy(String.CASE_INSENSITIVE_ORDER).put("ABORT", new ErrorHandler() {
                @Override
                public SchedulerResult procedureFailed(ErrorHandlerScheduler scheduler, ScheduledProcedure procedure) {
                    return SchedulerResult.createError(generateProcedureFailedMessage(procedure));
                }
            }).put("LOG", new ErrorHandler() {
                @Override
                public SchedulerResult procedureFailed(ErrorHandlerScheduler scheduler, ScheduledProcedure procedure) {
                    log.info(scheduler.generateLogMessage(generateProcedureFailedMessage(procedure)));
                    return null;
                }
            }).put("IGNORE", new ErrorHandler() {
                @Override
                public SchedulerResult procedureFailed(ErrorHandlerScheduler scheduler, ScheduledProcedure procedure) {
                    if (log.isDebugEnabled()) {
                        log.debug(scheduler.generateLogMessage(generateProcedureFailedMessage(procedure)));
                    }
                    return null;
                }
            }).build();

    private final String m_name;
    private final ErrorHandler m_errorHandler;

    static String generateProcedureFailedMessage(ScheduledProcedure procedure) {
        return "Procedure " + procedure.getProcedure() + " with parameters "
                + Arrays.toString(procedure.getProcedureParameters()) + " failed: "
                + procedure.getResponse().getStatusString();
    }

    static void validateParameters(SchedulerValidationErrors errors, String errorHandler) {
        if (!s_errorHandlers.containsKey(errorHandler)) {
            errors.addErrorMessage("Error handler does not exist: " + errorHandler);
        }
    }

    /**
     * @param name         of the schedule
     * @param errorHandler name of error handler to be used
     */
    ErrorHandlerScheduler(String name, String errorHandler) {
        m_name = name;
        m_errorHandler = s_errorHandlers.get(errorHandler);

        if (m_errorHandler == null) {
            throw new IllegalArgumentException("Error handler does not exist: " + errorHandler);
        }
    }

    @Override
    public final SchedulerResult nextRun(ScheduledProcedure previousProcedureRun) {
        if (previousProcedureRun != null) {
            ClientResponse response = previousProcedureRun.getResponse();
            if (response != null && response.getStatus() != ClientResponse.SUCCESS) {
                SchedulerResult result = m_errorHandler.procedureFailed(this, previousProcedureRun);
                if (result != null) {
                    return result;
                }
            }
        }

        return nextRunImpl(previousProcedureRun);
    }

    /**
     * @see Scheduler#nextRun(ScheduledProcedure)
     */
    abstract SchedulerResult nextRunImpl(ScheduledProcedure previousProcedureRun);

    /**
     * @return The delay for the next execution in nanoseconds
     */
    abstract long getNextDelayNs();

    String generateLogMessage(String body) {
        return SchedulerManager.generateLogMessage(m_name, body);
    }

    /**
     * Interface implemented by the different error handler types
     */
    interface ErrorHandler {
        /**
         * Handle when an executed procedure fails
         *
         * @param scheduler the {@link ErrorHandlerScheduler} instance which caused the error
         * @param procedure {@link ScheduledProcedure} which failed
         * @return {@link SchedulerResult} if a result should be returned or {@code null} if execution should continue
         */
        SchedulerResult procedureFailed(ErrorHandlerScheduler scheduler, ScheduledProcedure procedure);
    }
}
