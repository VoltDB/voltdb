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
import java.util.concurrent.TimeUnit;

/**
 * Base class for a {@link ErrorHandlerScheduler} instance which continuously executes the same procedure with the same
 * parameters repeatedly.
 */
abstract class SingleProcScheduler extends ErrorHandlerScheduler {
    final String m_procedure;
    final Object[] m_procedureParameters;

    static void validateParameters(SchedulerValidationErrors errors, SchedulerValidationHelper helper, String errorHandler, String procedure,
            String... procedureParameters) {
        ErrorHandlerScheduler.validateParameters(errors, errorHandler);
        helper.validateProcedureAndParams(errors, procedure, procedureParameters);
    }

    SingleProcScheduler(String name, String errorHandler, String procedure, String... procedureParameters) {
        super(name, errorHandler);
        m_procedure = procedure;
        m_procedureParameters = procedureParameters;
    }

    @Override
    public String toString() {
        return "Schuduled: " + m_procedure + " with parameters " + Arrays.toString(m_procedureParameters);
    }

    @Override
    final SchedulerResult nextRunImpl(ScheduledProcedure previousProcedureRun) {
        return SchedulerResult.createScheduledProcedure(getNextDelayNs(), TimeUnit.NANOSECONDS, m_procedure,
                m_procedureParameters);
    }
}
