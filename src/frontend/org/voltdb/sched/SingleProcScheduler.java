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
abstract class SingleProcScheduler implements Scheduler {
    final String m_procedure;
    final Object[] m_procedureParameters;

    static void validateParameters(SchedulerValidationErrors errors, SchedulerValidationHelper helper, String procedure,
            String... procedureParameters) {
        helper.validateProcedureAndParams(errors, true, procedure, procedureParameters);
    }

    SingleProcScheduler(String procedure, String... procedureParameters) {
        m_procedure = procedure;
        m_procedureParameters = procedureParameters;
    }

    @Override
    public boolean restrictProcedureByScope() {
        return true;
    }

    @Override
    public String toString() {
        return "Schuduled: " + m_procedure + " with parameters " + Arrays.toString(m_procedureParameters);
    }

    @Override
    public Action getFirstAction() {
        return getNextAction(null);
    }

    @Override
    public final Action getNextAction(ActionResult result) {
        return Action.createProcedure(getNextDelayNs(), TimeUnit.NANOSECONDS, m_procedure, m_procedureParameters);
    }

    /**
     * @return The delay for the next execution in nanoseconds
     */
    abstract long getNextDelayNs();
}
