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

import java.util.Arrays;

/**
 * Class which holds all of the information needed to describe an action. Used as a base class for {@link Action}
 */
class ActionDescription {
    private final ActionType m_type;
    private final String m_procedure;
    private final Object[] m_procedureParameters;
    private String m_statusMessage;

    ActionDescription(ActionType type, String statusMessage, String procedure, Object... procedureParameters) {
        m_type = type;
        m_procedure = procedure;
        m_procedureParameters = procedureParameters;
        m_statusMessage = statusMessage;
    }

    /**
     * @return The {@link ActionType} of this action
     */
    public ActionType getType() {
        return m_type;
    }

    /**
     * @return Optional status message provided with any action
     */
    public String getStatusMessage() {
        return m_statusMessage;
    }

    /**
     * Set the optional status massage which will be reported in the statistics for a task and if this is an
     * {@link ActionType#ERROR} or {@link ActionType#EXIT} action then it will also be logged. For
     * {@link ActionType#ERROR} or {@link ActionType#EXIT} actions this can be provided as part of the factory method.
     *
     * @param statusMessage To be reported
     * @return {@code this}
     */
    public ActionDescription setStatusMessage(String statusMessage) {
        m_statusMessage = statusMessage;
        return this;
    }

    /**
     * @return Name of procedure to execute. Will be {@code null} if this is not a {@link ActionType#PROCEDURE}
     */
    public String getProcedure() {
        return m_procedure;
    }

    /**
     * @return The parameters that are to be passed the the procedure returned by {@link #getProcedure()}
     */
    public Object[] getProcedureParameters() {
        return m_procedureParameters.clone();
    }

    Object[] getRawProcedureParameters() {
        return m_procedureParameters;
    }

    @Override
    public String toString() {
        return "ActionDescription [type=" + m_type + ", procedure=" + m_procedure + ", procedureParameters="
                + Arrays.toString(m_procedureParameters) + ", statusMessage=" + m_statusMessage + "]";
    }
}
