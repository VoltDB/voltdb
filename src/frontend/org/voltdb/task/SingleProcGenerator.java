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
 * Simple implementation of a {@link ActionGenerator} which always returns the same procedure to run with the same
 * parameters
 */
public final class SingleProcGenerator implements ActionGenerator {
    private Action m_action;
    private boolean m_isReadOnly;

    public static String validateParameters(TaskHelper helper, String procedure, Object... procedureParameters) {
        CompoundErrors errors = new CompoundErrors();
        helper.validateProcedure(errors, true, procedure, procedureParameters);
        return errors.getErrorMessage();
    }

    public void initialize(TaskHelper helper, String procedure, Object... procedureParameters) {
        m_action = Action.procedureCall(r -> m_action, procedure, procedureParameters);
        m_isReadOnly = helper.isProcedureReadOnly(procedure);
    }

    @Override
    public boolean restrictProcedureByScope() {
        return true;
    }

    @Override
    public Action getFirstAction() {
        return m_action;
    }

    @Override
    public boolean isReadOnly() {
        return m_isReadOnly;
    }
}
