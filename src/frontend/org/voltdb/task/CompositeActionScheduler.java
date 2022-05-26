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

/**
 * Simple {@link ActionScheduler} implementation which takes a {@link ActionGenerator} and {@link IntervalGenerator} and
 * combines the results of those two instances to make a {@link ScheduledAction}
 */
public final class CompositeActionScheduler implements ActionScheduler {
    private final ActionGenerator m_actionGenerator;
    private final IntervalGenerator m_intervalGenerator;

    public CompositeActionScheduler(ActionGenerator actionGenerator, IntervalGenerator intervalGenerator) {
        super();
        m_actionGenerator = actionGenerator;
        m_intervalGenerator = intervalGenerator;
    }

    @Override
    public ScheduledAction getFirstScheduledAction() {
        Action action = m_actionGenerator.getFirstAction();
        if (action.getType().isStop()) {
            return ScheduledAction.of(action);
        }
        return ScheduledAction.of(m_intervalGenerator.getFirstInterval(), action);
    }

    @Override
    public boolean restrictProcedureByScope() {
        return m_actionGenerator.restrictProcedureByScope();
    }

    @Override
    public boolean isReadOnly() {
        return m_actionGenerator.isReadOnly();
    }
}
