/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.planner;

import org.voltdb.plannodes.AbstractPlanNode;

/**
 * A simple wrapper class to carry around a generated plan and its status
 *
 */
public class PlanAssemblerResult {

    private final AbstractPlanNode m_planNode;
    private final PlanStatus m_status;

    public PlanAssemblerResult(AbstractPlanNode planNode, PlanStatus status) {
        assert(planNode != null || status != PlanStatus.SUCCESS);
        m_planNode = planNode;
        m_status = status;
    }

    public PlanAssemblerResult(AbstractPlanNode planNode) {
        this(planNode, PlanStatus.SUCCESS);
    }

    public PlanAssemblerResult(PlanStatus status) {
        this(null, status);
    }

    public AbstractPlanNode getPlan() {
        return m_planNode;
    }

    public PlanStatus getStatus() {
        return m_status;
    }
}
