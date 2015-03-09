/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.planner.microoptimizations;

import org.voltdb.compiler.DeterminismMode;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.AbstractPlanNode;

public abstract class MicroOptimization {
    protected AbstractParsedStmt m_parsedStmt;

    /// Provide a commonly used approach to optimization via a potentially recursive call to
    /// recursivelyApply that is expected to edit or replace the plan graph.
    /// The parsed statement is also available as a data member to provide context.
    /// Derived classes must implement recursiveApply, but that implementation could,
    /// theoretically, just "assert(false)" IF the class also completely re-implements "apply".
    /// Implementing recursivelyApply to do real work on the plan graph is the more common paradigm.
    void apply(CompiledPlan plan, DeterminismMode detMode, AbstractParsedStmt parsedStmt)
    {
        // seq scan to index scan overrides this function
        apply(plan, parsedStmt);
    }

    void apply(CompiledPlan plan, AbstractParsedStmt parsedStmt)
    {
        try {
            m_parsedStmt = parsedStmt;
            AbstractPlanNode planGraph = plan.rootPlanGraph;
            planGraph = recursivelyApply(planGraph);
            plan.rootPlanGraph = planGraph;
        }
        finally {
            // Avoid leaking a long-term reference from static optimizations
            // to a large parsed statement structure.
            m_parsedStmt = null;
        }
    }

    protected abstract AbstractPlanNode recursivelyApply(AbstractPlanNode plan);
}
