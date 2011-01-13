/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner.microoptimizations;

import java.util.ArrayList;
import java.util.List;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.*;

public class PushdownLimitsIntoScans implements MicroOptimization {

    @Override
    public List<CompiledPlan> apply(CompiledPlan plan) {
        ArrayList<CompiledPlan> retval = new ArrayList<CompiledPlan>();

        AbstractPlanNode planGraph = plan.fragments.get(0).planGraph;
        planGraph = recursivelyApply(planGraph);
        plan.fragments.get(0).planGraph = planGraph;

        retval.add(plan);
        return retval;
    }

    AbstractPlanNode recursivelyApply(AbstractPlanNode plan) {
        assert(plan != null);

        // depth first:
        //     find LimitPlanNodes with exactly one child
        //     where that child is an AbstractScanPlanNode
        //     disconnect the LimitPlanNode
        //     and inline the LimitPlanNode in to the AbstractScanPlanNode

        ArrayList<AbstractPlanNode> children = new ArrayList<AbstractPlanNode>();
        for (int i = 0; i < plan.getChildCount(); i++)
            children.add(plan.getChild(i));
        plan.clearChildren();

        for (AbstractPlanNode child : children) {
            // TODO this will break when children feed multiple parents
            child = recursivelyApply(child);
            child.clearParents();
            plan.addAndLinkChild(child);
        }

        if ((plan instanceof LimitPlanNode) == false)
            return plan;

        if (plan.getChildCount() != 1)
            return plan;

        AbstractPlanNode child = plan.getChild(0);
        if ((child instanceof AbstractScanPlanNode) == false)
            return plan;

        plan.clearChildren();
        child.clearParents();
        child.addInlinePlanNode(plan);

        return child;
    }

}
