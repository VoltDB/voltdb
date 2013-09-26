/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;

import org.voltdb.catalog.Database;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.types.JoinType;

public class PushdownLimits extends MicroOptimization {

    @Override
    public List<CompiledPlan> apply(CompiledPlan plan, Database db) {
        ArrayList<CompiledPlan> retval = new ArrayList<CompiledPlan>();

        AbstractPlanNode planGraph = plan.rootPlanGraph;
        planGraph = recursivelyApply(planGraph);
        plan.rootPlanGraph = planGraph;

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

        // push into Scans
        if (child instanceof AbstractScanPlanNode) {
            plan.clearChildren();
            child.clearParents();
            child.addInlinePlanNode(plan);
            return recursivelyApply(child);
        }

        // push down to Projection
        if (child instanceof ProjectionPlanNode) {
            assert (child.getChildCount() == 1);
            AbstractPlanNode leaf = child.getChild(0);
            leaf.clearParents();
            plan.clearChildren();
            plan.addAndLinkChild(leaf);
            child.clearChildren();
            child.clearParents();
            child.addAndLinkChild(plan);
            return recursivelyApply(child);
        }

        // push into JOINs
        if (child instanceof AbstractJoinPlanNode) {
            plan.clearChildren();
            child.clearParents();
            child.addInlinePlanNode(plan);
            if (((AbstractJoinPlanNode)child).getJoinType() == JoinType.LEFT) {
                // for LEFT OUTER, also need to push down to the left child (OUTER table)
                AbstractPlanNode leaf = child.getChild(0);
                LimitPlanNode copy = new LimitPlanNode((LimitPlanNode)plan);
                leaf.clearChildren();
                leaf.clearParents();
                copy.addAndLinkChild(leaf);
                child.setAndLinkChild(0, copy);
                // push down further in the left child if it's a Scan or a JOIN
                child = recursivelyApply(child);
            }
            return child;
        }

        return plan;

    }

}
