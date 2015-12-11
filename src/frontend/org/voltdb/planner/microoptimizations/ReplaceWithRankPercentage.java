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

import java.util.ArrayList;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.types.PlanNodeType;

public class ReplaceWithRankPercentage extends MicroOptimization {

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode plan)
    {
        assert(plan != null);

        // depth first:
        //     find AggregatePlanNode with exactly one child
        //     where that child is an AbstractScanPlanNode.
        //     Replace any qualifying AggregatePlanNode / AbstractScanPlanNode pair
        //     with an IndexCountPlanNode or TableCountPlanNode

        ArrayList<AbstractPlanNode> children = new ArrayList<AbstractPlanNode>();

        for (int i = 0; i < plan.getChildCount(); i++)
            children.add(plan.getChild(i));

        for (AbstractPlanNode child : children) {
            // TODO this will break when children feed multiple parents
            AbstractPlanNode newChild = recursivelyApply(child);
            // Do a graft into the (parent) plan only if a replacement for a child was found.
            if (newChild == child) {
                continue;
            }
            boolean replaced = plan.replaceChild(child, newChild);
            assert(true == replaced);
        }

        // check for an aggregation of the right form

        if (plan.getPlanNodeType() != PlanNodeType.SEQSCAN
                || plan.getPlanNodeType() == PlanNodeType.INDEXSCAN)
            return plan;

        if (plan.getPlanNodeType() == PlanNodeType.SEQSCAN) {
            SeqScanPlanNode sspn = (SeqScanPlanNode) plan;
            if (sspn.isSubQuery()) {
                return plan;
            }

        }
        AbstractScanPlanNode aspn = (AbstractScanPlanNode) plan;
        AbstractExpression ae = aspn.getPredicate();
        aspn.setPredicate(ExpressionUtil.replaceCVEasRankPercentageExpression(ae));

        // In future, index scan search key, end key, etc should also be processed here
        // more complex case like the search key has not been addressed

        return plan;
    }
}
