/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.util.LinkedList;
import java.util.Queue;

import org.voltdb.catalog.Index;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.types.PlanNodeType;

public class OffsetQueryUsingCountingIndex extends MicroOptimization {

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode planNode, AbstractParsedStmt parsedStmt)
    {
        assert(planNode != null);

        // breadth search first:
        //     find IndexScanPlannode

        Queue<AbstractPlanNode> children = new LinkedList<AbstractPlanNode>();
        children.add(planNode);

        while(!children.isEmpty()) {
            AbstractPlanNode plan = children.remove();
            rankIndexSearchApply(plan);

            for (int i = 0; i < plan.getChildCount(); i++) {
                children.add(plan.getChild(i));
            }
        }

        return planNode;
    }

    void rankIndexSearchApply(AbstractPlanNode plan) {
        // check for the index scan node of the right form
        if ( ! (plan instanceof IndexScanPlanNode) ) {
            return;
        }
        IndexScanPlanNode indexscan = (IndexScanPlanNode)plan;

        if (indexscan.getPredicate() != null ||
                indexscan.getEndExpression() != null ||
                indexscan.getSkipNullPredicate() != null ||
                indexscan.getInitialExpression() != null) {
            return;
        }

        // inlined nodes can be projection/limit/aggregation plan nodes.
        if (indexscan.getInlinePlanNodes().isEmpty()) {
            return;
        }

        LimitPlanNode limit = (LimitPlanNode) indexscan.getInlinePlanNodes().get(PlanNodeType.LIMIT);
        if (limit == null || !limit.hasOffset()) {
            return;
        }

        if (AggregatePlanNode.getInlineAggregationNode(indexscan) != null) {
            // do not apply offset before inline aggregation plan node
            return;
        }

        Index index = indexscan.getCatalogIndex();
        if (! index.getCountable()) {
            return;
        }

        if (! indexscan.getSearchKeyExpressions().isEmpty()) {
            return;
        }

        indexscan.setOffsetRank(true);
    }

}
