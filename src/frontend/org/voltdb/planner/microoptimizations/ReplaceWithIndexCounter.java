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
import java.util.List;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexCountPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.TableCountPlanNode;
import org.voltdb.types.ExpressionType;

public class ReplaceWithIndexCounter extends MicroOptimization {

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
        if ((plan instanceof AggregatePlanNode) == false)
            return plan;
        assert(plan.getChildCount() == 1);
        AggregatePlanNode aggplan = (AggregatePlanNode)plan;
        if (aggplan.isTableCountStar() == false) {
            return plan;
        }

        AbstractPlanNode child = plan.getChild(0);

        // A table count can replace a seq scan only if it has no predicates.
        if (child instanceof SeqScanPlanNode) {
            if (((SeqScanPlanNode)child).getPredicate() != null) {
                return plan;
            }

            AbstractExpression postPredicate = aggplan.getPostPredicate();
            if (postPredicate != null) {
                List<AbstractExpression> aggList = postPredicate.findAllSubexpressionsOfClass(AggregateExpression.class);

                boolean allCountStar = true;
                for (AbstractExpression expr: aggList) {
                    if (expr.getExpressionType() != ExpressionType.AGGREGATE_COUNT_STAR) {
                        allCountStar = false;
                        break;
                    }
                }
                if (allCountStar) {
                    return plan;
                }
            }
            return new TableCountPlanNode((AbstractScanPlanNode)child, aggplan);
        }

        // Otherwise, optimized counts only replace particular cases of index scan.
        if ((child instanceof IndexScanPlanNode) == false)
            return plan;

        IndexScanPlanNode isp = (IndexScanPlanNode)child;

        // Guard against (possible future?) cases of indexable subquery.
        if (((IndexScanPlanNode)child).isSubQuery()) {
            return plan;
        }

        // An index count or table count can replace an index scan only if it has no (post-)predicates
        // except those (post-)predicates are artifact predicates we added for reverse scan purpose only
        if (isp.getPredicate() != null && !isp.isPredicatesOptimizableForAggregate()) {
            return plan;
        }

        // With no start or end keys, there's not much a counting index can do.
        if (isp.getEndExpression() == null && isp.getSearchKeyExpressions().size() == 0) {
            // An indexed query without a where clause can fall back to a plain old table count.
            // This can only happen when a confused query like
            // "select count(*) from table order by index_key;"
            // meets a naive planner that doesn't just cull the no-op ORDER BY. Who, us?
            return new TableCountPlanNode(isp, aggplan);
        }

        // check for the index's support for counting
        Index idx = isp.getCatalogIndex();
        if ( ! idx.getCountable()) {
            return plan;
        }

        // The core idea is that counting index needs to know the start key and end key to
        // jump to to get counts instead of actually doing any scanning.
        // Options to be determined are:
        // - whether each of the start/end keys is missing, partial (a prefix of a compund key), or complete,
        // - whether the count should include or exclude entries exactly matching each of the start/end keys.
        // Not all combinations of these options are supported;
        // unsupportable cases cause the factory method to return null.
        IndexCountPlanNode countingPlan = IndexCountPlanNode.createOrNull(isp, aggplan);
        if (countingPlan == null) {
            return plan;
        }
        return countingPlan;
    }
}
