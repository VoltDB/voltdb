/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltdb.catalog.Index;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.IndexCountPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.TableCountPlanNode;
import org.voltdb.types.PlanNodeType;

public class ReplaceWithIndexCounter extends MicroOptimization {
    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode plan) {
        assert(plan != null);

        // depth first:
        //     find AggregatePlanNode with exactly one child
        //     where that child is an AbstractScanPlanNode.
        //     Replace any qualifying AggregatePlanNode / AbstractScanPlanNode pair
        //     with an IndexCountPlanNode or TableCountPlanNode

        for (int i = 0; i < plan.getChildCount(); i++) {
            AbstractPlanNode child = plan.getChild(i);
            // TODO this will break when children feed multiple parents
            AbstractPlanNode newChild = recursivelyApply(child);
            // Do a graft into the (parent) plan only if a replacement for a child was found.
            if (newChild == child) {
                continue;
            }
            boolean replaced = plan.replaceChild(child, newChild);
            assert(replaced);
        }

        // check for an aggregation of the right form
        if ((plan instanceof AggregatePlanNode) == false) {
            return plan;
        }

        assert(plan.getChildCount() == 1);
        AggregatePlanNode aggplan = (AggregatePlanNode)plan;
        // ENG-6131 fixed here.
        if (! (aggplan.isTableCountStar() ||
                aggplan.isTableNonDistinctCountConstant() ||
                aggplan.isTableCountNonDistinctNullableColumn() )) {
            return plan;
        }

        AbstractPlanNode child = plan.getChild(0);

        // A table count can replace a seq scan only if it has no predicates.
        if (child instanceof SeqScanPlanNode) {
            if (((SeqScanPlanNode)child).getPredicate() != null) {
                return plan;
            }

            if (aggplan.getPostPredicate() != null) {
                // The table count EE executor does not handle a post-predicate
                // (having clause), though it easily could be made to --
                // this is probably not useful enough to optimize for.
                return plan;
            }

            if (hasInlineLimit(aggplan)) {
                // The table count EE executor does not support an inline
                // limit clause. This is not useful enough to optimize for.
                return plan;
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

            if (hasInlineLimit(aggplan)) {
                return plan;
            }

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

    private static boolean hasInlineLimit(AbstractPlanNode node) {
        AbstractPlanNode inlineNode = node.getInlinePlanNode(PlanNodeType.LIMIT);
        if (inlineNode != null) {
            assert(inlineNode instanceof LimitPlanNode);
            // Table count with limit greater than 0 will not make a difference.
            // The better way is to check m_limit and return true ONLY for m_limit == 0.
            // However, the parameterized plan complicates that.
            // Be conservative about the silly query to prevent any wrong answer.
            return true;
        }
        return false;
    }
}
