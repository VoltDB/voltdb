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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.ExpressionType;

public class ApproxCountDistinctDistributor extends MicroOptimization {

    private AggregatePlanNode m_topAggPlanNode = null;
    private AggregatePlanNode m_lowerAggPlanNode = null;

    @Override
    protected AbstractPlanNode recursivelyApply(AbstractPlanNode plan) {

        // This object is re-used for different plans, so must reset state!
        m_topAggPlanNode = null;
        m_lowerAggPlanNode = null;

        applyHelper(false, plan);

        if (m_topAggPlanNode != null && m_lowerAggPlanNode != null) {
            // Transform any lower approx_count_distinct aggs to
            // VALS_TO_HYPERLOGLOG, which converts column values to a
            // a HyperLogLog data structure encoded as VARBINARY
            List<ExpressionType> lowerAggTypes = m_lowerAggPlanNode.getAggregateTypes();
            for (int i = 0; i < lowerAggTypes.size(); ++i) {
                if (lowerAggTypes.get(i) == ExpressionType.AGGREGATE_APPROX_COUNT_DISTINCT) {
                    m_lowerAggPlanNode.updateAggregate(i, ExpressionType.AGGREGATE_VALS_TO_HYPERLOGLOG);
                }
            }

            // Transform any upper approx_count_distinct aggs to
            // HYPERLOGLOGS_TO_CARD, which merges the hyperloglog data
            // structures and produces an cardinality estimate
            List<ExpressionType> topAggTypes = m_topAggPlanNode.getAggregateTypes();
            for (int i = 0; i < topAggTypes.size(); ++i) {
                if (topAggTypes.get(i) == ExpressionType.AGGREGATE_APPROX_COUNT_DISTINCT) {
                    m_topAggPlanNode.updateAggregate(i, ExpressionType.AGGREGATE_HYPERLOGLOGS_TO_CARD);
                }
            }
        }

        // TODO Auto-generated method stub
        return plan;
    }

    private void applyHelper(boolean inLowerFragment, AbstractPlanNode node) {
        if (node instanceof AggregatePlanNode) {
            AggregatePlanNode aggNode = (AggregatePlanNode) node;
            if (inLowerFragment) {
                m_lowerAggPlanNode = aggNode;
            }
            else {
                m_topAggPlanNode = aggNode;
            }
        }
        else if (node instanceof SendPlanNode) {
            inLowerFragment = true;
        }

        for (AbstractPlanNode inlineNode : node.getInlinePlanNodes().values()) {
            applyHelper(inLowerFragment, inlineNode);
        }

        for (int i = 0; i < node.getChildCount(); ++i) {
            applyHelper(inLowerFragment, node.getChild(i));
        }
    }

}
