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

package org.voltdb.plannerv2.rel.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.voltdb.plannerv2.rel.util.PlanCostUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;

import com.google.common.base.Preconditions;

public class VoltPhysicalLimit extends SingleRel implements VoltPhysicalRel {

    // TODO: limit / offset as expressions or parameters
    private final RexNode m_offset;
    private final RexNode m_limit;

    // In a partitioned query Limit could be pushed down to fragments
    // by the LimitExchange Transpose Rule -
    // Limit / RenNode => Coordinator Limit / Exchange / Fragment Limit / RelNode
    // This indicator prevents this rule to fire indefinitely by setting it to TRUE
    private final boolean m_isPushedDown;

    public VoltPhysicalLimit(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode offset, RexNode limit,
            boolean isPushedDown) {
        super(cluster, traitSet, input);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION);
        m_offset = offset;
        m_limit = limit;
        m_isPushedDown = isPushedDown;
    }

    public VoltPhysicalLimit copy(
            RelTraitSet traitSet, RelNode input, RexNode offset, RexNode limit, boolean isPushedDown) {
        return new VoltPhysicalLimit(getCluster(), traitSet, input, offset, limit, isPushedDown);
    }

    @Override
    public VoltPhysicalLimit copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), m_offset, m_limit, m_isPushedDown);
    }

    public RexNode getOffset() {
        return m_offset;
    }

    public RexNode getLimit() {
        return m_limit;
    }

    public boolean isPushedDown() {
        return m_isPushedDown;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.itemIf("limit", m_limit, m_limit != null);
        pw.itemIf("offset", m_offset, m_offset != null);
        pw.item("pusheddown", m_isPushedDown);
        return pw;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double childRowCount = getInput(0).estimateRowCount(mq);
        return PlanCostUtil.discountLimitOffsetRowCount(childRowCount, m_offset, m_limit);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // row count and cpu cost are the same
        double rowCount = estimateRowCount(mq);
        double cpu = rowCount;
        return planner.getCostFactory().makeCost(rowCount, cpu, 0);
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        final LimitPlanNode lpn = toPlanNode(m_limit, m_offset);

        if (this.getInput() != null) {
            // Limit is not inlined
            AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
            lpn.addAndLinkChild(child);
        }

        return lpn;
    }

    public static LimitPlanNode toPlanNode(RexNode limit, RexNode offset) {
        final LimitPlanNode lpn = new LimitPlanNode();
        if (limit != null) {
            if (limit instanceof RexDynamicParam) {
                lpn.setLimit(-1);
                lpn.setLimitParameterIndex(((RexDynamicParam) limit).getIndex());
            } else {
                lpn.setLimit(RexLiteral.intValue(limit));
            }
        }
        if (offset != null) {
            if (offset instanceof RexDynamicParam) {
                lpn.setOffset(0);
                lpn.setOffsetParameterIndex(((RexDynamicParam) offset).getIndex());
            } else {
                lpn.setOffset(RexLiteral.intValue(offset));
            }
        }
        return lpn;
    }
}
