/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import org.voltdb.plannerv2.converter.RexConverter;
import org.voltdb.plannerv2.rel.util.PlanCostUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;

import com.google.common.base.Preconditions;

public class VoltPhysicalLimit extends SingleRel implements VoltPhysicalRel {

    // TODO: limit / offset as expressions or parameters
    private RexNode m_offset;
    private RexNode m_limit;

    private final int m_splitCount;

    public VoltPhysicalLimit(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RexNode offset,
            RexNode limit,
            int splitCount) {
        super(cluster, traitSet, input);
        Preconditions.checkArgument(getConvention() == VoltPhysicalRel.CONVENTION);
        m_offset = offset;
        m_limit = limit;
        m_splitCount = splitCount;
    }

    public VoltPhysicalLimit copy(RelTraitSet traitSet, RelNode input,
                                  RexNode offset, RexNode limit, int splitCount) {
        return new VoltPhysicalLimit(
                getCluster(),
                traitSet,
                input,
                offset,
                limit,
                splitCount);
    }

    @Override
    public VoltPhysicalLimit copy(RelTraitSet traitSet,
                                  List<RelNode> inputs) {
        return copy(traitSet, sole(inputs), m_offset, m_limit, m_splitCount);
    }

    public RexNode getOffset() {
        return m_offset;
    }

    public RexNode getLimit() {
        return m_limit;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("split", m_splitCount);
        pw.itemIf("limit", m_limit, m_limit != null);
        pw.itemIf("offset", m_offset, m_offset != null);
        return pw;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        digest += "_split_" + m_splitCount;
        return digest;
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        double childRowCount = getInput(0).estimateRowCount(mq);
        return PlanCostUtil.discountLimitOffsetRowCount(childRowCount, m_offset, m_limit);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rowCount = estimateRowCount(mq);
        double cpu = rowCount;
        return planner.getCostFactory().makeCost(rowCount, cpu, 0);
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

    @Override
    public AbstractPlanNode toPlanNode() {

        LimitPlanNode lpn = new LimitPlanNode();
        if (m_limit != null) {
            lpn.setLimit(RexLiteral.intValue(m_limit));
        }
        if (m_offset != null) {
            lpn.setOffset(RexLiteral.intValue(m_offset));
        }

        if (this.getInput() != null) {
            // Limit is not inlined
            AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
            lpn.addAndLinkChild(child);
        }

        return lpn;
    }

    public static LimitPlanNode toPlanNode(RexNode limit, RexNode offset) {
        LimitPlanNode lpn = new LimitPlanNode();
        if (limit != null) {
            if (limit instanceof RexDynamicParam) {
                lpn.setLimit(-1);
                lpn.setLimitParameterIndex(RexConverter.PARAM_COUNTER.getAndIncrement());
            } else {
                lpn.setLimit(RexLiteral.intValue(limit));
            }
        }
        if (offset != null) {
            if (offset instanceof RexDynamicParam) {
                lpn.setOffset(0);
                lpn.setOffsetParameterIndex(RexConverter.PARAM_COUNTER.getAndIncrement());
            } else {
                lpn.setOffset(RexLiteral.intValue(offset));
            }
        }
        return lpn;
    }
}
