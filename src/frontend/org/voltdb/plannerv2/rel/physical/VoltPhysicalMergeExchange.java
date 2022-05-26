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
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.voltdb.plannerv2.guards.CalcitePlanningException;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.SendPlanNode;

import com.google.common.collect.ImmutableList;
import com.google_voltpatches.common.base.Preconditions;

public class VoltPhysicalMergeExchange extends Exchange implements VoltPhysicalRel {

    // Inline Sort Data
    private final ImmutableList<RexNode> m_sorFieldExps;
    // Inline Limit
    private final RexNode m_offset;
    private final RexNode m_limit;

    public VoltPhysicalMergeExchange(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution newDistribution) {
        this(cluster, traitSet, input, newDistribution, null, null, null);
    }

    private VoltPhysicalMergeExchange(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDistribution newDistribution,
            List<RexNode> sorFieldExps, RexNode offset, RexNode limit) {
        super(cluster, traitSet, input, newDistribution);
        Preconditions.checkArgument(! RelDistributions.ANY.getType().equals(
                traitSet.getTrait(RelDistributionTraitDef.INSTANCE).getType()));
        m_sorFieldExps = (sorFieldExps != null) ? ImmutableList.copyOf(sorFieldExps) : null;
        m_offset = offset;
        m_limit = limit;
    }

    @Override
    public VoltPhysicalMergeExchange copy(
            RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution) {
        return new VoltPhysicalMergeExchange(getCluster(), traitSet, newInput, newDistribution,
                m_sorFieldExps, m_offset, m_limit);
    }

    public VoltPhysicalMergeExchange copy(RelTraitSet traitSet, RelNode newInput, RelDistribution newDistribution, List<RexNode> sorFieldExps, RexNode offset, RexNode limit) {
        return new VoltPhysicalMergeExchange(
                getCluster(),
                traitSet,
                newInput,
                newDistribution,
                sorFieldExps,
                offset,
                limit
                );
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        // Limit / offset do not contribute to the estimates because they come from
        // an inlined Limit node at the very last stage to match VoltDB MergeReceivePlanNode
        return getInput().estimateRowCount(mq);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        // row count and cpu cost are the same
        double rowCount = estimateRowCount(mq);
        double cpu = rowCount;
        double io = rowCount;
        return planner.getCostFactory().makeCost(rowCount, cpu, io);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("collation", getTraitSet().getTrait(RelCollationTraitDef.INSTANCE));
        pw.itemIf("offset", m_offset, m_offset != null);
        pw.itemIf("limit", m_limit, m_limit != null);
        return pw;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        final AbstractPlanNode child = inputRelNodeToPlanNode(this, 0);
        final SendPlanNode spn = new SendPlanNode();
        spn.addAndLinkChild(child);
        final MergeReceivePlanNode rpn = new MergeReceivePlanNode();
        rpn.addAndLinkChild(spn);
        // Add Sort
        final RelCollation collation = getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        assert (collation != null);
        if (m_sorFieldExps == null) {
            throw new CalcitePlanningException("VoltPhysicalMergeExchange without inline Sort");
        }
        final OrderByPlanNode opn = VoltRexUtil.collationToOrderByNode(collation, m_sorFieldExps);
        //  Add Limit if any
        if (m_limit != null || m_offset != null) {
            final LimitPlanNode lpn = VoltPhysicalLimit.toPlanNode(m_limit, m_offset);
            opn.addInlinePlanNode(lpn);
        }
        rpn.addInlinePlanNode(opn);
        return rpn;
    }

    public RexNode getOffset() {
        return m_offset;
    }

    public RexNode getLimit() {
        return m_limit;
    }

    @Override
    public List<RexNode> getChildExps() {
        return m_sorFieldExps;
    }

}
