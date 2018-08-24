/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.calciteadapter.rel.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.util.VoltDBRexUtil;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.MergeReceivePlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.OrderByPlanNode;

import com.google.common.collect.ImmutableList;

public class VoltDBPMergeExchange extends AbstractVoltDBPExchange implements VoltDBPRel {

    // Inline Offset
    private RexNode m_offset;
    // Inline Limit
    private RexNode m_limit;

    // Collation fields expressions
    ImmutableList<RexNode> m_collationFieldExprs;

    public VoltDBPMergeExchange(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelDistribution childDistribution,
            int splitCount,
            boolean isTopExchange,
            List<RexNode> collationFieldExprs) {
        super(cluster, traitSet, input, childDistribution, splitCount, isTopExchange);
        m_collationFieldExprs = ImmutableList.copyOf(collationFieldExprs);
    }

    @Override
    protected VoltDBPMergeExchange copyInternal(
            RelTraitSet traitSet,
            RelNode newInput,
            RelDistribution childDistribution,
            boolean isTopExchange) {
        VoltDBPMergeExchange exchange = new VoltDBPMergeExchange(
                getCluster(),
                traitSet,
                newInput,
                getChildDistribution(),
                m_splitCount,
                isTopExchange,
                m_collationFieldExprs);
        return exchange;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        MergeReceivePlanNode rpn = new MergeReceivePlanNode();
        rpn = (MergeReceivePlanNode) super.toPlanNode(rpn);
        // Must set HaveSignificantOutputSchema after its own schema is generated
        rpn.setHaveSignificantOutputSchema(true);

        NodeSchema preAggregateOutputSchema = generatePreAggregateSchema(rpn);
        rpn.setPreAggregateOutputSchema(preAggregateOutputSchema);

        // Collation must be converted to the inline OrderByPlanNode
        RelTrait collationTrait = getTraitSet().getTrait(RelCollationTraitDef.INSTANCE);
        OrderByPlanNode inlineOrderByPlanNode =
                VoltDBRexUtil.collationToOrderByNode((RelCollation) collationTrait, this.m_collationFieldExprs);
        rpn.addInlinePlanNode(inlineOrderByPlanNode);

        // Inline Limit and / or Offset
        if (m_limit != null || m_offset != null) {
            LimitPlanNode inlineLimitPlanNode = new LimitPlanNode();
            if (m_limit != null) {
                inlineLimitPlanNode.setLimit(RexLiteral.intValue(m_limit));
            }
            if (m_offset != null) {
                inlineLimitPlanNode.setOffset(RexLiteral.intValue(m_offset));
            }
            rpn.addInlinePlanNode(inlineLimitPlanNode);
        }
        return rpn;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        if (m_offset != null) {
            digest += "_offset_" + m_offset.toString();
        }
        if (m_limit != null) {
            digest += "_limit_" + m_limit.toString();
        }
        return digest;
    }

    private NodeSchema generatePreAggregateSchema(MergeReceivePlanNode rpn) {
        // @TODO Set Pre aggregate output schema. for now no aggregate - from input
        assert (rpn.getOutputSchema() != null);
        return rpn.getOutputSchema();
    }

    public void setOffset(RexNode offset) {
        m_offset = offset;
    }

    public RexNode getOffset() {
        return m_offset;
    }

    public void setLimit(RexNode limit) {
        m_limit = limit;
    }

    public RexNode getLimit() {
        return m_limit;
    }

}
