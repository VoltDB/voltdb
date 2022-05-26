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

package org.voltdb.plannerv2.rel.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

import com.google.common.base.Preconditions;

/**
 * VoltDB logical limit operator.
 * There are specific reasons that we pulled the limit/offset information out of
 * the sort node. This limit operator may still be subject to changes.
 *
 * @see org.voltdb.plannerv2.rules.logical.VoltLSortRule
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltLogicalLimit extends SingleRel implements VoltLogicalRel {

    private RexNode m_offset;
    private RexNode m_limit;

    /**
     * Creates a VoltLogicalLimit.
     *
     * @param cluster   Cluster
     * @param traitSet  Trait set
     * @param input     Input relation
     * @param offset    The offset
     * @param limit     The Limit
     */
    public VoltLogicalLimit(
            RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexNode offset, RexNode limit) {
            super(cluster, traitSet, input);
            Preconditions.checkArgument(getConvention() == VoltLogicalRel.CONVENTION);
            m_offset = offset;
            m_limit = limit;
        }

    @Override public VoltLogicalLimit copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new VoltLogicalLimit(getCluster(), traitSet, sole(inputs), m_offset, m_limit);
    }

    /**
     * @return the offset value.
     */
    public RexNode getOffset() {
        return m_offset;
    }

    /**
     * @return the limit value.
     */
    public RexNode getLimit() {
        return m_limit;
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
                .itemIf("limit", m_limit, m_limit != null)
                .itemIf("offset", m_offset, m_offset != null);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        final RelOptCost cost = super.computeSelfCost(planner, mq);
        return planner.getCostFactory().makeCost(cost.getRows(),
                cost.getRows(),     // NOTE: CPU cost comes into effect in physical planning stage.
                cost.getIo());
    }

}
