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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public abstract class AbstractVoltDBPAggregate extends Aggregate implements VoltDBPRel {

    // HAVING expression
    final protected RexNode m_postPredicate;

    final protected int m_splitCount;

    // TRUE if this aggregate relation is part of a coordinator tree.
    // The indicator may be useful during the Exchange Transform rule when a coordinator aggregate
    // differs from a fragment one
    final protected boolean m_isCoordinatorAggr;

    /**
     * Constructor
     */
    protected AbstractVoltDBPAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            int splitCount,
            boolean isCoordinatorAggr) {
        super(cluster, traitSet, child, indicator, groupSet, groupSets, aggCalls);
        m_postPredicate = postPredicate;
        m_splitCount = splitCount;
        m_isCoordinatorAggr = isCoordinatorAggr;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("split", m_splitCount);
        pw.item("coorinator", m_isCoordinatorAggr);
        if (m_postPredicate != null) {
            pw.item("having", m_postPredicate);
        }
        return pw;
    }

    @Override
    protected String computeDigest() {
        String digest = super.computeDigest();
        digest += "_split_" + m_splitCount;
        digest += "_coordinator_" + m_isCoordinatorAggr;
        if (m_postPredicate != null) {
            digest += m_postPredicate.toString();
        }
        return digest;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
                                      RelMetadataQuery mq) {
        double rowCount = getInput().estimateRowCount(mq);
        return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    /**
     * Copy self
     *
     * @param cluster
     * @param traitSet
     * @param input
     * @param indicator
     * @param groupSet
     * @param groupSets
     * @param aggCalls
     * @param postPredicate
     * @param splitCount
     * @return
     */
    public abstract AbstractVoltDBPAggregate copy(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            int splitCount,
            boolean isCoordinatorAggr);

    public RexNode getPostPredicate() {
        return m_postPredicate;
    }

    @Override
    public int getSplitCount() {
        return m_splitCount;
    }

}
