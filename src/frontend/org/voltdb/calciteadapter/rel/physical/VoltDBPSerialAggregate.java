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
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.plannodes.AggregatePlanNode;

public class VoltDBPSerialAggregate extends AbstractVoltDBPAggregate {

    /** Constructor */
    public VoltDBPSerialAggregate(
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
      super(cluster,
            traitSet,
            child,
            indicator,
            groupSet,
            groupSets,
            aggCalls,
            postPredicate,
            splitCount,
            isCoordinatorAggr);
    }

    public VoltDBPSerialAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            boolean isCoordinatorAggr) {
        this(cluster,
                traitSet,
                child,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                postPredicate,
                1,
                isCoordinatorAggr);
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        double rowCount = getInput().estimateRowCount(mq);
        // Give a discount to the Aggregate based on the number of the collation fields.
        //  - Serial Aggregate - the collation size is equal to the number of the GROUP BY columns
        //          and max discount 1 - 0.1 -  0.01 - 0.001 - ...
        //  - Partial Aggregate - anything in between
        // The required order will be enforced by some index which collation would match / satisfy
        // the aggregate's collation. If a table has more than one index multiple Aggregate / IndexScan
        // combinations are possible and we want to pick the one that has the maximum GROUP BY columns
        // covered resulting in a more efficient aggregation (less hashing)
        double discountFactor = 1.0;
        final double MAX_PER_COLLATION_DISCOUNT = 0.1;
        for (int i = 0; i < getGroupCount(); ++i) {
            discountFactor -= Math.pow(MAX_PER_COLLATION_DISCOUNT, i + 1);
        }
        rowCount *= discountFactor;
        return planner.getCostFactory().makeCost(rowCount, 0, 0);
    }

    @Override
    public VoltDBPSerialAggregate copy(RelTraitSet traitSet,
            RelNode input,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        return new VoltDBPSerialAggregate(
                getCluster(),
                traitSet,
                input,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                m_postPredicate,
                m_splitCount,
                m_isCoordinatorAggr);
    }

    @Override
    public VoltDBPSerialAggregate copy(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            int splitCount,
            boolean isCoordinatorAggr) {
        return new VoltDBPSerialAggregate(
                cluster,
                traitSet,
                input,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                postPredicate,
                splitCount,
                isCoordinatorAggr);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("type", "serial");
        return pw;
    }

    protected AggregatePlanNode getAggregatePlanNode() {
        return new AggregatePlanNode();
    }

}
