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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;

public class VoltDBPHashAggregate extends AbstractVoltDBPAggregate {

    /** Constructor */
    public VoltDBPHashAggregate(
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

    public VoltDBPHashAggregate(
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
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("type", "hash");
        return pw;
    }

    @Override
    public VoltDBPHashAggregate copy(RelTraitSet traitSet, RelNode input,
            boolean indicator, ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new VoltDBPHashAggregate(
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

    public VoltDBPHashAggregate copy(RelTraitSet traitSet,
            RelNode input,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            int splitCount,
            boolean isCoordinatorAggr
            ) {
        return new VoltDBPHashAggregate(
                getCluster(),
                traitSet,
                input,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                m_postPredicate,
                splitCount,
                isCoordinatorAggr);
    }


    @Override
    public VoltDBPHashAggregate copy(
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
        return new VoltDBPHashAggregate(
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

    protected AggregatePlanNode getAggregatePlanNode() {
        return new HashAggregatePlanNode();
    }

}
