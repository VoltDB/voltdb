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

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.plannodes.AggregatePlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;

import java.util.List;

public class VoltPhysicalHashAggregate extends VoltPhysicalAggregate {

    /**
     * Constructor
     */
    public VoltPhysicalHashAggregate(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode child,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode postPredicate,
            boolean isCoordinatorAggr) {
        super(cluster,
                traitSet,
                child,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                postPredicate,
                isCoordinatorAggr);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("type", "hash");
        return pw;
    }

    @Override
    public VoltPhysicalHashAggregate copy(RelTraitSet traitSet, RelNode input,
                                          boolean indicator, ImmutableBitSet groupSet,
                                          List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
        return new VoltPhysicalHashAggregate(
                getCluster(),
                traitSet,
                input,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                getPostPredicate(),
                getIsCoordinatorAggr());
    }

    @Override
    public VoltPhysicalHashAggregate copy(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            boolean indicator,
            ImmutableBitSet groupSet,
            List<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls,
            RexNode havingExpression,
            boolean isCoordinatorAggr) {
        return new VoltPhysicalHashAggregate(
                cluster,
                traitSet,
                input,
                indicator,
                groupSet,
                groupSets,
                aggCalls,
                havingExpression,
                isCoordinatorAggr);

    }

    @Override
    protected AggregatePlanNode getAggregatePlanNode() {
        return new HashAggregatePlanNode();
    }
}
