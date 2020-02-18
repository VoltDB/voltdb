/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

package org.voltdb.plannerv2.rules.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.plannerv2.rel.logical.VoltLogicalAggregate;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalHashAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSerialAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSort;

import com.google.common.collect.ImmutableMap;
import com.google_voltpatches.common.base.Preconditions;


/**
 * VoltDB physical rule that transform {@link VoltLogicalAggregate} to {@link VoltPhysicalHashAggregate}
 * or {@link VoltPhysicalSerialAggregate}.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPAggregateRule extends RelOptRule {

    public static final VoltPAggregateRule INSTANCE = new VoltPAggregateRule();

    private VoltPAggregateRule() {
        super(operand(VoltLogicalAggregate.class, VoltLogicalRel.CONVENTION, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltLogicalAggregate aggregate = call.rel(0);
        RelTraitSet convertedAggrTraits = aggregate.getTraitSet().replace(VoltPhysicalRel.CONVENTION).simplify();

        final RelNode input = aggregate.getInput();
        RelTraitSet convertedInputTraits = input.getTraitSet().replace(VoltPhysicalRel.CONVENTION).simplify();
        final RelNode convertedInput = convert(input, convertedInputTraits);

        if (needHashAggregator(aggregate)) {
            // Transform to a physical Hash Aggregate
            call.transformTo(new VoltPhysicalHashAggregate(
                    aggregate.getCluster(), convertedAggrTraits, convertedInput, aggregate.indicator,
                    aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList(), null,
                    false));
            // We can't early return here, cause
            // hash aggregation are replaced with serial aggregation in large query
        }

        // Transform to a physical Serial Aggregate. To enforce a required ordering add a collation
        // that matches the aggreagte's GROUP BY columns (for now) to the aggregate's input.
        // Calcite will create a sort relation out of it.
        // The aggregate's output would also be effectively sorted by the same GROUP BY columns
        // (either because of an existing index or a Sort relation added by Calcite)
        if (hasGroupBy(aggregate)) {
            final RelCollation groupByCollation = buildGroupByCollation(aggregate);
            convertedInputTraits = convertedInputTraits.plus(groupByCollation);
            convertedAggrTraits = convertedAggrTraits.plus(groupByCollation);
        }
        final RelNode serialAggr = new VoltPhysicalSerialAggregate(
                aggregate.getCluster(), convertedAggrTraits, convert(input, convertedInputTraits), aggregate.indicator,
                aggregate.getGroupSet(), aggregate.getGroupSets(), aggregate.getAggCallList(), null,
                false);
        // The fact that the convertedAggrTraits does have non-empty collation would force Calcite to create
        // a Sort relation on top of the aggregate. We can add the sort ourselves and also declare
        // that both (a new sort and the serial aggregate) relations are equivalent to the original
        // logical aggregate. This way Calcite will later eliminate the added sort because it would have
        // higher processing cost
        if (hasGroupBy(aggregate)) {
            final VoltPhysicalSort sort = new VoltPhysicalSort(
                    aggregate.getCluster(), convertedAggrTraits, serialAggr,
                    convertedAggrTraits.getTrait(RelCollationTraitDef.INSTANCE), false);
            call.transformTo(sort, ImmutableMap.of(serialAggr, aggregate));
        } else {
            call.transformTo(serialAggr);
        }

    }

    RelCollation buildGroupByCollation(VoltLogicalAggregate aggr) {
        // Build a collation that represents each GROUP BY expression.
        // This collation implies that this serial aggregate requires its input
        // to be sorted in an order that is one of permutations of the fields from this collation
        ImmutableBitSet groupBy = aggr.getGroupSet();
        List<RelDataTypeField> rowTypeList = aggr.getRowType().getFieldList();
        List<RelFieldCollation> collationFields = new ArrayList<>();
        for (int index = groupBy.nextSetBit(0); index != -1;
             index = groupBy.nextSetBit(index + 1)) {
            Preconditions.checkState(index < rowTypeList.size());
            collationFields.add(new RelFieldCollation(index));
        }
        return RelCollations.of(collationFields.toArray(new RelFieldCollation[collationFields.size()]));
    }

    private boolean needHashAggregator(VoltLogicalAggregate aggr) {
        // A hash is required to build up per-group aggregates in parallel vs.
        // when there is only one aggregation over the entire table

        // TODO: should be `! isLargeQueryMode() && hasGroupBy(aggr);`
        // update when we have introduce large query mode in calcite.
        return hasGroupBy(aggr);
    }

    private boolean hasGroupBy(VoltLogicalAggregate aggr) {
        return !aggr.getGroupSet().isEmpty();
    }
}
