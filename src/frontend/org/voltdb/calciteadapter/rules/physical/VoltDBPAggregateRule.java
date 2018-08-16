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

package org.voltdb.calciteadapter.rules.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.voltdb.calciteadapter.rel.logical.VoltDBLAggregate;
import org.voltdb.calciteadapter.rel.logical.VoltDBLRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPHashAggregate;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSerialAggregate;


public class VoltDBPAggregateRule extends RelOptRule {

    public static final VoltDBPAggregateRule INSTANCE = new VoltDBPAggregateRule();

    private VoltDBPAggregateRule() {
        super(operand(VoltDBLAggregate.class, VoltDBLRel.VOLTDB_LOGICAL, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBLAggregate aggregate = call.rel(0);
        RelTraitSet convertedAggrTraits = aggregate.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL).simplify();

        RelNode input = aggregate.getInput();
        RelTraitSet convertedInputTraits = input.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL).simplify();
        RelNode convertedInput = convert(input, convertedInputTraits);

        if (needHashAggregator(aggregate)) {
            // Transform to a physical Hash Aggregate
            VoltDBPHashAggregate hashAggr = new VoltDBPHashAggregate(
                    aggregate.getCluster(),
                    convertedAggrTraits,
                    convertedInput,
                    aggregate.indicator,
                    aggregate.getGroupSet(),
                    aggregate.getGroupSets(),
                    aggregate.getAggCallList(),
                    null);
            call.transformTo(hashAggr);
        }

        // Transform to a physical Serial Aggregate. To enforce a required ordering add a collation
        // that matches the aggreagte's GROUP BY columns (for now) to the aggregate's input.
        // Calcite will create a sort relation out of it
        // The Serial Aggregate itself should not have a RelCollation trait even its output is sorted
        // The presence of this trait would force Clacite to add a Sort on top of the aggregate
        // which we don't need
        if (hasGroupBy(aggregate)) {
            RelCollation groupByCollation = buildGroupByCollation(aggregate);
            convertedInputTraits = convertedInputTraits.plus(groupByCollation);
        }
        RelNode convertedSerialAggrInput = convert(input, convertedInputTraits);
        VoltDBPSerialAggregate serialAggr = new VoltDBPSerialAggregate(
                aggregate.getCluster(),
                convertedAggrTraits,
                convertedSerialAggrInput,
                aggregate.indicator,
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                aggregate.getAggCallList(),
                null);
        call.transformTo(serialAggr);

    }

    RelCollation buildGroupByCollation(VoltDBLAggregate aggr) {
        // Build a collation that represents each GROUP BY expression.
        // This collation implies that this serial aggregate requires its input
        // to be sorted in an order that is one of permutations of the fields from this collation
        ImmutableBitSet groupBy = aggr.getGroupSet();
        List<RelDataTypeField> rowTypeList = aggr.getRowType().getFieldList();
        List<RelFieldCollation> collationFields = new ArrayList<>();
        for (int index = groupBy.nextSetBit(0); index != -1; index = groupBy.nextSetBit(index + 1)) {
            assert(index < rowTypeList.size());
            collationFields.add(new RelFieldCollation(index));
        }

        return RelCollations.of(collationFields.toArray(new RelFieldCollation[collationFields.size()]));
    }

    private boolean needHashAggregator(VoltDBLAggregate aggr) {
        // A hash is required to build up per-group aggregates in parallel vs.
        // when there is only one aggregation over the entire table
        return hasGroupBy(aggr);
    }

    private boolean hasGroupBy(VoltDBLAggregate aggr) {
        return !aggr.getGroupSet().isEmpty();
    }
}
