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
import java.util.Map;

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
import org.voltdb.calciteadapter.rel.logical.VoltDBLAggregate;
import org.voltdb.calciteadapter.rel.logical.VoltDBLRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPHashAggregate;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSerialAggregate;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSort;

import com.google.common.collect.ImmutableMap;


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
                    null,
                    false);
            call.transformTo(hashAggr);
        }

        // Transform to a physical Serial Aggregate. To enforce a required ordering add a collation
        // that matches the aggreagte's GROUP BY columns (for now) to the aggregate's input.
        // Calcite will create a sort relation out of it.
        // The aggregate's output would also be effectively sorted by the same GROUP BY columns
        // (either because of an existing index or a Sort relation added by Calcite)
        if (hasGroupBy(aggregate)) {
            RelCollation groupByCollation = buildGroupByCollation(aggregate);
            convertedInputTraits = convertedInputTraits.plus(groupByCollation);
            convertedAggrTraits = convertedAggrTraits.plus(groupByCollation);
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
                null,
                false);
        // The fact that the convertedAggrTraits does have non-empty collation would force Calcite to create
        // a Sort relation on top of the aggregate. We can add the sort ourselves and also declare
        // that both (a new sort and the serial aggregate) relations are equivalent to the original
        // logical aggregate. This way Caclite will later eliminate the added sort because if would have
        // higher processing cost
        if (hasGroupBy(aggregate)) {
            VoltDBPSort sort = new VoltDBPSort(
              aggregate.getCluster(),
              convertedAggrTraits,
              serialAggr,
              convertedAggrTraits.getTrait(RelCollationTraitDef.INSTANCE),
              1);
            Map<RelNode, RelNode> equiv = ImmutableMap.of(serialAggr, aggregate);
            call.transformTo(sort, equiv);
        } else {
            call.transformTo(serialAggr);
        }

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
