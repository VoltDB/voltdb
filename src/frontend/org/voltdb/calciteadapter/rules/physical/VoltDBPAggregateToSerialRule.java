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
import org.voltdb.calciteadapter.rel.physical.VoltDBPHashAggregate;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSerialAggregate;

public class VoltDBPAggregateToSerialRule extends RelOptRule {

    public static final VoltDBPAggregateToSerialRule INSTANCE = new VoltDBPAggregateToSerialRule();

    VoltDBPAggregateToSerialRule() {
        super(operand(VoltDBPHashAggregate.class, any()));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltDBPHashAggregate aggr = call.rel(0);
        return aggr.getGroupCount() != 0;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPHashAggregate aggr = call.rel(0);
        RelNode input = aggr.getInput();
        RelTraitSet traitSet = aggr.getTraitSet();

        if (input.getConvention() != VoltDBPRel.VOLTDB_PHYSICAL) {
            input = convert(input, input.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL));
        }
        RelCollation groupByCollation = buildGroupByCollation(aggr);
        VoltDBPSerialAggregate serialAggr = VoltDBPSerialAggregate.create(
                aggr.getCluster(),
                traitSet,
                input,
                aggr.indicator,
                aggr.getGroupSet(),
                aggr.getGroupSets(),
                aggr.getAggCallList(),
                groupByCollation,
                null);
        call.transformTo(serialAggr);
    }

    RelCollation buildGroupByCollation(VoltDBPHashAggregate aggr) {
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

}
