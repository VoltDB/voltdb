/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter.rules.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.voltdb.calciteadapter.rel.VoltDBAggregate;
import org.voltdb.calciteadapter.rel.VoltDBTableIndexScan;

public class VoltDBAggregateScanRule extends RelOptRule {

    public static final VoltDBAggregateScanRule INSTANCE = new VoltDBAggregateScanRule();

    private VoltDBAggregateScanRule() {
        // Match LogicalAggregate or LogicalAggregateMerge
        super(operand(Aggregate.class, operand(VoltDBTableIndexScan.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        VoltDBTableIndexScan scan = call.rel(1);

        RelTraitSet aggrTraitSet = aggregate.getTraitSet();
        RelTraitSet scanTraitSet = scan.getTraitSet();

        RelCollation aggrCollation = (RelCollation) aggrTraitSet.getTrait(RelCollations.EMPTY.getTraitDef());
        RelCollation scanCollation = (RelCollation) scanTraitSet.getTrait(RelCollations.EMPTY.getTraitDef());
        boolean result = !scanCollation.getFieldCollations().isEmpty() &&
                aggrCollation.getFieldCollations().isEmpty();
        return result;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Aggregate aggregate = call.rel(0);
        VoltDBTableIndexScan scan = call.rel(1);

        RelTraitSet aggrTraitSet = aggregate.getTraitSet();
        RelTraitSet scanTraitSet = scan.getTraitSet();

        RelCollation aggrCollation = (RelCollation) aggrTraitSet.getTrait(RelCollations.EMPTY.getTraitDef());
        RelCollation scanCollation = (RelCollation) scanTraitSet.getTrait(RelCollations.EMPTY.getTraitDef());

        RelNode newAggregate = VoltDBAggregate.createFrom(
                aggregate,
                scan,
                scanCollation);
        call.transformTo(newAggregate);
    }
}