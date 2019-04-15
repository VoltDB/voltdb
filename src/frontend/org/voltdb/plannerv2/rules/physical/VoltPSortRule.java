/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.logical.VoltLogicalSort;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalRel;

/**
 * Convert LogicalSort to a collation that would be propagated by Calcite and recreated back
 * a VoltPhysicalSort at the later stage
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPSortRule extends RelOptRule {

    // TODO: I don't understand the purpose of this rule. I removed it from our ruleset.
    public static final VoltPSortRule INSTANCE = new VoltPSortRule();

    VoltPSortRule() {
        super(operand(VoltLogicalSort.class, VoltLogicalRel.CONVENTION, any()));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltLogicalSort sort = call.rel(0);
        // Can convert to the collation trait only if there is no limit/offset
        // The limit/offset should be separated to a RelNode during LogicalSort conversion
        return sort.offset == null && sort.fetch == null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltLogicalSort sort = call.rel(0);
        RelNode input = sort.getInput();
        RelTraitSet convertedTraits = sort.getTraitSet().plus(VoltPhysicalRel.CONVENTION);
        RelNode convertedInput = convert(input, convertedTraits);
        call.transformTo(convertedInput);
    }
}
