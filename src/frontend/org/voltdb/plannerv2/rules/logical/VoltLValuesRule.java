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

package org.voltdb.plannerv2.rules.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.logical.LogicalValues;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.logical.VoltLogicalValues;

/**
 * VoltDB logical rule that transform {@link LogicalValues} to {@link VoltLogicalValues}.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class VoltLValuesRule extends RelOptRule {
    public static final VoltLValuesRule INSTANCE = new VoltLValuesRule();

    private VoltLValuesRule() {
        super(operand(LogicalValues.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalValues values = call.rel(0);
        RelTraitSet convertedTraits = values.getTraitSet().replace(VoltLogicalRel.CONVENTION).simplify();

        call.transformTo(new VoltLogicalValues(values.getCluster(), convertedTraits, values.getRowType(), values.getTuples()));
    }
}
