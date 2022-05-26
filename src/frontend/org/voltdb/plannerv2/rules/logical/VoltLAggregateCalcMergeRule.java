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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.voltdb.plannerv2.rel.logical.VoltLogicalAggregate;
import org.voltdb.plannerv2.rel.logical.VoltLogicalCalc;

import java.util.List;

/**
 * Logical rule that eliminates LogicalAggregate node with SINGLE_VALUE aggregate function
 * that sits on top of a LogicalCalc node. {@link org.apache.calcite.sql.fun.SqlSingleValueAggFunction}
 */
public class VoltLAggregateCalcMergeRule extends RelOptRule {
    public static final VoltLAggregateCalcMergeRule INSTANCE = new VoltLAggregateCalcMergeRule();

    private VoltLAggregateCalcMergeRule() {
        super(operand(VoltLogicalAggregate.class, operand(VoltLogicalCalc.class, none())));
    }
    @Override
    public void onMatch(RelOptRuleCall call) {
        final List<AggregateCall> aggs = ((Aggregate) call.rel(0)).getAggCallList();
        if (! aggs.isEmpty() && aggs.get(0).getAggregation().getKind() == SqlKind.SINGLE_VALUE) {
            call.transformTo(call.rel(1));
        }
    }
}
