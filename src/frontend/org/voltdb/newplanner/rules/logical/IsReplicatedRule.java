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

package org.voltdb.newplanner.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.voltdb.calciteadapter.rel.logical.VoltDBLCalc;
import org.voltdb.calciteadapter.rel.logical.VoltDBLJoin;
import org.voltdb.calciteadapter.rel.logical.VoltDBLTableScan;
import org.voltdb.newplanner.rules.physical.VoltDBPSeqScanRule;

public class IsReplicatedRule extends RelOptRule {
    public static final IsReplicatedRule INSTANCE_0 = new IsReplicatedRule(operand(VoltDBLCalc.class,
            operand(VoltDBLJoin.class, any())));

    public static final IsReplicatedRule INSTANCE_1 = new IsReplicatedRule(operand(VoltDBLJoin.class,
            operand(VoltDBLCalc.class, any())));

    public static final IsReplicatedRule INSTANCE_2 = new IsReplicatedRule(operand(VoltDBLJoin.class,
            operand(VoltDBLTableScan.class, any())));

    private IsReplicatedRule(RelOptRuleOperand operand) {
        super(operand);
    }

    @Override
    public void onMatch(RelOptRuleCall call){

    }
}
