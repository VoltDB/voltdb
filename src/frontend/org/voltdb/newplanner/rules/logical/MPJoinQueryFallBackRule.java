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
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.JoinProjectTransposeRule;
import org.voltdb.calciteadapter.rel.logical.VoltDBLCalc;
import org.voltdb.calciteadapter.rel.logical.VoltDBLJoin;
import org.voltdb.calciteadapter.rel.logical.VoltDBLTableScan;

public class MPJoinQueryFallBackRule extends RelOptRule {
    public static final MPJoinQueryFallBackRule INSTANCE_0 =
            new MPJoinQueryFallBackRule(
                    operand(VoltDBLJoin.class,
                            operand(VoltDBLCalc.class, any()),
                            operand(VoltDBLCalc.class, any())), "MPJoinQueryFallBackRule0");

    public static final MPJoinQueryFallBackRule INSTANCE_1 =
            new MPJoinQueryFallBackRule(
                    operand(VoltDBLJoin.class,
                            operand(VoltDBLTableScan.class, any()),
                            operand(VoltDBLTableScan.class, any())), "MPJoinQueryFallBackRule1");

    // TODO: check unordered()?
    public static final MPJoinQueryFallBackRule INSTANCE_2 =
            new MPJoinQueryFallBackRule(
                    operand(VoltDBLJoin.class,
                            operand(VoltDBLCalc.class, any()),
                            operand(VoltDBLTableScan.class, any())), "MPJoinQueryFallBackRule2");

    public static final MPJoinQueryFallBackRule INSTANCE_3 =
            new MPJoinQueryFallBackRule(
                    operand(VoltDBLJoin.class,
                            operand(VoltDBLTableScan.class, any()),
                            operand(VoltDBLCalc.class, any())), "MPJoinQueryFallBackRule3");

    private MPJoinQueryFallBackRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    private boolean isReplicated(RelNode node) {
        if (node instanceof VoltDBLCalc) {
            return ((VoltDBLCalc) node).getIsReplicated();
        } else if (node instanceof VoltDBLTableScan) {
            RelDistribution tableDist = node.getTable().getDistribution();
            return tableDist.getType() == RelDistribution.Type.SINGLETON;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBLJoin join = call.rel(0);
        boolean leftIsReplicated = isReplicated(call.rel(1));
        boolean rightIsReplicated = isReplicated(call.rel(2));
        System.out.println("hit: "+leftIsReplicated+"  "+rightIsReplicated);
        System.out.println(call.rel(1).toString());
        System.out.println(call.rel(2).toString());
        if(leftIsReplicated && rightIsReplicated){
            join.setIsReplicated(true);
        } else if(leftIsReplicated || rightIsReplicated){
            join.setIsReplicated(false);
        } else {
            System.out.println("throw");
            throw new UnsupportedOperationException("MP query not supported in Calcite planner.");
        }
    }
}
