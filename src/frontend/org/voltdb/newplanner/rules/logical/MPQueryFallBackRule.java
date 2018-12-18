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
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.sql.SqlKind;
import org.voltdb.calciteadapter.rel.logical.VoltDBLCalc;
import org.voltdb.calciteadapter.rel.logical.VoltDBLJoin;
import org.voltdb.calciteadapter.rel.logical.VoltDBLTableScan;

import java.util.List;

/**
 * Rule that fallback a query if it is multi-partitioned.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class MPQueryFallBackRule extends RelOptRule {
    public static final MPQueryFallBackRule INSTANCE_0 = new MPQueryFallBackRule(operand(VoltDBLCalc.class,
            operand(VoltDBLTableScan.class, any())), "MPQueryFallBackRule0");

    public static final MPQueryFallBackRule INSTANCE_1 = new MPQueryFallBackRule(operand(VoltDBLCalc.class,
            operand(VoltDBLJoin.class, any())), "MPQueryFallBackRule1");

    private MPQueryFallBackRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    /**
     * Helper function to decide whether the filtered result is located at a single partition.
     *
     * @param program       the program of a {@link LogicalCalc}.
     * @param rexNode       the condition of the program.
     * @param partitionKeys the list of partition keys for the target table.
     * @return true if the filtered result is located at a single partition.
     */
    private static boolean isSinglePartitioned(RexProgram program, RexNode rexNode, List<Integer> partitionKeys) {
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            SqlKind sqlKind = rexCall.getOperator().getKind();
            switch (sqlKind) {
                case EQUALS:
                    return isSinglePartitioned(program, rexCall.getOperands().get(0), partitionKeys) ||
                            isSinglePartitioned(program, rexCall.getOperands().get(1), partitionKeys);
                case AND:
                    return isSinglePartitioned(program, rexCall.getOperands().get(0), partitionKeys) ||
                            isSinglePartitioned(program, rexCall.getOperands().get(1), partitionKeys);
                case OR:
                    return isSinglePartitioned(program, rexCall.getOperands().get(0), partitionKeys) &&
                            isSinglePartitioned(program, rexCall.getOperands().get(1), partitionKeys);
                default:
                    return false;
            }
        }
        if (rexNode instanceof RexInputRef) {
            return partitionKeys.contains(((RexInputRef) rexNode).getIndex());
        }
        if (rexNode instanceof RexLocalRef) {
            return isSinglePartitioned(program,
                    program.getExprList().get(((RexLocalRef) rexNode).getIndex()), partitionKeys);
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        System.out.println("hit!!!!!1: ");
        System.out.println(call.rel(0).toString());
        VoltDBLCalc calc = call.rel(0);
        if (call.rel(1) instanceof VoltDBLTableScan) {
            VoltDBLTableScan tableScan = call.rel(1);
            RelDistribution tableDist = tableScan.getTable().getDistribution();
            if (tableDist.getType() != RelDistribution.Type.SINGLETON) {
                if (calc.getProgram().getCondition() == null ||
                        !isSinglePartitioned(calc.getProgram(), calc.getProgram().getCondition(), tableDist.getKeys())) {
                    throw new UnsupportedOperationException("MP query not supported in Calcite planner.");
                }
                // use in MPJoinQueryFallBackRule
                calc.setIsReplicated(false);
            }
        } else if (call.rel(1) instanceof VoltDBLJoin) {
            VoltDBLJoin join = call.rel(1);
            calc.setIsReplicated(join.getIsReplicated());
        }
    }
}

