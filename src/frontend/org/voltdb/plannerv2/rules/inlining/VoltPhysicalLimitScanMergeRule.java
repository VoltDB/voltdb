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

package org.voltdb.plannerv2.rules.inlining;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalLimit;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalSerialAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableScan;

/**
 * The rule that Merges/inline Limit into a Scan.
 *
 * @author mikealexeev
 */
public class VoltPhysicalLimitScanMergeRule extends RelOptRule {

    public static final VoltPhysicalLimitScanMergeRule INSTANCE_LIMIT_SCAN =
            new VoltPhysicalLimitScanMergeRule(operand(VoltPhysicalLimit.class,
                    operand(VoltPhysicalTableScan.class, none())));

    // TODO: This one seems redundant, we first apply the VoltPhysicalCalcScanMergeRule
    // to Transform VoltDBPLimit / Calc / AbstractVoltDBPTableScan to VoltDBPLimit / AbstractVoltDBPTableScan,
    // than it goes to the #INSTANCE_SORT_SCAN pattern
    public static final VoltPhysicalLimitScanMergeRule INSTANCE_2 =
            new VoltPhysicalLimitScanMergeRule(operand(VoltPhysicalLimit.class,
                    operand(VoltPhysicalCalc.class,
                            operand(VoltPhysicalTableScan.class, none()))));

    /**
     * Transform  VoltDBPLimit / AbstractVoltDBPTableScan to AbstractVoltDBPTableScan with Limit
     * Transform  VoltDBPLimit / Calc / AbstractVoltDBPTableScan to Calc AbstractVoltDBPTableScan with Limit
     * If a scan has already inline aggregate, the LIMIT can be inlined only if it is the serial aggregate.
     * The Hash aggregate requires a full scan.
     */
    private VoltPhysicalLimitScanMergeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        int scanIdx = call.rels.length - 1;
        VoltPhysicalTableScan scan = call.rel(scanIdx);
        // Can not inline LIMIT / OFFSET if there is an inlined HASH aggregate
        // Serial aggregate is fine
        RelNode aggregate = scan.getAggregateRelNode();
        return aggregate instanceof VoltPhysicalSerialAggregate || aggregate == null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltPhysicalLimit limitOffset = call.rel(0);
        int scanIdx = call.rels.length - 1;
        VoltPhysicalTableScan scan = call.rel(scanIdx);

        RelNode newRel = scan.copyWithLimitOffset(
                scan.getTraitSet(),
                limitOffset.getOffset(),
                limitOffset.getLimit());
        if (call.rels.length == 3) {
            VoltPhysicalCalc calc = call.rel(1);
            newRel = calc.copy(calc.getTraitSet(), newRel, calc.getProgram());
        }
        call.transformTo(newRel);
    }
}
