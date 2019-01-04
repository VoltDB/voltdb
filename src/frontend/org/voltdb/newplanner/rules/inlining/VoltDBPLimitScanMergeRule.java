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

package org.voltdb.newplanner.rules.inlining;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelNode;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPTableScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBPCalc;
import org.voltdb.calciteadapter.rel.physical.VoltDBPLimit;
import org.voltdb.calciteadapter.rel.physical.VoltDBPSerialAggregate;

public class VoltDBPLimitScanMergeRule extends RelOptRule {

    public static final VoltDBPLimitScanMergeRule INSTANCE_1 =
            new VoltDBPLimitScanMergeRule(operand(VoltDBPLimit.class,
                    operand(AbstractVoltDBPTableScan.class, none())));
    public static final VoltDBPLimitScanMergeRule INSTANCE_2 =
            new VoltDBPLimitScanMergeRule(operand(VoltDBPLimit.class,
                    operand(VoltDBPCalc.class,
                            operand(AbstractVoltDBPTableScan.class, none()))));

    /**
     * Transform  VoltDBPLimit / AbstractVoltDBPTableScan to AbstractVoltDBPTableScan with Limit
     * Transform  VoltDBPLimit / Calc / AbstractVoltDBPTableScan to Calc AbstractVoltDBPTableScan with Limit
     * If a scan has already inline aggregate, the LIMIT can be inlined only if it is the serial aggregate.
     * The Hash aggregate requires a full scan.
     */
    private VoltDBPLimitScanMergeRule(RelOptRuleOperand operand) {
        super(operand);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        int scanIdx = call.rels.length - 1;
        AbstractVoltDBPTableScan scan = call.rel(scanIdx);
        // Can not inline LIMIT / OFFSET if there is an inlined HASH aggregate
        // Serial aggregate is fine
        RelNode aggregate = scan.getAggregateRelNode();
        return aggregate instanceof VoltDBPSerialAggregate || aggregate == null;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBPLimit limitOffset = call.rel(0);
        int scanIdx = call.rels.length - 1;
        AbstractVoltDBPTableScan scan = call.rel(scanIdx);

        RelNode newRel = scan.copyWithLimitOffset(
                scan.getTraitSet(),
                limitOffset.getOffset(),
                limitOffset.getLimit());
        if (call.rels.length == 3) {
            VoltDBPCalc calc = call.rel(1);
            newRel = calc.copy(calc.getTraitSet(), newRel, calc.getProgram());
        }
        call.transformTo(newRel);
    }

}
