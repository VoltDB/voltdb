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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.voltdb.calciteadapter.rel.physical.AbstractVoltDBPhysicalTableScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBLimit;

public class VoltDBLimitScanMergeRule extends RelOptRule {

    public static final VoltDBLimitScanMergeRule INSTANCE = new VoltDBLimitScanMergeRule();

    private VoltDBLimitScanMergeRule() {
        super(operand(VoltDBLimit.class,
                operand(AbstractVoltDBPhysicalTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBLimit limitOffset = call.rel(0);
        AbstractVoltDBPhysicalTableScan scan = call.rel(1);

        RelNode newScan = scan.copy(scan.getTraitSet(), limitOffset.getOffset(), limitOffset.getLimit());
        call.transformTo(newScan);
    }

}
