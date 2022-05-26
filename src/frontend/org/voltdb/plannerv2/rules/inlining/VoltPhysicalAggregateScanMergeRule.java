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
import org.apache.calcite.rel.RelNode;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalAggregate;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableScan;

/**
 * The rule that Merges/inline Aggregate into a Scan.
 *
 * @author mikealexeev
 */
public class VoltPhysicalAggregateScanMergeRule extends RelOptRule {

    public static final VoltPhysicalAggregateScanMergeRule INSTANCE = new VoltPhysicalAggregateScanMergeRule();

    private VoltPhysicalAggregateScanMergeRule() {
        super(operand(VoltPhysicalAggregate.class,
                operand(VoltPhysicalTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltPhysicalAggregate aggregate = call.rel(0);
        VoltPhysicalTableScan scan = call.rel(1);

        RelNode newScan = scan.copyWithAggregate(scan.getTraitSet().merge(aggregate.getTraitSet()), aggregate);
        call.transformTo(newScan);
    }
}
