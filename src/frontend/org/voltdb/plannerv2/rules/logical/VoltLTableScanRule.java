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
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.logical.VoltLogicalTableScan;


public class VoltLTableScanRule extends RelOptRule {

    public static final VoltLTableScanRule INSTANCE = new VoltLTableScanRule();

    private VoltLTableScanRule() {
        super(operand(VoltLogicalTableScan.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltLogicalTableScan tableScan = call.rel(0);
        RelTraitSet convertedTraits = tableScan.getTraitSet().replace(VoltLogicalRel.CONVENTION);
        // The only change is replace(VoltDBLRel.VOLTDB_LOGICAL)
        call.transformTo(new VoltLogicalTableScan(
                tableScan.getCluster(),
                convertedTraits,
                tableScan.getTable(),
                tableScan.getVoltTable()));
    }
  }
