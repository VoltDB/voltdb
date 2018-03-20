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

package org.voltdb.calciteadapter.rules.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.voltdb.calciteadapter.rel.logical.VoltDBLogicalRel;
import org.voltdb.calciteadapter.rel.logical.VoltDBLogicalTableScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBPhysicalRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBProject;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableSeqScan;


public class VoltDBLogicalTableScanRule extends RelOptRule {

    public static final VoltDBLogicalTableScanRule INSTANCE = new VoltDBLogicalTableScanRule();

    private VoltDBLogicalTableScanRule() {
        super(operand(VoltDBLogicalTableScan.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBLogicalTableScan tableScan = (VoltDBLogicalTableScan) call.rel(0);
        RelTraitSet convertedTraits = tableScan.getTraitSet().replace(VoltDBLogicalRel.VOLTDB_LOGICAL);
        call.transformTo(new VoltDBLogicalTableScan(
                tableScan.getCluster(),
                convertedTraits,
                tableScan.getTable(),
                tableScan.getVoltDBTable()));
    }
  }
