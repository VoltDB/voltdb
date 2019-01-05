/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

package org.voltdb.plannerv2.rules.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistribution.Type;
import org.voltdb.plannerv2.rel.logical.VoltLogicalRel;
import org.voltdb.plannerv2.rel.logical.VoltLogicalTableScan;
import org.voltdb.plannerv2.rel.physical.VoltDBPRel;
import org.voltdb.plannerv2.rel.physical.VoltSeqTableScan;

/**
 * VoltDB physical rule that transform {@link VoltLogicalTableScan} to {@link VoltSeqTableScan}.
 * And add {@link AbstractVoltDBPExchange} on top of it.
 *
 * @author Michael Alexeev
 * @since 9.0
 */
public class VoltPSeqScanRule extends RelOptRule {

    public static final VoltPSeqScanRule INSTANCE = new VoltPSeqScanRule();

    private VoltPSeqScanRule() {
        super(operand(VoltLogicalTableScan.class, VoltLogicalRel.CONVENTION, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltLogicalTableScan tableScan = call.rel(0);
        RelTraitSet convertedTraits = tableScan.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL);

        // Table distribution
        RelDistribution tableDist = tableScan.getTable().getDistribution();
        int scanSplitCount = (Type.SINGLETON == tableDist.getType()) ?
                1 : Constants.DISTRIBUTED_SPLIT_COUNT;
        // TODO: we make the distribution trait here ALWAYS SINGLE because we only support SP in our initial release.
        // when come to MP, distribution trait can be HASH and Exchange nodes should be introduced to
        // handle this.
        VoltSeqTableScan scanRel = new VoltSeqTableScan(
                tableScan.getCluster(),
                convertedTraits,
                tableScan.getTable(),
                tableScan.getVoltTable(),
                scanSplitCount);

        call.transformTo(scanRel);
    }
}
