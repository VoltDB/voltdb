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

import java.util.List;

import org.aeonbits.owner.util.Collections;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.calciteadapter.rel.logical.VoltDBLRel;
import org.voltdb.calciteadapter.rel.logical.VoltDBLTableScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBPRel;
import org.voltdb.calciteadapter.rel.physical.VoltDBPTableSeqScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBPUnionExchange;
import org.voltdb.catalog.Column;


public class VoltDBPSeqScanRule extends RelOptRule {

    private static final int DISTRIBUTED_SPLIT_COUNT = 30;

    public static final VoltDBPSeqScanRule INSTANCE = new VoltDBPSeqScanRule();

    private VoltDBPSeqScanRule() {
        super(operand(VoltDBLTableScan.class, VoltDBLRel.VOLTDB_LOGICAL, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBLTableScan tableScan = (VoltDBLTableScan) call.rel(0);
        RelTraitSet convertedTraits = tableScan.getTraitSet().replace(VoltDBPRel.VOLTDB_PHYSICAL);
        VoltDBTable voltTable = tableScan.getVoltDBTable();
        if (voltTable.getCatTable().getIsreplicated()) {
            int scanSplitCount = 1;
            call.transformTo(new VoltDBPTableSeqScan(
                                tableScan.getCluster(),
                                convertedTraits,
                                tableScan.getTable(),
                                tableScan.getVoltDBTable(),
                                scanSplitCount));
        } else {
            // Table is partitioned. Add UnionExchange rel on top
            // @TODO Must be number of hosts * number of sites per host
            int scanSplitCount = DISTRIBUTED_SPLIT_COUNT;
            VoltDBPTableSeqScan scanRel = new VoltDBPTableSeqScan(
                    tableScan.getCluster(),
                    convertedTraits,
                    tableScan.getTable(),
                    tableScan.getVoltDBTable(),
                    scanSplitCount);
            // VoltDB uses HASH partitioning only
            Column partitionColumn = voltTable.getCatTable().getPartitioncolumn();
            List<Integer> partitionColumnIds = Collections.list(partitionColumn.getIndex());
            RelDistribution hashDist = RelDistributions.hash(partitionColumnIds);

            VoltDBPUnionExchange exchangeRel = new VoltDBPUnionExchange(
                    tableScan.getCluster(),
                    convertedTraits,
                    scanRel,
                    hashDist,
                    scanSplitCount);
            call.transformTo(exchangeRel);
        }
    }
  }
