/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

package org.voltdb.calciteadapter.rules.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.voltdb.calciteadapter.VoltDBTable;
import org.voltdb.calciteadapter.rel.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.rel.VoltDBTableSeqScan;
import org.voltdb.catalog.Index;

public class VoltDBSeqToIndexScans extends RelOptRule {

    public static final VoltDBSeqToIndexScans INSTANCE = new VoltDBSeqToIndexScans();

    private VoltDBSeqToIndexScans() {
        super(operand(VoltDBTableSeqScan.class, none()));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltDBTableSeqScan scan = call.rel(0);
        VoltDBTable table = scan.getVoltDBTable();
        assert(table != null);
        boolean matches = !table.getCatTable().getIndexes().isEmpty();
        return matches;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBTableSeqScan seqScan = call.rel(0);

        for (Index index : seqScan.getVoltDBTable().getCatTable().getIndexes()) {
            // @TODO filter indexes based on a program data
            VoltDBTableIndexScan indexScan = new VoltDBTableIndexScan(
                    seqScan.getCluster(),
                    seqScan.getTable(),
                    seqScan.getVoltDBTable(),
                    seqScan.getProgram(),
                    index,
                    seqScan.getLimitRexNode(),
                    seqScan.getOffsetRexNode());
            call.transformTo(indexScan);
        }
    }

}