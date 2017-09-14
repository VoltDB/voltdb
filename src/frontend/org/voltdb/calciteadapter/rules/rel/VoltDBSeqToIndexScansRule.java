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

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.VoltDBTable;
import org.voltdb.calciteadapter.rel.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.rel.VoltDBTableSeqScan;
import org.voltdb.calciteadapter.voltdb.IndexUtil;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.utils.CatalogUtil;

public class VoltDBSeqToIndexScansRule extends RelOptRule {

    public static final VoltDBSeqToIndexScansRule INSTANCE = new VoltDBSeqToIndexScansRule();

    private VoltDBSeqToIndexScansRule() {
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

        Table catTableable = seqScan.getVoltDBTable().getCatTable();
        List<Column> columns = CatalogUtil.getSortedCatalogItems(catTableable.getColumns(), "index");

        for (Index index : seqScan.getVoltDBTable().getCatTable().getIndexes()) {
            RexProgram program = seqScan.getProgram();
            assert(program != null);
            AccessPath accessPath = IndexUtil.getCalciteRelevantAccessPathForIndex(
                    catTableable, columns, program.getCondition(), program.getExprList(), index);

            // @TODO Adjust program based on the access path "other" filters
            if (accessPath != null) {
                VoltDBTableIndexScan indexScan = new VoltDBTableIndexScan(
                        seqScan.getCluster(),
                        seqScan.getTable(),
                        seqScan.getVoltDBTable(),
                        seqScan.getProgram(),
                        index,
                        accessPath,
                        seqScan.getLimitRexNode(),
                        seqScan.getOffsetRexNode());
                call.transformTo(indexScan);
            }
        }
    }

}