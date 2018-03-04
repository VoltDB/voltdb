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

package org.voltdb.calciteadapter.rules.physical;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.VoltDBTable;
import org.voltdb.calciteadapter.rel.physical.VoltDBFilter;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.rel.physical.VoltDBTableSeqScan;
import org.voltdb.calciteadapter.voltdb.IndexUtil;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.utils.CatalogUtil;

public class VoltDBIndexScansRule extends RelOptRule {

    public static final VoltDBIndexScansRule INSTANCE = new VoltDBIndexScansRule();

    private VoltDBIndexScansRule() {
        super(operand(VoltDBFilter.class, operand(VoltDBTableSeqScan.class, none())));
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        VoltDBTableSeqScan scan = call.rel(1);
        VoltDBTable table = scan.getVoltDBTable();
        assert(table != null);
        boolean matches = !table.getCatTable().getIndexes().isEmpty();
        return matches;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Filter filter = call.rel(0);
        VoltDBTableSeqScan scan = call.rel(1);

        RexProgram program = scan.getProgram();
        assert(program != null);
        Table catTableable = scan.getVoltDBTable().getCatTable();
        List<Column> columns = CatalogUtil.getSortedCatalogItems(catTableable.getColumns(), "index");

        RexNode filterCondition = filter.getCondition();

        for (Index index : catTableable.getIndexes()) {
            AccessPath accessPath = IndexUtil.getCalciteRelevantAccessPathForIndex(
                    catTableable, columns, filterCondition, program, index);

            // @TODO Adjust program based on the access path "other" filters
            if (accessPath != null) {
                // if accessPath.other is not null, need to create a new Filter
                VoltDBTableIndexScan indexScan = new VoltDBTableIndexScan(
                        scan.getCluster(),
                        scan.getTraitSet(),
                        scan.getTable(),
                        scan.getVoltDBTable(),
                        scan.getProgram(),
                        index,
                        accessPath,
                        scan.getLimitRexNode(),
                        scan.getOffsetRexNode());
                call.transformTo(indexScan);
            }
        }
    }

}