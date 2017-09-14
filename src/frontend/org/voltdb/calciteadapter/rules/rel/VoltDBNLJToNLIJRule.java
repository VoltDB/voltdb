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
import org.apache.calcite.rel.core.JoinRelType;
import org.voltdb.calciteadapter.rel.AbstractVoltDBTableScan;
import org.voltdb.calciteadapter.rel.VoltDBNLIJoin;
import org.voltdb.calciteadapter.rel.VoltDBNLJoin;
import org.voltdb.calciteadapter.rel.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.rel.VoltDBTableSeqScan;
import org.voltdb.calciteadapter.voltdb.IndexUtil;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.utils.CatalogUtil;

public class VoltDBNLJToNLIJRule extends RelOptRule {

    // TODO
    // 1. Match first operand should be any (not AbstractVoltDBTableScan)
    // 4. Inner node index expressions based on a combination of outer-inner and inner expressions

    public static final VoltDBNLJToNLIJRule INSTANCE = new VoltDBNLJToNLIJRule();

    private VoltDBNLJToNLIJRule() {
        super(operand(VoltDBNLJoin.class,
                // The first operand need to be any
                some(operand(AbstractVoltDBTableScan.class, none()),
                        operand(VoltDBTableSeqScan.class, none()))));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // 1. Do not need to be concerned with the outer node - Its filters are already pushed down and
        //      it will be converted to IndexScan if possible by the VoltDBSeqToIndexScansRule
        // 2. Calc access path for inner
        // 3. At this stage the join condition has outer-inner expressions only. Inner and Outer expressions
        // are already pushed down. If there is an access path its NLIJ

        VoltDBNLJoin join = call.rel(0);
        AbstractVoltDBTableScan outerScan = call.rel(1);
        VoltDBTableSeqScan innerScan = call.rel(2);

        int numLhsFieldsForJoin = outerScan.getRowType().getFieldCount();

        JoinRelType joinType = join.getJoinType();
        // INNER only at the moment
        if (joinType != JoinRelType.INNER) {
            return;
        }

        Table catTableable = innerScan.getVoltDBTable().getCatTable();
        List<Column> columns = CatalogUtil.getSortedCatalogItems(catTableable.getColumns(), "index");

        for (Index index : innerScan.getVoltDBTable().getCatTable().getIndexes()) {
            // @TODO: Potentially, there could be an expression index that is based on a combination of
            // outer-inner and outer filters. Need to take outer filters into an account.
            assert(innerScan.getProgram() != null);
            // need to pass the joinleftsize to the visitor
            AccessPath accessPath = IndexUtil.getCalciteRelevantAccessPathForIndex(
                    catTableable, columns, join.getCondition(), innerScan.getProgram().getExprList(), index, numLhsFieldsForJoin);

            // @TODO Adjust program based on the access path "other" filters
            if (accessPath != null) {
                VoltDBTableIndexScan indexScan = new VoltDBTableIndexScan(
                        innerScan.getCluster(),
                        innerScan.getTable(),
                        innerScan.getVoltDBTable(),
                        innerScan.getProgram(),
                        index,
                        accessPath,
                        innerScan.getLimitRexNode(),
                        innerScan.getOffsetRexNode());

                VoltDBNLIJoin nliJoin = new VoltDBNLIJoin(
                        join.getCluster(),
                        join.getTraitSet(),
                        outerScan,
                        indexScan,
                        join.getCondition(),
                        join.getVariablesSet(),
                        join.getJoinType(),
                        join.getProgram(),
                        index.getTypeName());

                call.transformTo(nliJoin);
            }

        }
    }

}