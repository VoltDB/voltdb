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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexLocalRef;
import org.voltdb.calciteadapter.RexConverter;
import org.voltdb.calciteadapter.rel.VoltDBNLIJoin;
import org.voltdb.calciteadapter.rel.VoltDBNLJoin;
import org.voltdb.calciteadapter.rel.VoltDBTableIndexScan;
import org.voltdb.calciteadapter.rel.VoltDBTableSeqScan;
import org.voltdb.calciteadapter.voltdb.IndexUtil;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AccessPath;
import org.voltdb.utils.CatalogUtil;

public class VoltDBNLJToNLIJRule extends RelOptRule {

    public static final VoltDBNLJToNLIJRule INSTANCE = new VoltDBNLJToNLIJRule();

    private VoltDBNLJToNLIJRule() {
        super(operand(VoltDBNLJoin.class,
                some(operand(RelNode.class, any()),
                        operand(VoltDBTableSeqScan.class, none()))));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // At this moment, the join condition contains only outer-inner expressions. The inner and outer ones
        // are supposed to be pushed down already.
        //
        // If there is an index that can be satisfied by the join condition, it will be a NLIJ

        VoltDBNLJoin join = call.rel(0);
        RelNode outerScan = call.rel(1);
        VoltDBTableSeqScan innerScan = call.rel(2);

        int numLhsFieldsForJoin = outerScan.getRowType().getFieldCount();

        JoinRelType joinType = join.getJoinType();
        // INNER only at the moment
        if (joinType != JoinRelType.INNER) {
            return;
        }

        Table catTableable = innerScan.getVoltDBTable().getCatTable();
        List<Column> columns = CatalogUtil.getSortedCatalogItems(catTableable.getColumns(), "index");
        // Filter out table columns that are not part of the projection
        List<RexLocalRef> projections = innerScan.getProgram().getProjectList();
        List<Column> projectedColumns = columns;
        if (projections != null && !projections.isEmpty()) {
            Set<Integer> columnIdexes = new HashSet<>();
            projectedColumns = new ArrayList<>();
            for (RexLocalRef projection : projections) {
                AbstractExpression ae = RexConverter.convertRefExpression(
                        projection, catTableable.getTypeName(), columns, innerScan.getProgram().getExprList(), -1);
                List<TupleValueExpression> tves = ae.findAllTupleValueSubexpressions();
                for(TupleValueExpression tve : tves) {
                    columnIdexes.add(tve.getColumnIndex());
                }
            }
            for (int index : columnIdexes) {
                projectedColumns.add(columns.get(index));
            }
        }

        // Convert an inner expression to be ready to added to a potential access path
        // as the OTHER expression.
        // If we add this expression to a list of potential candidates for an index we would need to
        // ignore indexes that solely based on the inner expressions. We are after the access path with
        // the outer-inner index expression - we need NLIJ and not the NLJ/IndeScan. The latter will be
        // handled via a different rule.
        RexLocalRef otherCondition = innerScan.getProgram().getCondition();
        AbstractExpression innerFilterExpr = null;
        if (otherCondition != null) {
            innerFilterExpr = RexConverter.convertRefExpression(
                    otherCondition, catTableable.getTypeName(), columns, innerScan.getProgram().getExprList(), -1);
        }

        for (Index index : innerScan.getVoltDBTable().getCatTable().getIndexes()) {
            assert(innerScan.getProgram() != null);
            // need to pass the joinleftsize to the visitor
            AccessPath accessPath = IndexUtil.getCalciteRelevantAccessPathForIndex(
                    catTableable, projectedColumns, join.getCondition(), innerScan.getProgram().getExprList(), index, numLhsFieldsForJoin);

            // @TODO Adjust program based on the access path "other" filters
            if (accessPath != null) {
                // Add the inner expression as an additional filter
                if (innerFilterExpr != null) {
                    accessPath.getOtherExprs().add(innerFilterExpr);
                }
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