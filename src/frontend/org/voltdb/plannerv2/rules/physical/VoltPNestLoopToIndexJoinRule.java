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

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexProgram;
import org.json_voltpatches.JSONException;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.plannerv2.guards.CalcitePlanningException;
import org.voltdb.plannerv2.rel.physical.*;
import org.voltdb.plannerv2.utils.IndexUtil;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.types.SortDirectionType;
import org.voltdb.utils.CatalogUtil;

import com.google.common.collect.ImmutableList;

public class VoltPNestLoopToIndexJoinRule extends RelOptRule{

    public static final RelOptRule INSTANCE_NLOOP_SEQSCAN = new VoltPNestLoopToIndexJoinRule(
            operand(VoltPhysicalNestLoopJoin.class,
                    some(operand(RelNode.class, any()), operand(VoltPhysicalTableSequentialScan.class, none()))),
            "VoltDBPNestLoopToIndexJoin_1");

    public static final RelOptRule INSTANCE_NLOOP_CALC_SEQSCAN = new VoltPNestLoopToIndexJoinRule(
            operand(VoltPhysicalNestLoopJoin.class,
                    some(operand(RelNode.class, any()),
                            operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableSequentialScan.class, none())))),
            "VoltDBPNestLoopToIndexJoin_2");

    private VoltPNestLoopToIndexJoinRule(RelOptRuleOperand operand, String dsc) {
        super(operand, dsc);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // At this moment, the join condition contains only outer-inner expressions. The inner and outer ones
        // are supposed to be pushed down already.
        //
        // If there is an index that can be satisfied by the join condition, it will be a NLIJ

        final VoltPhysicalJoin join = call.rel(0);
        final RelNode outerScan = call.rel(1);
        final VoltPhysicalTableScan innerScan;
        Calc innerCalc = null;
        if (call.rels.length == 3) {
            innerScan = call.rel(2);
        } else {
            innerCalc = call.rel(2);
            innerScan = call.rel(3);
        }

        final JoinRelType joinType = join.getJoinType();
        // INNER only at the moment
        if (joinType != JoinRelType.INNER) {
            return;
        }

        final Table table = innerScan.getVoltTable().getCatalogTable();
        // Combine inner calc and scan programs
        final RexProgram innerProgram;
        if (call.rels.length == 3) {
            innerProgram = innerScan.getProgram();
        } else {
            assert innerCalc != null;
            innerProgram = VoltRexUtil.mergeProgram(
                    innerScan.getProgram(), innerCalc.getProgram(), innerCalc.getCluster().getRexBuilder());
        }
        RelNode nliJoin = null;
        final Map<RelNode, RelNode> equiv = new HashMap<>();

        for (Index index : innerScan.getVoltTable().getCatalogTable().getIndexes()) {
            assert(innerScan.getProgram() != null);
            // need to pass the join outer child columns count to the visitor
            final AccessPath accessPath = IndexUtil.getCalciteRelevantAccessPathForIndex(
                    table, CatalogUtil.getSortedCatalogItems(table.getColumns(), "index"),
                    join.getCondition(), innerProgram, index, SortDirectionType.INVALID,
                    outerScan.getRowType().getFieldCount(), true);

            if (accessPath != null) {
                // Index's collation needs to be based on its own program only - the Calc sits above the scan
                final RelCollation indexCollation;
                try {
                    indexCollation = VoltRexUtil.createIndexCollation(
                            index, table, innerScan.getCluster().getRexBuilder(), innerScan.getProgram());
                } catch (JSONException e) {
                    throw new CalcitePlanningException(e.getMessage());
                }

                final TableScan indexScan = new VoltPhysicalTableIndexScan(
                        innerScan.getCluster(), innerScan.getTraitSet(), innerScan.getTable(), innerScan.getVoltTable(),
                        innerScan.getProgram(), index, accessPath, innerScan.getLimitRexNode(), innerScan.getOffsetRexNode(),
                        null, null, null, innerScan.getSplitCount(),
                        indexCollation);
                final RelNode innerChild;
                if (call.rels.length == 3) {
                    innerChild = indexScan;
                } else {
                    innerChild = innerCalc.copy(innerCalc.getTraitSet(), indexScan, innerCalc.getProgram());
                }
                final Join nextNliJoin = new VoltPhysicalNestLoopIndexJoin(
                        join.getCluster(), join.getTraitSet(), outerScan, innerChild, join.getCondition(),
                        join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
                        ImmutableList.copyOf(join.getSystemFieldList()), join.getSplitCount(), index.getTypeName());
                if (nliJoin == null) {
                    nliJoin = nextNliJoin;
                } else {
                    equiv.put(nextNliJoin, join);
                }
            }
        }
        if (nliJoin != null) {
            call.transformTo(nliJoin, equiv);
        }
    }
}
