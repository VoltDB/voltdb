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
import java.util.Optional;
import java.util.stream.StreamSupport;

import com.google_voltpatches.common.base.Preconditions;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
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

    public static final RelOptRule INSTANCE_SSCAN = new VoltPNestLoopToIndexJoinRule(
            operand(VoltPhysicalNestLoopJoin.class,
                    some(operand(RelNode.class, any()), operand(VoltPhysicalTableSequentialScan.class, none()))),
            "VoltPNestLoopToIndexJoin_NL_SScan");

    public static final RelOptRule INSTANCE_CALC_SSCAN = new VoltPNestLoopToIndexJoinRule(
            operand(VoltPhysicalNestLoopJoin.class,
                    some(operand(RelNode.class, any()),
                            operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableSequentialScan.class, none())))),
            "VoltPNestLoopToIndexJoin_Calc_SScan");

    private VoltPNestLoopToIndexJoinRule(RelOptRuleOperand operand, String dsc) {
        super(operand, dsc);
    }

    /**
     * Commute inner/outer table scan if needed, so that the inner table always has a matching index, if at least one
     * of them has a matching index.
     */
    private static final class RelExtractor {
        private final VoltPhysicalJoin m_join;
        private final RelNode m_outerScan;
        private final VoltPhysicalTableScan m_innerTableScan;
        private final Calc m_innerCalc;
        private final RexProgram m_innerProgram;

        RelExtractor(RelOptRuleCall call) {
            m_join = call.rel(0);
            m_outerScan = call.rel(1);
            if (call.rels.length == 3) {
                m_innerCalc = null;
                m_innerTableScan = call.rel(2);
                m_innerProgram = m_innerTableScan.getProgram();
            } else {
                m_innerCalc = call.rel(2);
                m_innerTableScan = call.rel(3);
                m_innerProgram = VoltRexUtil.mergeProgram(
                        m_innerTableScan.getProgram(),
                        m_innerCalc.getProgram(), m_innerCalc.getCluster().getRexBuilder());
            }
            Preconditions.checkNotNull(m_innerProgram, "Inner relation missing Program");
        }
        VoltPhysicalJoin getJoin() {
            return m_join;
        }
        RelNode getOuterScan() {
            return m_outerScan;
        }
        VoltPhysicalTableScan getInnerTableScan() {
            return m_innerTableScan;
        }
        Calc getInnerCalc() {
            return m_innerCalc;
        }
        RexProgram getInnerProgram() {
            return m_innerProgram;
        }
    }

    private static Optional<AccessPath> getAccessPathFromInnerRel(
            Table table, VoltPhysicalJoin join, RexProgram program, Index index, int numOuterFieldsForJoin) {
        return Optional.ofNullable(IndexUtil.getCalciteRelevantAccessPathForIndex(
                table, CatalogUtil.getSortedCatalogItems(table.getColumns(), "index"),
                join.getCondition(), program, index, SortDirectionType.INVALID,
                numOuterFieldsForJoin, true));
    }

    private static RelNode toIndexJoin(VoltPhysicalJoin join, RelNode outerScan, RelNode innerChild, String indexName) {
        return new VoltPhysicalNestLoopIndexJoin(
                join.getCluster(), join.getTraitSet(), outerScan, innerChild, join.getCondition(),
                join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
                ImmutableList.copyOf(join.getSystemFieldList()), indexName);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        // At this moment, the join condition contains only outer-inner expressions. The inner and outer ones
        // are supposed to be pushed down already.
        //
        // If there is an index that can be satisfied by the join condition, it will be a NLIJ
        final RelExtractor extractor = new RelExtractor(call);
        final VoltPhysicalJoin join = extractor.getJoin();
        final RelNode outerScan = extractor.getOuterScan();
        final VoltPhysicalTableScan innerScan = extractor.getInnerTableScan();
        final Calc innerCalc = extractor.getInnerCalc();
        final RexProgram program = extractor.getInnerProgram();

        // INNER only at the moment
        if (join.getJoinType() != JoinRelType.INNER) {
            return;
        }

        final Table innerTable = innerScan.getVoltTable().getCatalogTable();
        final Map<RelNode, RelNode> equiv = new HashMap<>();
        innerTable.getIndexes().forEach(index -> // need to pass the join outer child columns count to the visitor
                getAccessPathFromInnerRel(innerTable, join, program, index, outerScan.getRowType().getFieldCount())
                        .ifPresent(accessPath -> {
                            // Index's collation needs to be based on its own program only - the Calc sits above the scan
                            final RelCollation indexCollation;
                            try {
                                indexCollation = VoltRexUtil.createIndexCollation(
                                        index, innerTable, innerScan.getCluster().getRexBuilder(), innerScan.getProgram());
                            } catch (JSONException e) {
                                throw new CalcitePlanningException(e.getMessage());
                            }

                            final TableScan indexScan = new VoltPhysicalTableIndexScan(
                                    innerScan.getCluster(), innerScan.getTraitSet(), innerScan.getTable(),
                                    innerScan.getVoltTable(), innerScan.getProgram(), index, accessPath,
                                    innerScan.getLimitRexNode(), innerScan.getOffsetRexNode(), null,
                                    null, null, innerScan.getSplitCount(),
                                    indexCollation);
                            final RelNode innerChild;
                            if (call.rels.length == 3) {
                                innerChild = indexScan;
                            } else {
                                innerChild = innerCalc.copy(innerCalc.getTraitSet(), indexScan, innerCalc.getProgram());
                            }
                            equiv.put(toIndexJoin(join, outerScan, innerChild, index.getTypeName()), join);
                        }));
        if (! equiv.isEmpty()) {
            call.transformTo(equiv.keySet().iterator().next(), equiv);
        }
    }
}
