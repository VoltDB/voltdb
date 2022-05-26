/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.util.Pair;
import org.json_voltpatches.JSONException;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.planner.AccessPath;
import org.voltdb.plannerv2.guards.CalcitePlanningException;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalNestLoopIndexJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalNestLoopJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableIndexScan;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableScan;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableSequentialScan;
import org.voltdb.plannerv2.utils.IndexUtil;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.types.SortDirectionType;

import com.google.common.collect.ImmutableList;
import com.google_voltpatches.common.base.Preconditions;

/**
 * Matching rule sets from nest loop join to nest loop-index join
 */
public class VoltPNestLoopToIndexJoinRule extends RelOptRule{

    // Here, the "SSCAN" means sequential scan; "ISCAN" means index scan.
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
                // both are table scans: outer table contains matching index but inner doesn't:
                // swap the 2 tables, since we are looking for matching index from inner table.
                // TODO: this fails Litmus check on:
                // CREATE TABLE r2(a int, c int); CREATE TABLE r3(a int not null, c int); CREATE INDEX ind1 ON R3(a);
                // SELECT * FROM r3 JOIN r2 ON r3.a = r2.a;
//                if (m_outerScan instanceof VoltPhysicalTableScan &&
//                        ! containsIndex(m_innerTableScan) &&
//                        containsIndex((VoltPhysicalTableScan) m_outerScan)) {
//                    commute();
//                }
            } else {
                m_innerCalc = call.rel(2);
                m_innerTableScan = call.rel(3);
                m_innerProgram = VoltRexUtil.mergeProgram(
                        m_innerTableScan.getProgram(),
                        m_innerCalc.getProgram(), m_innerCalc.getCluster().getRexBuilder());
            }
            Preconditions.checkNotNull(m_innerProgram, "Inner relation missing Program");
        }
        /*private void commute() {       // swap member variables on outer <-> inner table
            final VoltPhysicalTableScan scanTmp = (VoltPhysicalTableScan) m_outerScan;
            m_outerScan = m_innerTableScan;
            m_innerTableScan = scanTmp;
            m_innerProgram = ((VoltPhysicalTableScan) m_outerScan).getProgram();
            m_join = new VoltPhysicalNestLoopJoin(m_join.getCluster(), m_join.getTraitSet(),
                    m_join.getRight(), m_join.getLeft(), m_join.getCondition(), m_join.getVariablesSet(),
                    m_join.getJoinType(), m_join.isSemiJoinDone(), ImmutableList.copyOf(m_join.getSystemFieldList()),
                    m_join.getOffset(), m_join.getLimit());
        }
        private boolean containsIndex(VoltPhysicalTableScan scan) {
            final Table table = scan.getVoltTable().getCatalogTable();
            return StreamSupport.stream(
                    ((Iterable<Index>)() -> table.getIndexes().iterator()).spliterator(), false)
                    .anyMatch(index -> getAccessPathFromInnerRel(
                            table, m_join, m_innerProgram, index, m_outerScan.getRowType().getFieldCount())
                            .isPresent());
        }*/

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

    private RelNode toIndexJoin(VoltPhysicalJoin join, RexNode preJoinPredicate, RelNode outerScan, RelNode innerChild, Index index, AccessPath accessPath) {
        return new VoltPhysicalNestLoopIndexJoin(
                join.getCluster(), join.getTraitSet(), outerScan, innerChild, join.getCondition(),
                join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
                ImmutableList.copyOf(join.getSystemFieldList()),
                join.getWhereCondition(), index, accessPath, preJoinPredicate);
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

        final Table innerTable = innerScan.getVoltTable().getCatalogTable();
        final Map<RelNode, RelNode> equiv = new HashMap<>();
        innerTable.getIndexes().forEach(index -> {
            // need to pass the join outer child columns count to the visitor
            Optional<Pair<AccessPath, RexNode>> accessPathPairOpt =
                    IndexUtil.getCalciteRelevantAccessPathForIndex(
                    join.getCluster().getRexBuilder(),
                    innerTable,
                    join.getCondition(),
                    program,
                    index,
                    SortDirectionType.INVALID,
                    outerScan.getRowType().getFieldCount(),
                    true,
                    join.getJoinType() != JoinRelType.INNER);

            accessPathPairOpt.ifPresent(accessPathPair -> {
                // Index's collation needs to be based on its own program only - the Calc sits above the scan
                final RelCollation indexCollation;
                try {
                    indexCollation = VoltRexUtil.createIndexCollation(
                            index, innerTable, innerScan.getCluster().getRexBuilder(), innerScan.getProgram());
                } catch (JSONException e) {
                    throw new CalcitePlanningException(e.getMessage());
                }
                AccessPath accessPath = accessPathPair.left;
                RexNode preJoinPredicate = accessPathPair.right;
                final TableScan indexScan = new VoltPhysicalTableIndexScan(
                        innerScan.getCluster(), innerScan.getTraitSet(), innerScan.getTable(),
                        innerScan.getVoltTable(), innerScan.getProgram(), index, accessPath,
                        innerScan.getLimitRexNode(), innerScan.getOffsetRexNode(), null,
                        null, null,
                        indexCollation, true);
                final RelNode innerChild;
                if (call.rels.length == 3) {
                    innerChild = indexScan;
                } else {
                    innerChild = innerCalc.copy(innerCalc.getTraitSet(), indexScan, innerCalc.getProgram());
                }
                equiv.put(toIndexJoin(join, preJoinPredicate, outerScan, innerChild, index, accessPath), join);
            });
        });
        if (! equiv.isEmpty()) {
            call.transformTo(equiv.keySet().iterator().next(), equiv);
        }
    }
}
