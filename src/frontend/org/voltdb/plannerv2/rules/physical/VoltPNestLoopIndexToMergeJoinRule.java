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
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rex.RexBuilder;
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
import org.voltdb.plannerv2.rel.physical.VoltPhysicalMergeJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalNestLoopIndexJoin;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableIndexScan;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableScan;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalTableSequentialScan;
import org.voltdb.plannerv2.utils.IndexUtil;
import org.voltdb.plannerv2.utils.VoltRexUtil;
import org.voltdb.types.IndexLookupType;
import org.voltdb.types.IndexType;
import org.voltdb.types.SortDirectionType;

import com.google.common.collect.ImmutableList;
import com.google_voltpatches.common.base.Preconditions;

public class VoltPNestLoopIndexToMergeJoinRule extends RelOptRule {

    // 8 permutations of child types: Outer / Inner Calc, Scan, Merge Join.
    // Here, the "SSCAN" means sequential scan; "ISCAN" means index scan.
    private enum MatchType {
        OUTER_SCAN_INNER_SCAN {
            @Override VoltPhysicalCalc getOuterCalc(RelOptRuleCall call) {
                return null;
            }

            @Override VoltPhysicalTableSequentialScan getOuterNode(RelOptRuleCall call) {
                return (VoltPhysicalTableSequentialScan) call.rels[1];
            }

            @Override VoltPhysicalCalc getInnerCalc(RelOptRuleCall call) {
                return null;
            }

            @Override VoltPhysicalTableIndexScan getInnerIndexScan(RelOptRuleCall call) {
                return (VoltPhysicalTableIndexScan) call.rels[2];
            }

            @Override RexProgram getCombinedOuterProgram(RelOptRuleCall call) {
                return ((VoltPhysicalTableSequentialScan) call.rels[1]).getProgram();
            }

            @Override RexProgram getCombinedInnerProgram(RelOptRuleCall call) {
                return ((VoltPhysicalTableIndexScan) call.rels[2]).getProgram();
            }
        },
        OUTER_CALC_SCAN_INNER_SCAN {
            @Override VoltPhysicalCalc getOuterCalc(RelOptRuleCall call) {
                return (VoltPhysicalCalc) call.rels[1];
            }

            @Override VoltPhysicalTableSequentialScan getOuterNode(RelOptRuleCall call) {
                return (VoltPhysicalTableSequentialScan) call.rels[2];
            }

            @Override VoltPhysicalCalc getInnerCalc(RelOptRuleCall call) {
                return null;
            }

            @Override VoltPhysicalTableIndexScan getInnerIndexScan(RelOptRuleCall call) {
                return (VoltPhysicalTableIndexScan) call.rels[3];
            }

            @Override RexProgram getCombinedOuterProgram(RelOptRuleCall call) {
                RexProgram calcProgram = ((VoltPhysicalCalc) call.rels[1]).getProgram();
                RexProgram scanProgram = ((VoltPhysicalTableSequentialScan) call.rels[2]).getProgram();
                return VoltRexUtil.mergeProgram(scanProgram, calcProgram, call.rels[1].getCluster().getRexBuilder());
            }

            @Override RexProgram getCombinedInnerProgram(RelOptRuleCall call) {
                return ((VoltPhysicalTableIndexScan) call.rels[3]).getProgram();
            }
        },
        OUTER_SCAN_INNER_CALC_SCAN {
            @Override VoltPhysicalCalc getOuterCalc(RelOptRuleCall call) {
                return null;
            }

            @Override VoltPhysicalTableSequentialScan getOuterNode(RelOptRuleCall call) {
                return (VoltPhysicalTableSequentialScan) call.rels[1];
            }

            @Override VoltPhysicalCalc getInnerCalc(RelOptRuleCall call) {
                return (VoltPhysicalCalc) call.rels[2];
            }

            @Override VoltPhysicalTableIndexScan getInnerIndexScan(RelOptRuleCall call) {
                return (VoltPhysicalTableIndexScan) call.rels[3];
            }

            @Override RexProgram getCombinedOuterProgram(RelOptRuleCall call) {
                return ((VoltPhysicalTableSequentialScan) call.rels[1]).getProgram();
            }

            @Override RexProgram getCombinedInnerProgram(RelOptRuleCall call) {
                RexProgram calcProgram = ((VoltPhysicalCalc) call.rels[2]).getProgram();
                RexProgram scanProgram = ((VoltPhysicalTableIndexScan) call.rels[3]).getProgram();
                return VoltRexUtil.mergeProgram(scanProgram, calcProgram, call.rels[2].getCluster().getRexBuilder());
            }
        },
        OUTER_CALC_SCAN_INNER_CALC_SCAN {
            @Override VoltPhysicalCalc getOuterCalc(RelOptRuleCall call) {
                return (VoltPhysicalCalc) call.rels[1];
            }

            @Override VoltPhysicalTableSequentialScan getOuterNode(RelOptRuleCall call) {
                return (VoltPhysicalTableSequentialScan) call.rels[2];
            }

            @Override VoltPhysicalCalc getInnerCalc(RelOptRuleCall call) {
                return (VoltPhysicalCalc) call.rels[3];
            }

            @Override VoltPhysicalTableIndexScan getInnerIndexScan(RelOptRuleCall call) {
                return (VoltPhysicalTableIndexScan) call.rels[4];
            }

            @Override RexProgram getCombinedOuterProgram(RelOptRuleCall call) {
                RexProgram calcProgram = ((VoltPhysicalCalc) call.rels[1]).getProgram();
                RexProgram scanProgram = ((VoltPhysicalTableSequentialScan) call.rels[2]).getProgram();
                return VoltRexUtil.mergeProgram(scanProgram, calcProgram, call.rels[1].getCluster().getRexBuilder());
            }

            @Override RexProgram getCombinedInnerProgram(RelOptRuleCall call) {
                RexProgram calcProgram = ((VoltPhysicalCalc) call.rels[3]).getProgram();
                RexProgram scanProgram = ((VoltPhysicalTableIndexScan) call.rels[4]).getProgram();
                return VoltRexUtil.mergeProgram(scanProgram, calcProgram, call.rels[3].getCluster().getRexBuilder());
            }
        },
        // MJ /IndexScan Matches
        OUTER_MJ_INNER_SCAN {
            @Override VoltPhysicalCalc getOuterCalc(RelOptRuleCall call) {
                return null;
            }

            @Override VoltPhysicalMergeJoin getOuterNode(RelOptRuleCall call) {
                return (VoltPhysicalMergeJoin) call.rels[1];
            }

            @Override VoltPhysicalCalc getInnerCalc(RelOptRuleCall call) {
                return null;
            }

            @Override VoltPhysicalTableIndexScan getInnerIndexScan(RelOptRuleCall call) {
                return (VoltPhysicalTableIndexScan) call.rels[2];
            }

            @Override RexProgram getCombinedOuterProgram(RelOptRuleCall call) {
                return null;
            }

            @Override RexProgram getCombinedInnerProgram(RelOptRuleCall call) {
                return ((VoltPhysicalTableIndexScan) call.rels[2]).getProgram();
            }

        },
        OUTER_CALC_MJ_INNER_SCAN {
            @Override VoltPhysicalCalc getOuterCalc(RelOptRuleCall call) {
                return (VoltPhysicalCalc) call.rels[1];
            }

            @Override VoltPhysicalMergeJoin getOuterNode(RelOptRuleCall call) {
                return (VoltPhysicalMergeJoin) call.rels[2];
            }

            @Override VoltPhysicalCalc getInnerCalc(RelOptRuleCall call) {
                return null;
            }

            @Override VoltPhysicalTableIndexScan getInnerIndexScan(RelOptRuleCall call) {
                return (VoltPhysicalTableIndexScan) call.rels[3];
            }

            @Override RexProgram getCombinedOuterProgram(RelOptRuleCall call) {
                return ((VoltPhysicalCalc) call.rels[1]).getProgram();
            }

            @Override RexProgram getCombinedInnerProgram(RelOptRuleCall call) {
                return  ((VoltPhysicalTableIndexScan) call.rels[3]).getProgram();
            }

        },
        OUTER_MJ_INNER_CALC_SCAN {
            @Override VoltPhysicalCalc getOuterCalc(RelOptRuleCall call) {
                return null;
            }

            @Override VoltPhysicalMergeJoin getOuterNode(RelOptRuleCall call) { //  Auto-generated method stub
                return (VoltPhysicalMergeJoin) call.rels[1];
            }

            @Override VoltPhysicalCalc getInnerCalc(RelOptRuleCall call) {
                return (VoltPhysicalCalc) call.rels[2];
            }

            @Override VoltPhysicalTableIndexScan getInnerIndexScan(RelOptRuleCall call) {
                return (VoltPhysicalTableIndexScan) call.rels[3];
            }

            @Override RexProgram getCombinedOuterProgram(RelOptRuleCall call) {
                return null;
            }

            @Override RexProgram getCombinedInnerProgram(RelOptRuleCall call) {
                RexProgram calcProgram = ((VoltPhysicalCalc) call.rels[2]).getProgram();
                RexProgram scanProgram = ((VoltPhysicalTableIndexScan) call.rels[3]).getProgram();
                return VoltRexUtil.mergeProgram(scanProgram, calcProgram, call.rels[2].getCluster().getRexBuilder());
            }

        },
        OUTER_CALC_MJ_INNER_CALC_SCAN {
            @Override VoltPhysicalCalc getOuterCalc(RelOptRuleCall call) {
                return (VoltPhysicalCalc) call.rels[1];
            }

            @Override VoltPhysicalMergeJoin getOuterNode(RelOptRuleCall call) {
                return (VoltPhysicalMergeJoin) call.rels[2];
            }

            @Override VoltPhysicalCalc getInnerCalc(RelOptRuleCall call) {
                return (VoltPhysicalCalc) call.rels[3];
            }

            @Override VoltPhysicalTableIndexScan getInnerIndexScan(RelOptRuleCall call) {
                return (VoltPhysicalTableIndexScan) call.rels[4];
            }

            @Override RexProgram getCombinedOuterProgram(RelOptRuleCall call) {
                return ((VoltPhysicalCalc) call.rels[1]).getProgram();
            }

            @Override RexProgram getCombinedInnerProgram(RelOptRuleCall call) {
                RexProgram calcProgram = ((VoltPhysicalCalc) call.rels[3]).getProgram();
                RexProgram scanProgram = ((VoltPhysicalTableIndexScan) call.rels[4]).getProgram();
                return VoltRexUtil.mergeProgram(scanProgram, calcProgram, call.rels[3].getCluster().getRexBuilder());
            }

        };

        abstract VoltPhysicalCalc getOuterCalc(RelOptRuleCall call);
        abstract RelNode getOuterNode(RelOptRuleCall call);
        abstract VoltPhysicalCalc getInnerCalc(RelOptRuleCall call);
        abstract VoltPhysicalTableIndexScan getInnerIndexScan(RelOptRuleCall call);
        abstract RexProgram getCombinedOuterProgram(RelOptRuleCall call);
        abstract RexProgram getCombinedInnerProgram(RelOptRuleCall call);

        int getNumOuterFieldsForJoin(RelOptRuleCall call) {
            return call.rels[1].getRowType().getFieldCount();
        }

        static boolean hasOuterChildMergeJoin(MatchType matchType) {
            switch (matchType) {
                case OUTER_MJ_INNER_CALC_SCAN:
                case OUTER_CALC_MJ_INNER_SCAN:
                case OUTER_CALC_MJ_INNER_CALC_SCAN:
                case OUTER_MJ_INNER_SCAN:
                    return true;
                default:
                    return false;
            }
        }
    }

    // Instance naming convention: INSTANCE_OUTER_INNER; SSCAN stands for sequential scan, ISCAN for index scan;
    // MJ for merge join; etc. CALC_?SCAN stands for Calc node on top of a ?Scan node (ISCAN or SSCAN).
    public static final RelOptRule INSTANCE_SSCAN_ISCAN = new VoltPNestLoopIndexToMergeJoinRule(
            operand(VoltPhysicalNestLoopIndexJoin.class,
                    some(operand(VoltPhysicalTableSequentialScan.class, none()),
                         operand(VoltPhysicalTableIndexScan.class, none()))),
            MatchType.OUTER_SCAN_INNER_SCAN);

    public static final RelOptRule INSTANCE_CALC_SSCAN_ISCAN = new VoltPNestLoopIndexToMergeJoinRule(
            operand(VoltPhysicalNestLoopIndexJoin.class,
                    some(operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableSequentialScan.class, none())),
                         operand(VoltPhysicalTableIndexScan.class, none()))),
            MatchType.OUTER_CALC_SCAN_INNER_SCAN);

    public static final RelOptRule INSTANCE_SSCAN_CALC_ISCAN = new VoltPNestLoopIndexToMergeJoinRule(
            operand(VoltPhysicalNestLoopIndexJoin.class,
                    some(operand(VoltPhysicalTableSequentialScan.class, none()),
                         operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableIndexScan.class, none())))),
            MatchType.OUTER_SCAN_INNER_CALC_SCAN);

    public static final RelOptRule INSTANCE_CALC_SSCAN_CALC_ISCAN = new VoltPNestLoopIndexToMergeJoinRule(
            operand(VoltPhysicalNestLoopIndexJoin.class,
                    some(operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableSequentialScan.class, none())),
                         operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableIndexScan.class, none())))),
            MatchType.OUTER_CALC_SCAN_INNER_CALC_SCAN);

    public static final RelOptRule INSTANCE_MJ_ISCAN = new VoltPNestLoopIndexToMergeJoinRule(
            operand(VoltPhysicalNestLoopIndexJoin.class,
                    some(operand(VoltPhysicalMergeJoin.class, any()), operand(VoltPhysicalTableIndexScan.class, none()))),
            MatchType.OUTER_MJ_INNER_SCAN);

    public static final RelOptRule INSTANCE_CALC_MJ_ISCAN = new VoltPNestLoopIndexToMergeJoinRule(
            operand(VoltPhysicalNestLoopIndexJoin.class,
                    some(operand(VoltPhysicalCalc.class, operand(VoltPhysicalMergeJoin.class, any())),
                         operand(VoltPhysicalTableIndexScan.class, none()))),
            MatchType.OUTER_CALC_MJ_INNER_SCAN);

    public static final RelOptRule INSTANCE_MJ_CALC_ISCAN = new VoltPNestLoopIndexToMergeJoinRule(
            operand(VoltPhysicalNestLoopIndexJoin.class,
                    some(operand(VoltPhysicalMergeJoin.class, any()),
                         operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableIndexScan.class, none())))),
            MatchType.OUTER_MJ_INNER_CALC_SCAN);

    public static final RelOptRule INSTANCE_CALC_MJ_CALC_ISCAN = new VoltPNestLoopIndexToMergeJoinRule(
            operand(VoltPhysicalNestLoopIndexJoin.class,
                    some(operand(VoltPhysicalCalc.class, operand(VoltPhysicalMergeJoin.class, any())),
                         operand(VoltPhysicalCalc.class, operand(VoltPhysicalTableIndexScan.class, none())))),
            MatchType.OUTER_CALC_MJ_INNER_CALC_SCAN);

    private final MatchType m_matchType;

    private VoltPNestLoopIndexToMergeJoinRule(RelOptRuleOperand operand, MatchType matchType) {
        super(operand, "MergeJoin_" + matchType.name());
        m_matchType = matchType;
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        // An index must be scannable to produce sorted result; and the join must be an equi-join
        return isIndexScannable(m_matchType.getInnerIndexScan(call).getIndex()) &&
                isEquijoin(call.rel(0));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final VoltPhysicalJoin join = call.rel(0);

        // Outer Child
        final RelNode outerNode = m_matchType.getOuterNode(call);
        final Calc outerCalc = m_matchType.getOuterCalc(call);
        final RexProgram outerProgram = m_matchType.getCombinedOuterProgram(call);
        int numOuterFieldsForJoin = m_matchType.getNumOuterFieldsForJoin(call);

        // Inner Child
        VoltPhysicalTableIndexScan innerIndexScan = m_matchType.getInnerIndexScan(call);
        Calc innerCalc = m_matchType.getInnerCalc(call);
        RelCollation innerCollation = innerIndexScan.getIndexCollation();
        RexProgram innerProgram = m_matchType.getCombinedInnerProgram(call);

        // Extract (outer, inner) field indexes from the equi-join expression
        final List<Pair<Integer, Integer>> joinExpressionCollation =
                VoltRexUtil.extractFieldIndexes(join.getCondition(), numOuterFieldsForJoin);

        // Verify inner collation satisfies the join predicate
        if(! verifyIndexCollation(innerCollation, innerProgram, joinExpressionCollation, numOuterFieldsForJoin)) {
            return;
        }

        final List<RelNode> mergeJoins;
        if (MatchType.hasOuterChildMergeJoin(m_matchType)) {
            // Verify whether an outer child's collation is compatible with an order that required by the top join's condition
            mergeJoins = Stream.of(nextOuterJoinCollation(call))
                    .filter(childJoinExpressionCollation ->
                        verifyOuterCildJoinCollation(childJoinExpressionCollation, outerProgram, joinExpressionCollation))
                    .map(__ -> {
                        RelNode newOuterChild = outerNode.copy(outerNode.getTraitSet(), outerNode.getInputs());
                        if (outerCalc != null) {
                            newOuterChild = outerCalc.copy(outerCalc.getTraitSet(), ImmutableList.of(newOuterChild));
                        }
                        final RelNode newInnerChild = newIndexScan(innerIndexScan.getIndex(),
                                innerIndexScan.getIndexCollation(), innerIndexScan, innerCalc, true);
                        return newMergeJoin(join, newOuterChild, newInnerChild,
                                "OuterMergeJoin" + outerNode.getId(), getIndexName(newInnerChild));
                    })
                    .collect(Collectors.toList());
        } else {
            // Try to find an outer's index which collation would match the inner's one
            mergeJoins = nextOuterIndexCollation(call).stream()
                .filter(collationIndexPair -> verifyIndexCollation(
                        collationIndexPair.left, outerProgram, joinExpressionCollation, -1))
                .map(collationIndexPair -> {
                    RelNode newOuterChild = newIndexScan(collationIndexPair.right,
                            collationIndexPair.left, outerNode, outerCalc, false);
                    RelNode newInnerChild = newIndexScan(innerIndexScan.getIndex(),
                            innerIndexScan.getIndexCollation(), innerIndexScan, innerCalc, true);
                    return newMergeJoin(join, newOuterChild, newInnerChild, getIndexName(newOuterChild), getIndexName(newInnerChild));
                    })
                .collect(Collectors.toList());
        }
        if (!mergeJoins.isEmpty()) {
            Map<RelNode, RelNode> equiv = new HashMap<>();
            mergeJoins.stream().skip(1).forEach(nextMergeJoin -> equiv.put(nextMergeJoin, join));
            call.transformTo(mergeJoins.get(0), equiv);
        }
     }

    private boolean isEquijoin(VoltPhysicalJoin join) {
        final RexNode joinCondition = join.getCondition();
        final int lhsFieldsCount = join.getLeft().getRowType().getFieldCount();
        return VoltRexUtil.isFieldEquivalenceExpr(joinCondition, lhsFieldsCount);
    }

    private RelNode newMergeJoin(
            VoltPhysicalJoin join, RelNode outerChild, RelNode innerChild,
            String ourterCollationName, String innerCollationName) {
        return new VoltPhysicalMergeJoin(
                join.getCluster(), join.getTraitSet(), outerChild, innerChild, join.getCondition(),
                join.getVariablesSet(), join.getJoinType(), join.isSemiJoinDone(),
                ImmutableList.copyOf(join.getSystemFieldList()),  join.getWhereCondition(),
                ourterCollationName, innerCollationName);
    }

    private RelNode newIndexScan(Index index, RelCollation indexCollation, RelNode oldNode, Calc oldCalc, boolean inlinedInner) {
        assert(oldNode instanceof VoltPhysicalTableScan);
        VoltPhysicalTableScan oldScan = (VoltPhysicalTableScan) oldNode;
        // Remove all existing index expressions since we need to join the whole table
        final AccessPath accessPath = new AccessPath(
                index,
                // With no index expression, the lookup type will be ignored and
                // the sort direction will determine the scan direction;
                IndexLookupType.EQ, SortDirectionType.ASC, true);

        RelNode newNode = new VoltPhysicalTableIndexScan(
                oldScan.getCluster(), oldScan.getTraitSet(), oldScan.getTable(), oldScan.getVoltTable(),
                oldScan.getProgram(), index, accessPath, oldScan.getLimitRexNode(), oldScan.getOffsetRexNode(),
                oldScan.getAggregateRelNode(), oldScan.getPreAggregateRowType(), oldScan.getPreAggregateProgram(),
                indexCollation, inlinedInner);
        if (oldCalc != null) {
            newNode = oldCalc.copy(oldCalc.getTraitSet(), newNode, oldCalc.getProgram());
        }
        return newNode;
    }

    private String getIndexName(RelNode childNode) {
        if (childNode instanceof VoltPhysicalCalc) {
            childNode = ((VoltPhysicalCalc) childNode).getInput();
        }
        assert(childNode instanceof VoltPhysicalTableIndexScan);
        return ((VoltPhysicalTableIndexScan) childNode).getIndex().getTypeName();
    }

    /**
     * Verify if an index collation (outer or inner) satisfies the join expression.
     *  - index collation must cover all conjunction sub=expressions from the join predicate
     *  - individual index collations' local references indexes must match the join predicates ones.
     *
     * @param indexCollation
     * @param calcProgram
     * @param joinCollation - RefLocal Indexes Collection (left - outer scan, right - inner scan) for join predicate
     * @param numOuterFieldsForJoin
     * @return
     */
    private boolean verifyIndexCollation(
            RelCollation indexCollation, RexProgram calcProgram, List<Pair<Integer, Integer>> joinCollation,
            int numOuterFieldsForJoin) {
        if (indexCollation.getFieldCollations().size() < joinCollation.size()) {
            return false;
        } else {
            return IntStream.range(0, joinCollation.size())
                    .allMatch(i -> {
                        final Pair<Integer, Integer> collation = joinCollation.get(i);
                        final int joinIndex = numOuterFieldsForJoin < 0 ?
                                collation.left : collation.right - numOuterFieldsForJoin;
                        return VoltRexUtil.getReferenceOrAccessIndex(calcProgram,
                                calcProgram.getProjectList().get(joinIndex), true) ==
                                indexCollation.getFieldCollations().get(i).getFieldIndex();
                    });
        }
    }

    /**
     * Verify that the child's collation is a superset of a parent's collation -
     * for each top join equivalence sub-expression there is an equivalence expression from the child's join condition
     * that refers the same field.
     *
     * @param outerChildJoinCollation
     * @param calcProgram
     * @param joinCollation
     * @return
     */
    private boolean verifyOuterCildJoinCollation(
            List<Pair<Integer, Integer>> outerChildJoinCollation, RexProgram calcProgram,
            List<Pair<Integer, Integer>> joinCollation) {
        return joinCollation.stream().allMatch(topJoinPair -> {
            final Pair<Integer, Integer> topJoinPairFinal = topJoinPair;
            // @TODO DO we need to worry about the match order like the verifyIndexCollation does?
            return outerChildJoinCollation.stream().anyMatch(
                    childJoinPair -> {
                        int topOuterFieldIndex = topJoinPairFinal.left;
                        if (calcProgram != null) {
                            topOuterFieldIndex = VoltRexUtil.getReferenceOrAccessIndex(
                                    calcProgram, calcProgram.getProjectList().get(topOuterFieldIndex), true);
                        }
                        return topOuterFieldIndex == childJoinPair.left || topOuterFieldIndex == childJoinPair.right;
                    });
        });
    }

    /**
     * Return a stream of collation / index pairs for an outer Index scan
     *
     * @param call
     * @return
     */
    private List<Pair<RelCollation, Index>> nextOuterIndexCollation(RelOptRuleCall call) {
        final RelNode outerNode = m_matchType.getOuterNode(call);
        assert(outerNode instanceof VoltPhysicalTableSequentialScan);
        final VoltPhysicalTableScan outerSeqScan = (VoltPhysicalTableSequentialScan) outerNode;
        final RexBuilder builder = outerSeqScan.getCluster().getRexBuilder();
        final RexProgram outerProgram = m_matchType.getCombinedOuterProgram(call);
        final RexNode joinCondition = ((Join)call.rels[0]).getCondition();
        int numOuterFieldsForJoin = m_matchType.getNumOuterFieldsForJoin(call);

        final Table table = outerSeqScan.getVoltTable().getCatalogTable();
        final Iterable<Index> iterable = () -> outerSeqScan.getVoltTable().getCatalogTable().getIndexes().iterator();
        return StreamSupport.stream(iterable.spliterator(), false)
                .map(index -> IndexUtil.getCalciteRelevantAccessPathForIndex(
                        builder, table, joinCondition, outerProgram,
                        index, SortDirectionType.INVALID, numOuterFieldsForJoin,
                        false, false))
                .filter(accessPathOpt -> accessPathOpt.isPresent())
                .map(accessPathOpt -> {
                        Pair<AccessPath, RexNode> accessPathPair = accessPathOpt.get();
                        AccessPath accessPath = accessPathPair.left;
                        try {
                            return Pair.of(VoltRexUtil.createIndexCollation(
                                accessPath.getIndex(), table, builder,
                                outerSeqScan.getProgram()),
                                accessPath.getIndex());
                        } catch (JSONException e) {
                            throw new CalcitePlanningException(e.getMessage());
                        }
                    })
                .collect(Collectors.toList());
    }

    /**
     * Return a collations pairs for an outer Merge Join
     *
     * @param call
     * @return
     */
    private List<Pair<Integer, Integer>> nextOuterJoinCollation(RelOptRuleCall call) {
        final RelNode outerNode = m_matchType.getOuterNode(call);
        Preconditions.checkState(outerNode instanceof VoltPhysicalMergeJoin);
        final VoltPhysicalMergeJoin outerChildJoin = (VoltPhysicalMergeJoin) outerNode;
        Preconditions.checkState(outerChildJoin.getInputs().size() == 2);
        // Extract (outer, inner) field indexes from the equi-join expression
        return VoltRexUtil.extractFieldIndexes(outerChildJoin.getCondition(),
                outerChildJoin.getInputs().get(0).getRowType().getFieldCount());
    }

    private boolean isIndexScannable(Index index) {
        switch (IndexType.get(index.getType())) {
            case BALANCED_TREE:
            case BTREE:
            case HASH_TABLE:
                // HASH indexes are not supported and replaced by TREE based ones
                return true;
            case COVERING_CELL_INDEX:
            case INVALID:
                default:
                return false;
        }
    }
}
