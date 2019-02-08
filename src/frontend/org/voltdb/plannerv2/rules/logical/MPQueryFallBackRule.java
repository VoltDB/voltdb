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

package org.voltdb.plannerv2.rules.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Util;
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.plannerv2.rel.logical.VoltLogicalCalc;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;
import org.voltdb.plannerv2.rel.logical.VoltLogicalTableScan;

/**
 * Rule that fallback the processing of a multi-partition query without joins to
 * the legacy planner.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class MPQueryFallBackRule extends RelOptRule {

    public static final MPQueryFallBackRule INSTANCE = new MPQueryFallBackRule();

    private MPQueryFallBackRule() {
        super(operand(SingleRel.class, RelDistributions.ANY,
                some(operand(RelNode.class, any()))));
    }

    @Override public void onMatch(RelOptRuleCall call) {
        // NOTE:
        // This rule depends on the VoltLogicalCalc and the VoltLogicalTableScan nodes at the leaf level to
        // determine whether this query is SP or not first, then propagate this information upwards to the root.
        // Therefore, it can only be run using a Hep planner following the bottom-up order.
        // It will not work properly with Hep planners following other orders or Volcano planners.

        if (call.rel(0) instanceof VoltLogicalCalc && call.rel(1) instanceof VoltLogicalTableScan) {
            // If it is a VoltLogicalCalc / VoltLogicalTableScan pattern, check the filter and see if it can be run SP.
            // VoltLogicalTableScan gives the partitioning scheme information,
            // and the VoltLogicalCalc above it gives the information about the filters.
            VoltLogicalCalc calc = call.rel(0);
            VoltLogicalTableScan tableScan = call.rel(1);
            // See: org.voltdb.plannerv2.VoltTable.getStatistic().getDistribution()
            RelDistribution tableDist = tableScan.getTable().getDistribution();
            // RelDistribution.Type.SINGLETON means that the table is replicated.
            // TODO: For views, the distribution type will be RANDOM_DISTRIBUTED.
            if (tableDist.getType() == RelDistribution.Type.RANDOM_DISTRIBUTED) {
                throw new PlannerFallbackException("MP query not supported in Calcite planner.");
            }
            // For partitioned tables, the distribution type will be HASH_DISTRIBUTED.
            if (tableDist.getType() != RelDistribution.Type.SINGLETON
                    && (calc.getProgram().getCondition() == null // SELECT ... FROM t (no filter)
                    || !isSinglePartitioned(calc.getProgram(), calc.getProgram().getCondition(), tableDist.getKeys()))) {
                throw new PlannerFallbackException("MP query not supported in Calcite planner.");
            }
            call.transformTo(calc.copy(calc.getTraitSet().replace(tableDist), calc.getInputs()));
        } else if (call.rel(0) instanceof VoltLogicalCalc && call.rel(1) instanceof VoltLogicalJoin) {
            final VoltLogicalCalc calc = call.rel(0);
            final VoltLogicalJoin join = call.rel(1);
            final RelOptTable outer = ((HepRelVertex) join.getLeft()).getCurrentRel().getTable();
            final RelOptTable inner = ((HepRelVertex) join.getRight()).getCurrentRel().getTable();
            if (outer != null && inner != null) {
                final boolean outerIsReplicated = outer.getDistribution().getType() == RelDistribution.Type.SINGLETON,
                        innerIsReplicated = inner.getDistribution().getType() == RelDistribution.Type.SINGLETON;
                final RelDistribution joinDist;
                if (!(outerIsReplicated && innerIsReplicated)) {
                    final int outerColumns = outer.getRowType().getFieldCount();
                    final Integer outerPartCol = ((RelOptTableImpl) outer).getTable().getPartitionColumn();
                    final Integer innerPartCol = ((RelOptTableImpl) inner).getTable().getPartitionColumn();
                    if (join.getJoinType() != JoinRelType.INNER) {
                        throw new PlannerFallbackException("MP query not supported in Calcite planner.");
                    } else if (!outerIsReplicated && !innerIsReplicated) { // has to be inner join with equal-condition on partition column to be SP
                        final RexLiteral joinValue = getInnerJoin2PEqualValue(join.getCondition(),
                                outerColumns, outerPartCol, innerPartCol);
                        if (joinValue == null) {
                            throw new PlannerFallbackException("MP query not supported in Calcite planner.");
                        } else {
                            outer.getDistribution().setPartitionEqualValue(joinValue);
                            inner.getDistribution().setPartitionEqualValue(joinValue);
                            joinDist = outer.getDistribution(); // TODO
                        }
                    } else if (outerIsReplicated) {     // replicated inner join partitioned
                        final RexLiteral literal = getInnerJoinPREqualValue(join.getCondition(), innerPartCol, outerColumns);
                        if (literal == null) {
                            throw new PlannerFallbackException("MP query not supported in Calcite planner.");
                        } else {
                            inner.getDistribution().setPartitionEqualValue(literal);
                            joinDist = inner.getDistribution();
                        }
                    } else {                            // partitioned inner join replicated
                        final RexLiteral literal = getInnerJoinPREqualValue(join.getCondition(), outerPartCol, outerColumns);
                        if (literal == null) {
                            throw new PlannerFallbackException("MP query not supported in Calcite planner.");
                        } else {
                            outer.getDistribution().setPartitionEqualValue(literal);
                            joinDist = outer.getDistribution();
                        }
                    }
                } else {
                    joinDist = outer.getDistribution();
                }
                if (join.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == null) {
                    join.getTraitSet().replace(joinDist);       // XXX: why join's traitset does not include distribution trait?
                }
                call.transformTo(calc.copy(calc.getTraitSet().replace(joinDist), calc.getInputs()));
                //call.transformTo(join.copy(join.getTraitSet().replace(joinDist), join.getInputs()));
            }
        } else {
            // Otherwise, propagate the DistributionTrait bottom up.
            RelNode child = call.rel(1);
            RelDistribution childDist = child.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
            if (childDist != RelDistributions.ANY) {
                SingleRel node = call.rel(0);
                call.transformTo(node.copy(node.getTraitSet().replace(childDist), node.getInputs()));
            }
        }
    }

    private static RexLiteral getInnerJoinPREqualValue(RexNode joinCondition, int partitionColumn, int outerColumns) {
        if (! (joinCondition.isA(SqlKind.AND))) {
            return null;
        } else {
            final RexCall cond = (RexCall) joinCondition;
            final RexNode leftConj = cond.getOperands().get(0), rightConj = cond.getOperands().get(1);
            // First condition must be in the form of T1.c1 = T2.c2
            if (! leftConj.isA(SqlKind.EQUALS)) {
                return null;
            } else {
                final RexCall col2 = (RexCall) leftConj;
                final RexNode leftColRef = col2.getOperands().get(0),
                        rightColRef = col2.getOperands().get(1);
                assert leftColRef instanceof RexInputRef && rightColRef instanceof RexInputRef;
                final int leftColIndex = ((RexInputRef) leftColRef).getIndex(),
                        rightColIndex = ((RexInputRef) rightColRef).getIndex(),
                        adjustedPartitionColumn = partitionColumn < outerColumns ?
                                partitionColumn : partitionColumn + outerColumns;
                if (leftColIndex != adjustedPartitionColumn && rightColIndex != adjustedPartitionColumn) {
                    return null;
                } else if (! rightConj.isA(SqlKind.EQUALS)) {
                    return null;
                } else {
                    final RexCall eq = (RexCall) rightConj;
                    final RexNode colRef, literal;
                    if (eq.getOperands().get(0).isA(SqlKind.LITERAL) == eq.getOperands().get(1).isA(SqlKind.LITERAL)) {
                        return null;
                    } else if (eq.getOperands().get(0).isA(SqlKind.LITERAL)){
                        colRef = eq.getOperands().get(1);
                        literal = eq.getOperands().get(0);
                    } else {
                        colRef = eq.getOperands().get(0);
                        literal = eq.getOperands().get(1);
                    }
                    if (! (colRef instanceof RexInputRef)) {
                        return null;
                    } else {
                        final int colIndex = ((RexInputRef) colRef).getIndex();
                        return colIndex == adjustedPartitionColumn ? (RexLiteral) literal : null;
                    }
                }
            }
        }
    }

    private static RexLiteral getInnerJoin2PEqualValue(
            RexNode joinCondition, int outerColumns, int outerPartColIndex, int innerPartColIndex) {
        if (! (joinCondition.isA(SqlKind.AND))) {
            return null;
        } else {
            final RexCall cond = (RexCall) joinCondition;
            final RexNode leftConj = cond.getOperands().get(0), rightConj = cond.getOperands().get(1);
            // First condition must be in the form of T1.c1 = T2.c2
            if (! leftConj.isA(SqlKind.EQUALS)) {
                return null;
            } else {
                final RexCall col2 = (RexCall) leftConj;
                final RexNode leftColRef = col2.getOperands().get(0),
                        rightColRef = col2.getOperands().get(1);
                assert leftColRef instanceof RexInputRef && rightColRef instanceof RexInputRef;
                final int leftColIndex = ((RexInputRef) leftColRef).getIndex(),
                        rightColIndex = ((RexInputRef) rightColRef).getIndex();
                if (! (leftColIndex == outerPartColIndex && rightColIndex == innerPartColIndex + outerColumns ||
                        leftColIndex == innerPartColIndex + outerColumns && rightColIndex ==outerPartColIndex)) {
                    return null;       // not joining on two partition columns
                } else if (! rightConj.isA(SqlKind.EQUALS)){    // check other conditions: must be in the form of PartitionColumRed = LITERAL
                    return null;
                } else {
                    final RexCall eqValue = (RexCall) rightConj;
                    final RexNode colRef, literal;
                    if (eqValue.getOperands().get(0).isA(SqlKind.LITERAL) ==
                            eqValue.getOperands().get(1).isA(SqlKind.LITERAL)) {
                        return null;
                    } else if (eqValue.getOperands().get(0).isA(SqlKind.LITERAL)) {
                        colRef = eqValue.getOperands().get(1);
                        literal = eqValue.getOperands().get(0);
                    } else {
                        colRef = eqValue.getOperands().get(0);
                        literal = eqValue.getOperands().get(1);
                    }
                    if (! (colRef instanceof RexInputRef)) {
                        return null;
                    } else {
                        final int colRefIndex = ((RexInputRef) colRef).getIndex();
                        if (colRefIndex == outerPartColIndex || colRefIndex == outerColumns + innerPartColIndex) {
                            return  (RexLiteral) literal;
                        } else {
                            return null;
                        }
                    }
                }
            }
        }
    }

    /**
     * Helper function to decide whether the filtered result is located at a single partition.
     *
     * @param program       the program of a {@link LogicalCalc}.
     * @param rexNode       the current condition we are checking.
     * @param partitionKeys the list of partition keys for the target table.
     * @return true if the filtered result is located at a single partition.
     */
    private static boolean isSinglePartitioned(RexProgram program, RexNode rexNode, List<Integer> partitionKeys) {
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            SqlKind sqlKind = rexCall.getOperator().getKind();
            switch (sqlKind) {
                case EQUALS:
                    // SELECT ... FROM t WHERE PK = 0;
                    // Note: SELECT ... FROM t WHERE PK = col1; is a MP query
                    return (isSinglePartitioned(program, rexCall.getOperands().get(0), partitionKeys)
                            && !hasTableColumn(program, rexCall.getOperands().get(1))) ||
                            (isSinglePartitioned(program, rexCall.getOperands().get(1), partitionKeys)
                                    && !hasTableColumn(program, rexCall.getOperands().get(0)));
                case AND:
                    // SELECT ... FROM t WHERE PK = 0 and A = 1;
                    return isSinglePartitioned(program, rexCall.getOperands().get(0), partitionKeys) ||
                            isSinglePartitioned(program, rexCall.getOperands().get(1), partitionKeys);
                case OR:
                    // SELECT ... FROM t WHERE PK = 0 or A = 1;
                    // SELECT ... FROM t WHERE PK in (1, 2, 3);
                    // TODO: It could be SP, if all the values of PK are in the same partition,
                    return false;
                case NOT:
                    // SELECT ... FROM t WHERE NOT PK <> 0;
                    return isComplementSinglePartitioned(program, rexCall.getOperands().get(0), partitionKeys);
                default:
                    return false;
            }
        } else if (rexNode instanceof RexInputRef) {
            return partitionKeys.contains(((RexInputRef) rexNode).getIndex());
        } else if (rexNode instanceof RexLocalRef) {
            return isSinglePartitioned(program,
                    program.getExprList().get(((RexLocalRef) rexNode).getIndex()), partitionKeys);
        } else {
            return false;
        }
    }

    /**
     * Helper function to decide whether the complement of the filtered result is located at a single partition.
     *
     * @param program       the program of a {@link LogicalCalc}.
     * @param rexNode       the condition of the program.
     * @param partitionKeys the list of partition keys for the target table.
     * @return true if the complement of the filtered result is located at a single partition.
     */
    private static boolean isComplementSinglePartitioned(RexProgram program, RexNode rexNode, List<Integer> partitionKeys) {
        if (rexNode instanceof RexCall) {
            RexCall rexCall = (RexCall) rexNode;
            SqlKind sqlKind = rexCall.getOperator().getKind();
            switch (sqlKind) {
                case NOT_EQUALS:
                    // SELECT ... FROM t WHERE NOT PK <> 0;
                    // Note: SELECT ... FROM t WHERE Not PK <> col1; is a MP query
                    return (isSinglePartitioned(program, rexCall.getOperands().get(0), partitionKeys)
                            && !hasTableColumn(program, rexCall.getOperands().get(1))) ||
                            (isSinglePartitioned(program, rexCall.getOperands().get(1), partitionKeys)
                                    && !hasTableColumn(program, rexCall.getOperands().get(0)));
                case NOT:
                    // SELECT ... FROM t WHERE NOT (NOT PK = 0);
                    // I assume this branch will never be reached, cause calcite have a ReduceExpressionsRule
                    // to simplify NOT(NOT..), but it is better safe than sorry.
                    return isSinglePartitioned(program, rexCall.getOperands().get(0), partitionKeys);
                case AND:
                    // SELECT ... FROM t WHERE NOT (PK <> 0 and A = 2);
                    return false;
                case OR:
                    // SELECT ... FROM t WHERE NOT (PK <> 0 or A = 2);
                    return isComplementSinglePartitioned(program, rexCall.getOperands().get(0), partitionKeys) ||
                            isComplementSinglePartitioned(program, rexCall.getOperands().get(1), partitionKeys);
                default:
                    return false;
            }
        } else if (rexNode instanceof RexLocalRef) {
            return isComplementSinglePartitioned(program,
                    program.getExprList().get(((RexLocalRef) rexNode).getIndex()), partitionKeys);
        } else {
            return false;
        }
    }

    /**
     * Helper function to check if the rexNode is a table column or includes a table column
     * in its subtrees.
     *
     * @param program the program of a {@link LogicalCalc}.
     * @param rexNode the rexNode
     * @return true if the rexNode is a table column or includes a table column or
     * includes a table column in its subtrees
     */
    private static boolean hasTableColumn(RexProgram program, RexNode rexNode) {
        try {
            rexNode.accept(new RexLocalRefFinder(program));
        } catch (Util.FoundOne found) {
            return true;
        }
        return false;
    }

    /**
     * A visitor to find {@link RexLocalRef} of a table column in a node tree.
     */
    private static final class RexLocalRefFinder extends RexVisitorImpl<Void> {
        final private RexProgram m_program;

        private RexLocalRefFinder(RexProgram program) {
            super(true);
            m_program = program;
        }

        @Override
        public Void visitLocalRef(RexLocalRef localRef) {
            // True if the localRef is the Table Column
            if (localRef.getIndex() < m_program.getProjectList().size()) {
                throw Util.FoundOne.NULL;
            }
            // will visit recursively if the localRef is a expression or subquery
            m_program.getExprList().get(localRef.getIndex()).accept(this);
            return null;
        }
    }
}

