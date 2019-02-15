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

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.*;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.tools.Program;
import org.voltcore.utils.Pair;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.plannerv2.rel.logical.VoltLogicalCalc;
import org.voltdb.plannerv2.rel.logical.VoltLogicalJoin;
import org.voltdb.plannerv2.rel.logical.VoltLogicalTableScan;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Rules that fallback a query with Join operator if it is multi-partitioned.
 *
 * @author Chao Zhou
 * @since 9.0
 */
public class MPJoinQueryFallBackRule extends RelOptRule {

    public static final MPJoinQueryFallBackRule INSTANCE =
            new MPJoinQueryFallBackRule(
                    operand(VoltLogicalJoin.class, RelDistributions.ANY,
                            some(operand(RelNode.class, any()),
                                    operand(RelNode.class, any()))), "MPJoinQueryFallBackRule");

    private MPJoinQueryFallBackRule(RelOptRuleOperand operand, String desc) {
        super(operand, desc);
    }

    private RelDistribution getDistribution(RelNode node) {
        if (node instanceof VoltLogicalTableScan) {
            return node.getTable().getDistribution();
        }
        return node.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE);
    }

    private static VoltLogicalTableScan getTableScan(RelNode node) {
        assert node.getInputs().size() == 1;
        final RelNode scan = node.getInput(0);
        assert scan instanceof HepRelVertex;
        final RelNode vscan = ((HepRelVertex) scan).getCurrentRel();
        assert vscan instanceof VoltLogicalTableScan;
        return (VoltLogicalTableScan) vscan;
    }

    static Integer getPartitionColumn(RelNode tbl) {
        if (tbl instanceof TableScan) {
            final RelDistribution dist = tbl.getTable().getDistribution();
            if (dist.getKeys().isEmpty()) {
                return null;
            } else {
                return dist.getKeys().get(0);
            }
        } else {
            assert tbl instanceof VoltLogicalCalc;
            return getTableScan(tbl).getVoltTable().getPartitionColumn();
        }
    }

    static Pair<Integer, RexLiteral> getEqualValuePredicate(RexLocalRef condRef, List<RexNode> exprs) {
        final RexNode condDeref = exprs.get(condRef.getIndex());
        assert condDeref instanceof RexCall;
        final RexCall call = (RexCall) condDeref;
        if (call.isA(SqlKind.EQUALS)) {
            RexNode left = call.getOperands().get(0), right = call.getOperands().get(1);
            assert left.isA(SqlKind.LOCAL_REF) && right.isA(SqlKind.LOCAL_REF);
            left = exprs.get(((RexLocalRef) left).getIndex());
            right = exprs.get(((RexLocalRef) right).getIndex());
            assert left.isA(SqlKind.INPUT_REF) && right.isA(SqlKind.LITERAL) ||
                    left.isA(SqlKind.LITERAL) && right.isA(SqlKind.INPUT_REF);
            if (left.isA(SqlKind.INPUT_REF)) {
                return Pair.of(((RexInputRef) left).getIndex(), (RexLiteral) right);
            } else {
                return Pair.of(((RexInputRef) right).getIndex(), (RexLiteral) left);
            }
        } else {
            return null;
        }
    }

    static Pair<Integer, RexLiteral> calcCondition(Calc calc) {
        final RexProgram prog = calc.getProgram();
        return getEqualValuePredicate(prog.getCondition(), prog.getExprList());
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltLogicalJoin join = call.rel(0);
        RelDistribution outerDist = getDistribution(call.rel(1));
        RelDistribution innerDist = getDistribution(call.rel(2));
        final Set<Pair<Integer, Integer>> joinColumnSet = MPQueryFallBackRule.getAllJoiningColumns((RexCall) join.getCondition());
        if (joinColumnSet.isEmpty()) {  // at least one side of the join condition is not a column ref: unsupported
            throw new PlannerFallbackException("MP query not supported in Calcite planner.");
        }
        final Pair<Integer, Integer> joinColumns = joinColumnSet.iterator().next();
        final boolean outerIsPartitioned = joinColumns.getFirst() != null,
                innerIsPartitioned = joinColumns.getSecond() != null;
        switch (join.getJoinType()) {
            case INNER:
                if (outerIsPartitioned || innerIsPartitioned) {
                    assert join.getCondition() instanceof RexCall;
                    final int outerJoiningCol = joinColumns.getFirst(), innerJoiningCol = joinColumns.getSecond();
                    final RelNode outer = call.rel(1), inner = call.rel(2);
                    final int outerTableColumns = outer.getRowType().getFieldCount();
                    final Integer outerPartColumn = getPartitionColumn(outer), innerPartColumn = getPartitionColumn(inner);
                    if (outerPartColumn != null && outerPartColumn != outerJoiningCol ||
                            innerPartColumn != null && innerPartColumn != innerJoiningCol - outerTableColumns) {
                        // Does not join on the partition column
                        throw new PlannerFallbackException("MP query not supported in Calcite planner.");
                    } else if (outer instanceof TableScan && inner instanceof TableScan) {
                        // missing filters on table scans
                        // TODO: separate additional predicates from join-condition
                        throw new PlannerFallbackException("MP query not supported in Calcite planner.");
                    } else {
                        final Pair<Integer, RexLiteral> outerFilter, innerFilter;
                        if (outer instanceof Calc) {
                            outerFilter = calcCondition((Calc) outer);
                        } else {
                            outerFilter = null;
                        }
                        if (inner instanceof Calc) {
                            innerFilter = calcCondition((Calc) inner);
                        } else {
                            innerFilter = null;
                        }
                        if (outerFilter == null && innerFilter == null) { // No equal-filter found
                            throw new PlannerFallbackException("MP query not supported in Calcite planner.");
                        } else if (outerFilter != null && outerFilter.getFirst().equals(outerPartColumn) ||
                                innerFilter != null && innerFilter.getFirst().equals(innerPartColumn)) {
                            // Has an equal-filter on partitioned column
                            // NOTE: in case there are 2 equal predicated on each partitioned column each,
                            // we just take out the first one.
                            // TODO: in that case, checking that they are equal or not, and optimize away is a
                            // minor-usage case of optimization.
                            final RexLiteral value = outerFilter == null ?
                                    innerFilter.getSecond() : outerFilter.getSecond();
                            if (innerIsPartitioned) {
                                innerDist.setPartitionEqualValue(value);
                            }
                            if (outerIsPartitioned) {
                                outerDist.setPartitionEqualValue(value);
                            }
                        } else {    // no equal-filter on partition column
                            throw new PlannerFallbackException("MP query not supported in Calcite planner.");
                        }
                    }
                } else {    // Both are replicated: SP
                }
                break;
            default:
                if (outerIsPartitioned || innerIsPartitioned) {
                    // Not inner-join type involving at least a partitioned table
                    throw new PlannerFallbackException("MP query not supported in Calcite planner.");
                }
        }

        if (outerDist == RelDistributions.SINGLETON && innerDist == RelDistributions.SINGLETON) {
            call.transformTo(join.copy(join.getTraitSet().replace(RelDistributions.SINGLETON), join.getInputs()));
        } else if (outerDist != RelDistributions.SINGLETON) {
            call.transformTo(join.copy(join.getTraitSet().replace(outerDist), join.getInputs()));
        } else {
            call.transformTo(join.copy(join.getTraitSet().replace(innerDist), join.getInputs()));
        }
    }
}
