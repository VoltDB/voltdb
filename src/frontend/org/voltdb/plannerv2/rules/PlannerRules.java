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

package org.voltdb.plannerv2.rules;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.voltdb.plannerv2.rules.inlining.VoltPhysicalAggregateScanMergeRule;
import org.voltdb.plannerv2.rules.inlining.VoltPhysicalCalcAggregateMergeRule;
import org.voltdb.plannerv2.rules.inlining.VoltPhysicalLimitSerialAggregateMergeRule;
import org.voltdb.plannerv2.rules.inlining.VoltPhysicalLimitSortMergeRule;
import org.voltdb.plannerv2.rules.inlining.VoltPhysicalLimitScanMergeRule;
import org.voltdb.plannerv2.rules.inlining.VoltPhysicalCalcScanMergeRule;
import org.voltdb.plannerv2.rules.logical.MPJoinQueryFallBackRule;
import org.voltdb.plannerv2.rules.logical.MPQueryFallBackRule;
import org.voltdb.plannerv2.rules.logical.VoltLAggregateRule;
import org.voltdb.plannerv2.rules.logical.VoltLCalcRule;
import org.voltdb.plannerv2.rules.logical.VoltLJoinRule;
import org.voltdb.plannerv2.rules.logical.VoltLSortRule;
import org.voltdb.plannerv2.rules.logical.VoltLTableScanRule;
import org.voltdb.plannerv2.rules.physical.VoltPAggregateRule;
import org.voltdb.plannerv2.rules.physical.VoltPCalcRule;
import org.voltdb.plannerv2.rules.physical.VoltPJoinRule;
import org.voltdb.plannerv2.rules.physical.VoltPLimitRule;
import org.voltdb.plannerv2.rules.physical.VoltPSeqScanRule;
import org.voltdb.plannerv2.rules.physical.VoltPSortConvertRule;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Rules used by the VoltDB query planner in various planning stages.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class PlannerRules {

    /**
     * Planning phases and the rules for them.
     */
    public enum Phase {
        LOGICAL {
            @Override public RuleSet getRules() {
                return PlannerRules.LOGICAL;
            }
        },
        // Always use a HEP_BOTTOM_UP planner for MP_FALLBACK, it is match order sensitive.
        MP_FALLBACK {
            @Override public RuleSet getRules() {
                return PlannerRules.MP_FALLBACK;
            }
        },
        PHYSICAL_CONVERSION {
            @Override public RuleSet getRules() {
                return PlannerRules.PHYSICAL_CONVERSION;
            }
        },
        INLINE {
            @Override
            public RuleSet getRules() {
                return PlannerRules.INLINE;
            }
        };

        public abstract RuleSet getRules();
    }

    private static final FilterJoinRule.Predicate MOVEABLE_TO_JOIN_COND = (join, joinType, expr) -> {     // moveableToOn
        if (joinType != JoinRelType.INNER || ! (expr instanceof RexCall)
                || ! expr.isA(SqlKind.EQUALS)) {
            return false;
        }
        final RexNode leftExpression = ((RexCall) expr).getOperands().get(0);
        final RexNode rightExpression = ((RexCall) expr).getOperands().get(1);
        if (leftExpression.isA(SqlKind.INPUT_REF) && rightExpression.isA(SqlKind.LITERAL)
                || rightExpression.isA(SqlKind.INPUT_REF) && leftExpression.isA(SqlKind.LITERAL)) {
            final RelOptTable outerRel = ((RelSubset) join.getLeft()).getOriginal().getTable(),
                    innerRel = ((RelSubset) join.getRight()).getOriginal().getTable();
            final Table outer = ((RelOptTableImpl) outerRel).getTable(),
                    inner = ((RelOptTableImpl) innerRel).getTable();
            final Integer outerPartitionColumn = outer.getPartitionColumn(),
                    innerPartitionColumn = inner.getPartitionColumn();
            if (outerPartitionColumn == null || innerPartitionColumn == null) {
                return false;       // one of the table is replicated.
            }
            final RexCall joinCondition = (RexCall) join.getCondition();
            final int outerTableColumnCount = join.getLeft().getRowType().getFieldCount();
            assert joinCondition.isA(SqlKind.EQUALS);
            final int outerJoinColumnIndex =
                    ((RexInputRef) joinCondition.getOperands().get(0)).getIndex();
            final int innerJoinColumnIndex =
                    ((RexInputRef) joinCondition.getOperands().get(1)).getIndex() -
                            outerTableColumnCount;
            if (outerJoinColumnIndex != outerPartitionColumn ||
                    innerJoinColumnIndex != innerPartitionColumn) {
                return false;       // The columns to be joined are not both partition columns
            }
            final int eqColRef = ((RexInputRef) (leftExpression.isA(SqlKind.INPUT_REF) ?
                    leftExpression : rightExpression)).getIndex();
            return eqColRef == outerPartitionColumn ||
                    eqColRef == outerTableColumnCount + innerPartitionColumn;
        } else {
            return false;
        }
    };

    private static List<Integer> collectColRefs(RexNode node, List<Integer> accum) {
        if (node == null || ! node.isA(SqlKind.COMPARISON)) {
            if (node.isA(SqlKind.INPUT_REF)) {
                accum.add(((RexInputRef) node).getIndex());
            }
        } else {
            for (RexNode child : ((RexCall) node).getOperands()) {
                accum = collectColRefs(child, accum);
            }
        }
        return accum;
    }

    private static boolean pushDownFilter(Join join, JoinRelType joinType, RexNode expr) {
        final int outerTableColumns = join.getLeft().getRowType().getFieldCount();
        return collectColRefs(expr, new ArrayList<>()).stream()
                .map(n -> n < outerTableColumns)
                .collect(Collectors.toSet())
                .size() == 2;
    }

    private static RelOptRule JOIN = new FilterJoinRule.JoinConditionPushRule(
            RelFactories.LOGICAL_BUILDER, PlannerRules::pushDownFilter);
    private static final FilterJoinRule FILTER_ON_JOIN =
            new FilterJoinRule.FilterIntoJoinRule(true, RelFactories.LOGICAL_BUILDER,
                    PlannerRules::pushDownFilter);

    /**
     * Look for suitable Calcite built-in logical rules and add them.
     * Add VoltDB logical rules - they are mostly for giving RelNodes a convention.
     *
     * @see org.apache.calcite.tools.Programs.CALC_RULES
     * @see org.apache.calcite.tools.Programs.RULE_SET
     */
    private static final RuleSet LOGICAL = RuleSets.ofList(
            ProjectMergeRule.INSTANCE,
            FilterMergeRule.INSTANCE,
            // Who produces LogicalCalc? - See comments in LogicalCalc.java
            ProjectToCalcRule.INSTANCE,
            FilterToCalcRule.INSTANCE,
            FilterCalcMergeRule.INSTANCE,
            ProjectCalcMergeRule.INSTANCE,
            // Merge two LogicalCalc's.
            // Is there an example of this merge?
            // - See comments in RexProgramBuilder.mergePrograms()
            CalcMergeRule.INSTANCE,
            //FilterProjectTransposeRule.INSTANCE,
            //FILTER_ON_JOIN,
            FilterJoinRule.FILTER_ON_JOIN,
            //JOIN,
            FilterJoinRule.JOIN,

            // Reduces constants inside a LogicalCalc.
            ReduceExpressionsRule.CALC_INSTANCE,

            // -- VoltDB logical rules.
            VoltLSortRule.INSTANCE,
            VoltLTableScanRule.INSTANCE,
            VoltLCalcRule.INSTANCE,
            VoltLAggregateRule.INSTANCE,
            VoltLJoinRule.INSTANCE

//            // Filter   ->  Project
//            // Project      Filter
//            FilterProjectTransposeRule.INSTANCE,
//            // This is similar to FilterProjectTransposeRule but it's for aggregations.
//            FilterAggregateTransposeRule.INSTANCE,
//            FilterJoinRule.FILTER_ON_JOIN,
//            // For example,
//            //    SELECT deptno, COUNT(*), SUM(bonus), MIN(DISTINCT sal)
//            //    FROM emp
//            //    GROUP BY deptno
//            //
//            // becomes
//            //
//            //    SELECT deptno, SUM(cnt), SUM(bonus), MIN(sal)
//            //    FROM (
//            //          SELECT deptno, COUNT(*) as cnt, SUM(bonus), sal
//            //          FROM EMP
//            //          GROUP BY deptno, sal)            // Aggregate B
//            //    GROUP BY deptno                        // Aggregate A
//            AggregateExpandDistinctAggregatesRule.INSTANCE,
//            // See comments inside for examples.
//            AggregateReduceFunctionsRule.INSTANCE,
//            JoinCommuteRule.INSTANCE,
//            JoinPushThroughJoinRule.LEFT,
//            JoinPushThroughJoinRule.RIGHT,
//            SortProjectTransposeRule.INSTANCE,
    );

    private static final RuleSet MP_FALLBACK = RuleSets.ofList(
            MPQueryFallBackRule.INSTANCE,
            MPJoinQueryFallBackRule.INSTANCE
    );

    private static final RuleSet PHYSICAL_CONVERSION = RuleSets.ofList(
            VoltPCalcRule.INSTANCE,
            VoltPSeqScanRule.INSTANCE,
            VoltPSortConvertRule.INSTANCE_VOLTDB,
            VoltPLimitRule.INSTANCE,
            VoltPAggregateRule.INSTANCE,
            VoltPJoinRule.INSTANCE
    );

    private static final RuleSet INLINE = RuleSets.ofList(
            VoltPhysicalCalcAggregateMergeRule.INSTANCE,
            VoltPhysicalCalcScanMergeRule.INSTANCE,
            VoltPhysicalLimitSerialAggregateMergeRule.INSTANCE,
            VoltPhysicalLimitSortMergeRule.INSTANCE_1,
            VoltPhysicalAggregateScanMergeRule.INSTANCE,
            VoltPhysicalLimitScanMergeRule.INSTANCE_1
    );

    private static final ImmutableList<Program> PROGRAMS = ImmutableList.copyOf(
            Programs.listOf(
                    LOGICAL,
                    MP_FALLBACK,
                    PHYSICAL_CONVERSION,
                    INLINE
            )
    );

    public static ImmutableList<Program> getPrograms() {
        return PROGRAMS;
    }
}
