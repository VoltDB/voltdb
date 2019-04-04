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
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
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
import org.voltdb.plannerv2.rules.logical.MPSetOpsQueryFallBackRule;
import org.voltdb.plannerv2.rules.logical.VoltLAggregateRule;
import org.voltdb.plannerv2.rules.logical.VoltLCalcRule;
import org.voltdb.plannerv2.rules.logical.VoltLJoinRule;
import org.voltdb.plannerv2.rules.logical.VoltLSetOpsRule;
import org.voltdb.plannerv2.rules.logical.VoltLSortRule;
import org.voltdb.plannerv2.rules.logical.VoltLTableScanRule;
import org.voltdb.plannerv2.rules.logical.VoltLValuesRule;
import org.voltdb.plannerv2.rules.physical.VoltPAggregateRule;
import org.voltdb.plannerv2.rules.physical.VoltPCalcRule;
import org.voltdb.plannerv2.rules.physical.VoltPJoinRule;
import org.voltdb.plannerv2.rules.physical.VoltPLimitRule;
import org.voltdb.plannerv2.rules.physical.VoltPSeqScanRule;
import org.voltdb.plannerv2.rules.physical.VoltPSetOpsRule;
import org.voltdb.plannerv2.rules.physical.VoltPSortConvertRule;
import org.voltdb.plannerv2.rules.physical.VoltPValuesRule;

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
            FilterProjectTransposeRule.INSTANCE,
            FilterJoinRule.FILTER_ON_JOIN,
            FilterJoinRule.JOIN,
            // combining two non-distinct SetOps into a single
            UnionMergeRule.INSTANCE,
            UnionMergeRule.INTERSECT_INSTANCE,
            UnionMergeRule.MINUS_INSTANCE,
            ProjectSetOpTransposeRule.INSTANCE,
            FilterSetOpTransposeRule.INSTANCE,

            // Reduces constants inside a LogicalCalc.
            ReduceExpressionsRule.CALC_INSTANCE,

            // -- VoltDB logical rules.
            VoltLSortRule.INSTANCE,
            VoltLTableScanRule.INSTANCE,
            VoltLCalcRule.INSTANCE,
            VoltLAggregateRule.INSTANCE,
            VoltLJoinRule.INSTANCE,
            VoltLSetOpsRule.INSTANCE_UNION,
            VoltLSetOpsRule.INSTANCE_INTERSECT,
            VoltLSetOpsRule.INSTANCE_EXCEPT,
            VoltLValuesRule.INSTANCE

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
            MPJoinQueryFallBackRule.INSTANCE,
            MPSetOpsQueryFallBackRule.INSTANCE
    );

    private static final RuleSet PHYSICAL_CONVERSION = RuleSets.ofList(
            VoltPCalcRule.INSTANCE,
            VoltPSeqScanRule.INSTANCE,
            VoltPSortConvertRule.INSTANCE_VOLTDB,
            VoltPLimitRule.INSTANCE,
            VoltPAggregateRule.INSTANCE,
            VoltPJoinRule.INSTANCE,
            VoltPSetOpsRule.INSTANCE_UNION,
            VoltPSetOpsRule.INSTANCE_INTERSECT,
            VoltPSetOpsRule.INSTANCE_EXCEPT,
            VoltPValuesRule.INSTANCE
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
