package org.voltdb.plannerv2;

import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.voltdb.plannerv2.rules.logical.VoltLAggregateRule;
import org.voltdb.plannerv2.rules.logical.VoltLCalcRule;
import org.voltdb.plannerv2.rules.logical.VoltLJoinRule;
import org.voltdb.plannerv2.rules.logical.VoltLSortRule;
import org.voltdb.plannerv2.rules.logical.VoltLTableScanRule;

import com.google.common.collect.ImmutableList;

/**
 * Rules used by the VoltDB query planner in various planning stages.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class VoltPlannerRules {

    /**
     * Planning phases and the rules for them.
     */
    public enum Phase {
        LOGICAL {
            @Override
            public RuleSet getRules() {
                return VoltPlannerRules.LOGICAL;
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
            // Merge two LogicalCalc's.
            // Who produces LogicalCalc? - See comments in LogicalCalc.java
            // Is there an example of this merge?
            // - See comments in RexProgramBuilder.mergePrograms()
            CalcMergeRule.INSTANCE,
            FilterCalcMergeRule.INSTANCE,
            FilterToCalcRule.INSTANCE,
            ProjectCalcMergeRule.INSTANCE,
            ProjectToCalcRule.INSTANCE,
            ProjectMergeRule.INSTANCE,
            // Filter   ->  Project
            // Project      Filter
            FilterProjectTransposeRule.INSTANCE,
            // This is similar to FilterProjectTransposeRule but it's for aggregations.
            FilterAggregateTransposeRule.INSTANCE,
            FilterJoinRule.FILTER_ON_JOIN,
            // For example,
            //    SELECT deptno, COUNT(*), SUM(bonus), MIN(DISTINCT sal)
            //    FROM emp
            //    GROUP BY deptno
            //
            // becomes
            //
            //    SELECT deptno, SUM(cnt), SUM(bonus), MIN(sal)
            //    FROM (
            //          SELECT deptno, COUNT(*) as cnt, SUM(bonus), sal
            //          FROM EMP
            //          GROUP BY deptno, sal)            // Aggregate B
            //    GROUP BY deptno                        // Aggregate A
            AggregateExpandDistinctAggregatesRule.INSTANCE,
            // See comments inside for examples.
            AggregateReduceFunctionsRule.INSTANCE,
            JoinCommuteRule.INSTANCE,
            JoinPushThroughJoinRule.LEFT,
            JoinPushThroughJoinRule.RIGHT,
            SortProjectTransposeRule.INSTANCE,

            // -- VoltDB logical rules.
            VoltLSortRule.INSTANCE,
            VoltLTableScanRule.INSTANCE,
            VoltLCalcRule.INSTANCE,
            VoltLAggregateRule.INSTANCE,
            VoltLJoinRule.INSTANCE
            );

    private static final ImmutableList<Program> PROGRAMS = ImmutableList.copyOf(
            Programs.listOf(
                    LOGICAL
                    )
            );

    public static ImmutableList<Program> getPrograms() {
        return PROGRAMS;
    }
}
