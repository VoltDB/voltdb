package org.voltdb.plannerv2;

import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
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

import com.google.common.collect.ImmutableList;

/**
 * Rules used by VoltDB for various planning stages.
 * @author Yiqun Zhang
 * @since 8.4
 */
public class VoltPlannerRules {

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
     * See {@link org.apache.calcite.tools.Programs.CALC_RULES}
     * Calcite logical rules are for dealing with projection and filters.
     * The consecutive projections and filters are later merged into a LogicalCalc. */
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
            FilterProjectTransposeRule.INSTANCE);

    private static final RuleSet VOLT_LOGICAL = RuleSets.ofList(
            VoltLSortRule.INSTANCE,
            VoltLTableScanRule.INSTANCE,
            VoltLCalcRule.INSTANCE,
            VoltLAggregateRule.INSTANCE,
            VoltLJoinRule.INSTANCE);

    private static final RuleSet VOLT_PHYSICAL_CONVERSION = RuleSets.ofList(
            VoltPCalcRule.INSTANCE,
            VoltPSeqScanRule.INSTANCE,
            VoltPSortConvertRule.INSTANCE_VOLTDB,
            VoltPLimitRule.INSTANCE,
            VoltPAggregateRule.INSTANCE,
            VoltPJoinRule.INSTANCE);

    private static final ImmutableList<Program> PROGRAMS = ImmutableList.copyOf(
            Programs.listOf(
                    LOGICAL,
                    VOLT_PHYSICAL_CONVERSION)
            );

    public static ImmutableList<Program> getPrograms() {
        return PROGRAMS;
    }
}
