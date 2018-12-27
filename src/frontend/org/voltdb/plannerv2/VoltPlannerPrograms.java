package org.voltdb.plannerv2;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
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
 * Programs used by VoltDB for various planning stages.
 * @author Yiqun Zhang
 * @since 8.4
 */
public class VoltPlannerPrograms {

    public enum directory {
        CALC_LOGICAL,
        VOLT_LOGICAL,
        LOGICAL,
        VOLT_PHYSICAL_CONVERSION
    }

    /**
     * See {@link org.apache.calcite.tools.Programs.CALC_RULES}
     * Calcite logical rules are for dealing with projection and filters.
     * The consecutive projections and filters are later merged into a LogicalCalc. */
    private static final RuleSet CALC_LOGICAL = RuleSets.ofList(
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

    private static final RuleSet LOGICAL = concat(CALC_LOGICAL, VOLT_LOGICAL);

    private static final RuleSet VOLT_PHYSICAL_CONVERSION = RuleSets.ofList(
            VoltPCalcRule.INSTANCE,
            VoltPSeqScanRule.INSTANCE,
            VoltPSortConvertRule.INSTANCE_VOLTDB,
            VoltPLimitRule.INSTANCE,
            VoltPAggregateRule.INSTANCE,
            VoltPJoinRule.INSTANCE);

    private static final ImmutableList<Program> PROGRAMS = ImmutableList.copyOf(
            Programs.listOf(
                    CALC_LOGICAL,
                    VOLT_LOGICAL,
                    LOGICAL,
                    VOLT_PHYSICAL_CONVERSION)
            );

    static RuleSet concat(RuleSet... ruleSets) {
        final List<RelOptRule> ruleList = new ArrayList<>();
        for (final RuleSet ruleSet : ruleSets) {
            for (final RelOptRule relOptRule : ruleSet) {
                ruleList.add(relOptRule);
            }
        }
        return RuleSets.ofList(ruleList);
    }

    public static ImmutableList<Program> get() {
        return PROGRAMS;
    }
}
