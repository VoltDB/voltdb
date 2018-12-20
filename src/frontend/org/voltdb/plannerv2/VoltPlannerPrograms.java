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
import org.voltdb.plannerv2.rules.logical.VoltDBLAggregateRule;
import org.voltdb.plannerv2.rules.logical.VoltDBLCalcRule;
import org.voltdb.plannerv2.rules.logical.VoltDBLJoinRule;
import org.voltdb.plannerv2.rules.logical.VoltDBLSortRule;
import org.voltdb.plannerv2.rules.logical.VoltDBLTableScanRule;
import org.voltdb.plannerv2.rules.physical.VoltDBPAggregateRule;
import org.voltdb.plannerv2.rules.physical.VoltDBPCalcRule;
import org.voltdb.plannerv2.rules.physical.VoltDBPJoinRule;
import org.voltdb.plannerv2.rules.physical.VoltDBPLimitRule;
import org.voltdb.plannerv2.rules.physical.VoltDBPSeqScanRule;
import org.voltdb.plannerv2.rules.physical.VoltDBPSortConvertRule;

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

    private static final RuleSet CALC_LOGICAL = RuleSets.ofList(
            CalcMergeRule.INSTANCE,
            FilterCalcMergeRule.INSTANCE,
            FilterToCalcRule.INSTANCE,
            ProjectCalcMergeRule.INSTANCE,
            ProjectToCalcRule.INSTANCE,
            ProjectMergeRule.INSTANCE,
            FilterProjectTransposeRule.INSTANCE);

    private static final RuleSet VOLT_LOGICAL = RuleSets.ofList(
            VoltDBLSortRule.INSTANCE,
            VoltDBLTableScanRule.INSTANCE,
            VoltDBLCalcRule.INSTANCE,
            VoltDBLAggregateRule.INSTANCE,
            VoltDBLJoinRule.INSTANCE);

    private static final RuleSet LOGICAL = concat(CALC_LOGICAL, VOLT_LOGICAL);

    private static final RuleSet VOLT_PHYSICAL_CONVERSION = RuleSets.ofList(
            VoltDBPCalcRule.INSTANCE,
            VoltDBPSeqScanRule.INSTANCE,
            VoltDBPSortConvertRule.INSTANCE_VOLTDB,
            VoltDBPLimitRule.INSTANCE,
            VoltDBPAggregateRule.INSTANCE,
            VoltDBPJoinRule.INSTANCE);

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
