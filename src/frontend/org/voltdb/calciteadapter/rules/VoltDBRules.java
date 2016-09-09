package org.voltdb.calciteadapter.rules;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;

public class VoltDBRules {
    //public static final ConverterRule PROJECT_RULE = new VoltDBProjectRule();
    //public static final RelOptRule PROJECT_SCAN_MERGE_RULE = new VoltDBProjectScanMergeRule();

    public static List<RelOptRule> getRules() {
        List<RelOptRule> rules = new ArrayList<>();

        rules.add(VoltDBCalcScanMergeRule.INSTANCE);
        rules.add(VoltDBProjectRule.INSTANCE);
        rules.add(VoltDBProjectRule.INSTANCE);
        rules.add(VoltDBJoinRule.INSTANCE);

        rules.add(VoltDBCalcJoinMergeRule.INSTANCE);

        // We don't actually handle this.
        //rules.add(VoltDBProjectJoinMergeRule.INSTANCE);

        rules.add(CalcMergeRule.INSTANCE);
        rules.add(FilterCalcMergeRule.INSTANCE);
        rules.add(FilterToCalcRule.INSTANCE);
        rules.add(ProjectCalcMergeRule.INSTANCE);
        rules.add(ProjectToCalcRule.INSTANCE);
        rules.add(ProjectToCalcRule.INSTANCE);


        return rules;
    }
}