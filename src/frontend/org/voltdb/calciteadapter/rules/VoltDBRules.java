package org.voltdb.calciteadapter.rules;

import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;

public class VoltDBRules {
    //public static final ConverterRule PROJECT_RULE = new VoltDBProjectRule();
    //public static final RelOptRule PROJECT_SCAN_MERGE_RULE = new VoltDBProjectScanMergeRule();

    public static Program getProgram() {

        Program standardRules = Programs.ofRules(
                CalcMergeRule.INSTANCE,
                FilterCalcMergeRule.INSTANCE,
                FilterToCalcRule.INSTANCE,
                ProjectCalcMergeRule.INSTANCE,
                ProjectToCalcRule.INSTANCE,//);

        // Pull up the send nodes as high as possible
        //Program pullUpSendProg = Programs.ofRules(
                VoltDBSendPullUp.PROJECT,
                VoltDBSendPullUp.CALC,
                VoltDBSendPullUpJoin.INSTANCE,//);

        //Program voltDBConversionRules = Programs.ofRules(
                VoltDBCalcScanMergeRule.INSTANCE,
                VoltDBProjectRule.INSTANCE,
                VoltDBJoinRule.INSTANCE,
                VoltDBCalcJoinMergeRule.INSTANCE);

        Program metaProgram = Programs.sequence(
                standardRules);//,
        //pullUpSendProg,
          //      voltDBConversionRules);

        // We don't actually handle this.
        //    VoltDBProjectJoinMergeRule.INSTANCE

        return metaProgram;
    }
}