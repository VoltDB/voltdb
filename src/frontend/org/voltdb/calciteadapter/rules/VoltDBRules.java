package org.voltdb.calciteadapter.rules;

import org.apache.calcite.rel.rules.CalcMergeRule;
import org.apache.calcite.rel.rules.FilterCalcMergeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterToCalcRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.ProjectCalcMergeRule;
import org.apache.calcite.rel.rules.ProjectToCalcRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.voltdb.calciteadapter.rules.convert.VoltDBJoinRule;
import org.voltdb.calciteadapter.rules.convert.VoltDBProjectRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBCalcJoinMergeRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBCalcScanMergeRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBCalcSendPullUpRule;
import org.voltdb.calciteadapter.rules.rel.VoltDBJoinSendPullUpRule;

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
               
                // Join Order
//                LoptOptimizeJoinRule.INSTANCE,
//                MultiJoinOptimizeBushyRule.INSTANCE,
                JoinCommuteRule.INSTANCE,
                
                FilterJoinRule.FILTER_ON_JOIN,
                FilterJoinRule.JOIN

        // Pull up the send nodes as high as possible
        //Program pullUpSendProg = Programs.ofRules(
                , VoltDBCalcSendPullUpRule.INSTANCE
                , VoltDBJoinSendPullUpRule.INSTANCE
//                , VoltDBProjectSendPullUpRule.INSTANCE
//                VoltDBSendPullUpJoin.INSTANCE,//);

        //Program voltDBConversionRules = Programs.ofRules(
                , VoltDBCalcScanMergeRule.INSTANCE
                , VoltDBProjectRule.INSTANCE
                , VoltDBJoinRule.INSTANCE
                , VoltDBCalcJoinMergeRule.INSTANCE
//                , VoltDBDistributedScanRule.INSTANCE
                );

        Program metaProgram = Programs.sequence(
                standardRules);//,
        //pullUpSendProg,
          //      voltDBConversionRules);

        // We don't actually handle this.
        //    VoltDBProjectJoinMergeRule.INSTANCE

        return metaProgram;
    }
}