package org.voltdb.calciteadapter.rules.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.voltdb.calciteadapter.rel.VoltDBTableScan;

public class VoltDBCalcScanMergeRule extends RelOptRule {

    public static final VoltDBCalcScanMergeRule INSTANCE = new VoltDBCalcScanMergeRule();

    private VoltDBCalcScanMergeRule() {
        super(operand(LogicalCalc.class, operand(VoltDBTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalCalc calc = call.rel(0);
        VoltDBTableScan scan = call.rel(1);
        
        call.transformTo(scan.copy(calc.getProgram()));
    }
}