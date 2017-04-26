package org.voltdb.calciteadapter.rules.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.voltdb.calciteadapter.rel.VoltDBSend;

public class VoltDBCalcSendPullUpRule extends RelOptRule {

    public static final VoltDBCalcSendPullUpRule INSTANCE = new VoltDBCalcSendPullUpRule();


    private VoltDBCalcSendPullUpRule() {
        super(operand(LogicalCalc.class, operand(VoltDBSend.class, any())), 
                VoltDBCalcSendPullUpRule.class.getSimpleName());
    }

    @Override public boolean matches(RelOptRuleCall call) {
        boolean m = super.matches(call);
        LogicalCalc calc = call.rel(0);
        VoltDBSend send = call.rel(1);
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalCalc calc = call.rel(0);
        VoltDBSend send = call.rel(1);

        call.transformTo(send.copy(calc.getProgram()));
    }
}