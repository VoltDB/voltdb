package org.voltdb.calciteadapter.rules.rel;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalProject;
import org.voltdb.calciteadapter.rel.VoltDBSend;

public class VoltDBProjectSendPullUpRule extends RelOptRule {

    public static final VoltDBProjectSendPullUpRule INSTANCE = new VoltDBProjectSendPullUpRule();


    private VoltDBProjectSendPullUpRule() {
        super(operand(LogicalProject.class, operand(VoltDBSend.class, any())));
    }

    @Override public boolean matches(RelOptRuleCall call) {
        boolean m = super.matches(call);
        LogicalProject proj = call.rel(0);
        VoltDBSend send = call.rel(1);
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalProject proj = call.rel(0);
        VoltDBSend send = call.rel(1);

        call.transformTo(send.copy(proj.getProjects(), proj.getRowType()));
    }
}