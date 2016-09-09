package org.voltdb.calciteadapter.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.util.Pair;
import org.voltdb.calciteadapter.rel.VoltDBJoin;
import org.voltdb.calciteadapter.rel.VoltDBProject;

class VoltDBProjectJoinMergeRule extends RelOptRule {

    public static final VoltDBProjectJoinMergeRule INSTANCE = new VoltDBProjectJoinMergeRule();

    private VoltDBProjectJoinMergeRule() {
        super(operand(VoltDBProject.class, operand(VoltDBJoin.class, any())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        VoltDBProject proj= call.rel(0);
        VoltDBJoin join = call.rel(1);

        RexBuilder rexBuilder = proj.getCluster().getRexBuilder();
        RexProgramBuilder rpb = new RexProgramBuilder(join.getRowType(), rexBuilder);

        int i = 0;
        for (Pair<RexNode, String> item : proj.getNamedProjects()) {
            rpb.addProject(item.left, item.right);
        }
        call.transformTo(join.copy(rpb.getProgram()));
    }

}