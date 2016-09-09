package org.voltdb.calciteadapter.rules;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.voltdb.calciteadapter.rel.VoltDBJoin;

class VoltDBCalcJoinMergeRule extends RelOptRule {

      public static final VoltDBCalcJoinMergeRule INSTANCE = new VoltDBCalcJoinMergeRule();

      private VoltDBCalcJoinMergeRule() {
          super(operand(LogicalCalc.class, operand(VoltDBJoin.class, any())));
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
          LogicalCalc calc = call.rel(0);
          VoltDBJoin join = call.rel(1);

          call.transformTo(join.copy(calc.getProgram()));
      }

  }