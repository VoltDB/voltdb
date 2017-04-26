package org.voltdb.calciteadapter.rules.rel;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.voltdb.calciteadapter.VoltDBPartitioning;
import org.voltdb.calciteadapter.rel.VoltDBJoin;
import org.voltdb.calciteadapter.rel.VoltDBSend;

public class VoltDBJoinSendPullUpRule extends RelOptRule {

      public static final VoltDBJoinSendPullUpRule INSTANCE = new VoltDBJoinSendPullUpRule();

      private VoltDBJoinSendPullUpRule() {
          super(operand(VoltDBJoin.class, operand(VoltDBSend.class, any()),
                  operand(VoltDBSend.class, any())));
      }

      @Override
      public boolean matches(RelOptRuleCall call) {
          VoltDBJoin join = call.rel(0);
          VoltDBSend left = call.rel(1);
          VoltDBSend right = call.rel(2);

          VoltDBPartitioning leftPartitioning = left.getPartitioning();
          VoltDBPartitioning rightPartitioning = right.getPartitioning();
          RexNode joinCondition = join.getCondition();
          boolean innerJoin = join.getJoinType() == JoinRelType.INNER;
          boolean compartiblePartitions = leftPartitioning.isCompartible(rightPartitioning, joinCondition);
          return innerJoin && compartiblePartitions;
      }

      @Override
      public void onMatch(RelOptRuleCall call) {
          VoltDBJoin join = call.rel(0);
          VoltDBSend left = call.rel(1);
          VoltDBSend right = call.rel(2);

          VoltDBPartitioning leftPartitioning = left.getPartitioning();
          VoltDBPartitioning rightPartitioning = right.getPartitioning();

          RexNode joinCondition = join.getCondition();
          RelNode newJoin = join.copy(left.getInput(), right.getInput());
          VoltDBPartitioning combinePartitioning = leftPartitioning.mergeWith(rightPartitioning, joinCondition);
          VoltDBSend combinedSend = new VoltDBSend(
                  join.getCluster(),
                  join.getTraitSet(),
                  newJoin,
                  join.getCluster().getRexBuilder().identityProjects(join.getRowType()),
                  join.getRowType(),
                  combinePartitioning,
                  0.5);

          call.transformTo(combinedSend);
      }

  }