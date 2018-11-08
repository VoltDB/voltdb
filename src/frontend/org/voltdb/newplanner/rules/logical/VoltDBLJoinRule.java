package org.voltdb.newplanner.rules.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.voltdb.calciteadapter.rel.logical.VoltDBLJoin;
import org.voltdb.calciteadapter.rel.logical.VoltDBLRel;

public class VoltDBLJoinRule extends RelOptRule {
    public static final VoltDBLJoinRule INSTANCE = new VoltDBLJoinRule();

    VoltDBLJoinRule() {
        super(operand(LogicalJoin.class, Convention.NONE, any()));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalJoin join = call.rel(0);
        RelNode left = join.getLeft();
        RelNode right = join.getRight();
        RelTraitSet convertedTraits = join.getTraitSet().replace(VoltDBLRel.VOLTDB_LOGICAL);
        RelNode convertedLeft = convert(left, left.getTraitSet().replace(VoltDBLRel.VOLTDB_LOGICAL));
        RelNode convertedRight = convert(right, right.getTraitSet().replace(VoltDBLRel.VOLTDB_LOGICAL));
        ImmutableList<RelDataTypeField> systemFieldList = ImmutableList.copyOf(join.getSystemFieldList());

        call.transformTo(new VoltDBLJoin(join.getCluster(), convertedTraits, convertedLeft, convertedRight,
                join.getCondition(), join.getVariablesSet(), join.getJoinType(),
                join.isSemiJoinDone(), systemFieldList));
    }
}
