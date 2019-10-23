package org.voltdb.plannerv2.rules.logical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.sql.SqlKind;

import java.util.List;

/**
 * Logical rule that eliminates LogicalAggregate node with SINGLE_VALUE aggregate function
 * that sits on top of a LogicalCalc node.
 */
public class VoltLAggregateCalcMergeRule extends RelOptRule {
    public static final VoltLAggregateCalcMergeRule INSTANCE = new VoltLAggregateCalcMergeRule();

    private VoltLAggregateCalcMergeRule() {
        super(operand(LogicalAggregate.class, operand(LogicalCalc.class, none())));
    }
    @Override
    public void onMatch(RelOptRuleCall call) {
        final List<AggregateCall> aggs = ((Aggregate) call.rel(0)).getAggCallList();
        if (! aggs.isEmpty() && aggs.get(0).getAggregation().getKind() == SqlKind.SINGLE_VALUE) {
            call.transformTo(call.rel(1));
        }
    }
}
