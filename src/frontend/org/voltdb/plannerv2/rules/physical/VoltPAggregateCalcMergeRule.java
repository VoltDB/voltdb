package org.voltdb.plannerv2.rules.physical;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.sql.SqlKind;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalCalc;
import org.voltdb.plannerv2.rel.physical.VoltPhysicalHashAggregate;

import java.util.List;

/**
 * When Calcite has a HashAggregate node on top of PhysicalCalc node and the aggregate function is
 * SINGLE_VALUE, there is no need to aggregation.
 */
public class VoltPAggregateCalcMergeRule extends RelOptRule {

    public static final VoltPAggregateCalcMergeRule INSTANCE = new VoltPAggregateCalcMergeRule();

    private VoltPAggregateCalcMergeRule() {
        super(operand(VoltPhysicalHashAggregate.class, operand(VoltPhysicalCalc.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final List<AggregateCall> aggs = ((Aggregate) call.rel(0)).getAggCallList();
        if (! aggs.isEmpty() && aggs.get(0).getAggregation().getKind() == SqlKind.SINGLE_VALUE) {
            call.transformTo(call.rel(1));
        }
    }
}
