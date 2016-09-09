package org.voltdb.calciteadapter.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;

public class VoltDBSendReceive extends VoltDBProject {
    public VoltDBSendReceive(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelDataType rowType) {
        super(cluster,
                traitSet,
                input,
                cluster.getRexBuilder().identityProjects(input.getRowType()),
                input.getRowType());
    }

}
