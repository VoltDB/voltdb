package org.voltdb.calciteadapter.rules;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.voltdb.calciteadapter.VoltDBConvention;
import org.voltdb.calciteadapter.rel.VoltDBPartitionedTableScan;

public class VoltDBPartitionedTableScanRule extends ConverterRule {
    static final VoltDBPartitionedTableScanRule INSTANCE = new VoltDBPartitionedTableScanRule();

    private VoltDBPartitionedTableScanRule() {
      super(LogicalProject.class, RelOptUtil.PROJECT_PREDICATE, Convention.NONE,
          VoltDBConvention.INSTANCE, "VoltDBProjectRule");
    }

    @Override
    public RelNode convert(RelNode rel) {
      final VoltDBPartitionedTableScan scan = (VoltDBPartitionedTableScan)rel;
      return rel;
    }

}
