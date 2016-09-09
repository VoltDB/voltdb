package org.voltdb.calciteadapter.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.voltdb.calciteadapter.VoltDBTable;

public class VoltDBPartitionedTableScan extends TableScan {

    private final VoltDBTable m_voltDBTable;

    public VoltDBPartitionedTableScan(RelOptCluster cluster, RelOptTable table, VoltDBTable voltDBTable) {
        super(cluster, cluster.traitSet(), table);
        this.m_voltDBTable = voltDBTable;
    }
}
