package org.voltdb.calciteadapter.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.voltdb.calciteadapter.VoltDBTable;

public class VoltDBPartitionedTableScan extends TableScan {

    private final VoltDBTable m_voltDBTable;

    public VoltDBPartitionedTableScan(RelOptCluster cluster, RelOptTable table, VoltDBTable voltDBTable) {
        // What should trait set be?
        // Note this type of node is not implementable, needs to
        // be transformed first.
        super(cluster, cluster.traitSet(), table);
        this.m_voltDBTable = voltDBTable;
    }
}
