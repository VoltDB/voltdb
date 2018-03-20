/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.calciteadapter.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.voltdb.calciteadapter.rel.VoltDBTable;

public abstract class AbstractVoltDBTableScan extends TableScan {

    public static final int MAX_TABLE_ROW_COUNT = 1000000;

    protected final VoltDBTable m_voltDBTable;

    protected AbstractVoltDBTableScan(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            VoltDBTable voltDBTable) {
          super(cluster, traitSet, table);
          this.m_voltDBTable = voltDBTable;
    }

    /**
     * The digest needs to be updated because Calcite considers any two nodes with the same digest
     * to be identical.
     */
    @Override
    protected String computeDigest() {
        // Make an instance of the scan unique for Calcite to be able to distinguish them
        // specially when we merge scans with other redundant nodes like sort for example.
        // Are there better ways of doing this?
        String dg = super.computeDigest();
        return dg + "_" + m_voltDBTable.getCatTable().getTypeName();
    }

    public VoltDBTable getVoltDBTable() {
        return m_voltDBTable;
    }

    @Override public RelDataType deriveRowType() {
        return table.getRowType();
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner,
            RelMetadataQuery mq) {
        double dRows = estimateRowCount(mq);
        double dCpu = dRows + 1; // ensure non-zero cost
        double dIo = 0;
        RelOptCost cost = planner.getCostFactory().makeCost(dRows, dCpu, dIo);
        return cost;
    }

}
