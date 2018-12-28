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

package org.voltdb.plannerv2.rel;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;

/**
 * This is from Mike A. as an adapter of <code>org.apache.calcite.rel.core.TableScan</code> that
 * views a <code>org.voltdb.calciteadapter.rel.VoltDBTable</code> as a relational expression.
 */
public abstract class AbstractVoltTableScan extends TableScan {

    protected final VoltTable m_voltTable;

    protected AbstractVoltTableScan(RelOptCluster cluster,
                                      RelTraitSet traitSet,
                                      RelOptTable table,
                                      VoltTable voltTable) {
        super(cluster, traitSet, table);
        assert(voltTable != null) : "VoltTable is null";
        this.m_voltTable = voltTable;
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
        return dg + "_" + m_voltTable.getCatalogTable().getTypeName();
    }

    public VoltTable getVoltTable() {
        return m_voltTable;
    }
}
