/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import org.apache.calcite.plan.RelOptTable;
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
