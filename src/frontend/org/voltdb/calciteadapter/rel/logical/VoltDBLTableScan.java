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

package org.voltdb.calciteadapter.rel.logical;

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.voltdb.calciteadapter.rel.VoltDBTable;
import org.voltdb.calciteadapter.rel.AbstractVoltDBTableScan;

public class VoltDBLTableScan extends AbstractVoltDBTableScan implements VoltDBLRel {

    public VoltDBLTableScan(RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table,
            VoltDBTable voltDBTable) {
          super(cluster,
                  traitSet,
                  table,
                  voltDBTable);
    }

    @Override
    public RelNode copy(RelTraitSet traits, List<RelNode> inputs) {
        return new VoltDBLTableScan(
                getCluster(),
                traits,
                getTable(),
                getVoltDBTable());
    }

    public VoltDBLTableScan copy() {
        return new VoltDBLTableScan(
                getCluster(),
                getTraitSet(),
                getTable(),
                getVoltDBTable());
    }
}
