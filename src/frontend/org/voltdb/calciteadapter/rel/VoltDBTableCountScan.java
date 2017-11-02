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

import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.RexConverter;
import org.voltdb.calciteadapter.VoltDBTable;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.TableCountPlanNode;

public class VoltDBTableCountScan extends AbstractVoltDBTableScan implements VoltDBRel {

    private RelDataType rowType;

    public VoltDBTableCountScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, RelDataType rowType) {
          this(cluster, table, voltDBTable,
                  RexProgram.createIdentity(voltDBTable.getRowType(cluster.getTypeFactory())), rowType);
    }

    protected VoltDBTableCountScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, RexProgram program, RelDataType nodeRowType) {
          super(cluster, table, voltDBTable, program, null, null);
          rowType = nodeRowType;
    }

    @Override
    public RelDataType deriveRowType() {
        return rowType;
    }

    @Override
    public AbstractPlanNode toPlanNode() {
        String tableName = m_voltDBTable.getCatTable().getTypeName();
        List<String> qualName = table.getQualifiedName();
        String tableAlias = qualName.get(0);
        // Generate output schema
        NodeSchema schema = RexConverter.convertToVoltDBNodeSchema(rowType);

        TableCountPlanNode tcpn = new TableCountPlanNode(tableName, tableAlias, schema);
        return tcpn;
    }

}
