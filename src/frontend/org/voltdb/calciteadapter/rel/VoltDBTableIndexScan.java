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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.VoltDBTable;
import org.voltdb.catalog.Index;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;

public class VoltDBTableIndexScan extends AbstractVoltDBTableScan implements VoltDBRel {

    private final Index m_index;

    public VoltDBTableIndexScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, Index index) {
          this(cluster, table, voltDBTable,
                  RexProgram.createIdentity(voltDBTable.getRowType(cluster.getTypeFactory())),
                  index);
    }

    protected VoltDBTableIndexScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, RexProgram program, Index index) {
          super(cluster, table, voltDBTable, program);
          assert(index != null);
          m_index = index;
    }

    /**
     * The digest needs to be updated because Calcite considers any two nodes with the same digest
     * to be identical.
     */
    @Override
    protected String computeDigest() {
        String dg = super.computeDigest();
        dg += "_index_" + m_index.getTypeName();
        return dg;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        pw.item("index", m_index.getTypeName());
        return pw;
    }

    @Override
    public AbstractPlanNode toPlanNode() {

        List<String> qualName = table.getQualifiedName();
        IndexScanPlanNode ispn =
                new IndexScanPlanNode(m_voltDBTable.getCatTable().getTypeName(), qualName.get(0), m_index);

        addPredicate(ispn);
        addLimitOffset(ispn);
        addProjection(ispn);

        return ispn;
    }

    public RelNode copy(RexProgram program, RexBuilder programRexBuilder) {
        return AbstractVoltDBTableScan.copy(this, program, programRexBuilder);
    }

    public RelNode copyWithLimitOffset(RexNode limit, RexNode offset) {
        // Do we need a deep copy including the inputs?
        VoltDBTableIndexScan newScan =
                new VoltDBTableIndexScan(getCluster(), getTable(), m_voltDBTable, m_program, m_index);
        newScan.m_limit = (limit == null) ? m_limit : limit;
        newScan.m_offset = (offset == null) ? m_offset : offset;

        return newScan;
    }

    public Index getIndex() {
        return m_index;
    }
}
