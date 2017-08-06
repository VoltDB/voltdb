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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.voltdb.calciteadapter.VoltDBTable;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Index;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.types.IndexType;
import org.voltdb.utils.CatalogUtil;

public class VoltDBTableIndexScan extends AbstractVoltDBTableScan implements VoltDBRel {

    private final Index m_index;

    public VoltDBTableIndexScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, Index index) {
          this(cluster, table, voltDBTable,
                  RexProgram.createIdentity(voltDBTable.getRowType(cluster.getTypeFactory())),
                  index);
    }

    public VoltDBTableIndexScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, RexProgram program, Index index) {
        this(cluster, table, voltDBTable, program, index, null, null);
    }

    public VoltDBTableIndexScan(RelOptCluster cluster, RelOptTable table,
            VoltDBTable voltDBTable, RexProgram program, Index index, RexNode limit, RexNode offset) {
          super(cluster, table, voltDBTable, program, limit, offset);
          assert(index != null);
          m_index = index;
          //Set collation trait from the index if it's a sortable one
          if (IndexType.isScannable(m_index.getType())) {
              // @TODO Ignore the expressions for a sec
              CatalogMap<Column> tableColumns = m_voltDBTable.getCatTable().getColumns();
              List<ColumnRef> indexedColRefs = CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
              List<RelFieldCollation> collationsList = new ArrayList<>();
              for (ColumnRef indexColumnRef : indexedColRefs) {
                  String indexColumnName = indexColumnRef.getTypeName();
                  Column indexColumn = tableColumns.get(indexColumnName);
                  int columnIndex = indexColumn.getIndex();
                  collationsList.add(new RelFieldCollation(columnIndex));
              }
              RelCollation indexCollation =  RelCollations.of(collationsList);
              RelCollation outputCollation = indexCollation;
              if (program != null) {
                  // Convert index collation to take the program into an account
                  outputCollation = adjustIndexCollation(program, indexCollation);
              }
              traitSet = getTraitSet().replace(outputCollation);
          } else {
              // Hash index
              traitSet = getTraitSet().replace(RelCollations.EMPTY);
          }
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

    public RelCollation getCollation() {
        // @TODO if we can get collations from the RelMetadataQuery then we don't need to
        // make Sort and Scan to be next to each other (I think)
        // final RelMetadataQuery mq = call.getMetadataQuery();
        //mq.collations(scan);
        RelTrait collationTrait = getTraitSet().getTrait(RelCollations.EMPTY.getTraitDef());
        assert (collationTrait instanceof RelCollation);
        return (RelCollation) collationTrait;
    }

    /**
     * Convert index collation into a collation that is adjusted for this node possible projection.
     * Adopted from the RexProgram.deduceCollations
     *
     * @param program - IndexScan node program
     * @param inputCollation - index collation
     * @return RelCollation
     */
    public static RelCollation adjustIndexCollation(
            RexProgram program,
            RelCollation inputCollation) {
        assert (program != null);

        int sourceCount = program.getInputRowType().getFieldCount();
        List<RexLocalRef> refs = program.getProjectList();
        int[] targets = new int[sourceCount];
        Arrays.fill(targets, -1);
        for (int i = 0; i < refs.size(); i++) {
            final RexLocalRef ref = refs.get(i);
            final int source = ref.getIndex();
            if ((source < sourceCount) && (targets[source] == -1)) {
                targets[source] = i;
            }
        }
        final List<RelFieldCollation> fieldCollations = new ArrayList<>(0);
        for (RelFieldCollation fieldCollation : inputCollation.getFieldCollations()) {
            final int source = fieldCollation.getFieldIndex();
            final int target = targets[source];
            if (target < 0) {
                // Stop at the first mismatched field
                return RelCollations.of(fieldCollations);
            }
            fieldCollations.add(
                    fieldCollation.copy(target));
        }

        // Success -- all of the source fields of this key are mapped
        // to the output.
        return RelCollations.of(fieldCollations);
    }

}
