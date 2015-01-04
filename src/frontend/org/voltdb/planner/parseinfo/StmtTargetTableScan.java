/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltdb.planner.parseinfo;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.catalog.Column;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.utils.CatalogUtil;

/**
 * StmtTableScan caches data related to a given instance of a table within the statement scope
 */
public class StmtTargetTableScan extends StmtTableScan {
    // Catalog table
    private final Table m_table;
    private List<Index> m_indexes;
    private List<Column> m_columns;

    public StmtTargetTableScan(Table table, String tableAlias) {
        super(tableAlias);
        assert (table != null);
        m_table = table;

        findPartitioningColumns();
    }

    @Override
    public String getTableName() {
        return m_table.getTypeName();
    }

    public Table getTargetTable() {
        assert(m_table != null);
        return m_table;
    }

    @Override
    public boolean getIsReplicated() {
        return m_table.getIsreplicated();
    }

    private List<SchemaColumn> findPartitioningColumns() {
        if (m_partitioningColumns != null) {
            return m_partitioningColumns;
        }

        if (getIsReplicated()) {
            return null;
        }
        Column partitionCol = m_table.getPartitioncolumn();
        // "(partitionCol != null)" tests around an obscure edge case.
        // The table is declared non-replicated yet specifies no partitioning column.
        // This can occur legitimately when views based on partitioned tables neglect to group by the partition column.
        // The interpretation of this edge case is that the table has "randomly distributed data".
        // In such a case, the table is valid for use by MP queries only and can only be joined with replicated tables
        // because it has no recognized partitioning join key.
        if (partitionCol == null) {
            return null;
        }

        String tbName = m_table.getTypeName();
        String colName = partitionCol.getTypeName();

        TupleValueExpression tve = new TupleValueExpression(
                tbName, m_tableAlias, colName, colName, partitionCol.getIndex());
        tve.setTypeSizeBytes(partitionCol.getType(), partitionCol.getSize(), partitionCol.getInbytes());

        SchemaColumn scol = new SchemaColumn(tbName, m_tableAlias, colName, colName, tve);
        m_partitioningColumns = new ArrayList<SchemaColumn>();
        m_partitioningColumns.add(scol);
        return m_partitioningColumns;
    }

    @Override
    public List<Index> getIndexes() {
        if (m_indexes == null) {
            m_indexes = new ArrayList<Index>();
            for (Index index : m_table.getIndexes()) {
                m_indexes.add(index);
            }
        }
        return m_indexes;
    }

    @Override
    public String getColumnName(int columnIndex) {
        if (m_columns == null) {
            m_columns = CatalogUtil.getSortedCatalogItems(m_table.getColumns(), "index");
        }
        return m_columns.get(columnIndex).getTypeName();
    }

    @Override
    public void processTVE(TupleValueExpression expr, String columnName) {
        expr.resolveForTable(m_table);
    }


}
