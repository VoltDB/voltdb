/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
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

    // An original subquery scan that was optimized out and replaced by this table scan
    // It's required for the column indexes resolution
    private StmtSubqueryScan m_origSubqueryScan = null;

    public StmtTargetTableScan(Table table, String tableAlias, int stmtId) {
        super(tableAlias, stmtId);
        assert (table != null);
        m_table = table;

        findPartitioningColumns();
    }

    public StmtTargetTableScan(Table table) {
        this(table, table.getTypeName(), 0);
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

        TupleValueExpression tve = new TupleValueExpression(
                tbName, m_tableAlias, partitionCol, partitionCol.getIndex());

        String colName = partitionCol.getTypeName();
        SchemaColumn scol =
                new SchemaColumn(tbName, m_tableAlias, colName, colName, tve);
        m_partitioningColumns = new ArrayList<>();
        m_partitioningColumns.add(scol);
        return m_partitioningColumns;
    }

    @Override
    public List<Index> getIndexes() {
        if (m_indexes == null) {
            m_indexes = new ArrayList<>();
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

    /*
     * Process this tve named columnName.  Most often we just
     * resolve the tve in the table of this scan.  But if this
     * is a table which is a replacement for a derived table,
     * we may need to return the expression which is in the
     * display list for the derived table and update aliases
     * in tves in that expression.
     */
    @Override
    public AbstractExpression processTVE(TupleValueExpression tve, String columnName) {
        if (m_origSubqueryScan == null) {
            tve.resolveForTable(m_table);
            return tve;
        }

        // Example:
        // SELECT TA1.CA CA1 FROM (SELECT T.C CA FROM T TA) TA1;
        // gets simplified into
        // SELECT TA1.C CA1 FROM T TA1;
        // The TA1(TA1).(CA)CA1 TVE needs to be adjusted to be T(TA1).C(CA)
        // since the original SELECT T.C CA FROM T TA subquery was optimized out.
        // Table name TA1 is replaced with the original table name T
        // Column name CA is replaced with the original column name C
        // Expression differentiator to be replaced with the differentiator
        // from the original column (T.C)
        Integer columnIndex = m_origSubqueryScan.getColumnIndex(columnName,
                tve.getDifferentiator());
        if (columnIndex == null) {
            throw new PlanningErrorException("Column <"
                                                + columnName
                                                + "> not found. Please update your query.",
                                             1);
        }
        SchemaColumn originalSchemaColumn = m_origSubqueryScan.getSchemaColumn(columnIndex);
        assert(originalSchemaColumn != null);
        String origColumnName = originalSchemaColumn.getColumnName();
        // Get the original column expression and adjust its aliases
        AbstractExpression colExpr = originalSchemaColumn.getExpression();
        List<TupleValueExpression> tves = ExpressionUtil.getTupleValueExpressions(colExpr);
        for (TupleValueExpression subqTve : tves) {
            if (subqTve == colExpr) {
                subqTve.setTableName(getTableName());
                subqTve.setColumnName(origColumnName);
                subqTve.setColumnAlias(tve.getColumnAlias());
            }
            subqTve.setTableAlias(tve.getTableAlias());
            subqTve.resolveForTable(m_table);
        }
        return colExpr;
    }

    public void setOriginalSubqueryScan(StmtSubqueryScan origSubqueryScan) {
        m_origSubqueryScan = origSubqueryScan;
    }

    @Override
    public JoinNode makeLeafNode(int nodeId, AbstractExpression joinExpr, AbstractExpression whereExpr) {
        return new TableLeafNode(nodeId, joinExpr, whereExpr, this);
    }
}
