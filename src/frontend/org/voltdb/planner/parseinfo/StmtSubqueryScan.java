/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.planner.ParsedUnionStmt;
import org.voltdb.planner.PartitioningForStatement;
import org.voltdb.planner.ParsedSelectStmt.ParsedColInfo;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtTableScan caches data related to a given instance of a sub-query within the statement scope
 */
public class StmtSubqueryScan extends StmtTableScan {

    // Sub-Query
    private final AbstractParsedStmt m_subquery;
    private final List<ParsedColInfo> m_origSchema;
    private CompiledPlan m_bestCostPlan = null;
    // The partitioning object for that sub-query
    PartitioningForStatement m_partitioning = null;
    // The mapping - the temp table column to the sub-query (column, table) pair
    Map<String, ParsedColInfo> m_columnMap = new HashMap<String, ParsedColInfo>();
    // Store a unique list of the subquery result columns actually used by the parent query.
    protected Map<String, SchemaColumn> m_scanColumns = new HashMap<String, SchemaColumn>();

    public StmtSubqueryScan(String tableAlias, AbstractParsedStmt subquery) {
        super(tableAlias);
        m_subquery = subquery;
        AbstractParsedStmt columnBase = subquery;
        while (columnBase instanceof ParsedUnionStmt) {
            assert( ! ((ParsedUnionStmt)columnBase).m_children.isEmpty());
            columnBase = ((ParsedUnionStmt)columnBase).m_children.get(0);
        }
        assert (columnBase instanceof ParsedSelectStmt);
        m_origSchema = ((ParsedSelectStmt)columnBase).displayColumns();
        assert(m_origSchema != null);
    }

    @Override
    public TABLE_SCAN_TYPE getScanType() {
        return TABLE_SCAN_TYPE.TEMP_TABLE_SCAN;
    }

    @Override
    public String getTableName() {
        return null;
    }

    /**
     * The subquery is replicated if all tables from the FROM clause defining this subquery
     * are replicated
     * @return True if the subquery is replicated
     */
    @Override
    public boolean getIsReplicated() {
        boolean isReplicated = true;
        for (StmtTableScan tableScan : m_subquery.tableAliasMap.values()) {
            isReplicated = isReplicated && tableScan.getIsReplicated();
            if ( ! isReplicated) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void setPartitioning(PartitioningForStatement partitioning) {
        m_partitioning = partitioning;
    }

    @Override
    public String getPartitionColumnName() {
        //TODO: implement identification of exported subquery partitioning column(s)
        return null;
    }

    static final Collection<Index> noIndexesSupportedOnSubqueryScans = new ArrayList<Index>();
    @Override
    public Collection<Index> getIndexes() {
        return noIndexesSupportedOnSubqueryScans;
    }

    public AbstractParsedStmt getSubquery() {
        return m_subquery;
    }

    public List<ParsedColInfo> getOrigSchema() {
        return m_origSchema;
    }

    public CompiledPlan getBestCostPlan() {
        return m_bestCostPlan;
    }

    public void setBestCostPlan(CompiledPlan costPlan) {
        m_bestCostPlan = costPlan;
    }

    @Override
    public String getColumnName(int columnIndex) {
        return m_origSchema.get(columnIndex).alias;
    }

    @Override
    public void resolveTVE(TupleValueExpression expr, String columnName) {
        // TODO resolve index by matching columnName to subquery output column aliases
        // expr.resolveForTable(m_table);
        if (m_scanColumns.get(columnName) == null) {
            SchemaColumn scol = new SchemaColumn("", m_tableAlias,
                    columnName, columnName, (TupleValueExpression) expr.clone());
            m_scanColumns.put(columnName, scol);
        }
    }

    @Override
    public Collection<SchemaColumn> getScanColumns() {
        return m_scanColumns.values();
    }
}
