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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.planner.ParsedSelectStmt.ParsedColInfo;
import org.voltdb.planner.ParsedUnionStmt;
import org.voltdb.planner.PartitioningForStatement;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtTableScan caches data related to a given instance of a sub-query within the statement scope
 */
public class StmtSubqueryScan extends StmtTableScan {

    // Sub-Query
    private final AbstractParsedStmt m_subquery;
    private ArrayList<SchemaColumn> m_outputColumnList = new ArrayList<>();
    private Map<String, Integer> m_outputColumnIndexMap = new HashMap<String, Integer>();

    private CompiledPlan m_bestCostPlan = null;
    // The partitioning object for that sub-query
    PartitioningForStatement m_partitioning = null;

    /*
     * This 'subquery' actually is the parent query on the derived table with alias 'tableAlias'
     */
    public StmtSubqueryScan(AbstractParsedStmt subquery, String tableAlias) {
        super(tableAlias);
        m_subquery = subquery;
        // A union or other set operator uses the output columns of its left-most leaf child statement.
        while (subquery instanceof ParsedUnionStmt) {
            assert( ! ((ParsedUnionStmt)subquery).m_children.isEmpty());
            subquery = ((ParsedUnionStmt)subquery).m_children.get(0);
        }
        assert (subquery instanceof ParsedSelectStmt);

        int i = 0;
        for (ParsedColInfo col: ((ParsedSelectStmt)subquery).displayColumns()) {
            String colAlias = col.alias == null? col.columnName : col.alias;
            SchemaColumn scol = new SchemaColumn(col.tableName, col.tableAlias, col.columnName, col.alias, col.expression);
            m_outputColumnList.add(scol);
            m_outputColumnIndexMap.put(colAlias, i);
            i++;
        }

    }

    @Override
    public String getTableName() {
        // Because derived table must have specify an alias, use its alias instead.
        return m_tableAlias;
    }

    /**
     * The subquery is replicated if all tables from the FROM clause defining this subquery
     * are replicated
     * @return True if the subquery is replicated
     */
    @Override
    public boolean getIsReplicated() {
        boolean isReplicated = true;
        for (StmtTableScan tableScan : m_subquery.m_tableAliasMap.values()) {
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

    static final List<Index> noIndexesSupportedOnSubqueryScans = new ArrayList<Index>();
    @Override
    public List<Index> getIndexes() {
        return noIndexesSupportedOnSubqueryScans;
    }

    public AbstractParsedStmt getSubquery() {
        return m_subquery;
    }

    public CompiledPlan getBestCostPlan() {
        return m_bestCostPlan;
    }

    public void setBestCostPlan(CompiledPlan costPlan) {
        m_bestCostPlan = costPlan;
    }

    @Override
    public String getColumnName(int m_columnIndex) {
        return m_outputColumnList.get(m_columnIndex).getColumnName();
    }

    @Override
    public void processTVE(TupleValueExpression expr, String columnName) {
        Integer idx = m_outputColumnIndexMap.get(columnName);
        if (idx == null) {
            throw new PlanningErrorException("Mismatched columns " + columnName + " in subquery");
        }
        SchemaColumn schemaCol = m_outputColumnList.get(idx.intValue());

        expr.setColumnIndex(idx.intValue());
        expr.setValueType(schemaCol.getType());
        expr.setValueSize(schemaCol.getSize());
    }

}
