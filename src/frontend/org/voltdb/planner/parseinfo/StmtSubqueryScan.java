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
/// I think it's confusing to reuse the subquery variable here to hold the subquery child statement that
/// defines the "real" subquery's schema, instead of a separate local variable (as it was) initially set = subquery
/// This code is technically correct since m_subquery is already safely set, but it's just a little confusing.
/// This needs a comment like -- A union or other set operator uses the output columns of its left-most leaf child statement.
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
    public TABLE_SCAN_TYPE getScanType() {
        return TABLE_SCAN_TYPE.TEMP_TABLE_SCAN;
    }

/// This is wrong. There is probably no actual catalog table by this name and if there WAS by accident a table by this name, this might cause confusion.
/// A possible alternative is to return null -- SOME of the callers would be satisfied by this value (and more correct).
/// Other callers could ask for the alias instead if that's what they want, or they could ask for the alias only if they got a null table name.
/// Another idea is to remove this function from StmtTableScan and fix each caller to do the right thing.
/// That might mean adding a cast after "if (tableScan instanceof StmtTargetTableScan)... " or "assert(tableScan instanceof StmtTargetTableScan)".
/// Caller by caller, decide "What does it mean for the tableScan here to be a subquery?"
/// "What alternative value (null? the alias?) or alternative other action would make the most sense here?"
    @Override
    public String getTableName() {
        // derived table name is generated as "SYSTEM_SUBQUERY" + "hashCode".
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
    public void resolveTVE(TupleValueExpression expr, String columnName) {
        Integer idx = m_outputColumnIndexMap.get(columnName);
        if (idx == null) {
            throw new PlanningErrorException("Mismatched columns " + columnName + " in subquery");
        }
        SchemaColumn schemaCol = m_outputColumnList.get(idx.intValue());

        expr.setColumnIndex(idx.intValue());
        expr.setValueType(schemaCol.getType());
        expr.setValueSize(schemaCol.getSize());


/// This code is repeated in StmtTargetTableScan's resolveTVE and it operates on StmtTableScan members -- refactor out to a protected StmtTableScan method and make the members private.
        if (!m_scanColumnNameSet.contains(columnName)) {
            SchemaColumn scol = new SchemaColumn("", m_tableAlias,
                    columnName, columnName, (TupleValueExpression) expr.clone());
            m_scanColumnNameSet.add(columnName);
            m_scanColumnsList.add(scol);
        }
    }

}
