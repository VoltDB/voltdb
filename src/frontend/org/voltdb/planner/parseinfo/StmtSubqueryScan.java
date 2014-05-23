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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.planner.ParsedSelectStmt.ParsedColInfo;
import org.voltdb.planner.ParsedUnionStmt;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.StatementPartitioning;
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

    private StatementPartitioning m_subqueriesPartitioning = null;

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

    public StatementPartitioning getPartitioningForStatement() {
        return m_subqueriesPartitioning;
    }

    public void setSubqueriesPartitioning(StatementPartitioning subqueriesPartitioning) {
        assert(subqueriesPartitioning != null);
        m_subqueriesPartitioning = subqueriesPartitioning;
        findPartitioningColumns();
    }

    /**
     * upgrade single partitioning expression to parent level
     * add the info to equality sets and input value equivalence
     * @param valueEquivalence
     * @param eqSets
     */
    public void promoteSinglePartitionInfo(
            HashMap<AbstractExpression, Set<AbstractExpression>> valueEquivalence,
            Set< Set<AbstractExpression> > eqSets)
    {
        StatementPartitioning stmtPartitioning = getPartitioningForStatement();

        if (stmtPartitioning.getCountOfPartitionedTables() == 0 ||
            stmtPartitioning.requiresTwoFragments()) {
            return;
        }
        // this sub-query is single partitioned query on partitioned tables
        // promoting the single partition express up the its parent level
        AbstractExpression spExpr = stmtPartitioning.singlePartitioningExpression();

        for (SchemaColumn col: m_partitioningColumns) {
            AbstractExpression tveKey = col.getExpression();
            assert(tveKey instanceof TupleValueExpression);
            Set<AbstractExpression> values = null;
            if (valueEquivalence.containsKey(tveKey)) {
                values = valueEquivalence.get(tveKey);
            } else if (valueEquivalence.containsKey(spExpr)) {
                values = valueEquivalence.get(spExpr);
            } else {
                for (SchemaColumn otherCol: m_partitioningColumns) {
                    if (col == otherCol) continue;
                    if (valueEquivalence.containsKey(otherCol.getExpression())) {
                        values = valueEquivalence.get(otherCol.getExpression());
                        break;
                    }
                }
                if (values == null) {
                    values = new HashSet<AbstractExpression>();
                }
            }
            updateEqualSets(values, valueEquivalence, eqSets, tveKey, spExpr);
        }
    }

    // update the new equal sets for partitioning columns and values
    // (Xin): If it changes valueEquivalence, we have to update eqSets
    // Because HashSet stored a legacy hashcode for the non-final object.
    private void updateEqualSets(Set<AbstractExpression> values,
            HashMap<AbstractExpression, Set<AbstractExpression>> valueEquivalence,
            Set< Set<AbstractExpression> > eqSets,
            AbstractExpression tveKey, AbstractExpression spExpr)
    {
        boolean hasLegacyValues = false;
        if (eqSets.contains(values)) {
            eqSets.remove(values);
            hasLegacyValues = true;
        }
        values.add(spExpr);
        values.add(tveKey);
        if (hasLegacyValues) {
            eqSets.add(values);
        }
        valueEquivalence.put(spExpr, values);
        valueEquivalence.put(tveKey, values);
    }

    // exported subquery partitioning column(s)
    private List<SchemaColumn> findPartitioningColumns() {
        if (m_partitioningColumns != null)
            return m_partitioningColumns;

        m_partitioningColumns = new ArrayList<>();
        assert(m_subqueriesPartitioning != null);

        if (m_subqueriesPartitioning.getCountOfPartitionedTables() > 0) {
            for (StmtTableScan tableScan : m_subquery.m_tableAliasMap.values()) {
                List<SchemaColumn> scols;
                scols = tableScan.getPartitioningColumns();
                addPartitioningColumns(scols);
            }
        }
        return m_partitioningColumns;
    }

    private void addPartitioningColumns(List<SchemaColumn> scols) {
        if (scols == null) return;

        // The partitioning columns have to be in its output column list
        // in order to be referenced on parent level.
        for (SchemaColumn partitionCol: scols) {
            boolean existsInDisplayList = false;
            // Find whether the partition column is in output column list
            for (SchemaColumn outputCol: m_outputColumnList) {
                if (outputCol.getExpression() instanceof TupleValueExpression)
                {
                    TupleValueExpression tve = (TupleValueExpression) outputCol.getExpression();
                    if (tve.getTableName().equals(partitionCol.getTableName()) &&
                        tve.getColumnName().equals(partitionCol.getColumnName()))
                    {
                        existsInDisplayList = true;

                        String colNameForParentQuery = outputCol.getColumnAlias();
                        partitionCol.reset(m_tableAlias, m_tableAlias,
                                colNameForParentQuery, colNameForParentQuery);
                        m_partitioningColumns.add(partitionCol);
                        break;
                    }
                }
            }
            // single partition sub-query case can be single partition without
            // including partition column in its display column list
            if (! existsInDisplayList && ! m_subqueriesPartitioning.requiresTwoFragments()) {
                String colNameForParentQuery = partitionCol.getColumnName();
                partitionCol.reset(m_tableAlias, m_tableAlias,
                        colNameForParentQuery, colNameForParentQuery);
                m_partitioningColumns.add(partitionCol);
            }
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

    public List<StmtTargetTableScan> getAllTargetTables() {
        List <StmtTargetTableScan> stmtTables = new ArrayList<StmtTargetTableScan>();
        for (StmtTableScan tableScan : m_subquery.m_tableAliasMap.values()) {
            if (tableScan instanceof StmtTargetTableScan) {
                stmtTables.add((StmtTargetTableScan)tableScan);
            } else {
                assert(tableScan instanceof StmtSubqueryScan);
                StmtSubqueryScan subScan = (StmtSubqueryScan)tableScan;
                stmtTables.addAll(subScan.getAllTargetTables());
            }
        }

        return stmtTables;
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
        expr.setInBytes(schemaCol.getExpression().getInBytes());
    }

}
