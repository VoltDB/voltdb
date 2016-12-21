/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltcore.utils.Pair;
import org.voltdb.catalog.Index;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.planner.ParsedUnionStmt;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtTableScan caches data related to a given instance of a sub-query within the statement scope
 */
public class StmtSubqueryScan extends StmtTableScan {
    // Sub-Query
    private final AbstractParsedStmt m_subqueryStmt;
    private final ArrayList<SchemaColumn> m_outputColumnList = new ArrayList<>();
    private final Map<Pair<String, Integer>, Integer> m_outputColumnIndexMap = new HashMap<>();

    private CompiledPlan m_bestCostPlan = null;

    private StatementPartitioning m_subqueriesPartitioning = null;

    private boolean m_failedSingleFragmentTest = false;

    private boolean m_tableAggregateSubquery = false;

    /*
     * This 'subquery' actually is the parent query on the derived table with alias 'tableAlias'
     */
    public StmtSubqueryScan(AbstractParsedStmt subqueryStmt, String tableAlias, int stmtId) {
        super(tableAlias, stmtId);
        m_subqueryStmt = subqueryStmt;

        // A union or other set operator uses the output columns of its left-most leaf child statement.
        while (subqueryStmt instanceof ParsedUnionStmt) {
            assert( ! ((ParsedUnionStmt)subqueryStmt).m_children.isEmpty());
            subqueryStmt = ((ParsedUnionStmt)subqueryStmt).m_children.get(0);
        }
        assert (subqueryStmt instanceof ParsedSelectStmt);

        int i = 0;
        for (ParsedColInfo col: ((ParsedSelectStmt)subqueryStmt).displayColumns()) {
            String colAlias = col.alias == null? col.columnName : col.alias;
            SchemaColumn scol = col.asSchemaColumn();
            m_outputColumnList.add(scol);
            m_outputColumnIndexMap.put(Pair.of(colAlias, col.differentiator), i);
            i++;
        }
    }

    public StmtSubqueryScan(AbstractParsedStmt subqueryStmt, String tableAlias) {
        this(subqueryStmt, tableAlias, 0);
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
            Set< Set<AbstractExpression> > eqSets) {
        assert(m_subqueriesPartitioning != null);
        if (m_subqueriesPartitioning.getCountOfPartitionedTables() == 0 ||
                m_subqueriesPartitioning.requiresTwoFragments()) {
            return;
        }

        // This subquery is a single partitioned query on partitioned tables
        // promoting the single partition expression up to its parent level.
        AbstractExpression spExpr = m_subqueriesPartitioning.singlePartitioningExpression();

        for (SchemaColumn col: m_partitioningColumns) {
            AbstractExpression tveKey = col.getExpression();
            assert(tveKey instanceof TupleValueExpression);
            Set<AbstractExpression> values = null;
            if (valueEquivalence.containsKey(tveKey)) {
                values = valueEquivalence.get(tveKey);
            }
            else if (valueEquivalence.containsKey(spExpr)) {
                values = valueEquivalence.get(spExpr);
            }
            else {
                for (SchemaColumn otherCol: m_partitioningColumns) {
                    if (col != otherCol &&
                        valueEquivalence.containsKey(otherCol.getExpression())) {
                        values = valueEquivalence.get(otherCol.getExpression());
                        break;
                    }
                }
                if (values == null) {
                    values = new HashSet<>();
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
            AbstractExpression tveKey, AbstractExpression spExpr) {
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
        if (m_partitioningColumns != null) {
            return m_partitioningColumns;
        }

        m_partitioningColumns = new ArrayList<>();
        assert(m_subqueriesPartitioning != null);

        if (m_subqueriesPartitioning.getCountOfPartitionedTables() > 0) {
            for (StmtTableScan tableScan : m_subqueryStmt.allScans()) {
                List<SchemaColumn> scols = tableScan.getPartitioningColumns();
                if (scols != null) {
                    addPartitioningColumns(scols);
                }
            }
        }
        return m_partitioningColumns;
    }

    private void addPartitioningColumns(List<SchemaColumn> scols) {
        // The partitioning columns have to be in its output column list
        // in order to be referenced on parent level.
        for (SchemaColumn partitionCol: scols) {
            SchemaColumn matchedCol = null;
            // Find whether the partition column is in output column list
            for (SchemaColumn outputCol: m_outputColumnList) {
                AbstractExpression outputExpr = outputCol.getExpression();
                if ( ! (outputExpr instanceof TupleValueExpression)) {
                    continue;
                }

                TupleValueExpression tve = (TupleValueExpression)outputExpr;
                if (tve.getTableName().equals(partitionCol.getTableName()) &&
                        tve.getColumnName().equals(partitionCol.getColumnName())) {
                    matchedCol = outputCol;
                    break;
                }
            }

            String colNameForParentQuery;
            if (matchedCol != null) {
                colNameForParentQuery = matchedCol.getColumnAlias();
            }
            // single partition sub-query case can be single partition without
            // including partition column in its display column list
            else if ( ! m_subqueriesPartitioning.requiresTwoFragments()) {
                colNameForParentQuery = partitionCol.getColumnName();
            }
            else {
                continue;
            }
            partitionCol.reset(m_tableAlias, m_tableAlias,
                    colNameForParentQuery, colNameForParentQuery);
            m_partitioningColumns.add(partitionCol);
        }
    }

    @Override
    public String getTableName() {
        // Because a derived table must have an alias, use its alias instead.
        return m_tableAlias;
    }

    /**
     * The subquery is replicated if all tables from the FROM clause defining this subquery
     * are replicated
     * @return True if the subquery is replicated
     */
    @Override
    public boolean getIsReplicated() {
        for (StmtTableScan tableScan : m_subqueryStmt.allScans()) {
            if ( ! tableScan.getIsReplicated()) {
                return false;
            }
        }
        return true;
    }

    public List<StmtTargetTableScan> getAllTargetTables() {
        List <StmtTargetTableScan> stmtTables = new ArrayList<>();
        for (StmtTableScan tableScan : m_subqueryStmt.allScans()) {
            if (tableScan instanceof StmtTargetTableScan) {
                stmtTables.add((StmtTargetTableScan)tableScan);
            }
            else {
                assert(tableScan instanceof StmtSubqueryScan);
                StmtSubqueryScan subScan = (StmtSubqueryScan)tableScan;
                stmtTables.addAll(subScan.getAllTargetTables());
            }
        }

        return stmtTables;
    }

    static final List<Index> noIndexesSupportedOnSubqueryScans = new ArrayList<>();
    @Override
    public List<Index> getIndexes() {
        return noIndexesSupportedOnSubqueryScans;
    }

    public AbstractParsedStmt getSubqueryStmt() {
        return m_subqueryStmt;
    }

    public CompiledPlan getBestCostPlan() {
        return m_bestCostPlan;
    }

    public void setBestCostPlan(CompiledPlan costPlan) {
        m_bestCostPlan = costPlan;
    }

    @Override
    public String getColumnName(int columnIndex) {
        return getSchemaColumn(columnIndex).getColumnName();
    }

    public SchemaColumn getSchemaColumn(int columnIndex) {
        return m_outputColumnList.get(columnIndex);
    }

    public Integer getColumnIndex(String columnAlias, int differentiator) {
        return m_outputColumnIndexMap.get(Pair.of(columnAlias, differentiator));
    }

    @Override
    public AbstractExpression processTVE(TupleValueExpression expr, String columnName) {
        Integer idx = m_outputColumnIndexMap.get(Pair.of(columnName, expr.getDifferentiator()));
        if (idx == null) {
            throw new PlanningErrorException("Mismatched columns " + columnName + " in subquery");
        }
        SchemaColumn schemaCol = m_outputColumnList.get(idx.intValue());

        expr.setColumnIndex(idx.intValue());
        expr.setTypeSizeAndInBytes(schemaCol);
        return expr;
    }

    /**
     * Some subquery results can only be joined with a partitioned table after
     * it finishes some work on the coordinator. With the 2 fragment plan limit,
     * those queries can not be supported.
     * Other than that case, the planner will typically have added a
     * send/receive pair to the subquery plan that is actually only suitable to
     * a stand-alone plan. This function distinguishes subqueries that should NOT
     * have a send/receive pair.
     * @param root
     * @return true if there is no aspect to the plan that requires execution on the coordinator.
     */
    public boolean canRunInOneFragment() {
        assert(m_subqueriesPartitioning != null);
        assert(m_subqueryStmt != null);

        if (m_subqueriesPartitioning.getCountOfPartitionedTables() == 0) {
            return true;
        }

        // recursive check for its nested subqueries that require coordination
        // of their results.
        if (failsSingleFragmentTest()) {
            return false;
        }

        // Tentative assignment in case of early return.
        // This gets immediately reset if it passes all the tests.
        m_failedSingleFragmentTest = true;

        if (m_subqueryStmt instanceof ParsedUnionStmt) {
            // Union are just returned
            return false;
        }

        if ( ! (m_subqueryStmt instanceof ParsedSelectStmt)) {
            throw new PlanningErrorException("Unsupported subquery found in FROM clause:" +
                    m_subqueryStmt);
        }

        ParsedSelectStmt selectStmt = (ParsedSelectStmt)m_subqueryStmt;

        // Now If query has LIMIT/OFFSET/DISTINCT on a replicated table column,
        // we should get rid of the receive node. I (--paul) don't know what this means.
        if (selectStmt.hasLimitOrOffset() || selectStmt.hasDistinctWithGroupBy()) {
            return false;
        }

        // If the query uses the partitioned materialized view table with the
        // need to Re-aggregate, then we can not get rid of the receive node.
        // This is also caught in StatementPartitioning when analyzing the join criteria,
        // because it contains a partitioned view that does not have a partition column.
        if (selectStmt.m_mvFixInfo.needed()) {
            return false;
        }

        // Table aggregate cases should not get rid of the receive node
        if (selectStmt.hasAggregateOrGroupby()) {
            if (!selectStmt.isGrouped()) {
                m_tableAggregateSubquery = true;
                return false;
            }

            // For group by queries, there are two cases on group by columns.
            // (1) Does not contain a partition column:
            // If joined with a partitioned table in the parent query, it will
            // violate the partitioned table join criteria.
            // Detect case (1) to mark receive node.
            if ( ! selectStmt.hasPartitionColumnInGroupby()) {
                return false;
            }
        }
        if ( ! selectStmt.hasPartitionColumnInWindowFunctionExpression()) {
            return false;
        }

        // Now. If this sub-query joins with a partitioned table in the parent statement,
        // push the join down by removing the send/receive plan node pair.
        m_failedSingleFragmentTest = false;
        return true;
    }

    public boolean failsSingleFragmentTest() {
        if (m_failedSingleFragmentTest) {
            return true;
        }

        for (StmtTableScan tableScan : m_subqueryStmt.allScans()) {
            if (tableScan instanceof StmtSubqueryScan) {
                StmtSubqueryScan subScan = (StmtSubqueryScan)tableScan;
                if (subScan.failsSingleFragmentTest()) {
                    // Cache known test failures on parent subqueries.
                    m_failedSingleFragmentTest = true;
                    return true;
                }
            }
        }
        return false;
    }

    public boolean isTableAggregate() {
        return m_tableAggregateSubquery;
    }

    /** Produce a tuple value expression for a column produced by this subquery */
    public TupleValueExpression getOutputExpression(int index) {
        SchemaColumn schemaCol = m_outputColumnList.get(index);
        TupleValueExpression tve = new TupleValueExpression(getTableAlias(), getTableAlias(),
                schemaCol.getColumnAlias(), schemaCol.getColumnAlias(), index);
        return tve;
    }

    public List<SchemaColumn> getOutputSchema() {
        return m_outputColumnList;
    }

    public String calculateContentDeterminismMessage() {
        return m_subqueryStmt.calculateContentDeterminismMessage();
    }
}
