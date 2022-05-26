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

import java.util.*;

import org.voltdb.catalog.Index;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.CompiledPlan;
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.planner.ParsedUnionStmt;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.planner.StmtEphemeralTableScan;
import org.voltdb.plannodes.SchemaColumn;

/**
 * StmtTableScan caches data related to a given instance of a sub-query within the statement scope
 */
public class StmtSubqueryScan extends StmtEphemeralTableScan {
    // Sub-Query
    /**
     * This is the parsed statement defining this subquery.
     */
    private final AbstractParsedStmt m_subqueryStmt;

    private boolean m_failedSingleFragmentTest = false;

    private boolean m_tableAggregateSubquery = false;

    /**
     * When this scan is planned, this is where the best plan will be cached.
     */
    private CompiledPlan m_bestCostPlan = null;

    /*
     * This 'subquery' actually is the parent query on the derived table with alias 'tableAlias'
     */
    public StmtSubqueryScan(AbstractParsedStmt subqueryStmt, String tableAlias, int stmtId) {
        super(tableAlias, tableAlias, stmtId);
        m_subqueryStmt = subqueryStmt;

        // A union or other set operator uses the output columns of its left-most leaf child statement.
        while (subqueryStmt instanceof ParsedUnionStmt) {
            assert( ! ((ParsedUnionStmt)subqueryStmt).m_children.isEmpty());
            subqueryStmt = ((ParsedUnionStmt)subqueryStmt).m_children.get(0);
        }
        assert (subqueryStmt instanceof ParsedSelectStmt);

        for (ParsedColInfo col: ((ParsedSelectStmt)subqueryStmt).displayColumns()) {
            SchemaColumn scol = col.asSchemaColumn();
            addOutputColumn(scol);
        }
    }

    public StmtSubqueryScan(AbstractParsedStmt subqueryStmt, String tableAlias) {
        this(subqueryStmt, tableAlias, 0);
    }

    @Override
    public void setScanPartitioning(StatementPartitioning currentPartitioning) {
        assert(currentPartitioning != null);
        super.setScanPartitioning(currentPartitioning);
        findPartitioningColumns();
    }

    /**
     * upgrade single partitioning expression to parent level
     * add the info to equality sets and input value equivalence
     * @param valueEquivalence
     * @param eqSets
     */
    public void promoteSinglePartitionInfo(
            Map<AbstractExpression, Set<AbstractExpression>> valueEquivalence,
            Set<Set<AbstractExpression>> eqSets) {
        if (getScanPartitioning() == null) {
            throw new PlanningErrorException("Unsupported statement, subquery expressions are only supported for " +
                    "single partition procedures and AdHoc replicated tables.");
        } else if (getScanPartitioning().getCountOfPartitionedTables() == 0 || getScanPartitioning().requiresTwoFragments()) {
            return;
        }

        // This subquery is a single partitioned query on partitioned tables
        // promoting the single partition expression up to its parent level.
        AbstractExpression spExpr = getScanPartitioning().singlePartitioningExpression();

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
            Map<AbstractExpression, Set<AbstractExpression>> valueEquivalence,
            Set<Set<AbstractExpression> > eqSets,
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
        assert(getScanPartitioning() != null);

        if (getScanPartitioning().getCountOfPartitionedTables() > 0) {
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
            for (SchemaColumn outputCol: getOutputSchema()) {
                AbstractExpression outputExpr = outputCol.getExpression();
                if (! (outputExpr instanceof TupleValueExpression)) {
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
            } else if ( ! getScanPartitioning().requiresTwoFragments()) {
                // single partition sub-query case can be single partition without
                // including partition column in its display column list
                colNameForParentQuery = partitionCol.getColumnName();
            } else {
                continue;
            }
            partitionCol.reset(m_tableAlias, m_tableAlias,
                    colNameForParentQuery, colNameForParentQuery);
            m_partitioningColumns.add(partitionCol);
        }
    }

    /**
     * The subquery is replicated if all tables from the FROM clause defining this subquery
     * are replicated
     * @return True if the subquery is replicated
     */
    @Override
    public boolean getIsReplicated() {
        return m_subqueryStmt.allScans().stream().allMatch(StmtTableScan::getIsReplicated);
    }

    public List<StmtTargetTableScan> getAllTargetTables() {
        List <StmtTargetTableScan> stmtTables = new ArrayList<>();
        for (StmtTableScan tableScan : m_subqueryStmt.allScans()) {
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

    @Override
    public List<Index> getIndexes() {
        return noIndexesSupportedOnSubqueryScansOrCommonTables;
    }

    public AbstractParsedStmt getSubqueryStmt() {
        return m_subqueryStmt;
    }

    @Override
    public String getColumnName(int columnIndex) {
        return getSchemaColumn(columnIndex).getColumnName();
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
    @Override
    public boolean canRunInOneFragment() {
        assert(getScanPartitioning() != null);
        assert(m_subqueryStmt != null);

        if (getScanPartitioning().getCountOfPartitionedTables() == 0) {
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
        } if (! (m_subqueryStmt instanceof ParsedSelectStmt)) {
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
            if (! selectStmt.hasPartitionColumnInGroupby()) {
                return false;
            }
        }
        if (! selectStmt.hasPartitionColumnInWindowFunctionExpression()) {
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
        SchemaColumn schemaCol = getSchemaColumn(index);
        return new TupleValueExpression(getTableAlias(), getTableAlias(),
                schemaCol.getColumnAlias(), schemaCol.getColumnAlias(), index);
    }

    public String calculateContentDeterminismMessage() {
        return m_subqueryStmt.calculateContentDeterminismMessage();
    }

    @Override
    public JoinNode makeLeafNode(int nodeId, AbstractExpression joinExpr, AbstractExpression whereExpr) {
        SubqueryLeafNode leafNode = new SubqueryLeafNode(nodeId, joinExpr, whereExpr, this);
        leafNode.updateContentDeterminismMessage(calculateContentDeterminismMessage());
        return leafNode;
    }

    @Override
    public boolean isOrderDeterministic(boolean orderIsDeterministic) {
        CompiledPlan plan = getBestCostPlan();
        // If the plan is null, there is an error in the query,
        // so we can return anything.
        if (plan != null) {
            return orderIsDeterministic && plan.isOrderDeterministic();
        }
        return orderIsDeterministic;
    }

    @Override
    public String contentNonDeterminismMessage(String isContentDeterministic) {
        /*
         * If it's already known to be content deterministic, and
         * we have an error message, then use that.
         */
        if (isContentDeterministic != null) {
            return isContentDeterministic;
        }
        CompiledPlan plan = getBestCostPlan();
        // If the plan is null we don't care, since there is
        // an error in the query.
        if (plan != null && !plan.isContentDeterministic()) {
            isContentDeterministic = plan.nondeterminismDetail();
        }
        return isContentDeterministic;
    }

    @Override
    public boolean hasSignificantOffsetOrLimit(boolean hasSignificantOffsetOrLimit) {
        // Offsets or limits in subqueries are only significant (only effect content determinism)
        // when they apply to un-ordered subquery contents.  If the plan is
        // null there is some error, so this computation is irrelevant.
        CompiledPlan scanBestPlan = getBestCostPlan();
        if (scanBestPlan != null) {
            return hasSignificantOffsetOrLimit || (! scanBestPlan.isOrderDeterministic() && scanBestPlan.hasLimitOrOffset());
        }
        return hasSignificantOffsetOrLimit;
    }
    public final CompiledPlan getBestCostPlan() {
        return m_bestCostPlan;
    }

    public final void setBestCostPlan(CompiledPlan costPlan) {
        m_bestCostPlan = costPlan;
    }
}
