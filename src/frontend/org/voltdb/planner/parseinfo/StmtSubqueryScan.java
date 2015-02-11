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
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.planner.ParsedUnionStmt;
import org.voltdb.planner.PlanningErrorException;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.ReceivePlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.plannodes.SendPlanNode;
import org.voltdb.types.PlanNodeType;

/**
 * StmtTableScan caches data related to a given instance of a sub-query within the statement scope
 */
public class StmtSubqueryScan extends StmtTableScan {
    // Sub-Query
    private final AbstractParsedStmt m_subqueryStmt;
    private final ArrayList<SchemaColumn> m_outputColumnList = new ArrayList<>();
    private final Map<String, Integer> m_outputColumnIndexMap = new HashMap<String, Integer>();

    private CompiledPlan m_bestCostPlan = null;

    private StatementPartitioning m_subqueriesPartitioning = null;

    private boolean m_hasReceiveNode = false;

    private boolean m_tableAggregateSubquery = false;

    /*
     * This 'subquery' actually is the parent query on the derived table with alias 'tableAlias'
     */
    public StmtSubqueryScan(AbstractParsedStmt subqueryStmt, String tableAlias) {
        super(tableAlias);
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
            for (StmtTableScan tableScan : m_subqueryStmt.m_tableAliasMap.values()) {

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
        for (StmtTableScan tableScan : m_subqueryStmt.m_tableAliasMap.values()) {
            isReplicated = isReplicated && tableScan.getIsReplicated();
            if ( ! isReplicated) {
                return false;
            }
        }
        return true;
    }

    public List<StmtTargetTableScan> getAllTargetTables() {
        List <StmtTargetTableScan> stmtTables = new ArrayList<StmtTargetTableScan>();
        for (StmtTableScan tableScan : m_subqueryStmt.m_tableAliasMap.values()) {
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
        expr.setTypeSizeBytes(schemaCol.getType(), schemaCol.getSize(),
                schemaCol.getExpression().getInBytes());

    }


    /**
     * Some subquery results can only be joined with a partitioned table after it finishes work
     * on the coordinator. With 2 fragment plan limit, those queries should not be supported.
     * Other than that, planner can get rid of the send/receive pair and push down the join.
     * @param root
     * @return
     */
    public AbstractPlanNode processReceiveNode(AbstractPlanNode root) {
        assert(m_subqueriesPartitioning != null);
        if (! m_subqueriesPartitioning.requiresTwoFragments()) {
            return root;
        }
        assert(root.findAllNodesOfType(PlanNodeType.RECEIVE).size() == 1);
        assert(m_subqueryStmt != null);

        // recursive check for its nested subqueries for should have receive node.
        if (hasReceiveNode()) {
            m_hasReceiveNode = true;
            return root;
        }

        m_hasReceiveNode = true;
        ParsedSelectStmt selectStmt = (ParsedSelectStmt)m_subqueryStmt;
        if (selectStmt == null) {
            // Union are just returned
            assert(m_subqueryStmt instanceof ParsedUnionStmt);
            return root;
        }

        // Now If query has LIMIT/OFFSET/DISTINCT on a replicated table column,
        // we should get rid of the receive node.
        if (selectStmt.hasLimitOrOffset() || selectStmt.hasDistinctWithGroupBy()) {
            return root;
        }

        // If the query contains the partition materialized table with the need to Re-aggregate,
        // then we can not get rid of the receive node.
        // This is also caught in StatementPartitioning when analysing the join criteria,
        // because it contains a partitioned view that does not have partition column.
        if (selectStmt.m_mvFixInfo.needed()) {
            return root;
        }

        // Table aggregate cases should not get rid of the receive node
        if (selectStmt.hasAggregateOrGroupby()) {
            if (!selectStmt.isGrouped()) {
                m_tableAggregateSubquery = true;
                return root;
            }
            // For group by queries, there are two cases on group by columns.
            // (1) Does not Contain the partition columns: If join with partition table on outer
            //     level, it will violates the join criteria.
            // Detect case (1) to mark receive node.
            if (! selectStmt.hasPartitionColumnInGroupby()) {
                return root;
            }

            //
            // (2) Group by columns contain the partition columns:
            //     This is the interesting case that we are going to support.
            //     At this point, subquery does not contain LIMIT/OFFSET.
            //     But if the aggregate has distinct, we have to compute on coordinator.
            if ( selectStmt.hasAggregateDistinct() ) {
                return root;
            }

            //     Now. If this sub-query joins with partition table on outer level,
            //     we are able to push the join down by removing the send/receive plan node pair.
        }

        //
        // Remove the send/receive pair on distributed node
        //
        root = removeCoordinatorSendReceivePair(root);

        m_hasReceiveNode = false;
        return root;
    }

    public boolean hasReceiveNode() {
        if (m_hasReceiveNode) {
            return true;
        }

        for (StmtTableScan tableScan : m_subqueryStmt.m_tableAliasMap.values()) {
            if (tableScan instanceof StmtSubqueryScan) {
                StmtSubqueryScan subScan = (StmtSubqueryScan)tableScan;
                if (subScan.hasReceiveNode()) {
                    return true;
                }
            }
        }

        return m_hasReceiveNode;
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

    /**
     * Remove the coordinator send/receive pair if any from the graph.
     *
     * @param root the complete plan node.
     * @return the plan without the send/receive pair.
     */
    static public AbstractPlanNode removeCoordinatorSendReceivePair(AbstractPlanNode root) {
        assert(root != null);
        return removeCoordinatorSendReceivePairRecursive(root, root);
    }

    static public AbstractPlanNode removeCoordinatorSendReceivePairRecursive(AbstractPlanNode root,
            AbstractPlanNode current) {
        if (current instanceof ReceivePlanNode) {
            assert(current.getChildCount() == 1);

            AbstractPlanNode child = current.getChild(0);
            assert(child instanceof SendPlanNode);

            assert(child.getChildCount() == 1);
            child = child.getChild(0);
            child.clearParents();
            if (current.getParentCount() == 0) {
                return child;
            } else {
                assert(current.getParentCount() == 1);
                AbstractPlanNode parent = current.getParent(0);
                parent.unlinkChild(current);
                parent.addAndLinkChild(child);
                return root;
            }
        } else if (current.getChildCount() == 1) {
            // This is still a coordinator node
            return removeCoordinatorSendReceivePairRecursive(root, current.getChild(0));
        } else {
            // We are about to branch and leave the coordinator
            return root;
        }
    }
}
