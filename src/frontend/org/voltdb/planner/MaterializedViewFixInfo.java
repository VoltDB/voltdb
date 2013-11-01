/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.HashAggregatePlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;
import org.voltdb.utils.CatalogUtil;

public class MaterializedViewFixInfo {
    /**
     * This class contain all the information that Materialized view partitioned query need to be fixed.
     */

    // New inlined projection node for the scan node, contain extra group by columns.
    private ProjectionPlanNode m_scanInlinedProjectionNode = null;
    // New re-Aggregation plan node on the coordinator to eliminate the duplicated rows.
    private HashAggregatePlanNode m_reAggNode = null;

    // Does this mv partitioned based query needs to be fixed.
    private boolean m_needed = false;
    // materialized view table
    private StmtTableScan m_mvTableScan = null;

    // number of group-by s.
    private int m_numOfGroupByColumns;

    // Scan Node for join query.
    AbstractScanPlanNode m_scanNode = null;

    public boolean needed() {
        return m_needed;
    }

    public void setNeeded(boolean need) {
        m_needed = need;
    }

    public String getMVTableName() {
        assert(m_mvTableScan != null);
        return m_mvTableScan.m_table.getTypeName();
    }

    public String getMVTableAlias() {
        assert(m_mvTableScan != null);
        return m_mvTableScan.m_tableAlias;
    }

    public HashAggregatePlanNode getReAggregationPlanNode() {
        return m_reAggNode;
    }

    /**
     * Check whether the table need to be fixed or not.
     * Set the need flag to true, only if it needs to be fixed.
     * @return
     */
    public boolean checkFixNeeded(StmtTableScan mvTableScan) {
        // Check valid cases first
        Table table = mvTableScan.m_table;
        String mvTableName = table.getTypeName();
        Table srcTable = table.getMaterializer();
        if (srcTable == null) {
            return false;
        }
        Column partitionCol = srcTable.getPartitioncolumn();
        if (partitionCol == null) {
            return false;
        }

        String partitionColName = partitionCol.getName();
        MaterializedViewInfo mvInfo = srcTable.getViews().get(mvTableName);

        // Justify whether partition column is in group by column list or not

        String complexGroupbyJson = mvInfo.getGroupbyexpressionsjson();
        if (complexGroupbyJson.length() > 0) {
            List<AbstractExpression> mvComplexGroupbyCols = null;
            try {
                mvComplexGroupbyCols = AbstractExpression.fromJSONArrayString(complexGroupbyJson);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            m_numOfGroupByColumns = mvComplexGroupbyCols.size();

            for (AbstractExpression expr: mvComplexGroupbyCols) {
                if (expr instanceof TupleValueExpression) {
                    TupleValueExpression tve = (TupleValueExpression) expr;
                    if (tve.getColumnName().equals(partitionColName)) {
                        // If group by columns contain partition column from source table.
                        // Then, query on MV table will have duplicates from each partition.
                        // There is no need to fix this case, so just return.
                        return false;
                    }
                }
            }
        } else {
            CatalogMap<ColumnRef> mvSimpleGroupbyCols = mvInfo.getGroupbycols();
            m_numOfGroupByColumns = mvSimpleGroupbyCols.size();

            for (ColumnRef colRef: mvSimpleGroupbyCols) {
                if (colRef.getColumn().getName().equals(partitionColName)) {
                    // If group by columns contain partition column from source table.
                    // Then, query on MV table will have duplicates from each partition.
                    // There is no need to fix this case, so just return.
                    return false;
                }
            }
        }
        assert(m_numOfGroupByColumns > 0);

        m_mvTableScan = mvTableScan;
        m_needed = true;
        return true;
    }

    /**
     * Find the scan node on MV table, replace it with reAggNode for join query.
     * This scan node can not be in-lined, so it should be as a child of a join node.
     * @param node
     */
    public boolean processScanNodeWithReAggNode(AbstractPlanNode node, AbstractPlanNode reAggNode) {
        // MV table scan node can not be in in-lined nodes.
        for (int i = 0; i < node.getChildCount(); i++) {
            AbstractPlanNode child = node.getChild(i);

            if (child instanceof AbstractScanPlanNode) {
                AbstractScanPlanNode scanNode = (AbstractScanPlanNode) child;
                if (!scanNode.getTargetTableName().equals(getMVTableName())) {
                    continue;
                }
                if (reAggNode != null) {
                    // Join query case.
                    node.setAndLinkChild(i, reAggNode);
                }
                // Process scan node.
                // Set up the scan plan node's scan columns. Add in-line projection node for scan node.
                scanNode.addInlinePlanNode(m_scanInlinedProjectionNode);
                m_scanNode = scanNode;
                return true;
            } else {
                boolean replaced = processScanNodeWithReAggNode(child, reAggNode);
                if (replaced) {
                    return true;
                }
            }
        }
        return false;
    }

    private void processInlineProjectionsAndReAggNode(Set<SchemaColumn> scanColumns, List<Column> mvColumnArray) {
        assert(m_needed);
        String mvTableName = getMVTableName();
        String mvTableAlias = getMVTableAlias();

        // (1) construct new projection columns for scan plan node.
        Set<SchemaColumn> mvDDLGroupbyColumns = new HashSet<SchemaColumn>();
        NodeSchema inlineProjSchema = new NodeSchema();
        for (SchemaColumn scol: scanColumns) {
            inlineProjSchema.addColumn(scol);
        }

        for (int i = 0; i < m_numOfGroupByColumns; i++) {
            Column mvCol = mvColumnArray.get(i);
            String colName = mvCol.getName();

            TupleValueExpression tve = new TupleValueExpression(mvTableName, mvTableAlias, colName, colName, i);
            tve.setValueType(VoltType.get((byte)mvCol.getType()));
            tve.setValueSize(mvCol.getSize());

            SchemaColumn scol = new SchemaColumn(mvTableName, mvTableAlias, colName, colName, tve);

            mvDDLGroupbyColumns.add(scol);
            if (!scanColumns.contains(scol)) {
                scanColumns.add(scol);
                // construct new projection columns for scan plan node.
                inlineProjSchema.addColumn(scol);
            }
        }
        m_scanInlinedProjectionNode = new ProjectionPlanNode();
        m_scanInlinedProjectionNode.setOutputSchema(inlineProjSchema);

        // (2) Construct the reAggregation Node.
        // Record the re-aggregation type for each scan columns.
        Map<String, ExpressionType> mvColumnAggType = new HashMap<String, ExpressionType>();
        for (int i = m_numOfGroupByColumns; i < mvColumnArray.size(); i++) {
            Column mvCol = mvColumnArray.get(i);
            ExpressionType reAggType = ExpressionType.get(mvCol.getAggregatetype());
            if (reAggType == ExpressionType.AGGREGATE_COUNT_STAR ||
                    reAggType == ExpressionType.AGGREGATE_COUNT) {
                reAggType = ExpressionType.AGGREGATE_SUM;
            }
            mvColumnAggType.put(mvCol.getName(), reAggType);
        }

        // Construct the reAggregation plan node's aggSchema
        m_reAggNode = new HashAggregatePlanNode();
        int outputColumnIndex = 0;
        // inlineProjSchema contains the group by columns, while aggSchema may do not.
        NodeSchema aggSchema = new NodeSchema();

        // Construct reAggregation node's aggregation and group by list.
        for (SchemaColumn scol: scanColumns) {
            if (mvDDLGroupbyColumns.contains(scol)) {
                // Add group by expression.
                m_reAggNode.addGroupByExpression(scol.getExpression());
            } else {
                ExpressionType reAggType = mvColumnAggType.get(scol.getColumnName());
                assert(reAggType != null);
                AbstractExpression agg_input_expr = scol.getExpression();
                assert(agg_input_expr instanceof TupleValueExpression);
                // Add aggregation information.
                m_reAggNode.addAggregate(reAggType, false, outputColumnIndex, agg_input_expr);
            }
            aggSchema.addColumn(scol);
            outputColumnIndex++;
        }
        m_reAggNode.setOutputSchema(aggSchema);

    }

    private boolean fromMVTableOnly(List<AbstractExpression> tves) {
        String mvTableName = m_mvTableScan.m_table.getTypeName();
        for (AbstractExpression tve: tves) {
            assert(tve instanceof TupleValueExpression);
            String tveTableName = ((TupleValueExpression)tve).getTableName();
            if (!mvTableName.equals(tveTableName)) {
                return false;
            }
        }
        return true;
    }

    private void collectReAggNodePostExpressions(JoinNode joinTree,
            List<TupleValueExpression> needReAggTVEs, List<AbstractExpression> aggPostExprs) {
        if (joinTree.m_leftNode != null) {
            assert (joinTree.m_rightNode != null);
            collectReAggNodePostExpressions(joinTree.m_leftNode, needReAggTVEs, aggPostExprs);
            collectReAggNodePostExpressions(joinTree.m_rightNode, needReAggTVEs, aggPostExprs);
        }
        else {
            assert(joinTree.m_tableAliasIndex != -1);
            joinTree.m_joinExpr = processFilters(joinTree.m_joinExpr, needReAggTVEs, aggPostExprs);

            // For outer join filters. Inner join or single table query will have whereExpr be null.
            joinTree.m_whereExpr = processFilters(joinTree.m_whereExpr, needReAggTVEs, aggPostExprs);
        }
    }

    private AbstractExpression processFilters (AbstractExpression filters,
            List<TupleValueExpression> needReAggTVEs, List<AbstractExpression> aggPostExprs) {
        if (filters != null) {
            // Collect all TVEs that need to be do re-aggregation in coordinator.
            List<AbstractExpression> remaningExprs = new ArrayList<AbstractExpression>();
            // Check where clause.
            List<AbstractExpression> exprs = ExpressionUtil.uncombine(filters);

            for (AbstractExpression expr: exprs) {
                ArrayList<AbstractExpression> tves = expr.findBaseTVEs();

                boolean canPushdown = true;
                // If the expression is built on a join expression referencing two tables,
                // There is no need to handle it.
                if (fromMVTableOnly(tves)) {
                    for (TupleValueExpression needReAggTVE: needReAggTVEs) {
                        if (tves.contains(needReAggTVE)) {
                            canPushdown = false;
                            break;
                        }
                    }
                }
                if (canPushdown) {
                    remaningExprs.add(expr);
                } else {
                    aggPostExprs.add(expr);
                }
            }
            AbstractExpression remaningFilters = ExpressionUtil.combine(remaningExprs);
            // Update new filters for the scanNode.
            return remaningFilters;
        }
        return null;
    }

    /**
     * Start to do real materialized view processing to fix the duplicates problem.
     * @param scanColumns
     * @param joinTree
     */
    public void processMVBasedQueryFix(Set<SchemaColumn> scanColumns, JoinNode joinTree) {
        List<Column> mvColumnArray =
                CatalogUtil.getSortedCatalogItems(m_mvTableScan.m_table.getColumns(), "index");

        processInlineProjectionsAndReAggNode(scanColumns, mvColumnArray);

        // Collect all TVEs that need to be do re-aggregation in coordinator.
        List<TupleValueExpression> needReAggTVEs = new ArrayList<TupleValueExpression>();
        List<AbstractExpression> aggPostExprs = new ArrayList<AbstractExpression>();

        for (int i=m_numOfGroupByColumns; i < mvColumnArray.size(); i++) {
            Column mvCol = mvColumnArray.get(i);
            String colName = mvCol.getName();
            TupleValueExpression tve = new TupleValueExpression(getMVTableName(), getMVTableAlias(), colName, colName);
            tve.setValueType(VoltType.get((byte)mvCol.getType()));
            tve.setValueSize(mvCol.getSize());

            needReAggTVEs.add(tve);
        }

        collectReAggNodePostExpressions(joinTree, needReAggTVEs, aggPostExprs);

        AbstractExpression aggPostExpr = ExpressionUtil.combine(aggPostExprs);
        // Add post filters for the reAggregation node.
        m_reAggNode.setPostPredicate(aggPostExpr);
    }
}
