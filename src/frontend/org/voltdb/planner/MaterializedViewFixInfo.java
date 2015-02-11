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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.planner.parseinfo.BranchNode;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
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

    // Scan Node for join query.
    AbstractScanPlanNode m_scanNode = null;

    // ENG-5386: Edge case query.
    private boolean m_edgeCaseQueryNoFixNeeded = true;

    public boolean needed() {
        return m_needed;
    }

    public void setNeeded(boolean need) {
        m_needed = need;
    }

    public String getMVTableName () {
        assert(m_mvTableScan != null);
        return m_mvTableScan.getTableName();
    }

    public String getMVTableAlias() {
        assert(m_mvTableScan != null);
        return m_mvTableScan.getTableAlias();
    }

    public HashAggregatePlanNode getReAggregationPlanNode () {
        return m_reAggNode;
    }

    public void setEdgeCaseQueryNoFixNeeded (boolean edgeCase) {
        m_edgeCaseQueryNoFixNeeded = edgeCase;
    }

    /**
     * Check whether the table need to be fixed or not.
     * Set the need flag to true, only if it needs to be fixed.
     * @return
     */
    public boolean processMVBasedQueryFix(StmtTableScan mvTableScan, Set<SchemaColumn> scanColumns, JoinNode joinTree,
            List<ParsedColInfo> displayColumns, List<ParsedColInfo> groupByColumns) {

        // Check valid cases first
        //@TODO
        if  ( ! (mvTableScan instanceof StmtTargetTableScan)) {
            return false;
        }
        Table table = ((StmtTargetTableScan)mvTableScan).getTargetTable();
        assert (table != null);
        String mvTableName = table.getTypeName();
        Table srcTable = table.getMaterializer();
        if (srcTable == null) {
            return false;
        }
        Column partitionCol = srcTable.getPartitioncolumn();
        if (partitionCol == null) {
            return false;
        }

        int partitionColIndex = partitionCol.getIndex();
        MaterializedViewInfo mvInfo = srcTable.getViews().get(mvTableName);

        int numOfGroupByColumns;
        // Justify whether partition column is in group by column list or not
        String complexGroupbyJson = mvInfo.getGroupbyexpressionsjson();
        if (complexGroupbyJson.length() > 0) {
            List<AbstractExpression> mvComplexGroupbyCols = null;
            try {
                mvComplexGroupbyCols = AbstractExpression.fromJSONArrayString(complexGroupbyJson, null);
            } catch (JSONException e) {
                e.printStackTrace();
            }
            numOfGroupByColumns = mvComplexGroupbyCols.size();

            for (AbstractExpression expr: mvComplexGroupbyCols) {
                if (expr instanceof TupleValueExpression) {
                    TupleValueExpression tve = (TupleValueExpression) expr;
                    if (tve.getColumnIndex() == partitionColIndex) {
                        // If group by columns contain partition column from source table.
                        // Then, query on MV table will have duplicates from each partition.
                        // There is no need to fix this case, so just return.
                        return false;
                    }
                }
            }
        } else {
            CatalogMap<ColumnRef> mvSimpleGroupbyCols = mvInfo.getGroupbycols();
            numOfGroupByColumns = mvSimpleGroupbyCols.size();

            for (ColumnRef colRef: mvSimpleGroupbyCols) {
                if (colRef.getColumn().getIndex() == partitionColIndex) {
                    // If group by columns contain partition column from source table.
                    // Then, query on MV table will have duplicates from each partition.
                    // There is no need to fix this case, so just return.
                    return false;
                }
            }
        }
        assert(numOfGroupByColumns > 0);
        m_mvTableScan = mvTableScan;

        Set<String> mvDDLGroupbyColumnNames = new HashSet<String>();
        List<Column> mvColumnArray =
                CatalogUtil.getSortedCatalogItems(table.getColumns(), "index");

        // Start to do real materialized view processing to fix the duplicates problem.
        // (1) construct new projection columns for scan plan node.
        Set<SchemaColumn> mvDDLGroupbyColumns = new HashSet<SchemaColumn>();
        NodeSchema inlineProjSchema = new NodeSchema();
        for (SchemaColumn scol: scanColumns) {
            inlineProjSchema.addColumn(scol);
        }

        String mvTableAlias = getMVTableAlias();

        for (int i = 0; i < numOfGroupByColumns; i++) {
            Column mvCol = mvColumnArray.get(i);
            String colName = mvCol.getName();

            TupleValueExpression tve = new TupleValueExpression(mvTableName, mvTableAlias, colName, colName, i);
            tve.setTypeSizeBytes(mvCol.getType(), mvCol.getSize(), mvCol.getInbytes());

            mvDDLGroupbyColumnNames.add(colName);

            SchemaColumn scol = new SchemaColumn(mvTableName, mvTableAlias, colName, colName, tve);

            mvDDLGroupbyColumns.add(scol);
            if (!scanColumns.contains(scol)) {
                scanColumns.add(scol);
                // construct new projection columns for scan plan node.
                inlineProjSchema.addColumn(scol);
            }
        }


        // Record the re-aggregation type for each scan columns.
        Map<String, ExpressionType> mvColumnReAggType = new HashMap<String, ExpressionType>();
        for (int i = numOfGroupByColumns; i < mvColumnArray.size(); i++) {
            Column mvCol = mvColumnArray.get(i);
            ExpressionType reAggType = ExpressionType.get(mvCol.getAggregatetype());

            if (reAggType == ExpressionType.AGGREGATE_COUNT_STAR ||
                    reAggType == ExpressionType.AGGREGATE_COUNT) {
                reAggType = ExpressionType.AGGREGATE_SUM;
            }
            mvColumnReAggType.put(mvCol.getName(), reAggType);
        }

        m_scanInlinedProjectionNode = new ProjectionPlanNode();
        m_scanInlinedProjectionNode.setOutputSchema(inlineProjSchema);

        // (2) Construct the reAggregation Node.

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
                ExpressionType reAggType = mvColumnReAggType.get(scol.getColumnName());
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


        // Collect all TVEs that need to be do re-aggregation in coordinator.
        List<TupleValueExpression> needReAggTVEs = new ArrayList<TupleValueExpression>();
        List<AbstractExpression> aggPostExprs = new ArrayList<AbstractExpression>();

        for (int i=numOfGroupByColumns; i < mvColumnArray.size(); i++) {
            Column mvCol = mvColumnArray.get(i);
            String colName = mvCol.getName();

            TupleValueExpression tve = new TupleValueExpression(mvTableName, mvTableAlias, colName, colName);
            tve.setTypeSizeBytes(mvCol.getType(), mvCol.getSize(), mvCol.getInbytes());

            needReAggTVEs.add(tve);
        }

        collectReAggNodePostExpressions(joinTree, needReAggTVEs, aggPostExprs);

        AbstractExpression aggPostExpr = ExpressionUtil.combine(aggPostExprs);
        // Add post filters for the reAggregation node.
        m_reAggNode.setPostPredicate(aggPostExpr);


        // ENG-5386
        if (m_edgeCaseQueryNoFixNeeded &&
                edgeCaseQueryNoFixNeeded(mvDDLGroupbyColumnNames, mvColumnReAggType, displayColumns, groupByColumns)) {
            return false;
        }

        m_needed = true;
        return true;
    }

    // ENG-5386: do not fix some cases in order to get better performance.
    private boolean edgeCaseQueryNoFixNeeded(Set<String> mvDDLGroupbyColumnNames,
            Map<String, ExpressionType> mvColumnAggType, List<ParsedColInfo> displayColumns, List<ParsedColInfo> groupByColumns) {

        // Condition (1): Group by columns must be part of or all from MV DDL group by TVEs.
        for (ParsedColInfo gcol: groupByColumns) {
            assert(gcol.expression instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) gcol.expression;
            if (tve.getTableName().equals(getMVTableName()) && !mvDDLGroupbyColumnNames.contains(tve.getColumnName())) {
                return false;
            }
        }

        // Condition (2): Aggregation must be:
        for (ParsedColInfo dcol: displayColumns) {
            if (groupByColumns.contains(dcol)) {
                continue;
            }
            if (dcol.expression instanceof AggregateExpression == false) {
                return false;
            }
            AggregateExpression aggExpr = (AggregateExpression) dcol.expression;
            if (aggExpr.getLeft() instanceof TupleValueExpression == false) {
                return false;
            }
            ExpressionType type = aggExpr.getExpressionType();
            TupleValueExpression tve = (TupleValueExpression) aggExpr.getLeft();
            String columnName = tve.getColumnName();

            if (type != ExpressionType.AGGREGATE_SUM && type != ExpressionType.AGGREGATE_MIN
                    && type != ExpressionType.AGGREGATE_MAX) {
                return false;
            }

            if (tve.getTableName().equals(getMVTableName())) {
                if (mvColumnAggType.get(columnName) != type ) {
                    return false;
                }
            } else {
                // The other join table.
                if (type == ExpressionType.AGGREGATE_SUM) {
                    return false;
                }
            }
        }

        // Edge case query can be optimized with correct answer without MV reAggregation fix.
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

    private void collectReAggNodePostExpressions(JoinNode joinTree,
            List<TupleValueExpression> needReAggTVEs, List<AbstractExpression> aggPostExprs) {
        if (joinTree instanceof BranchNode) {
            collectReAggNodePostExpressions(((BranchNode)joinTree).getLeftNode(), needReAggTVEs, aggPostExprs);
            collectReAggNodePostExpressions(((BranchNode)joinTree).getRightNode(), needReAggTVEs, aggPostExprs);
            return;
        }
        joinTree.setJoinExpression(processFilters(joinTree.getJoinExpression(),
                                                  needReAggTVEs, aggPostExprs));
        // For outer join filters. Inner join or single table query will have whereExpr be null.
        joinTree.setWhereExpression(processFilters(joinTree.getWhereExpression(),
                                    needReAggTVEs, aggPostExprs));
    }


    private boolean fromMVTableOnly(List<AbstractExpression> tves) {
        String mvTableName = getMVTableName();
        for (AbstractExpression tve: tves) {
            assert(tve instanceof TupleValueExpression);
            String tveTableName = ((TupleValueExpression)tve).getTableName();
            if (!mvTableName.equals(tveTableName)) {
                return false;
            }
        }
        return true;
    }

    private AbstractExpression processFilters (AbstractExpression filters,
            List<TupleValueExpression> needReAggTVEs, List<AbstractExpression> aggPostExprs) {
        if (filters == null) {
            return null;
        }

        // Collect all TVEs that need to be do re-aggregation in coordinator.
        List<AbstractExpression> remaningExprs = new ArrayList<AbstractExpression>();
        // Check where clause.
        List<AbstractExpression> exprs = ExpressionUtil.uncombine(filters);

        for (AbstractExpression expr: exprs) {
            ArrayList<AbstractExpression> tves = expr.findBaseTVEs();

            boolean canPushdown = true;

            for (TupleValueExpression needReAggTVE: needReAggTVEs) {
                if (tves.contains(needReAggTVE)) {
                    m_edgeCaseQueryNoFixNeeded = false;

                    if (fromMVTableOnly(tves)) {
                        canPushdown = false;
                    }

                    break;
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
}
