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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.MaterializedViewHandlerInfo;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
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

/**
 * When a materialized view has a source table that is partitioned, and the source table's
 * partition key is not a group by key for the materialized view, we need to "re-aggregate"
 * the contents of the view in the coordinator fragment, in order to account for possible
 * duplicate keys coming from different sites.  This "re-aggregation" is done by injecting
 * an extra aggregation node above the receive node on the coordinator fragment.
 *
 * This class encapsulates the info required for adding in re-aggregation so that scans
 * of the materialized view get correct answers.
 */
public class MaterializedViewFixInfo {
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
     * Check whether the results from a materialized view need to be
     * re-aggregated on the coordinator by the view's GROUP BY columns
     * prior to any of the processing specified by the query.
     * This is normally the case when a mat view's source table is partitioned
     * and the view's GROUP BY does not include the partition key.
     * There is a special edge case where the query already contains the exact
     * reaggregations that the added-cost fix would introduce, so the fix can
     * be skipped as an optimization.
     * Set the m_needed flag to true, only if the reaggregation fix is needed.
     * @return The value of m_needed
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
        if (table.getIsreplicated()) {
            return false;
        }

        // Justify whether partition column is in group by column list or not
        if (table.getPartitioncolumn() != null) {
            return false;
        }

        m_mvTableScan = mvTableScan;

        Set<String> mvDDLGroupbyColumnNames = new HashSet<>();
        List<Column> mvColumnArray =
                CatalogUtil.getSortedCatalogItems(table.getColumns(), "index");

        String mvTableAlias = getMVTableAlias();

        // Get the number of group-by columns.
        int numOfGroupByColumns;
        MaterializedViewInfo mvInfo = srcTable.getViews().get(mvTableName);
        if (mvInfo != null) {
            // single table view
            String complexGroupbyJson = mvInfo.getGroupbyexpressionsjson();
            if (complexGroupbyJson.length() > 0) {
                List<AbstractExpression> mvComplexGroupbyCols = null;
                try {
                    mvComplexGroupbyCols = AbstractExpression.fromJSONArrayString(complexGroupbyJson, null);
                }
                catch (JSONException e) {
                    e.printStackTrace();
                }
                numOfGroupByColumns = mvComplexGroupbyCols.size();
            }
            else {
                numOfGroupByColumns = mvInfo.getGroupbycols().size();
            }
        }
        else {
            // joined table view
            MaterializedViewHandlerInfo mvHandlerInfo = table.getMvhandlerinfo().get("mvHandlerInfo");
            numOfGroupByColumns = mvHandlerInfo.getGroupbycolumncount();
        }

        if (scanColumns.isEmpty() && numOfGroupByColumns == 0) {
            // This is an edge case that can happen if the view
            // has no group by keys, and we are just
            // doing a count(*) on the output of the view.
            //
            // Having no GB keys or scan columns would cause us to
            // produce plan nodes that have a 0-column output schema.
            // We can't handle this in several places, so add the
            // count(*) column from the view to the scan columns.
            Column mvCol = mvColumnArray.get(0); // this is the "count(*)" column.
            TupleValueExpression tve = new TupleValueExpression(
                    mvTableName, mvTableAlias, mvCol, 0);
            tve.setOrigStmtId(mvTableScan.getStatementId());

            String colName = mvCol.getName();
            SchemaColumn scol = new SchemaColumn(mvTableName, mvTableAlias,
                    colName, colName, tve);
            scanColumns.add(scol);
        }

        // Start to do real materialized view processing to fix the duplicates problem.
        // (1) construct new projection columns for scan plan node.
        Set<SchemaColumn> mvDDLGroupbyColumns = new HashSet<>();
        NodeSchema inlineProjSchema = new NodeSchema();
        for (SchemaColumn scol: scanColumns) {
            inlineProjSchema.addColumn(scol);
        }


        for (int i = 0; i < numOfGroupByColumns; i++) {
            Column mvCol = mvColumnArray.get(i);
            String colName = mvCol.getName();

            TupleValueExpression tve = new TupleValueExpression(
                    mvTableName, mvTableAlias, mvCol, i);
            tve.setOrigStmtId(mvTableScan.getStatementId());

            mvDDLGroupbyColumnNames.add(colName);

            SchemaColumn scol = new SchemaColumn(mvTableName, mvTableAlias,
                    colName, colName, tve);
            mvDDLGroupbyColumns.add(scol);
            if (!scanColumns.contains(scol)) {
                scanColumns.add(scol);
                // construct new projection columns for scan plan node.
                inlineProjSchema.addColumn(scol);
            }
        }

        // Record the re-aggregation type for each scan columns.
        Map<String, ExpressionType> mvColumnReAggType = new HashMap<>();
        for (int i = numOfGroupByColumns; i < mvColumnArray.size(); i++) {
            Column mvCol = mvColumnArray.get(i);
            ExpressionType reAggType = ExpressionType.get(mvCol.getAggregatetype());

            if (reAggType == ExpressionType.AGGREGATE_COUNT_STAR ||
                    reAggType == ExpressionType.AGGREGATE_COUNT) {
                reAggType = ExpressionType.AGGREGATE_SUM;
            }
            mvColumnReAggType.put(mvCol.getName(), reAggType);
        }

        assert (inlineProjSchema.size() > 0);
        m_scanInlinedProjectionNode =
                new ProjectionPlanNode(inlineProjSchema);

        // (2) Construct the reAggregation Node.

        // Construct the reAggregation plan node's aggSchema
        m_reAggNode = new HashAggregatePlanNode();
        int outputColumnIndex = 0;
        // inlineProjSchema contains the group by columns, while aggSchema may do not.
        NodeSchema aggSchema = new NodeSchema();

        // Construct reAggregation node's aggregation and group by list.
        for (SchemaColumn scol: inlineProjSchema) {
            if (mvDDLGroupbyColumns.contains(scol)) {
                // Add group by expression.
                m_reAggNode.addGroupByExpression(scol.getExpression());
            }
            else {
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

        assert (aggSchema.size() > 0);
        m_reAggNode.setOutputSchema(aggSchema);


        // Collect all TVEs that need to be do re-aggregation in coordinator.
        List<TupleValueExpression> needReAggTVEs = new ArrayList<>();
        List<AbstractExpression> aggPostExprs = new ArrayList<>();

        for (int i = numOfGroupByColumns; i < mvColumnArray.size(); i++) {
            Column mvCol = mvColumnArray.get(i);

            TupleValueExpression tve = new TupleValueExpression(
                    mvTableName, mvTableAlias, mvCol, -1);
            tve.setOrigStmtId(mvTableScan.getStatementId());

            needReAggTVEs.add(tve);
        }

        collectReAggNodePostExpressions(joinTree, needReAggTVEs, aggPostExprs);

        AbstractExpression aggPostExpr = ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, aggPostExprs);
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

    /** ENG-5386: do not fix some cases in order to get better performance.
     * There is a special edge case when certain queries are applied to
     * partitioned materialized views that do not contain the partition key in
     * their GROUP BY columns. In this special case, where the query duplicates
     * the reaggregation behavior of the fix -- which must consist of MIN, MAX
     * and/or non-distinct SUM reaggregations -- the added-cost fix code can be
     * skipped as an optimization.
     */
    private boolean edgeCaseQueryNoFixNeeded(Set<String> mvDDLGroupbyColumnNames,
            Map<String, ExpressionType> mvColumnAggType,
            List<ParsedColInfo> displayColumns,
            List<ParsedColInfo> groupByColumns) {

        // Condition (1): Group by columns must be part of or all from MV DDL group by TVEs.
        for (ParsedColInfo gcol: groupByColumns) {
            assert(gcol.m_expression instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) gcol.m_expression;
            if (tve.getTableName().equals(getMVTableName()) &&
                    ! mvDDLGroupbyColumnNames.contains(tve.getColumnName())) {
                return false;
            }
        }

        // Condition (2): All the aggregations must qualify.
        for (ParsedColInfo dcol: displayColumns) {
            if (groupByColumns.contains(dcol)) {
                // Skip a group-by column pass-through.
                continue;
            }
            if (dcol.m_expression instanceof AggregateExpression == false) {
                return false;
            }
            AggregateExpression aggExpr = (AggregateExpression) dcol.m_expression;
            if (aggExpr.getLeft() instanceof TupleValueExpression == false) {
                return false;
            }
            ExpressionType type = aggExpr.getExpressionType();

            // Only MIN, MAX, and non-DISTINCT SUM
            // can tolerate a skipped reaggregation.
            if ((type != ExpressionType.AGGREGATE_SUM || aggExpr.isDistinct())
                    && type != ExpressionType.AGGREGATE_MIN
                    && type != ExpressionType.AGGREGATE_MAX) {
                return false;
            }

            TupleValueExpression tve = (TupleValueExpression) aggExpr.getLeft();
            if (tve.getTableName().equals(getMVTableName())) {
                String columnName = tve.getColumnName();
                // The type of the aggregation in the query must match the
                // type of aggregation defined for the view column --
                // SUMming a SUM, MINning a MIN, or MAXxing a MAX.
                if (mvColumnAggType.get(columnName) != type ) {
                    return false;
                }
            }
            else {
                // The aggregate argument is a column from the
                // other (non-view) side of the join.
                // It's OK for its rows to get duplicated by joining
                // with multiple "partial group" rows ONLY if it is
                // feeding a MIN or MAX.
                // The duplication would corrupt a SUM.
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
            }
            else {
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


    private boolean fromMVTableOnly(List<TupleValueExpression> tves) {
        String mvTableName = getMVTableName();
        for (TupleValueExpression tve: tves) {
            String tveTableName = tve.getTableName();
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

        // Collect all TVEs that need re-aggregation in the coordinator.
        List<AbstractExpression> remaningExprs = new ArrayList<>();
        // Check where clause.
        List<AbstractExpression> exprs = ExpressionUtil.uncombinePredicate(filters);

        for (AbstractExpression expr: exprs) {
            List<TupleValueExpression> tves =
                    expr.findAllTupleValueSubexpressions();

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
            }
            else {
                aggPostExprs.add(expr);
            }
        }
        AbstractExpression remaningFilters = ExpressionUtil.combinePredicates(ExpressionType.CONJUNCTION_AND, remaningExprs);
        // Update new filters for the scanNode.
        return remaningFilters;
    }
}
