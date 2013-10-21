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
    private Table m_mvTable = null;

    // number of group-by s.
    private int m_numOfGroupByColumns;

    private boolean m_isJoin = false;

    public boolean needed () {
        return m_needed;
    }

    public void setNeeded (boolean need) {
        m_needed = need;
    }

    public boolean isJoin () {
        return m_isJoin;
    }

    public void setIsJoin (boolean join) {
        m_isJoin = join;
    }

    public String getMVTableName () {
        assert(m_mvTable != null);
        return m_mvTable.getTypeName();
    }

    public ProjectionPlanNode getScanInlinedProjectionNode () {
        return m_scanInlinedProjectionNode;
    }

    public HashAggregatePlanNode getReAggregationPlanNode () {
        return m_reAggNode;
    }

    /**
     * Check whether the table need to be fixed or not.
     * Set the need flag to true, only if it needs to be fixed.
     * @return
     */
    public boolean checkNeedFix(Table table) {
        // Check valid cases first

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

        m_mvTable = table;
        m_needed = true;
        return true;
    }


    /**
     * Grasp the scan node on MV table from the join node.
     * This scan node can not be in-lined, so it should be as a child of a join node.
     * @param rootNode
     */
    public AbstractScanPlanNode replaceAndReturnScanNode(AbstractPlanNode rootNode, AbstractPlanNode reAggNode) {
        // MV table scan node can not be in in-lined nodes.
        for (int i = 0; i < rootNode.getChildCount(); i++) {
            AbstractPlanNode child = rootNode.getChild(i);
            AbstractPlanNode scanNodeFromChild = replaceAndReturnScanNode(child, reAggNode);
            if (scanNodeFromChild != null) {
                rootNode.setAndLinkChild(i, reAggNode);
                return (AbstractScanPlanNode) scanNodeFromChild;
            }
        }

        if (rootNode instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode scanNode = (AbstractScanPlanNode) rootNode;
            if (scanNode.getTargetTableName().equals(getMVTableName())) {
                assert(scanNode.getParentCount() == 1);
                AbstractPlanNode parent = scanNode.getParent(0);

                // Find the i-th child index, and replace it.
                for (int i = 0; i < parent.getChildCount(); i++) {
                    AbstractPlanNode child = parent.getChild(i);
                    if (child == scanNode) {
                        parent.setAndLinkChild(i, scanNode);
                        return scanNode;
                    }
                }
            }
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
                CatalogUtil.getSortedCatalogItems(m_mvTable.getColumns(), "index");

        processInlineProjectionsAndReAggNode(scanColumns, mvColumnArray);
        processWhereClause(joinTree, mvColumnArray);

    }

    private void processInlineProjectionsAndReAggNode(Set<SchemaColumn> scanColumns, List<Column> mvColumnArray) {
        assert(m_needed);
        String mvTableName = m_mvTable.getTypeName();

        // (1) construct new projection columns for scan plan node.
        Set<SchemaColumn> mvDDLGroupbyColumns = new HashSet<SchemaColumn>();
        NodeSchema inlineProjSchema = new NodeSchema();
        for (SchemaColumn scol: scanColumns) {
            inlineProjSchema.addColumn(scol);
        }

        for (int i = 0; i < m_numOfGroupByColumns; i++) {
            Column mvCol = mvColumnArray.get(i);
            String colName = mvCol.getName();

            TupleValueExpression tve = new TupleValueExpression();
            tve.setColumnIndex(i);
            tve.setColumnName(colName);
            tve.setTableName(mvTableName);
            tve.setColumnAlias(colName);
            tve.setValueType(VoltType.get((byte)mvCol.getType()));
            tve.setValueSize(mvCol.getSize());

            SchemaColumn scol = new SchemaColumn(mvTableName, colName, colName, tve);

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

    private void processWhereClause(JoinNode joinTree, List<Column> mvColumnArray) {
        // (1) Process where clause.
        AbstractExpression where = analyzeJoinTreeFilters(joinTree);

        if (where != null) {
            // Collect all TVEs that need to be do re-aggregation in coordinator.
            List<TupleValueExpression> needReAggTVEs = new ArrayList<TupleValueExpression>();
            for (int i=m_numOfGroupByColumns; i < mvColumnArray.size(); i++) {
                Column mvCol = mvColumnArray.get(i);
                TupleValueExpression tve = new TupleValueExpression();
                tve.setColumnIndex(i);
                tve.setColumnName(mvCol.getName());
                tve.setTableName(getMVTableName());
                tve.setColumnAlias(mvCol.getName());
                tve.setValueType(VoltType.get((byte)mvCol.getType()));
                tve.setValueSize(mvCol.getSize());

                needReAggTVEs.add(tve);
            }

            List<AbstractExpression> pushdownExprs = new ArrayList<AbstractExpression>();
            List<AbstractExpression> aggPostExprs = new ArrayList<AbstractExpression>();
            // Check where clause.
            List<AbstractExpression> exprs = ExpressionUtil.uncombine(where);

            for (AbstractExpression expr: exprs) {
                ArrayList<AbstractExpression> tves = expr.findBaseTVEs();
                boolean pushdown = true;
                for (TupleValueExpression needReAggTVE: needReAggTVEs) {
                    if (tves.contains(needReAggTVE)) {
                        pushdown = false;
                        break;
                    }
                }
                if (pushdown) {
                    pushdownExprs.add(expr);
                } else {
                    aggPostExprs.add(expr);
                }
            }
            AbstractExpression aggPostExpr = ExpressionUtil.combine(aggPostExprs);
            // Add post filters for the reAggregation node.
            m_reAggNode.setPostPredicate(aggPostExpr);

            AbstractExpression scanFilters = ExpressionUtil.combine(pushdownExprs);
            // Update new filters for the scanNode.
            boolean updated = updateJoinFilters(joinTree, scanFilters);
            assert(updated);
        }
    }

    private AbstractExpression analyzeJoinTreeFilters(JoinNode joinTree) {
        assert(joinTree != null);
        AbstractExpression where = null;
        if (joinTree.m_table == null) {
            // Non-join case.
            assert(joinTree.m_whereExpr == null);
            // Follow HSQL's logic to store the where expression in joinExpr for single table.
            where = joinTree.m_joinExpr;

        } else {
            // Join case.
            where = findJoinFilters(joinTree);
        }

        return where;
    }

    private AbstractExpression findJoinFilters(JoinNode joinTree) {
        AbstractExpression result = null;
        if (joinTree.m_leftNode != null) {
            result = findJoinFilters(joinTree.m_leftNode);
            if (result != null) {
                return result;
            }
        }
        if (joinTree.m_rightNode != null) {
            result = findJoinFilters(joinTree.m_rightNode);
            if (result != null) {
                return result;
            }
        }

        if (joinTree.m_table != null && joinTree.m_table.getTypeName().equals(getMVTableName())) {
            return joinTree.m_joinExpr;
        }

        return null;
    }


    private boolean updateJoinFilters(JoinNode joinTree, AbstractExpression scanFilters) {
        boolean update = false;
        if (joinTree.m_leftNode != null) {
            update = updateJoinFilters(joinTree.m_leftNode, scanFilters);
            if (update) {
                return update;
            }
        }
        if (joinTree.m_rightNode != null) {
            update = updateJoinFilters(joinTree.m_rightNode, scanFilters);
            if (update) {
                return update;
            }
        }

        if (joinTree.m_table.getTypeName().equals(getMVTableName())) {
            joinTree.m_joinExpr = scanFilters;
            update = true;
        }

        return update;
    }

}
