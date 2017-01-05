/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.StatementCompiler;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.RowSubqueryExpression;
import org.voltdb.expressions.ScalarValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.expressions.WindowFunctionExpression;
import org.voltdb.planner.parseinfo.BranchNode;
import org.voltdb.planner.parseinfo.JoinNode;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.LimitPlanNode;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.JoinType;

public class ParsedSelectStmt extends AbstractParsedStmt {

    public ArrayList<ParsedColInfo> m_displayColumns = new ArrayList<>();
    private ArrayList<ParsedColInfo> m_orderColumns = new ArrayList<>();
    private AbstractExpression m_having = null;
    public ArrayList<ParsedColInfo> m_groupByColumns = new ArrayList<>();
    public ArrayList<ParsedColInfo> m_distinctGroupByColumns = null;
    private boolean m_groupAndOrderByPermutationWasTested = false;
    private boolean m_groupAndOrderByPermutationResult = false;

    // It will store the final projection node schema for this plan if it is needed.
    // Calculate once, and use it everywhere else.
    private NodeSchema m_projectSchema = null;
    private NodeSchema m_distinctProjectSchema = null;

    // It may has the consistent element order as the displayColumns
    // This list contains the core information for aggregation.
    // It collects aggregate expression, group by expression from display columns,
    // group by columns, having, order by columns.
    public ArrayList<ParsedColInfo> m_aggResultColumns = new ArrayList<>();
    // It represents the group by expression and replace TVE if it's complex group by.
    public Map<String, AbstractExpression> m_groupByExpressions = null;

    private ArrayList<ParsedColInfo> m_avgPushdownDisplayColumns = null;
    private ArrayList<ParsedColInfo> m_avgPushdownAggResultColumns = null;
    private ArrayList<ParsedColInfo> m_avgPushdownGroupByColumns = null;
    private ArrayList<ParsedColInfo> m_avgPushdownDistinctGroupByColumns = null;
    private ArrayList<ParsedColInfo> m_avgPushdownOrderColumns = null;
    private AbstractExpression m_avgPushdownHaving = null;
    private NodeSchema m_avgPushdownProjectSchema;
    private NodeSchema m_avgPushdownFinalProjectSchema;

    private boolean m_hasPartitionColumnInGroupby = false;
    private boolean m_hasAggregateDistinct = false;
    private boolean m_hasPartitionColumnInDistinctGroupby = false;
    private boolean m_isComplexOrderBy = false;

    // Limit plan node information.
    public static class LimitOffset {
        private LimitPlanNode m_limitNodeTop = null;
        private LimitPlanNode m_limitNodeDist = null;
        private boolean m_limitCanPushdown = false;
        private long m_limit = -1;
        private long m_offset = 0;
        private long m_limitParameterId = -1;
        private long m_offsetParameterId = -1;

        private boolean hasLimit() {
            return m_limit != -1 || m_limitParameterId != -1;
        }

        public boolean hasOffset() {
            return m_offset > 0 || m_offsetParameterId != -1;
        }

        public boolean hasLimitOrOffset() {
            if (hasLimit() || hasOffset()) {
                return true;
            }
            return false;
        }

        public boolean hasLimitOrOffsetParameters() {
            return m_limitParameterId != -1 || m_offsetParameterId != -1;
        }

        public LimitPlanNode getLimitNodeTop() {
            return m_limitNodeTop;
        }

        public void setLimit(long number) {
            m_limit = number;
        }

        public long getLimit() {
            return m_limit;
        }

        public long getOffset() {
            return m_offset;
        }

        public long getLimitParameterId () {
            return m_limitParameterId;
        }
    }

    LimitOffset m_limitOffset = new LimitOffset();

    private boolean m_distinct = false;
    private boolean m_hasComplexAgg = false;
    private boolean m_hasComplexGroupby = false;
    private boolean m_hasAggregateExpression = false;
    private boolean m_hasAverage = false;

    public MaterializedViewFixInfo m_mvFixInfo = new MaterializedViewFixInfo();

    private boolean m_hasLargeNumberOfTableJoins = false;
    // this list is the join order either from the user or parser if it has large number of table joins.
    private final ArrayList<JoinNode> m_joinOrderList = new ArrayList<>();
    private boolean m_hasPartitionColumnsInWindowedAggregates = false;
    // If this statement is the query for a materialized view, and
    // some expression has operations which are unsafe to create
    // with non-empty source tables, then this string will have
    // the error message.  If this is null, the view can be created
    // with populated tables with no problem.
    private String m_mvUnSafeErrorMessage = null;

    /**
     * Class constructor
     * @param paramValues
     * @param db
     */
    public ParsedSelectStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        String node;
        if ((node = stmtNode.attributes.get("distinct")) != null) {
            m_distinct = Boolean.parseBoolean(node);
        }

        VoltXMLElement limitElement = null;
        VoltXMLElement offsetElement = null;
        VoltXMLElement havingElement = null;
        VoltXMLElement displayElement = null;
        VoltXMLElement orderbyElement = null;
        VoltXMLElement groupbyElement = null;
        for (VoltXMLElement child : stmtNode.children) {
            if (child.name.equalsIgnoreCase("limit")) {
                assert(limitElement == null);
                limitElement = child;
            }
            else if (child.name.equalsIgnoreCase("offset")) {
                assert(offsetElement == null);
                offsetElement = child;
            }
            else if (child.name.equalsIgnoreCase("columns")) {
                assert(displayElement == null);
                displayElement = child;
            }
            else if (child.name.equalsIgnoreCase("ordercolumns")) {
                assert(orderbyElement == null);
                orderbyElement = child;
            }
            else if (child.name.equalsIgnoreCase("groupcolumns")) {
                assert(groupbyElement == null);
                groupbyElement = child;
            }
            else if (child.name.equalsIgnoreCase("having")) {
                assert(havingElement == null);
                havingElement = child;
            }
        }
        parseLimitAndOffset(limitElement, offsetElement, m_limitOffset);

        if (m_aggregationList == null) {
            m_aggregationList = new ArrayList<>();
        }
        // We want to extract display first, groupBy second before processing
        // orderBy because groupBy and orderBy need display columns to tag
        // their columns and orderBy needs groupBy columns to stop recursively
        // finding TVEs for pass-through columns.
        assert(displayElement != null);
        parseDisplayColumns(displayElement, false);

        // rewrite DISTINCT
        // function may need to change the groupbyElement by rewriting.
        groupbyElement = processDistinct(displayElement,
                groupbyElement, havingElement);

        if (groupbyElement != null) {
            parseGroupByColumns(groupbyElement);
            insertToColumnList(m_aggResultColumns, m_groupByColumns);
        }

        if (havingElement != null) {
            parseHavingExpression(havingElement, false);
        }

        if (orderbyElement != null && ! hasAOneRowResult()) {
            parseOrderColumns(orderbyElement, false);
        }

        // At this point, we have collected all aggregations in the select
        // statement. We do not need aggregationList container in parseXMLtree
        // Make it null to prevent others adding elements to it when parsing
        // the tree.
        m_aggregationList = null;

        if (needComplexAggregation()) {
            fillUpAggResultColumns();
        }
        else {
            m_aggResultColumns = m_displayColumns;
        }

        // If we have seen any windowed expressions we will have
        // saved them in m_windowFunctionExpressions by now.  We need to
        // verify their validity.
        verifyWindowFunctionExpressions();

        /*
         * Calculate the content determinism message before we place the TVEs in
         * the columns. After the TVEs are placed the actual expressions are
         * hard to find.
         */
        calculateContentDeterminismMessage();
        placeTVEsinColumns();

        // prepare the limit plan node if it needs one.
        if (hasLimitOrOffset()) {
            m_limitOffset.m_limitCanPushdown = canPushdownLimit();
            prepareLimitPlanNode(this, m_limitOffset);
        }

        // Prepare for the AVG push-down optimization only if it might be required.
        if (mayNeedAvgPushdown()) {
            processAvgPushdownOptimization(displayElement,
                    groupbyElement, havingElement, orderbyElement);
        }
        prepareMVBasedQueryFix();
    }

    private void processAvgPushdownOptimization (VoltXMLElement displayElement,
            VoltXMLElement groupbyElement, VoltXMLElement havingElement,
            VoltXMLElement orderbyElement) {

        ArrayList<ParsedColInfo> tmpDisplayColumns = m_displayColumns;
        m_displayColumns = new ArrayList<>();
        ArrayList<ParsedColInfo> tmpAggResultColumns = m_aggResultColumns;
        m_aggResultColumns = new ArrayList<>();
        ArrayList<ParsedColInfo> tmpGroupByColumns = m_groupByColumns;
        m_groupByColumns = new ArrayList<>();
        ArrayList<ParsedColInfo> tmpDistinctGroupByColumns = m_distinctGroupByColumns;
        m_distinctGroupByColumns = new ArrayList<>();
        ArrayList<ParsedColInfo> tmpOrderColumns = m_orderColumns;
        m_orderColumns = new ArrayList<>();
        AbstractExpression tmpHaving = m_having;

        boolean tmpHasComplexAgg = hasComplexAgg();
        NodeSchema tmpProjectSchema = m_projectSchema;
        NodeSchema tmpDistinctProjectSchema = m_distinctProjectSchema;

        m_aggregationList = new ArrayList<>();
        assert(displayElement != null);
        parseDisplayColumns(displayElement, true);

        // rewrite DISTINCT
        // function may need to change the groupbyElement by rewriting.
        groupbyElement = processDistinct(displayElement,
                groupbyElement, havingElement);

        if (groupbyElement != null) {
            parseGroupByColumns(groupbyElement);
            insertToColumnList(m_aggResultColumns, m_groupByColumns);
        }

        if (havingElement != null) {
            parseHavingExpression(havingElement, true);
        }

        if (orderbyElement != null && ! hasAOneRowResult()) {
            parseOrderColumns(orderbyElement, true);
        }

        m_aggregationList = null;
        fillUpAggResultColumns();
        placeTVEsinColumns();

        // Switch them back
        m_avgPushdownDisplayColumns = m_displayColumns;
        m_avgPushdownAggResultColumns = m_aggResultColumns;
        m_avgPushdownGroupByColumns = m_groupByColumns;
        m_avgPushdownDistinctGroupByColumns = m_distinctGroupByColumns;
        m_avgPushdownOrderColumns = m_orderColumns;
        m_avgPushdownProjectSchema = m_projectSchema;
        m_avgPushdownFinalProjectSchema = m_distinctProjectSchema;
        m_avgPushdownHaving = m_having;

        m_displayColumns = tmpDisplayColumns;
        m_aggResultColumns = tmpAggResultColumns;
        m_groupByColumns = tmpGroupByColumns;
        m_distinctGroupByColumns = tmpDistinctGroupByColumns;
        m_orderColumns = tmpOrderColumns;
        m_projectSchema = tmpProjectSchema;
        m_distinctProjectSchema = tmpDistinctProjectSchema;
        m_hasComplexAgg = tmpHasComplexAgg;
        m_having = tmpHaving;
    }

    /**
     * Switch the optimal set for pushing down AVG
     */
    public void switchOptimalSuiteForAvgPushdown () {
        m_displayColumns = m_avgPushdownDisplayColumns;
        m_aggResultColumns = m_avgPushdownAggResultColumns;
        m_groupByColumns = m_avgPushdownGroupByColumns;
        m_distinctGroupByColumns = m_avgPushdownDistinctGroupByColumns;
        m_orderColumns = m_avgPushdownOrderColumns;
        m_projectSchema = m_avgPushdownProjectSchema;
        m_distinctProjectSchema = m_avgPushdownFinalProjectSchema;
        m_hasComplexAgg = true;
        m_having = m_avgPushdownHaving;
    }

    /**
     * Prepare for the mv based distributed query fix only if it might be required.
     */
    private void prepareMVBasedQueryFix() {

        // ENG-5386: Edge cases query returning correct answers with
        // aggregation push down does not need reAggregation work.
        if (m_hasComplexGroupby) {
            m_mvFixInfo.setEdgeCaseQueryNoFixNeeded(false);
        }

        // Handle joined query case case.
        // MV partitioned table without partition column can only join with
        // replicated tables. For all tables in this query, the # of tables
        // that need to be fixed should not exceed one.
        for (StmtTableScan mvTableScan: allScans()) {
            Set<SchemaColumn> mvNewScanColumns = new HashSet<>();

            Collection<SchemaColumn> columns = mvTableScan.getScanColumns();
            // For a COUNT(*)-only scan, a table may have no scan columns.
            // For a joined query without processed columns from table TB,
            // TB has no scan columns
            if (columns != null) {
                mvNewScanColumns.addAll(columns);
            }
            // ENG-5669: HAVING aggregation and order by aggregation
            // also need to be checked.
            if (m_mvFixInfo.processMVBasedQueryFix(mvTableScan,
                    mvNewScanColumns, m_joinTree,
                    m_aggResultColumns, groupByColumns())) {
                break;
            }
        }
    }

    private boolean needComplexAggregation () {
        if (!m_hasAggregateExpression && !isGrouped()) {
            m_hasComplexAgg = false;
            return false;
        }

        if (hasComplexAgg()) {
            return true;
        }

        int numDisplayCols = m_displayColumns.size();
        if (m_aggResultColumns.size() > numDisplayCols) {
            m_hasComplexAgg = true;
            return true;
        }

        // Disqualify the case of duplicate columns in the SELECT list
        HashSet<ParsedColInfo> tmpContainer = new HashSet<>();

        for (ParsedColInfo col : m_displayColumns) {
            if ( ! tmpContainer.add(col)) {
                // duplicate display column.
                m_hasComplexAgg = true;
                return true;
            }

            AbstractExpression expr = col.expression;
            boolean isNewAgg = true;
            for (ParsedColInfo existingAggCol : m_aggResultColumns) {
                AbstractExpression existingExpr = existingAggCol.expression;

                if (expr.equals(existingExpr)) {
                    isNewAgg = false;
                    break;
                }
            }

            if ( ! isNewAgg) {
                continue;
            }

            // Now Only TVEs in displayColumns are left for AggResultColumns
            if ( ! (col.expression instanceof TupleValueExpression)) {
                // Col has complex expression (like: TVE + 1, TVE + AGG)
                m_hasComplexAgg = true;
                return true;
            }

            m_aggResultColumns.add(col);
        }

        // The size of the aggResultColumns list should be the same as
        // numDisplayCols as it would be a substitute of DisplayCols.
        if (m_aggResultColumns.size() != numDisplayCols) {
            // Display columns have several pass-through columns if group by
            // primary key or a HAVING clause using aggs that are
            // not in the SELECT list (larger than case).
            m_hasComplexAgg = true;
            return true;
        }

        return false;
    }

    /**
     * Continue adding TVEs from DisplayColumns that are left in
     * function needComplexAggregation().
     * After this function, aggResultColumns construction work.
     */
    private void fillUpAggResultColumns () {
        for (ParsedColInfo col : m_displayColumns) {
            if (m_aggResultColumns.contains(col)) {
                continue;
            }

            if (col.expression instanceof TupleValueExpression) {
                m_aggResultColumns.add(col);
            }
            else {
                // Col must be complex expression (like: TVE + 1, TVE + AGG)
                List<TupleValueExpression> tveList = new ArrayList<>();
                findAllTVEs(col.expression, tveList);
                insertTVEsToAggResultColumns(tveList);
            }
        }
    }

    /**
     * Generate new output Schema and Place TVEs for display columns if needed.
     * Place TVEs for order by columns always.
     */
    private void placeTVEsinColumns () {
        // Build the association between the table column with its index
        Map<AbstractExpression, Integer> aggTableIndexMap = new HashMap<>();
        Map<Integer, ParsedColInfo> indexToColumnMap = new HashMap<>();
        int index = 0;
        for (ParsedColInfo col : m_aggResultColumns) {
            aggTableIndexMap.put(col.expression, index);
            if (col.alias == null) {
                // hack any unique string
                col.alias = "$$_" +
                        col.expression.getExpressionType().symbol() +
                        "_$$_" +
                        index;
            }
            indexToColumnMap.put(index, col);
            index++;
        }

        // Replace TVE for group by columns
        m_groupByExpressions = new HashMap<>();
        for (ParsedColInfo groupbyCol : m_groupByColumns) {
            AbstractExpression expr = groupbyCol.expression;
            assert(aggTableIndexMap.get(expr) != null);

            expr = expr.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
            m_groupByExpressions.put(groupbyCol.alias, expr);
        }

        if (m_having != null) {
            m_having = m_having.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
            ExpressionUtil.finalizeValueTypes(m_having);
        }

        // Replace TVE for display columns
        m_projectSchema = new NodeSchema();
        for (ParsedColInfo col : m_displayColumns) {
            AbstractExpression expr = col.expression;
            if (hasComplexAgg()) {
                expr = expr.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
            }
            m_projectSchema.addColumn(col.tableName, col.tableAlias,
                    col.columnName, col.alias,
                    expr, col.differentiator);
        }

        // DISTINCT group by expressions are already TVEs when set

        placeTVEsForOrderby(aggTableIndexMap, indexToColumnMap);
    }

    /**
     * Replace TVE for order by columns.
     * Restrictions: Order by clause must operate on display column list,
     * except for columns that are
     * (1) GROUP BY columns list without DISTINCT,
     * (2) in tables or joined tables without GROUP BY or DISTINCT clause.
     *
     * Display columns means exact columns/expressions
     * in the select list or tag alias.
     * Order by clause can be columns or expressions on the columns.
     */
    private void placeTVEsForOrderby(
            Map<AbstractExpression, Integer> aggTableIndexMap,
            Map<Integer, ParsedColInfo> indexToColumnMap) {
        // Detect the edge order by case
        detectComplexOrderby();

        if (isComplexOrderBy()) {
            // Case that ORDER BY is below Projection node
            boolean replaceAll = hasComplexAgg() || hasComplexGroupby();
            for (ParsedColInfo orderCol : m_orderColumns) {
                AbstractExpression expr = orderCol.expression;
                if (replaceAll ||
                        expr.hasAnySubexpressionOfClass(AggregateExpression.class)) {
                    orderCol.expression = expr.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
                }
            }
        }
        else if (hasAggregateOrGroupby()) {
            // Case that ORDER BY is above Projection node

            Map<AbstractExpression, Integer> displayIndexMap = new HashMap<>();
            Map<Integer, ParsedColInfo> displayIndexToColumnMap = new HashMap<>();

            int orderByIndex = 0;
            for (ParsedColInfo col : m_displayColumns) {
                displayIndexMap.put(col.expression, orderByIndex);
                assert(col.alias != null);
                displayIndexToColumnMap.put(orderByIndex, col);
                orderByIndex++;
            }

            // place the TVEs from Display columns in the ORDER BY expression
            for (ParsedColInfo orderCol : m_orderColumns) {
                AbstractExpression expr = orderCol.expression;
                expr = expr.replaceWithTVE(displayIndexMap, displayIndexToColumnMap);
                orderCol.expression = expr;
            }
        }
        // other cases like (2) from the function comments.
    }

    /**
     * ParseDisplayColumns and ParseOrderColumns will call this function
     * to add Aggregation expressions to aggResultColumns
     * @param aggColumns
     * @param cookedCol
     */
    private void insertAggExpressionsToAggResultColumns (
            List<AbstractExpression> aggColumns, ParsedColInfo cookedCol) {
        for (AbstractExpression expr: aggColumns) {
            assert(expr instanceof AggregateExpression);
            if (expr.hasSubquerySubexpression()) {
                throw new PlanningErrorException(
                        "SQL Aggregate with subquery expression is not allowed.");
            }

            ParsedColInfo col = new ParsedColInfo();
            col.expression = expr.clone();
            assert(col.expression instanceof AggregateExpression);
            if (col.expression.getExpressionType() == ExpressionType.AGGREGATE_AVG) {
                m_hasAverage = true;
            }
            if (aggColumns.size() == 1 &&
                    cookedCol.expression.equals(aggColumns.get(0))) {
                col.alias = cookedCol.alias;
                col.tableName = cookedCol.tableName;
                col.tableAlias = cookedCol.tableAlias;
                col.columnName = cookedCol.columnName;
                if (!m_aggResultColumns.contains(col)) {
                    m_aggResultColumns.add(col);
                }
                return;
            }
            // Try to check complexAggs earlier
            m_hasComplexAgg = true;
            // Aggregation column use the the hacky stuff
            col.tableName = TEMP_TABLE_NAME;
            col.tableAlias = TEMP_TABLE_NAME;
            col.columnName = "";
            if (!m_aggResultColumns.contains(col)) {
                m_aggResultColumns.add(col);
            }
            ExpressionUtil.finalizeValueTypes(col.expression);
        }
    }

    private void insertTVEsToAggResultColumns (
            List<TupleValueExpression> colCollection) {
        // TVEs do not need to take care
        for (TupleValueExpression tve: colCollection) {
            ParsedColInfo col = new ParsedColInfo();
            col.alias = tve.getColumnAlias();
            col.columnName = tve.getColumnName();
            col.tableName = tve.getTableName();
            col.tableAlias = tve.getTableAlias();
            col.expression = tve;
            if (!m_aggResultColumns.contains(col)) {
                m_aggResultColumns.add(col);
            }
        }
    }

    // Concat elements to the XXXColumns list
    private static void insertToColumnList(
            List<ParsedColInfo> columnList, List<ParsedColInfo> newCols) {
        for (ParsedColInfo col : newCols) {
            if (!columnList.contains(col)) {
                columnList.add(col);
            }
        }
    }

    private static boolean isNewtoColumnList(
            List<ParsedColInfo> columnList, AbstractExpression expr) {
        for (ParsedColInfo ic : columnList) {
            if (ic.expression.equals(expr)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Find all TVEs except inside of AggregationExpression
     * @param expr
     * @param tveList
     */
    private void findAllTVEs(
            AbstractExpression expr, List<TupleValueExpression> tveList) {
        if (!isNewtoColumnList(m_aggResultColumns, expr)) {
            return;
        }

        if (expr instanceof TupleValueExpression) {
            tveList.add((TupleValueExpression) expr.clone());
            return;
        }

        if ( expr.getLeft() != null) {
            findAllTVEs(expr.getLeft(), tveList);
        }

        if (expr.getRight() != null) {
            findAllTVEs(expr.getRight(), tveList);
        }

        if (expr.getArgs() != null) {
            for (AbstractExpression ae: expr.getArgs()) {
                findAllTVEs(ae, tveList);
            }
        }
    }

    private void updateAvgExpressions () {
        List<AbstractExpression> optimalAvgAggs = new ArrayList<>();
        Iterator<AbstractExpression> itr = m_aggregationList.iterator();
        while (itr.hasNext()) {
            AbstractExpression aggExpr = itr.next();
            assert(aggExpr instanceof AggregateExpression);
            if (aggExpr.getExpressionType() == ExpressionType.AGGREGATE_AVG) {
                itr.remove();

                AbstractExpression left =
                        new AggregateExpression(ExpressionType.AGGREGATE_SUM);
                left.setLeft(aggExpr.getLeft().clone());
                AbstractExpression right =
                        new AggregateExpression(ExpressionType.AGGREGATE_COUNT);
                right.setLeft(aggExpr.getLeft().clone());

                optimalAvgAggs.add(left);
                optimalAvgAggs.add(right);
            }
        }
        m_aggregationList.addAll(optimalAvgAggs);
    }

    static void parseLimitAndOffset(
            VoltXMLElement limitNode,
            VoltXMLElement offsetNode,
            LimitOffset limitOffset) {
        String node;
        if (limitNode != null) {
            // Parse limit
            if ((node = limitNode.attributes.get("limit_paramid")) != null)
                limitOffset.m_limitParameterId = Long.parseLong(node);
            else {
                assert(limitNode.children.size() == 1);
                VoltXMLElement valueNode = limitNode.children.get(0);
                String isParam = valueNode.attributes.get("isparam");
                if ((isParam != null) && (isParam.equalsIgnoreCase("true"))) {
                    limitOffset.m_limitParameterId =
                            Long.parseLong(valueNode.attributes.get("id"));
                }
                else {
                    node = limitNode.attributes.get("limit");
                    assert(node != null);
                    limitOffset.m_limit = Long.parseLong(node);
                }
            }
        }
        if (offsetNode != null) {
            // Parse offset
            if ((node = offsetNode.attributes.get("offset_paramid")) != null)
                limitOffset.m_offsetParameterId = Long.parseLong(node);
            else {
                if (offsetNode.children.size() == 1) {
                    VoltXMLElement valueNode = offsetNode.children.get(0);
                    String isParam = valueNode.attributes.get("isparam");
                    if ((isParam != null) && (isParam.equalsIgnoreCase("true"))) {
                        limitOffset.m_offsetParameterId =
                                Long.parseLong(valueNode.attributes.get("id"));
                    }
                    else {
                        node = offsetNode.attributes.get("offset");
                        assert(node != null);
                        limitOffset.m_offset = Long.parseLong(node);
                    }
                }
            }
        }

        // limit and offset can't have both value and parameter
        if (limitOffset.m_limit != -1) {
            assert limitOffset.m_limitParameterId == -1 : "Parsed value and param. limit.";
        }
        if (limitOffset.m_offset != 0) {
            assert limitOffset.m_offsetParameterId == -1 : "Parsed value and param. offset.";
        }
    }

    private void parseDisplayColumns(VoltXMLElement columnsNode,
            boolean isDistributed) {
        try {
            m_parsingInDisplayColumns = true;
            int index = 0;
            for (VoltXMLElement child : columnsNode.children) {
                parseDisplayColumn(index, child, isDistributed);
                ++index;
            }
        }
        finally {
            m_parsingInDisplayColumns = false;
        }
    }

    private void setParsedColumnNamesAndAliases(ParsedColInfo col, VoltXMLElement columnRefElmt) {
        if (col.expression instanceof TupleValueExpression) {
            // Set the column data from the TVE itself rather than from the VoltXML.
            // For example, the original select
            // SELECT TA.CA FROM (SELECT C CA FROM T) TA;
            // could be simplified to be as simple as
            // SELECT TA.C CA FROM T TA;
            // The TVE will reflect this change while the VoltXML won't
            TupleValueExpression tvexpr = (TupleValueExpression) col.expression;
            col.columnName = tvexpr.getColumnName();
            col.tableName = tvexpr.getTableName();
            col.tableAlias = tvexpr.getTableAlias();
            col.alias = tvexpr.getColumnAlias();
            return;
        }

        col.columnName = columnRefElmt.attributes.get("column");
        col.tableName = columnRefElmt.attributes.get("table");
        col.tableAlias = columnRefElmt.attributes.get("tablealias");
        col.alias = columnRefElmt.attributes.get("alias");
    }

    private void parseDisplayColumn(int index, VoltXMLElement child,
            boolean isDistributed) {
        ParsedColInfo col = new ParsedColInfo();
        m_aggregationList.clear();
        // This index calculation is only used for sanity checking
        // materialized views (which use the parsed select statement but
        // don't go through the planner pass that does more involved
        // column index resolution).
        col.index = index;

        // Parse the expression.  We may substitute for this later
        // on, but it's a place to start.
        AbstractExpression colExpr = parseExpressionTree(child);
        if (colExpr instanceof ConstantValueExpression) {
            assert(colExpr.getValueType() != VoltType.NUMERIC);
        }
        assert(colExpr != null);

        if (isDistributed) {
            colExpr = colExpr.replaceAVG();
            updateAvgExpressions();
        }
        ExpressionUtil.finalizeValueTypes(colExpr);

        if (colExpr.getValueType() == VoltType.BOOLEAN) {
            throw new PlanningErrorException(
                    "A SELECT clause does not allow a BOOLEAN expression. " +
                    "consider using CASE WHEN to decode the BOOLEAN expression " +
                    "into a value of some other type.");
        }

        // ENG-6291: If parent is UNION, voltdb wants to make inline varchar to be outlined
        if (isParentUnionClause() && AbstractExpression.hasInlineVarType(colExpr)) {
            AbstractExpression expr = new OperatorExpression();;
            expr.setExpressionType(ExpressionType.OPERATOR_CAST);

            VoltType voltType = colExpr.getValueType();
            // We don't support parameterized casting,
            // such as specifically to "VARCHAR(3)" vs. VARCHAR,
            // so assume max length for variable-length types
            // (VARCHAR and VARBINARY).
            int size = expr.getInBytes() ?
                    voltType.getMaxLengthInBytes() :
                    VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS;
            expr.setValueType(voltType);
            expr.setValueSize(size);
            expr.setInBytes(colExpr.getInBytes());

            expr.setLeft(colExpr);

            // switch the new expression for CAST
            colExpr = expr;
        }

        // Remember the column expression.
        col.expression = colExpr;

        calculateColumnNames(child, col);

        insertAggExpressionsToAggResultColumns(m_aggregationList, col);
        if (m_aggregationList.size() >= 1) {
            m_hasAggregateExpression = true;

            for (AbstractExpression agg: m_aggregationList) {
                assert(agg instanceof AggregateExpression);
                if (! m_hasAggregateDistinct &&
                        ((AggregateExpression)agg).isDistinct() ) {
                    m_hasAggregateDistinct = true;
                    break;
                }
            }
        }

        // The differentiator is used when ParsedColInfo is converted to a
        // SchemaColumn object, to differentiate between columns that have the
        // same name within a table (which can happen for subqueries or joins).
        col.differentiator = index;
        m_displayColumns.add(col);
    }

    private void calculateColumnNames(VoltXMLElement child, ParsedColInfo col) {
        // Calculate the names.
        if (child.name.equals("columnref")) {
            setParsedColumnNamesAndAliases(col, child);
        }
        else if (child.name.equals("tablesubquery")) {
            // Scalar subquery like 'select c, (select count(*) from t1) from t2;'
            ScalarValueExpression sve = (ScalarValueExpression)col.expression;
            col.tableName = sve.getSubqueryScan().getTableName();
            col.tableAlias = sve.getSubqueryScan().getTableAlias();
            col.columnName = child.attributes.get("alias");
            col.alias = child.attributes.get("alias");
        }
        else {
            // XXX hacky, label all non-column non-subquery refs
            // as coming from a temp table -- and hope it doesn't matter.
            col.tableName = TEMP_TABLE_NAME;
            col.tableAlias = TEMP_TABLE_NAME;
            col.columnName = child.attributes.get("alias");
            col.alias = child.attributes.get("alias");
        }
        // Default aliases to names if they are not set
        if (col.tableAlias == null) {
            col.tableAlias = col.tableName;
        }
        if (col.alias == null) {
            col.alias = col.columnName;
        }
    }

    /**
     * Verify the validity of the windowed expressions.
     *
     * @return
     */
    private void verifyWindowFunctionExpressions() {
        // Check for windowed expressions.
        if (m_windowFunctionExpressions.size() > 0) {
            if (m_windowFunctionExpressions.size() > 1) {
                throw new PlanningErrorException(
                        "Only one windowed function call may appear in a selection list.");
            }

            //
            // This could be an if statement, but I think it's better to
            // leave this as a pattern in case we decide to implement more
            // legality conditions for other windowed operators.
            //
            WindowFunctionExpression windowFunctionExpression = m_windowFunctionExpressions.get(0);
            List<AbstractExpression> orderByExpressions =
                    windowFunctionExpression.getOrderByExpressions();
            ExpressionType exprType = windowFunctionExpression.getExpressionType();
            String aggName = exprType.symbol().toUpperCase();
            switch (exprType) {
            case AGGREGATE_WINDOWED_RANK:
            case AGGREGATE_WINDOWED_DENSE_RANK:
                if (orderByExpressions.size() == 0) {
                    throw new PlanningErrorException(
                            "Windowed " + aggName +
                            " function call expressions require an ORDER BY specification.");
                }

                VoltType valType = orderByExpressions.get(0).getValueType();
                assert(valType != null);
                if (!valType.isAnyIntegerType() && (valType != VoltType.TIMESTAMP)) {
                    throw new PlanningErrorException(
                            "Windowed function call expressions can have only integer or TIMESTAMP value types in the ORDER BY expression of their window.");
                }
                break;
            case AGGREGATE_WINDOWED_COUNT:
                if (windowFunctionExpression.getAggregateArguments().size() > 1) {
                    throw new PlanningErrorException(
                            String.format("Windowed COUNT must have either exactly one argument or else a star for an argument"));
                }
                // Any type is ok, so we won't inspect the type.
                break;
            case AGGREGATE_WINDOWED_MAX:
            case AGGREGATE_WINDOWED_MIN:
                if (windowFunctionExpression.getAggregateArguments().size() != 1) {
                    throw new PlanningErrorException(
                            String.format("Windowed %s must have exactly one argument", aggName));
                }
                // Any type is ok, so we won't inspect the type.
                break;
            case AGGREGATE_WINDOWED_SUM:
                if (windowFunctionExpression.getAggregateArguments().size() != 1) {
                    throw new PlanningErrorException(
                            String.format("Windowed SUM must have exactly one numeric argument"));
                }
                AbstractExpression arg = windowFunctionExpression.getAggregateArguments().get(0);
                VoltType vt = arg.getValueType();
                assert(vt != null);
                if (! vt.isNumber()) {
                    throw new PlanningErrorException(
                                "Windowed SUM must have exactly one numeric argument");
                }
                break;
            default:
                {
                    String opName = (exprType == null) ? "NULL" : exprType.symbol();
                    throw new PlanningErrorException("Unknown windowed aggregate function type: " +
                            opName);
                }
            }
        }
    }

    private void parseGroupByColumns(VoltXMLElement columnsNode) {
        if (hasWindowFunctionExpression()) {
            throw new PlanningErrorException(
                    "Use of both a windowed function call and GROUP BY in a single query is not supported.");
        }
        for (VoltXMLElement child : columnsNode.children) {
            parseGroupByColumn(child);
        }
    }

    private void parseGroupByColumn(VoltXMLElement groupByNode) {
        ParsedColInfo groupbyCol = new ParsedColInfo();
        groupbyCol.expression = parseExpressionTree(groupByNode);
        assert(groupbyCol.expression != null);
        ExpressionUtil.finalizeValueTypes(groupbyCol.expression);
        groupbyCol.groupBy = true;

        if (groupbyCol.expression.getValueType() == VoltType.BOOLEAN) {
            throw new PlanningErrorException(
                    "A GROUP BY clause does not allow a BOOLEAN expression.");
        }
        if (groupByNode.name.equals("columnref")) {
            setParsedColumnNamesAndAliases(groupbyCol, groupByNode);
            if (groupbyCol.tableAlias == null) {
                groupbyCol.tableAlias = groupbyCol.tableName;
            }

            // This col.index set up is only useful for Materialized view.
            Table tb = getTableFromDB(groupbyCol.tableName);
            if (tb != null) {
                org.voltdb.catalog.Column catalogColumn =
                        tb.getColumns().getExact(groupbyCol.columnName);
                groupbyCol.index = catalogColumn.getIndex();
            }
        }
        else {
            // XXX hacky, assume all non-column refs come from a temp table
            groupbyCol.tableName = TEMP_TABLE_NAME;
            groupbyCol.tableAlias = TEMP_TABLE_NAME;
            groupbyCol.columnName = "";
            m_hasComplexGroupby = true;
        }

        // find the matching columns in selection list
        for (ParsedColInfo col : m_displayColumns) {
            if (! col.expression.equals(groupbyCol.expression)) {
                continue;
            }
            groupbyCol.alias = col.alias;
            groupbyCol.groupByInDisplay = true;
            col.groupBy = true;
            break;
        }

        m_groupByColumns.add(groupbyCol);
    }

    private void parseOrderColumns(
            VoltXMLElement columnsNode, boolean isDistributed) {
        for (VoltXMLElement child : columnsNode.children) {
            parseOrderColumn(child, isDistributed);
        }
    }

    private void parseOrderColumn(
            VoltXMLElement orderByNode, final boolean isDistributed) {
        // Aggregation list needs to be cleared before parsing the order by expression
        m_aggregationList.clear();

        ParsedColInfo.ExpressionAdjuster adjuster =
                new ParsedColInfo.ExpressionAdjuster() {
            @Override
            public AbstractExpression adjust(AbstractExpression expr) {
                if (isDistributed) {
                    expr = expr.replaceAVG();
                    updateAvgExpressions();
                }

                ExpressionUtil.finalizeValueTypes(expr);
                return expr;
            }
        };

        ParsedColInfo order_col =
                ParsedColInfo.fromOrderByXml(this, orderByNode, adjuster);

        AbstractExpression order_exp = order_col.expression;
        assert(order_exp != null);

        // guards against subquery inside of order by clause
        if (order_exp.hasSubquerySubexpression()) {
            throw new PlanningErrorException(
                    "ORDER BY clause with subquery expression is not allowed.");
        }

        // Mark the order by column if it is in displayColumns. The ORDER BY
        // column MAY be identical to a simple display column, in which case,
        // tagging the actual display column as being also an order by column
        // helps later when trying to determine ORDER BY coverage
        // (for determinism).
        for (ParsedColInfo col : m_displayColumns) {
            if (col.alias.equals(order_col.alias) ||
                    col.expression.equals(order_exp)) {
                col.orderBy = true;
                col.ascending = order_col.ascending;

                order_col.alias = col.alias;
                order_col.columnName = col.columnName;
                order_col.tableName = col.tableName;
                break;
            }
        }
        assert( ! (order_exp instanceof ConstantValueExpression));
        assert( ! (order_exp instanceof ParameterValueExpression));

        insertAggExpressionsToAggResultColumns(m_aggregationList, order_col);
        if (m_aggregationList.size() >= 1) {
            m_hasAggregateExpression = true;
        }
        // Add TVEs in ORDER BY statement if we have,
        // stop recursive finding when we have it in AggResultColumns
        List<TupleValueExpression> tveList = new ArrayList<>();
        findAllTVEs(order_col.expression, tveList);
        insertTVEsToAggResultColumns(tveList);
        m_orderColumns.add(order_col);
    }

    private void parseHavingExpression(VoltXMLElement havingNode, boolean isDistributed) {
        m_aggregationList.clear();
        assert(havingNode.children.size() == 1);
        m_having = parseConditionTree(havingNode.children.get(0));
        assert(m_having != null);
        if (m_having.hasSubquerySubexpression()) {
            m_aggregationList.clear();
            throw new PlanningErrorException(
                    "SQL HAVING with subquery expression is not allowed.");
        }
        if (isDistributed) {
            m_having = m_having.replaceAVG();
            updateAvgExpressions();
        }
        ExpressionUtil.finalizeValueTypes(m_having);
        m_having = ExpressionUtil.evaluateExpression(m_having);
        // If the condition is a trivial CVE(TRUE)
        // (after the evaluation) simply drop it
        if (ConstantValueExpression.isBooleanTrue(m_having)) {
            m_aggregationList.clear();
            m_having = null;
        }

        if (m_aggregationList.isEmpty()) {
            return;
        }

        m_hasAggregateExpression = true;

        for (AbstractExpression expr: m_aggregationList) {
            assert(expr instanceof AggregateExpression);
            if (expr.getExpressionType() == ExpressionType.AGGREGATE_AVG) {
                m_hasAverage = true;
            }

            boolean isNewAgg = true;
            for (ParsedColInfo existingAggCol : m_aggResultColumns) {
                AbstractExpression existingExpr = existingAggCol.expression;

                if (expr.equals(existingExpr)) {
                    isNewAgg = false;
                    break;
                }
            }

            if (isNewAgg) {
                ExpressionUtil.finalizeValueTypes(expr);

                ParsedColInfo col = new ParsedColInfo();
                col.expression = expr.clone();
                col.tableName = TEMP_TABLE_NAME;
                col.tableAlias = TEMP_TABLE_NAME;
                col.columnName = "";

                m_aggResultColumns.add(col);
                m_hasComplexAgg = true;
            }
        }
    }

    private VoltXMLElement processDistinct(VoltXMLElement displayElement,
            VoltXMLElement groupbyElement, VoltXMLElement havingElement) {
        // process DISTINCT clause
        if ( ! m_distinct) {
            return groupbyElement;
        }

        // DISTINCT without GROUP BY
        if (groupbyElement == null || groupbyElement.children.isEmpty()) {
            // Tricky: rewrote DISTINCT without GROUP BY with GROUP BY clause
            if ( ! m_hasAggregateExpression) {
                // attribute "id" is the only one that differs from a real
                // GROUP BY query
                groupbyElement = displayElement.duplicate();
            }
            // When it is table aggregate, it's also safe to drop DISTINCT.
            m_distinct = false;
            return groupbyElement;
        }
        // DISTINCT with GROUP BY
        m_distinctGroupByColumns = new ArrayList<>();
        m_distinctProjectSchema = new NodeSchema();

        // Iterate the Display columns
        for (ParsedColInfo col : m_displayColumns) {
            TupleValueExpression tve = new TupleValueExpression(
                    col.tableName, col.tableAlias,
                    col.columnName, col.alias,
                    col.index);

            ParsedColInfo pcol = new ParsedColInfo();
            pcol.tableName = col.tableName;
            pcol.tableAlias = col.tableAlias;
            pcol.columnName = col.columnName;
            pcol.alias = col.alias;
            pcol.expression = tve;
            m_distinctGroupByColumns.add(pcol);

            m_distinctProjectSchema.addColumn(col.tableName, col.tableAlias,
                    col.columnName, col.alias,
                    tve, col.differentiator);
        }

        return groupbyElement;
    }

    /**
     * Check if the LimitPlanNode can be pushed down.
     * The LimitPlanNode may have a LIMIT clause only,
     * OFFSET clause only, or both.
     * Offset only cannot be pushed down.
     * @return
     */
    private boolean canPushdownLimit() {
        boolean limitCanPushdown = (m_limitOffset.hasLimit() && !m_distinct);
        if (limitCanPushdown) {
            for (ParsedColInfo col : m_displayColumns) {
                AbstractExpression rootExpr = col.expression;
                if (rootExpr instanceof AggregateExpression) {
                    if (((AggregateExpression)rootExpr).isDistinct()) {
                        limitCanPushdown = false;
                        break;
                    }
                }
            }
        }
        return limitCanPushdown;
    }

    public static void prepareLimitPlanNode(
            AbstractParsedStmt stmt, LimitOffset limitOffset) {
        int limitParamIndex = stmt.parameterCountIndexById(limitOffset.m_limitParameterId);
        int offsetParamIndex = stmt.parameterCountIndexById(limitOffset.m_offsetParameterId);

        // The coordinator's top limit graph fragment for a MP plan.
        // If planning "order by ... limit", getNextSelectPlan()
        // will have already added an order by to the coordinator frag.
        // This is the only limit node in a SP plan
        limitOffset.m_limitNodeTop = new LimitPlanNode();
        limitOffset.m_limitNodeTop.setLimit((int) limitOffset.m_limit);
        limitOffset.m_limitNodeTop.setOffset((int) limitOffset.m_offset);
        limitOffset.m_limitNodeTop.setLimitParameterIndex(limitParamIndex);
        limitOffset.m_limitNodeTop.setOffsetParameterIndex(offsetParamIndex);

        if (limitOffset.m_limitCanPushdown) {
            limitOffset.m_limitNodeDist = new LimitPlanNode();
            // Offset on a pushed-down limit node makes no sense,
            // just defaults to 0
            // -- the original offset must be factored into the pushed-down
            // limit as a pad on the limit.
            if (limitOffset.m_limit != -1) {
                limitOffset.m_limitNodeDist.setLimit(
                        (int) (limitOffset.m_limit + limitOffset.m_offset));
            }

            if (limitOffset.hasLimitOrOffsetParameters()) {
                AbstractExpression left =
                        stmt.getParameterOrConstantAsExpression(
                                limitOffset.m_offsetParameterId,
                                limitOffset.m_offset);
                assert (left != null);
                AbstractExpression right =
                        stmt.getParameterOrConstantAsExpression(
                                limitOffset.m_limitParameterId,
                                limitOffset.m_limit);
                assert (right != null);
                OperatorExpression expr =
                        new OperatorExpression(ExpressionType.OPERATOR_PLUS,
                                left, right);
                expr.setValueType(VoltType.INTEGER);
                expr.setValueSize(VoltType.INTEGER.getLengthInBytesForFixedTypes());
                limitOffset.m_limitNodeDist.setLimitExpression(expr);
            }
            // else let the parameterized forms of offset/limit default to unused/invalid.
        }
    }

    public LimitPlanNode getLimitNodeTop() {
        return new LimitPlanNode(m_limitOffset.m_limitNodeTop);
    }

    public LimitPlanNode getLimitNodeDist() {
        return new LimitPlanNode(m_limitOffset.m_limitNodeDist);
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval += "LIMIT " + String.valueOf(m_limitOffset.m_limit) + "\n";
        retval += "OFFSET " + String.valueOf(m_limitOffset.m_offset) + "\n";

        retval += "DISPLAY COLUMNS:\n";
        for (ParsedColInfo col : m_displayColumns) {
            retval += "\tColumn: " + col.alias + ": ";
            retval += col.expression.toString() + "\n";
        }

        retval += "ORDER COLUMNS:\n";
        for (ParsedColInfo col : m_orderColumns) {
            retval += "\tColumn: " + col.alias + ": ASC?: " + col.ascending + ": ";
            retval += col.expression.toString() + "\n";
        }

        retval += "GROUP_BY COLUMNS:\n";
        for (ParsedColInfo col : m_groupByColumns) {
            retval += "\tColumn: " + col.alias + ": ";
            retval += col.expression.toString() + "\n";
        }

        retval = retval.trim();

        return retval;
    }


    /**
     * Converts an IN expression into the equivalent EXISTS one
     * IN (SELECT" forms e.g. "(A, B) IN (SELECT X, Y, FROM ...) ==
     * EXISTS (SELECT 42 FROM ... AND|WHERE|HAVING A=X AND|WHERE|HAVING B=Y)
     *
     * @param selectStmt select subquery from the IN expression
     * @param inListExpr TVE for the columns from the IN list
     * @return modified subquery
     */
    protected static void rewriteInSubqueryAsExists(
            ParsedSelectStmt selectStmt, AbstractExpression inListExpr) {
        List<AbstractExpression> whereList = new ArrayList<>();
        List<AbstractExpression> havingList = new ArrayList<>();

        // multi-column IN expression is a RowSubqueryExpression
        // where each arg represents an individual column
        List<AbstractExpression> inExprList = null;
        if (inListExpr instanceof RowSubqueryExpression) {
            inExprList = inListExpr.getArgs();
        }
        else {
            inExprList = new ArrayList<>();
            inExprList.add(inListExpr);
        }
        int idx = 0;
        assert(inExprList.size() == selectStmt.m_displayColumns.size());
        selectStmt.m_aggregationList = new ArrayList<>();

        // Iterate over the columns from the IN list
        // and the subquery output schema
        // For each pair create a new equality expression.
        // If the output column is part of the aggregate expression, the
        // new expression must be added to the subquery's HAVING expressions.
        // If not, it should be added to the WHERE expressions
        for (AbstractExpression expr : inExprList) {
            ParsedColInfo colInfo = selectStmt.m_displayColumns.get(idx++);
            assert(colInfo.expression != null);
            // The TVE and the aggregated expressions from the IN clause will
            // be parameters to the child select statement once the IN
            // expression is replaced with the EXISTS one.
            expr = selectStmt.replaceExpressionsWithPve(expr);
            // Finalize the expression. The subquery's own expressions
            // are already finalized but not the expressions from the IN list
            ExpressionUtil.finalizeValueTypes(expr);

            // Create new compare equal expression
            AbstractExpression equalityExpr =
                    new ComparisonExpression(ExpressionType.COMPARE_EQUAL,
                            expr, colInfo.expression.clone());
            // Check if this column contains aggregate expression
            if (ExpressionUtil.containsAggregateExpression(colInfo.expression)) {
                // We are not creating any new aggregate expressions so
                // the aggregation list doen't need to be updated.
                // Only the HAVING expression itself
                havingList.add(equalityExpr);
            }
            else {
                whereList.add(equalityExpr);
            }
        }
        // Add new WHERE expressions
        if ( ! whereList.isEmpty()) {
            if (selectStmt.m_joinTree.getWhereExpression() != null) {
                whereList.add(selectStmt.m_joinTree.getWhereExpression());
            }
            selectStmt.m_joinTree.setWhereExpression(
                    ExpressionUtil.combinePredicates(whereList));
        }
        // Add new HAVING expressions
        if (!havingList.isEmpty()) {
            if (selectStmt.m_having != null) {
                havingList.add(selectStmt.m_having);
            }
            selectStmt.m_having = ExpressionUtil.combinePredicates(havingList);
            // reprocess HAVING expressions
            ExpressionUtil.finalizeValueTypes(selectStmt.m_having);
        }

        selectStmt.m_aggregationList = null;

        if (selectStmt.needComplexAggregation()) {
            selectStmt.fillUpAggResultColumns();
        }
        else {
            selectStmt.m_aggResultColumns = selectStmt.m_displayColumns;
        }
        selectStmt.placeTVEsinColumns();
    }

    /**
     * Simplify the EXISTS expression:
     *  1. EXISTS ( table-agg-without-having-groupby) => TRUE
     *  2. Replace the display columns with a single dummy column "1"
     *     and GROUP BY expressions
     *  3. Drop DISTINCT expression
     *  4. Add LIMIT 1
     *  5. Remove ORDER BY expressions if HAVING expression is not present
     *
     * @param selectStmt
     * @return existsExpr
     */
    protected AbstractExpression simplifyExistsSubqueryStmt(
            AbstractExpression originalExistExpr) {
        // Verify the edge case of EXISTS ( table-agg-without-having-groupby)
        // for which the correct handling is to optimize out the exists
        // clause entirely as trivially true or false
        // if limit = 0 or offset > 0
        if (m_limitOffset.getLimit() == 0) {
            return ConstantValueExpression.getFalse();
        }
        // Except for "limit 0 offset ?" which is already covered,
        // can't optimize away the entire expression if there are
        // limit and/or offset parameters
        if (m_having == null &&
                m_groupByColumns.isEmpty() &&
                ! hasLimitOrOffsetParameters() &&
                displaysAgg()) {
            if (m_limitOffset.getOffset() == 0) {
                return ConstantValueExpression.getTrue();
            }
            else {
                return ConstantValueExpression.getFalse();
            }
        }

        // Remove ORDER BY columns
        m_orderColumns.clear();

        // Can drop GROUP BY expressions if there are no
        // HAVING/OFFSET expressions
        if (m_having == null && !hasOffset()) {
            m_groupByColumns.clear();
            m_groupByExpressions.clear();
        }

        // Remove all non-aggregate display columns if GROUP BY is empty
        if (m_groupByColumns.isEmpty()) {
            Iterator<ParsedColInfo >iter = m_displayColumns.iterator();
            while (iter.hasNext()) {
                ParsedColInfo col = iter.next();
                if (!col.expression.hasAnySubexpressionOfClass(
                        AggregateExpression.class)) {
                    iter.remove();
                }
            }
        }

        // If m_displayColumns is empty from the previous step
        // add a single dummy column
        if (m_displayColumns.isEmpty()) {
            ParsedColInfo col = new ParsedColInfo();
            col.expression =
                    ConstantValueExpression.makeExpression(VoltType.NUMERIC, "1");
            ExpressionUtil.finalizeValueTypes(col.expression);

            col.tableName = TEMP_TABLE_NAME;
            col.tableAlias = TEMP_TABLE_NAME;
            col.columnName = "$$_EXISTS_$$";
            col.alias = "$$_EXISTS_$$";
            col.index = 0;
            m_projectSchema = null;
            m_displayColumns.add(col);
        }

        if (m_aggResultColumns.isEmpty()) {
            m_hasAggregateExpression = false;
            m_hasAverage = false;
        }

        placeTVEsinColumns();
        needComplexAggregation();

        // Drop DISTINCT expression
        m_distinct = false;

        // Set LIMIT 1
        if (m_limitOffset.getLimitParameterId() == -1) {
            m_limitOffset.setLimit(1);
        }

        prepareLimitPlanNode(this, m_limitOffset);

        return originalExistExpr;
    }

    public boolean hasJoinOrder() {
        return m_joinOrder != null || m_hasLargeNumberOfTableJoins;
    }

    public ArrayList<JoinNode> getJoinOrder() {
        return m_joinOrderList;
    }

    public AbstractExpression getHavingPredicate() {
        return m_having;
    }

    @Override
    void postParse(String sql, String joinOrder) {
        super.postParse(sql, joinOrder);

        if (m_joinOrder != null) {
            // User indicates a join order already
            tryAddOneJoinOrder(m_joinOrder);
            return;
        }

        // prepare the join order if needed
        if (m_joinOrder == null &&
                m_tableAliasListAsJoinOrder.size() >
                StatementCompiler.DEFAULT_MAX_JOIN_TABLES) {
            // When there are large number of table joins,
            // give up the all permutations.
            // By default, try the join order with the
            // SQL query table order first.
            m_hasLargeNumberOfTableJoins = true;

            StringBuilder sb = new StringBuilder();
            String separator = "";
            for (int ii = 0; ii < m_tableAliasListAsJoinOrder.size(); ii++) {
                String tableAlias = m_tableAliasListAsJoinOrder.get(ii);
                sb.append(separator).append(tableAlias);
                separator = ",";
            }
            if (tryAddOneJoinOrder(sb.toString())) {
                return;
            }

            // The input join order is not valid
            // Find one valid join order to run,
            // which may not be the most efficient.
            ArrayDeque<JoinNode> joinOrderQueue =
                    SelectSubPlanAssembler.queueJoinOrders(m_joinTree, false);

            // Currently, we get one join order,
            // but it is easy to change the hard coded number
            // to get more join orders for large table joins.
            assert(joinOrderQueue.size() == 1);
            assert(m_joinOrderList.size() == 0);
            m_joinOrderList.addAll(joinOrderQueue);
        }
    }

    private boolean tryAddOneJoinOrder(String joinOrder) {
        ArrayList<String> tableAliases = new ArrayList<>();
        //Don't allow dups for now since self joins aren't supported
        HashSet<String> dupCheck = new HashSet<>();
        // Calling trim() up front is important only in the case of a
        // trailing comma.
        // It allows a trailing comma followed by whitespace as in "A,B, "
        // to be ignored like a normal trailing comma as in "A,B,".
        // The alternatives would be to treat these as different cases
        // (strange) or to complain about both -- which could be
        // accomplished by appending an additional space to the join
        // order here instead of calling trim.
        for (String element : joinOrder.trim().split(",")) {
            String alias = element.trim().toUpperCase();
            tableAliases.add(alias);
            if (!dupCheck.add(alias)) {
                if (m_hasLargeNumberOfTableJoins) {
                    return false;
                }

                StringBuilder sb = new StringBuilder();
                sb.append("The specified join order \"")
                .append(joinOrder)
                .append("\" contains a duplicate element \"")
                .append(alias)
                .append("\".");
                throw new PlanningErrorException(sb.toString());
            }
        }

        //TODO: now that the table aliases list is built, the remaining validations
        // here and in isValidJoinOrder should be combined in one AbstractParsedStmt function
        // that generates a JoinNode tree or throws an exception.
        if (m_tableAliasMap.size() != tableAliases.size()) {
            if (m_hasLargeNumberOfTableJoins) {
                return false;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("The specified join order \"")
            .append(joinOrder)
            .append("\" does not contain the correct number of elements\n")
            .append("Expected ")
            .append(m_tableList.size())
            .append(" but found ")
            .append(tableAliases.size())
            .append(" elements.");
            throw new PlanningErrorException(sb.toString());
        }

        Set<String> aliasSet = m_tableAliasMap.keySet();
        Set<String> specifiedNames = new HashSet<>(tableAliases);
        specifiedNames.removeAll(aliasSet);
        if (specifiedNames.isEmpty() == false) {
            if (m_hasLargeNumberOfTableJoins) {
                return false;
            }

            StringBuilder sb = new StringBuilder();
            sb.append("The specified join order \"");
            sb.append(joinOrder).append("\" contains ");
            int i = 0;
            for (String name : specifiedNames) {
                sb.append(name);
                if (++i != specifiedNames.size()) {
                    sb.append(',');
                }
            }
            sb.append(" which ");
            if (specifiedNames.size() == 1) {
                sb.append("doesn't ");
            }
            else {
                sb.append("don't ");
            }
            sb.append("exist in the FROM clause");
            throw new PlanningErrorException(sb.toString());
        }

        // Now check whether the specified join order is valid or not
        if ( ! isValidJoinOrder(tableAliases)) {
            if (m_hasLargeNumberOfTableJoins) return false;
            throw new PlanningErrorException(
                    "The specified join order is invalid for the given query");
        }

        // Inserted one join tree to the list
        assert(m_joinOrderList.size() > 0);
        m_joinTree = m_joinOrderList.get(0);
        return true;
    }

    /**
     * Validate the specified join order against the join tree.
     * In general, outer joins are not associative and commutative.
     * Not all orders are valid.
     * In case of a valid join order, the initial join tree is
     * rebuilt to match the specified order
     * @param tables list of table aliases(or tables) to join
     * @return true if the join order is valid
     */
    private boolean isValidJoinOrder(List<String> tableAliases) {
        assert(m_joinTree != null);

        // Split the original tree into the sub-trees
        // having the same join type for all nodes
        List<JoinNode> subTrees = m_joinTree.extractSubTrees();

        // For a sub-tree with inner joins only, any join order is valid.
        // The only requirement is that each and every table from that
        // sub-tree constitute an uninterrupted sequence in the specified
        // join order.
        // The outer joins are associative but changing the join order
        // precedence includes moving ON clauses to preserve the initial
        // SQL semantics.
        // For example,
        // T1 right join T2 on T1.C1 = T2.C1 left join T3 on T2.C2=T3.C2
        // can be rewritten as
        // T1 right join (T2 left join T3 on T2.C2=T3.C2) on T1.C1 = T2.C1
        // At the moment, such transformations are not supported.
        // The specified joined order must match the SQL order.
        int tableNameIdx = 0;
        List<JoinNode> finalSubTrees = new ArrayList<>();
        // Ee need to process the sub-trees last one first because
        // the top sub-tree is the first one on the list.
        for (int i = subTrees.size() - 1; i >= 0; --i) {
            JoinNode subTree = subTrees.get(i);
            // Get all tables for the subTree
            List<JoinNode> subTableNodes = subTree.generateLeafNodesJoinOrder();
            JoinNode joinOrderSubTree;
            if ((subTree instanceof BranchNode) &&
                    ((BranchNode)subTree).getJoinType() != JoinType.INNER) {
                // add the sub-tree as is
                joinOrderSubTree = subTree;
                for (JoinNode tableNode : subTableNodes) {
                    if (tableNode.getId() >= 0) {
                        String tableAlias = tableNode.getTableAlias();
                        if ( ! tableAliases.get(tableNameIdx++).equals(tableAlias)) {
                            return false;
                        }
                    }
                }
            }
            else {
                // Collect all the "real" tables from the sub-tree
                // skipping the nodes representing
                // the sub-trees with the different join type (id < 0)
                Map<String, JoinNode> nodeNameMap = new HashMap<>();
                for (JoinNode tableNode : subTableNodes) {
                    if (tableNode.getId() >= 0) {
                        nodeNameMap.put(tableNode.getTableAlias(), tableNode);
                    }
                }

                // rearrange the sub tree to match the order
                List<JoinNode> joinOrderSubNodes = new ArrayList<>();
                for (int j = 0; j < subTableNodes.size(); ++j) {
                    if (subTableNodes.get(j).getId() >= 0) {
                        assert(tableNameIdx < tableAliases.size());
                        String tableAlias = tableAliases.get(tableNameIdx);
                        if (tableAlias == null ||
                                ! nodeNameMap.containsKey(tableAlias)) {
                            return false;
                        }
                        joinOrderSubNodes.add(nodeNameMap.get(tableAlias));
                        ++tableNameIdx;
                    }
                    else {
                        // It's dummy node
                        joinOrderSubNodes.add(subTableNodes.get(j));
                    }
                }
                joinOrderSubTree = JoinNode.reconstructJoinTreeFromTableNodes(
                        joinOrderSubNodes, JoinType.INNER);
                //Collect all the join/where conditions to reassign them later
                AbstractExpression combinedWhereExpr = subTree.getAllFilters();
                if (combinedWhereExpr != null) {
                    joinOrderSubTree.setWhereExpression(
                            combinedWhereExpr.clone());
                }
                // The new tree root node id must match the original one
                // to be able to reconnect the subtrees
                joinOrderSubTree.setId(subTree.getId());
            }
            finalSubTrees.add(0, joinOrderSubTree);
        }
        // if we got there the join order is OK. Rebuild the whole tree
        JoinNode newNode = JoinNode.reconstructJoinTreeFromSubTrees(finalSubTrees);
        m_joinOrderList.add(newNode);
        return true;
    }

    public boolean hasWindowFunctionExpression() {
        return m_windowFunctionExpressions.size() > 0;
    }

    public boolean hasAggregateExpression() {
        return m_hasAggregateExpression;
    }

    public boolean hasAggregateOrGroupby() {
        return m_hasAggregateExpression || isGrouped();
    }

    public NodeSchema getFinalProjectionSchema() {
        return m_projectSchema;
    }

    public NodeSchema getDistinctProjectionSchema() {
        return m_distinctProjectSchema;
    }

    public boolean hasComplexGroupby() {
        return m_hasComplexGroupby;
    }

    public boolean hasComplexAgg() {
        return m_hasComplexAgg;
    }

    public boolean mayNeedAvgPushdown() {
        return m_hasAverage;
    }

    public boolean hasPartitionColumnInGroupby() {
        return m_hasPartitionColumnInGroupby;
    }

    public void setHasPartitionColumnInGroupby() {
        m_hasPartitionColumnInGroupby = true;
    }

    public boolean hasPartitionColumnInDistinctGroupby() {
        return m_hasPartitionColumnInDistinctGroupby;
    }

    public void setHasPartitionColumnInDistinctGroupby() {
        m_hasPartitionColumnInDistinctGroupby = true;
    }

    /**
     * Return true iff all windowed aggregate expressions have a
     * partition key in their partition by columns.
     *
     * @return
     */
    public boolean hasPartitionColumnInWindowFunctionExpression() {
        if (getWindowFunctionExpressions().size() == 0) {
            return true;
        }
        return m_hasPartitionColumnsInWindowedAggregates;
    }

    /**
     * Call this if we discover that some partition by column of a windowed
     * aggregate is a column on which a table is partitioned.
     */
    public void setHasPartitionColumnInWindowedAggregate() {
        m_hasPartitionColumnsInWindowedAggregates = true;
    }

    @Override
    /**
     * Return true if this ParsedSelectStmt has order by columns
     */
    public boolean hasOrderByColumns() {
        return ! m_orderColumns.isEmpty();
    }

    public void setUnsafeMVMessage(String msg) {
        m_mvUnSafeErrorMessage = msg;
    }

    public String getUnsafeMVMessage() {
        return m_mvUnSafeErrorMessage;
    }

    /**
     * For aggregation queries, ORDER BY can only apply on display columns list.
     * However, this is an edge case :
     * ORDER BY on GROUP BY columns or expression that are not selected.
     *
     * GROUP BY keys are not in select list will make it complex aggregated.
     * For this case, the PROJECTION node should apply after the ORDER BY node.
     *
     * However, any expression involved with ConstantValueExpression make us hard
     * to detect and compare, so we are just pessimistic about them and fall back
     * to use the old plan node tree.
     *
     * @return true when this query is the edge case query, false otherwise.
     */
    public boolean isComplexOrderBy() {
        return m_isComplexOrderBy;
    }

    private void detectComplexOrderby() {
        m_isComplexOrderBy = false;

        if (! hasOrderByColumns() || ! isGrouped()) {
            return;
        }

        // HAVING clause does not matter
        Set<AbstractExpression> missingGroupBySet = new HashSet<>();
        for (ParsedColInfo col : m_groupByColumns) {
            if (col.groupByInDisplay) {
                continue;
            }
            if (col.expression.hasAnySubexpressionOfClass(
                    ParameterValueExpression.class)) {
                // group by expression has ParameterValueExpression
                assert(hasComplexGroupby());

                // hsql has already guarded invalid ORDER BY expression.
                // let's be pessimistic about these cases to place Projection
                // above ORDER BY.
                m_isComplexOrderBy = true;
                return;
            }

            missingGroupBySet.add(col.expression);
        }

        // place the TVEs from Display columns in the ORDER BY expression
        for (ParsedColInfo orderCol : m_orderColumns) {
            AbstractExpression expr = orderCol.expression;
            // be pessimistic at this point
            if (expr.hasAnySubexpressionOfClass(ParameterValueExpression.class)) {
                m_isComplexOrderBy = true;
                return;
            }

            if (expr.hasSubExpressionFrom(missingGroupBySet)) {
                m_isComplexOrderBy = true;
                return;
            }
        }
    }

    /**
     * DISTINCT without GROUP BY is completely rewrote with a GROUP BY query.
     * DISTINCT with GROUP BY is not tricky, it needs another GROUP BY
     * on the original GROUP BY.
     * @return true if query has a DISTINCT with GROUP BY
     */
    public boolean hasDistinctWithGroupBy() {
        return m_distinct;
    }

    public boolean hasAggregateDistinct() {
        return m_hasAggregateDistinct;
    }

    public List<ParsedColInfo> displayColumns() {
        return Collections.unmodifiableList(m_displayColumns);
    }

    public List<ParsedColInfo> groupByColumns() {
        return Collections.unmodifiableList(m_groupByColumns);
    }

    @Override
    public List<ParsedColInfo> orderByColumns() {
        return Collections.unmodifiableList(m_orderColumns);
    }

    public boolean hasOffset() {
        return m_limitOffset.hasOffset();
    }

    @Override
    public boolean hasLimitOrOffset() {
        return m_limitOffset.hasLimitOrOffset();
    }

    public boolean hasLimitOrOffsetParameters() {
        return m_limitOffset.hasLimitOrOffsetParameters();
    }

    public boolean getCanPushdownLimit() {
        return m_limitOffset.m_limitCanPushdown;
    }

    /**
     * Returns true if this select statement can be proved to always produce
     * its result rows in the same order every time that it is executed.
     */
    @Override
    public boolean isOrderDeterministic() {
        if ( ! hasTopLevelScans()) {
            // This currently applies to parent queries that do all their
            // scanning in subqueries and so take on the order determinism of
            // their subqueries. This might have to be rethought to allow
            // ordering in parent queries to effect determinism of unordered
            // "FROM CLAUSE" subquery results.
            return true;
        }

        if (hasAOneRowResult()) {
            return true;
        }

        if ( ! hasOrderByColumns() ) {
            return false;
        }

        // The nonOrdered expression list is used as a short-cut
        // -- if an expression has been determined to be
        // non-ordered when encountered as a GROUP BY expression,
        // it will also be non-ordered when encountered in the select list.
        ArrayList<AbstractExpression> nonOrdered = new ArrayList<>();

        if (isGrouped()) {
            // Does the ordering of a statements's GROUP BY columns
            // ensure determinism?
            // All display columns and order-by expressions
            // are functionally dependent on the GROUP BY
            // columns even if the display column's values
            // are not ordered or unique,
            // so ordering by ALL of the GROUP BY columns is enough
            // to get full determinism,
            // EVEN if ordering by other (dependent) expressions,
            // regardless of the placement of non-GROUP BY expressions
            // in the ORDER BY list.
            if (orderByColumnsDetermineAllColumns(m_groupByColumns, nonOrdered)) {
                return true;
            }

            if (orderByColumnsDetermineAllDisplayColumns(nonOrdered)) {
                return true;
            }

        }
        else {
            if (orderByColumnsDetermineAllDisplayColumns(nonOrdered)) {
                return true;
            }

            if (orderByColumnsCoverUniqueKeys()) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isOrderDeterministicInSpiteOfUnorderedSubqueries() {
        if (hasAOneRowResult()) {
            return true;
        }

        if ( ! hasOrderByColumns() ) {
            return false;
        }

        // This is a trivial empty container.
        // In other code paths, it would list expressions
        // that have been pre-determined to be nonOrdered.
        ArrayList<AbstractExpression> nonOrdered = new ArrayList<>();

        if (orderByColumnsDetermineAllDisplayColumns(nonOrdered)) {
            return true;
        }

        return false;
    }

    private boolean orderByColumnsDetermineAllDisplayColumns(
            List<AbstractExpression> nonOrdered) {
        List<ParsedColInfo> unorderedDisplayColumns = new ArrayList<>();
        for (ParsedColInfo col : m_displayColumns) {
            if (! col.orderBy) {
                unorderedDisplayColumns.add(col);
            }
        }

        return orderByColumnsDetermineAllDisplayColumns(
                unorderedDisplayColumns, m_orderColumns, nonOrdered);
    }

    private boolean orderByColumnsDetermineAllColumns(
            List<ParsedColInfo> candidateColumns,
            List<AbstractExpression> outNonOrdered) {

        List<ParsedColInfo> filteredCandidateColumns = new ArrayList<>();
        for (ParsedColInfo col : candidateColumns) {
            if ( ! col.orderBy) {
                filteredCandidateColumns.add(col);
            }
        }

        return orderByColumnsDetermineAllColumns(
                m_orderColumns, filteredCandidateColumns, outNonOrdered);
    }

    /**
     * Returns true if the specified set of "display columns" passed in by caller
     * will be determistically ordered if sorting by the specified set of columns
     * from an "ORDER BY" clause.  Note that the ORDER BY columns could be from an
     * set operator (UNION, INTERSECT, etc) that has this select statement as a child.
     *
     * @param displayColumns  The set of display columns whose order we care about
     * @param orderColumns    The columns that we are ordering by
     * @param nonOrdered      Columns whose values are known to appear in
     *                        non-deterministic order
     * @return true if the given order by columns will determine
     *         the order of the given display columns
     */
    boolean orderByColumnsDetermineAllDisplayColumns(
            List<ParsedColInfo> displayColumns,
            List<ParsedColInfo> orderColumns,
            List<AbstractExpression> nonOrdered) {
        ArrayList<ParsedColInfo> candidateColumns = new ArrayList<>();
        for (ParsedColInfo displayCol : displayColumns) {
            if (displayCol.groupBy) {
                AbstractExpression displayExpr = displayCol.expression;
                // Round up the usual suspects
                // -- if there were uncooperative GROUP BY expressions,
                // they will often also be uncooperative
                // display column expressions.
                for (AbstractExpression nonStarter : nonOrdered) {
                    if (displayExpr.equals(nonStarter)) {
                        return false;
                    }
                }
                // A GROUP BY expression that was not nonOrdered must be covered.
                continue;
            }
            candidateColumns.add(displayCol);
        }
        return orderByColumnsDetermineAllColumns(
                orderColumns, candidateColumns, null);
    }

    private boolean orderByColumnsDetermineAllColumns(
            List<ParsedColInfo> orderColumns,
            List<ParsedColInfo> candidateColumns,
            List<AbstractExpression> outNonOrdered) {
        HashSet<AbstractExpression> orderByExprs = null;
        ArrayList<AbstractExpression> candidateExprHardCases = null;
        // First try to get away with a brute force
        // N by M search for exact equalities.
        for (ParsedColInfo candidateCol : candidateColumns) {
            AbstractExpression candidateExpr = candidateCol.expression;
            if (orderByExprs == null) {
                orderByExprs = new HashSet<>();
                for (ParsedColInfo orderByCol : orderColumns) {
                    orderByExprs.add(orderByCol.expression);
                }
                addHonoraryOrderByExpressions(orderByExprs, candidateColumns);
            }
            if (orderByExprs.contains(candidateExpr)) {
                continue;
            }
            if (candidateExpr instanceof TupleValueExpression) {
                // Simple column references can only be
                // exactly equal to but not "based on" an ORDER BY.
                return false;
            }

            if (candidateExprHardCases == null) {
                candidateExprHardCases = new ArrayList<>();
            }
            candidateExprHardCases.add(candidateExpr);
        }

        if (candidateExprHardCases == null) {
            return true;
        }

        // Plan B. profile the ORDER BY list and try to include/exclude
        // the hard cases on that basis.
        HashSet<AbstractExpression> orderByTVEs = new HashSet<>();
        ArrayList<AbstractExpression> orderByNonTVEs = new ArrayList<>();
        ArrayList<List<TupleValueExpression>> orderByNonTVEBaseTVEs = new ArrayList<>();
        HashSet<AbstractExpression> orderByAllBaseTVEs = new HashSet<>();

        for (AbstractExpression orderByExpr : orderByExprs) {
            if (orderByExpr instanceof TupleValueExpression) {
                orderByTVEs.add(orderByExpr);
                orderByAllBaseTVEs.add(orderByExpr);
            }
            else {
                orderByNonTVEs.add(orderByExpr);
                List<TupleValueExpression> baseTVEs =
                        orderByExpr.findAllTupleValueSubexpressions();
                orderByNonTVEBaseTVEs.add(baseTVEs);
                orderByAllBaseTVEs.addAll(baseTVEs);
            }
        }

        boolean result = true;

        for (AbstractExpression candidateExpr : candidateExprHardCases) {
            Collection<TupleValueExpression> candidateBases =
                    candidateExpr.findAllTupleValueSubexpressions();
            if (orderByTVEs.containsAll(candidateBases)) {
                continue;
            }
            if (orderByAllBaseTVEs.containsAll(candidateBases) == false) {
                if (outNonOrdered == null) {
                    // Short-circuit if the remaining non-qualifying
                    // expressions are not of interest.
                    return false;
                }

                result = false;
                outNonOrdered.add(candidateExpr);
                continue;
            }

            // At this point, if the candidateExpr is a match,
            // then it is based on but not equal to one or more
            // orderByNonTVE(s) and optionally orderByTVE(s).
            // The simplest example is like
            // "SELECT a+(b-c) ... ORDER BY a, b-c;"
            // If it is a non-match,
            // it is an original expression based on orderByAllBaseTVEs
            // The simplest example is like
            // "SELECT a+b ... ORDER BY a, b-c;"
            // TODO: process REALLY HARD CASES
            // TODO: issue a warning, short-term?
            // For now, err on the side of non-determinism.
            if (outNonOrdered == null) {
                // Short-circuit if the remaining
                // non-qualifying expressions are not of interest.
                return false;
            }

            outNonOrdered.add(candidateExpr);
            result = false;
        }
        return result;
    }

    /**
     * This is a very simple version of the above method for when an ORDER BY
     * clause appears on a UNION.  Does the ORDER BY clause reference every item
     * on the display list?  If so, then the order is deterministic.
     *
     * Note that in this method we don't do more sophisticated analysis
     * (like using value equivalence, or knowledge of unique indexes)
     * because we want to prove that *every* child of a UNION is deterministic,
     * not just this one.
     *
     * @param orderColumns  ORDER BY columns on the UNION
     * @return  true if all items on display list are in the UNION's ORDER BY
     */
    public boolean orderByColumnsDetermineAllDisplayColumnsForUnion(
            List<ParsedColInfo> orderColumns) {
        Set<AbstractExpression> orderExprs = new HashSet<>();
        for (ParsedColInfo col : orderColumns) {
            orderExprs.add(col.expression);
        }

        for (ParsedColInfo col : m_displayColumns) {
            if (! orderExprs.contains(col.expression)) {
                return false;
            }
        }

        return true;
    }

    private boolean hasAOneRowResult() {
        if ( ( ! isGrouped() ) && displaysAgg()) {
            return true;
        }
        return producesOneRowOutput();
    }

    private boolean hasTopLevelScans() {
        for (StmtTableScan scan : allScans()) {
            if (scan instanceof StmtTargetTableScan) {
                return true;
            }
        }
        return false;
    }

    private boolean displaysAgg() {
        for (ParsedColInfo displayCol : m_displayColumns) {
            if (displayCol.expression.hasAnySubexpressionOfClass(
                    AggregateExpression.class)) {
                return true;
            }
        }
        return false;
    }

    public boolean isGrouped() { return ! m_groupByColumns.isEmpty(); }

    public boolean displayColumnsContainAllGroupByColumns() {
        for (ParsedColInfo groupedCol : m_groupByColumns) {
            boolean missing = true;
            for (ParsedColInfo displayCol : m_displayColumns) {
                if (displayCol.groupBy) {
                    if (groupedCol.equals(displayCol)) {
                        missing = false;
                        break;
                    }
                }
            }
            if (missing) {
                return false;
            }
        }
        return true;
    }

    boolean groupByIsAnOrderByPermutation() {
        if (m_groupAndOrderByPermutationWasTested) {
            return m_groupAndOrderByPermutationResult;
        }
        m_groupAndOrderByPermutationWasTested = true;

        int size = m_groupByColumns.size();
        if (size != m_orderColumns.size()) {
            return false;
        }
        Set<AbstractExpression> orderPrefixExprs = new HashSet<>(size);
        Set<AbstractExpression> groupExprs = new HashSet<>(size);
        int ii = 0;
        for (ParsedColInfo gb : m_groupByColumns) {
            AbstractExpression gexpr = gb.expression;
            if (gb.expression == null) {
                return false;
            }
            AbstractExpression oexpr = m_orderColumns.get(ii).expression;
            ++ii;
            // Save some cycles in the common case of matching by position.
            if (gb.expression.equals(oexpr)) {
                continue;
            }
            groupExprs.add(gexpr);
            orderPrefixExprs.add(oexpr);
        }
        m_groupAndOrderByPermutationResult = groupExprs.equals(orderPrefixExprs);
        return m_groupAndOrderByPermutationResult;
    }

    void checkPlanColumnMatch(List<SchemaColumn> columns) {
        // Sanity-check the output NodeSchema columns against the display columns
        if (m_displayColumns.size() != columns.size()) {
            throw new PlanningErrorException(
                    "Mismatched plan output cols to parsed display columns");
        }
        int ii = 0;
        for (ParsedColInfo display_col : m_displayColumns) {
            SchemaColumn sc = columns.get(ii);
            ++ii;
            boolean sameTable = false;

            if (display_col.tableAlias != null) {
                if (display_col.tableAlias.equals(sc.getTableAlias())) {
                    sameTable = true;
                }
            }
            else if (display_col.tableName.equals(sc.getTableName())) {
                sameTable = true;
            }

            if (sameTable) {
                String displayAlias = display_col.alias;
                if (displayAlias != null && ! displayAlias.equals("")) {
                    if (displayAlias.equals(sc.getColumnAlias())) {
                        continue;
                    }
                }
                else {
                    String displayName = display_col.columnName;
                    if (displayName != null &&
                        ( ! displayName.equals("")) &&
                        displayName.equals(sc.getColumnName())) {
                        continue;
                    }
                }
            }
            throw new PlanningErrorException(
                    "Mismatched plan output cols to parsed display columns");
        }
    }

    /**
     * This functions tries to find all expression from the statement. For complex group by or complex aggregate,
     * we have a special function ParsedSelectStmt::placeTVEsinColumns() to replace the expression with TVE for
     * the convenience of projection node after group by node.
     *
     * So use the original expression from the XML of hsqldb, other than the processed information. See ENG-8263.
     * m_having and m_projectSchema seem to have the same issue in ENG-8263.
     */
    @Override
    public Set<AbstractExpression> findAllSubexpressionsOfClass(
            Class< ? extends AbstractExpression> aeClass) {
        Set<AbstractExpression> exprs =
                super.findAllSubexpressionsOfClass(aeClass);

        if (m_having != null) {
            Collection<AbstractExpression> found =
                    m_having.findAllSubexpressionsOfClass(aeClass);
            if (! found.isEmpty()) {
                exprs.addAll(found);
            }
        }
        addAllSubexpressionsOfClassFromColList(exprs, aeClass, m_groupByColumns);
        if (m_projectSchema != null) {
            m_projectSchema.addAllSubexpressionsOfClassFromNodeSchema(exprs, aeClass);
        }
        // m_having, m_groupByExpressions, or m_projectSchema may no longer contain
        // the aggregation or group by expression if they have been replaced with
        // TVEs already and added to m_aggResultColumns.
        // So, also look for the original expression in m_aggResultColumns.
        addAllSubexpressionsOfClassFromColList(exprs, aeClass, m_aggResultColumns);

        if (m_avgPushdownHaving != null &&
                m_avgPushdownHaving != m_having) {
            Collection<AbstractExpression> found =
                    m_avgPushdownHaving.findAllSubexpressionsOfClass(aeClass);
            if (! found.isEmpty()) {
                exprs.addAll(found);
            }
        }
        if (m_avgPushdownGroupByColumns != null &&
                m_avgPushdownGroupByColumns != m_groupByColumns) {
            addAllSubexpressionsOfClassFromColList(exprs, aeClass,
                    m_avgPushdownGroupByColumns);
        }
        if (m_avgPushdownProjectSchema != null &&
                m_avgPushdownProjectSchema != m_projectSchema) {
            m_avgPushdownProjectSchema.addAllSubexpressionsOfClassFromNodeSchema(exprs, aeClass);
        }
        // m_avgPushdownHaving, m_avgPushdownGroupByColumns,
        // or m_avgPushdownProjectSchema may no longer contain
        // the aggregation or group by expression if they have been
        // replaced with TVEs already and added to
        // m_avgPushdownAggResultColumns.
        // So, also look for the original expression in
        // m_avgPushdownAggResultColumns.
        if (m_avgPushdownAggResultColumns != null &&
                m_avgPushdownAggResultColumns != m_aggResultColumns) {
            addAllSubexpressionsOfClassFromColList(exprs, aeClass,
                    m_avgPushdownAggResultColumns);
        }
        return exprs;

    }

    private static void addAllSubexpressionsOfClassFromColList(
            Set<AbstractExpression> exprs,
            Class<? extends AbstractExpression> aeClass,
            List<ParsedColInfo> colList) {
        for (ParsedColInfo col : colList) {
            AbstractExpression colExpr = col.expression;
            if (colExpr == null) {
                continue;
            }

            Collection<AbstractExpression> found =
                    colExpr.findAllSubexpressionsOfClass(aeClass);
            if (found.isEmpty()) {
                continue;
            }

            exprs.addAll(found);
        }
    }

    @Override
    public String calculateContentDeterminismMessage() {
        String ans = m_contentDeterminismMessage;

        // Don't search if we already know the answer.
        if (ans == null) {
            /*
             * Is there a message in the display columns?
             */
            for (ParsedColInfo displayCol : m_displayColumns) {
                AbstractExpression displayExpr = displayCol.expression;
                ans = displayExpr.getContentDeterminismMessage();
                if (ans != null) {
                    break;
                }
            }

            /*
             * Is there a message in the having expression?
             */
            if (ans == null && m_having != null) {
                ans = m_having.getContentDeterminismMessage();
            }

            /*
             * Is there a message in the join tree?
             */
            if (ans == null && m_joinTree != null) {
                ans = m_joinTree.getContentDeterminismMessage();
            }
        }
        if (ans != null) {
            updateContentDeterminismMessage(ans);
        }
        return ans;
    }

    @Override
    public boolean isDML() { return false; }

    /**
     * Return true iff all the windowed partition expressions
     * have a table partition column in their partition by list,
     * and if there is one such windowed partition expression.
     * If there are no windowed expressions, we return false.
     * Note that there can only be one windowed
     * expression currently, so this is more general than it needs to be.
     *
     * @return
     */
    public boolean isPartitionColumnInWindowedAggregatePartitionByList() {
        if (getWindowFunctionExpressions().size() == 0) {
            return false;
        }
        // We can't really have more than one Windowed Aggregate Expression.
        // If we ever do, this should fail gracelessly.
        assert(getWindowFunctionExpressions().size() == 1);
        WindowFunctionExpression we = getWindowFunctionExpressions().get(0);
        List<AbstractExpression> partitionByExprs = we.getPartitionByExpressions();
        boolean foundPartExpr = false;
        for (AbstractExpression ae : partitionByExprs) {
            if ( ! (ae instanceof TupleValueExpression ) ) {
                continue;
            }

            TupleValueExpression tve = (TupleValueExpression) ae;
            String tableAlias    = tve.getTableAlias();
            String columnName    = tve.getColumnName();
            StmtTableScan scanTable = getStmtTableScanByAlias(tableAlias);
            if (scanTable == null || scanTable.getPartitioningColumns() == null) {
                continue;
            }

            boolean foundPartCol = false;
            for (SchemaColumn pcol : scanTable.getPartitioningColumns()) {
                if (pcol != null && pcol.getColumnName().equals(columnName)) {
                    foundPartCol = true;
                    break;
                }
            }
            // If we found a partition column, then we don't
            // need to look at any other partition by expressions
            // in this windowed expression.
            if (foundPartCol) {
                foundPartExpr = true;
                break;
            }
        }
        return foundPartExpr;
    }

    /**
     * Gather up all the join expressions.  This happens before planning.
     * So, all the innerouter and innerinner lists are not yet populated.
     *
     * @param checkExpressions
     */
    public void gatherJoinExpressions(List<AbstractExpression> checkExpressions) {
        // TODO Auto-generated method stub
        m_joinTree.gatherJoinExpressions(checkExpressions);
    }

}
