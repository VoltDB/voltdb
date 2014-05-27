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

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.parseinfo.StmtSubqueryScan;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;
import org.voltdb.types.ExpressionType;

public class ParsedSelectStmt extends AbstractParsedStmt {

    public static class ParsedColInfo implements Cloneable{
        public String alias = null;
        public String columnName = null;
        public String tableName = null;
        public String tableAlias = null;
        public AbstractExpression expression = null;
        public int index = 0;

        // orderby stuff
        public boolean orderBy = false;
        public boolean ascending = true;

        // groupby
        public boolean groupBy = false;

        @Override
        public boolean equals (Object obj) {
            if (obj == null) return false;
            if (obj instanceof ParsedColInfo == false) return false;
            ParsedColInfo col = (ParsedColInfo) obj;
            if ( columnName != null && columnName.equals(col.columnName) &&
                    tableName != null && tableName.equals(col.tableName) &&
                    tableAlias != null && tableAlias.equals(col.tableAlias) &&
                    expression != null && expression.equals(col.expression) )
                return true;
            return false;
        }

        // Based on implementation on equals().
        @Override
        public int hashCode() {
            int result = new HashCodeBuilder(17, 31).
                    append(columnName).append(tableName).append(tableAlias).
                    toHashCode();
            if (expression != null) {
                result += expression.hashCode();
            }
            return result;
        }

        @Override
        public ParsedColInfo clone() {
            ParsedColInfo col = null;
            try {
                col = (ParsedColInfo) super.clone();
            } catch (CloneNotSupportedException e) {
                e.printStackTrace();
            }
            col.expression = (AbstractExpression) expression.clone();
            return col;
        }
    }

    public ArrayList<ParsedColInfo> m_displayColumns = new ArrayList<ParsedColInfo>();
    public ArrayList<ParsedColInfo> m_orderColumns = new ArrayList<ParsedColInfo>();
    public AbstractExpression m_having = null;
    public ArrayList<ParsedColInfo> m_groupByColumns = new ArrayList<ParsedColInfo>();
    private boolean m_groupAndOrderByPermutationWasTested = false;
    private boolean m_groupAndOrderByPermutationResult = false;

    // It will store the final projection node schema for this plan if it is needed.
    // Calculate once, and use it everywhere else.
    private NodeSchema m_projectSchema = null;

    // It may has the consistent element order as the displayColumns
    public ArrayList<ParsedColInfo> m_aggResultColumns = new ArrayList<ParsedColInfo>();
    public Map<String, AbstractExpression> m_groupByExpressions = null;

    private ArrayList<ParsedColInfo> m_avgPushdownDisplayColumns = null;
    private ArrayList<ParsedColInfo> m_avgPushdownAggResultColumns = null;
    private ArrayList<ParsedColInfo> m_avgPushdownOrderColumns = null;
    private AbstractExpression m_avgPushdownHaving = null;
    private NodeSchema m_avgPushdownNewAggSchema;
    private boolean m_hasPartitionColumnInGroupby = false;

    public long m_limit = -1;
    public long m_offset = 0;

    private long m_limitParameterId = -1;
    private long m_offsetParameterId = -1;
    private boolean m_distinct = false;
    private boolean m_hasComplexAgg = false;
    private boolean m_hasComplexGroupby = false;
    private boolean m_hasAggregateExpression = false;
    private boolean m_hasAverage = false;

    public MaterializedViewFixInfo m_mvFixInfo = new MaterializedViewFixInfo();

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
        if ((node = stmtNode.attributes.get("distinct")) != null)
            m_distinct = Boolean.parseBoolean(node);

        VoltXMLElement limitElement = null, offsetElement = null, havingElement = null;
        VoltXMLElement displayElement = null, orderbyElement = null, groupbyElement = null;
        for (VoltXMLElement child : stmtNode.children) {
            if (child.name.equalsIgnoreCase("limit")) {
                limitElement = child;
            } else if (child.name.equalsIgnoreCase("offset")) {
                offsetElement = child;
            } else if (child.name.equalsIgnoreCase("columns")) {
                displayElement = child;
            } else if (child.name.equalsIgnoreCase("ordercolumns")) {
                orderbyElement = child;
            } else if (child.name.equalsIgnoreCase("groupcolumns")) {
                groupbyElement = child;
            } else if (child.name.equalsIgnoreCase("having")) {
                havingElement = child;
            }
        }
        parseLimitAndOffset(limitElement, offsetElement);

        if (m_aggregationList == null) {
            m_aggregationList = new ArrayList<AbstractExpression>();
        }
        // We want to extract display first, groupBy second before processing orderBy
        // Because groupBy and orderBy need display columns to tag its columns
        // OrderBy needs groupBy columns to stop recursively finding TVEs for pass-through columns.
        assert(displayElement != null);
        parseDisplayColumns(displayElement, false);

        if (groupbyElement != null) {
            parseGroupByColumns(groupbyElement);
            insertToColumnList(m_aggResultColumns, m_groupByColumns);
        }

        if (orderbyElement != null && ! hasAOneRowResult()) {
            parseOrderColumns(orderbyElement, false);
        }

        if (havingElement != null) {
            parseHavingExpression(havingElement, false);
        }
        // At this point, we have collected all aggregations in the select statement.
        // We do not need aggregationList container in parseXMLtree
        // Make it null to prevent others adding elements to it when parsing the tree
        m_aggregationList = null;

        if (needComplexAggregation()) {
            fillUpAggResultColumns();
        } else {
            m_aggResultColumns = m_displayColumns;
        }
        placeTVEsinColumns();

        // Prepare for the AVG push-down optimization only if it might be required.
        if (mayNeedAvgPushdown()) {
            processAvgPushdownOptimization(displayElement, orderbyElement, groupbyElement, havingElement);
        }

        prepareMVBasedQueryFix();
    }

    private void processAvgPushdownOptimization (VoltXMLElement displayElement,
            VoltXMLElement orderbyElement, VoltXMLElement groupbyElement, VoltXMLElement havingElement) {

        ArrayList<ParsedColInfo> tmpDisplayColumns = m_displayColumns;
        m_displayColumns = new ArrayList<ParsedColInfo>();
        ArrayList<ParsedColInfo> tmpAggResultColumns = m_aggResultColumns;
        m_aggResultColumns = new ArrayList<ParsedColInfo>();
        ArrayList<ParsedColInfo> tmpOrderColumns = m_orderColumns;
        m_orderColumns = new ArrayList<ParsedColInfo>();
        AbstractExpression tmpHaving = m_having;


        boolean tmpHasComplexAgg = hasComplexAgg();
        NodeSchema tmpNodeSchema = m_projectSchema;

        // Make final schema output null to get a new schema when calling placeTVEsinColumns().
        m_projectSchema = null;

        m_aggregationList = new ArrayList<AbstractExpression>();
        assert(displayElement != null);
        parseDisplayColumns(displayElement, true);

        if (groupbyElement != null) {
            insertToColumnList(m_aggResultColumns, m_groupByColumns);
        }
        if (orderbyElement != null) {
            parseOrderColumns(orderbyElement, true);
        }
        if (havingElement != null) {
            parseHavingExpression(havingElement, true);
        }
        m_aggregationList = null;
        fillUpAggResultColumns();
        placeTVEsinColumns();

        // Switch them back
        m_avgPushdownDisplayColumns = m_displayColumns;
        m_avgPushdownAggResultColumns = m_aggResultColumns;
        m_avgPushdownOrderColumns = m_orderColumns;
        m_avgPushdownNewAggSchema = m_projectSchema;
        m_avgPushdownHaving = m_having;

        m_displayColumns = tmpDisplayColumns;
        m_aggResultColumns = tmpAggResultColumns;
        m_orderColumns = tmpOrderColumns;
        m_projectSchema = tmpNodeSchema;
        m_hasComplexAgg = tmpHasComplexAgg;
        m_having = tmpHaving;
    }

    /**
     * Switch the optimal set for pushing down AVG
     */
    public void switchOptimalSuiteForAvgPushdown () {
        m_displayColumns = m_avgPushdownDisplayColumns;
        m_aggResultColumns = m_avgPushdownAggResultColumns;
        m_orderColumns = m_avgPushdownOrderColumns;
        m_projectSchema = m_avgPushdownNewAggSchema;
        m_hasComplexAgg = true;
        m_having = m_avgPushdownHaving;
    }

    /**
     * Prepare for the mv based distributed query fix only if it might be required.
     */
    private void prepareMVBasedQueryFix() {

        // ENG-5386: Edge cases query returning correct answers with aggregation push down does not need reAggregation work.
        if (m_hasComplexGroupby) {
            m_mvFixInfo.setEdgeCaseQueryNoFixNeeded(false);
        }

        // Handle joined query case case.
        // MV partitioned table without partition column can only join with replicated tables.
        // For all tables in this query, the # of tables that need to be fixed should not exceed one.
        for (StmtTableScan mvTableScan: m_tableAliasMap.values()) {
            Set<SchemaColumn> mvNewScanColumns = new HashSet<SchemaColumn>();

            Collection<SchemaColumn> columns = mvTableScan.getScanColumns();
            // For a COUNT(*)-only scan, a table may have no scan columns.
            // For a joined query without processed columns from table TB, TB has no scan columns
            if (columns != null) {
                mvNewScanColumns.addAll(columns);
            }
            // ENG-5669: HAVING aggregation and order by aggregation also need to be checked.
            if (m_mvFixInfo.processMVBasedQueryFix(mvTableScan, mvNewScanColumns, m_joinTree, m_aggResultColumns, groupByColumns())) {
                break;
            }
        }
    }

    private boolean needComplexAggregation () {
        if (!hasAggregateExpression() && !isGrouped()) {
            m_hasComplexAgg = false;
            return false;
        }
        if (hasComplexAgg()) return true;

        int numDisplayCols = m_displayColumns.size();
        if (m_aggResultColumns.size() > numDisplayCols) {
            m_hasComplexAgg = true;
            return true;
        }

        for (ParsedColInfo col : m_displayColumns) {
            if (!m_aggResultColumns.contains(col)) {
                // Now Only TVEs in displayColumns are left for AggResultColumns
                if (col.expression instanceof TupleValueExpression) {
                    m_aggResultColumns.add(col);
                } else {
                    // Col must be complex expression (like: TVE + 1, TVE + AGG)
                    m_hasComplexAgg = true;
                    return true;
                }
            }
        }
        // size of aggResultColumns list should be the same as numDisplayCols
        // as it would be a substitue of DisplayCols.
        if (m_aggResultColumns.size() != numDisplayCols) {
            // Display columns have duplicated Aggs or TVEs (less than case)
            // Display columns have several pass-through columns if group by primary key (larger than case)
            m_hasComplexAgg = true;
            return true;
        }

        // Case: Groupby cols do not appear in SELECT list
        // Find duplicates
        HashSet <ParsedColInfo> tmpContainer = new HashSet<ParsedColInfo>();

        for (int i=0; i < numDisplayCols; i++) {
            ParsedColInfo icol = m_displayColumns.get(i);
            if (tmpContainer.contains(icol)) {
                m_hasComplexAgg = true;
                return true;
            } else {
                tmpContainer.add(icol);
            }
        }

        return false;
    }

    /**
     * Continue adding TVEs from DisplayColumns that are left in function needComplexAggregation().
     * After this function, aggResultColumns construction work.
     */
    private void fillUpAggResultColumns () {
        for (ParsedColInfo col: m_displayColumns) {
            if (!m_aggResultColumns.contains(col)) {
                if (col.expression instanceof TupleValueExpression) {
                    m_aggResultColumns.add(col);
                } else {
                    // Col must be complex expression (like: TVE + 1, TVE + AGG)
                    List<TupleValueExpression> tveList = new ArrayList<TupleValueExpression>();
                    findAllTVEs(col.expression, tveList);
                    insertTVEsToAggResultColumns(tveList);
                }
            }
        }
    }

    /**
     * Generate new output Schema and Place TVEs for display columns if needed.
     * Place TVEs for order by columns always.
     */
    private void placeTVEsinColumns () {
        // Build the association between the table column with its index
        Map <AbstractExpression, Integer> aggTableIndexMap = new HashMap <AbstractExpression,Integer>();
        Map <Integer, ParsedColInfo> indexToColumnMap = new HashMap <Integer, ParsedColInfo>();
        int index = 0;
        for (ParsedColInfo col: m_aggResultColumns) {
            aggTableIndexMap.put(col.expression, index);
            if ( col.alias == null) {
                // hack any unique string
                col.alias = "$$_" + col.expression.getExpressionType().symbol() + "_$$_" + index;
            }
            indexToColumnMap.put(index, col);
            index++;
        }

        // Replace TVE for display columns
        if (m_projectSchema == null) {
            m_projectSchema = new NodeSchema();
            for (ParsedColInfo col : m_displayColumns) {
                AbstractExpression expr = col.expression;
                if (hasComplexAgg()) {
                    expr = col.expression.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
                }
                SchemaColumn schema_col = new SchemaColumn(col.tableName, col.tableAlias, col.columnName, col.alias, expr);
                m_projectSchema.addColumn(schema_col);
            }
        }

        // Replace TVE for order by columns
        for (ParsedColInfo orderCol : m_orderColumns) {
            AbstractExpression expr = orderCol.expression.replaceWithTVE(aggTableIndexMap, indexToColumnMap);

            if (hasComplexAgg()) {
                orderCol.expression = expr;
            } else {
                // This if case checking is to rule out cases like: select PKEY + A_INT from O1 order by PKEY + A_INT,
                // This case later needs a projection node on top of sort node to make it work.

                // Assuming the restrictions: Order by columns are (1) columns from table
                // (2) tag from display columns (3) actual expressions from display columns
                // Currently, we do not allow order by complex expressions that are not in display columns

                // If there is a complexGroupby at his point, it means that Display columns contain all the order by columns.
                // In that way, this plan does not require another projection node on top of sort node.
                if (orderCol.expression.hasAnySubexpressionOfClass(AggregateExpression.class) ||
                        hasComplexGroupby()) {
                    orderCol.expression = expr;
                }
            }
        }

        // Replace TVE for group by columns
        m_groupByExpressions = new HashMap<String, AbstractExpression>();
        for (ParsedColInfo groupbyCol: m_groupByColumns) {
            assert(aggTableIndexMap.get(groupbyCol.expression) != null);
            assert(m_groupByExpressions.get(groupbyCol.alias) == null);
            AbstractExpression expr = groupbyCol.expression.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
            m_groupByExpressions.put(groupbyCol.alias,expr);
        }

        if (m_having != null) {
            m_having = m_having.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
            ExpressionUtil.finalizeValueTypes(m_having);
        }

    }

    /**
     * ParseDisplayColumns and ParseOrderColumns will call this function
     * to add Aggregation expressions to aggResultColumns
     * @param aggColumns
     * @param cookedCol
     */
    private void insertAggExpressionsToAggResultColumns (List<AbstractExpression> aggColumns, ParsedColInfo cookedCol) {
        for (AbstractExpression expr: aggColumns) {
            ParsedColInfo col = new ParsedColInfo();
            col.expression = (AbstractExpression) expr.clone();
            assert(col.expression instanceof AggregateExpression);
            if (col.expression.getExpressionType() == ExpressionType.AGGREGATE_AVG) {
                m_hasAverage = true;
            }
            if (aggColumns.size() == 1 && cookedCol.expression.equals(aggColumns.get(0))) {
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
            col.tableName = "VOLT_TEMP_TABLE";
            col.tableAlias = "VOLT_TEMP_TABLE";
            col.columnName = "";
            if (!m_aggResultColumns.contains(col)) {
                m_aggResultColumns.add(col);
            }
            ExpressionUtil.finalizeValueTypes(col.expression);
        }
    }

    private void insertTVEsToAggResultColumns (List<TupleValueExpression> colCollection) {
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
    private static void insertToColumnList (List<ParsedColInfo>columnList,
            List<ParsedColInfo> newCols) {
        for (ParsedColInfo col: newCols) {
            if (!columnList.contains(col)) {
                columnList.add(col);
            }
        }
    }

    private static boolean isNewtoColumnList(List<ParsedColInfo>columnList, AbstractExpression expr) {
        boolean isNew = true;
        for (ParsedColInfo ic: columnList) {
            if (ic.expression.equals(expr)) {
                isNew = false;
                break;
            }
        }
        return isNew;
    }

    /**
     * Find all TVEs except inside of AggregationExpression
     * @param expr
     * @param tveList
     */
    private void findAllTVEs(AbstractExpression expr, List<TupleValueExpression> tveList) {
        if (!isNewtoColumnList(m_aggResultColumns, expr))
            return;
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
        List<AbstractExpression> optimalAvgAggs = new ArrayList<AbstractExpression>();
        Iterator<AbstractExpression> itr = m_aggregationList.iterator();
        while(itr.hasNext()) {
            AbstractExpression aggExpr = itr.next();
            assert(aggExpr instanceof AggregateExpression);
            if (aggExpr.getExpressionType() == ExpressionType.AGGREGATE_AVG) {
                itr.remove();

                AbstractExpression left = new AggregateExpression(ExpressionType.AGGREGATE_SUM);
                left.setLeft(aggExpr.getLeft());
                AbstractExpression right = new AggregateExpression(ExpressionType.AGGREGATE_COUNT);
                right.setLeft(aggExpr.getLeft());

                optimalAvgAggs.add(left);
                optimalAvgAggs.add(right);
            }
        }
        m_aggregationList.addAll(optimalAvgAggs);
    }

    private void parseLimitAndOffset(VoltXMLElement limitNode, VoltXMLElement offsetNode) {
        String node;
        if (limitNode != null) {
            // Parse limit
            if ((node = limitNode.attributes.get("limit_paramid")) != null)
                m_limitParameterId = Long.parseLong(node);
            else {
                assert(limitNode.children.size() == 1);
                VoltXMLElement valueNode = limitNode.children.get(0);
                String isParam = valueNode.attributes.get("isparam");
                if ((isParam != null) && (isParam.equalsIgnoreCase("true"))) {
                    m_limitParameterId = Long.parseLong(valueNode.attributes.get("id"));
                } else {
                    node = limitNode.attributes.get("limit");
                    assert(node != null);
                    m_limit = Long.parseLong(node);
                }
            }
        }
        if (offsetNode != null) {
            // Parse offset
            if ((node = offsetNode.attributes.get("offset_paramid")) != null)
                m_offsetParameterId = Long.parseLong(node);
            else {
                if (offsetNode.children.size() == 1) {
                    VoltXMLElement valueNode = offsetNode.children.get(0);
                    String isParam = valueNode.attributes.get("isparam");
                    if ((isParam != null) && (isParam.equalsIgnoreCase("true"))) {
                        m_offsetParameterId = Long.parseLong(valueNode.attributes.get("id"));
                    } else {
                        node = offsetNode.attributes.get("offset");
                        assert(node != null);
                        m_offset = Long.parseLong(node);
                    }
                }
            }
        }

        // limit and offset can't have both value and parameter
        if (m_limit != -1) assert m_limitParameterId == -1 : "Parsed value and param. limit.";
        if (m_offset != 0) assert m_offsetParameterId == -1 : "Parsed value and param. offset.";
    }

    private void parseDisplayColumns(VoltXMLElement columnsNode, boolean isDistributed) {
        for (VoltXMLElement child : columnsNode.children) {
            ParsedColInfo col = new ParsedColInfo();
            m_aggregationList.clear();
            col.expression = parseExpressionTree(child);
            if (col.expression instanceof ConstantValueExpression) {
                assert(col.expression.getValueType() != VoltType.NUMERIC);
            }
            assert(col.expression != null);
            if (isDistributed) {
                col.expression = col.expression.replaceAVG();
                updateAvgExpressions();
            }
            ExpressionUtil.finalizeValueTypes(col.expression);

            if (child.name.equals("columnref")) {
                col.columnName = child.attributes.get("column");
                col.tableName = child.attributes.get("table");
                col.tableAlias = child.attributes.get("tablealias");
                if (col.tableAlias == null) {
                    col.tableAlias = col.tableName;
                }
            }
            else
            {
                // XXX hacky, assume all non-column refs come from a temp table
                col.tableName = "VOLT_TEMP_TABLE";
                col.tableAlias = "VOLT_TEMP_TABLE";
                col.columnName = "";
            }
            col.alias = child.attributes.get("alias");
            if (col.alias == null) {
                col.alias = col.columnName;
            }
           // This index calculation is only used for sanity checking
            // materialized views (which use the parsed select statement but
            // don't go through the planner pass that does more involved
            // column index resolution).
            col.index = m_displayColumns.size();

            insertAggExpressionsToAggResultColumns(m_aggregationList, col);
            if (m_aggregationList.size() >= 1) {
                m_hasAggregateExpression = true;
            }

            m_displayColumns.add(col);
        }
    }

    private void parseGroupByColumns(VoltXMLElement columnsNode) {
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

        if (groupByNode.name.equals("columnref"))
        {
            groupbyCol.alias = groupByNode.attributes.get("alias");
            groupbyCol.columnName = groupByNode.attributes.get("column");
            groupbyCol.tableName = groupByNode.attributes.get("table");
            groupbyCol.tableAlias = groupByNode.attributes.get("tablealias");
            if (groupbyCol.tableAlias == null) {
                groupbyCol.tableAlias = groupbyCol.tableName;
            }

            // This col.index set up is only useful for Materialized view.
            Table tb = getTableFromDB(groupbyCol.tableName);
            if (tb != null) {
                org.voltdb.catalog.Column catalogColumn =
                        tb.getColumns().getExact(groupbyCol.columnName);
                groupbyCol.index = catalogColumn.getIndex();

                Column partitionColumn = tb.getPartitioncolumn();
                if (partitionColumn != null &&
                    partitionColumn.getTypeName().equals(groupbyCol.columnName)) {
                    m_hasPartitionColumnInGroupby = true;
                }
            }
        }
        else
        {
            // TODO(XIN): throw a error for Materialized view when possible.
            // XXX hacky, assume all non-column refs come from a temp table
            groupbyCol.tableName = "VOLT_TEMP_TABLE";
            groupbyCol.tableAlias = "VOLT_TEMP_TABLE";
            groupbyCol.columnName = "";
            m_hasComplexGroupby = true;

            ParsedColInfo orig_col = null;
            for (int i = 0; i < m_displayColumns.size(); ++i) {
                ParsedColInfo col = m_displayColumns.get(i);
                if (col.expression.equals(groupbyCol.expression)) {
                    groupbyCol.alias = col.alias;
                    orig_col = col;
                    break;
                }
            }
            if (orig_col != null) {
                orig_col.groupBy = true;
            }
        }
        m_groupByColumns.add(groupbyCol);
    }

    private void parseOrderColumns(VoltXMLElement columnsNode, boolean isDistributed) {
        for (VoltXMLElement child : columnsNode.children) {
            parseOrderColumn(child, isDistributed);
        }
    }

    private void parseOrderColumn(VoltXMLElement orderByNode, boolean isDistributed) {
        // make sure everything is kosher
        assert(orderByNode.name.equalsIgnoreCase("orderby"));

        // get desc/asc
        String desc = orderByNode.attributes.get("desc");
        boolean descending = (desc != null) && (desc.equalsIgnoreCase("true"));

        // get the columnref or other expression inside the orderby node
        VoltXMLElement child = orderByNode.children.get(0);
        assert(child != null);

        // create the orderby column
        ParsedColInfo order_col = new ParsedColInfo();
        order_col.orderBy = true;
        order_col.ascending = !descending;
        m_aggregationList.clear();
        AbstractExpression order_exp = parseExpressionTree(child);
        assert(order_exp != null);
        if (isDistributed) {
            order_exp = order_exp.replaceAVG();
            updateAvgExpressions();
        }
        order_col.expression = order_exp;
        ExpressionUtil.finalizeValueTypes(order_col.expression);

        // Cases:
        // child could be columnref, in which case it's just a normal column.
        // Just make a ParsedColInfo object for it and the planner will do the right thing later
        if (child.name.equals("columnref")) {
            assert(order_exp instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) order_exp;
            order_col.columnName = tve.getColumnName();
            order_col.tableName = tve.getTableName();
            order_col.tableAlias = tve.getTableAlias();
            if (order_col.tableAlias == null) {
                order_col.tableAlias = order_col.tableName;
            }

            order_col.alias = tve.getColumnAlias();
        } else {
            String alias = child.attributes.get("alias");
            order_col.alias = alias;
            order_col.tableName = "VOLT_TEMP_TABLE";
            order_col.tableAlias = "VOLT_TEMP_TABLE";
            order_col.columnName = "";
            // Replace its expression to TVE after we build the ExpressionIndexMap

            if ((child.name.equals("operation") == false) &&
                    (child.name.equals("aggregation") == false) &&
                    (child.name.equals("function") == false)) {
               throw new RuntimeException("ORDER BY parsed with strange child node type: " + child.name);
           }
        }

        // Mark the order by column if it is in displayColumns
        // The ORDER BY column MAY be identical to a simple display column, in which case,
        // tagging the actual display column as being also an order by column
        // helps later when trying to determine ORDER BY coverage (for determinism).
        ParsedColInfo orig_col = null;
        for (ParsedColInfo col : m_displayColumns) {
            if (col.alias.equals(order_col.alias) || col.expression.equals(order_exp)) {
                orig_col = col;
                break;
            }
        }
        if (orig_col != null) {
            orig_col.orderBy = true;
            orig_col.ascending = order_col.ascending;

            order_col.alias = orig_col.alias;
            order_col.columnName = orig_col.columnName;
            order_col.tableName = orig_col.tableName;
        }
        assert( ! (order_exp instanceof ConstantValueExpression));
        assert( ! (order_exp instanceof ParameterValueExpression));

        insertAggExpressionsToAggResultColumns(m_aggregationList, order_col);
        if (m_aggregationList.size() >= 1) {
            m_hasAggregateExpression = true;
        }
        // Add TVEs in ORDER BY statement if we have, stop recursive finding when we have it in AggResultColumns
        List<TupleValueExpression> tveList = new ArrayList<TupleValueExpression>();
        findAllTVEs(order_col.expression, tveList);
        insertTVEsToAggResultColumns(tveList);
        m_orderColumns.add(order_col);
    }

    private void parseHavingExpression(VoltXMLElement havingNode, boolean isDistributed) {
        m_aggregationList.clear();
        assert(havingNode.children.size() == 1);
        m_having = parseExpressionTree(havingNode.children.get(0));
        assert(m_having != null);
        if (isDistributed) {
            m_having = m_having.replaceAVG();
            updateAvgExpressions();
        }
        ExpressionUtil.finalizeValueTypes(m_having);
        if (m_aggregationList.size() >= 1) {
            m_hasAggregateExpression = true;
        }

        for (AbstractExpression expr: m_aggregationList) {
            ParsedColInfo col = new ParsedColInfo();
            col.expression = (AbstractExpression) expr.clone();
            assert(col.expression instanceof AggregateExpression);
            if (col.expression.getExpressionType() == ExpressionType.AGGREGATE_AVG) {
                m_hasAverage = true;
            }

            col.tableName = "VOLT_TEMP_TABLE";
            col.tableAlias = "VOLT_TEMP_TABLE";
            col.columnName = "";

            if (!m_aggResultColumns.contains(col)) {
                // Try to check complexAggs earlier
                m_hasComplexAgg = true;
                ExpressionUtil.finalizeValueTypes(col.expression);
                m_aggResultColumns.add(col);
            }
        }
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval += "LIMIT " + String.valueOf(m_limit) + "\n";
        retval += "OFFSET " + String.valueOf(m_offset) + "\n";

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

    public boolean hasAggregateExpression () {
        return m_hasAggregateExpression;
    }

    public NodeSchema getFinalProjectionSchema () {
        return m_projectSchema;
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

    public boolean hasOrderByColumns() {
        return ! m_orderColumns.isEmpty();
    }

    public boolean hasDistinct() {
        return m_distinct;
    }

    public List<ParsedColInfo> displayColumns() {
        return Collections.unmodifiableList(m_displayColumns);
    }

    public List<ParsedColInfo> groupByColumns() {
        return Collections.unmodifiableList(m_groupByColumns);
    }

    public List<ParsedColInfo> orderByColumns() {
        return Collections.unmodifiableList(m_orderColumns);
    }

    @Override
    public boolean hasLimitOrOffset() {
        if ((m_limit != -1) || (m_limitParameterId != -1) ||
            (m_offset > 0) || (m_offsetParameterId != -1)) {
            return true;
        }
        return false;
    }

    public boolean hasLimitOrOffsetParameters() {
        return m_limitParameterId != -1 || m_offsetParameterId != -1;
    }

    /// This is for use with integer-valued row count parameters, namely LIMITs and OFFSETs.
    /// It should be called (at least) once for each LIMIT or OFFSET parameter to establish that
    /// the parameter is being used in a BIGINT context.
    /// There may be limitations elsewhere that restrict limits and offsets to 31-bit unsigned values,
    /// but enforcing that at parameter passing/checking time seems a little arbitrary, so we keep
    /// the parameters at maximum width -- a 63-bit unsigned BIGINT.
    private int parameterCountIndexById(long paramId) {
        if (paramId == -1) {
            return -1;
        }
        assert(m_paramsById.containsKey(paramId));
        ParameterValueExpression pve = m_paramsById.get(paramId);
        // As a side effect, re-establish these parameters as integer-typed
        // -- this helps to catch type errors earlier in the invocation process
        // and prevents a more serious error in HSQLBackend statement reconstruction.
        // The HSQL parser originally had these correctly pegged as BIGINTs,
        // but the VoltDB code ( @see AbstractParsedStmt#parseParameters )
        // skeptically second-guesses that pending its own verification. This case is now verified.
        pve.refineValueType(VoltType.BIGINT, VoltType.BIGINT.getLengthInBytesForFixedTypes());
        return pve.getParameterIndex();
    }

    public int getLimitParameterIndex() {
        return parameterCountIndexById(m_limitParameterId);
    }

    public int getOffsetParameterIndex() {
        return parameterCountIndexById(m_offsetParameterId);
    }

    private AbstractExpression getParameterOrConstantAsExpression(long id, long value) {
        // The id was previously passed to parameterCountIndexById, so if not -1,
        // it has already been asserted to be a valid id for a parameter, and the
        // parameter's type has been refined to INTEGER.
        if (id != -1) {
            return m_paramsById.get(id);
        }
        // The limit/offset is a non-parameterized literal value that needs to be wrapped in a
        // BIGINT constant so it can be used in the addition expression for the pushed-down limit.
        ConstantValueExpression constant = new ConstantValueExpression();
        constant.setValue(Long.toString(value));
        constant.refineValueType(VoltType.BIGINT, VoltType.BIGINT.getLengthInBytesForFixedTypes());
        return constant;
    }

    public AbstractExpression getLimitExpression() {
        return getParameterOrConstantAsExpression(m_limitParameterId, m_limit);
    }

    public AbstractExpression getOffsetExpression() {
        return getParameterOrConstantAsExpression(m_offsetParameterId, m_offset);
    }

    @Override
    public boolean isOrderDeterministic()
    {
        if ( ! hasTopLevelScans()) {
            // This currently applies to parent queries that do all their scanning in subqueries and so
            // take on the order determinism of their subqueries. This might have to be rethought to allow
            // ordering in parent queries to effect determinism of unordered "FROM CLAUSE" subquery results.
            return true;
        }
        if (hasAOneRowResult()) {
            return true;
        }
        if ( ! hasOrderByColumns() ) {
            return false;
        }

        // The nonOrdered expression list is used as a short-cut -- if an expression has been
        // determined to be non-ordered when encountered as a GROUP BY expression,
        // it will also be non-ordered when encountered in the select list.
        ArrayList<AbstractExpression> nonOrdered = new ArrayList<AbstractExpression>();

        if (isGrouped()) {
            // Does the ordering of a statements's GROUP BY columns ensure determinism?
            // All display columns and order-by expressions are functionally dependent on the GROUP BY
            // columns even if the display column's values are not ordered or unique,
            // so ordering by ALL of the GROUP BY columns is enough to get full determinism,
            // EVEN if ordering by other (dependent) expressions,
            // regardless of the placement of non-GROUP BY expressions in the ORDER BY list.
            if (orderByColumnsDetermineAllColumns(m_groupByColumns, nonOrdered)) {
                return true;
            }
            if (orderByColumnsDetermineAllDisplayColumns(nonOrdered)) {
                return true;
            }
        } else {
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
    public boolean isOrderDeterministicInSpiteOfUnorderedSubqueries()
    {
        if (hasAOneRowResult()) {
            return true;
        }
        if ( ! hasOrderByColumns() ) {
            return false;
        }

        // This is a trivial empty container.
        // In other code paths, it would list expressions that have been pre-determined to be nonOrdered.
        ArrayList<AbstractExpression> nonOrdered = new ArrayList<AbstractExpression>();

        if (orderByColumnsDetermineAllDisplayColumns(nonOrdered)) {
            return true;
        }
        return false;
    }

    private boolean orderByColumnsCoverUniqueKeys()
    {
        // In theory, if EVERY table in the query has a uniqueness constraint
        // (primary key or other unique index) on columns that are all listed in the ORDER BY values,
        // the result is deterministic.
        // This holds regardless of whether the associated index is actually used in the selected plan,
        // so this check is plan-independent.
        HashMap<String, List<AbstractExpression> > baseTableAliases =
                new HashMap<String, List<AbstractExpression> >();
        for (ParsedColInfo col : m_orderColumns) {
            AbstractExpression expr = col.expression;
            List<AbstractExpression> baseTVEs = expr.findBaseTVEs();
            if (baseTVEs.size() != 1) {
                // Table-spanning ORDER BYs -- like ORDER BY A.X + B.Y are not helpful.
                // Neither are (nonsense) constant (table-less) expressions.
                continue;
            }
            // This loops exactly once.
            AbstractExpression baseTVE = baseTVEs.get(0);
            String nextTableAlias = ((TupleValueExpression)baseTVE).getTableAlias();
            assert(nextTableAlias != null);
            List<AbstractExpression> perTable = baseTableAliases.get(nextTableAlias);
            if (perTable == null) {
                perTable = new ArrayList<AbstractExpression>();
                baseTableAliases.put(nextTableAlias, perTable);
            }
            perTable.add(expr);
        }

        if (m_tableAliasMap.size() > baseTableAliases.size()) {
            // FIXME: This would be one of the tricky cases where the goal would be to prove that the
            // row with no ORDER BY component came from the right side of a 1-to-1 or many-to-1 join.
            return false;
        }
        boolean allScansAreDeterministic = true;
        for (Entry<String, List<AbstractExpression>> orderedAlias : baseTableAliases.entrySet()) {
            List<AbstractExpression> orderedAliasExprs = orderedAlias.getValue();
            StmtTableScan tableScan = m_tableAliasMap.get(orderedAlias.getKey());
            if (tableScan == null) {
                assert(false);
                return false;
            }

            if (tableScan instanceof StmtSubqueryScan) {
                return false; // don't yet handle FROM clause subquery, here.
            }

            Table table = ((StmtTargetTableScan)tableScan).getTargetTable();

            // This table's scans need to be proven deterministic.
            allScansAreDeterministic = false;
            // Search indexes for one that makes the order by deterministic
            for (Index index : table.getIndexes()) {
                // skip non-unique indexes
                if ( ! index.getUnique()) {
                    continue;
                }

                // get the list of expressions for the index
                List<AbstractExpression> indexExpressions = new ArrayList<AbstractExpression>();

                String jsonExpr = index.getExpressionsjson();
                // if this is a pure-column index...
                if (jsonExpr.isEmpty()) {
                    for (ColumnRef cref : index.getColumns()) {
                        Column col = cref.getColumn();
                        TupleValueExpression tve = new TupleValueExpression(table.getTypeName(),
                                                                            orderedAlias.getKey(),
                                                                            col.getName(),
                                                                            col.getName(),
                                                                            col.getIndex());
                        indexExpressions.add(tve);
                    }
                }
                // if this is a fancy expression-based index...
                else {
                    try {
                        indexExpressions = AbstractExpression.fromJSONArrayString(jsonExpr, tableScan);
                    } catch (JSONException e) {
                        e.printStackTrace();
                        assert(false);
                        continue;
                    }
                }

                // If the sort covers the index, then it's a unique sort.
                //TODO: The statement's equivalence sets would be handy here to recognize cases like
                //    WHERE B.unique_id = A.b_id
                //    ORDER BY A.unique_id, A.b_id
                if (orderedAliasExprs.containsAll(indexExpressions)) {
                    allScansAreDeterministic = true;
                    break;
                }
            }
            // ALL tables' scans need to have proved deterministic
            if ( ! allScansAreDeterministic) {
                return false;
            }
        }
        return true;
    }

    private boolean orderByColumnsDetermineAllDisplayColumns(ArrayList<AbstractExpression> nonOrdered)
    {
        ArrayList<ParsedColInfo> candidateColumns = new ArrayList<ParsedSelectStmt.ParsedColInfo>();
        for (ParsedColInfo displayCol : m_displayColumns) {
            if (displayCol.orderBy) {
                continue;
            }
            if (displayCol.groupBy) {
                AbstractExpression displayExpr = displayCol.expression;
                // Round up the usual suspects -- if there were uncooperative GROUP BY expressions,
                // they will often also be uncooperative display column expressions.
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
        return orderByColumnsDetermineAllColumns(candidateColumns, null);
    }

    private boolean orderByColumnsDetermineAllColumns(ArrayList<ParsedColInfo> candidateColumns,
                                                      ArrayList<AbstractExpression> outNonOrdered) {
        HashSet<AbstractExpression> orderByExprs = null;
        ArrayList<AbstractExpression> candidateExprHardCases = null;
        // First try to get away with a brute force N by M search for exact equalities.
        for (ParsedColInfo candidateCol : candidateColumns)
        {
            if (candidateCol.orderBy) {
                continue;
            }
            AbstractExpression candidateExpr = candidateCol.expression;
            if (orderByExprs == null) {
                orderByExprs = new HashSet<AbstractExpression>();
                for (ParsedColInfo orderByCol : m_orderColumns) {
                    orderByExprs.add(orderByCol.expression);
                }
            }
            if (orderByExprs.contains(candidateExpr)) {
                continue;
            }
            if (candidateExpr instanceof TupleValueExpression) {
                // Simple column references can only be exactly equal to but not "based on" an ORDER BY.
                return false;
            }

            if (candidateExprHardCases == null) {
                candidateExprHardCases = new ArrayList<AbstractExpression>();
            }
            candidateExprHardCases.add(candidateExpr);
        }

        if (candidateExprHardCases == null) {
            return true;
        }

        // Plan B. profile the ORDER BY list and try to include/exclude the hard cases on that basis.
        HashSet<AbstractExpression> orderByTVEs = new HashSet<AbstractExpression>();
        ArrayList<AbstractExpression> orderByNonTVEs = new ArrayList<AbstractExpression>();
        ArrayList<List<AbstractExpression>> orderByNonTVEBaseTVEs = new ArrayList<List<AbstractExpression>>();
        HashSet<AbstractExpression> orderByAllBaseTVEs = new HashSet<AbstractExpression>();

        for (AbstractExpression orderByExpr : orderByExprs) {
            if (orderByExpr instanceof TupleValueExpression) {
                orderByTVEs.add(orderByExpr);
                orderByAllBaseTVEs.add(orderByExpr);
            } else {
                orderByNonTVEs.add(orderByExpr);
                List<AbstractExpression> baseTVEs = orderByExpr.findBaseTVEs();
                orderByNonTVEBaseTVEs.add(baseTVEs);
                orderByAllBaseTVEs.addAll(baseTVEs);
            }
        }

        boolean result = true;

        for (AbstractExpression candidateExpr : candidateExprHardCases)
        {
            Collection<AbstractExpression> candidateBases = candidateExpr.findBaseTVEs();
            if (orderByTVEs.containsAll(candidateBases)) {
                continue;
            }
            if (orderByAllBaseTVEs.containsAll(candidateBases) == false) {
                if (outNonOrdered == null) {
                    // Short-circuit if the remaining non-qualifying expressions are not of interest.
                    return false;
                }
                result = false;
                outNonOrdered.add(candidateExpr);
                continue;
            }
            // At this point, if the candidateExpr is a match,
            // then it is based on but not equal to one or more orderByNonTVE(s) and optionally orderByTVE(s).
            // The simplest example is like "SELECT a+(b-c) ... ORDER BY a, b-c;"
            // If it is a non-match, it is an original expression based on orderByAllBaseTVEs
            // The simplest example is like "SELECT a+b ... ORDER BY a, b-c;"
            // TODO: process REALLY HARD CASES
            // TODO: issue a warning, short-term?
            // For now, err on the side of non-determinism.
            if (outNonOrdered == null) {
                // Short-circuit if the remaining non-qualifying expressions are not of interest.
                return false;
            }
            outNonOrdered.add(candidateExpr);
            result = false;
        }
        return result;
    }

    private boolean hasAOneRowResult()
    {
        if ( ( ! isGrouped() ) && displaysAgg()) {
            return true;
        }
        return false;
    }

    private boolean hasTopLevelScans() {
        for (StmtTableScan scan : m_tableAliasMap.values()) {
            if (scan instanceof StmtTargetTableScan) {
                return true;
            }
        }
        return false;
    }

    private boolean displaysAgg() {
        for (ParsedColInfo displayCol : m_displayColumns) {
            if (displayCol.expression.hasAnySubexpressionOfClass(AggregateExpression.class)) {
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
            } else if (display_col.tableName.equals(sc.getTableName())) {
                sameTable = true;
            }
            if (sameTable) {
                if (display_col.alias != null && ! display_col.alias.equals("")) {
                    if (display_col.alias.equals(sc.getColumnAlias())) {
                        continue;
                    }
                }
                else if (display_col.columnName != null && ! display_col.columnName.equals("")) {
                    if (display_col.columnName.equals(sc.getColumnName())) {
                        continue;
                    }
                }
            }
            throw new PlanningErrorException(
                    "Mismatched plan output cols to parsed display columns");
        }
    }
}
