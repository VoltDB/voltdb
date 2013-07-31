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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.NodeSchema;
import org.voltdb.plannodes.SchemaColumn;

public class ParsedSelectStmt extends AbstractParsedStmt {

    public static class ParsedColInfo {
        public String alias = null;
        public String columnName = null;
        public String tableName = null;
        public AbstractExpression expression = null;
        public boolean finalOutput = true;
        public int index = 0;
        public int size = 0;

        // orderby stuff
        public boolean orderBy = false;
        public boolean ascending = true;

        // groupby
        public boolean groupBy = false;

        public boolean simpleEquals (Object obj) {
            if (obj == null) return false;
            if (obj instanceof ParsedColInfo == false) return false;
            ParsedColInfo col = (ParsedColInfo) obj;
            if ( columnName != null && columnName.equals(col.columnName)
                    && tableName != null && tableName.equals(col.tableName) &&
                    expression != null && expression.equals(col.expression) )
                return true;
            return false;
        }

        @Override
        public boolean equals (Object obj) {
            if (obj == null) return false;
            if (obj instanceof ParsedColInfo == false) return false;
            ParsedColInfo col = (ParsedColInfo) obj;
            if ( alias != null && alias.equals(col.alias )
                    && columnName != null && columnName.equals(col.columnName)
                    && tableName != null && tableName.equals(col.tableName)
                    && expression.equals(col.expression) && index == col.index && size == col.size
                    && orderBy == col.orderBy && ascending == col.ascending && groupBy == col.groupBy
                    && finalOutput == col.finalOutput) {
                return true;
            }
            return false;
        }

        @Override
        public int hashCode() {
            int result = 0;
            if (expression != null)
                result += expression.hashCode();
            // calculate hash for other member variable
            result = new HashCodeBuilder(17, 31).
                    append(alias).append(columnName).append(tableName).
                    //append(index).append(size).
                    //append(finalOutput).append(orderBy).append(ascending).append(groupBy).
                    toHashCode();
            return result;
        }

        @Override
        public Object clone() {
            ParsedColInfo col = new ParsedColInfo();
            col.expression = (AbstractExpression) expression.clone();
            col.tableName = tableName;
            col.columnName = columnName;
            col.alias = alias;
            return col;
        }
    }

    public ArrayList<ParsedColInfo> displayColumns = new ArrayList<ParsedColInfo>();
    public ArrayList<ParsedColInfo> orderColumns = new ArrayList<ParsedColInfo>();
    public AbstractExpression having = null;
    public ArrayList<ParsedColInfo> groupByColumns = new ArrayList<ParsedColInfo>();

    // It may has the consistent element order as the displayColumns
    public ArrayList<ParsedColInfo> aggResultColumns = new ArrayList<ParsedColInfo>();
    public Map<String, AbstractExpression> groupByExpressions = null;
    private NodeSchema newAggSchema;

    public long limit = -1;
    public long offset = 0;
    private long limitParameterId = -1;
    private long offsetParameterId = -1;
    public boolean distinct = false;
    private boolean hasComplexAgg = false;
    private boolean hasComplexGroupby = false;
    private boolean hasAggregateExpression = false;

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

        if ((node = stmtNode.attributes.get("limit")) != null)
            limit = Long.parseLong(node);
        if ((node = stmtNode.attributes.get("offset")) != null)
            offset = Long.parseLong(node);
        if ((node = stmtNode.attributes.get("limit_paramid")) != null)
            limitParameterId = Long.parseLong(node);
        if ((node = stmtNode.attributes.get("offset_paramid")) != null)
            offsetParameterId = Long.parseLong(node);
        if ((node = stmtNode.attributes.get("distinct")) != null)
            distinct = Boolean.parseBoolean(node);

        // limit and offset can't have both value and parameter
        if (limit != -1) assert limitParameterId == -1 : "Parsed value and param. limit.";
        if (offset != 0) assert offsetParameterId == -1 : "Parsed value and param. offset.";

        VoltXMLElement displayElement = null, orderbyElement = null, groupbyElement = null;
        for (VoltXMLElement child : stmtNode.children) {
            if (child.name.equalsIgnoreCase("columns")) {
                displayElement = child;
            } else if (child.name.equalsIgnoreCase("ordercolumns")) {
                orderbyElement = child;
            } else if (child.name.equalsIgnoreCase("groupcolumns")) {
                groupbyElement = child;
            }
        }
        if (aggregationList == null) {
            aggregationList = new ArrayList<AbstractExpression>();
        }
        // We want to extract display first, groupBy second before processing orderBy
        // Because groupBy and orderBy need display columns to tag its columns
        // OrderBy needs groupBy columns to stop recursively finding TVEs for pass-through columns.
        if (displayElement != null) {
            parseDisplayColumns(displayElement);
        }
        if (groupbyElement != null) {
            parseGroupByColumns(groupbyElement);
            insertToAggResultColumns(groupByColumns);
        }

        if (orderbyElement != null) {
            parseOrderColumns(orderbyElement);
        }

        // We do not need aggregationList container in parseXMLtree
        // Make it null to prevent others adding elements to it when parsing the tree
        aggregationList = null;

        if (needComplexAggregation()) {
            fillUpAggResultColumns();
        } else {
            aggResultColumns = displayColumns;
        }
        placeTVEsinColumns();
    }

    private boolean needComplexAggregation () {
        if (!hasAggregateExpression && !isGrouped()) {
            hasComplexAgg = false;
            return false;
        }
        if (hasComplexAgg()) return true;

        int numDisplayCols = displayColumns.size();
        if (aggResultColumns.size() > numDisplayCols) {
            hasComplexAgg = true;
            return true;
        }

        for (ParsedColInfo col : displayColumns) {
            if (isNewtoAggResultColumn(col)) {
                // Now Only TVEs in displayColumns are left for AggResultColumns
                if (col.expression instanceof TupleValueExpression) {
                    aggResultColumns.add(col);
                } else {
                    // Col must be complex expression (like: TVE + 1, TVE + AGG)
                    hasComplexAgg = true;
                    return true;
                }
            }
        }
        if (aggResultColumns.size() != numDisplayCols) {
            // Display columns have duplicated Aggs or TVEs (less than case)
            // Display columns have several pass-through columns if group by primary key (larger than case)
            hasComplexAgg = true;
            return true;
        }

        return false;
    }
    /**
     * Continue adding TVEs from DisplayColumns that are left in function needComplexAggregation().
     * After this function, aggResultColumns construction work.
     */
    private void fillUpAggResultColumns () {
        for (ParsedColInfo col: displayColumns) {
            if (isNewtoAggResultColumn(col)) {
                if (col.expression instanceof TupleValueExpression) {
                    aggResultColumns.add(col);
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
        for (ParsedColInfo col: aggResultColumns) {
            aggTableIndexMap.put(col.expression, index);
            if ( col.alias == null) {
                // hack any unique string
                col.alias = "$$_" + col.expression.getExpressionType().symbol() + "_$$_" + index;
            }
            indexToColumnMap.put(index, col);
            index++;
        }

        // Replace TVE for display columns
        if (hasComplexAgg()) {
            newAggSchema = new NodeSchema();
            for (ParsedColInfo col : displayColumns) {
                AbstractExpression expr = col.expression.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
                SchemaColumn schema_col = new SchemaColumn(col.tableName, col.columnName, col.alias, expr);
                newAggSchema.addColumn(schema_col);
            }
        }

        // Replace TVE for order by columns
        for (ParsedColInfo orderCol : orderColumns) {
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
        groupByExpressions = new HashMap<String, AbstractExpression>();
        for (ParsedColInfo groupbyCol: groupByColumns) {
            assert(aggTableIndexMap.get(groupbyCol.expression) != null);
            assert(groupByExpressions.get(groupbyCol.alias) == null);
            AbstractExpression expr = groupbyCol.expression.replaceWithTVE(aggTableIndexMap, indexToColumnMap);
            groupByExpressions.put(groupbyCol.alias,expr);
        }
    }

    private boolean isNewtoAggResultColumn(ParsedColInfo col) {
        boolean isNew = true;
        for (ParsedColInfo ic: aggResultColumns) {
            if (ic.simpleEquals(col)) {
                isNew = false;
                break;
            }
        }
        return isNew;
    }

    private boolean isNewtoAggResultColumn(AbstractExpression expr) {
        boolean isNew = true;
        for (ParsedColInfo ic: aggResultColumns) {
            if (ic.expression.equals(expr)) {
                isNew = false;
                break;
            }
        }
        return isNew;
    }

    // Concat elements to the aggResultColumns
    private void insertToAggResultColumns (List<ParsedColInfo> colCollection) {
        for (ParsedColInfo col: colCollection) {
            if (isNewtoAggResultColumn(col)) {
                aggResultColumns.add(col);
            }
        }
    }

    /**
     * ParseDisplayColumns and ParseOrderColumns will call this function to add Aggregation expressions to aggResultColumns
     * @param colCollection
     * @param cookedCol
     */
    private void insertAggExpressionsToAggResultColumns (List<AbstractExpression> colCollection, ParsedColInfo cookedCol) {
        // Why IF ELSE, because AggResultColumn care about its alias,tableName
        if (colCollection.size() == 1 && cookedCol.expression.equals(colCollection.get(0))) {
            ParsedColInfo col = new ParsedColInfo();
            col.expression = (AbstractExpression) colCollection.get(0).clone();
            col.alias = cookedCol.alias;
            col.tableName = cookedCol.tableName;
            col.columnName = cookedCol.columnName;
            if (isNewtoAggResultColumn(col)) {
                aggResultColumns.add(col);
            }
        } else if (colCollection.size() != 0) {
            // Try to check complexAggs earlier
            hasComplexAgg = true;
            for (AbstractExpression expr: colCollection) {
                ParsedColInfo col = new ParsedColInfo();
                col.expression = (AbstractExpression) expr.clone();
                ExpressionUtil.finalizeValueTypes(col.expression);
                // Aggregation column use the the hacky stuff
                col.tableName = "VOLT_TEMP_TABLE";
                col.columnName = "";
                if (isNewtoAggResultColumn(col)) {
                    aggResultColumns.add(col);
                }
            }
        }
    }

    private void insertTVEsToAggResultColumns (List<TupleValueExpression> colCollection) {
        // TVEs do not need to take care
        for (TupleValueExpression tve: colCollection) {
            ParsedColInfo col = new ParsedColInfo();
            col.alias = tve.getColumnAlias();
            col.columnName = tve.getColumnName();
            col.tableName = tve.getTableName();
            col.expression = tve;
            if (isNewtoAggResultColumn(col)) {
                aggResultColumns.add(col);
            }
        }
    }

    /**
     * Find all TVEs except inside of AggregationExpression
     * @param expr
     * @param tveList
     */
    private void findAllTVEs(AbstractExpression expr, List<TupleValueExpression> tveList) {
        if (!isNewtoAggResultColumn(expr))
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

    private void parseDisplayColumns(VoltXMLElement columnsNode) {
        for (VoltXMLElement child : columnsNode.children) {
            ParsedColInfo col = new ParsedColInfo();
            aggregationList.clear();
            col.expression = parseExpressionTree(child);
            if (col.expression instanceof ConstantValueExpression) {
                assert(col.expression.getValueType() != VoltType.NUMERIC);
            }
            ExpressionUtil.finalizeValueTypes(col.expression);
            assert(col.expression != null);
            col.alias = child.attributes.get("alias");

            if (child.name.equals("columnref")) {
                col.columnName =
                    child.attributes.get("column");
                col.tableName =
                    child.attributes.get("table");
            }
            else
            {
                // XXX hacky, assume all non-column refs come from a temp table
                col.tableName = "VOLT_TEMP_TABLE";
                col.columnName = "";
            }
            // This index calculation is only used for sanity checking
            // materialized views (which use the parsed select statement but
            // don't go through the planner pass that does more involved
            // column index resolution).
            col.index = displayColumns.size();

            insertAggExpressionsToAggResultColumns(aggregationList, col);
            if (aggregationList.size() >= 1) {
                hasAggregateExpression = true;
            }
            displayColumns.add(col);
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

            // This col.index set up is only useful for Materialized view.
            org.voltdb.catalog.Column catalogColumn =
                    getTableFromDB(groupbyCol.tableName).getColumns().getIgnoreCase(groupbyCol.columnName);
            groupbyCol.index = catalogColumn.getIndex();
        }
        else
        {
            // TODO(XIN): throw a error for Materialized view when possible.
            // XXX hacky, assume all non-column refs come from a temp table
            groupbyCol.tableName = "VOLT_TEMP_TABLE";
            groupbyCol.columnName = "";
            hasComplexGroupby = true;

            ParsedColInfo orig_col = null;
            for (int i = 0; i < displayColumns.size(); ++i) {
                ParsedColInfo col = displayColumns.get(i);
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
        groupByColumns.add(groupbyCol);
    }

    private void parseOrderColumns(VoltXMLElement columnsNode) {
        for (VoltXMLElement child : columnsNode.children) {
            parseOrderColumn(child);
        }
    }

    private void parseOrderColumn(VoltXMLElement orderByNode) {
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
        aggregationList.clear();
        AbstractExpression order_exp = parseExpressionTree(child);

        // Cases:
        // child could be columnref, in which case it's just a normal column.
        // Just make a ParsedColInfo object for it and the planner will do the right thing later
        if (child.name.equals("columnref")) {
            assert(order_exp instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) order_exp;
            order_col.columnName = tve.getColumnName();
            order_col.tableName = tve.getTableName();
            order_col.alias = tve.getColumnAlias();
        } else {
            String alias = child.attributes.get("alias");
            order_col.alias = alias;
            order_col.tableName = "VOLT_TEMP_TABLE";
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
        for (ParsedColInfo col : displayColumns) {
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

        ExpressionUtil.finalizeValueTypes(order_exp);
        order_col.expression = order_exp;

        insertAggExpressionsToAggResultColumns(aggregationList, order_col);
        if (aggregationList.size() >= 1) {
            hasAggregateExpression = true;
        }
        // Add TVEs in ORDER BY statement if we have, stop recursive finding when we have it in AggResultColumns
        List<TupleValueExpression> tveList = new ArrayList<TupleValueExpression>();
        findAllTVEs(order_col.expression, tveList);
        insertTVEsToAggResultColumns(tveList);
        orderColumns.add(order_col);
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval += "LIMIT " + String.valueOf(limit) + "\n";
        retval += "OFFSET " + String.valueOf(limit) + "\n";

        retval += "DISPLAY COLUMNS:\n";
        for (ParsedColInfo col : displayColumns) {
            retval += "\tColumn: " + col.alias + ": ";
            retval += col.expression.toString() + "\n";
        }

        retval += "ORDER COLUMNS:\n";
        for (ParsedColInfo col : orderColumns) {
            retval += "\tColumn: " + col.alias + ": ASC?: " + col.ascending + ": ";
            retval += col.expression.toString() + "\n";
        }

        retval += "GROUP_BY COLUMNS:\n";
        for (ParsedColInfo col : groupByColumns) {
            retval += "\tColumn: " + col.alias + ": ";
            retval += col.expression.toString() + "\n";
        }

        retval = retval.trim();

        return retval;
    }

    public boolean hasAggregateExpression () {
        return hasAggregateExpression;
    }

    public NodeSchema getNewSchema () {
        return newAggSchema;
    }

    public boolean hasComplexGroupby() {
        return hasComplexGroupby;
    }

    public boolean hasComplexAgg() {
        return hasComplexAgg;
    }

    public boolean hasOrderByColumns() {
        return ! orderColumns.isEmpty();
    }

    public List<ParsedColInfo> displayColumns() {
        return Collections.unmodifiableList(displayColumns);
    }

    public List<ParsedColInfo> groupByColumns() {
        return Collections.unmodifiableList(groupByColumns);
    }

    public List<ParsedColInfo> orderByColumns() {
        return Collections.unmodifiableList(orderColumns);
    }

    public boolean hasLimitOrOffset() {
        if ((limit != -1) || (limitParameterId != -1) ||
            (offset > 0) || (offsetParameterId != -1)) {
            return true;
        }
        return false;
    }

    public boolean hasLimitOrOffsetParameters() {
        return limitParameterId != -1 || offsetParameterId != -1;
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
        return parameterCountIndexById(limitParameterId);
    }

    public int getOffsetParameterIndex() {
        return parameterCountIndexById(offsetParameterId);
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
        return getParameterOrConstantAsExpression(limitParameterId, limit);
    }

    public AbstractExpression getOffsetExpression() {
        return getParameterOrConstantAsExpression(offsetParameterId, offset);
    }

    public boolean isOrderDeterministic() {
        if (guaranteesUniqueRow()) {
            return true;
        }
        ArrayList<AbstractExpression> nonOrdered = new ArrayList<AbstractExpression>();
        if (orderByColumnsDetermineUniqueColumns(nonOrdered)) {
            return true;
        }
        if (orderByColumnsDetermineAllDisplayColumns(nonOrdered)) {
            return true;
        }
        return false;
    }

    /**
     * Does the ordering of a statements's GROUP BY columns ensure determinism.
     * All display columns are functionally dependent on the GROUP BY columns
     * even if the display column's values are not ordered or unique, so ordering by GROUP BY columns is enough to get determinism.
     * @param outNonOrdered - list of non-matching GROUP BY columns, populated as a side effect
     * @return whether there are GROUP BY columns and they are all order-determined by ORDER BY columns
     */
    private boolean orderByColumnsDetermineUniqueColumns(ArrayList<AbstractExpression> outNonOrdered) {
        if ( ! isGrouped()) {
            // TODO: Are there other ways to determine a unique set of columns without considering every display column?
            return false;
        }
        if (orderByColumnsDetermineAllColumns(groupByColumns, outNonOrdered)) {
            return true;
        }
        return false;
    }

    private boolean orderByColumnsDetermineAllDisplayColumns(ArrayList<AbstractExpression> nonOrdered) {
        ArrayList<ParsedColInfo> candidateColumns = new ArrayList<ParsedSelectStmt.ParsedColInfo>();
        for (ParsedColInfo displayCol : displayColumns) {
            if (displayCol.orderBy) {
                continue;
            }
            if (displayCol.groupBy) {
                AbstractExpression displayExpr = displayCol.expression;
                // Round up the usual suspects -- if there were uncooperative GROUP BY expressions, they will
                // often also be display column expressions.
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
                for (ParsedColInfo orderByCol : orderColumns) {
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

    boolean guaranteesUniqueRow() {
        if ( ( ! isGrouped() ) && displaysAgg()) {
            return true;
        }
        return false;
    }

    private boolean displaysAgg() {
        for (ParsedColInfo displayCol : displayColumns) {
            if (displayCol.expression.hasAnySubexpressionOfClass(AggregateExpression.class)) {
                return true;
            }
        }
        return false;
    }

    public boolean isGrouped() { return ! groupByColumns.isEmpty(); }

    public boolean displayColumnsContainAllGroupByColumns() {
        for (ParsedColInfo groupedCol : groupByColumns) {
            boolean missing = true;
            for (ParsedColInfo displayCol : displayColumns) {
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
}
