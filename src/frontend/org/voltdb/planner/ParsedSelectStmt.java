/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.planner;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleValueExpression;

public class ParsedSelectStmt extends AbstractParsedStmt {

    public static class ParsedColInfo {
        public String alias;
        public String columnName;
        public String tableName;
        public AbstractExpression expression;
        public boolean finalOutput = true;
        public int index = 0;
        public int size;

        // orderby stuff
        public boolean orderBy = false;
        public boolean ascending = true;

        // groupby
        public boolean groupBy = false;
    }

    public ArrayList<ParsedColInfo> displayColumns = new ArrayList<ParsedColInfo>();
    public ArrayList<ParsedColInfo> orderColumns = new ArrayList<ParsedColInfo>();
    public AbstractExpression having = null;
    public ArrayList<ParsedColInfo> groupByColumns = new ArrayList<ParsedColInfo>();

    public long limit = -1;
    public long offset = 0;
    private long limitParameterId = -1;
    private long offsetParameterId = -1;
    public boolean grouped = false;
    public boolean distinct = false;

    @Override
    void parse(VoltXMLElement stmtNode, Database db) {
        String node;

        if ((node = stmtNode.attributes.get("limit")) != null)
            limit = Long.parseLong(node);
        if ((node = stmtNode.attributes.get("offset")) != null)
            offset = Long.parseLong(node);
        if ((node = stmtNode.attributes.get("limit_paramid")) != null)
            limitParameterId = Long.parseLong(node);
        if ((node = stmtNode.attributes.get("offset_paramid")) != null)
            offsetParameterId = Long.parseLong(node);
        if ((node = stmtNode.attributes.get("grouped")) != null)
            grouped = Boolean.parseBoolean(node);
        if ((node = stmtNode.attributes.get("distinct")) != null)
            distinct = Boolean.parseBoolean(node);

        // limit and offset can't have both value and parameter
        if (limit != -1) assert limitParameterId == -1 : "Parsed value and param. limit.";
        if (offset != 0) assert offsetParameterId == -1 : "Parsed value and param. offset.";

        for (VoltXMLElement child : stmtNode.children) {
            if (child.name.equalsIgnoreCase("columns"))
                parseDisplayColumns(child, db);
            else if (child.name.equalsIgnoreCase("querycondition"))
                parseQueryCondition(child, db);
            else if (child.name.equalsIgnoreCase("ordercolumns"))
                parseOrderColumns(child, db);
            else if (child.name.equalsIgnoreCase("groupcolumns")) {
                parseGroupByColumns(child, db);
            }
        }
    }

    void parseDisplayColumns(VoltXMLElement columnsNode, Database db) {
        for (VoltXMLElement child : columnsNode.children) {
            ParsedColInfo col = new ParsedColInfo();
            col.expression = parseExpressionTree(child, db);
            ExpressionUtil.assignLiteralConstantTypesRecursively(col.expression);
            ExpressionUtil.assignOutputValueTypesRecursively(col.expression);
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
            displayColumns.add(col);
        }
    }

    void parseQueryCondition(VoltXMLElement conditionNode, Database db) {
        if (conditionNode.children.size() == 0)
            return;

        VoltXMLElement exprNode = conditionNode.children.get(0);
        where = parseExpressionTree(exprNode, db);
        ExpressionUtil.assignLiteralConstantTypesRecursively(where);
        ExpressionUtil.assignOutputValueTypesRecursively(where);
    }

    void parseGroupByColumns(VoltXMLElement columnsNode, Database db) {
        for (VoltXMLElement child : columnsNode.children) {
            parseGroupByColumn(child, db);
        }
    }

    void parseGroupByColumn(VoltXMLElement groupByNode, Database db) {

        ParsedColInfo col = new ParsedColInfo();
        col.expression = parseExpressionTree(groupByNode, db);
        assert(col.expression != null);

        if (groupByNode.name.equals("columnref"))
        {
            col.alias = groupByNode.attributes.get("alias");
            col.columnName = groupByNode.attributes.get("column");
            col.tableName = groupByNode.attributes.get("table");
            col.groupBy = true;
        }
        else
        {
            throw new RuntimeException("GROUP BY with complex expressions not yet supported");
        }

        assert(col.alias.equalsIgnoreCase(col.columnName));
        assert(db.getTables().getIgnoreCase(col.tableName) != null);
        assert(db.getTables().getIgnoreCase(col.tableName).getColumns().getIgnoreCase(col.columnName) != null);
        org.voltdb.catalog.Column catalogColumn =
            db.getTables().getIgnoreCase(col.tableName).getColumns().getIgnoreCase(col.columnName);
        col.index = catalogColumn.getIndex();
        groupByColumns.add(col);
    }

    void parseOrderColumns(VoltXMLElement columnsNode, Database db) {
        for (VoltXMLElement child : columnsNode.children) {
            parseOrderColumn(child, db);
        }
    }

    void parseOrderColumn(VoltXMLElement orderByNode, Database db) {
        // make sure everything is kosher
        assert(orderByNode.name.equalsIgnoreCase("operation"));
        String operationType = orderByNode.attributes.get("type");
        assert(operationType != null);
        assert(operationType.equalsIgnoreCase("orderby"));

        // get desc/asc
        String desc = orderByNode.attributes.get("desc");
        boolean descending = (desc != null) && (desc.equalsIgnoreCase("true"));

        // get the columnref expression inside the orderby node
        VoltXMLElement child = orderByNode.children.get(0);
        assert(child != null);

        // create the orderby column
        ParsedColInfo order_col = new ParsedColInfo();
        order_col.orderBy = true;
        order_col.ascending = !descending;
        AbstractExpression order_exp = parseExpressionTree(child, db);

        // Cases:
        // inner child could be columnref, in which case it's just a normal
        // column.  Just make a ParsedColInfo object for it and the planner
        // will do the right thing later
        if (child.name.equals("columnref"))
        {
            order_col.columnName = child.attributes.get("column");
            order_col.tableName = child.attributes.get("table");
            String alias = child.attributes.get("alias");
            order_col.alias = alias;

            // The ORDER BY column MAY be identical to a simple display column, in which case,
            // tagging the actual display column as being also an order by column
            // helps later when trying to determine ORDER BY coverage (for determinism).
            if (order_exp instanceof TupleValueExpression)
            {
                ParsedColInfo orig_col = null;
                for (ParsedColInfo col : displayColumns)
                {
                    if (col.expression.equals(order_exp)) {
                        orig_col = col;
                        break;
                    }
                }
                if (orig_col != null) {
                    orig_col.orderBy = true;
                    orig_col.ascending = order_col.ascending;
                }
            }
        }
        else if (child.name.equals("operation"))
        {
            order_col.columnName = "";
            // I'm not sure anyone actually cares about this table name
            order_col.tableName = "VOLT_TEMP_TABLE";

            // If it's a simplecolumn operation.  This means that the alias that
            //    we have should refer to a column that we compute
            //    somewhere else (like an aggregate, mostly).
            //    Look up that column in the displayColumns list,
            //    and then create a new order by column with a magic TVE.
            // This case seems to be the result of cross-referencing a display column
            // by its position, as in "ORDER BY 2, 3". Otherwise the ORDER BY column
            // is a columnref as handled in the prior code block.
            if (order_exp instanceof TupleValueExpression) {
                String alias = child.attributes.get("alias");
                order_col.alias = alias;
                ParsedColInfo orig_col = null;
                for (ParsedColInfo col : displayColumns)
                {
                    if (col.alias.equals(alias))
                    {
                        orig_col = col;
                        break;
                    }
                }
                // We need the original column expression so we can extract
                // the value size and type for our TVE that refers back to it.
                // XXX: This check runs into problems for some cases where a display column expression gets re-used in the ORDER BY.
                // I THINK one problem case was "select x, max(y) from t group by x order by max(y);" --paul
                if (orig_col == null)
                {
                    throw new PlanningErrorException("Unable to find source " +
                                                     "column for simplecolumn: " +
                                                     alias);
                }

                // Tagging the actual display column as being also an order by column
                // helps later when trying to determine ORDER BY coverage (for determinism).
                orig_col.orderBy = true;
                orig_col.ascending = order_col.ascending;

                assert(orig_col.tableName.equals("VOLT_TEMP_TABLE"));
                // Construct our fake TVE that will point back at the input
                // column.
                TupleValueExpression tve = (TupleValueExpression) order_exp;
                tve.setColumnAlias(alias);
                tve.setColumnName("");
                tve.setColumnIndex(-1);
                tve.setTableName("VOLT_TEMP_TABLE");
                tve.setValueSize(orig_col.expression.getValueSize());
                tve.setValueType(orig_col.expression.getValueType());
            }
        }
        else
        {
            throw new RuntimeException("ORDER BY parsed with strange child node type: " + child.name);
        }
        ExpressionUtil.assignLiteralConstantTypesRecursively(order_exp);
        ExpressionUtil.assignOutputValueTypesRecursively(order_exp);
        order_col.expression = order_exp;
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

    public int getLimitParameterIndex() {
        return paramIndexById(limitParameterId);
    }

    public int getOffsetParameterIndex() {
        return paramIndexById(offsetParameterId);
    }

    private AbstractExpression getParameterOrConstantAsExpression(long id, long value) {
        if (id != -1) {
            ParameterValueExpression parameter = new ParameterValueExpression();
            ParameterInfo paramInfo = paramsById.get(id);
            parameter.setParameterIndex(paramInfo.index);
            parameter.setValueType(paramInfo.type);
            parameter.setValueSize(paramInfo.type.getLengthInBytesForFixedTypes());
            return parameter;
        }
        else {
            ConstantValueExpression constant = new ConstantValueExpression();
            constant.setValue(Long.toString(value));
            constant.setValueType(VoltType.INTEGER);
            return constant;
        }
    }

    public AbstractExpression getOffsetExpression() {
        return getParameterOrConstantAsExpression(offsetParameterId, offset);
    }

    public AbstractExpression getLimitExpression() {
        return getParameterOrConstantAsExpression(limitParameterId, limit);
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
        if ((grouped == false) || groupByColumns.isEmpty()) {
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

    private boolean guaranteesUniqueRow() {
        if (((grouped == false) || groupByColumns.isEmpty() ) && displaysAgg()) {
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

}
