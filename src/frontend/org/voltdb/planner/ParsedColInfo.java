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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.SchemaColumn;

import com.google_voltpatches.common.base.MoreObjects;

/**
 * This class represents an instance of a column in a parsed statement.
 * Note that an instance might refer to a downstream temp table, rather
 * than a column in a persistent table.
 *
 * This class is pretty C-struct-like, with all public members and a
 * 0-argument constructor.  It would be nice at some point to clean this
 * up.
 */
public class ParsedColInfo implements Cloneable {
    /* Schema information: table may be AbstractParsedStmt.TEMP_TABLE_NAME */
    public String m_alias = null;
    public String m_columnName = null;
    public String m_tableName = null;
    public String m_tableAlias = null;

    /** The expression that this column refers to */
    public AbstractExpression m_expression = null;

    /** Index of the column in its table.  This is used for MV sanity-checking */
    public int m_index = 0;

    /** A differentiator used to tell apart column references when there are
     * duplicate column names produced by the expansion of "*" that happens in HSQL.*/
    public int m_differentiator = -1;
    //
    // Used in ParsedSelectStmt.m_displayColumns
    //
    public boolean m_orderBy = false;
    public boolean m_ascending = true; // Only relevant if orderBy is true.
    public boolean m_groupBy = false;

    // Used in ParsedSelectStmt.m_groupByColumns
    public boolean m_groupByInDisplay = false;

    /** Sometimes the expression in the parsed column needs to be adjusted
     * for its context.  This is the case for AVG aggregates in SELECT statements,
     * which get transformed into SUM/COUNT.  This callback interface allows
     * clients constructing instances of ParsedColInfo to transform the expression
     * as needed. */
    public interface ExpressionAdjuster {
        AbstractExpression adjust(AbstractExpression expr);
    }

    /** Construct a ParsedColInfo from Volt XML. */
    static public ParsedColInfo fromOrderByXml(AbstractParsedStmt parsedStmt,
            VoltXMLElement orderByXml) {
        // A generic adjuster that just calls finalizeValueTypes
        ExpressionAdjuster adjuster = new ExpressionAdjuster() {
            @Override
            public AbstractExpression adjust(AbstractExpression expr) {
                ExpressionUtil.finalizeValueTypes(expr);
                return expr;
            }
        };

        return fromOrderByXml(parsedStmt, orderByXml, adjuster);
    }

    /**
     * Helpers
     */
    public ParsedColInfo updateTableName(String tblName, String tblAlias) {
       m_tableName = tblName;
       m_tableAlias = tblAlias;
       return this;
    }

    /**
     * Helpers
     */
    public ParsedColInfo updateColName(String colName, String colAlias) {
       m_columnName = colName;
       m_alias = colAlias;
       if (m_expression instanceof TupleValueExpression) {
          TupleValueExpression expr = (TupleValueExpression) m_expression;
          expr.setColumnName(colName);
          expr.setColumnAlias(colAlias);
       }
       return this;
    }

    // Convert any non-TupleValueExpression, i.e. AggregateExpression to TupleValueExpression,
    // and syncs with (table/column) * (name/alias).
    public ParsedColInfo toTVE(int indx, int diff) {
       TupleValueExpression exp = new TupleValueExpression(m_tableName, m_tableAlias,
               m_columnName, m_alias, m_expression, indx);
       exp.setDifferentiator(diff);
       m_expression = exp;
       return this;
    }

    /** Construct a ParsedColInfo from Volt XML.
     *  Allow caller to specify actions to finalize the parsed expression.
     */
    static public ParsedColInfo fromOrderByXml(AbstractParsedStmt parsedStmt,
            VoltXMLElement orderByXml, ExpressionAdjuster adjuster) {

        // make sure everything is kosher
        assert(orderByXml.name.equalsIgnoreCase("orderby"));

        // get desc/asc
        String desc = orderByXml.attributes.get("desc");
        boolean descending = (desc != null) && (desc.equalsIgnoreCase("true"));

        // get the columnref or other expression inside the orderby node
        VoltXMLElement child = orderByXml.children.get(0);
        assert(child != null);

        // create the orderby column
        ParsedColInfo orderCol = new ParsedColInfo();
        orderCol.m_orderBy = true;
        orderCol.m_ascending = !descending;
        AbstractExpression orderExpr = parsedStmt.parseExpressionTree(child);
        assert(orderExpr != null);
        orderCol.m_expression = adjuster.adjust(orderExpr);

        // Cases:
        // child could be columnref, in which case it's either a normal column
        // or an expression.
        // The latter could be a case if this column came from a subquery that
        // was optimized out.
        // Just make a ParsedColInfo object for it and the planner will do the
        // right thing later.
        if (orderExpr instanceof TupleValueExpression) {
            TupleValueExpression tve = (TupleValueExpression) orderExpr;
            orderCol.m_columnName = tve.getColumnName();
            orderCol.m_tableName = tve.getTableName();
            orderCol.m_tableAlias = tve.getTableAlias();
            if (orderCol.m_tableAlias == null) {
                orderCol.m_tableAlias = orderCol.m_tableName;
            }

            orderCol.m_alias = tve.getColumnAlias();
        }
        else {
            switch (orderCol.m_expression.getExpressionType()) {
            case VALUE_CONSTANT:
                throw new PlanningErrorException(
                        "ORDER BY cannot contain constant values: " + child.attributes.get("value"), 0);
            case VALUE_PARAMETER:
                String table = MoreObjects.firstNonNull(child.attributes.get("tablealias"),
                        child.attributes.get("table"));
                throw new PlanningErrorException(
                        "ORDER BY cannot contain references to columns or values which are not involved in the current select: "
                                + table + '.' + child.attributes.get("column"),
                        0);
            default:
            }

            String alias = child.attributes.get("alias");
            orderCol.m_alias = alias;
            orderCol.m_tableName = AbstractParsedStmt.TEMP_TABLE_NAME;
            orderCol.m_tableAlias = AbstractParsedStmt.TEMP_TABLE_NAME;
            orderCol.m_columnName = "";
            // Replace its expression to TVE after we build the ExpressionIndexMap

            if ((child.name.equals("operation") == false) &&
                    (child.name.equals("aggregation") == false) &&
                    (child.name.equals("win_aggregation") == false) &&
                    (child.name.equals("function") == false) &&
                    (child.name.equals("rank") == false) &&
                    (child.name.equals("value") == false) &&
                    (child.name.equals("columnref") == false)) {
               throw new RuntimeException(
                       "ORDER BY parsed with strange child node type: " +
                       child.name);
            }
        }

        return orderCol;
    }

    /** Return this as an instance of SchemaColumn */
    public SchemaColumn asSchemaColumn() {
        String columnAlias = (m_alias == null) ? m_columnName : m_alias;
        return new SchemaColumn(m_tableName, m_tableAlias,
                m_columnName, columnAlias,
                m_expression, m_differentiator);
    }

    @Override
    public String toString() {
        return "ParsedColInfo: " + m_tableName + "(" + m_tableAlias + ")." +
                m_columnName + "(" + m_alias + ") diff'tor:" + m_differentiator +
            " expr:(" + m_expression + ")";
    }

    @Override
    public boolean equals (Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj instanceof ParsedColInfo == false) {
            return false;
        }
        ParsedColInfo col = (ParsedColInfo) obj;
        if (m_columnName != null && m_columnName.equals(col.m_columnName) &&
                m_tableName != null && m_tableName.equals(col.m_tableName) &&
                m_tableAlias != null && m_tableAlias.equals(col.m_tableAlias) &&
                m_expression != null && m_expression.equals(col.m_expression) ) {
            return true;
        }
        return false;
    }

    // Based on implementation on equals().
    @Override
    public int hashCode() {
        int result = new HashCodeBuilder(17, 31).
                append(m_columnName).append(m_tableName).append(m_tableAlias).
                toHashCode();
        if (m_expression != null) {
            result += m_expression.hashCode();
        }
        return result;
    }

    @Override
    public ParsedColInfo clone() {
        ParsedColInfo col = null;
        try {
            col = (ParsedColInfo) super.clone();
        }
        catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }
        col.m_expression = m_expression.clone();
        return col;
    }
}
