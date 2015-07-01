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

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.plannodes.SchemaColumn;

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

    /* Schema information: table may be "VOLT_TEMP_TABLE" */
    public String alias = null;
    public String columnName = null;
    public String tableName = null;
    public String tableAlias = null;

    /** The expression that this column refers to */
    public AbstractExpression expression = null;

    /** Index of the column in its table.  This is used for MV sanity-checking */
    public int index = 0;

    //
    // Used in ParsedSelectStmt.m_displayColumns
    //
    public boolean orderBy = false;
    public boolean ascending = true; // Only relevant if orderBy is true.
    public boolean groupBy = false;

    // Used in ParsedSelectStmt.m_groupByColumns
    public boolean groupByInDisplay = false;

    /** Sometimes the expression in the parsed column needs to be adjusted
     * for its context.  This is the case for AVG aggregates in SELECT statements,
     * which get transformed into SUM/COUNT.  This callback interface allows
     * clients constructing instances of ParsedColInfo to transform the expression
     * as needed. */
    public interface ExpressionAdjuster {
        AbstractExpression adjust(AbstractExpression expr);
    }

    /** Construct a ParsedColInfo from Volt XML. */
    static public ParsedColInfo fromOrderByXml(AbstractParsedStmt parsedStmt, VoltXMLElement orderByXml) {
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

    /** Construct a ParsedColInfo from Volt XML.  Allow caller to specify actions to finalize the parsed expression. */
    static public ParsedColInfo fromOrderByXml(AbstractParsedStmt parsedStmt, VoltXMLElement orderByXml, ExpressionAdjuster adjuster) {

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
        orderCol.orderBy = true;
        orderCol.ascending = !descending;
        AbstractExpression orderExpr = parsedStmt.parseExpressionTree(child);
        assert(orderExpr != null);
        orderCol.expression = adjuster.adjust(orderExpr);

        // Cases:
        // child could be columnref, in which case it's just a normal column.
        // Just make a ParsedColInfo object for it and the planner will do the right thing later
        if (child.name.equals("columnref")) {
            assert(orderExpr instanceof TupleValueExpression);
            TupleValueExpression tve = (TupleValueExpression) orderExpr;
            orderCol.columnName = tve.getColumnName();
            orderCol.tableName = tve.getTableName();
            orderCol.tableAlias = tve.getTableAlias();
            if (orderCol.tableAlias == null) {
                orderCol.tableAlias = orderCol.tableName;
            }

            orderCol.alias = tve.getColumnAlias();
        } else {
            String alias = child.attributes.get("alias");
            orderCol.alias = alias;
            orderCol.tableName = "VOLT_TEMP_TABLE";
            orderCol.tableAlias = "VOLT_TEMP_TABLE";
            orderCol.columnName = "";
            // Replace its expression to TVE after we build the ExpressionIndexMap

            if ((child.name.equals("operation") == false) &&
                    (child.name.equals("aggregation") == false) &&
                    (child.name.equals("function") == false)) {
               throw new RuntimeException("ORDER BY parsed with strange child node type: " + child.name);
           }
        }

        return orderCol;
    }

    /** Return this as an instance of SchemaColumn */
    public SchemaColumn asSchemaColumn() {
        return new SchemaColumn(tableName, tableAlias, columnName, alias, expression);
    }

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
