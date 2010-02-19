/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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
import java.util.HashMap;
import java.util.Map.Entry;

import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 *
 *
 */
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

    public HashMap<String, ParsedColInfo> allColumns = new HashMap<String, ParsedColInfo>();
    public ArrayList<ParsedColInfo> displayColumns = new ArrayList<ParsedColInfo>();
    public ArrayList<ParsedColInfo> orderColumns = new ArrayList<ParsedColInfo>();
    public AbstractExpression having = null;
    public ArrayList<ParsedColInfo> groupByColumns = new ArrayList<ParsedColInfo>();

    public long limit = -1;
    public long offset = 0;
    public long limitParameterId = -1;
    public long offsetParameterId = -1;
    public boolean grouped = false;
    public boolean distinct = false;

    @Override
    void parse(Node stmtNode, Database db) {
        NamedNodeMap attrs = stmtNode.getAttributes();
        Node node;

        if ((node = attrs.getNamedItem("limit")) != null)
            limit = Long.parseLong(node.getNodeValue());
        if ((node = attrs.getNamedItem("offset")) != null)
            offset = Long.parseLong(node.getNodeValue());
        if ((node = attrs.getNamedItem("limit_paramid")) != null)
            limitParameterId = Long.parseLong(node.getNodeValue());
        if ((node = attrs.getNamedItem("offset_paramid")) != null)
            offsetParameterId = Long.parseLong(node.getNodeValue());
        if ((node = attrs.getNamedItem("grouped")) != null)
            grouped = Boolean.parseBoolean(node.getNodeValue());
        if ((node = attrs.getNamedItem("distinct")) != null)
            distinct = Boolean.parseBoolean(node.getNodeValue());

        // limit and offset can't have both value and parameter
        if (limit != -1) assert limitParameterId == -1 : "Parsed value and param. limit.";
        if (offset != 0) assert offsetParameterId == -1 : "Parsed value and param. offset.";

        for (Node child = stmtNode.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() != Node.ELEMENT_NODE)
                continue;
            if (child.getNodeName().equalsIgnoreCase("columns"))
                parseDisplayColumns(child, db);
            else if (child.getNodeName().equalsIgnoreCase("querycondition"))
                parseQueryCondition(child, db);
            else if (child.getNodeName().equalsIgnoreCase("ordercolumns"))
                parseOrderColumns(child, db);
            else if (child.getNodeName().equalsIgnoreCase("groupcolumns")) {
                parseGroupByColumns(child, db);
            }
        }
    }

    void parseDisplayColumns(Node columnsNode, Database db) {
        for (Node child = columnsNode.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() != Node.ELEMENT_NODE)
                continue;
            final String nodeName = child.getNodeName();
            ParsedColInfo col = new ParsedColInfo();
            col.expression = parseExpressionTree(child, db);
            ExpressionUtil.assignLiteralConstantTypesRecursively(col.expression);
            ExpressionUtil.assignOutputValueTypesRecursively(col.expression);
            assert(col.expression != null);
            col.alias = child.getAttributes().getNamedItem("alias").getNodeValue();

            if (nodeName.equals("columnref")) {
                col.columnName =
                    child.getAttributes().getNamedItem("column").getNodeValue();
                col.tableName =
                    child.getAttributes().getNamedItem("table").getNodeValue();
            }
            col.index = allColumns.size();
            displayColumns.add(col);
            allColumns.put(col.alias, col);
        }
    }

    void parseQueryCondition(Node conditionNode, Database db) {
        Node exprNode = conditionNode.getFirstChild();
        while ((exprNode != null) && (exprNode.getNodeType() != Node.ELEMENT_NODE))
            exprNode = exprNode.getNextSibling();
        if (exprNode == null)
            return;
        where = parseExpressionTree(exprNode, db);
        ExpressionUtil.assignLiteralConstantTypesRecursively(where);
        ExpressionUtil.assignOutputValueTypesRecursively(where);
    }

    void parseGroupByColumns(Node columnsNode, Database db) {
        for (Node child = columnsNode.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() != Node.ELEMENT_NODE)
                continue;

            parseGroupByColumn(child, db);
        }
    }

    void parseGroupByColumn(Node groupByNode, Database db) {
        // make sure everything is kosher
        assert(groupByNode.getNodeType() == Node.ELEMENT_NODE);

        ParsedColInfo col = new ParsedColInfo();
        col.expression = parseExpressionTree(groupByNode, db);
        assert(col.expression != null);

        if (groupByNode.getNodeName().equals("columnref"))
        {
            NamedNodeMap attrs = groupByNode.getAttributes();
            col.alias = attrs.getNamedItem("alias").getNodeValue();
            col.columnName =
                attrs.getNamedItem("column").getNodeValue();
            col.tableName =
                attrs.getNamedItem("table").getNodeValue();
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

    void parseOrderColumns(Node columnsNode, Database db) {
        for (Node child = columnsNode.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() != Node.ELEMENT_NODE)
                continue;

            parseOrderColumn(child, db);
        }
    }

    void parseOrderColumn(Node orderByNode, Database db) {
        // make sure everything is kosher
        assert(orderByNode.getNodeType() == Node.ELEMENT_NODE);
        assert(orderByNode.getNodeName().equalsIgnoreCase("operation"));
        NamedNodeMap attrs = orderByNode.getAttributes();
        Node operationTypeNode = attrs.getNamedItem("type");
        assert(operationTypeNode != null);
        assert(operationTypeNode.getNodeValue().equalsIgnoreCase("orderby"));

        // get desc/asc
        Node descNode = attrs.getNamedItem("desc");
        boolean descending = descNode.getNodeValue().equalsIgnoreCase("true");

        // get the columnref expression inside the orderby node
        Node child = orderByNode.getFirstChild();
        while (child.getNodeType() != Node.ELEMENT_NODE)
            child = child.getNextSibling();
        assert(child != null);
        if (child.getNodeName().equals("columnref"))
        {
            NamedNodeMap childAttrs = child.getAttributes();
            String alias = childAttrs.getNamedItem("alias").getNodeValue();

            // create the orderby column
            ParsedColInfo col = allColumns.get(alias);
            if (col == null) {
                col = new ParsedColInfo();
                col.alias = alias;
                col.expression = parseExpressionTree(child, db);
                ExpressionUtil.assignLiteralConstantTypesRecursively(col.expression);
                ExpressionUtil.assignOutputValueTypesRecursively(col.expression);
                col.index = allColumns.size();
                col.columnName =
                    childAttrs.getNamedItem("column").getNodeValue();
                col.tableName =
                    childAttrs.getNamedItem("table").getNodeValue();
                allColumns.put(alias, col);
            }
            col.orderBy = true;
            col.ascending = !descending;
            orderColumns.add(col);
        }
        else
        {
            throw new RuntimeException("ORDER BY with complex expressions not yet supported");
        }
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval += "LIMIT " + String.valueOf(limit) + "\n";
        retval += "OFFSET " + String.valueOf(limit) + "\n";

        retval += "ALL COLUMNS:\n";
        for (Entry<String, ParsedColInfo> col : allColumns.entrySet()) {
            retval += "\tColumn: " + col.getKey() + ": ";
            retval += col.getValue().expression.toString() + "\n";
        }

        retval += "DISPLAY COLUMNS:\n";
        for (ParsedColInfo col : displayColumns) {
            retval += "\tColumn: " + col.alias + ": ";
            retval += col.expression.toString() + "\n";
        }

        retval += "ORDER COLUMNS:\n";
        for (ParsedColInfo col : orderColumns) {
            retval += "\tColumn: " + col.alias + ": " + col.ascending;
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
}
