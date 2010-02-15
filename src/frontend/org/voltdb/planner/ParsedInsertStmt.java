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

import java.util.HashMap;
import java.util.Map.Entry;

import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 *
 *
 */
public class ParsedInsertStmt extends AbstractParsedStmt {

    public HashMap<Column, AbstractExpression> columns = new HashMap<Column, AbstractExpression>();

    ParsedInsertStmt() {
        columns = new HashMap<Column, AbstractExpression>();
    }

    @Override
    void parse(Node stmtNode, Database db) {
        assert(tableList.size() <= 1);

        NamedNodeMap attrs = stmtNode.getAttributes();
        Node tableNameAttr = attrs.getNamedItem("table");
        String tableName = tableNameAttr.getNodeValue();
        Table table = db.getTables().getIgnoreCase(tableName);

        // if the table isn't in the list add it
        // if it's there, good
        // if something else is there, we have a problem
        if (tableList.size() == 0)
            tableList.add(table);
        else
            assert(tableList.get(0) == table);

        NodeList children = stmtNode.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeName().equalsIgnoreCase("columns")) {
                NodeList colChildren = node.getChildNodes();
                for (int j = 0; j < colChildren.getLength(); j++) {
                    Node colNode = colChildren.item(j);
                    if (colNode.getNodeName().equalsIgnoreCase("column")) {
                         parseInsertColumn(colNode, db, table);
                    }
                }
            }
        }
    }

    void parseInsertColumn(Node columnNode, Database db, Table table) {
        NamedNodeMap attrs = columnNode.getAttributes();
        Node tableNameAttr = attrs.getNamedItem("table");
        Node columnNameAttr = attrs.getNamedItem("name");
        String tableName = tableNameAttr.getNodeValue();
        String columnName = columnNameAttr.getNodeValue();

        assert(tableName.equalsIgnoreCase(table.getTypeName()));
        Column column = table.getColumns().getIgnoreCase(columnName);

        AbstractExpression expr = null;
        NodeList children = columnNode.getChildNodes();
        for (int i = 0; i < children.getLength(); i++) {
            Node node = children.item(i);
            if (node.getNodeType() == Node.ELEMENT_NODE) {
                expr = parseExpressionTree(node, db);
                ExpressionUtil.assignLiteralConstantTypesRecursively(expr,
                        VoltType.get((byte)column.getType()));
                ExpressionUtil.assignOutputValueTypesRecursively(expr);
            }
        }

        columns.put(column, expr);
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval += "COLUMNS:\n";
        for (Entry<Column, AbstractExpression> col : columns.entrySet()) {
            retval += "\tColumn: " + col.getKey().getTypeName() + ": ";
            retval += col.getValue().toString() + "\n";
        }
        retval = retval.trim();

        return retval;
    }
}
