/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

import java.util.LinkedHashMap;

import org.voltdb.catalog.Database;
import org.voltdb.VoltType;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.Column;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 *
 *
 */
public class ParsedUpdateStmt extends AbstractParsedStmt {
    Table table = null;

    // maintaining column ordering is important for deterministic
    // schema generation: see ENG-1660.
    LinkedHashMap<Column, AbstractExpression> columns =
        new LinkedHashMap<Column, AbstractExpression>();

    @Override
    void parse(Node stmtNode, Database db) {
        NamedNodeMap attrs = stmtNode.getAttributes();
        Node node = attrs.getNamedItem("table");
        assert(node != null);
        String tableName = node.getNodeValue().trim();
        table = db.getTables().getIgnoreCase(tableName);
        tableList.add(table);

        for (Node child = stmtNode.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() != Node.ELEMENT_NODE)
                continue;
            if (child.getNodeName().equalsIgnoreCase("columns"))
                parseColumns(child, db);
            else if (child.getNodeName().equalsIgnoreCase("condition"))
                parseCondition(child, db);
        }
    }

    void parseColumns(Node columnsNode, Database db) {
        for (Node child = columnsNode.getFirstChild(); child != null; child = child.getNextSibling()) {
            if (child.getNodeType() != Node.ELEMENT_NODE)
                continue;
            assert(child.getNodeName().equals("column"));

            Column col = null;
            NamedNodeMap attrs = child.getAttributes();
            Node node = attrs.getNamedItem("table");
            assert(node != null);
            assert(node.getNodeValue().equalsIgnoreCase(table.getTypeName()));
            node = attrs.getNamedItem("name");
            assert(node != null);
            col = table.getColumns().getIgnoreCase(node.getNodeValue().trim());

            AbstractExpression expr = null;
            for (Node subChild = child.getFirstChild(); subChild != null; subChild = subChild.getNextSibling()) {
                if (subChild.getNodeType() != Node.ELEMENT_NODE)
                    continue;
                expr = parseExpressionTree(subChild, db);
                ExpressionUtil.assignLiteralConstantTypesRecursively(expr, VoltType.get((byte)col.getType()));
                ExpressionUtil.assignOutputValueTypesRecursively(expr);
            }
            assert(expr != null);
            columns.put(col, expr);
        }
    }

    void parseCondition(Node conditionNode, Database db) {
        Node exprNode = conditionNode.getFirstChild();
        while ((exprNode != null) && (exprNode.getNodeType() != Node.ELEMENT_NODE))
            exprNode = exprNode.getNextSibling();
        if (exprNode == null)
            return;
        where = parseExpressionTree(exprNode, db);
        ExpressionUtil.assignLiteralConstantTypesRecursively(where);
        ExpressionUtil.assignOutputValueTypesRecursively(where);
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval += "COLUMNS:\n";
        for (Column col : columns.keySet()) {
            retval += "\tColumn: " + col.getTypeName() + ": ";
            retval += columns.get(col).toString() + "\n";
        }

        retval = retval.trim();

        return retval;
    }
}
