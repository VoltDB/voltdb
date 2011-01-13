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

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 *
 *
 */
public class ParsedDeleteStmt extends AbstractParsedStmt {
    Table table = null;

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
            else if (child.getNodeName().equalsIgnoreCase("condition"))
                parseCondition(child, db);
        }
    }

    void parseCondition(Node conditionNode, Database db) {
        AbstractExpression tempWhere = null;
        for (Node exprNode = conditionNode.getFirstChild(); exprNode != null; exprNode = exprNode.getNextSibling()) {
            if (exprNode.getNodeType() != Node.ELEMENT_NODE) continue;
            if (tempWhere == null) {
                tempWhere = parseExpressionTree(exprNode, db);
            }
            else {
                tempWhere = ExpressionUtil.combine(tempWhere, parseExpressionTree(exprNode, db));
            }
        }
        where = tempWhere;
        ExpressionUtil.assignLiteralConstantTypesRecursively(where);
        ExpressionUtil.assignOutputValueTypesRecursively(where);
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval = retval.trim();

        return retval;
    }
}
