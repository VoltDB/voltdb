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

import java.util.LinkedHashMap;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;

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
    void parse(VoltXMLElement stmtNode, Database db) {
        String tableName = stmtNode.attributes.get("table");
        assert(tableName != null);
        tableName = tableName.trim();
        table = db.getTables().getIgnoreCase(tableName);
        tableList.add(table);

        for (VoltXMLElement child : stmtNode.children) {
            if (child.name.equalsIgnoreCase("columns"))
                parseColumns(child, db);
            else if (child.name.equalsIgnoreCase("condition"))
                parseCondition(child, db);
        }
    }

    void parseColumns(VoltXMLElement columnsNode, Database db) {
        for (VoltXMLElement child : columnsNode.children) {
            assert(child.name.equals("column"));

            Column col = null;
            String tableName = child.attributes.get("table");
            assert(tableName != null);
            assert(tableName.equalsIgnoreCase(table.getTypeName()));
            String name = child.attributes.get("name");
            assert(name != null);
            col = table.getColumns().getIgnoreCase(name.trim());

            AbstractExpression expr = null;
            for (VoltXMLElement subChild : child.children) {
                expr = parseExpressionTree(subChild, db);
                ExpressionUtil.assignLiteralConstantTypesRecursively(expr, VoltType.get((byte)col.getType()));
                ExpressionUtil.assignOutputValueTypesRecursively(expr);
            }
            assert(expr != null);
            columns.put(col, expr);
        }
    }

    void parseCondition(VoltXMLElement conditionNode, Database db) {
        if (conditionNode.children.size() == 0)
            return;
        VoltXMLElement exprNode = conditionNode.children.get(0);
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
