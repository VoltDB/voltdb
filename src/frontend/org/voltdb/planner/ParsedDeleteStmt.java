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

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.ExpressionUtil;

/**
 *
 *
 */
public class ParsedDeleteStmt extends AbstractParsedStmt {
    Table table = null;

    @Override
    void parse(VoltXMLElement stmtNode, Database db) {
        String tableName = stmtNode.attributes.get("table");
        assert(tableName != null);
        tableName = tableName.trim();
        table = db.getTables().getIgnoreCase(tableName);
        tableList.add(table);

        for (VoltXMLElement child : stmtNode.children) {
            if (child.name.equalsIgnoreCase("condition"))
                parseCondition(child, db);
        }
    }

    void parseCondition(VoltXMLElement conditionNode, Database db) {
        AbstractExpression tempWhere = null;
        for (VoltXMLElement exprNode : conditionNode.children) {
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
