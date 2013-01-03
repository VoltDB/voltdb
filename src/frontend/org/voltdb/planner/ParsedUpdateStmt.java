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

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedUpdateStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        String tableName = stmtNode.attributes.get("table");
        assert(tableName != null);
        tableName = tableName.trim();
        table = getTableFromDB(tableName);
        tableList.add(table);

        for (VoltXMLElement child : stmtNode.children) {
            if (child.name.equalsIgnoreCase("columns"))
                parseColumns(child);
            else if (child.name.equalsIgnoreCase("condition"))
                parseConditions(child);
        }
    }

    void parseColumns(VoltXMLElement columnsNode) {
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
            assert(child.children.size() == 1);
            VoltXMLElement subChild = child.children.get(0);
            expr = parseExpressionTree(subChild);
            assert(expr != null);
            expr.refineValueType(VoltType.get((byte)col.getType()));
            ExpressionUtil.finalizeValueTypes(expr);
            columns.put(col, expr);
        }
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
