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

import java.util.HashMap;
import java.util.Map.Entry;

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
public class ParsedInsertStmt extends AbstractParsedStmt {

    public HashMap<Column, AbstractExpression> columns = new HashMap<Column, AbstractExpression>();

    ParsedInsertStmt() {
        columns = new HashMap<Column, AbstractExpression>();
    }

    @Override
    void parse(VoltXMLElement stmtNode, Database db) {
        assert(tableList.size() <= 1);

        String tableName = stmtNode.attributes.get("table");
        Table table = db.getTables().getIgnoreCase(tableName);

        // if the table isn't in the list add it
        // if it's there, good
        // if something else is there, we have a problem
        if (tableList.size() == 0)
            tableList.add(table);
        else
            assert(tableList.get(0) == table);

        for (VoltXMLElement node : stmtNode.children) {
            if (node.name.equalsIgnoreCase("columns")) {
                for (VoltXMLElement colNode : node.children) {
                    if (colNode.name.equalsIgnoreCase("column")) {
                         parseInsertColumn(colNode, db, table);
                    }
                }
            }
        }
    }

    void parseInsertColumn(VoltXMLElement columnNode, Database db, Table table) {
        String tableName = columnNode.attributes.get("table");
        String columnName = columnNode.attributes.get("name");

        assert(tableName.equalsIgnoreCase(table.getTypeName()));
        Column column = table.getColumns().getIgnoreCase(columnName);

        AbstractExpression expr = null;
        for (VoltXMLElement node : columnNode.children) {
            expr = parseExpressionTree(node, db);
            ExpressionUtil.assignLiteralConstantTypesRecursively(expr,
                    VoltType.get((byte)column.getType()));
            ExpressionUtil.assignOutputValueTypesRecursively(expr);
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
