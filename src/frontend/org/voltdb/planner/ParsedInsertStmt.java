/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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

import java.util.HashMap;
import java.util.Map.Entry;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;

/**
 *
 *
 */
public class ParsedInsertStmt extends AbstractParsedStmt {

    public HashMap<Column, AbstractExpression> columns = new HashMap<Column, AbstractExpression>();

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedInsertStmt(String[] paramValues, Database db) {
        super(paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        // A simple INSERT ... VALUES statement (all that's supported initially) has no underlying
        // table scans, so the table list should actually be empty until the statement's target
        // table is inserted, below.
        // TODO: When INSERT ... SELECT is supported, it's unclear how the source and target tables will
        // be distinguished (positionally? in separate members?) and/or how soon thereafter the SELECT
        // clause will need to allow joins.
        assert(tableList.isEmpty());

        String tableName = stmtNode.attributes.get("table");
        Table table = getTableFromDB(tableName);

        tableList.add(table);

        for (VoltXMLElement node : stmtNode.children) {
            if (node.name.equalsIgnoreCase("columns")) {
                parseTargetColumns(node, table, columns);
            }
        }
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
