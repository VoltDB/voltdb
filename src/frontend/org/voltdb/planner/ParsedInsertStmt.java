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

    private final HashMap<Column, AbstractExpression> m_columns = new HashMap<Column, AbstractExpression>();
    private ParsedSelectStmt m_subselect;

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
        assert(m_tableList.isEmpty());

        String tableName = stmtNode.attributes.get("table");
        Table table = getTableFromDB(tableName);

        m_tableList.add(table);

        for (VoltXMLElement node : stmtNode.children) {
            if (node.name.equalsIgnoreCase("columns")) {
                parseTargetColumns(node, table, m_columns);
            }
            else if (node.name.equalsIgnoreCase(SELECT_NODE_NAME)) {
                // TODO: When INSERT ... SELECT is supported, it's unclear whether the source tables need to be
                // reflected in the m_tableList -- that would likely be propagated here --
                // and how the target table would then be distinguished
                // (positionally? in a separate member?) and/or how soon thereafter the SELECT
                // clause will need to allow joins.
                m_subselect = parseSubquery(node);
            }
        }
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval += "COLUMNS:\n";
        for (Entry<Column, AbstractExpression> col : m_columns.entrySet()) {
            retval += "\tColumn: " + col.getKey().getTypeName() + ": ";
            retval += col.getValue().toString() + "\n";
        }

        if (m_subselect != null) {
            retval += "SUBSELECT:\n";
            retval += m_subselect.toString();
        }
        return retval;
    }

    public AbstractExpression getValuesExpression(Column column) {
        return m_columns.get(column);
    }

    /**
     * @return the m_subselect
     */
    public ParsedSelectStmt getSubselect() {
        return m_subselect;
    }

}
