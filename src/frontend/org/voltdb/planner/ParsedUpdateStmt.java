/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.expressions.AbstractExpression;

/**
 *
 *
 */
public class ParsedUpdateStmt extends AbstractParsedStmt {
    // maintaining column ordering is important for deterministic
    // schema generation: see ENG-1660.
    LinkedHashMap<Column, AbstractExpression> m_columns =
        new LinkedHashMap<>();

    /**
    * Class constructor
    * @param paramValues
    * @param db
    */
    public ParsedUpdateStmt(AbstractParsedStmt parent, String[] paramValues, Database db) {
        super(parent, paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        assert(m_tableList.size() == 1);
        Table table = m_tableList.get(0);
        // Need to add the table to the cache. It may be required to resolve the
        // correlated TVE in case of WHERE clause contains IN subquery
        addTableToStmtCache(table, table.getTypeName());

        for (VoltXMLElement child : stmtNode.children) {
            if (child.name.equalsIgnoreCase("columns")) {
                parseTargetColumns(child, table, m_columns);
            }
        }
    }

    @Override
    public String toString() {
        String retval = super.toString() + "\n";

        retval += "COLUMNS:\n";
        for (Column col : m_columns.keySet()) {
            retval += "\tColumn: " + col.getTypeName() + ": ";
            retval += m_columns.get(col).toString() + "\n";
        }

        retval = retval.trim();

        return retval;
    }

    @Override
    public Set<AbstractExpression> findAllSubexpressionsOfClass(Class< ? extends AbstractExpression> aeClass) {
        Set<AbstractExpression> exprs = super.findAllSubexpressionsOfClass(aeClass);

        for (AbstractExpression expr : m_columns.values()) {
            if (expr != null) {
                exprs.addAll(expr.findAllSubexpressionsOfClass(aeClass));
            }
        }

        return exprs;
    }

    @Override
    public String calculateContentDeterminismMessage() {
        updateContentDeterminismMessage(getContentDeterminismMessage());
        return getContentDeterminismMessage();
    }

    @Override
    public boolean isDML() { return true; }

    @Override
    protected void parseCommonTableExpressions(VoltXMLElement root) {
        // No with statements here.
    }

}
