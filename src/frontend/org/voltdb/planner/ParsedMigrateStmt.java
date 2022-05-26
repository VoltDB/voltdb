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

import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.hsqldb_voltpatches.VoltXMLElement;
import org.voltdb.catalog.Database;
import org.voltdb.expressions.AbstractExpression;

public class ParsedMigrateStmt extends AbstractParsedStmt {
    public ParsedMigrateStmt(AbstractParsedStmt parent, String[] paramValues, Database db) {
        super(parent, paramValues, db);
    }

    @Override
    void parse(VoltXMLElement stmtNode) {
        assert(m_tableList.size() == 1);
    }

    @Override
    public boolean hasOrderByColumns() {
        return false;
    }

    /** Returns items in ORDER BY clause as a list of ParsedColInfo */
    @Override
    public List<ParsedColInfo> orderByColumns() {
        return Collections.emptyList();
    }

    /** Returns true if this statement has a LIMIT or OFFSET clause */
    @Override
    public boolean hasLimitOrOffset() {
        return false;
    }

    @Override
    public String calculateContentDeterminismMessage() {
        return null;
    }

    @Override
    public Set<AbstractExpression> findAllSubexpressionsOfClass(Class< ? extends AbstractExpression> aeClass) {
        return super.findAllSubexpressionsOfClass(aeClass);
    }

    @Override
    public boolean isDML() { return true; }

    @Override
    protected void parseCommonTableExpressions(VoltXMLElement root) {
        // No with statements here.
    }
}
