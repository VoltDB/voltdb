/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;

/**
 * Keep track of common table clauses.  These are WITH clauses which
 * may precede a select or DML statement.  The table definitions themselves
 * are stored in the table scan map of the enclosing AbstractParsedStatement,
 * so we only need to record if the CTE is recursive and the names of the
 * tables.  We may not need the names of the tables, so we may be able to
 * simplify this into a single boolean.  A more complete implementation
 * could not do without knowledge of the common tables, in order to compute
 * evaluation orders and validate the dependence graphs.
 *
 * For now we just retain the names of the common tables and whether the
 * statement is recursive.
 *
 * Note that in this initial implementation all statements are recursive
 * and there will be only one common table.
 */
public class CommonTableClause {
    private final boolean m_isRecursive;
    private final List<String> m_commonTableNames = new ArrayList<>();

    public CommonTableClause(boolean isRecursive) {
        m_isRecursive = isRecursive;
    }
    public final boolean isRecursive() {
        return m_isRecursive;
    }
    public final List<String> getCommonTableNames() {
        return m_commonTableNames;
    }
}
