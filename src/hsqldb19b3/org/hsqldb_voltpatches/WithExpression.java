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

package org.hsqldb_voltpatches;

import java.util.ArrayList;
import java.util.List;

import org.hsqldb_voltpatches.HsqlNameManager.HsqlName;

public class WithExpression {
    // Note: These should not be strings.  They
    //       should be identifier references somehow.
    private List<HsqlName> m_columnNames = new ArrayList<>();
    private HsqlName m_queryName;
    private boolean m_isRecursive = false;
    private QueryExpression m_baseQuery;
    private QueryExpression m_recursiveQuery;

    public final boolean isRecursive() {
        return m_isRecursive;
    }
    public final void setRecursive(boolean isRecursive) {
        m_isRecursive = isRecursive;
    }
    public final List<HsqlName> getColumnNames() {
        return m_columnNames;
    }
    public final void setColumnNames(List<HsqlName> columnNames) {
        m_columnNames = columnNames;
    }
    public final HsqlName getQueryName() {
        return m_queryName;
    }
    public final void setQueryName(HsqlName queryName) {
        m_queryName = queryName;
    }
    public final QueryExpression getBaseQuery() {
        return m_baseQuery;
    }
    public final void setBaseQuery(QueryExpression baseQuery) {
        m_baseQuery = baseQuery;
    }
    public final QueryExpression getRecursiveQuery() {
        return m_recursiveQuery;
    }
    public final void setRecursiveQuery(QueryExpression recursiveQuery) {
        m_recursiveQuery = recursiveQuery;
    }
}
