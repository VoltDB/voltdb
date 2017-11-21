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

import java.util.Map;
import java.util.TreeMap;

public class LocalTableSchema {
    private final String m_tableName;
    private final Map<String, LocalTableColumn> m_columns;
    public LocalTableSchema(String tableName) {
        m_tableName = tableName;
        m_columns = new TreeMap<>();
    }
    public final String getTableName() {
        return m_tableName;
    }
    public final LocalTableColumn getColumnByName(String columnName) {
        return m_columns.get(columnName);
    }
    public void addColumn(LocalTableColumn col) {
        m_columns.put(col.getColumnName(), col);
    }
}
