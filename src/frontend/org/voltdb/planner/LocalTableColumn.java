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

import org.voltdb.VoltType;

public class LocalTableColumn {
    private final String m_columnName;
    private final VoltType m_columnType;
    private final int m_columnIndex;
    private final int m_columnSize;
    private final boolean m_isNullable;
    private final boolean m_isBytes;

    public LocalTableColumn(String name,
                            VoltType columnType,
                            int index,
                            int size,
                            boolean nullable,
                            boolean bytes) {
        m_columnName = name;
        m_columnType = columnType;
        m_columnIndex = index;
        m_columnSize = size;
        m_isNullable = nullable;
        m_isBytes = bytes;
    }
    public final String getColumnName() {
        return m_columnName;
    }
    public final VoltType getColumnType() {
        return m_columnType;
    }
    public final int getColumnIndex() {
        return m_columnIndex;
    }
    public final int getColumnSize() {
        return m_columnSize;
    }
    public final boolean isNullable() {
        return m_isNullable;
    }
    public final boolean isBytes() {
        return m_isBytes;
    }
}
