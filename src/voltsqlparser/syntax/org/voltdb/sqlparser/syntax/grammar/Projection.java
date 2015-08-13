/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 /* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 package org.voltdb.sqlparser.syntax.grammar;

public class Projection {

        String m_tabName;
        String m_columnName;
        String m_alias;
        int    m_lineNo;
        int    m_colNo;

    public Projection(String aTableName,
                      String aColumnName,
                      String aAlias,
                      int    aLineNo,
                      int    aColNo) {
        m_tabName = aTableName;
        m_columnName = aColumnName;
        m_alias = aAlias;
        m_lineNo = aLineNo;
        m_colNo = aColNo;
    }

    public String getTableName() {
        return m_tabName;
    }

    public String getColumnName() {
        return m_columnName;
    }

    public String getAlias() {
        return m_alias;
    }

    public int getLineNo() {
        return m_lineNo;
    }

    public int getColNo() {
        return m_colNo;
    }
}
