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
        boolean m_star;

        public Projection(int aLineNumber, int aColumnNumber) {
            m_star   = true;
            m_lineNo = aLineNumber;
            m_colNo  = aColumnNumber;
            m_alias  = m_tabName = m_columnName = null;
        }

        public Projection(String aTableName,
                      String aColumnName,
                      String aAlias,
                      int    aLineNo,
                      int    aColNo) {
        m_tabName    = aTableName;
        m_columnName = aColumnName;
        m_alias      = aAlias;
        m_lineNo     = aLineNo;
        m_star       = false;
        m_colNo      = aColNo;
    }

    public final String getTableName() {
        return m_tabName;
    }

    public final String getColumnName() {
        return m_columnName;
    }

    public final String getAlias() {
        return m_alias;
    }

    public final int getLineNo() {
        return m_lineNo;
    }

    public final int getColNo() {
        return m_colNo;
    }

    public final boolean isStar() {
        return m_star;
    }

}
