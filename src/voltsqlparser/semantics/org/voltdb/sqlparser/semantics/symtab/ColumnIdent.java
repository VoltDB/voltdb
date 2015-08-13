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
package org.voltdb.sqlparser.semantics.symtab;

import org.voltdb.sqlparser.syntax.IColumnIdent;

/**
 * Hold the parts needed to define a column in an insert statement.
 *
 * @author bwhite
 */
public class ColumnIdent implements IColumnIdent {
    private final String m_colName;
    private final int    m_colLineNo;
    private final int    m_colColNo;
    public ColumnIdent(String aColName, int aColLineNo, int aColColNo) {
        m_colName = aColName;
        m_colLineNo = aColLineNo;
        m_colColNo = aColColNo;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.syntax.IColumnIdent#getColName()
     */
    @Override
    public final String getColumnName() {
        return m_colName;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.syntax.IColumnIdent#getColLineNo()
     */
    @Override
    public final int getColLineNo() {
        return m_colLineNo;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.syntax.IColumnIdent#getColColNo()
     */
    @Override
    public final int getColColNo() {
        return m_colColNo;
    }
}

