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
 package org.voltdb.sqlparser.semantics.symtab;

import org.voltdb.sqlparser.syntax.symtab.ITop;

/**
 * Top is the implementation of ITop.
 *
 * @author bwhite
 *
 */
public class Top implements ITop {
    long m_nominalSize;
    long m_maxSize;
    String m_name;

    Top(String aName, long aNominalSize, long aMaxSize) {
        m_name = aName;
        m_nominalSize = aNominalSize;
        m_maxSize = aMaxSize;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ITop#getNominalSize()
     */
    @Override
    public final long getNominalSize() {
        return m_nominalSize;
    }
    public final void setNominalSize(long aNominalSize) {
        m_nominalSize = aNominalSize;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ITop#getMaxSize()
     */
    @Override
    public final long getMaxSize() {
        return m_maxSize;
    }
    public final void setMaxSize(long aMaxSize) {
        m_maxSize = aMaxSize;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ITop#getName()
     */
    @Override
    public final String getName() {
        return m_name;
    }
    public final void setName(String aName) {
        m_name = aName;
    }

    public String toString() {
        return m_name.toUpperCase();
    }

}
