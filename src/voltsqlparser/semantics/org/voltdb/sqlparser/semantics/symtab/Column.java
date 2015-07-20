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

import org.voltdb.sqlparser.syntax.symtab.IColumn;

public class Column extends Top implements IColumn {
    protected Type m_type;
    protected boolean m_hasDefaultValue = false;
    protected String  m_defaultValue    = null;
    protected boolean m_isNullable      = false;
    protected boolean m_isPrimaryKey    = false;
    protected boolean m_isUnique        = false;
    protected boolean m_isNull          = false;

    public Column(String name,Type type) {
        super(name, 0, 0);
            this.m_type=type;
    }

    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.IColumn#getType()
     */
        @Override
    public final Type getType() {
        return m_type.getType();
    }


    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.IColumn#toString()
     */
    @Override
    public boolean hasDefaultValue() {
        return m_hasDefaultValue;
    }

    @Override
    public String getDefaultValue() {
        return m_defaultValue;
    }

    @Override
    public boolean isPrimaryKey() {
        return m_isPrimaryKey;
    }

    @Override
    public boolean isNullable() {
        return m_isNullable;
    }

    @Override
    public void setHasDefaultValue(boolean hasDefaultValue) {
        m_hasDefaultValue = hasDefaultValue;
    }

    @Override
    public void setDefaultValue(String defaultValue) {
        m_defaultValue = defaultValue;
    }

    @Override
    public void setIsPrimaryKey(boolean isPrimaryKey) {
        m_isPrimaryKey = isPrimaryKey;
    }

    @Override
    public void setIsNullable(boolean value) {
        m_isNullable = value;
    }

    @Override
    public void setIsUniqueConstraint(boolean isUniqueConstraint) {
        m_isUnique = isUniqueConstraint;
    }

    @Override
    public boolean isUniqueConstraint() {
        return m_isUnique;
    }

    @Override
    public void setIsNull(boolean value) {
        m_isNull           = value;
    }

    @Override
    public boolean isNull() {
        // TODO Auto-generated method stub
        return m_isNull;
    }
}
