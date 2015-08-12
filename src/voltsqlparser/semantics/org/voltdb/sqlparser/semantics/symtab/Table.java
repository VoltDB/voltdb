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
import java.util.*; // Uses arrayList, maybe j.u.* is too much.

import org.voltdb.sqlparser.syntax.symtab.IColumn;
import org.voltdb.sqlparser.syntax.symtab.ITable;
import org.voltdb.sqlparser.syntax.symtab.IType;

public class Table extends Top implements ITable {
    Map<String, Column> m_lookup = new TreeMap<String, Column>(String.CASE_INSENSITIVE_ORDER);
    List<String> m_colNames = new ArrayList<String>();

    public Table(String aTableName) {
        super(aTableName);
    }

    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ITable#addColumn(java.lang.String, org.voltdb.sqlparser.symtab.Column)
     */
        @Override
    public void addColumn(String name, IColumn column) {
        assert(column instanceof Column);
        m_lookup.put(name, (Column)column);
        m_colNames.add(name);
    }


    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ITable#toString()
     */
    public String toString() {
        String str = "{---Name:" + getName() + "---,";
        for (String key : m_lookup.keySet()) {
            IColumn icol = m_lookup.get(key);
            assert(icol instanceof Column);
            str += ((Column)icol).toString();
        }
        str += "}";
        return str;
    }

    public Column getColumnByName(String aName) {
        return m_lookup.get(aName);
    }

    public List<String> getColumnNames() {
        return m_colNames;
    }

    public List<IType> getColumnTypes() {
        List<IType> colTypes = new ArrayList<IType>();
        for (String colName : m_colNames) {
            IType ctype = m_lookup.get(colName).getType();
            colTypes.add(ctype);
        }
        return colTypes;
    }

    @Override
    public Set<String> getColumnNamesAsSet() {
        return m_lookup.keySet();
    }
}
