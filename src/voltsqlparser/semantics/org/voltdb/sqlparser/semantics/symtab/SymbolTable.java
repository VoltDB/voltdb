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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.voltdb.sqlparser.syntax.symtab.ISymbolTable;
import org.voltdb.sqlparser.syntax.symtab.ITop;

/**
 * A SymbolTable associates values and types with
 * strings.  SymbolTables may be nested.  Tables are
 * a kind of SymbolTable, since they have columns
 * and columns have names.  All entities have numeric
 * ids as well.  Numeric IDs with value less than 1000
 * are pre-defined entities, such as pre-defined types.
 * IDs with values more than 1000 are user defined entities,
 * such as columns and tables.
 *
 * @author bwhite
 *
 */
public class SymbolTable implements ISymbolTable {
    ISymbolTable m_parent;
    Type         m_integerType = null;
    public class TablePair {
        Table m_table;
        String m_alias;
        public TablePair(Table aTable, String aAlias) {
            m_table = aTable;
            m_alias = aAlias;
        }
        public final Table getTable() {
            return m_table;
        }
        public final String getAlias() {
            return m_alias;
        }
    }
    List<TablePair> m_tables = new ArrayList<TablePair>();
    Map<String, Top> m_lookup = new TreeMap<String, Top>(String.CASE_INSENSITIVE_ORDER);

    public SymbolTable(SymbolTable aParent) {
        m_parent = aParent;
    }

    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ISymbolTable#define(org.voltdb.sqlparser.symtab.Top)
     */
    @Override
    public void define(ITop aEntity) {
        if (aEntity.getName() != null) {
            m_lookup.put(aEntity.getName(), (Top) aEntity);
        }
        if (aEntity instanceof Table) {
            m_tables.add(new TablePair((Table)aEntity, aEntity.getName()));
        }
    }

    public String toString() {
        return m_lookup.toString();
    }

    public void addTable(Table aTable,String aAlias) {
        m_lookup.put(aAlias, aTable);
        m_tables.add(new TablePair(aTable, aAlias));
    }

    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ISymbolTable#size()
     */
    @Override
    public int size() {
        return m_lookup.size();
    }

    /**
     * called with input tables as arguments, so that this table knows what to do.
     * @param args
     */
    public void buildLookup(String[] args) {

    }

    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ISymbolTable#isEmpty()
     */
    @Override
    public boolean isEmpty() {
        return m_lookup.size() == 0;
    }
    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ISymbolTable#get(java.lang.String)
     */
    @Override
    public final Top get(String aName) {
        Top ret = m_lookup.get(aName);
        if (ret == null) {
                if (m_parent != null)
                        ret = (Top) m_parent.get(aName);
        }
        return ret;
    }

    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ISymbolTable#getType(java.lang.String)
     */
    @Override
    public final Type getType(String aName) { // is it illegal to name tables the same thing as types? I don't think that would work here.
        Top answer = get(aName);
        if (answer != null && answer instanceof Type) {
            return (Type)answer;
        } else if (m_parent != null) {
            return (Type) m_parent.getType(aName);
        } else {
            return null;
        }
    }

    /* (non-Javadoc)
     * @see org.voltdb.sqlparser.symtab.ISymbolTable#getValue(java.lang.String)
     */
    @Override
    public final Value getValue(String aName) {
        Top answer = get(aName);
        if (answer != null && answer instanceof Value) {
            return (Value)answer;
        }
        return null;
    }

    public final Table getTable(String aName) {
        Top table = get(aName);
        if (table != null && table instanceof Table)
                return (Table)table;
        return null;
    }

    public static ISymbolTable newStandardPrelude() {
        ISymbolTable answer = new SymbolTable(null);
        answer.define(new IntegerType("bigint", 8, 8));
        answer.define(new IntegerType("integer", 4, 4));
        answer.define(new IntegerType("tinyint", 1, 1));
        answer.define(new IntegerType("smallint", 2, 2));
        return answer;
    }

    public String getTableAliasByColumn(String aColName) {
        for (TablePair tp : m_tables) {
            Column col = tp.getTable().getColumnByName(aColName);
            if (col != null) {
                if (tp.getAlias() == null) {
                    return tp.getTable().getName();
                }
                return tp.getAlias();
            }
        }
        return null;
    }

    public String getTableNameByColumn(String aColName) {
        for (TablePair tp : m_tables) {
            Column col = tp.getTable().getColumnByName(aColName);
            if (col != null) {
                return tp.getTable().getName();
            }
        }
        return null;
    }

    public final List<TablePair> getTables() {
        return m_tables;
    }

    public final int getSize() {
        return m_lookup.size();
    }
}
