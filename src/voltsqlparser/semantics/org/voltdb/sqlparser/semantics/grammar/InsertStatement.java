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
 package org.voltdb.sqlparser.semantics.grammar;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.voltdb.sqlparser.syntax.ColumnIdent;
import org.voltdb.sqlparser.syntax.grammar.IInsertStatement;
import org.voltdb.sqlparser.syntax.symtab.IColumn;
import org.voltdb.sqlparser.syntax.symtab.IParserFactory;
import org.voltdb.sqlparser.syntax.symtab.ISymbolTable;
import org.voltdb.sqlparser.syntax.symtab.ITable;
import org.voltdb.sqlparser.syntax.symtab.IType;
import org.voltdb.sqlparser.syntax.util.ErrorMessageSet;
import org.voltdb.sqlparser.semantics.symtab.Type;

public class InsertStatement implements IInsertStatement, IDQLStatement {
    String m_tableName;
    ITable m_table;
    List<String> m_colNames = new ArrayList<String>();
    List<Type> m_colTypes = new ArrayList<Type>();
    List<String> m_colVals = new ArrayList<String>();

    @Override
    public void addTable(ITable aTable) {
        m_tableName = aTable.getName();
        m_table = aTable;
    }

    public int getNumberColumns() {
        return m_colNames.size();
    }

    public String getColumnName(int idx) {
        return m_colNames.get(idx);
    }

    public Type getColumnType(int idx) {
        return m_colTypes.get(idx);
    }

    public String getColumnValue(int idx) {
        return m_colVals.get(idx);
    }

    public String getTableName() {
        return m_tableName;
    }

    @Override
    public void addColumns(int aLineNo,
                           int aColNo,
                           ErrorMessageSet aErrors,
                           List<ColumnIdent> aColIdents,
                           List<String> aColVals) {
        Set<String> colNames = new HashSet<String>(m_table.getColumnNamesAsSet());
        int idx = 0;
        for (ColumnIdent cn : aColIdents) {
            String colName = cn.getColName();
            IColumn column = m_table.getColumnByName(colName);
            if (column == null) {
                aErrors.addError(cn.getColLineNo(), cn.getColColNo(),
                                 "Undefined table \"%s\".",
                                 colName);
            } else {
                m_colNames.add(cn.getColName());
                IType aType = column.getType();
                assert(aType instanceof Type);
                m_colTypes.add((Type)aType);
                // TODO: When we process Neutrinos in values, do type checking here.
                //       tuac to the rescue, eh?
                m_colVals.add(aColVals.get(idx));
                colNames.remove(cn.getColName());
            }
        }
        /*
         * Sweep up all the ones which were not given
         * explicit values.
         */
        for (String cn : colNames) {
            IColumn column = m_table.getColumnByName(cn);
            if (!column.hasDefaultValue() && !column.isNullable()) {
                aErrors.addError(aLineNo, aColNo,
                                 "Column \"%s\" is not nullable and has no specified value.",
                                 cn);
            }
            m_colNames.add(cn);
            assert(column.getType() instanceof Type);
            m_colTypes.add((Type)column.getType());
            if (column.hasDefaultValue()) {
                m_colVals.add(column.getDefaultValue());
            } else if (column.isNullable()) {
                // TODO: This needs to add the Null Neutrino.
                m_colVals.add(null);
            } else {
                assert false;
            }
        }
    }
}