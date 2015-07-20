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
 package org.voltdb.sqlparser.syntax.symtab;

import java.util.List;
import java.util.Set;

import org.voltdb.sqlparser.syntax.grammar.SQLParserParser.Column_nameContext;

public interface ITable extends ITop {

    /**
     * adds Column parameter newcolumn to table
     * @param newcolumn
     */
    public abstract void addColumn(String name, IColumn column);

    public abstract String toString();

    /**
     * Given a column name, find the IColumn object.
     *
     * @param aColumnName
     * @return
     */
    public IColumn getColumnByName(String aColumnName);

    /**
     * Return a list of all column names, in declaration order.  The order
     * of names in this list must be identical to the order of types returned
     * by ITable.getColumnTypes.
     *
     * @return
     */
    public abstract List<String> getColumnNames();
    /**
     * Return a list of all column types, in declaration order.  The order
     * of types in this list must be identical to the order of names returned
     * by ITable.getColumnNames.
     * @return
     */
    public abstract List<IType> getColumnTypes();

    /**
     * Return a copy of the set of strings which name columns.
     *
     * @return
     */
    public abstract Set<String> getColumnNamesAsSet();

}
