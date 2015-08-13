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

import java.util.List;

import org.voltdb.sqlparser.syntax.IColumnIdent;
import org.voltdb.sqlparser.syntax.symtab.ITable;
import org.voltdb.sqlparser.syntax.util.ErrorMessageSet;

/**
 * The interface to insert statements.  This will be needed later.
 *
 * @author bwhite
 */
public interface IInsertStatement {

    void addTable(ITable aTable);

    /**
     * Insert a collection of columns, given the columns' names
     * and values.  The values are Strings now, but they should be
     * Neutrinos.
     *
     * @param colNames
     * @param colVals
     */
    void addColumns(int aLineNo,
                    int aColNo,
                    ErrorMessageSet aErrs,
                    List<IColumnIdent> aColNames,
                    List<String> aColVals);

}
