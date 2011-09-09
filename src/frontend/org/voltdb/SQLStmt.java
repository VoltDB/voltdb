/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb;

import org.voltdb.catalog.Statement;

/**
 * <p>A simple wrapper of a parameterized SQL statement. VoltDB uses this instead of
 * a Java String type for performance reasons and to cache statement meta-data like
 * result schema, etc..</p>
 *
 * <p>SQLStmts are used exclusively in subclasses of {@link VoltProcedure}</p>
 *
 * @see VoltProcedure
 */
public class SQLStmt {
    String sqlText;
    String joinOrder;

    /**
     * Construct a SQLStmt instance from a SQL statement.
     *
     * @param sqlText Valid VoltDB compliant SQL with question marks as parameter
     * place holders.
     */
    public SQLStmt(String sqlText) {
        this(sqlText, null);
    }

    /**
     * Construct a SQLStmt instance from a SQL statement.
     *
     * @param sqlText Valid VoltDB compliant SQL with question marks as parameter
     * place holders.
     * @param joinOrder separated list of tables used by the query specifying the order they should be joined in
     */
    public SQLStmt(String sqlText, String joinOrder) {
        this.sqlText = sqlText;
        this.joinOrder = joinOrder;
    }

    /**
     * Get the text of the SQL statement represented.
     *
     * @return String containing the text of the SQL statement represented.
     */
    public String getText() {
        return sqlText;
    }

    /**
     * Get the join order hint supplied in the constructor.
     *
     * @return String containing the join order hint.
     */
    public String getJoinOrder() {
        return joinOrder;
    }

    byte statementParamJavaTypes[];
    int numStatementParamJavaTypes;

    long fragGUIDs[];
    int numFragGUIDs;

    Statement catStmt = null;
}
