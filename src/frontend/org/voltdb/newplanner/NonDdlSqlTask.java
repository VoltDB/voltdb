/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

package org.voltdb.newplanner;

import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.List;

/**
 * SqlTask for non-DDL query. Stores a parsed query and a List of {@link SqlLiteral}.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class NonDdlSqlTask extends SqlTask {
    private List<SqlLiteral> m_sqlLiteralList;

    /**
     * Create a {@link NonDdlSqlTask} from a SQL query string.
     *
     * @param sql the query string.
     * @throws SqlParseException        when the parsing goes wrong.
     * @throws IllegalArgumentException if the SQL string is null or empty.
     */
    public NonDdlSqlTask(String sql) throws SqlParseException {
        super(sql);
    }

    /**
     * Create a {@link NonDdlSqlTask} from a SQL query string and a parsed {@link SqlNode}.
     *
     * @param sql     the query string.
     * @param sqlNode the parsed SqlNode.
     */
    public NonDdlSqlTask(String sql, SqlNode sqlNode) {
        super(sql, sqlNode);
    }

    /**
     * Use {@link ParameterizeVisitor} to parameterize all of the literals in the parsed SqlNode in-place.
     * Add all the parameter values to a List of {@link SqlLiteral}.
     *
     * @return the parameterized SqlNode.
     */
    public SqlNode parameterize() {
        ParameterizeVisitor visitor = new ParameterizeVisitor();
        getParsedQuery().accept(visitor);
        m_sqlLiteralList = visitor.getSqlLiteralList();
        return getParsedQuery();
    }

    /**
     * Get the List of parameter values.
     *
     * @return the List of parameter values.
     */
    public List<SqlLiteral> getSqlLiteralList() {
        return m_sqlLiteralList;
    }
}
