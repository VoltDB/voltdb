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

package org.voltdb.plannerv2;

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;

/**
 * SqlTask stores a parsed query.
 * @since 8.4
 * @author Yiqun Zhang
 */
public class SqlTaskImpl implements SqlTask {

    private final String m_sqlString;
    private final SqlNode m_parsedQuery;

    /**
     * Create a {@link SqlTaskImpl} from a SQL query string.
     * @param sql the query string.
     * @throws SqlParseException when the parsing goes wrong.
     * @throws IllegalArgumentException if the SQL string is null or empty.
     */
    SqlTaskImpl(String sql) throws SqlParseException {
        if ((sql == null) || (sql = sql.trim()).isEmpty()) { // remove any spaces or newlines
            throw new IllegalArgumentException("Can't plan empty or null SQL.");
        }
        m_sqlString = sql;
        m_parsedQuery = VoltSqlParser.parse(sql);
    }

    /**
     * Tell if this {@link SqlTaskImpl} is a DDL task.
     * @return true if this {@code SqlTask} is a DDL task.
     */
    @Override public boolean isDDL() {
        return m_parsedQuery.isA(SqlKind.DDL);
    }

    @Override public boolean isDQL() {
        return m_parsedQuery.getKind() == SqlKind.SELECT;
    }

    @Override public boolean isDML() {
        return m_parsedQuery.isA(SqlKind.DML);
    }

    /**
     * Get the original SQL query text.
     * @return the original SQL query text.
     */
    @Override public String getSQL() {
        return m_sqlString;
    }

    /**
     * Get the parsed query node tree.
     * @return the parsed query node tree.
     */
    @Override public SqlNode getParsedQuery() {
        return m_parsedQuery;
    }

    @Override public String toString() {
        return getSQL();
    }
}
