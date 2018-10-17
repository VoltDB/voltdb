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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.voltdb.parser.ParserFactory;

/**
 * SqlTask stores a parsed query.
 * @since 8.4
 * @author Yiqun Zhang
 */
public class SqlTask {
    private final String m_sqlString;
    private final SqlNode m_parsedQuery;

    /**
     * Create a {@link SqlTask} from a SQL query string.
     * @param sql the query string.
     * @throws SqlParseException when the parsing goes wrong.
     * @throws IllegalArgumentException if the SQL string is null or empty.
     */
    public SqlTask(String sql) throws SqlParseException {
        if ((sql == null) || (sql = sql.trim()).isEmpty()) { // remove any spaces or newlines
            throw new IllegalArgumentException("Can't plan empty or null SQL.");
        }
        m_sqlString = sql;
        SqlParser parser = ParserFactory.create(sql);
        m_parsedQuery = parser.parseStmt();
    }

    /**
     * Create a {@link SqlTask} from a SQL query string and a parsed {@link SqlNode}.
     * @param sql     the query string.
     * @param sqlNode the parsed SqlNode
     */
    public SqlTask(String sql, SqlNode sqlNode) {
        m_sqlString = sql;
        m_parsedQuery = sqlNode;
    }

    /**
     * Tell if this {@link SqlTask} is a DDL task.
     * @return true if this {@code SqlTask} is a DDL task.
     */
    public boolean isDDL() {
        return m_parsedQuery.isA(SqlKind.DDL);
    }

    /**
     * Get the original SQL query text.
     * @return the original SQL query text.
     */
    public String getSQL() {
        return m_sqlString;
    }

    /**
     * Get the parsed query node tree.
     * @return the parsed query node tree.
     */
    public SqlNode getParsedQuery() {
        return m_parsedQuery;
    }
}
