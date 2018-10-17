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
 * This class provides a factory method to create a {@link SqlTask} or its subclasses.
 *
 * @author Chao Zhou
 * @since 8.4
 */
public class SqlTaskFactory {
    /**
     * Given a SQL statement (could be either a DDL or DQL/DML),
     * create a {@link SqlTask} for it.
     *
     * @param sql the SQL statement.
     * @return a {@link SqlTask} created from the SQL statement.
     */
    public static SqlTask createSqlTask(String sql) throws SqlParseException {
        if ((sql == null) || (sql = sql.trim()).isEmpty()) { // remove any spaces or newlines
            throw new IllegalArgumentException("Can't plan empty or null SQL.");
        }
        SqlParser parser = ParserFactory.create(sql);
        SqlNode parsedQuery = parser.parseStmt();
        if (parsedQuery.isA(SqlKind.DDL)) {
            return new SqlTask(sql, parsedQuery);
        }
        return new NonDdlSqlTask(sql, parsedQuery);
    }
}
