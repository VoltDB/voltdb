/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * Direct API provider for parsing a SQL query.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class VoltFastSqlParser {

    /**
     * Given a SQL statement (could be either a DDL or DQL/DML),
     * parse it into a {@link SqlNode}.
     * If the sql ends with a semicolon, make sure that it is removed before passing to SqlParser,
     * which would not recognize query that ends with semicolon.
     *
     * @param sql the SQL statement to parse.
     * @return the parsed SqlNode tree for it.
     */
    public static SqlNode parse(String sql) throws SqlParseException {
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return SqlParser.create(sql, VoltFrameworkConfig.PARSER_CONFIG).parseQuery(sql);
    }
}
