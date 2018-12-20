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

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;

/**
 * This class provides a wrapper function around Calcite SqlParser to parse a query.
 * @since 8.4
 * @author Yiqun Zhang
 */
public class VoltSqlParser {

    /**
     * Given a SQL statement (could be either a DDL or DQL/DML),
     * parse it into a {@link SqlNode}.
     * @param sql the SQL statement to parse.
     * @return the parsed SqlNode tree for it.
     */
    public static SqlNode parse(String sql) throws SqlParseException {
        return SqlParser.create(sql, VoltFrameworkConfig.PARSER_CONFIG).parseQuery(sql);
    }
}
