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

package org.voltdb.parser;

import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl;

/**
 * This class provides a factory method to create a parser that is used by VoltDB.
 */
public class ParserFactory {

    /**
     * Given a SQL statement (could be either a DDL or DQL/DML),
     * create a {@link org.apache.calcite.sql.parser.SqlParser} for it.
     * @param sql The SQL statement to parse.
     * @return The created SQL parser.
     */
    public static SqlParser create(String sql) {
        return SqlParser.create(sql, SqlParser.configBuilder()
                                              .setParserFactory(SqlDdlParserImpl.FACTORY)
                                              .build());
    }
}
