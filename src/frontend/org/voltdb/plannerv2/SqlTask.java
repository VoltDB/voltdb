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

/**
 * SqlTask stores a parsed query tree.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public interface SqlTask {

    /**
     * @return true if this {@link SqlTask} is a DDL task.
     */
    boolean isDDL();

    /**
     * @return the original SQL query text.
     */
    String getSQL();

    /**
     * @return the parsed query node tree.
     */
    SqlNode getParsedQuery();

    /**
     * Create a basic {@link SqlTask} from a SQL query.
     *
     * @param sql the SQL query
     * @return the created SqlTask.
     * @throws SqlParseException when the parsing goes wrong.
     * @throws IllegalArgumentException if the SQL string is null or empty.
     */
    static SqlTask from(String sql) throws SqlParseException {
        return new SqlTaskImpl(sql);
    }
}
