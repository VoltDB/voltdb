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

package org.voltdb.planner;

import java.util.HashMap;
import java.util.Map;

import org.apache.calcite.sql.SqlKind;

/**
 * An abstract class for SqlHanders which handles various types of SQL queries.
 * @since 8.4
 * @author Yiqun Zhang
 */
public abstract class SqlHandler {

    public abstract void doAction(SqlTask task);

    private static final Map<SqlKind, SqlHandler> m_sqlHandlerBySqlKind;

    static {
        m_sqlHandlerBySqlKind = new HashMap<>();
    }

    /**
     * The factory method to get the {@link SqlHandler} implementation class
     * for a specific kind of query.
     * @param kind the kind of query.
     * @return the {@link SqlHandler} implementation.
     */
    public static SqlHandler ofKind(SqlKind kind) {
        SqlHandler sqlHandler = m_sqlHandlerBySqlKind.get(kind);
        if (sqlHandler == null) {
            return DefaultSqlHandler.INSTANCE;
        }
        return sqlHandler;
    }
}
