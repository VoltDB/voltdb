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

package org.voltdb.jdbc;

import java.sql.*;

public class SQLError
{
    public static final String CONNECTION_UNSUCCESSFUL = "08001";
    public static final String CONNECTION_CLOSED = "08003";
    public static final String CONNECTION_FAILURE = "08006";
    public static final String GENERAL_ERROR = "s1000";
    public static final String ILLEGAL_ARGUMENT = "s1009";
    public static final String ILLEGAL_STATEMENT = "s1010";
    public static final String QUERY_PARSING_ERROR = "s1011";
    public static final String INVALID_QUERY_TYPE = "s1012";
    public static final String UNTERMINATED_STRING = "22024";
    public static final String COLUMN_NOT_FOUND = "42S22";
    public static final String PARAMETER_NOT_FOUND = "42S23";
    public static final String CONVERSION_NOT_FOUND = "42S72";
    public static final String TRANSLATION_NOT_FOUND = "42S82";

    public static SQLException get(String sqlState)
    {
        return new SQLException(Resources.getString("SQLState." + sqlState), sqlState);
    }

    public static SQLException get(String sqlState, Object... args)
    {
        return new SQLException(Resources.getString("SQLState." + sqlState + "." + args.length, args), sqlState);
    }

    public static SQLException get(Throwable cause)
    {
        return new SQLException(GENERAL_ERROR, cause);
    }

    public static SQLException get(Throwable cause, String sqlState)
    {
        return new SQLException(Resources.getString("SQLState." + sqlState), sqlState, cause);
    }

    public static SQLException get(Throwable cause, String sqlState, Object... args)
    {
        return new SQLException(Resources.getString("SQLState." + sqlState + "." + args.length, args), sqlState, cause);
    }

    public static SQLException noSupport()
    {
        return new SQLFeatureNotSupportedException();
    }

    public static boolean isConnectionError(String status)
    {
        return (status.equals(CONNECTION_UNSUCCESSFUL) ||
                status.equals(CONNECTION_CLOSED) ||
                status.equals(CONNECTION_FAILURE));
    }
}

