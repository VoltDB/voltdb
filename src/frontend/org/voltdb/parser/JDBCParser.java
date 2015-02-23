/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides an API for performing various lexing operations on SQL/DML/DDL text.
 * Ideally it shouldn't be doing "parsing", i.e. language-aware token processing.
 * In reality the code is not split cleanly and lexing and parsing overlap a bit.
 *
 * Keep the regular expressions private and just expose methods needed for parsing.
 *
 * Avoid external dependencies since this is linked with the client.
 */
public class JDBCParser
{
    //private static final VoltLogger COMPILER_LOG = new VoltLogger("COMPILER");

    //========== Private Parsing Data ==========

    private static final Pattern PAT_CALL_WITH_PARAMETERS = Pattern.compile(
            "^\\s*\\{\\s*call\\s+([^\\s()]+)\\s*\\(([?,\\s]+)\\)\\s*\\}\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PAT_CALL_WITHOUT_PARAMETERS = Pattern.compile(
            "^\\s*\\{\\s*call\\s+([^\\s()]+)\\s*\\}\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern PAT_CLEAN_CALL_PARAMETERS = Pattern.compile("[\\s,]+");

    //========== Public Methods ==========

    /**
     * Object class returned by parseJDBCCall()
     */
    public static class ParsedCall
    {
        public String sql;
        public int parameterCount;

        ParsedCall(String sql, int parameterCount)
        {
            this.sql = sql;
            this.parameterCount = parameterCount;
        }
    }

    /**
     * Parse call statements for JDBC.
     * @param jdbcCall  statement to parse
     * @return          object with parsed data or null if it didn't parse
     * @throws SQLParser.Exception
     */
    public static ParsedCall parseJDBCCall(String jdbcCall) throws SQLParser.Exception
    {
        Matcher m = PAT_CALL_WITH_PARAMETERS.matcher(jdbcCall);
        if (m.matches()) {
            String sql = m.group(1);
            int parameterCount = PAT_CLEAN_CALL_PARAMETERS.matcher(m.group(2)).replaceAll("").length();
            return new ParsedCall(sql, parameterCount);
        }
        m = PAT_CALL_WITHOUT_PARAMETERS.matcher(jdbcCall);
        if (m.matches()) {
            return new ParsedCall(m.group(1), 0);
        }
        return null;
    }
}
