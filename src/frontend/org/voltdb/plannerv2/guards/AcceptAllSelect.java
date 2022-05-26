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

package org.voltdb.plannerv2.guards;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Accepts all SELECT queries.
 *
 * @author Yiqun Zhang
 * @since 9.0
 */
public class AcceptAllSelect extends CalciteCompatibilityCheck {
    private static final Pattern IGNORE_PATTERN;
    private static final Pattern SELECT_PATTERN;

    static {
        // TODO: MICROS is a keyword in the TO_TIMESTAMP() function.
        // QUARTER, MILLISECOND, MILLIS, MICROSECOND are keywords in the DATEADD() function.
        // WEEK is a fake keyword using in the test of DATEADD()
        // NOW is a keyword in volt
        // The ignore list should be resolved when we introduce those functions in calcite
        String[] ignoreList = new String[]{"'MICROS'", "'QUARTER'", "'MILLISECOND'", "'MILLIS'", "'MICROSECOND'",
                "'WEEK'", "'NOW'"};
        String pattern = String.format(".*Column (%s) not found in any table.*",
                StringUtils.join(ignoreList, "|"));
        IGNORE_PATTERN = Pattern.compile(pattern, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

        final String selectStart = "( *\\( *)*(SELECT)";
        SELECT_PATTERN = Pattern.compile(selectStart, Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

    }

    @Override protected final boolean doCheck(String sql) {
        return SELECT_PATTERN.matcher(sql).find();
    }

    /**
     * Some of the validation errors happened because of the lack of support we ought to add
     * to Calcite. We need to fallback for those cases.
     * @param message the error message.
     * @return true if we need to fallback.
     */
    public static boolean fallback(String message) {
        if (message.contains("No match found for function signature")) {
            return true;
        }
        // TODO: select myUdf(NULL) from T; select myUdf(?) from T; will throw this exception
        // Calcite will try to infer the parameter types in function
        // works for non-Udf(null) see ENG-15222
        // not works for Udf(null) see #SqlValidatorImpl.inferUnknownTypes
        if (message.contains("Illegal use of 'NULL'") || message.contains("Illegal use of dynamic parameter")) {
            return true;
        }

        if (IGNORE_PATTERN.matcher(message).find()) {
            return true;
        }
        return false;
    }
}
