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

package org.voltdb.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.hsqldb_voltpatches.HSQLDDLInfo;

/**
 * Provides an API for performing various parse operations on SQL/DML/DDL text.
 *
 * Keep the regular expressions private and just expose methods needed for parsing.
 */
public class HSQLLexer
{
    //private static final VoltLogger COMPILER_LOG = new VoltLogger("COMPILER");

    //========== Private Parsing Data ==========

    // Glean some basic info about DDL statements sent to HSQLDB
    private static final Pattern HSQL_DDL_PREPROCESSOR = Pattern.compile(
            "^\\s*" +  // start of line, 0 or more whitespace
            "(create|drop|alter)" + // DDL commands we're looking for
            "\\s+" + // one or more whitespace
            "((assume)?unique\\s+)?" + // assume | unique for index parsing
            "(table|view|index)" +
            "\\s+" + // one or more whitespace
            "([a-z][a-z0-9_]*)" + // table/view/index name symbol
            "(\\s+on\\s+([a-z][a-z0-9_]*))?" + // on table/view second name
            ".*$", // all the rest
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );

    // does ddl the statement end with cascade or have if exists in the right place
    private static final Pattern DDL_IFEXISTS_OR_CASCADE_CHECK = Pattern.compile(
            "^.*?" + // start of line, then anything (greedy)
            "(?<ie>\\s+if\\s+exists)?" + // may contain if exists preceded by whitespace
            "(?<c>\\s+cascade)?" + // may contain cascade preceded by whitespace
            "\\s*;?\\s*" + // then optional whitespace, a single optional semi, then ws
            "$", // end of line
            Pattern.CASE_INSENSITIVE | Pattern.DOTALL
            );

    //========== Public Interface ==========

    /**
     * Glean some basic info about DDL statements sent to HSQLDB
     */
    public static HSQLDDLInfo preprocessHSQLDDL(String ddl) {
        ddl = SQLLexer.stripComments(ddl);

        Matcher matcher = HSQL_DDL_PREPROCESSOR.matcher(ddl);
        if (matcher.find()) {
            String verbString = matcher.group(1);
            HSQLDDLInfo.Verb verb = HSQLDDLInfo.Verb.get(verbString);
            if (verb == null) {
                return null;
            }

            String nounString = matcher.group(4);
            HSQLDDLInfo.Noun noun = HSQLDDLInfo.Noun.get(nounString);
            if (noun == null) {
                return null;
            }

            String name = matcher.group(5);
            if (name == null) {
                return null;
            }

            String secondName = matcher.group(7);
            if (secondName != null) {
                secondName = secondName.toLowerCase();
            }

            // cascade/if exists are interesting on alters and drops
            boolean cascade = false;
            boolean ifexists = false;
            if (verb != HSQLDDLInfo.Verb.CREATE) {
                matcher = DDL_IFEXISTS_OR_CASCADE_CHECK.matcher(ddl);
                if (matcher.matches()) {
                    ifexists = matcher.group("ie") != null;
                    cascade = matcher.group("c") != null;
                }
            }

            return new HSQLDDLInfo(verb, noun, name.toLowerCase(), secondName, cascade, ifexists);
        }
        return null;
    }
}
