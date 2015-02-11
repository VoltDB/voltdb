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

import org.hsqldb_voltpatches.HSQLDDLInfo;

/**
 * Provides an API for performing various parse operations on SQL/DML/DDL text.
 *
 * Keep the regular expressions private and just expose methods needed for parsing.
 */
public class HSQLLexer extends SQLPatternFactory
{
    //private static final VoltLogger COMPILER_LOG = new VoltLogger("COMPILER");

    //===== Private parsing data

    // Glean some basic info about DDL statements sent to HSQLDB
    private static final Pattern HSQL_DDL_PREPROCESSOR =
        SPF.statementLeader(
            SPF.capture("verb", SPF.tokenAlternatives("create", "drop", "alter")),
            SPF.capture("unique", SPF.optional(SPF.tokenAlternatives("unique", "assumeunique"))),
            SPF.capture("object", SPF.tokenAlternatives("table", "view", "index")),
            SPF.capture("name", SPF.symbol()),  // table/view/index name
            SPF.optional(SPF.clause(
                    SPF.token("on"),
                    SPF.capture("subject", SPF.symbol())))
        ).compile();

    // Does the ddl statement end with cascade or have if exists in the right place?
    private static final Pattern DDL_IFEXISTS_OR_CASCADE_CHECK =
        SPF.statementTrailer(
            SPF.capture("ie", SPF.optional(SPF.clause(SPF.token("if"), SPF.token("exists")))),
            SPF.capture("c", SPF.optional(SPF.clause(SPF.token("cascade"))))
        ).compile();

    //===== Public interface

    /**
     * Glean some basic info about DDL statements sent to HSQLDB
     */
    public static HSQLDDLInfo preprocessHSQLDDL(String ddl) {
        ddl = SQLLexer.stripComments(ddl);

        Matcher matcher = HSQL_DDL_PREPROCESSOR.matcher(ddl);
        if (matcher.find()) {
            String verbString = matcher.group("verb");
            HSQLDDLInfo.Verb verb = HSQLDDLInfo.Verb.get(verbString);
            if (verb == null) {
                return null;
            }

            String nounString = matcher.group("object");
            HSQLDDLInfo.Noun noun = HSQLDDLInfo.Noun.get(nounString);
            if (noun == null) {
                return null;
            }

            String name = matcher.group("name");
            if (name == null) {
                return null;
            }

            String secondName = matcher.group("subject");
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
