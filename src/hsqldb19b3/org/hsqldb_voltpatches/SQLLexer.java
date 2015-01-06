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

package org.hsqldb_voltpatches;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltcore.logging.VoltLogger;

public class SQLLexer
{
    private static final VoltLogger COMPILER_LOG = new VoltLogger("COMPILER");
    // Simplest possible SQL DDL token lexer
    private static final Pattern DDL_MATCH = Pattern.compile(
            "^\\s*" +  // start of line, 0 or more whitespace
            "(alter|create|drop|export|import|partition)" + // tokens we consider DDL
            "\\s+", // one or more whitespace
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL
            );

    private static final Pattern WHITELIST_1 = Pattern.compile(
            "^\\s*" +  // start of line, 0 or more whitespace
            "(alter|create|drop|export|partition)" + // DDL we're ready to handle
            "\\s+" + // one or more whitespace
            "(table|assumeunique|unique|index|view|procedure|role)" +
            "\\s+" + // one or more whitespace
            ".*$", // all the rest
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL
            );

    private static final Pattern[] WHITELISTS = { WHITELIST_1 };

    // Don't accept these DDL tokens yet
    private static final Pattern BLACKLIST_1 = Pattern.compile(
            "^\\s*" +  // start of line, 0 or more whitespace
            "(import)" + // DDL we're not ready to handle
            "\\s+" + // one or more whitespace
            ".*$", // all the rest
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL
            );

    // Also, don't accept RENAME for the tokens we do take yet
    private static final Pattern BLACKLIST_2 = Pattern.compile(
            "^\\s*" +  // start of line, 0 or more whitespace
            "alter" + // DDL we're ready to handle
            "\\s+" + // one or more whitespace
            "(table|index)" + // but it's gotta be on tables or indexes
            "\\s+" + // one or more whitespace
            ".*" + // some stuff
            "\\s+" + // one or more whitespace
            "rename" + // VERBOTEN
            "\\s+" + // one or more whitespace
            "to" + // VERBOTEN, CONT'D
            "\\s+" + // one or more whitespace
            ".*$", // all the rest
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL
            );

    private static final Pattern[] BLACKLISTS = { BLACKLIST_1, BLACKLIST_2 };

    /**
     * Get the DDL token, if any, at the start of this statement.
     * @return returns token, or null if it wasn't DDL
     */
    public static String extractDDLToken(String sql)
    {
        String ddlToken = null;
        Matcher ddlMatcher = DDL_MATCH.matcher(sql);
        if (ddlMatcher.find()) {
            ddlToken = ddlMatcher.group(1).toLowerCase();
        }
        return ddlToken;
    }

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
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL
            );

    // does ddl the statement end with cascade
    private static final Pattern DDL_CASCADE_CHECK = Pattern.compile(
            "^.*" + // start of line, then anything
            "cascade" + // must contain cascade
            "\\s*;?\\s*" + // then optional whitespace, a sigle optional semi, then ws
            "$", // end of line
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL
            );

    /**
     * CREATE, ALTER or DROP
     */
    public static enum HSQLDDLVerb {
        CREATE, ALTER, DROP;

        public static HSQLDDLVerb get(String name) {
            if (name.equalsIgnoreCase("CREATE")) {
                return CREATE;
            }
            else if (name.equalsIgnoreCase("ALTER")) {
                return ALTER;
            }
            else if (name.equalsIgnoreCase("DROP")) {
                return DROP;
            }
            else {
                return null;
            }
        }
    }

    /**
     * TABLE, INDEX or VIEW
     */
    public static enum HSQLDDLNoun {
        TABLE, INDEX, VIEW;

        public static HSQLDDLNoun get(String name) {
            if (name.equalsIgnoreCase("TABLE")) {
                return TABLE;
            }
            else if (name.equalsIgnoreCase("INDEX")) {
                return INDEX;
            }
            else if (name.equalsIgnoreCase("VIEW")) {
                return VIEW;
            }
            else {
                return null;
            }
        }
    }

    /**
     *  Information about DDL passed to HSQL that allows it to be better understood by VoltDB
     */
    public static class HSQLDDLInfo {
        public final HSQLDDLVerb verb;
        public final HSQLDDLNoun noun;
        public final String name;
        public final String secondName;
        public boolean cascade;

        HSQLDDLInfo(HSQLDDLVerb verb, HSQLDDLNoun noun, String name, String secondName, boolean cascade) {
            this.verb = verb; this.noun = noun; this.name = name; this.secondName = secondName; this.cascade = cascade;
        }
    }

    /**
     * Glean some basic info about DDL statements sent to HSQLDB
     */
    public static HSQLDDLInfo preprocessHSQLDDL(String ddl) {
        Matcher matcher = HSQL_DDL_PREPROCESSOR.matcher(ddl);
        if (matcher.find()) {
            String verbString = matcher.group(1);
            HSQLDDLVerb verb = HSQLDDLVerb.get(verbString);
            if (verb == null) {
                return null;
            }

            String nounString = matcher.group(4);
            HSQLDDLNoun noun = HSQLDDLNoun.get(nounString);
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

            // cascade is interesting on alters and drops
            boolean cascade = false;
            if (verb != HSQLDDLVerb.CREATE) {
                matcher = DDL_CASCADE_CHECK.matcher(ddl);
                cascade = matcher.matches();
            }

            return new HSQLDDLInfo(verb, noun, name.toLowerCase(), secondName, cascade);
        }
        return null;
    }

    // Extracts the table name for DDL batch conflicting command checks.
    private static final Pattern CREATE_DROP_TABLE_PREAMBLE = Pattern.compile(
            "^\\s*" +  // start of line, 0 or more whitespace
            "(create|drop)" + // DDL commands we're looking for
            "\\s+" + // one or more whitespace
            "table" +
            "\\s+" + // one or more whitespace
            "([a-z][a-z0-9_]*)" + // table name symbol
            ".*$", // all the rest
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL
            );



    /**
     * Get the table name for a CREATE or DROP DDL statement.
     * @return returns token, or null if the DDL isn't (CREATE|DROP) TABLE
     */
    public static String extractDDLTableName(String sql)
    {
        Matcher matcher = CREATE_DROP_TABLE_PREAMBLE.matcher(sql);
        if (matcher.find()) {
            return matcher.group(2).toLowerCase();
        }
        return null;
    }

    // Naive filtering for stuff we haven't implemented yet.
    // Hopefully this gets whittled away and eventually disappears.
    public static boolean isPermitted(String sql)
    {
        for (Pattern wl : WHITELISTS) {
            Matcher wlMatcher = wl.matcher(sql);
            if (!wlMatcher.matches()) {
                COMPILER_LOG.info("Statement: " + sql + " , failed whitelist: " + wlMatcher.toString());
                return false;
            }
        }
        for (Pattern bl : BLACKLISTS) {
            Matcher blMatcher = bl.matcher(sql);
            if (blMatcher.matches()) {
                COMPILER_LOG.info("Statement: " + sql + " , failed blacklist: " + blMatcher.toString());
                return false;
            }
        }
        return true;
    }
}
