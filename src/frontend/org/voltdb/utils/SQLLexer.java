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

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLDDLInfo;
import org.voltcore.logging.VoltLogger;

public class SQLLexer
{
    private static final VoltLogger COMPILER_LOG = new VoltLogger("COMPILER");

    // Match single-line comments
    private static final Pattern SINGLE_LINE_COMMENT_MATCH = Pattern.compile(
            "^\\s*" + // start of line, 0 or more whitespace
            "--" + // start of comment
            ".*$"); // everything to end of line

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

    public static boolean isComment(String sql)
    {
        Matcher commentMatcher = SINGLE_LINE_COMMENT_MATCH.matcher(sql);
        return commentMatcher.matches();
    }

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

    /**
     * Glean some basic info about DDL statements sent to HSQLDB
     */
    public static HSQLDDLInfo preprocessHSQLDDL(String ddl) {
        ddl = stripComments(ddl);

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

    /** Remove c-style comments globally and -- comments from the end of lines */
    public static String stripComments(String ddl) {
        ddl = removeCStyleComments(ddl);
        StringBuilder sb = new StringBuilder();
        String[] ddlLines = ddl.split("\n");
        for (String ddlLine : ddlLines) {
            sb.append(stripCommentFromLine(ddlLine)).append(' ');
        }
        return sb.toString();
    }

    private static final Pattern STRIP_CSTYLE_COMMENTS = Pattern.compile(
            "/\\*(.|\\n)*?\\*/"
            );

    /** Remove c-style comments from a string aggressively */
    public static String removeCStyleComments(String ddl) {
        String[] parts = STRIP_CSTYLE_COMMENTS.split(ddl);
        return StringUtils.join(parts);
    }

    /** Strip -- comments from the end of a single line */
    public static String stripCommentFromLine(String ddlLine) {
        boolean inQuote = false;
        char quoteChar = ' '; // will be written before use
        boolean lastCharWasDash = false;
        int length = ddlLine.length();

        for (int i = 0; i < length; i++) {
            char c = ddlLine.charAt(i);
            if (inQuote) {
                if (quoteChar == c) {
                    inQuote = false;
                }
            }
            else {
                if (c == '-') {
                    if (lastCharWasDash) {
                        return ddlLine.substring(0, i - 1);
                    }
                    else {
                        lastCharWasDash = true;
                    }
                }
                else {
                    lastCharWasDash = false;
                    if (c == '\"' || c == '\'') {
                        inQuote = true;
                        quoteChar = c;
                    }
                }
            }
        }

        return ddlLine;
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
        boolean hadWLMatch = false;
        for (Pattern wl : WHITELISTS) {
            Matcher wlMatcher = wl.matcher(sql);
            if (wlMatcher.matches()) {
                hadWLMatch = true;
            }
        }

        if (!hadWLMatch) {
            COMPILER_LOG.info("Statement: " + sql + " , failed to match any whitelist");
            return false;
        }

        for (Pattern bl : BLACKLISTS) {
            Matcher blMatcher = bl.matcher(sql);
            if (blMatcher.matches()) {
                COMPILER_LOG.info("Statement: " + sql + " , failed blacklist: " + blMatcher.toString());
                return false;
            }
        }

        return hadWLMatch;
    }
}
