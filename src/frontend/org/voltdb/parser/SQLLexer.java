/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.List;
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
public class SQLLexer extends SQLPatternFactory
{
    //===== Fundamental (not derived) parsing data

    private static class VerbToken
    {
        final String token;
        final boolean supported;

        VerbToken(String token, boolean supported)
        {
            this.token = token;
            this.supported = supported;
        }
    };

    private final static VerbToken[] VERB_TOKENS = {
        // Supported verbs
        new VerbToken("alter", true),
        new VerbToken("create", true),
        new VerbToken("drop", true),
        new VerbToken("export", true),
        new VerbToken("partition", true),
        new VerbToken("dr", true),
        new VerbToken("set", true),
        // Unsupported verbs
        new VerbToken("import", false)
    };

    private static class ObjectToken
    {
        final String token;
        final boolean renameable;

        ObjectToken(String token, boolean renameable)
        {
            this.token = token;
            this.renameable = renameable;
        }
    };

    private final static ObjectToken[] OBJECT_TOKENS = {
        // Rename-able objects
        new ObjectToken("table", true),
        new ObjectToken("stream", true),
        new ObjectToken("column", true),
        new ObjectToken("index", true),
        // Non-rename-able objects
        new ObjectToken("view", false),
        new ObjectToken("procedure", false),
        new ObjectToken("role", false)
    };

    private final static String[] MODIFIER_TOKENS = {
        "assumeunique", "unique"
    };

    static final char BLOCK_DELIMITER_CHAR = '#';
    static final String BLOCK_DELIMITER = "###";

    //===== Special non-DDL/DML/SQL patterns

    // Match single-line comments
    private static final Pattern PAT_SINGLE_LINE_COMMENT = Pattern.compile(
            "^\\s*" + // start of line, 0 or more whitespace
            "--" + // start of comment
            ".*$"); // everything to end of line

    private static final Pattern PAT_STRIP_CSTYLE_COMMENTS = Pattern.compile(
            "/\\*(.|\\n)*?\\*/"
            );

    //===== Derived parsing data (populated in static block on first demand)

    // Simplest possible SQL DDL token lexer. (set in static block)
    private static Pattern PAT_ANY_DDL_FIRST_TOKEN = null;

    // All handled patterns. (set in static block)
    private static CheckedPattern[] WHITELISTS = null;

    // All rejected patterns. (set in static block)
    private static CheckedPattern[] BLACKLISTS = null;

    // Extracts the table or stream name for DDL batch conflicting command checks.
    private static final Pattern PAT_TABLE_DDL_PREAMBLE =
        SPF.statementLeader(
            SPF.capture(SPF.tokenAlternatives("create", "drop")),   // DDL commands we're looking for
            SPF.tokenAlternatives("table", "stream"),               // target is table or stream
            SPF.capture(SPF.databaseObjectName())                   // table name (captured)
        ).compile("PAT_TABLE_DDL_PREAMBLE");

    // Matches the start of a SELECT statement
    private static final Pattern PAT_SELECT_STATEMENT_PREAMBLE =
        SPF.statementLeader(
            SPF.token("select")
        ).compile("PAT_SELECT_STATEMENT_PREAMBLE");

    // Capture group number defns for regex below.
    // Don't use capture labels because it is not supported in 1.6
    // and this class needs to compile in 1.6.
    private static final int PARENTTYPE_GROUP=1;
    @SuppressWarnings("unused") // We don't get this group as of now
    private static final int PARENTNAME_GROUP=2;
    private static final int CHILDTYPE_GROUP=3;
    @SuppressWarnings("unused") // We don't get this group as of now
    private static final int CHILDNAME_GROUP=4;
    // Pattern for plausible ALTER...RENAME statements.
    // Keep the matching loose in order to support clear messaging.
    private static final Pattern PAT_ALTER_RENAME =
        SPF.statementLeader(
            SPF.token("alter"),
            SPF.capture(SPF.databaseObjectTypeName()),
            SPF.capture(SPF.databaseObjectName()),
            SPF.optional(
                SPF.clause(
                    SPF.token("alter"),
                    SPF.capture(SPF.databaseObjectTypeName()),
                    SPF.capture(SPF.databaseObjectName())
                )
            ),
            SPF.token("rename"), SPF.token("to")
        ).compile("PAT_ALTER_RENAME");

    //========== Public Methods ==========

    /**
     * Check if a SQL string is a comment.
     * @param sql  SQL string
     * @return     true if it's a comment
     */
    public static boolean isComment(String sql)
    {
        Matcher commentMatcher = PAT_SINGLE_LINE_COMMENT.matcher(sql);
        return commentMatcher.matches();
    }

    /**
     * Test if character is block delimiter
     * @param c  character to test
     * @return   true if c is block delimiter
     */
    public static boolean isBlockDelimiter(char c)
    {
        return c == BLOCK_DELIMITER_CHAR;
    }

    /**
     * Get the DDL token, if any, at the start of this statement.
     * @return returns token, or null if it wasn't DDL
     */
    public static String extractDDLToken(String sql)
    {
        String ddlToken = null;
        Matcher ddlMatcher = PAT_ANY_DDL_FIRST_TOKEN.matcher(sql);
        if (ddlMatcher.find()) {
            ddlToken = ddlMatcher.group(1).toLowerCase();
        }
        return ddlToken;
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

    /**
     * Get the table name for a CREATE or DROP DDL statement.
     * @return returns token, or null if the DDL isn't (CREATE|DROP) TABLE
     */
    public static String extractDDLTableName(String sql)
    {
        Matcher matcher = PAT_TABLE_DDL_PREAMBLE.matcher(sql);
        if (matcher.find()) {
            return matcher.group(2).toLowerCase();
        }
        return null;
    }

    /**
     * Naive filtering for stuff we haven't implemented yet.
     * Hopefully this gets whittled away and eventually disappears.
     *
     * @param sql  statement to check
     * @return     rejection explanation string or null if accepted
     */
    public static String checkPermitted(String sql)
    {
        /*
         *  IMPORTANT: Black-lists are checked first because they know more about
         * what they don't like about a statement and can provide a better message.
         * It requires that black-lists patterns be very selective and that they
         * don't mind seeing statements that wouldn't pass the white-lists.
         */

        //=== Check against blacklists, must not be rejected by any.

        for (CheckedPattern cp : BLACKLISTS) {
            CheckedPattern.Result result = cp.check(sql);
            if (result.matcher != null) {
                return String.format("%s, in statement: %s", result.explanation, sql);
            }
        }

        //=== Check against whitelists, must be accepted by at least one.

        boolean hadWLMatch = false;
        for (CheckedPattern cp : WHITELISTS) {
            if (cp.matches(sql)) {
                hadWLMatch = true;
                break;
            }
        }
        if (!hadWLMatch) {
            return String.format("AdHoc DDL contains an unsupported statement: %s", sql);
        }

        // The statement is permitted.
        return null;
    }

    /**
     * Split SQL statements on semi-colons with quoted string and comment support.
     *
     * Degenerate formats such as escape as the last character or unclosed strings are ignored and
     * left to the SQL parser to complain about. This is a simple string splitter that errs on the
     * side of not splitting.
     *
     * Regexes are avoided.
     *
     * Handle single and double quoted strings and backslash escapes. Backslashes escape a single
     * character.
     *
     * Handle double-dash (single line) and C-style (muli-line) comments. Nested C-style comments
     * are not supported.
     *
     * @param sql raw SQL text to split
     * @return list of individual SQL statements
     */
    public static List<String> splitStatements(final String sql) {
        List<String> statements = new ArrayList<String>();
        // Use a character array for efficient character-at-a-time scanning.
        char[] buf = sql.toCharArray();
        // Set to null outside of quoted segments or the quote character inside them.
        Character cQuote = null;
        // Set to null outside of comments or to the string that ends the comment.
        String sCommentEnd = null;
        // Index to start of current statement.
        int iStart = 0;
        // Index to current character.
        // IMPORTANT: The loop is structured in a way that requires all if/else/... blocks to bump
        // iCur appropriately. Failure of a corner case to bump iCur will cause an infinite loop.
        boolean statementIsComment = false;
        boolean inStatement = false;
        int iCur = 0;
        while (iCur < buf.length) {
            // Eat up whitespace outside of a statement
            if (!inStatement) {
                if (Character.isWhitespace(buf[iCur])) {
                    iCur++;
                    iStart = iCur;
                }
                else {
                    inStatement = true;
                }
            }
            else if (sCommentEnd != null) {
                // Processing the interior of a comment. Check if at the comment or buffer end.
                if (iCur >= buf.length - sCommentEnd.length()) {
                    // Exit
                    iCur = buf.length;
                } else if (String.copyValueOf(buf, iCur, sCommentEnd.length()).equals(sCommentEnd)) {
                    // Move past the comment end.
                    iCur += sCommentEnd.length();
                    sCommentEnd = null;
                    // If the comment is the whole of the statement so far, terminate it
                    if (statementIsComment) {
                        String statement = String.copyValueOf(buf, iStart, iCur - iStart).trim();
                        if (!statement.isEmpty()) {
                            statements.add(statement);
                        }
                        iStart = iCur;
                        statementIsComment = false;
                        inStatement = false;
                    }
                } else {
                    // Keep going inside the comment.
                    iCur++;
                }
            } else if (cQuote != null) {
                // Processing the interior of a quoted string.
                if (buf[iCur] == '\\') {
                    // Skip the '\' escape and the trailing single escaped character.
                    // Doesn't matter if iCur is beyond the end, it won't be used in that case.
                    iCur += 2;
                } else if (buf[iCur] == cQuote) {
                    // Look at the next character to distinguish a double escaped quote
                    // from the end of the quoted string.
                    iCur++;
                    if (iCur < buf.length) {
                        if (buf[iCur] != cQuote) {
                            // Not a double escaped quote - end of quoted string.
                            cQuote = null;
                        } else {
                            // Move past the double escaped quote.
                            iCur++;
                        }
                    }
                } else {
                    // Move past an ordinary character.
                    iCur++;
                }
            } else {
                // Outside of a quoted string - watch for the next separator, quote or comment.
                if (buf[iCur] == ';') {
                    // Add terminated statement (if not empty after trimming).
                    String statement = String.copyValueOf(buf, iStart, iCur - iStart).trim();
                    if (!statement.isEmpty()) {
                        statements.add(statement);
                    }
                    iStart = iCur + 1;
                    iCur = iStart;
                    inStatement = false;
                } else if (buf[iCur] == '"' || buf[iCur] == '\'') {
                    // Start of quoted string.
                    cQuote = buf[iCur];
                    iCur++;
                } else if (iCur <= buf.length - 2) {
                    // Comment (double-dash or C-style)?
                    if (buf[iCur] == '-' && buf[iCur+1] == '-') {
                        // One line double-dash comment start.
                        sCommentEnd = "\n"; // Works for *IX (\n) and Windows (\r\n).
                        if (iCur == iStart) {
                            statementIsComment = true;
                        }
                        iCur += 2;
                    } else if (buf[iCur] == '/' && buf[iCur+1] == '*') {
                        // Multi-line C-style comment start.
                        sCommentEnd = "*/";
                        if (iCur == iStart) {
                            statementIsComment = true;
                        }
                        iCur += 2;
                    } else {
                        // Not a comment start, move past this character.
                        iCur++;
                    }
                } else {
                    // Move past a non-quote/non-separator character.
                    iCur++;
                }
            }
        }
        // Get the last statement, if any.
        if (iStart < buf.length) {
            String statement = String.copyValueOf(buf, iStart, iCur - iStart).trim();
            if (!statement.isEmpty()) {
                statements.add(statement);
            }
        }
        return statements;
    }

    /**
     * Check if a statement is a SELECT.
     * @param statement  statement to check
     * @return           true if it's a SELECT statement
     */
    public static boolean isSelect(String statement)
    {
        return PAT_SELECT_STATEMENT_PREAMBLE.matcher(statement).matches();
    }

    //========== Private ==========

    /**
     * Initialize derived data
     */
    static
    {
        // Simplest possible SQL DDL token lexer
        String[] verbsAll = new String[VERB_TOKENS.length];
        for (int i = 0; i < VERB_TOKENS.length; ++i) {
            verbsAll[i] = VERB_TOKENS[i].token;
        }
        PAT_ANY_DDL_FIRST_TOKEN =
            SPF.statementLeader(
                SPF.capture(SPF.tokenAlternatives(verbsAll)),
                SPF.anyClause()
            ).compile("PAT_ANY_DDL_FIRST_TOKEN");

        // Whitelists for acceptable statement preambles.
        WHITELISTS = new CheckedPattern[] {
            new WhitelistSupportedPreamblePattern(),
            new CheckedPattern(SQLParser.SET_GLOBAL_PARAM_FOR_WHITELIST) {
                @Override
                String explainMatch(Matcher matcher) {
                    return null;
                }
            }
        };

        BLACKLISTS = new CheckedPattern[] {
            new BlacklistUnsupportedPreamblePattern(),
            new BlacklistRenamePattern()
        };
    }

    /** Remove c-style comments from a string aggressively */
    private static String removeCStyleComments(String ddl)
    {
        // Avoid Apache commons StringUtils.join() to minimize client dependencies.
        StringBuilder sb = new StringBuilder();
        for (String part : PAT_STRIP_CSTYLE_COMMENTS.split(ddl)) {
            sb.append(part);
        }
        return sb.toString();
    }

    /**
     * Find information about an object type token, if it's a known object type.
     * @param objectTypeName  object type name to look up
     * @return                object token information or null if it wasn't found
     */
    private static ObjectToken findObjectToken(String objectTypeName)
    {
        if (objectTypeName != null) {
            for (ObjectToken ot : OBJECT_TOKENS) {
                if (ot.token.equalsIgnoreCase(objectTypeName)) {
                    return ot;
                }
            }
        }
        return null;
    }

    /**
     * Abstract base for whitelists and blacklists
     */
    private static abstract class CheckedPattern
    {
        Pattern pattern;

        CheckedPattern(Pattern pattern)
        {
            this.pattern = pattern;
        }

        static class Result
        {
            // non-null Matcher with groups() set if it matched
            Matcher matcher = null;
            // optional explanation, e.g. blacklist rejection message, or null if it didn't match
            String explanation = null;
        }

        /**
         * Check if statement matches.
         * @param statement  statement to match against
         * @return           result object with m
         */
        Result check(String statement)
        {
            Result result = new Result();
            Matcher matcher = this.pattern.matcher(statement);
            if (matcher.matches()) {
                result.matcher = matcher;
                result.explanation = this.explainMatch(matcher);
            }
            return result;
        }

        /**
         * Simplified yes/no match check
         * @param statement  statement to match against
         * @return           true if it matches
         */
        boolean matches(String statement)
        {
            return this.check(statement).matcher != null;
        }

        // Override to provide an explanation, e.g. for blacklist rejection.
        abstract String explainMatch(Matcher matcher);
    }

    /**
     * Whitelist matcher for supported two token preambles.
     *
     * Provides no explanation.
     */
    private static class WhitelistSupportedPreamblePattern extends CheckedPattern
    {
        private static Pattern initPattern()
        {
            // All handled (white-listed) patterns.
            String[] secondTokens = new String[OBJECT_TOKENS.length + MODIFIER_TOKENS.length];
            for (int i = 0; i < OBJECT_TOKENS.length; ++i) {
                secondTokens[i] = OBJECT_TOKENS[i].token;
            }
            // Modifier tokens are supported in the place of object tokens following
            // a verb to allow the "verb modifier object" pattern like "CREATE UNIQUE INDEX".
            // For simplicity, "CREATE UNIQUE" et. al. are considered sufficient evidence that
            // the statement is a permitted white-listed DDL statement.
            // We seem to be more concerned about accidentally permitting
            // "CREATE <non-permitted-object> ..."
            // than "CREATE UNIQUE <non-permitted-object> ...".
            // Otherwise, we'd require the modifiers to be part of a nested
            // "modifier object" subpattern.
            for (int j = 0; j < MODIFIER_TOKENS.length; ++j) {
                secondTokens[OBJECT_TOKENS.length + j] = MODIFIER_TOKENS[j];
            }
            int supportedVerbCount = 0;
            for (int i = 0; i < VERB_TOKENS.length; ++i) {
                if (VERB_TOKENS[i].supported) {
                    supportedVerbCount++;
                }
            }
            String[] verbsSupported = new String[supportedVerbCount];
            supportedVerbCount = 0;     // Reuse to build supported verb array.
            for (int i = 0; i < VERB_TOKENS.length; ++i) {
                if (VERB_TOKENS[i].supported) {
                    verbsSupported[supportedVerbCount++] = VERB_TOKENS[i].token;
                }
            }
            Pattern whitelistPattern =
                SPF.statementLeader(
                    SPF.clause(
                        SPF.tokenAlternatives(verbsSupported),
                        SPF.tokenAlternatives(secondTokens)
                    )
                ).compile("PAT_WHITELISTS-PREAMBLES");
            return whitelistPattern;
        }

        WhitelistSupportedPreamblePattern()
        {
            super(initPattern());
        }

        // Whitelist match provides no explanation.
        @Override
        String explainMatch(Matcher matcher)
        {
            return null;
        }
    }

    /**
     * Blacklists known unsupported statement preambles and explains rejections.
     */
    private static class BlacklistUnsupportedPreamblePattern extends CheckedPattern
    {
        private static Pattern initPattern()
        {
            int unsupportedVerbCount = 0;
            for (int i = 0; i < VERB_TOKENS.length; ++i) {
                if (!VERB_TOKENS[i].supported) {
                    unsupportedVerbCount++;
                }
            }
            String[] verbsNotSupported = new String[unsupportedVerbCount];
            unsupportedVerbCount = 0;   // Reuse to build unsupported verb array.
            for (int i = 0; i < VERB_TOKENS.length; ++i) {
                if (!VERB_TOKENS[i].supported) {
                    verbsNotSupported[unsupportedVerbCount++] = VERB_TOKENS[i].token;
                }
            }
            Pattern blacklistPattern =
                SPF.statementLeader(
                    SPF.capture(SPF.tokenAlternatives(verbsNotSupported))
                ).compile("PAT_BLACKLISTS-PREAMBLES");
            return blacklistPattern;
        }

        BlacklistUnsupportedPreamblePattern()
        {
            super(initPattern());
        }

        /**
         * Provide a match explanation, assuming it's a rejection.
         */
        @Override
        String explainMatch(Matcher matcher)
        {
            return String.format("Statement is not supported: %s", matcher.group(1).toUpperCase());
        }
    }

    /**
     * Blacklists ALTER/RENAME and provides focused rejection explanations.
     */
    private static class BlacklistRenamePattern extends CheckedPattern
    {
        BlacklistRenamePattern()
        {
            super(PAT_ALTER_RENAME);
        }

        /**
         * (Internal)
         * See if there's something to say about a parent or child target object type.
         * @param typeName   object type name to check
         * @param isParent   true when a child object is available and this is the parent
         * @return           explanation string or null when a child still needs checking
         */
        private static String getExplanation(String typeName, boolean isParent)
        {
            assert typeName != null;
            ObjectToken token = findObjectToken(typeName);
            if (token == null) {
                return String.format("AdHoc DDL ALTER/RENAME refers to an unknown object type '%s'", typeName);
            }
            if (isParent) {
                // The parent is okay, still need to check the child.
                return null;
            }
            if (!token.renameable) {
                return String.format("AdHoc DDL ALTER/RENAME is not supported for object type '%s'", typeName);
            }
            return "AdHoc DDL ALTER/RENAME is not yet supported";
        }

        /**
         * Provide a match explanation, assuming it's a rejection.
         */
        @Override
        String explainMatch(Matcher matcher)
        {
            String parentType = matcher.group(PARENTTYPE_GROUP);
            String childType = matcher.group(CHILDTYPE_GROUP);
            // See if there's something to say about the parent object type.
            String explanation = getExplanation(parentType, childType != null);
            // If not see if there's something to say about the child type, when applicable.
            if (explanation == null) {
                explanation = getExplanation(childType, false);
            }
            return explanation;
        }
    }
}
