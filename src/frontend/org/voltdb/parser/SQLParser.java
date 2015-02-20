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

import java.io.File;
import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.utils.Encoder;

import com.google_voltpatches.common.collect.ImmutableMap;

/**
 * Provides an API for performing various parse operations on SQL/DML/DDL text.
 *
 * Keep the regular expressions private and just expose methods needed for parsing.
 */
public class SQLParser extends SQLPatternFactory
{
    public static class Exception extends RuntimeException
    {
        public Exception(String message, Object... args)
        {
            super(String.format(message, args));
        }

        public Exception(Throwable cause)
        {
            super(cause.getMessage(), cause);
        }

        public Exception(Throwable cause, String message, Object... args)
        {
            super(String.format(message, args), cause);
        }

        private static final long serialVersionUID = -4043500523038225173L;
    }

    //private static final VoltLogger COMPILER_LOG = new VoltLogger("COMPILER");

    //========== Private Parsing Data ==========

    /**
     * Pattern: PARTITION PROCEDURE|TABLE ...
     *
     * Capture groups:
     *  (1) target type: "procedure" or "table"
     */
    private static final Pattern PAT_PARTITION_ANY_PREAMBLE =
            SPF.statement(
                SPF.token("partition"),
                SPF.capture(SPF.tokenAlternatives("procedure", "table")),
                SPF.anyClause()
            ).compile();

    /**
     * Pattern: PARTITION TABLE tablename ON COLUMN columnname
     *
     * NB supports only unquoted table and column names
     *
     * Capture groups:
     *  (1) table name
     *  (2) column name
     */
    private static final Pattern PAT_PARTITION_TABLE =
        SPF.statement(
            SPF.token("partition"), SPF.token("table"), SPF.capture(SPF.ddlName()),
            SPF.token("on"), SPF.token("column"), SPF.capture(SPF.ddlName())
        ).compile();

    /**
     * PARTITION PROCEDURE procname ON TABLE tablename COLUMN columnname [PARAMETER paramnum]
     *
     * NB supports only unquoted table and column names
     *
     * Capture groups:
     *  (1) Procedure name
     *  (2) Table name
     *  (3) Column name
     *  (4) Parameter number
     */
    private static final Pattern PAT_PARTITION_PROCEDURE =
        SPF.statement(
            SPF.token("partition"), SPF.token("procedure"), SPF.capture(SPF.ddlName()),
            SPF.token("on"), SPF.token("table"), SPF.capture(SPF.ddlName()),
            SPF.token("column"), SPF.capture(SPF.ddlName()),
            SPF.optional(SPF.clause(SPF.token("parameter"), SPF.capture(SPF.integer())))
        ).compile();

    //TODO: Convert to pattern factory usage below this point.
    //TODO: Consider implementing FIXME/TODO comments -- and TESTING --
    // prior to conversion to pattern factory.

    /**
     * CREATE PROCEDURE from Java class statement regex
     * NB supports only unquoted table and column names
     * Capture groups:
     *  (1) ALLOW/PARTITION clauses - needs further parsing
     *  (2) Class name
     */
    private static final Pattern PAT_CREATE_PROCEDURE_FROM_CLASS = Pattern.compile(String.format(
            "(?i)" +                  // ignore case instruction
            "\\A" +                   // beginning of statement
            "CREATE\\s+PROCEDURE" +   // CREATE PROCEDURE tokens with whitespace separator
            "((?:\\s+%s)*)" +         // (group 1) optional ALLOW and or PARTITION clause(s)
            "\\s+FROM\\s+CLASS\\s+" + // FROM CLASS tokens with whitespace terminators
            "([\\w$.]+)" +            // (group 2) class name
            "\\s*" +                  // trailing whitespace
            ";\\z",                   // required semicolon at end of statement
            formatCreateProcedureClause(true)));

    /**
     * CREATE PROCEDURE with single SELECT or DML statement regex
     * NB supports only unquoted table and column names
     * Capture groups:
     *  (1) Procedure name
     *  (2) ALLOW/PARTITION clauses - needs further parsing
     *  (3) SELECT or DML statement
     */
    private static final Pattern PAT_CREATE_PROCEDURE_FROM_SQL = Pattern.compile(String.format(
            "(?i)" +                    // ignore case instruction
            "\\A" +                     // beginning of statement
            "CREATE\\s+PROCEDURE\\s+" + // CREATE PROCEDURE tokens with whitespace terminators
            "([\\w.$]+)" +              // (group 1) procedure name
            "((?:\\s+%s)*)" +           // (group 2) optional ALLOW and or PARTITION clause(s)
            "\\s+AS\\s+" +              // AS token with surrounding whitespace
            "(.+)" +                    // (group 3) SELECT or DML statement
            ";\\z",                     // required semicolon at end of statement
            formatCreateProcedureClause(true)));

    /**
     * CREATE PROCEDURE with inline implementation script, e.g. Groovy, statement regex
     * NB supports only unquoted table and column names
     * Capture groups:
     *  (1) Procedure name
     *  (2) ALLOW/PARTITION clauses - needs further parsing
     *  (3) Code block content
     *  (4) Language name
     */
    private static final Pattern PAT_CREATE_PROCEDURE_AS_SCRIPT = Pattern.compile(
            String.format(
                //FIXME: ignore case instruction is redundant w/ Pattern.CASE_INSENSITIVE below
                "(?i)" +                    // ignore case instruction
                "\\A" +                     // beginning of statement
                "CREATE\\s+PROCEDURE\\s+" + // CREATE PROCEDURE tokens with whitespace terminators
                "([\\w.$]+)" +              // (group 1) procedure name
                "((?:\\s+%s)*)" +           // (group 2) optional ALLOW and or PARTITION clause(s)
                "\\s+AS\\s+" +              // AS token with surrounding whitespace
                "%s" +                      // leading block delimiter ###
                "(.+)" +                    // (group 3) code block content
                "%s" +                      // trailing block delimiter ###
                "\\s+LANGUAGE\\s+" +        // LANGUAGE token with surrounding whitespace
                "(GROOVY)" +                // (group 4) language name
                "\\S*" +                    // optional trailing whitespace BUG? should be \\s*?
                ";\\z",                     // required semicolon at end of statement
                formatCreateProcedureClause(true), SQLLexer.BLOCK_DELIMITER, SQLLexer.BLOCK_DELIMITER),
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);

    /**
     * Pattern for parsing the ALLOW and PARTITION clauses inside CREATE PROCEDURE statements.
     * Capture groups are enabled.
     */
    private static final Pattern PAT_ANY_CREATE_PROCEDURE_STATEMENT_CLAUSE = Pattern.compile(
            formatCreateProcedureClause(false),
            Pattern.CASE_INSENSITIVE | Pattern.MULTILINE | Pattern.DOTALL);

    /**
     * DROP PROCEDURE  statement regex
     */
    private static final Pattern PAT_DROP_PROCEDURE = Pattern.compile(
            "(?i)" +                                // ignore case
            "\\A" +                                 // beginning of statement
            "DROP" +                                // DROP token
            "\\s+" +                                // one or more spaces
            "PROCEDURE" +                           // PROCEDURE token
            "\\s+" +                                // one or more spaces
            "([\\w$.]+)" +                          // (1) class name or procedure name
            "(\\s+IF EXISTS)?" +                    // (2) <optional IF EXISTS>
            "\\s*" +                                // zero or more spaces
            ";" +                                   // semi-colon terminator
            "\\z"                                   // end of statement
            );

    /**
     * IMPORT CLASS with pattern for matching classfiles in
     * the current classpath.
     */
    private static final Pattern PAT_IMPORT_CLASS = Pattern.compile(
            "(?i)" +                                // (ignore case)
            "\\A" +                                 // (start statement)
            "IMPORT\\s+CLASS\\s+" +                 // IMPORT CLASS
            "([^;]+)" +                             // (1) class matching pattern
            ";\\z"                                  // (end statement)
            );

    /**
     * Regex to parse the CREATE ROLE statement with optional WITH clause.
     * Leave the WITH clause argument as a single group because regexes
     * aren't capable of producing a variable number of groups.
     * Capture groups are tagged as (1) and (2) in comments below.
     */
    private static final Pattern PAT_CREATE_ROLE = Pattern.compile(
            "(?i)" +                            // (ignore case)
            "\\A" +                             // (start statement)
            "CREATE\\s+ROLE\\s+" +              // CREATE ROLE
            "([\\w.$]+)" +                      // (1) <role name>
            "(?:\\s+WITH\\s+" +                 // (start optional WITH clause block)
                "(\\w+(?:\\s*,\\s*\\w+)*)" +    //   (2) <comma-separated argument string>
            ")?" +                              // (end optional WITH clause block)
            ";\\z"                              // (end statement)
            );

    /**
     * Regex to parse the DROP ROLE statement.
     * Capture group is tagged as (1) in comments below.
     */
    private static final Pattern PAT_DROP_ROLE = Pattern.compile(
            "(?i)" +                            // (ignore case)
            "\\A" +                             // (start statement)
            "DROP\\s+ROLE\\s+" +                // DROP ROLE
            "([\\w.$]+)" +                      // (1) <role name>
            "(\\s+IF EXISTS)?" +                // (2) <optional IF EXISTS>
            ";\\z"                              // (end statement)
            );

    /**
     * NB supports only unquoted table names
     * Captures 1 group, the table name.
     */
    private static final Pattern PAT_REPLICATE_TABLE = Pattern.compile(
            "(?i)" +                    // ignore case instruction
            "\\A" +                     // beginning of statement
            "REPLICATE\\s+TABLE\\s+" +  // REPLICATE TABLE tokens with whitespace terminators
            "([\\w$]+)" +               // (group 1) table name
            "\\s*" +                    // optional whitespace
            ";\\z"                      // semicolon at end of statement
            );

    /**
     * EXPORT TABLE statement regex
     * NB supports only unquoted table names
     * Capture groups are tagged as (1) in comments below.
     */
    private static final Pattern PAT_EXPORT_TABLE = Pattern.compile(
            "(?i)" +                            // (ignore case)
            "\\A"  +                            // start statement
            "EXPORT\\s+TABLE\\s+"  +            // EXPORT TABLE
            "([\\w.$]+)" +                      // (group 1) <table name>
            "(?:\\s+TO\\s+STREAM\\s+" +         // begin optional TO STREAM <export target> clause
            "([\\w.$]+)" +                      // (group 2) <export target>
            ")?" +                              // end optional TO STREAM <export target> clause
            "\\s*;\\z"                          // (end statement)
            );
    /**
     *  If the statement starts with either create procedure, create role, drop, partition,
     *  replicate, export, or import, the first match group is set to respectively procedure,
     *  role, drop, partition, replicate, export, or import.
     */
    private static final Pattern PAT_ALL_VOLTDB_STATEMENT_PREAMBLES = Pattern.compile(
            "(?i)" +                               // ignore case instruction
            //TODO: why not factor \\A out of the group -- it's common to all options
            "(" +                                  // start (group 1)
            // <= means zero-width positive lookbehind.
            // This means that the "CREATE\\s{}" is required to match but is not part of the capture.
            "(?<=\\ACREATE\\s{0,1024})" +          //TODO: 0 min whitespace should be 1?
            "(?:PROCEDURE|ROLE)|" +                // token options after CREATE
            // the rest are stand-alone token options
            "\\ADROP|" +
            "\\APARTITION|" +
            "\\AREPLICATE|" +
            "\\AEXPORT|" +
            "\\AIMPORT" +
            ")" +                                  // end (group 1)
            "\\s"                                  // one required whitespace to terminate keyword
            );

    //========== Patterns from SQLCommand ==========

    private static final Pattern EscapedSingleQuote = Pattern.compile("''", Pattern.MULTILINE);
    private static final Pattern SingleLineComments = Pattern.compile(
            "^\\s*" +       // ignore whitespace indent prior to comment
            "(\\/\\/|--)" + // (group 1 -- not used?) '--' or even C++-style '//' comment starter
            ".*$",          // commented out text continues to end of line
            Pattern.MULTILINE);
    private static final Pattern MidlineComments = Pattern.compile(
            "(\\/\\/|--)" + //  (group 1 -- not used?) '--' or even C++-style '//' comment starter
            ".*$",          // commented out text continues to end of line
            Pattern.MULTILINE);
    private static final Pattern Extract = Pattern.compile("'[^']*'", Pattern.MULTILINE);
    private static final Pattern AutoSplitParameters = Pattern.compile("[\\s,]+", Pattern.MULTILINE);

    /**
     * Matches a command followed by an SQL CRUD statement verb
     */
    private static final Pattern ParserStringKeywords = Pattern.compile(
            "\\s*" +                                          // 0 or more spaces
            "(" +                                             // start group 1
              "exec|execute|explain|explainproc" +            // command
            ")" +                                             // end group 1
            "\\s+" +                                          // one or more spaces
            "(" +                                             // start group 2
              "select|insert|update|upsert|delete|truncate" + // SQL CRUD statement verb
            ")" +                                             // end group 2
            "\\s+", // one or more spaces
            Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);

    // HELP can support sub-commands someday. Capture group 1 is the sub-command.
    private static final Pattern HelpToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "help" +          // required HELP command token
            "\\s*" +          // optional whitespace
            "(.*)" +          // optional subcommand BUG: subcommand should require prior whitespace.
            //FIXME: simplify -- a prior .* starves out all of these optional patterns
            "\\s*" +          // optional whitespace before semicolon
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +            // optional semicolons
            "\\s*$",          // optional terminating whitespace
            Pattern.CASE_INSENSITIVE);
    private static final Pattern GoToken = Pattern.compile(
            "^\\s*" +              // optional indent at start of line
            "go" +                 // required GO command token
            //TODO: allow "\\s*" + // optional whitespace before semicolon
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +                 // optional semicolons
            "\\s*$",               //
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ExitToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "(exit|quit)" +   // required command (group 1 -- probably not used)
            //TODO: allow "\\s*" + // optional whitespace before semicolon
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +            // optional semicolons
            "\\s*$",          // optional terminating whitespace
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ShowToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "list|show" +     // keyword alternatives -- BUG! SHOULD BE in a group (non-capturing)
            "\\s+" +          // one or more spaces
            "([a-z+])" +      // subcommand (group 1) -- 2 BUGS
            // long-term, move + outside '[]'s for "([a-z]+)" +
            // short-term, over-specify as "(proc|procedure|tables|classes)" + // See ***Note***
            //TODO: allow "\\s*" + // optional whitespace before semicolon
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +            // optional semicolons
            "\\s*$",          // optional terminating whitespace
            Pattern.CASE_INSENSITIVE);
    // ***Note***
    // TODO: It would be nice to be very forgiving initially about subcommands as in:
    // "([a-z]*)" + // alphabetic subcommand (group 1)
    // That would allow list/show command processing to be "locked in" here even if the
    // subcommand was later found to be garbage -- or missing.
    // A custom error message could usefully explain the correct list/show command options.
    // For now, with a strict match for only valid list/show subcommands, invalid or
    // missing subcommands fail through to the generic @AdHoc sql statement
    // processor which unhelpfully claims that show or list is some kind of syntax error.
    // The reason NOT to fix this right away is that sqlcmd has a bug so that it
    // tries to parse things like list and show commands on each new line of input
    // EVEN when it is processing the middle of a multi-line sql statement. So, it could
    // legitimately encounter the line "list integer" (in the middle of a create
    // table statement) and passing it on to @AdHoc is the correct behavior.
    // It should NOT treat this case as an invalid list subcommand.
    // Once that bug is fixed, we can go back to very general subcommand matching here.
    // For now (as before) look for only the specific valid subcommands and let @AdHoc
    // give its misleading error messages for the actual invalid subcommands.
    private static final Pattern SemicolonToken = Pattern.compile(
            "^.*" +           // match anything
            //FIXME: simplify -- a prior .* starves out this next optional pattern
            "\\s*" +          // optional whitespace
            ";+" +            // one required semicolon at end except for
            "\\s*$",          // optional terminating whitespace
            Pattern.CASE_INSENSITIVE);
    private static final Pattern RecallToken = Pattern.compile(
            "^\\s*" +      // optional indent at start of line
            "recall\\s+" + // required RECALL command token, whitespace terminated
            "([^;]+)" +    // required non-whitespace non-semicolon parameter (group 1)
            "\\s*" +       // optional whitespace
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +         // optional semicolons
            "\\s*$",       // optional terminating whitespace
            Pattern.CASE_INSENSITIVE);
    private static final Pattern FileToken = Pattern.compile(
            "^\\s*" +      // optional indent at start of line
            "file\\s+" +   // required FILE command token, whitespace terminated
            "['\"]*" +     // optional opening quotes of either kind (ignored) (?)
            "([^;'\"]+)" + // file path assumed to end at the next quote or semicolon
            "['\"]*" +     // optional closing quotes -- assumed to match opening quotes (?)
            "\\s*" +       // optional whitespace
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +         // optional semicolons
            "\\s*",        // more optional whitespace
            Pattern.CASE_INSENSITIVE);
    private static final Pattern FileBatchToken = Pattern.compile(
            "^\\s*" +      // optional indent at start of line
            "file\\s+" +   // required FILE command token, whitespace terminated
            "-batch\\s+" + // required -batch option token, whitespace terminated
            "['\"]*" +     // optional opening quotes of either kind (ignored) (?)
            "([^;'\"]+)" + // file path assumed to end at the next quote or semicolon
            "['\"]*" +     // optional closing quotes -- assumed to match opening quotes (?)
            "\\s*" +       // optional whitespace
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +         // optional semicolons
            "\\s*",        // more optional whitespace
            Pattern.CASE_INSENSITIVE);

    // Query Execution
    private static final Pattern ExecuteCall = Pattern.compile(
            "^" +              //TODO: allow indent at start of line -- or is input always trimmed?
            "(exec|execute)" + // required command alternatives TODO: make non-grouping?
            " ",               //TODO: allow any whitespace(s), not just ' '
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explain" (case insensitive).  We'll convert them to @Explain invocations.
    private static final Pattern ExplainCall = Pattern.compile(
            "^" +           //TODO: allow indent at start of line -- or is input always trimmed?
            "explain" +     // required command
            " ",            //TODO: allow any whitespace(s), not just ' '
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explainproc" (case insensitive).  We'll convert them to @ExplainProc invocations.
    private static final Pattern ExplainProcCall = Pattern.compile(
            "^" +           //TODO: allow indent at start of line -- or is input always trimmed?
            "explainProc" + // required command
            " ",            //TODO: allow any whitespace(s), not just ' '
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);

    private static final SimpleDateFormat DateParser = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final Pattern Unquote = Pattern.compile("^'|'$", Pattern.MULTILINE);

    private static final Map<String, String> FRIENDLY_TYPE_NAMES =
            ImmutableMap.<String, String>builder().put("tinyint", "byte numeric")
                                                  .put("smallint", "short numeric")
                                                  .put("int", "numeric")
                                                  .put("integer", "numeric")
                                                  .put("bigint", "long numeric")
                                                  .build();

    // The argument capture group for LOAD/REMOVE CLASSES loosely captures everything
    // through the trailing semi-colon. It relies on post-parsing code to make sure
    // the argument is reasonable.
    // Capture group 1 for LOAD CLASSES is the jar file.
    private static final SingleArgumentCommandParser loadClassesParser =
            new SingleArgumentCommandParser("load classes", "jar file");
    private static final SingleArgumentCommandParser removeClassesParser =
            new SingleArgumentCommandParser("remove classes", "class selector");
    private static final Pattern ClassSelectorToken = Pattern.compile(
            "^[\\w*.$]+$", Pattern.CASE_INSENSITIVE);

    //========== Public Interface ==========

    /**
     * Match statement against pattern for all VoltDB-specific statement preambles
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchAllVoltDBStatementPreambles(String statement)
    {
        return PAT_ALL_VOLTDB_STATEMENT_PREAMBLES.matcher(statement);
    }

    /**
     * Match statement against create role pattern
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchCreateRole(String statement)
    {
        return PAT_CREATE_ROLE.matcher(statement);
    }

    /**
     * Match statement against drop role pattern
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchDropRole(String statement)
    {
        return PAT_DROP_ROLE.matcher(statement);
    }

    /**
     * Match statement against export table pattern
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchExportTable(String statement)
    {
        return PAT_EXPORT_TABLE.matcher(statement);
    }

    /**
     * Match statement against import class pattern
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchImportClass(String statement)
    {
        return PAT_IMPORT_CLASS.matcher(statement);
    }

    /**
     * Match statement against pattern for start of any partition statement
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchPartitionStatementPreamble(String statement)
    {
        return PAT_PARTITION_ANY_PREAMBLE.matcher(statement);
    }

    /**
     * Match statement against pattern for partition table statement
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchPartitionTable(String statement)
    {
        return PAT_PARTITION_TABLE.matcher(statement);
    }

    /**
     * Match statement against pattern for partition procedure statement
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchPartitionProcedure(String statement)
    {
        return PAT_PARTITION_PROCEDURE.matcher(statement);
    }

    /**
     * Match statement against pattern for create procedure as SQL
     * with allow/partition clauses
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchCreateProcedureAsSQL(String statement)
    {
        return PAT_CREATE_PROCEDURE_FROM_SQL.matcher(statement);
    }

    /**
     * Match statement against pattern for create procedure as script
     * with allow/partition clauses
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchCreateProcedureAsScript(String statement)
    {
        return PAT_CREATE_PROCEDURE_AS_SCRIPT.matcher(statement);
    }

    /**
     * Match statement against pattern for create procedure from class
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchCreateProcedureFromClass(String statement)
    {
        return PAT_CREATE_PROCEDURE_FROM_CLASS.matcher(statement);
    }

    /**
     * Match statement against pattern for drop procedure
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchDropProcedure(String statement)
    {
        return PAT_DROP_PROCEDURE.matcher(statement);
    }

    /**
     * Match statement against pattern for allow/partition clauses of create procedure statement
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchAnyCreateProcedureStatementClause(String statement)
    {
        return PAT_ANY_CREATE_PROCEDURE_STATEMENT_CLAUSE.matcher(statement);
    }

    /**
     * Match statement against pattern for replicate table
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchReplicateTable(String statement)
    {
        return PAT_REPLICATE_TABLE.matcher(statement);
    }

    /**
     * Build an ALLOW clause that can be used to assign roles in CREATE PROCEDURE statements.
     * @param nonCapturing the regex group doesn't capture
     * @return pattern string
     *
     * Regex Description:
     * <pre>
     *  ALLOW\\s+                   -- ALLOW token and trailing whitespace
     *  ( or (?:                    -- begin group (1 if capturing)
     *    [\\w.$]+                  -- first role name
     *    (?:                       -- begin non-capturing group for additional role names
     *      \\s*,\\s*               -- comma and optional surrounding whitespace
     *      [\\w.$]+                -- additional role name
     *    )*                        -- end non-capturing group for additional role names with repetition
     *  )                           -- end group
     * </pre>
     *
     * Capture groups (if not non-capturing):
     *  (1) Entire role list with commas and internal whitespace
     */
    static String formatProcedureAllowClause(boolean nonCapturing)
    {
        final String groupPrefix = nonCapturing ? "(?:" : "(";
        return String.format("ALLOW\\s+%s[\\w.$]+(?:\\s*,\\s*[\\w.$]+)*)", groupPrefix);
    }

    /**
     * Build a PARTITION clause for a CREATE PROCEDURE statement.
     * NB supports only unquoted table and column names
     *
     * @param nonCapturing the regex group doesn't capture
     * @return pattern string
     *
     * Regex Description:
     * <pre>
     *  PARTITION\\s+               -- PARTITION token plus whitespace
     *  ON\\s+TABLE\\s+             -- ON TABLE tokens plus whitespace
     *  ([\\w$]+) or (?:[\\w$]+)    -- table name group (1 if capturing)
     *  \\s+COLUMN\\s+              -- COLUMN token and whitespace
     *  ([\\w$]+) or (?:[\\w$]+)    -- column name group (2 if capturing)
     *  (?:                         -- begin optional non-capturing parameter group
     *    \\s+PARAMETER\\s+         -- PARAMETER token and whitespace
     *    (\\d+)                    -- parameter number group (3 if capturing)
     *  )?                          -- end optional non-capturing parameter group
     * </pre>
     *
     * Capture groups (if not non-capturing):
     *  (1) Procedure name
     *  (2) Table name
     *  (3) Column name
     */
    static final String formatProcedurePartitionClause(boolean nonCapturing)
    {
        final String groupPrefix = nonCapturing ? "(?:" : "(";
        return String.format(
            "PARTITION\\s+ON\\s+TABLE\\s+%s[\\w$]+)\\s+COLUMN\\s+%s[\\w$]+)(?:\\s+PARAMETER\\s+%s\\d+))?",
            groupPrefix, groupPrefix, groupPrefix);
    }

    /**
     * Optional ALLOW or PARTITION clause that can modify CREATE PROCEDURE statements.
     * 2 repetitions support one of each possible clause. The code should check that
     * if there are two clauses it is one of each, not one repeated twice. The code
     * should also not care about ordering.
     *
     * Regex Description:
     * <pre>
     *  (?:"                        -- begin OR group with both possible clauses (non-capturing)
     *    (?:<allow-clause>)        -- ALLOW clause group (non-capturing)
     *    |                         -- OR operator
     *    (?:<partition-clause>)    -- PARTITION clause group (non-capturing)
     *  )                           -- end OR group with both possible clauses
     * </pre>
     *
     * Capture groups (if not non-capturing):
     *  (1) Entire role list with commas and internal whitespace
     */
    static String formatCreateProcedureClause(boolean nonCapturing)
    {
        return String.format("(?:(?:%s)|(?:%s))",
                             formatProcedureAllowClause(nonCapturing),
                             formatProcedurePartitionClause(nonCapturing));
    }

    //========== Other utilities from or for SQLCommand ==========

    /**
     * Parses locally-interpreted commands with a prefix and a single quoted
     * or unquoted string argument.
     * This can be more general if the need arises, e.g. more than one argument
     * or other argument data types.
     */
    public static class SingleArgumentCommandParser
    {
        final String prefix;
        final Pattern patPrefix;
        final Pattern patFull;
        final String argName;

        /**
         * Constructor
         * @param prefix  command prefix (blank separator is replaced with \s+)
         */
        SingleArgumentCommandParser(String prefix, String argName)
        {
            // Replace single space with flexible whitespace pattern.
            this.prefix = prefix.toUpperCase();
            String prefixPat = prefix.replace(" ", "\\s+");
            this.patPrefix = Pattern.compile(
                    String.format(
                        "^\\s*" + // optional indent at start of line
                        "%s" +    // modified prefix
                        "\\s" +   // a required whitespace
                        ".*$",    // arbitrary end matter?
                        prefixPat),
                    Pattern.CASE_INSENSITIVE);
            this.patFull = Pattern.compile(
                    String.format(
                        "^\\s*" +   // optional indent at start of line
                        "%s" +      // modified prefix
                        "\\s+" +    // a required whitespace
                        "([^;]+)" + // at least one other non-semicolon character (?)
                        "[;\\s]*$", // optional trailing semicolons or whitespace
                        prefixPat),
                    Pattern.CASE_INSENSITIVE);
            this.argName = argName;
        }

        /**
         * Parse line and return argument or null if parsing fails.
         * @param line  input line
         * @return      output argument or null if parsing fails
         */
        String parse(String line) throws SQLParser.Exception
        {
            // If it doesn't start with the expected command prefix return null.
            // Allows better errors for missing or inappropriate arguments,
            // rather than passing it along to the engine for a strange error.
            if (line == null || !this.patPrefix.matcher(line).matches()) {
                return null;
            }
            Matcher matcher = this.patFull.matcher(line);
            String arg = null;
            if (matcher.matches()) {
                arg = parseOptionallyQuotedString(matcher.group(1));
                if (arg == null) {
                    throw new SQLParser.Exception("Bad %s argument to %s: %s", this.argName, this.prefix, arg);
                }
            }
            else {
                throw new SQLParser.Exception("Missing %s argument to %s.", this.argName, this.prefix);
            }

            return arg;
        }

        private static String parseOptionallyQuotedString(String sIn) throws SQLParser.Exception
        {
            String sOut = null;
            if (sIn != null) {
                // If it starts with a quote make sure it ends with the same one.
                if (sIn.startsWith("'") || sIn.startsWith("\"")) {
                    if (sIn.length() > 1 && sIn.endsWith(sIn.substring(0, 1))) {
                        sOut = sIn.substring(1, sIn.length() - 1);
                    }
                    else {
                        throw new SQLParser.Exception("Quoted string is not properly closed: %s", sIn);
                    }
                }
                else {
                    // Unquoted string returned as is.
                    sOut = sIn;
                }
            }
            return sOut;
        }
    }

    public static List<String> parseQuery(String query)
    {
        if (query == null) {
            return null;
        }

        //* enable to debug */ System.err.println("Parsing command queue:\n" + query);
        /*
         * Mark any parser string keyword matches by interposing the #SQL_PARSER_STRING_KEYWORD#
         * tag. Which is later stripped at the end of this procedure. This tag is here to
         * aide the evaluation of SetOp and AutoSplit REGEXPs, meaning that an
         * 'explain select foo from bar will cause SetOp and AutoSplit match on the select as
         * is prefixed with the #SQL_PARSER_STRING_KEYWORD#
         *
         * For example
         *     'explain select foo from bar'
         *  becomes
         *     'explain #SQL_PARSER_STRING_KEYWORD#select foo from bar'
         */
        query = ParserStringKeywords.matcher(query).replaceAll(" $1 #SQL_PARSER_STRING_KEYWORD#$2 ");
        /*
         * strip out single line comments
         */
        query = SingleLineComments.matcher(query).replaceAll("");
        /*
         * replace all escaped single quotes with the #(SQL_PARSER_ESCAPE_SINGLE_QUOTE) tag
         */
        query = EscapedSingleQuote.matcher(query).replaceAll("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)");

        /*
         * move all single quoted strings into the string fragments list, and do in place
         * replacements with numbered instances of the #(SQL_PARSER_STRING_FRAGMENT#[n]) tag
         *
         */
        Matcher stringFragmentMatcher = Extract.matcher(query);
        ArrayList<String> stringFragments = new ArrayList<String>();
        int i = 0;
        while (stringFragmentMatcher.find()) {
            stringFragments.add(stringFragmentMatcher.group());
            query = stringFragmentMatcher.replaceFirst("#(SQL_PARSER_STRING_FRAGMENT#" + i + ")");
            stringFragmentMatcher = Extract.matcher(query);
            i++;
        }

        // strip out inline comments
        // At the point, all the quoted strings have been pulled out of the code because they may contain semicolons
        // and they will not be restored until after the split. So any user's quoted string will be safe here.
        query = MidlineComments.matcher(query).replaceAll("");

        String[] sqlFragments = query.split("\\s*;+\\s*");

        ArrayList<String> queries = new ArrayList<String>();
        for (String fragment : sqlFragments) {
            fragment = SingleLineComments.matcher(fragment).replaceAll("");
            fragment = fragment.trim();
            if (fragment.isEmpty()) {
                continue;
            }
            if (fragment.indexOf("#(SQL_PARSER_STRING_FRAGMENT#") > -1) {
                int k = 0;
                for (String strFrag : stringFragments) {
                    fragment = fragment.replace("#(SQL_PARSER_STRING_FRAGMENT#" + k + ")", strFrag);
                    k++;
                }
            }
            fragment = fragment.replace("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)", "''");
            fragment = fragment.replace("#SQL_PARSER_STRING_KEYWORD#","");
            queries.add(fragment);
        }
        return queries;
    }

    public static List<String> parseProcedureCallParameters(String query)
    {
        if (query == null) {
            return null;
        }

        query = SingleLineComments.matcher(query).replaceAll("");
        query = EscapedSingleQuote.matcher(query).replaceAll("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)");
        Matcher stringFragmentMatcher = Extract.matcher(query);
        ArrayList<String> stringFragments = new ArrayList<String>();
        int i = 0;
        while (stringFragmentMatcher.find()) {
            stringFragments.add(stringFragmentMatcher.group());
            query = stringFragmentMatcher.replaceFirst("#(SQL_PARSER_STRING_FRAGMENT#" + i + ")");
            stringFragmentMatcher = Extract.matcher(query);
            i++;
        }
        query = AutoSplitParameters.matcher(query).replaceAll(",");
        String[] sqlFragments = query.split("\\s*,+\\s*");
        ArrayList<String> queries = new ArrayList<String>();
        for (int j = 0; j<sqlFragments.length; j++) {
            sqlFragments[j] = sqlFragments[j].trim();
            if (sqlFragments[j].length() != 0) {
                if (sqlFragments[j].indexOf("#(SQL_PARSER_STRING_FRAGMENT#") > -1) {
                    for (int k = 0; k<stringFragments.size(); k++) {
                        sqlFragments[j] = sqlFragments[j].replace("#(SQL_PARSER_STRING_FRAGMENT#" + k + ")", stringFragments.get(k));
                    }
                }
                sqlFragments[j] = sqlFragments[j].replace("#(SQL_PARSER_ESCAPE_SINGLE_QUOTE)", "''");
                sqlFragments[j] = sqlFragments[j].trim();
                queries.add(sqlFragments[j]);
            }
        }
        return queries;
    }

    /**
     * Check for statement terminated by semi-colon.
     * @param statement  statement to check
     * @return           true if it is terminated by a semi-colon
     */
    public static boolean isSemiColonTerminated(String statement)
    {
        return SemicolonToken.matcher(statement).matches();
    }

    /**
     * Check for EXIT command.
     * @param statement  statement to check
     * @return           true if it is EXIT command
     */
    public static boolean isExitCommand(String statement)
    {
        return ExitToken.matcher(statement).matches();
    }

    /**
     * Check for GO command.
     * @param statement  statement to check
     * @return           true if it is GO command
     */
    public static boolean isGoCommand(String statement)
    {
        return GoToken.matcher(statement).matches();
    }

    /**
     * Results from parseRecallStatement
     */
    public static class ParseRecallResults
    {
        public int line;
        public String error;

        ParseRecallResults(int line, String error)
        {
            this.line = line;
            this.error = error;
        }
    }

    /**
     * Parse RECALL statement for sqlcmd.
     * @param statement  statement to parse
     * @param lineMax    maximum line # (0-n)
     * @return           results object or NULL if statement wasn't recognized
     */
    public static ParseRecallResults parseRecallStatement(String statement, int lineMax)
    {
        Matcher matcher = RecallToken.matcher(statement);
        if (matcher.matches()) {
            try {
                int line = Integer.parseInt(matcher.group(1)) - 1;
                if (line < 0 || line > lineMax) {
                    throw new NumberFormatException();
                }
                return new ParseRecallResults(line, null);
            }
            catch(NumberFormatException e)
            {
                return new ParseRecallResults(-1, String.format("Invalid RECALL reference: '%s'", matcher.group(1)));
            }
        }
        return null;
    }

    public static final class FileInfo {
        private final File m_file;
        private final boolean m_batch;

        FileInfo(String fileName, boolean b) {
            m_file = new File(fileName);
            m_batch = b;
        }

        public File getFile() {
            return m_file;
        }

        public boolean isBatch() {
            return m_batch;
        }
    }

    /**
     * Parse FILE statement for sqlcmd.
     * @param statement  statement to parse
     * @return           File object or NULL if statement wasn't recognized
     */
    public static FileInfo parseFileStatement(String statement)
    {
        Matcher batchMatcher = FileBatchToken.matcher(statement);
        if (batchMatcher.matches()) {
            return new FileInfo(batchMatcher.group(1), true);
        }
        Matcher matcher = FileToken.matcher(statement);
        if (matcher.matches()) {
            return new FileInfo(matcher.group(1), false);
        }

        return null;
    }

    /**
     * Parse SHOW or LIST statement for sqlcmd.
     * @param statement  statement to parse
     * @return           results object or NULL if statement wasn't recognized
     */
    public static String parseShowStatementSubcommand(String statement)
    {
        Matcher matcher = ShowToken.matcher(statement);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Parse HELP statement for sqlcmd.
     * The sub-command will be "" if the user just typed HELP.
     * @param statement  statement to parse
     * @return           Sub-command or NULL if statement wasn't recognized
     */
    public static String parseHelpStatement(String statement)
    {
        Matcher matcher = HelpToken.matcher(statement);
        if (matcher.matches()) {
            return matcher.group(1).trim();
        }
        return null;
    }

    /**
     * Parse a date string.
     * @param dateIn  input date string
     * @return        Date object
     * @throws SQLParser.Exception
     */
    public static Date parseDate(String dateIn) throws SQLParser.Exception
    {
        // Don't ask... Java is such a crippled language!
        DateParser.setLenient(true);

        // Remove any quotes around the timestamp value.  ENG-2623
        try {
            return DateParser.parse(dateIn.replaceAll("^\"|\"$", "").replaceAll("^'|'$", ""));
        }
        catch (ParseException e) {
            throw new SQLParser.Exception(e);
        }
    }

    /**
     * Results returned by parseExecuteCall()
     */
    public static class ExecuteCallResults
    {
        public String query = null;
        public String procedure = null;
        public List<String> params = null;
        public List<String> paramTypes = null;

        // Uppercase param.
        // Remove any quotes.
        // Trim
        private static String preprocessParam(String param)
        {
            if ((param.charAt(0) == '\'' && param.charAt(param.length()-1) == '\'') ||
                    (param.charAt(0) == '"' && param.charAt(param.length()-1) == '"')) {
                // The position of the closing quote, param.length()-1 is where to end the substring
                // to get a result with two fewer characters.
                param = param.substring(1, param.length()-1);
            }
            param = param.trim();
            param = param.toUpperCase();
            return param;
        }

        private static String friendlyTypeDescription(String paramType) {
            String friendly = FRIENDLY_TYPE_NAMES.get(paramType);
            if (friendly != null) {
                return friendly;
            }
            return paramType;
        }

        public Object[] getParameterObjects() throws SQLParser.Exception
        {
            Object[] objectParams = new Object[this.params.size()];
            int i = 0;
            try {
                for (; i < this.params.size(); i++) {
                    String paramType = this.paramTypes.get(i);
                    String param = this.params.get(i);
                    Object objParam = null;
                    // For simplicity, handle first the types that don't allow null as a special value.
                    if (paramType.equals("bit")) {
                        //TODO: upper/mixed case Yes and True should be treated as "1"?
                        //TODO: non-0 integers besides 1 should be treated as "1"?
                        //TODO: garbage values and null should be rejected, not accepted as "0":
                        //      (case-insensitive) "no"/"false"/"0" should be required for "0"?
                        if (param.equals("yes") || param.equals("true") || param.equals("1")) {
                            objParam = (byte)1;
                        } else {
                            objParam = (byte)0;
                        }
                    }
                    else if (paramType.equals("statisticscomponent") ||
                             paramType.equals("sysinfoselector") ||
                             paramType.equals("metadataselector")) {
                        objParam = preprocessParam(param);
                    }
                    else if ( ! "null".equalsIgnoreCase(param)) {
                        if (paramType.equals("tinyint")) {
                            objParam = Byte.parseByte(param);
                        }
                        else if (paramType.equals("smallint")) {
                            objParam = Short.parseShort(param);
                        }
                        else if (paramType.equals("int") || paramType.equals("integer")) {
                            objParam = Integer.parseInt(param);
                        }
                        else if (paramType.equals("bigint")) {
                            objParam = Long.parseLong(param);
                        }
                        else if (paramType.equals("float")) {
                            objParam = Double.parseDouble(param);
                        }
                        else if (paramType.equals("varchar")) {
                            objParam = Unquote.matcher(param).replaceAll("").replace("''","'");
                        }
                        else if (paramType.equals("decimal")) {
                            objParam = new BigDecimal(param);
                        }
                        else if (paramType.equals("timestamp")) {
                            objParam = parseDate(param);
                        }
                        else if (paramType.equals("varbinary") || paramType.equals("tinyint_array")) {
                            String val = Unquote.matcher(param).replaceAll("");
                            objParam = Encoder.hexDecode(val);
                            // Make sure we have an even number of characters, otherwise it is an invalid byte string
                            if (param.length() % 2 == 1) {
                                throw new SQLParser.Exception(
                                        "Invalid varbinary value (%s) (param %d) : "
                                        + "must have an even number of hex characters to be valid.",
                                        param, i+1);
                            }
                        }
                        else {
                            throw new SQLParser.Exception("Unsupported Data Type: %s", paramType);
                        }
                    } // else param is keyword "null", so leave objParam as null.
                    objectParams[i] = objParam;
                }
            } catch (NumberFormatException nfe) {
                throw new SQLParser.Exception(nfe,
                        "Invalid parameter:  Expected a %s value, got '%s' (param %d).",
                        friendlyTypeDescription(this.paramTypes.get(i)), this.params.get(i), i+1);
            }
            return objectParams;
        }

        // No public constructor.
        ExecuteCallResults()
        {}
    }

    /**
     * Parse EXECUTE procedure call.
     * @param statement   statement to parse
     * @param procedures  maps procedures to parameter signature maps
     * @return            results object or NULL if statement wasn't recognized
     * @throws SQLParser.Exception
     */
    public static ExecuteCallResults parseExecuteCall(
            String statement,
            Map<String,Map<Integer, List<String>>> procedures) throws SQLParser.Exception
    {
        return parseExecuteCallInternal(statement, procedures, false);
    }

    /**
     * Parse EXECUTE procedure call for testing without looking up parameter types.
     * @param statement   statement to parse
     * @param procedures  maps procedures to parameter signature maps
     * @return            results object or NULL if statement wasn't recognized
     * @throws SQLParser.Exception
     */
    public static ExecuteCallResults parseExecuteCallWithoutParameterTypes(
            String statement,
            Map<String,Map<Integer, List<String>>> procedures) throws SQLParser.Exception
    {
        return parseExecuteCallInternal(statement, procedures, true);
    }

    /**
     * Private implementation of parse EXECUTE procedure call.
     * Also supports short-circuiting procedure lookup for testing.
     * @param statement   statement to parse
     * @param procedures  maps procedures to parameter signature maps
     * @return            results object or NULL if statement wasn't recognized
     * @throws SQLParser.Exception
     */
    private static ExecuteCallResults parseExecuteCallInternal(
            String statement,
            Map<String,Map<Integer, List<String>>> procedures,
            boolean mockTypes) throws SQLParser.Exception
    {
        Matcher matcher = ExecuteCall.matcher(statement);
        if (matcher.find()) {
            ExecuteCallResults results = new ExecuteCallResults();
            results.query = matcher.replaceFirst("");
            results.params = parseProcedureCallParameters(results.query);
            results.procedure = results.params.remove(0);
            // TestSqlCmdInterface doesn't need/want the param types
            if (mockTypes) {
                // Mock everything as integer.
                results.paramTypes = new ArrayList<String>(results.params.size());
                for (int i = 0; i < results.params.size(); ++i) {
                    results.paramTypes.add("integer");
                }
            }
            else {
                Map<Integer, List<String>> signature = procedures.get(results.procedure);
                if (signature == null) {
                    throw new SQLParser.Exception("Undefined procedure: %s", results.procedure);
                }

                results.paramTypes = signature.get(results.params.size());
                if (results.paramTypes == null || results.params.size() != results.paramTypes.size()) {
                    String expectedSizes = "";
                    for (Integer expectedSize : signature.keySet()) {
                        expectedSizes += expectedSize + ", ";
                    }
                    throw new SQLParser.Exception(
                            "Invalid parameter count for procedure: %s (expected: %s received: %d)",
                            results.procedure, expectedSizes, results.params.size());
                }
            }

            return results;
        }
        return null;
    }

    /**
     * Parse EXPLAIN <query>
     * @param statement  statement to parse
     * @return           query parameter string or NULL if statement wasn't recognized
     */
    public static String parseExplainCall(String statement)
    {
        if (ExplainCall.matcher(statement).find()) {
            // This all could probably be done more elegantly via a group extracted
            // from a more comprehensive regexp.
            String query = statement.substring("explain ".length());
            return query;
        }
        return null;
    }

    /**
     * Parse EXPLAINPROC <procedure>
     * @param statement  statement to parse
     * @return           query parameter string or NULL if statement wasn't recognized
     */
    public static String parseExplainProcCall(String statement)
    {
        if (ExplainProcCall.matcher(statement).find()) {
            // This all could probably be done more elegantly via a group extracted
            // from a more comprehensive regexp.
            String query = statement.substring("explainProc ".length());
            // Clean up any extra spaces from between explainproc and the proc name.
            query = query.trim();
            return query;
        }
        return null;
    }

    /**
     * Check if query is DDL
     * @param query  query to check
     * @return       true if query is DDL
     */
    public static boolean queryIsDDL(String query)
    {
        return SQLLexer.extractDDLToken(query) != null;
    }

    /**
     * Handle internally translated commands.
     * @param statement  input statement
     * @return           translated statement (may be unchanged)
     * @throws SQLParser.Exception
     */
    public static String translateStatement(String statement) throws SQLParser.Exception
    {
        // LOAD CLASS <jar>?
        String arg = loadClassesParser.parse(statement);
        if (arg != null) {
            if (! new File(arg).isFile()) {
                throw new SQLParser.Exception("Jar file not found: %s", arg);
            }
            return String.format("exec @UpdateClasses '%s', NULL;", arg);
        }

        // REMOVE CLASS <class-selector>?
        arg = removeClassesParser.parse(statement);
        if (arg != null) {
            // reject obviously bad class selectors
            if (!ClassSelectorToken.matcher(arg).matches()) {
                throw new SQLParser.Exception("Bad characters in class selector: %s", arg);
            }
            return String.format("exec @UpdateClasses NULL, '%s';", arg);
        }

        // None of the above - return the untranslated input command.
        return statement;
    }

    /**
     * Make sure that a batch starts with a DDL statement by checking the first keyword.
     * @param batch  A SQL string containing multiple statements separated by semicolons
     * @return true if the first keyword of the first statement is a DDL verb
     *     like CREATE, ALTER, DROP, PARTITION, or EXPORT
     */
    public static boolean batchBeginsWithDDLKeyword(String batch) {
        // This method is really supposed to look at a single statement, but it seems
        // also to work for a batch of statements.
        return queryIsDDL(batch);
    }
}
