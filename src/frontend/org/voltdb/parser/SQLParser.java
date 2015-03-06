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

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
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
            ).compile("PAT_PARTITION_ANY_PREAMBLE");

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
            SPF.token("partition"), SPF.token("table"), SPF.capture(SPF.databaseObjectName()),
            SPF.token("on"), SPF.token("column"), SPF.capture(SPF.databaseObjectName())
        ).compile("PAT_PARTITION_TABLE");

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
            SPF.token("partition"), SPF.token("procedure"), SPF.capture(SPF.procedureName()),
            SPF.token("on"), SPF.token("table"), SPF.capture(SPF.databaseObjectName()),
            SPF.token("column"), SPF.capture(SPF.databaseObjectName()),
            SPF.optional(SPF.clause(SPF.token("parameter"), SPF.capture(SPF.integer())))
        ).compile("PAT_PARTITION_PROCEDURE");

    //TODO: Convert to pattern factory usage below this point.
    //TODO: Consider implementing FIXME/TODO comments -- and TESTING --
    // prior to conversion to pattern factory.

    /*
     * CREATE PROCEDURE [ <MODIFIER_CLAUSE> ... ] FROM <JAVA_CLASS>
     *
     * CREATE PROCEDURE from Java class statement pattern.
     * NB supports only unquoted table and column names
     *
     * Capture groups:
     *  (1) ALLOW/PARTITION clauses full text - needs further parsing
     *  (2) Class name
     */
    private static final Pattern PAT_CREATE_PROCEDURE_FROM_CLASS =
        SPF.statement(
            SPF.token("create"), SPF.token("procedure"),
            unparsedProcedureModifierClauses(),
            SPF.token("from"), SPF.token("class"), SPF.capture(SPF.className())
        ).compile("PAT_CREATE_PROCEDURE_FROM_CLASS");

    /*
     * CREATE PROCEDURE <NAME> [ <MODIFIER_CLAUSE> ... ] AS <SQL_STATEMENT>
     *
     * CREATE PROCEDURE with single SELECT or DML statement pattern
     * NB supports only unquoted table and column names
     *
     * Capture groups:
     *  (1) Procedure name
     *  (2) ALLOW/PARTITION clauses full text - needs further parsing
     *  (3) SELECT or DML statement
     */
    private static final Pattern PAT_CREATE_PROCEDURE_FROM_SQL =
        SPF.statement(
            SPF.token("create"), SPF.token("procedure"), SPF.capture(SPF.procedureName()),
            unparsedProcedureModifierClauses(),
            SPF.token("as"), SPF.capture(SPF.anyClause())
        ).compile("PAT_CREATE_PROCEDURE_FROM_SQL");

    /*
     * CREATE PROCEDURE <NAME> [ <MODIFIER_CLAUSE> ... ] AS ### <PROCEDURE_CODE> ### LANGUAGE <LANGUAGE_NAME>
     *
     * CREATE PROCEDURE with inline implementation script, e.g. Groovy, statement regex
     * NB supports only unquoted table and column names
     * The only supported language is GROOVY for now, but to avoid confusing with the
     * other CREATE PROCEDURE ... AS variant match anything that has the block delimiters.
     *
     * Capture groups:
     *  (1) Procedure name
     *  (2) ALLOW/PARTITION clauses - needs further parsing
     *  (3) Code block content
     *  (4) Language name
     */
    private static final Pattern PAT_CREATE_PROCEDURE_AS_SCRIPT =
        SPF.statement(
            SPF.token("create"), SPF.token("procedure"), SPF.capture(SPF.procedureName()),
            unparsedProcedureModifierClauses(),
            SPF.token("as"),
            SPF.delimitedCaptureBlock(SQLLexer.BLOCK_DELIMITER, null),
            // Match anything after the last delimiter to get a good error for a bad language clause.
            SPF.oneOf(
                SPF.clause(SPF.token("language"), SPF.capture(SPF.languageName())),
                SPF.anyClause()
            )
        ).compile("PAT_CREATE_PROCEDURE_AS_SCRIPT");

    /**
     * Pattern for parsing a single ALLOW or PARTITION clauses within a CREATE PROCEDURE statement.
     *
     * Capture groups:
     *  (1) ALLOW clause: entire role list with commas and internal whitespace
     *  (2) PARTITION clause: procedure name
     *  (3) PARTITION clause: table name
     *  (4) PARTITION clause: column name
     *
     *  An ALLOW clause will have (1) be non-null and (2,3,4) be null.
     *  A PARTITION clause will have (1) be null and (2,3,4) be non-null.
     */
    private static final Pattern PAT_ANY_CREATE_PROCEDURE_STATEMENT_CLAUSE =
        parsedProcedureModifierClause().compile("PAT_ANY_CREATE_PROCEDURE_STATEMENT_CLAUSE");

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
     *  replicate, export, import, or dr, the first match group is set to respectively procedure,
     *  role, drop, partition, replicate, export, or import, or dr.
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
            "\\AIMPORT|" +
            "\\ADR" +
            ")" +                                  // end (group 1)
            "\\s" +                                // one required whitespace to terminate keyword
            "");

    private static final Pattern PAT_DR_TABLE = Pattern.compile(
            "(?i)" +                                // (ignore case)
            "\\A"  +                                // start statement
            "DR\\s+TABLE\\s+" +                     // DR TABLE
            "([\\w.$|\\\\*]+)" +                    // (1) <table name>
            "(?:\\s+(DISABLE))?" +                  //     (2) optional DISABLE argument
            "\\s*;\\z"                              // (end statement)
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
     * Matches a command followed by and SQL CRUD statement verb
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
            "(.*)" +          // optional subcommand (group 1) BUG: subcommand should require prior whitespace.
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
            "\\s*$",               // optional terminating whitespace
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ExitToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "(exit|quit)" +   // required command (group 1 -- probably not used)
            "\\s*" +          // optional whitespace before semicolon
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +            // optional semicolons
            "\\s*$",          // optional terminating whitespace
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ShowToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "(?:list|show)" + // keyword alternatives, synonymous so don't bother capturing
            "\\s+" +          // one or more spaces
            "(proc|procedure|tables|classes)" + // subcommand (group 1) over-specified, see ***Note***
            "\\s*" + // optional whitespace before semicolon
            // long-term, move + outside '[]'s for "([a-z]+)" +
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +            // optional semicolons
            "\\s*$",          // optional terminating whitespace
            Pattern.CASE_INSENSITIVE);
    // ***Note***
    // TODO: It would be better to be very forgiving initially about subcommands as in:
    // "([a-z]*)" + // alphabetic subcommand (group 1)
    // or even
    // "([^;\\s]*)" + // non-space non-semicolon subcommand (group 1)
    // That would allow list/show command processing to be "locked in" here even if the
    // subcommand was later found to be garbage -- or missing or followed by garbage.
    // A custom error message could usefully explain the correct list/show command options.
    // For now, with a strict match for only valid list/show subcommands, invalid or
    // missing subcommands or directives with extra garbage sharacters fail
    // through to the generic @AdHoc sql statement processor which unhelpfully
    // claims that show or list is some kind of syntax error.
    // The reason NOT to fix this right away is that sqlcmd has a bug so that it
    // tries to parse things like list and show commands on each new line of input
    // EVEN when it is processing the middle of a multi-line sql statement. So, it could
    // legitimately encounter the line "list integer" (in the middle of a create
    // table statement) and must correctly pass it on to @AdHoc.
    // It should NOT treat this case as an invalid list subcommand.
    // Once that bug is fixed, we can shift to very general subcommand and
    // end-of-directive matching here and for other directives.
    // For now (as before) look for only the specific valid subcommands and let @AdHoc
    // give its misleading error messages for any invalid usage.
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
            //TODO: For now, at least until the directive processor is fixed,
            // avoid false positives on uses of "recall" as a schema name by
            // completely failing to recognize a recall directive that does
            // not have its expected integer argument.
            // When the directive processing is fixed,
            // this pattern match can be generalized to something like
            // "([^;]+)" +    // required non-whitespace non-semicolon parameter (group 1)
            // leaving it to ParseRecallResults to produce a more precise
            // error message than the @AdHoc fallback could give.
            "([0-9]+)" +   // required integer parameter (group 1)
            "\\s*" +       // optional whitespace
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +         // optional semicolons
            "\\s*$",       // optional terminating whitespace
            Pattern.CASE_INSENSITIVE);

    // SQLCommand's FILE command.  If this pattern matches, we
    // assume that the user meant to enter a file command, and
    // produce appropriate error messages.
    private static final Pattern FileToken = Pattern.compile(
            "^\\s*" +          // optional indent at start of line
            "file" +           // FILE keyword
            "(?:(?=\\s|;)|$)", // Must be either followed by whitespace or semicolon
                               //   (zero-width consumed)
                               // or the end of the line
            Pattern.CASE_INSENSITIVE);

    private static final Pattern DashBatchToken = Pattern.compile(
            "\\s+" +   // required preceding whitespace
            "-batch",  // -batch option, whitespace terminated
            Pattern.CASE_INSENSITIVE);

    private static final Pattern DashInlineBatchToken = Pattern.compile(
            "\\s+" +        // required preceding whitespace
            "-inlinebatch",  // -inlinebatch option, whitespace terminated
            Pattern.CASE_INSENSITIVE);

    private static final Pattern FilenameToken = Pattern.compile(
            "\\s+" +        // required preceding whitespace
            "['\"]*" +     // optional opening quotes of either kind (ignored) (?)
            "([^;'\"]+)" + // file path assumed to end at the next quote or semicolon
            "['\"]*" +     // optional closing quotes -- assumed to match opening quotes (?)
            "\\s*" +       // optional whitespace
            //FIXME: strangely allowing more than one strictly adjacent semicolon.
            ";*" +         // optional semicolons
            "\\s*",        // more optional whitespace
            Pattern.CASE_INSENSITIVE);

    private static final Pattern DelimiterToken = Pattern.compile(
            "\\s+" +        // required preceding whitespace
            "([^\\s;]+)" +  // a string of characters not containing semis or spaces
            "\\s*;?\\s*",   // an optional semicolon surrounded by whitespace
            Pattern.CASE_INSENSITIVE);

    // Query Execution
    private static final Pattern ExecuteCall = Pattern.compile(
            "^" +                   //TODO: allow indent at start of line -- or is input always trimmed?
            "(?:exec|execute)\\s+", // required command or alias whitespace terminated non-grouping
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explain" (case insensitive).  We'll convert them to @Explain invocations.
    private static final Pattern ExplainCall = Pattern.compile(
            "^" +           //TODO: allow indent at start of line -- or is input always trimmed?
            "explain\\s+",  // required command, whitespace terminated
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explainproc" (case insensitive).  We'll convert them to @ExplainProc invocations.
    private static final Pattern ExplainProcCall = Pattern.compile(
            "^" +              //TODO: allow indent at start of line -- or is input always trimmed?
            "explainProc\\s+", // required command, whitespace terminated
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
     * Match statement against DR table pattern
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchDRTable(String statement)
    {
        return PAT_DR_TABLE.matcher(statement);
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
     * Build a pattern segment to accept a single optional ALLOW or PARTITION clause
     * to modify CREATE PROCEDURE statements.
     *
     * @param captureTokens  Capture individual tokens if true
     * @return               Inner pattern to be wrapped by the caller as appropriate
     *
     * Capture groups (when captureTokens is true):
     *  (1) ALLOW clause: entire role list with commas and internal whitespace
     *  (2) PARTITION clause: procedure name
     *  (3) PARTITION clause: table name
     *  (4) PARTITION clause: column name
     */
    private static SQLPatternPart makeInnerProcedureModifierClausePattern(boolean captureTokens)
    {
        return
            SPF.oneOf(
                SPF.clause(
                    SPF.token("allow"),
                    SPF.group(captureTokens, SPF.commaList(SPF.userName()))
                ),
                SPF.clause(
                    SPF.token("partition"), SPF.token("on"), SPF.token("table"),
                    SPF.group(captureTokens, SPF.databaseObjectName()),
                    SPF.token("column"),
                    SPF.group(captureTokens, SPF.databaseObjectName()),
                    SPF.optional(
                        SPF.clause(
                            SPF.token("parameter"),
                            SPF.group(captureTokens, SPF.integer())
                        )
                    )
                )
            );
    }

    /**
     * Build a pattern segment to accept and parse a single optional ALLOW or PARTITION
     * clause used to modify a CREATE PROCEDURE statement.
     *
     * @return ALLOW/PARTITION modifier clause parsing pattern.
     *
     * Capture groups:
     *  (1) ALLOW clause: entire role list with commas and internal whitespace
     *  (2) PARTITION clause: procedure name
     *  (3) PARTITION clause: table name
     *  (4) PARTITION clause: column name
     */
    static SQLPatternPart parsedProcedureModifierClause()
    {
        return SPF.clause(makeInnerProcedureModifierClausePattern(true));

    }

    /**
     * Build a pattern segment to recognize all the ALLOW or PARTITION modifier clauses
     * of a CREATE PROCEDURE statement.
     *
     * @return Pattern to be used by the caller inside a CREATE PROCEDURE pattern.
     *
     * Capture groups:
     *  (1) All ALLOW/PARTITION modifier clauses as one string
     */
    static SQLPatternPart unparsedProcedureModifierClauses()
    {
        // Force the leading space to go inside the repeat block.
        return SPF.capture(SPF.repeat(makeInnerProcedureModifierClausePattern(false))).withFlags(SQLPatternFactory.ADD_LEADING_SPACE_TO_CHILD);
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
        // These are declared public because trying to declare them as private
        // and force use of public accessors gets a mysterious NoSuchMethodError.
        public final int line;
        public final String error;

        ParseRecallResults(int line, String error)
        {
            this.line = line;
            this.error = error;
        }
        // Attempts to use these methods gets a mysterious NoSuchMethodError,
        // so keep them disabled and keep the attributes public for now.
        // public int getLine() { return line; }
        // public String getError() { return error; }
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

    /**
     * An enum that describes the options that can be applied
     * to sqlcmd's "file" command
     */
    static public enum FileOption {
        PLAIN,
        BATCH,
        INLINEBATCH
    }

    /**
     * This class encapsulates information produced by
     * parsing sqlcmd's "file" command.
     */
    public static final class FileInfo {
        private final FileOption m_option;
        private final File m_file;
        private final String m_delimiter;

        FileInfo(FileOption option, String filenameOrDelimiter) {
            m_option = option;
            switch (option) {
            case PLAIN:
            case BATCH:
                m_file = new File(filenameOrDelimiter);
                m_delimiter = null;
                break;
            case INLINEBATCH:
            default:
                assert(option == FileOption.INLINEBATCH);
                m_file = null;
                m_delimiter = filenameOrDelimiter;
                break;
            }
        }

        public File getFile() {
            return m_file;
        }

        public String getDelimiter() {
            assert (m_option == FileOption.INLINEBATCH);
            return m_delimiter;
        }

        public boolean isBatch() {
            return m_option == FileOption.BATCH
                    || m_option == FileOption.INLINEBATCH;
        }

        public FileOption getOption() {
            return m_option;
        }

        @Override
        public String toString() {
            return "FILE command: " + m_option.name() +
                    ", file: \"" + m_file.getName() +
                    "\", delimiter: " + m_delimiter;
        }
    }

    /**
     * Parse FILE statement for sqlcmd.
     * @param statement  statement to parse
     * @return           File object or NULL if statement wasn't recognized
     */
    public static FileInfo parseFileStatement(String statement)
    {
        Matcher fileMatcher = FileToken.matcher(statement);

        if (! fileMatcher.lookingAt()) {
            // This input does not start with FILE,
            // so it's not a file command, it's something else.
            // Return to caller a null and no errors.
            return null;
        }

        String remainder = statement.substring(fileMatcher.end(), statement.length());

        Matcher inlineBatchMatcher = DashInlineBatchToken.matcher(remainder);
        if (inlineBatchMatcher.lookingAt()) {
            remainder = remainder.substring(inlineBatchMatcher.end(), remainder.length());
            Matcher delimiterMatcher = DelimiterToken.matcher(remainder);

            // use matches here (not lookingAt) because we want to match
            // all of the remainder, not just beginning
            if (delimiterMatcher.matches()) {
                String delimiter = delimiterMatcher.group(1);
                return new FileInfo(FileOption.INLINEBATCH, delimiter);
            }

            throw new SQLParser.Exception(
                    "Did not find valid delimiter for \"file -inlinebatch\" command.");
        }

        // It is either a plain or a -batch file command.
        FileOption option = FileOption.PLAIN;
        Matcher batchMatcher = DashBatchToken.matcher(remainder);
        if (batchMatcher.lookingAt()) {
            option = FileOption.BATCH;
            remainder = remainder.substring(batchMatcher.end(), remainder.length());
        }

        Matcher filenameMatcher = FilenameToken.matcher(remainder);
        String filename = null;

        // Use matches to match all input, not just beginning
        if (filenameMatcher.matches()) {
            filename = filenameMatcher.group(1);

            // Trim whitespace from beginning and end of the file name.
            // User may have wanted quoted whitespace at the beginning or end
            // of the file name, but that seems very unlikely.
            filename = filename.trim();
        }

        // If no filename, or a filename of only spaces, then throw an error.
        if (filename == null || filename.length() == 0) {
            String msg = String.format("Did not find valid file name in \"file%s\" command.",
                    option == FileOption.BATCH ? " -batch" : "");
            throw new SQLParser.Exception(msg);
        }

        return new FileInfo(option, filename);
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
     * Make sure that the batch starts with an appropriate DDL verb.  We do not
     * look further than the first token of the first non-comment and non-whitespace line.
     *
     * Empty batches are considered to be trivially valid.
     *
     * @param batch  A SQL string containing multiple statements separated by semicolons
     * @return true if the first keyword of the first statement is a DDL verb
     *     like CREATE, ALTER, DROP, PARTITION, or EXPORT, or if the
     *     batch is empty.
     */
    public static boolean appearsToBeValidDDLBatch(String batch) {

        BufferedReader reader = new BufferedReader(new StringReader(batch));
        String line;
        try {
            while ((line = reader.readLine()) != null) {

                line = SingleLineComments.matcher(line).replaceAll("");
                line = line.trim();
                if (line.equals(""))
                    continue;

                // we have a non-blank line that contains more than just a comment.
                return queryIsDDL(line);
            }
        }
        catch (IOException e) {
            // This should never happen for a StringReader
            assert(false);
        }


        // trivial empty batch: no lines are non-blank or non-comments
        return true;
    }
}
