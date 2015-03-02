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

import org.hsqldb_voltpatches.VoltToken;
import org.hsqldb_voltpatches.VoltTokenStream;
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
     * NB supports only unquoted table and column names
     *
     * Regex Description:
     * <pre>
     * (?i) -- ignore case
     * \\A -- beginning of statement
     * REPLICATE -- token
     * \\s+ -- one or more spaces
     * TABLE -- token
     * \\s+ -- one or more spaces
     * ([\\w$.]+) -- [table name capture group 1]
     *    [\\w$]+ -- one or more identifier character (letters, numbers, dollar sign ($) or underscore (_))
     * \\s* -- 0 or more spaces
     * ; -- a semicolon
     * \\z -- end of string
     * </pre>
     */
    private static final Pattern PAT_REPLICATE_TABLE = Pattern.compile(
            "(?i)\\AREPLICATE\\s+TABLE\\s+([\\w$]+)\\s*;\\z"
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
            "([\\w.$]+)" +                      // (1) <table name>
            "(?:\\s+TO\\s+STREAM\\s+" +         // begin optional TO STREAM clause
            "([\\w.$]+)" +                      // (2) <export target>
            ")?" +                              // end optional STREAM clause
            "\\s*;\\z"                          // (end statement)
            );
    /**
     * Regex Description:
     *
     *  if the statement starts with either create procedure, partition,
     *  replicate, or role the first match group is set to respectively procedure,
     *  partition, replicate, or role.
     * <pre>
     * (?i) -- ignore case
     * ((?<=\\ACREATE\\s{0,1024})(?:PROCEDURE|ROLE)|\\APARTITION|\\AREPLICATE\\AEXPORT) -- voltdb ddl
     *    [capture group 1]
     *      (?<=\\ACREATE\\s{1,1024})(?:PROCEDURE|ROLE) -- create procedure or role ddl
     *          (?<=\\ACREATE\\s{0,1024}) -- CREATE zero-width positive lookbehind
     *              \\A -- beginning of statement
     *              CREATE -- token
     *              \\s{1,1024} -- one or up to 1024 spaces
     *              (?:PROCEDURE|ROLE) -- procedure or role token
     *      | -- or
     *      \\A -- beginning of statement
     *      -- PARTITION token
     *      | -- or
     *      \\A -- beginning of statement
     *      REPLICATE -- token
     *      | -- or
     *      \\A -- beginning of statement
     *      EXPORT -- token
     *      | -- or
     *      \\A -- beginning of statement
     *      DR -- token
     * \\s -- one space
     * </pre>
     */
    private static final Pattern PAT_ALL_VOLTDB_STATEMENT_PREAMBLES = Pattern.compile(
            "(?i)((?<=\\ACREATE\\s{0,1024})" +
            "(?:PROCEDURE|ROLE)|" +
            "\\ADROP|\\APARTITION|\\AREPLICATE|\\AEXPORT|\\AIMPORT|\\ADR)\\s"
            );

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
    private static final Pattern SingleLineComments = Pattern.compile("^\\s*(\\/\\/|--).*$", Pattern.MULTILINE);
    private static final Pattern Extract = Pattern.compile("'[^']*'", Pattern.MULTILINE);
    private static final Pattern AutoSplitParameters = Pattern.compile("[\\s,]+", Pattern.MULTILINE);

    // HELP can support sub-commands someday. Capture group 1 is the sub-command.
    private static final Pattern HelpToken = Pattern.compile("^\\s*help\\s*(.*)\\s*;*\\s*$", Pattern.CASE_INSENSITIVE);

    private static final Pattern GoToken = Pattern.compile("^\\s*go;*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ExitToken = Pattern.compile("^\\s*(exit|quit);*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern ShowToken = Pattern.compile("^\\s*list|show\\s+([a-z+])\\s*;*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern SemicolonToken = Pattern.compile("^.*\\s*;+\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern RecallToken = Pattern.compile("^\\s*recall\\s+([^;]+)\\s*;*\\s*$", Pattern.CASE_INSENSITIVE);
    private static final Pattern FileToken = Pattern.compile("^\\s*file\\s+['\"]*([^;'\"]+)['\"]*\\s*;*\\s*", Pattern.CASE_INSENSITIVE);

    // Query Execution
    private static final Pattern ExecuteCall = Pattern.compile("^(exec|execute) ", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explain" (case insensitive).  We'll convert them to @Explain invocations.
    private static final Pattern ExplainCall = Pattern.compile("^explain ", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explainproc" (case insensitive).  We'll convert them to @ExplainProc invocations.
    private static final Pattern ExplainProcCall = Pattern.compile("^explainProc ", Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
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
            this.patPrefix = Pattern.compile(String.format("^\\s*%s\\s.*$", prefixPat), Pattern.CASE_INSENSITIVE);
            this.patFull = Pattern.compile(String.format("^\\s*%s\\s+([^;]+)[;\\s]*$", prefixPat), Pattern.CASE_INSENSITIVE);
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

        List<String> queries = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        VoltTokenStream tokenStream = new VoltTokenStream(query);
        boolean firstToken = true;
        for (VoltToken token : tokenStream) {

            if (! firstToken) {
                sb.append(" ");
            }

            if (token.kind() == VoltToken.Kind.SEMICOLON) {
                queries.add(sb.toString());
                sb.setLength(0);
                firstToken = true;
            }
            else {
                sb.append(token.toSqlText());
                firstToken = false;
            }
        }

        if (sb.length() > 0)
            queries.add(sb.toString());

        System.out.println(queries);

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

        System.out.println("Parameters are: " + queries);

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

    /**
     * Parse FILE statement for sqlcmd.
     * @param statement  statement to parse
     * @return           File object or NULL if statement wasn't recognized
     */
    public static File parseFileStatement(String statement)
    {
        Matcher matcher = FileToken.matcher(statement);
        if (matcher.matches()) {
            return new File(matcher.group(1));
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
}
