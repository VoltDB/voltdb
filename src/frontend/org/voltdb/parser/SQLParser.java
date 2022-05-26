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

package org.voltdb.parser;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.voltdb.types.GeographyPointValue;
import org.voltdb.types.GeographyValue;
import org.voltdb.types.TimestampType;
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
        private Exception(String message, Object... args)
        {
            super(String.format(message, args));
        }

        private Exception(Throwable cause)
        {
            super(cause.getMessage(), cause);
        }

        private Exception(Throwable cause, String message, Object... args)
        {
            super(String.format(message, args), cause);
        }

        private static final long serialVersionUID  = -4043500523038225173L;
    }

    //========== Private Parsing Data ==========

    /**
     * Pattern: SET <PARAMETER NAME> <PARAMETER VALUE>
     *
     * Capture groups:
     *  (1) parameter name
     *  (2) parameter value
     */
    private static final Pattern SET_GLOBAL_PARAM = Pattern.compile(
            "(?i)" +                            // (ignore case)
            "\\A" +                             // (start statement)
            "SET" +                             // SET
            "\\s+([\\w_]+)" +                   // (1) PARAMETER NAME
            "\\s*=\\s*([\\w_]+)" +              // (2) PARAMETER VALUE
            "\\s*;\\z"                          // (end statement)
            );
    static final Pattern SET_GLOBAL_PARAM_FOR_WHITELIST = Pattern.compile(
            "(?i)" +                            // (ignore case)
            "\\A" +                             // (start statement)
            "SET" +                             // SET
            "\\s+.*\\z"                         // (end statement)
            );

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
     * Pattern: CREATE TABLE table_name (column...) other-stuff
     *
     * Plain, no EXPORT or MIGRATE clause
     *
     * Capture groups:
     *  (1) table name
     */
    private static final Pattern PAT_CREATE_TABLE_PLAIN =
            SPF.statement(
                    SPF.token("create"), SPF.token("table"),
                    SPF.capture(SPF.databaseObjectName()),
                    new SQLPatternPartString("\\s*"), // see ENG-11862 for reason about adding this pattern
                    SPF.anyColumnFields().withFlags(ADD_LEADING_SPACE_TO_CHILD),
                    SPF.anythingOrNothing().withFlags(ADD_LEADING_SPACE_TO_CHILD) // TODO, is this valid here?
            ).compile("PAT_CREATE_TABLE_PLAIN");

    /**
     * Pattern part: ON trigger...
     * used for EXPORT TO TARGET|TOPIC
     */
    public static final String CAPTURE_TRIGGER_LIST = "triggerList";

    private static final SQLPatternPart TRIGGER_CLAUSE =
        SPF.clause(SPF.token("on"),
                   SPF.capture(CAPTURE_TRIGGER_LIST, SPF.commaList(SPF.databaseTrigger())));

    /**
     * Pattern part: WITH [KEY (key...)] [VALUE (value...)]
     * used for EXPORT|MIGRATE TO TOPIC
     */
    public static final String CAPTURE_TOPIC_KEY_COLUMNS = "keyColumns";
    public static final String CAPTURE_TOPIC_VALUE_COLUMNS = "valueColumns";

    private static final SQLPatternPart KEY_VALUE_CLAUSE =
        SPF.clause(SPF.token("with"),
                   SPF.optional(SPF.token("key\\s*\\(\\s*"),
                                SPF.capture(CAPTURE_TOPIC_KEY_COLUMNS, SPF.commaList(SPF.databaseObjectName()))
                                   .withFlags(ADD_LEADING_SPACE_TO_CHILD),
                                SPF.token("\\s*\\)")
                                   .withFlags(ADD_LEADING_SPACE_TO_CHILD)),
                   SPF.optional(SPF.token("value\\s*\\(\\s*"),
                                SPF.capture(CAPTURE_TOPIC_VALUE_COLUMNS, SPF.commaList(SPF.databaseObjectName()))
                                   .withFlags(ADD_LEADING_SPACE_TO_CHILD),
                                SPF.token("\\s*\\)")
                                   .withFlags(ADD_LEADING_SPACE_TO_CHILD)));

    /**
     * Pattern: CREATE TABLE table_name EXPORT TO TARGET target_name
     *                 [ON trigger...] (column...) other-stuff
     *
     * Capture groups:
     *  (1) table name
     *  (2) target name
     *  (3) [triggers, comma separated]
     */
    private static final Pattern PAT_CREATE_TABLE_EXPORT_TO_TARGET =
            SPF.statement(
                    SPF.token("create"), SPF.token("table"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.token("export"), SPF.token("to"), SPF.token("target"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.optional(TRIGGER_CLAUSE),
                    new SQLPatternPartString("\\s*"),
                    SPF.anyColumnFields().withFlags(ADD_LEADING_SPACE_TO_CHILD),
                    SPF.anythingOrNothing().withFlags(ADD_LEADING_SPACE_TO_CHILD)
            ).compile("PAT_CREATE_TABLE_EXPORT_TO_TARGET");

    /**
     * Pattern: CREATE TABLE table_name EXPORT TO TOPIC topic_name
     *                 [ON trigger...] [WITH [KEY key...] [VALUE value...]]
     *                 (column...) other-stuff
     *
     * Capture groups:
     *  (1) table name
     *  (2) topic name
     *  (3) [triggers, comma separated]
     *  (n) [topic key/value lists]
     */
    private static final Pattern PAT_CREATE_TABLE_EXPORT_TO_TOPIC =
            SPF.statement(
                    SPF.token("create"), SPF.token("table"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.token("export"), SPF.token("to"), SPF.token("topic"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.optional(TRIGGER_CLAUSE),
                    SPF.optional(KEY_VALUE_CLAUSE),
                    new SQLPatternPartString("\\s*"),
                    SPF.anyColumnFields().withFlags(ADD_LEADING_SPACE_TO_CHILD),
                    SPF.anythingOrNothing().withFlags(ADD_LEADING_SPACE_TO_CHILD)
            ).compile("PAT_CREATE_TABLE_EXPORT_TO_TOPIC");

    /**
     * Pattern: CREATE TABLE table_name MIGRATE TO TARGET target_name
     *                 (column...) other-stuff
     *
     * Capture groups:
     *  (1) table name
     *  (2) target name
     */
    private static final Pattern PAT_CREATE_TABLE_MIGRATE_TO_TARGET =
            SPF.statement(
                    SPF.token("create"), SPF.token("table"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.token("migrate"), SPF.token("to"), SPF.token("target"),
                    SPF.capture(SPF.databaseObjectName()),
                    new SQLPatternPartString("\\s*"), // see ENG-11862 for reason about adding this pattern
                    SPF.anyColumnFields().withFlags(ADD_LEADING_SPACE_TO_CHILD),
                    SPF.anythingOrNothing().withFlags(ADD_LEADING_SPACE_TO_CHILD)
            ).compile("PAT_CREATE_TABLE");

    /**
     * Pattern: CREATE TABLE table_name MIGRATE TO TOPIC topic_name
     *                 [WITH [KEY key...] [VALUE value...]]
     *                 (column...) other-stuff
     *
     * Capture groups:
     *  (1) table name
     *  (2) topic name
     *  (n) [topic key/value lists]
     */
    private static final Pattern PAT_CREATE_TABLE_MIGRATE_TO_TOPIC =
            SPF.statement(
                    SPF.token("create"), SPF.token("table"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.token("migrate"), SPF.token("to"), SPF.token("topic"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.optional(KEY_VALUE_CLAUSE),
                    new SQLPatternPartString("\\s*"), // see ENG-11862 for reason about adding this pattern
                    SPF.anyColumnFields().withFlags(ADD_LEADING_SPACE_TO_CHILD),
                    SPF.anythingOrNothing().withFlags(ADD_LEADING_SPACE_TO_CHILD)
            ).compile("PAT_CREATE_TABLE");

    /**
     * Pattern: ALTER TABLE tablename
     *
     * Capture groups:
     *  (1) table name
     */
    private static final Pattern PAT_ALTER_TTL =
        SPF.statement(
            SPF.token("alter"), SPF.token("table"), SPF.capture("name", SPF.databaseObjectName()),
            SPF.token("using"), SPF.token("TTL"),
            SPF.anythingOrNothing()
        ).compile("PAT_ALTER_TTL");

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
     *  (1) Optional type modifier, DIRECTED or COMPOUND
     *  (2) ALLOW/PARTITION clauses full text - needs further parsing
     *  (3) Class name
     */
    private static final Pattern PAT_CREATE_PROCEDURE_FROM_CLASS =
        SPF.statement(
            SPF.token("create"),
            SPF.optional(SPF.capture(SPF.tokenAlternatives("directed", "compound"))),
            SPF.token("procedure"),
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
     *  (1) Optional type modifier, DIRECTED or COMPOUND
     *  (2) Procedure name
     *  (3) ALLOW/PARTITION clauses full text - needs further parsing
     *  (4) SELECT or DML statement
     */
    private static final Pattern PAT_CREATE_PROCEDURE_FROM_SQL =
        SPF.statement(
            SPF.token("create"),
            SPF.optional(SPF.capture(SPF.tokenAlternatives("directed", "compound"))),
            SPF.token("procedure"), SPF.capture(SPF.procedureName()),
            unparsedProcedureModifierClauses(),
            SPF.token("as"), SPF.capture(SPF.anyClause())
        ).compile("PAT_CREATE_PROCEDURE_FROM_SQL");

    /*
     * CREATE PROCEDURE <NAME> [ <MODIFIER_CLAUSE> ... ] AS BEGIN <SQL_STATEMENTS> END
     *
     * CREATE PROCEDURE with multiple SELECT or DML statement pattern
     * NB supports only unquoted table and column names
     * This regular expression is only for matching BEGIN...END and NOT for finding multi statement procedures
     * because multi statement procedures cannot be captured using regular expressions (nested CASE-END issue),
     * matching is done in a loop in SQLexer.splitStatements()
     *
     * Capture groups:
     *  (1) Optional type modifier, DIRECTED or COMPOUND
     *  (2) Procedure name
     *  (3) ALLOW/PARTITION clauses full text - needs further parsing
     *  (4) SELECT or DML statement
     */
    private static final Pattern PAT_CREATE_PROCEDURE_FROM_MULTI_STMT_SQL =
        SPF.statement(
            SPF.token("create"),
            SPF.optional(SPF.capture(SPF.tokenAlternatives("directed", "compound"))),
            SPF.token("procedure"), SPF.capture(SPF.procedureName()),
            unparsedProcedureModifierClauses(),
            SPF.token("as"), SPF.token("begin"), SPF.capture(SPF.anyClause())
        ).compile("PAT_CREATE_PROCEDURE_FROM_MULTI_STMT_SQL");

    /*
     * CREATE FUNCTION <NAME> FROM METHOD <CLASS NAME>.<METHOD NAME>
     *
     * CREATE FUNCTION with the designated method from the given class.
     *
     * Capture groups:
     *  (1) Function name
     *  (2) The class name
     *  (3) The method name
     */
    private static final Pattern PAT_CREATE_FUNCTION_FROM_METHOD =
        SPF.statement(
            SPF.token("create"), SPF.token("function"), SPF.capture(SPF.functionName()),
            SPF.token("from"), SPF.token("method"),
            SPF.capture(SPF.classPath()), SPF.dot().withFlags(ADD_LEADING_SPACE_TO_CHILD),
            SPF.capture(SPF.functionName().withFlags(ADD_LEADING_SPACE_TO_CHILD))
        ).compile("PAT_CREATE_FUNCTION_FROM_METHOD");

    /*
     * CREATE AGGREGATE FUNCTION <NAME> FROM CLASS <CLASS NAME>
     *
     * CREATE AGGREGATE FUNCTION with the designated method from the given class.
     *
     * Capture groups:
     *  (1) Function name
     *  (2) The class name
     */
    private static final Pattern PAT_CREATE_AGGREGATE_FUNCTION_FROM_CLASS =
        SPF.statement(
            SPF.token("create"), SPF.token("aggregate"), SPF.token("function"),
            SPF.capture(SPF.functionName()), SPF.token("from"), SPF.token("class"),
            SPF.capture(SPF.classPath())
        ).compile("PAT_CREATE_AGGREGATE_FUNCTION_FROM_CLASS");

    /*
     * DROP AGGREGATE FUNCTION <NAME> [IF EXISTS]
     *
     * Drop a user-defined aggregate function.
     *
     * Capture groups:
     *  (1) Function name
     *  (2) If exists
     */
    private static final Pattern PAT_DROP_AGGREGATE_FUNCTION =
        SPF.statement(
            SPF.token("drop"), SPF.token("aggregate"), SPF.token("function"), SPF.capture(SPF.functionName()),
            SPF.optional(SPF.capture(SPF.clause(SPF.token("if"), SPF.token("exists"))))
        ).compile("PAT_DROP_AGGREGATE_FUNCTION");

    /*
     * DROP FUNCTION <NAME> [IF EXISTS]
     *
     * Drop a user-defined function.
     *
     * Capture groups:
     *  (1) Function name
     *  (2) If exists
     */
    private static final Pattern PAT_DROP_FUNCTION =
        SPF.statement(
            SPF.token("drop"), SPF.token("function"), SPF.capture(SPF.functionName()), SPF.ifExists()
        ).compile("PAT_DROP_FUNCTION");

    /*
     * CREATE PROCEDURE <NAME> [ <MODIFIER_CLAUSE> ... ] AS ### <PROCEDURE_CODE> ### LANGUAGE <LANGUAGE_NAME>
     *
     * CREATE PROCEDURE with inline implementation script, e.g. Groovy, statement regex
     * NB supports only unquoted table and column names
     * This used to support GROOVY, but now will just offer a compile error.
     *
     * Capture groups:
     *  (1) Optional type modifier, DIRECTED or COMPOUND
     *  (2) Procedure name
     *  (3) ALLOW/PARTITION clauses - needs further parsing
     *  (4) Code block content
     *  (5) Language name
     */
    private static final Pattern PAT_CREATE_PROCEDURE_AS_SCRIPT =
        SPF.statement(
            SPF.token("create"),
            SPF.optional(SPF.capture(SPF.tokenAlternatives("directed", "compound"))),
            SPF.token("procedure"), SPF.capture(SPF.procedureName()),
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
     * Pattern for parsing a single EXPORT or PARTITION clauses within a CREATE STREAM statement.
     *
     * Capture groups:
     *  (1) ALLOW clause: target name
     *  (2) PARTITION clause: column name
     *
     */
    private static final Pattern PAT_ANY_CREATE_STREAM_STATEMENT_CLAUSE =
        parsedStreamModifierClause().compile("PAT_ANY_CREATE_STREAM_STATEMENT_CLAUSE");

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
            ";" +                                   // semicolon terminator
            "\\z"                                   // end of statement
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
     * Regex to parse the DROP STREAM statement.
     * Capture group is tagged as (1) in comments below.
     */
    private static final Pattern PAT_DROP_STREAM =
            SPF.statementLeader(
                    SPF.token("drop"), SPF.token("stream"), SPF.capture("name", SPF.databaseObjectName()),
                    SPF.optional(SPF.clause(SPF.token("if"), SPF.token("exists")))
                    ).compile("PAT_DROP_STREAM");

    /**
     * Build regex to support create task statement in the from of
     * <p>
     *
     * <pre>
     * CREATE TASK {name}
     *     {
     *         {
     *             ON SCHEDULE {
     *                 DELAY {interval} {unit} |
     *                 EVERY {interval} {unit} |
     *                 CRON {exp} |
     *                 FROM CLASS {class} [WITH {args}]
     *             }
     *             PROCEDURE {{name} | FROM CLASS {class}} [WITH {args}]
     *         } |
     *         FROM CLASS {class} [WITH {args}]
     *     }
     *     [ ON ERROR { STOP | CONTINUE | IGNORE } ]
     *     [ RUN ON { DATABASE | HOSTS | PARTITIONS } ]
     *     [ AS USER {user-name} ]
     *     [ ENABLE | DISABLE ]
     * </pre>
     */
    private static final Pattern PAT_CREATE_TASK =
            SPF.statement(
                SPF.token("create"), SPF.token("task"), SPF.capture("name", SPF.databaseObjectName()),
                SPF.oneOf(
                    SPF.clause(SPF.token("from"), SPF.token("class"), SPF.capture("class", SPF.className())),
                    SPF.clause(
                        SPF.token("on"), SPF.token("SCHEDULE"),
                        SPF.oneOf(
                            SPF.clause(
                                SPF.capture("intervalSchedule", SPF.oneOf("delay", "every")),
                                SPF.capture("interval", SPF.integer()),
                                SPF.capture("timeUnit", SPF.oneOf("milliseconds", "seconds", "minutes", "hours", "days"))
                            ),
                            SPF.clause(SPF.token("cron"),
                                SPF.capture("cron",
                                    SPF.clause(SPF.token("[0-9\\*\\-,/]+").withFlags(ADD_LEADING_SPACE_TO_CHILD),
                                    SPF.repeat(5, 5, SPF.token("[\\w\\*\\?\\-,/#]+"))).withFlags(ADD_LEADING_SPACE_TO_CHILD)
                                )
                            ),
                            SPF.clause(
                                SPF.token("from"), SPF.token("class"), SPF.capture("scheduleClass", SPF.className()),
                                SPF.optional(
                                    SPF.token("with"), SPF.token("\\(\\s*"),
                                    SPF.capture("scheduleParameters", SPF.commaList(SPF.token(".+"))).withFlags(ADD_LEADING_SPACE_TO_CHILD),
                                    SPF.token("\\s*\\)").withFlags(ADD_LEADING_SPACE_TO_CHILD)
                                )
                            )
                        ),
                        SPF.token("procedure"),
                        SPF.oneOf(
                            SPF.capture("procedure", SPF.token("@?[\\w.$]+")),
                            SPF.clause(
                                SPF.token("from"), SPF.token("class"), SPF.capture("generatorClass", SPF.className())
                            )
                        )
                    )
                ),
                SPF.optional(
                    SPF.token("with"), SPF.token("\\(\\s*"),
                    SPF.capture("parameters", SPF.commaList(SPF.token(".+"))).withFlags(ADD_LEADING_SPACE_TO_CHILD),
                    SPF.token("\\s*\\)").withFlags(ADD_LEADING_SPACE_TO_CHILD)
                ),
                SPF.optional(SPF.token("on"), SPF.token("error"),
                    SPF.capture("onError", SPF.oneOf(SPF.token("stop"), SPF.token("log"), SPF.token("ignore")))),
                SPF.optional(SPF.token("run"), SPF.token("on"),
                    SPF.capture("scope", SPF.oneOf("database", "hosts", "partitions"))),
                SPF.optional(SPF.token("as"), SPF.token("user"), SPF.capture("asUser", SPF.userName())),
                SPF.optional(SPF.oneOf(SPF.capture("disabled", SPF.token("disable")), SPF.token("enable")))
            ).compile("PAT_CREATE_TASK");

    /**
     * Build regex to support drop task statement in the form of
     * <p>
     * <code>
     * DROP TASK <task name> [IF EXISTS]
     * </code>
     */
    private static final Pattern PAT_DROP_TASK =
            SPF.statement(
                    SPF.token("drop"), SPF.token("task"), SPF.capture("name", SPF.databaseObjectName()),
                    SPF.ifExists())
            .compile("PAT_DROP_TASK");

    /**
     * Build regex to support alter task statement in the from of
     * <p>
     * <code>
     * ALTER TASK <task name> (ENABLE | DISABLE)
     * or
     * ALTER TASK <task name> ALTER ON ERROR (STOP | LOG | IGNORE)
     * </code>
     */
    private static final Pattern PAT_ALTER_TASK =
            SPF.statement(
                SPF.token("alter"), SPF.token("task"), SPF.capture("name", SPF.databaseObjectName()),
                    SPF.oneOf(
                        SPF.capture("action", SPF.oneOf("enable", "disable")),
                        SPF.clause(
                            SPF.token("alter"), SPF.token("on"), SPF.token("error"),
                                SPF.capture("onError",
                                    SPF.oneOf(SPF.token("stop"), SPF.token("log"), SPF.token("ignore")))
                        )
                    )
            ).compile("PAT_ALTER_TASK");

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

    /*
     * CREATE STREAM statement regex
     *
     * Capture groups:
     *  (1) stream name
     *  (2) optional target name
     */
    // There was a bug filed as ENG-11862 where the CREATE STREAM statement can fail if no space is added before the
    // opening parenthesis which indicates the start of the stream table definition.
    // The problem is that we automatically add a leading space between tokens, i.e., between unparsedStreamModifierClauses()
    // and SPF.anyColumnFields(). To avoid that, I added the ADD_LEADING_SPACE_TO_CHILD flag to SPF.anyColumnFields().
    // This flag will suppress the leading space. Then I added an optional space "\\s*". So both cases can get through.
    // Check SQLPatternPartElement.java for reason why the ADD_LEADING_SPACE_TO_CHILD flag can suppress the leading space.
    // The logic is in generateExpression(), we add the leading space when (leadingSpace && !leadingSpaceToChild) is satisfied.
    private static final Pattern PAT_CREATE_STREAM =
            SPF.statement(
                    SPF.token("create"), SPF.token("stream"), SPF.capture("name", SPF.databaseObjectName()),
                    unparsedStreamModifierClauses(),
                    new SQLPatternPartString("\\s*"),
                    SPF.anyColumnFields().withFlags(ADD_LEADING_SPACE_TO_CHILD)
            ).compile("PAT_CREATE_STREAM");

    /**
     *  If the statement starts with a VoltDB-specific DDL command,
     *  one of create procedure, create role, create function, create aggregate function,
     *  drop procedure, drop role, partition, replicate, export, import, or dr,
     *  the one match group is set to the matching command EXCEPT as special
     *  (needlessly obscure) cases, simply returns only "procedure" for "create
     *  procedure", only "role" for "create role", only "aggregate" for "create aggregate
     *  function" and only "drop" for either "drop procedure" OR "drop role".
     *  ALSO (less than helpfully) returns "drop" for non-VoltDB-specific
     *  "drop" commands like "drop table".
     *  TODO: post-processing would be much simpler if this pattern reliably
     *  accepted VoltDB commands, rejected non-VoltDB commands, and grouped
     *  the actual command keyword(s) with their arbitrary whitespace
     *  separators. A wrapper function should clean up from there.
     *
     * Regex crib sheet for the "?(...)" expressions:
     * <= means zero-width positive lookbehind; the "CREATE\\s{}" is required to
     *    match but is not part of the capture group(1)
     * :  means simple non-capture; the token sequence will not be assigned a group
     *    but it will be part of the enclosing capture group(1).
     */
    private static final Pattern PAT_ALL_VOLTDB_STATEMENT_PREAMBLES = Pattern.compile(
            "(?i)" +                               // ignore case instruction
            //TODO: why not factor \\A out of the group -- it's common to all options
            "(" +                                  // start (group 1)
            "(?<=\\ACREATE\\s{1,1024})(?>COMPOUND\\s+|DIRECTED\\s+)?(?:PROCEDURE)|" +
            "(?<=\\ACREATE\\s{1,1024})(?:ROLE|FUNCTION|TASK|AGGREGATE)|" +
            // the rest are stand-alone token options (except for the one that isn't)
            "\\ADROP|" +
            "\\APARTITION|" +
            "\\AREPLICATE|" +
            "\\AIMPORT|" +
            "\\ADR|" +
            "\\ASET|" +
            "\\AALTER\\s+(?:TASK|TOPIC)" +
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

     /**
     * Pattern: CREATE VIEW table_name MIGRATE TO TARGET target_name
     *                 (column...) other-stuff
     *
     * Capture groups:
     *  (1) table name
     *  (2) target name
     */
    private static final Pattern PAT_CREATE_VIEW_MIGRATE_TO_TARGET =
            SPF.statement(
                    SPF.token("create"), SPF.token("view"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.token("migrate"), SPF.token("to"), SPF.token("target"),
                    SPF.capture(SPF.databaseObjectName()),
                    new SQLPatternPartString("\\s*"), // see ENG-11862 for reason about adding this pattern
                    SPF.anyColumnFields().withFlags(ADD_LEADING_SPACE_TO_CHILD),
                    SPF.anythingOrNothing().withFlags(ADD_LEADING_SPACE_TO_CHILD)
            ).compile("PAT_CREATE_VIEW");

    /**
     * Pattern: CREATE VIEW table_name MIGRATE TO TOPIC topic_name
     *                 [WITH [KEY key...] [VALUE value...]]
     *                 (column...) other-stuff
     *
     * Capture groups:
     *  (1) table name
     *  (2) topic name
     *  (n) [topic key/value lists]
     */
    private static final Pattern PAT_CREATE_VIEW_MIGRATE_TO_TOPIC =
            SPF.statement(
                    SPF.token("create"), SPF.token("view"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.token("migrate"), SPF.token("to"), SPF.token("topic"),
                    SPF.capture(SPF.databaseObjectName()),
                    SPF.optional(KEY_VALUE_CLAUSE),
                    new SQLPatternPartString("\\s*"), // see ENG-11862 for reason about adding this pattern
                    SPF.anyColumnFields().withFlags(ADD_LEADING_SPACE_TO_CHILD),
                    SPF.anythingOrNothing().withFlags(ADD_LEADING_SPACE_TO_CHILD)
            ).compile("PAT_CREATE_VIEW");

    //========== Patterns from SQLCommand ==========

    private static final String EndOfLineCommentPatternString =
            "(?:\\/\\/|--)" + // '--' or even C++-style '//' comment starter
            ".*$";            // commented out text continues to end of line
    private static final Pattern OneWholeLineComment = Pattern.compile(
            "^\\s*" +                       // optional whitespace indent prior to comment
            EndOfLineCommentPatternString);
    public static final Pattern AnyWholeLineComments = Pattern.compile(
            "^\\s*" +                       // optional whitespace indent prior to comment
            EndOfLineCommentPatternString,
            Pattern.MULTILINE);
    public static final Pattern EndOfLineComment = Pattern.compile(
            EndOfLineCommentPatternString,
            Pattern.MULTILINE);

    private static final Pattern OneWhitespace = Pattern.compile("\\s");
    private static final Pattern SingleQuotedString = Pattern.compile("'[^']*'", Pattern.MULTILINE);
    private static final Pattern SingleQuotedStringContainingParameterSeparators =
            Pattern.compile(
            "'" +
            "[^',\\s]*" +  // arbitrary string content NOT matching param separators
            "[,\\s]" +     // the first match for a param separator
            "[^']*" +      // arbitrary string content
            "'",           // end of string OR start of escaped quote
            Pattern.MULTILINE);

    private static final Pattern SingleQuotedHexLiteral = Pattern.compile("[Xx]'([0-9A-Fa-f]*)'", Pattern.MULTILINE);

    // Define a common pattern to sweep up a mix of semicolons and space and
    // meaningless garbage at the end of the simpler sqlcmd directives.
    // The garbage parts (well, enough of them, anyway) are captured so that
    // they can optionally be detected in a post-processing step that MAY
    // generate a complaint about an improperly terminated command.
    private static String InitiallyForgivingDirectiveTermination =
            "\\s*" +          // spaces
            "([^;\\s]*)" +    // (first) non-space non-semicolon garbage word (last group +1)
            "[;\\s]*" +       // trailing spaces and semicolons
            "(.*)" +          // trailing garbage (last group +2)
            "$";
    // HELP can support sub-commands someday. Capture group 2 is the sub-command.
    private static final Pattern HelpToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "help" +          // required HELP command token
            "(\\W|$)" +       // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that help
            // command diagnostics can "own" any line starting with the word
            // help.
            "\\s*" +          // optional whitespace before subcommand
            "([^;\\s]*)" +    // optional subcommand (group 2)
            InitiallyForgivingDirectiveTermination,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern EchoToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "echo" +          // required ECHO command token
            "(\\W|$)" +       // require an end to the keyword OR EOL (group 1)
            "(.*)" +          // Make everything that follows optional (group 2).
            "$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern EchoErrorToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "echoerror" +     // required ECHOERROR command token
            "(\\W|$)" +       // require an end to the keyword OR EOL (group 1)
            "(.*)" +          // Make everything that follows optional (group 2).
            "$",
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ExitToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "(?:exit|quit)" + // keyword alternatives, synonymous so don't bother capturing
            "(\\W|$)" +       // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that exit/quit
            // command diagnostics can "own" any line starting with the word
            // exit or quit.
            InitiallyForgivingDirectiveTermination,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern ShowToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "(?:list|show)" + // keyword alternatives, synonymous so don't bother capturing
            "(\\W|$)" +       // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that list/show
            // command diagnostics can "own" any line starting with the word
            // list or show.
            "\\s*" +          // extra spaces
            "([^;\\s]*)" +    // non-space non-semicolon subcommand (group 2)
            InitiallyForgivingDirectiveTermination,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern DescribeToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "(?:desc|describe)" + // keyword alternatives, synonymous so don't bother capturing
            "(\\W|$)" +       // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that desc/describe
            // command diagnostics can "own" any line starting with the word
            // desc or describe.
            "\\s*" +          // extra spaces
            "([^;\\s]*)" +    // non-space non-semicolon subcommand (group 2)
            InitiallyForgivingDirectiveTermination,
            Pattern.CASE_INSENSITIVE);
    private static final Pattern RecallToken = Pattern.compile(
            "^\\s*" +      // optional indent at start of line
            "recall" +     // required RECALL command token
            "(\\W|$)" +    // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that recall command
            // diagnostics can "own" any line starting with the word recall.
            "\\s*" +          // extra spaces
            "([^;\\s]*)" + // (first) non-space non-semicolon garbage word (group 2)
            InitiallyForgivingDirectiveTermination,
            Pattern.CASE_INSENSITIVE);

    private static final Pattern SemicolonToken = Pattern.compile(
            "^.*" +           // match anything
            ";+" +            // one required semicolon at end except for
            "\\s*" +          // optional whitespace
            "(--)?$",         // and an optional end-of-line comment
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
            "\\s+" +         // required preceding whitespace
            "-inlinebatch",  // -inlinebatch option, whitespace terminated
            Pattern.CASE_INSENSITIVE);

    private static final Pattern FilenameToken = Pattern.compile(
            "\\s*" +       // optional preceding whitespace
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

    // QUERYSTATS is followed by a SQL query. Capture group 2 is the query.
    private static final Pattern QueryStatsToken = Pattern.compile(
            "^\\s*" +         // optional indent at start of line
            "querystats" +          // required QUERYSTATS command token
            "(\\W|$)" +       // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that help
            // command diagnostics can "own" any line starting with the word
            // help.
            "\\s*" +          // optional whitespace before subcommand
            "([^;\\s]*)" +    // optional subcommand (group 2)
            InitiallyForgivingDirectiveTermination,
            Pattern.CASE_INSENSITIVE);

    // Query Execution
    private static final Pattern ExecuteCallPreamble = Pattern.compile(
            "^\\s*" +            // optional indent at start of line
            "(?:exec|execute)" + // required command or alias non-grouping
            "(\\W|$)" +          // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that exec command
            // diagnostics can "own" any line starting with the word
            // exec or execute.
            "\\s*",              // extra spaces
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explain" (case insensitive).  We'll convert them to @Explain invocations.
    private static final Pattern ExplainCallPreamble = Pattern.compile(
            "^\\s*" +            // optional indent at start of line
            "explain" +          // required command, whitespace terminated
            "(\\W|$)" +          // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that explain command
            // diagnostics can "own" any line starting with the word
            // explain.
            "\\s*",              // extra spaces
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explainjson" (case insensitive).  We'll convert them to @ExplainJSON invocations.
    private static final Pattern ExplainJSONCallPreamble = Pattern.compile(
            "^\\s*" +            // optional indent at start of line
            "explainjson" +      // required command, whitespace terminated
            "(\\W|$)" +          // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that explain command
            // diagnostics can "own" any line starting with the word
            // explain.
            "\\s*",              // extra spaces
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explaincatalog" (case insensitive).  We'll convert them to @ExplainCatalog invocations.
    private static final Pattern ExplainCatalogCallPreamble = Pattern.compile(
            "^\\s*" +            // optional indent at start of line
            "explaincatalog" +   // required command, whitespace terminated
            "\\s*",              // extra spaces
            Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explainproc" (case insensitive).  We'll convert them to @ExplainProc invocations.
    private static final Pattern ExplainProcCallPreamble = Pattern.compile(
            "^\\s*" +            // optional indent at start of line
            "explainProc" +      // required command, whitespace terminated
            "(\\W|$)" +          // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that explainproc command
            // diagnostics can "own" any line starting with the word
            // explainproc.
            "\\s*",              // extra spaces
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);
    // Match queries that start with "explainview" (case insensitive).  We'll convert them to @ExplainView invocations.
    private static final Pattern ExplainViewCallPreamble = Pattern.compile(
            "^\\s*" +            // optional indent at start of line
            "explainView" +      // required command, whitespace terminated
            "(\\W|$)" +          // require an end to the keyword OR EOL (group 1)
            // Make everything that follows optional so that explainproc command
            // diagnostics can "own" any line starting with the word
            // explainview.
            "\\s*",              // extra spaces
            Pattern.MULTILINE + Pattern.CASE_INSENSITIVE);

    private static final Pattern Unquote = Pattern.compile("^'|'$", Pattern.MULTILINE);

    private static final Map<String, String> FRIENDLY_TYPE_NAMES =
            ImmutableMap.<String, String>builder().put("tinyint", "byte numeric")
                                                  .put("smallint", "short numeric")
                                                  .put("int", "numeric")
                                                  .put("integer", "numeric")
                                                  .put("bigint", "long numeric")
                                                  .build();

    // The argument capture group for LOAD/REMOVE CLASSES loosely captures everything
    // through the trailing semicolon. It relies on post-parsing code to make sure
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
     * Match statement against set global parameter pattern
     * @param statement statement to match against
     * @return          pattern matcher object
     */
    public static Matcher matchSetGlobalParam(String statement)
    {
        return SET_GLOBAL_PARAM.matcher(statement);
    }

    /**
     * Match statement against pattern for all VoltDB-specific statement preambles
     * TODO: Much more useful would be a String parseVoltDBSpecificDdlStatementPreamble
     * function that used a corrected pattern and some minimal post-processing to return
     * an upper cased single-space-separated preamble token string for ONLY VoltDB-specific
     * commands (or null if not a match).
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
     * Match statement against drop stream pattern
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchDropStream(String statement)
    {
        return PAT_DROP_STREAM.matcher(statement);
    }

    /**
     * Match statement against create stream pattern
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchCreateStream(String statement)
    {
        return PAT_CREATE_STREAM.matcher(statement);
    }

    /**
     * Match statement against create table ...  pattern,
     * with no migrate or export clause.
     * @param statement statement to match against
     * @return          pattern matcher object
     */
    public static Matcher matchCreateTablePlain(String statement) {
        return PAT_CREATE_TABLE_PLAIN.matcher(statement);
    }

    /**
     * Match statement against create table ... migrate to target ... pattern.
     * @param statement statement to match against
     * @return          pattern matcher object
     */
    public static Matcher matchCreateTableMigrateToTarget(String statement) {
        return PAT_CREATE_TABLE_MIGRATE_TO_TARGET.matcher(statement);
    }

     /**
     * Match statement against create table ... migrate to topic ... pattern.
     * @param statement statement to match against
     * @return          pattern matcher object
     */
    public static Matcher matchCreateTableMigrateToTopic(String statement) {
        return PAT_CREATE_TABLE_MIGRATE_TO_TOPIC.matcher(statement);
    }

    /**
     * Match statement against create table ... export to target ... pattern.
     * @param statement statement to match against
     * @return          pattern matcher object
     */
    public static Matcher matchCreateTableExportToTarget(String statement) {
        return PAT_CREATE_TABLE_EXPORT_TO_TARGET.matcher(statement);
    }

    /**
     * Match statement against create table ... export to topic ... pattern.
     * @param statement statement to match against
     * @return          pattern matcher object
     */
    public static Matcher matchCreateTableExportToTopic(String statement) {
        return PAT_CREATE_TABLE_EXPORT_TO_TOPIC.matcher(statement);
    }

    public static Matcher matchAlterTTL(String statement)
    {
        return PAT_ALTER_TTL.matcher(statement);
    }

    /**
     * Match statement against create view ... migrate to target ... pattern.
     * @param statement statement to match against
     * @return          pattern matcher object
     */
    public static Matcher matchCreateViewMigrateToTarget(String statement) {
        return PAT_CREATE_VIEW_MIGRATE_TO_TARGET.matcher(statement);
    }

     /**
     * Match statement against create view ... migrate to topic ... pattern.
     * @param statement statement to match against
     * @return          pattern matcher object
     */
    public static Matcher matchCreateViewMigrateToTopic(String statement) {
        return PAT_CREATE_VIEW_MIGRATE_TO_TOPIC.matcher(statement);
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
     * Match statement against pattern for create procedure as SQL
     * with allow/partition clauses with multiple statements
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchCreateProcedureAsMultiStmtSQL(String statement)
    {
        return PAT_CREATE_PROCEDURE_FROM_MULTI_STMT_SQL.matcher(statement);
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
     * Match statement against the pattern for create function from method
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchCreateFunctionFromMethod(String statement)
    {
        return PAT_CREATE_FUNCTION_FROM_METHOD.matcher(statement);
    }

    /**
     * Match statement against the pattern for create aggregate function from class
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchCreateAggregateFunctionFromClass(String statement)
    {
        return PAT_CREATE_AGGREGATE_FUNCTION_FROM_CLASS.matcher(statement);
    }

    /**
     * Match statement against the pattern for drop aggregate function
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchDropAggregateFunction(String statement)
    {
        return PAT_DROP_AGGREGATE_FUNCTION.matcher(statement);
    }

    /**
     * Match statement against the pattern for drop function
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchDropFunction(String statement)
    {
        return PAT_DROP_FUNCTION.matcher(statement);
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
     * Match statement against pattern for export/partition clauses of create stream statement
     * @param statement  statement to match against
     * @return           pattern matcher object
     */
    public static Matcher matchAnyCreateStreamStatementClause(String statement)
    {
        return PAT_ANY_CREATE_STREAM_STATEMENT_CLAUSE.matcher(statement);
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
     * Match statement against pattern for create task
     * <p>
     * If a match is found the following named groups are in the returned {@link Matcher}:
     * <table>
     * <tr>
     * <th>Capture name</th>
     * <th>Presence</th>
     * <th>Description</th>
     * </tr>
     * <tr>
     * <td>name</td>
     * <td>Required</td>
     * <td>Name of task</td>
     * </tr>
     * <tr>
     * <td>class|intervalSchedule|cron|scheduleClass</td>
     * <td>Required</td>
     * <td>Scheduler class, fixed delay, every or cron expression to be used</td>
     * </tr>
     * <tr>
     * <td>interval</td>
     * <td>Required for delay and every</td>
     * <td>Interval of time for scheduling</td>
     * </tr>
     * <tr>
     * <td>timeUnit</td>
     * <td>Required for delay and every</td>
     * <td>Time unit of the interval</td>
     * </tr>
     * <tr>
     * <td>scheduleParameters</td>
     * <td>Optional</td>
     * <td>Parameters to pass to the scheduleClass</td>
     * </tr>
     * <tr>
     * <td>procedure|generatorClass</td>
     * <td>Required for cron, delay, every or scheduleClass</td>
     * <td>Procedure name with comma separated list of parameters to pass to the procedure</td>
     * </tr>
     * <tr>
     * <td>parameters</td>
     * <td>Optional</td>
     * <td>Comma separated list of parameters to pass to the scheduler, procedure or generator</td>
     * </tr>
     * <tr>
     * <td>onError</td>
     * <td>Optional</td>
     * <td>How error responses from a procedure should be handled</td>
     * </tr>
     * <tr>
     * <td>scope</td>
     * <td>Optional</td>
     * <td>Scope of the task. IE database, hosts, partitions</td>
     * </tr>
     * <tr>
     * <td>asUser</td>
     * <td>Optional</td>
     * <td>Which user to use to execute the procedures run by the scheduler</td>
     * </tr>
     * <td>disabled</td>
     * <td>Optional</td>
     * <td>If present this task is part of the catalog but not executed</td>
     * </tr>
     * </table>
     *
     * @param statement statement to match against
     * @return pattern matcher object
     */
    public static Matcher matchCreateTask(String statement) {
        return PAT_CREATE_TASK.matcher(statement);
    }

    /**
     * Match statement against pattern for drop task
     * <p>
     * If a match is found the following named groups are in the returned {@link Matcher}:
     * <table>
     * <tr>
     * <th>Capture name</th>
     * <th>Presence</th>
     * <th>Description</th>
     * </tr>
     * <tr>
     * <td>name</td>
     * <td>Required</td>
     * <td>Name of task</td>
     * </tr>
     * <tr>
     * <td>ifExists</td>
     * <td>Optional</td>
     * <td>If present then it is not an error if the task does not exist</td>
     * </tr>
     * </table>
     *
     * @param statement statement to match against
     * @return pattern matcher object
     */
    public static Matcher matchDropTask(String statement) {
        return PAT_DROP_TASK.matcher(statement);
    }

    /**
     * Match statement against pattern for alter task
     * <p>
     * If a match is found the following named groups are in the returned {@link Matcher}:
     * <table>
     * <tr>
     * <th>Capture name</th>
     * <th>Presence</th>
     * <th>Description</th>
     * </tr>
     * <tr>
     * <td>name</td>
     * <td>Required</td>
     * <td>Name of task</td>
     * </tr>
     * <tr>
     * <td>action</td>
     * <td>Required</td>
     * <td>What alter action should be performed. Enable or disable</td>
     * </tr>
     * <tr>
     * <td>onError</td>
     * <td>Optional</td>
     * <td>How error responses from a procedure should be handled</td>
     * </tr>
     * </table>
     *
     * @param statement statement to match against
     * @return pattern matcher object
     */
    public static Matcher matchAlterTask(String statement) {
        return PAT_ALTER_TASK.matcher(statement);
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
     *  (2) PARTITION clause: table name
     *  (3) PARTITION clause: column name
     *  (4) PARTITION clause: parameter number
     *  (5) PARTITION clause: table name 2
     *  (6) PARTITION clause: column name 2
     *  (7) PARTITION clause: parameter number 2
     *  (8) DIRECTED|COMPOUND modifier
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
                    ),
                    // parse a two-partition transaction clause
                    SPF.optional(
                        SPF.clause(
                            SPF.token("and"), SPF.token("on"), SPF.token("table"),
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
                     )
                ),
                SPF.group(captureTokens, SPF.tokenAlternatives("directed", "compound"))
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
        return SPF.capture(SPF.repeat(makeInnerProcedureModifierClausePattern(false)))
                  .withFlags(SQLPatternFactory.ADD_LEADING_SPACE_TO_CHILD);
    }

    /**
     * Build a pattern segment to accept single optional EXPORT, PARTITION,
     * to modify CREATE STREAM statements.
     *
     * @param captureTokens  Capture individual tokens if true
     * @return               Inner pattern to be wrapped by the caller as appropriate
     *
     * Capture groups, when captureTokens is true (with named capture groups):
     *
     *  (1) EXPORT TO TARGET    ("targetName"): target name
     *  (2) PARTITION           ("partitionColumnName"): column name
     */
    public static final String CAPTURE_EXPORT_TARGET = "targetName";
    public static final String CAPTURE_EXPORT_TOPIC = "topicName";
    public static final String CAPTURE_STREAM_PARTITION_COLUMN = "partitionColumnName";

    // TODO: consider using KEY_VALUE_CLAUSE
    private static SQLPatternPart makeInnerStreamModifierClausePattern(boolean captureTokens)
    {
        return
            SPF.oneOf(
                SPF.clause(
                    SPF.token("export"),SPF.token("to"),SPF.token("target"),
                    SPF.group(captureTokens, CAPTURE_EXPORT_TARGET, SPF.databaseObjectName())
                ),
                SPF.clause(
                        SPF.token("export"),SPF.token("to"),SPF.token("topic"),
                        SPF.group(captureTokens, CAPTURE_EXPORT_TOPIC, SPF.databaseObjectName()),
                        SPF.optional(
                                SPF.token("with"),
                                SPF.optional(
                                        SPF.token("key\\s*\\(\\s*"),
                                        SPF.capture(CAPTURE_TOPIC_KEY_COLUMNS, SPF.commaList(SPF.databaseObjectName())).withFlags(ADD_LEADING_SPACE_TO_CHILD),
                                        SPF.token("\\s*\\)").withFlags(ADD_LEADING_SPACE_TO_CHILD)
                                ),
                                SPF.optional(
                                        SPF.token("value\\s*\\(\\s*"),
                                        SPF.capture(CAPTURE_TOPIC_VALUE_COLUMNS, SPF.commaList(SPF.databaseObjectName())).withFlags(ADD_LEADING_SPACE_TO_CHILD),
                                        SPF.token("\\s*\\)").withFlags(ADD_LEADING_SPACE_TO_CHILD)
                                )
                            )
                    ),
                SPF.clause(
                    SPF.token("partition"), SPF.token("on"), SPF.token("column"),
                    SPF.group(captureTokens, CAPTURE_STREAM_PARTITION_COLUMN, SPF.databaseObjectName())
                )
            );
    }

    /**
     * Build a pattern segment to accept and parse a single optional EXPORT or PARTITION
     * clause used to modify a CREATE STREAM statement.
     *
     * @return EXPORT/PARTITION modifier clause parsing pattern.
     *
     * Capture groups:
     *  (1) EXPORT clause: target name     *
     *  (2) PARTITION clause: column name
     */
    static SQLPatternPart parsedStreamModifierClause() {
        return SPF.clause(makeInnerStreamModifierClausePattern(true));
    }

    /**
     * Build a pattern segment to recognize all the EXPORT or PARTITION modifier clauses
     * of a CREATE STREAM statement.
     *
     * @return Pattern to be used by the caller inside a CREATE STREAM pattern.
     *
     * Capture groups:
     *  (1) All EXPORT/PARTITION modifier clauses as one string
     */
    private static SQLPatternPart unparsedStreamModifierClauses() {
        // Force the leading space to go inside the repeat block.
        return SPF.capture(SPF.repeat(makeInnerStreamModifierClausePattern(false))).withFlags(SQLPatternFactory.ADD_LEADING_SPACE_TO_CHILD);
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

    // Process the quirky syntax for "exec" arguments -- a procedure name and
    // parameter values (optionally SINGLE-quoted) separated by arbitrary
    // whitespace and commas.
    // Assumes that this is the exact text between the "exec/execute" and
    // its terminating semicolon (exclusive) and that
    // to the extent that comments are supported they have already been stripped out.
    private static List<String> parseExecParameters(String paramText)
    {
        final String SafeParamStringValuePattern = "#(SQL_PARSER_SAFE_PARAMSTRING)";
        // Find all quoted strings.
        // Mask out strings that contain whitespace or commas
        // that must not be confused with parameter separators.
        // "Safe" strings that don't contain these characters don't need to be masked
        // but they DO need to be found and explicitly skipped so that their closing
        // quotes don't trigger a false positive for the START of an unsafe string.
        // Skipping is accomplished by resetting paramText to an offset substring
        // after copying the skipped (or substituted) text to a string builder.
        ArrayList<String> originalString = new ArrayList<>();
        Matcher stringMatcher = SingleQuotedString.matcher(paramText);
        StringBuilder safeText = new StringBuilder();
        while (stringMatcher.find()) {
            // Save anything before the found string.
            safeText.append(paramText.substring(0, stringMatcher.start()));
            String asMatched = stringMatcher.group();
            if (SingleQuotedStringContainingParameterSeparators.matcher(asMatched).matches()) {
                // The matched string is unsafe, provide cover for it in safeText.
                originalString.add(asMatched);
                safeText.append(SafeParamStringValuePattern);
            } else {
                // The matched string is safe. Add it to safeText.
                safeText.append(asMatched);
            }
            paramText = paramText.substring(stringMatcher.end());
            stringMatcher = SingleQuotedString.matcher(paramText);
        }
        // Save anything after the last found string.
        safeText.append(paramText);

        ArrayList<String> params = new ArrayList<>();
        int subCount = 0;
        int neededSubs = originalString.size();
        // Split the params at the separators
        String[] split = safeText.toString().split("[\\s,]+");
        for (String fragment : split) {
            if (fragment.isEmpty()) {
                continue; // ignore effects of leading or trailing separators
            }
            // Replace each substitution in order exactly once.
            if (subCount < neededSubs) {
                // Substituted strings will normally take up an entire parameter,
                // but some cases like parameters containing escaped single quotes
                // may require multiple serial substitutions.
                while (fragment.indexOf(SafeParamStringValuePattern) > -1) {
                    fragment = fragment.replace(SafeParamStringValuePattern,
                            originalString.get(subCount));
                    ++subCount;
                }
            }
            params.add(fragment);
        }
        assert(subCount == neededSubs);
        return params;
    }

    /**
     * Check whether statement is terminated by a semicolon.
     * @param statement  statement to check
     * @return           true if it is terminated by a semicolon
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
        //TODO: consider processing match groups to detect and
        // complain about garbage parameters.
        return ExitToken.matcher(statement).matches();
    }

    /**
     * Results from parseRecallStatement
     */
    public static class ParseRecallResults
    {
        private final int line;
        private final String error;

        ParseRecallResults(int line)
        {
            this.line = line;
            this.error = null;
        }

        ParseRecallResults(String error)
        {
            this.line = -1;
            this.error = error;
        }

        // Attempts to use these methods gets a mysterious NoSuchMethodError,
        // so keep them disabled and keep the attributes public for now.
        public int getLine() { return line; }
        public String getError() { return error; }
    }

    /**
     * Parse RECALL statement for sqlcmd.
     * @param statement  statement to parse
     * @param lineMax    maximum line # + 1
     * @return           results object or NULL if statement wasn't recognized
     */
    public static ParseRecallResults parseRecallStatement(String statement, int lineMax)
    {
        Matcher matcher = RecallToken.matcher(statement);
        if (matcher.matches()) {
            String commandWordTerminator = matcher.group(1);
            String lineNumberText = matcher.group(2);
            String error;
            if (OneWhitespace.matcher(commandWordTerminator).matches()) {
                String trailings = matcher.group(3) + ";" + matcher.group(4);
                // In a valid command, both "trailings" groups should be empty.
                if (trailings.equals(";")) {
                    try {
                        int line = Integer.parseInt(lineNumberText) - 1;
                        if (line < 0 || line > lineMax) {
                            throw new NumberFormatException();
                        }
                        // Return the recall line number.
                        return new ParseRecallResults(line);
                    }
                    catch (NumberFormatException e) {
                        error = "Invalid RECALL line number argument: '" + lineNumberText + "'";
                    }
                }
                // For an invalid form of the command,
                // return an approximation of the garbage input.
                else {
                    error = "Invalid RECALL line number argument: '" +
                            lineNumberText + " " + trailings + "'";
                }
            }
            else if (commandWordTerminator.equals("") || commandWordTerminator.equals(";")) {
                error = "Incomplete RECALL command. RECALL expects a line number argument.";
            } else {
                error = "Invalid RECALL command: a space and line number are required after 'recall'";
            }
            return new ParseRecallResults(error);
        }
        return null;
    }

    /**
     * An enum that describes the options that can be applied
     * to sqlcmd's "file" command
     */
    static public enum FileOption {
        PLAIN {
            @Override
            String optionString() { return ""; }
        },
        BATCH {
            @Override
            String optionString() { return "-batch "; }
        },
        INLINEBATCH {
            @Override
            String optionString() { return "-inlinebatch "; }
        };

        abstract String optionString();
    }

    /**
     * This class encapsulates information produced by
     * parsing sqlcmd's "file" command.
     */
    public static class FileInfo {
        private final FileInfo m_context;
        private final FileOption m_option;
        private final File m_file;
        private final String m_delimiter;
        private static FileInfo m_oneForSystemIn = null; // Create on demand.

        public FileInfo(String path) {
            m_context = null;
            m_option = FileOption.PLAIN;
            m_file = new File(path);
            m_delimiter = null;
        }
        FileInfo(FileInfo context, FileOption option, String filenameOrDelimiter) {
            m_context = context;
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
                assert(m_context != null);
                m_file = null;
                m_delimiter = filenameOrDelimiter;
                break;
            }
        }

        // special case constructor for System.in.
        private FileInfo() {
            m_context = null;
            m_option = FileOption.PLAIN;
            m_file = null;
            m_delimiter = null;
        }

        /** @return a dummy FileInfo instance to describe System.in **/
        public static FileInfo forSystemIn() {
            if (m_oneForSystemIn == null) {
                m_oneForSystemIn = new FileInfo() {
                    @Override
                    public String getFilePath() {
                        return "(standard input)";
                    }
                };
            }
            return m_oneForSystemIn;
        }

        public File getFile() {
            return m_file;
        }

        public String getFilePath() {
            switch (m_option) {
            case PLAIN:
            case BATCH:
                return m_file.getPath();
            case INLINEBATCH:
            default:
                String filePath = (m_context == null) ? "AdHoc DDL Input" : m_context.getFilePath();
                assert(m_option == FileOption.INLINEBATCH);
                return "(inline batch delimited by '" + m_delimiter +
                        "' in " + filePath + ")";
            }
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

        /**
         * This is actually echoed back to the user so make it look
         * more or less like their input line.
         **/
        @Override
        public String toString() {
            return "FILE " + m_option.optionString() +
                    ((m_file != null) ? m_file.toString() : m_delimiter);
        }
    }

    /**
     * Parse FILE statement for sqlcmd.
     * @param fileInfo   optional parent file context for better diagnostics.
     * @param statement  statement to parse
     * @return           File object or NULL if statement wasn't recognized
     */
    public static List<FileInfo> parseFileStatement(FileInfo parentContext, String statement)
    {
        Matcher fileMatcher = FileToken.matcher(statement);

        if (! fileMatcher.lookingAt()) {
            // This input does not start with FILE,
            // so it's not a file command, it's something else.
            // Return to caller a null and no errors.
            return null;
        }

        String remainder = statement.substring(fileMatcher.end(), statement.length());

        List<FileInfo> filesInfo = new ArrayList<>();

        Matcher inlineBatchMatcher = DashInlineBatchToken.matcher(remainder);
        if (inlineBatchMatcher.lookingAt()) {
            remainder = remainder.substring(inlineBatchMatcher.end(), remainder.length());
            Matcher delimiterMatcher = DelimiterToken.matcher(remainder);

            // use matches here (not lookingAt) because we want to match
            // all of the remainder, not just beginning
            if (delimiterMatcher.matches()) {
                String delimiter = delimiterMatcher.group(1);
                filesInfo.add(new FileInfo(parentContext, FileOption.INLINEBATCH, delimiter));
                return filesInfo;
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

        // remove spaces before and after filenames
        remainder = remainder.trim();

        // split filenames assuming they are separated by space ignoring spaces within quotes
        // tests for parsing in TestSqlCmdInterface.java
        List<String> filenames = new ArrayList<>();
        Pattern regex = Pattern.compile("[^\\s\']+|'[^']*'");
        Matcher regexMatcher = regex.matcher(remainder);
        while (regexMatcher.find()) {
            filenames.add(regexMatcher.group());
        }

        for (String filename : filenames) {
            Matcher filenameMatcher = FilenameToken.matcher(filename);
            // Use matches to match all input, not just beginning
            if (filenameMatcher.matches()) {
                filename = filenameMatcher.group(1);

                // Trim whitespace from beginning and end of the file name.
                // User may have wanted quoted whitespace at the beginning or end
                // of the file name, but that seems very unlikely.
                filename = filename.trim();

                if (filename.startsWith("~")) {
                    filename = filename.replaceFirst("~", System.getProperty("user.home"));
                }
                filesInfo.add(new FileInfo(parentContext, option, filename));
            }
        }

        // If no filename, or a filename of only spaces, then throw an error.
        if ( filesInfo.size() == 0 ) {
            String msg = String.format("Did not find valid file name in \"file%s\" command.",
                    option == FileOption.BATCH ? " -batch" : "");
            throw new SQLParser.Exception(msg);
        }

        return filesInfo;
    }
    /**
     * Parse FILE statement for interactive sqlcmd (or simple tests).
     * @param statement  statement to parse
     * @return           File object or NULL if statement wasn't recognized
     */
    public static List<FileInfo> parseFileStatement(String statement)
    {
        // There is no parent file context to reference.
        return parseFileStatement(null, statement);
    }

    /**
     * Parse a SHOW or LIST statement for sqlcmd.
     * @param statement  statement to parse
     * @return           String containing captured argument(s) possibly invalid,
     *                   or null if a show/list statement wasn't recognized
     */
    public static String parseShowStatementSubcommand(String statement)
    {
        Matcher matcher = ShowToken.matcher(statement);
        if (matcher.matches()) {
            String commandWordTerminator = matcher.group(1);
            if (OneWhitespace.matcher(commandWordTerminator).matches()) {
                String trailings = matcher.group(3) + ";" + matcher.group(4);
                // In a valid command, both "trailings" groups should be empty.
                if (trailings.equals(";")) {
                    // Return the subcommand keyword -- possibly a valid one.
                    return matcher.group(2);
                }
                // For an invalid form of the command,
                // return an approximation of the garbage input.
                return matcher.group(2) + " " + trailings;
            }
            if (commandWordTerminator.equals("") || commandWordTerminator.equals(";")) {
                return commandWordTerminator; // EOL or ; reached before subcommand
            }
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
            String commandWordTerminator = matcher.group(1);
            if (OneWhitespace.matcher(commandWordTerminator).matches()) {
                String trailings = matcher.group(3) + ";" + matcher.group(4);
                // In a valid command, both "trailings" groups should be empty.
                if (trailings.equals(";")) {
                    // Return the subcommand keyword -- possibly a valid one.
                    return matcher.group(2);
                }
                // For an invalid form of the command,
                // return an approximation of the garbage input.
                return matcher.group(2) + " " + trailings;
            }
            if (commandWordTerminator.equals("") || commandWordTerminator.equals(";")) {
                return ""; // EOL or ; reached before subcommand
            }
            return matcher.group(1).trim();
        }
        return null;
    }

    /**
     * Parse a date string.  We parse the documented forms, which are:
     * <ul>
     *   <li>YYYY-MM-DD</li>
     *   <li>YYYY-MM-DD HH:MM:SS</li>
     *   <li>YYYY-MM-DD HH:MM:SS.SSSSSS</li>
     * </ul>
     *
     * As it turns out, TimestampType takes string parameters in just this
     * format.  So, we defer to TimestampType, and return what it
     * constructs.  This has microsecond granularity.
     *
     * @param dateIn  input date string
     * @return        TimestampType object
     * @throws SQLParser.Exception
     */
    public static TimestampType parseDate(String dateIn)
    {
        // Remove any quotes around the timestamp value.  ENG-2623
        boolean shouldTrim = (dateIn.startsWith("\"") && dateIn.endsWith("\"")) || (dateIn.startsWith("'") && dateIn.endsWith("'"));
        String dateRepled = shouldTrim ? dateIn.substring(1, dateIn.length() - 1) : dateIn;
        return new TimestampType(dateRepled);
    }

    public static GeographyPointValue parseGeographyPoint(String param) {
        int spos = param.indexOf("'");
        int epos = param.lastIndexOf("'");
        if (spos < 0) {
            spos = -1;
        }
        if (epos < 0) {
            epos = param.length();
        }
        return GeographyPointValue.fromWKT(param.substring(spos+1, epos));
    }

    public static GeographyValue parseGeography(String param) {
        int spos = param.indexOf("'");
        int epos = param.lastIndexOf("'");
        if (spos < 0) {
            spos = -1;
        }
        if (epos < 0) {
            epos = param.length();
        }
        return GeographyValue.fromWKT(param.substring(spos+1, epos));
    }


    /**
     * Given a parameter string, if it's of the form x'0123456789ABCDEF',
     * return a string containing just the digits.  Otherwise, return null.
     */
    public static String getDigitsFromHexLiteral(String paramString) {
        Matcher matcher = SingleQuotedHexLiteral.matcher(paramString);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Given a string of hex digits, produce a long value, assuming
     * a 2's complement representation.
     */
    public static long hexDigitsToLong(String hexDigits) throws SQLParser.Exception {

        // BigInteger.longValue() will truncate to the lowest 64 bits,
        // so we need to explicitly check if there's too many digits.
        if (hexDigits.length() > 16) {
            throw new SQLParser.Exception("Too many hexadecimal digits for BIGINT value");
        }

        if (hexDigits.length() == 0) {
            throw new SQLParser.Exception("Zero hexadecimal digits is invalid for BIGINT value");
        }

        // The method
        //   Long.parseLong(<digits>, <radix>);
        // Doesn't quite do what we want---it expects a '-' to
        // indicate negative values, and doesn't want the sign bit set
        // in the hex digits.
        //
        // Once we support Java 1.8, we can use Long.parseUnsignedLong(<digits>, 16)
        // instead.

        long val = new BigInteger(hexDigits, 16).longValue();
        return val;
    }

    /**
     * Results returned by parseExecuteCall()
     */
    public static class ExecuteCallResults
    {
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
                            // Could be literal of the form x'0007'
                            // or just a simple decimal literal
                            String hexDigits = getDigitsFromHexLiteral(param);
                            if (hexDigits != null) {
                                objParam = hexDigitsToLong(hexDigits);
                            }
                            else {
                                // It's a decimal literal
                                objParam = Long.parseLong(param);
                            }
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
                        } else if (paramType.equals("geography_point")) {
                            objParam = parseGeographyPoint(param);
                        }
                        else if (paramType.equals("geography")) {
                            objParam = parseGeography(param);
                        }
                        else if (paramType.equals("varbinary") || paramType.equals("tinyint_array")) {
                            // A VARBINARY literal may or may not be
                            // prefixed with an X.
                            String hexDigits = getDigitsFromHexLiteral(param);
                            if (hexDigits == null) {
                                hexDigits = Unquote.matcher(param).replaceAll("");
                            }
                            // The following call with throw an exception if we
                            // have an odd number of hex digits.
                            objParam = Encoder.hexDecode(hexDigits);
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

        @Override
        public String toString() {
            return "ExecuteCallResults { "
                            + "procedure: " + procedure + ", "
                            + "params: " + params + ", "
                            + "paramTypes: " + paramTypes + " }";
        }
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
        assert(procedures != null);
        return parseExecuteCallInternal(statement, procedures);
    }

    /**
     * Parse EXECUTE procedure call for testing without looking up parameter types.
     * Used for testing.
     * @param statement   statement to parse
     * @param procedures  maps procedures to parameter signature maps
     * @return            results object or NULL if statement wasn't recognized
     * @throws SQLParser.Exception
     */
    public static ExecuteCallResults parseExecuteCallWithoutParameterTypes(
            String statement) throws SQLParser.Exception
    {
        return parseExecuteCallInternal(statement, null);
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
            String statement, Map<String,Map<Integer, List<String>>> procedures
            ) throws SQLParser.Exception
    {
        Matcher matcher = ExecuteCallPreamble.matcher(statement);
        if ( ! matcher.lookingAt()) {
            return null;
        }

        String commandWordTerminator = matcher.group(1);
        if (OneWhitespace.matcher(commandWordTerminator).matches() ||
                // Might as well accept a comma delimiter anywhere in the exec command,
                // even near the start
                commandWordTerminator.equals(",")) {
            ExecuteCallResults results = new ExecuteCallResults();
            String rawParams = statement.substring(matcher.end());
            results.params = parseExecParameters(rawParams);
            results.procedure = results.params.remove(0);
            // TestSqlCmdInterface passes procedures==null because it
            // doesn't need/want the param types.
            if (procedures == null) {
                results.paramTypes = null;
                return results;
            }
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
                throw new SQLParser.Exception("Invalid parameter count for procedure: %s (expected: %s received: %d)",
                                              results.procedure, expectedSizes, results.params.size());
            }
            return results;
        }
        if (commandWordTerminator.equals(";")) {
            // EOL or ; reached before subcommand
            throw new SQLParser.Exception("Incomplete EXECUTE command. EXECUTE requires a procedure name argument.");
        }
        throw new SQLParser.Exception("Invalid EXECUTE command. unexpected input: '" + commandWordTerminator + "'.");
    }

    /**
     * Parse EXPLAIN <query>
     * @param statement  statement to parse
     * @return           query parameter string or NULL if statement wasn't recognized
     */
    public static String parseExplainCall(String statement)
    {
        Matcher matcher = ExplainCallPreamble.matcher(statement);
        if ( ! matcher.lookingAt()) {
            return null;
        }
        return statement.substring(matcher.end());
    }

    /**
     * Parse EXPLAINJSON <query>
     * @param statement  statement to parse
     * @return           query parameter string or NULL if statement wasn't recognized
     */
    public static String parseExplainJSONCall(String statement)
    {
        Matcher matcher = ExplainJSONCallPreamble.matcher(statement);
        if ( ! matcher.lookingAt()) {
            return null;
        }
        return statement.substring(matcher.end());
    }

    /**
     * Parse EXPLAINCATALOG
     * @param statement  statement to parse
     * @return           true if recognized
     */
    public static boolean parseExplainCatalogCall(String statement)
    {
        Matcher matcher = ExplainCatalogCallPreamble.matcher(statement);
        return matcher.matches();
    }

    /**
     * Parse EXPLAINPROC <procedure>
     * @param statement  statement to parse
     * @return           procedure name parameter string or NULL if statement wasn't recognized
     */
    public static String parseExplainProcCall(String statement)
    {
        Matcher matcher = ExplainProcCallPreamble.matcher(statement);
        if ( ! matcher.lookingAt()) {
            return null;
        }
        // This all could probably be done more elegantly via a group extracted
        // from a more comprehensive regexp.
        // Clean up any extra spaces around the remainder of the line,
        // which should be a proc name.
        return statement.substring(matcher.end()).trim();
    }

    /**
     * Parse EXPLAINVIEW <view>
     * @param statement  statement to parse
     * @return           view name parameter string or NULL if statement wasn't recognized
     */
    public static String parseExplainViewCall(String statement)
    {
        Matcher matcher = ExplainViewCallPreamble.matcher(statement);
        if ( ! matcher.lookingAt()) {
            return null;
        }
        // This all could probably be done more elegantly via a group extracted
        // from a more comprehensive regexp.
        // Clean up any extra spaces around the remainder of the line,
        // which should be a view name.
        return statement.substring(matcher.end()).trim();
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
     * @param statement  input statement
     * @return           jar file path argument, or null if statement is not "LOAD CLASSES"
     * @throws SQLParser.Exception if the LOAD CLASSES argument is not a valid file path.
     */
    public static String parseLoadClasses(String statement) throws SQLParser.Exception
    {
        String arg = loadClassesParser.parse(statement);
        if (arg == null) {
            return null;
        }
        if (! new File(arg).isFile()) {
            throw new SQLParser.Exception("Jar file not found: '" + arg + "'");
        }
        return arg;
    }

    /**
     * @param statement  input statement
     * @return           class selector argument, or null if statement is not "REMOVE CLASSES"
     * @throws SQLParser.Exception if the REMOVE CLASSES argument is not a valid class selector.
     */
    public static String parseRemoveClasses(String statement) throws SQLParser.Exception
    {
        String arg = removeClassesParser.parse(statement);
        if (arg == null) {
            return null;
        }
        // reject obviously bad class selectors
        if (!ClassSelectorToken.matcher(arg).matches()) {
            throw new SQLParser.Exception("Invalid class selector: '" + arg + "'");
        }
        return arg;
    }

    /**
     * @param line
     * @return true if the input contains only a SQL line comment with optional indent.
     */
    public static boolean isWholeLineComment(String line) {
        return OneWholeLineComment.matcher(line).matches();
    }

    /**
     * Make sure that the batch starts with an appropriate DDL verb.  We do not
     * look further than the first token of the first non-comment and non-whitespace line.
     *
     * Empty batches are considered to be trivially valid.
     *
     * @param batch  A SQL string containing multiple statements separated by semicolons
     * @return true if the first keyword of the first statement is a DDL verb
     *     like CREATE, ALTER, DROP, PARTITION, DR, SET or EXPORT,
     *     or if the batch is empty.
     *     See the official list of DDL verbs in the "// Supported verbs" section of
     *     the static initializer for SQLLexer.VERB_TOKENS)
     */
    public static boolean appearsToBeValidDDLBatch(String batch) {

        BufferedReader reader = new BufferedReader(new StringReader(batch));
        String line;
        try {
            while ((line = reader.readLine()) != null) {
                if (isWholeLineComment(line)) {
                    continue;
                }
                line = line.trim();
                if (line.equals("")) {
                    continue;
                }

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

    /**
     * Parse ECHO statement for sqlcmd.
     * The result will be "" if the user just typed ECHO.
     * @param statement  statement to parse
     * @return           Argument text or NULL if statement wasn't recognized
     */
    public static String parseEchoStatement(String statement)
    {
        Matcher matcher = EchoToken.matcher(statement);
        if (matcher.matches()) {
            String commandWordTerminator = matcher.group(1);
            if (OneWhitespace.matcher(commandWordTerminator).matches()) {
                return matcher.group(2);
            }
            return "";
        }
        return null;
    }

    /**
     * Parse ECHOERROR statement for sqlcmd.
     * The result will be "" if the user just typed ECHOERROR.
     * @param statement  statement to parse
     * @return           Argument text or NULL if statement wasn't recognized
     */
    public static String parseEchoErrorStatement(String statement) {
        Matcher matcher = EchoErrorToken.matcher(statement);
        if (matcher.matches()) {
            String commandWordTerminator = matcher.group(1);
            if (OneWhitespace.matcher(commandWordTerminator).matches()) {
                return matcher.group(2);
            }
            return "";
        }
        return null;
    }

    /**
     * Parse DESCRIBE statement for sqlcmd.
     * The result will be "" if the user just typed DESCRIBE or DESC.
     * @param statement  statement to parse
     * @return           String containing possible table name
     */
    public static String parseDescribeStatement(String statement) {
        Matcher matcher = DescribeToken.matcher(statement);
        if (matcher.matches()) {
            String commandWordTerminator = matcher.group(1);
            if (OneWhitespace.matcher(commandWordTerminator).matches()) {
                String trailings = matcher.group(3) + ";" + matcher.group(4);
                // In a valid command, both "trailings" groups should be empty.
                if (trailings.equals(";")) {
                    // Return the subcommand keyword -- possibly a valid one.
                    return matcher.group(2);
                }
                // For an invalid form of the command,
                // return an approximation of the garbage input.
                return matcher.group(2) + " " + trailings;
            }
            if (commandWordTerminator.equals("") || commandWordTerminator.equals(";")) {
                return commandWordTerminator; // EOL or ; reached before subcommand
            }
        }
        return null;
    }

    /**
     * Parse QUERYSTATS statement for sqlcmd.
     * @param statement  statement to parse
     * @return           String containing full SQL query
     */
    public static String parseQueryStatsStatement(String statement) {
        Matcher matcher = QueryStatsToken.matcher(statement);
        if (matcher.matches()) {
            String commandWordTerminator = matcher.group(1);
            if (OneWhitespace.matcher(commandWordTerminator).matches()) {
                String trailings = matcher.group(3) + ";" + matcher.group(4);
                // In a valid command, both "trailings" groups should be empty.
                if (trailings.equals(";")) {
                    // Return the subcommand keyword -- possibly a valid one.
                    return matcher.group(2);
                }
                // For an invalid form of the command,
                // return an approximation of the garbage input.
                return matcher.group(2) + " " + trailings;
            }
            if (commandWordTerminator.equals("") || commandWordTerminator.equals(";")) {
                return commandWordTerminator; // EOL or ; reached before subcommand
            }
        }
        return null;
    }
}
