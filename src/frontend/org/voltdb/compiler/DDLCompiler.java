/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.compiler;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Table;
import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.VoltTypeUtil;


/**
 * Compiles schema (SQL DDL) text files and stores the results in a given catalog.
 *
 */
public class DDLCompiler {

    static final int MAX_COLUMNS = 1024;
    static final int MAX_ROW_SIZE = 1024 * 1024 * 2;

    /**
     * Regex Description:
     * <pre>
     * (?i) -- ignore case
     * \\A -- beginning of statement
     * PARTITION -- token
     * \\s+ one or more spaces
     * (PROCEDURE|TABLE) -- either PROCEDURE or TABLE token
     * \\s+ -- one or more spaces
     * .+ -- one or more of any characters
     * ; -- a semicolon
     * \\z -- end of string
     * </pre>
     */
    static final Pattern prePartitionPattern = Pattern.compile(
            "(?i)\\APARTITION\\s+(PROCEDURE|TABLE)\\s+.+;\\z"
            );
    /**
     * NB supports only unquoted table and column names
     *
     * Regex Description:
     * <pre>
     * (?i) -- ignore case
     * \\A -- beginning of statement
     * PARTITION -- token
     * \\s+ -- one or more spaces
     * TABLE -- token
     * \\s+ -- one or more spaces
     * ([\\w$]+) -- [table name capture group 1]
     *    [\\w$]+ -- 1 or more identifier character
     *        (letters, numbers, dollar sign ($) underscore (_))
     * \\s+ -- one or more spaces
     * ON -- token
     * \\s+ -- one or more spaces
     * COLUMN -- token
     * \\s+ -- one or more spaces
     * ([\\w$]+) -- [column name capture group 2]
     *    [\\w$]+ -- 1 or more identifier character
     *        (letters, numbers, dollar sign ($) or underscore (_))
     * \\s* -- 0 or more spaces
     * ; a -- semicolon
     * \\z -- end of string
     * </pre>
     */
    static final Pattern partitionTablePattern = Pattern.compile(
            "(?i)\\APARTITION\\s+TABLE\\s+([\\w$]+)\\s+ON\\s+COLUMN\\s+([\\w$]+)\\s*;\\z"
            );
    /**
     * NB supports only unquoted table and column names
     *
     * Regex Description:
     * <pre>
     * (?i) -- ignore case
     * \\A -- beginning of statement
     * PARTITION -- token
     * \\s+ -- one or more spaces
     * PROCEDURE -- token
     * \\s+ -- one or more spaces
     * ([\\w$]+) -- [procedure name capture group 1]
     *    [\\w$]+ -- one or more identifier character
     *        (letters, numbers, dollar sign ($) or underscore (_))
     * \\s+ -- one or more spaces
     * ON -- token
     * \\s+ -- one or more spaces
     * TABLE -- token
     * \\s+ -- one or more spaces
     * ([\\w$]+) -- [table name capture group 2]
     *    [\\w$]+ -- one or more identifier character
     *        (letters, numbers, dollar sign ($) or underscore (_))
     * \\s+ -- one or more spaces
     * COLUMN -- token
     * \\s+ -- one or more spaces
     * ([\\w$]+) -- [column name capture group 3]
     *    [\\w$]+ -- one or more identifier character
     *        (letters, numbers, dollar sign ($) or underscore (_))
     * (?:\\s+PARAMETER\\s+(\\d+))? 0 or 1 parameter clause [non-capturing]
     *    \\s+ -- one or more spaces
     *    PARAMETER -- token
     *    \\s+ -- one or more spaces
     *    \\d+ -- one ore more number digits [parameter index capture group 4]
     * \\s* -- 0 or more spaces
     * ; -- a semicolon
     * \\z -- end of string
     * </pre>
     */
    static final Pattern partitionProcedurePattern = Pattern.compile(
            "(?i)\\APARTITION\\s+PROCEDURE\\s+([\\w$]+)\\s+ON\\s+TABLE\\s+" +
            "([\\w$]+)\\s+COLUMN\\s+([\\w$]+)(?:\\s+PARAMETER\\s+(\\d+))?\\s*;\\z"
            );

    /**
     * CREATE PROCEDURE from Java class statement regex
     * NB supports only unquoted table and column names
     * Capture groups are tagged as (1) and (2) in comments below.
     */
    static final Pattern procedureClassPattern = Pattern.compile(
            "(?i)" +                                // ignore case
            "\\A" +                                 // beginning of statement
            "CREATE" +                              // CREATE token
            "\\s+" +                                // one or more spaces
            "PROCEDURE" +                           // PROCEDURE token
            "(?:" +                                 // begin optional ALLOW clause
            "\\s+" +                                //   one or more spaces
            "ALLOW" +                               //   ALLOW token
            "\\s+" +                                //   one or more spaces
            "([\\w.$]+(?:\\s*,\\s*[\\w.$]+)*)" +    //   (1) comma-separated role list
            ")?" +                                  // end optional ALLOW clause
            "\\s+" +                                // one or more spaces
            "FROM" +                                // FROM token
            "\\s+" +                                // one or more spaces
            "CLASS" +                               // CLASS token
            "\\s+" +                                // one or more spaces
            "([\\w$.]+)" +                          // (2) class name
            "\\s*" +                                // zero or more spaces
            ";" +                                   // semi-colon terminator
            "\\z"                                   // end of statement
            );

    /**
     * CREATE PROCEDURE with single SELECT or DML statement regex
     * NB supports only unquoted table and column names
     * Capture groups are tagged as (1) and (2) in comments below.
     */
    static final Pattern procedureSingleStatementPattern = Pattern.compile(
            "(?i)" +                                // ignore case
            "\\A" +                                 // beginning of DDL statement
            "CREATE" +                              // CREATE token
            "\\s+" +                                // one or more spaces
            "PROCEDURE" +                           // PROCEDURE token
            "\\s+" +                                // one or more spaces
            "([\\w.$]+)" +                          // (1) procedure name
            "(?:" +                                 // begin optional ALLOW clause
            "\\s+" +                                //   one or more spaces
            "ALLOW" +                               //   ALLOW token
            "\\s+" +                                //   one or more spaces
            "([\\w.$]+(?:\\s*,\\s*[\\w.$]+)*)" +    //   (2) comma-separated role list
            ")?" +                                  // end optional ALLOW clause
            "\\s+" +                                // one or more spaces
            "AS" +                                  // AS token
            "\\s+" +                                // one or more spaces
            "(" +                                   // (3) begin SELECT or DML statement
            "(?:SELECT|INSERT|UPDATE|DELETE)" +     //   valid DML start tokens (not captured)
            "\\s+" +                                //   one or more spaces
            ".+)" +                                 //   end SELECT or DML statement
            ";" +                                   // semi-colon terminator
            "\\z"                                   // end of DDL statement
            );

    /**
     * Regex to parse the CREATE ROLE statement with optional WITH clause.
     * Leave the WITH clause argument as a single group because regexes
     * aren't capable of producing a variable number of groups.
     * Capture groups are tagged as (1) and (2) in comments below.
     */
    static final Pattern createRolePattern = Pattern.compile(
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
     * Regex to match CREATE TABLE or CREATE VIEW statements.
     * Unlike the other matchers, this is just designed to pull out the
     * name of the table or view, not the whole statement.
     * It's used to preserve as-entered schema for each table/view
     * for the catalog report generator for the moment.
     * Capture group (1) is ignored, but (2) is used.
     */
    static final Pattern createTablePattern = Pattern.compile(
            "(?i)" +                            // (ignore case)
            "\\A" +                             // (start statement)
            "CREATE\\s+(TABLE|VIEW)\\s+" +      // (1) CREATE TABLE
            "([\\w.$]+)"                        // (2) <table name>
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
    static final Pattern replicatePattern = Pattern.compile(
            "(?i)\\AREPLICATE\\s+TABLE\\s+([\\w$]+)\\s*;\\z"
            );

    /**
     * EXPORT TABLE statement regex
     * NB supports only unquoted table names
     * Capture groups are tagged as (1) in comments below.
     */
    static final Pattern exportPattern = Pattern.compile(
            "(?i)" +                            // (ignore case)
            "\\A"  +                            // start statement
            "EXPORT\\s+TABLE\\s+"  +            // EXPORT TABLE
            "([\\w.$]+)" +                      // (1) <table name>
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
     * \\s -- one space
     * </pre>
     */
    static final Pattern voltdbStatementPrefixPattern = Pattern.compile(
            "(?i)((?<=\\ACREATE\\s{0,1024})(?:PROCEDURE|ROLE)|\\APARTITION|\\AREPLICATE|\\AEXPORT)\\s"
            );

    static final String TABLE = "TABLE";
    static final String PROCEDURE = "PROCEDURE";
    static final String PARTITION = "PARTITION";
    static final String REPLICATE = "REPLICATE";
    static final String EXPORT = "EXPORT";
    static final String ROLE = "ROLE";

    enum Permission {
        adhoc,
        sysproc,
        defaultproc,
        export;

        static String toListString() {
            return Arrays.asList(values()).toString();
        }
    }

    HSQLInterface m_hsql;
    VoltCompiler m_compiler;
    String m_fullDDL = "";
    int m_currLineNo = 1;

    /// Partition descriptors parsed from DDL PARTITION or REPLICATE statements.
    final VoltDDLElementTracker m_tracker;

    HashMap<String, Column> columnMap = new HashMap<String, Column>();
    HashMap<String, Index> indexMap = new HashMap<String, Index>();
    HashMap<Table, String> matViewMap = new HashMap<Table, String>();

    // Track the original CREATE TABLE statement for each table
    // Currently used for catalog report generation.
    // There's specifically no cleanup here because I don't think
    // any is needed.
    Map<String, String> m_tableNameToDDL = new TreeMap<String, String>();

    private class DDLStatement {
        public DDLStatement() {
        }
        String statement = "";
        int lineNo;
    }

    public DDLCompiler(VoltCompiler compiler, HSQLInterface hsql, VoltDDLElementTracker tracker) {
        assert(compiler != null);
        assert(hsql != null);
        assert(tracker != null);
        this.m_hsql = hsql;
        this.m_compiler = compiler;
        this.m_tracker = tracker;
    }

    /**
     * Compile a DDL schema from a file on disk
     * @param path
     * @param db
     * @throws VoltCompiler.VoltCompilerException
     */
    public void loadSchema(String path, Database db)
            throws VoltCompiler.VoltCompilerException {
        File inputFile = new File(path);
        FileReader reader = null;
        try {
            reader = new FileReader(inputFile);
        } catch (FileNotFoundException e) {
            throw m_compiler.new VoltCompilerException("Unable to open schema file for reading");
        }

        m_currLineNo = 1;
        loadSchema(path, db, reader);
    }

    /**
     * Compile a file from an open input stream
     * @param path
     * @param db
     * @param reader
     * @throws VoltCompiler.VoltCompilerException
     */
    private void loadSchema(String path, Database db, FileReader reader)
            throws VoltCompiler.VoltCompilerException {

        DDLStatement stmt = getNextStatement(reader, m_compiler);
        while (stmt != null) {
            // Some statements are processed by VoltDB and the rest are handled by HSQL.
            boolean processed = false;
            try {
                processed = processVoltDBStatement(stmt.statement, db);
            } catch (VoltCompilerException e) {
                // Reformat the message thrown by VoltDB DDL processing to have a line number.
                String msg = "VoltDB DDL Error: \"" + e.getMessage() + "\" in statement starting on lineno: " + stmt.lineNo;
                throw m_compiler.new VoltCompilerException(msg);
            }
            if (!processed) {
                try {
                    // Check for CREATE TABLE or CREATE VIEW.
                    // We sometimes choke at parsing statements with newlines, so
                    // check against a newline free version of the stmt.
                    String oneLinerStmt = stmt.statement.replace("\n", " ");
                    Matcher tableMatcher = createTablePattern.matcher(oneLinerStmt);
                    if (tableMatcher.find()) {
                        String tableName = tableMatcher.group(2);
                        m_tableNameToDDL.put(tableName.toUpperCase(), stmt.statement);
                    }

                    // kind of ugly.  We hex-encode each statement so we can
                    // avoid embedded newlines so we can delimit statements
                    // with newline.
                    m_fullDDL += Encoder.hexEncode(stmt.statement) + "\n";
                    m_hsql.runDDLCommand(stmt.statement);
                } catch (HSQLParseException e) {
                    String msg = "DDL Error: \"" + e.getMessage() + "\" in statement starting on lineno: " + stmt.lineNo;
                    throw m_compiler.new VoltCompilerException(msg, stmt.lineNo);
                }
            }
            stmt = getNextStatement(reader, m_compiler);
        }

        try {
            reader.close();
        } catch (IOException e) {
            throw m_compiler.new VoltCompilerException("Error closing schema file");
        }
    }

    /**
     * Checks whether or not the start of the given identifier is java (and
     * thus DDL) compliant. An identifier may start with: _ [a-zA-Z] $
     * @param identifier the identifier to check
     * @param statement the statement where the identifier is
     * @return the given identifier unmodified
     * @throws VoltCompilerException when it is not compliant
     */
    private String checkIdentifierStart(
            final String identifier, final String statement
            ) throws VoltCompilerException {

        assert identifier != null && ! identifier.trim().isEmpty();
        assert statement != null && ! statement.trim().isEmpty();

        int loc = 0;
        do {
            if( ! Character.isJavaIdentifierStart(identifier.charAt(loc))) {
                String msg = "Unknown indentifier in DDL: \"" +
                        statement.substring(0,statement.length()-1) +
                        "\" contains invalid identifier \"" + identifier + "\"";
                throw m_compiler.new VoltCompilerException(msg);
            }
            loc = identifier.indexOf('.', loc) + 1;
        }
        while( loc > 0 && loc < identifier.length());

        return identifier;
    }

    /**
     * Process a VoltDB-specific DDL statement, like PARTITION, REPLICATE,
     * CREATE PROCEDURE, and CREATE ROLE.
     * @param statement  DDL statement string
     * @param db
     * @return true if statement was handled, otherwise it should be passed to HSQL
     * @throws VoltCompilerException
     */
    private boolean processVoltDBStatement(String statement, Database db) throws VoltCompilerException {
        if (statement == null || statement.trim().isEmpty()) {
            return false;
        }

        statement = statement.trim();

        // matches if it is the beginning of a voltDB statement
        Matcher statementMatcher = voltdbStatementPrefixPattern.matcher(statement);
        if( ! statementMatcher.find()) {
            return false;
        }

        // either PROCEDURE, REPLICATE, PARTITION, ROLE, or EXPORT
        String commandPrefix = statementMatcher.group(1).toUpperCase();

        // matches if it is CREATE PROCEDURE [ALLOW <role> ...] FROM CLASS <class-name>;
        statementMatcher = procedureClassPattern.matcher(statement);
        if( statementMatcher.matches()) {
            String clazz = checkIdentifierStart(statementMatcher.group(2), statement);

            ProcedureDescriptor descriptor = m_compiler.new ProcedureDescriptor(
                    new ArrayList<String>(), clazz);

            // Add roles if specified.
            if (statementMatcher.group(1) != null) {
                for (String roleName : StringUtils.split(statementMatcher.group(1), ',')) {
                    // Don't put the same role in the list more than once.
                    String roleNameFixed = roleName.trim().toLowerCase();
                    if (!descriptor.m_authGroups.contains(roleNameFixed)) {
                        descriptor.m_authGroups.add(roleNameFixed);
                    }
                }
            }

            // track the defined procedure
            m_tracker.add(descriptor);

            return true;
        }

        // matches if it is CREATE PROCEDURE <proc-name> [ALLOW <role> ...] AS <select-or-dml-statement>
        statementMatcher = procedureSingleStatementPattern.matcher(statement);
        if( statementMatcher.matches()) {
            String clazz = checkIdentifierStart(statementMatcher.group(1), statement);
            String sqlStatement = statementMatcher.group(3);

            ProcedureDescriptor descriptor = m_compiler.new ProcedureDescriptor(
                    new ArrayList<String>(), clazz, sqlStatement, null, null, false);

            // Add roles if specified.
            if (statementMatcher.group(2) != null) {
                for (String roleName : StringUtils.split(statementMatcher.group(2), ',')) {
                    descriptor.m_authGroups.add(roleName.trim().toLowerCase());
                }
            }

            m_tracker.add(descriptor);

            return true;
        }

        // matches if it is the beginning of a partition statement
        statementMatcher = prePartitionPattern.matcher(statement);
        if( statementMatcher.matches()) {

            // either TABLE or PROCEDURE
            String partitionee = statementMatcher.group(1).toUpperCase();
            if( TABLE.equals(partitionee)) {

                // matches if it is PARTITION TABLE <table> ON COLUMN <column>
                statementMatcher = partitionTablePattern.matcher(statement);

                if( ! statementMatcher.matches()) {
                    throw m_compiler.new VoltCompilerException(String.format(
                            "Invalid PARTITION statement: \"%s\", " +
                            "expected syntax: PARTITION TABLE <table> ON COLUMN <column>",
                            statement.substring(0,statement.length()-1))); // remove trailing semicolon
                }
                // group(1) -> table, group(2) -> column
                m_tracker.put(
                        checkIdentifierStart(statementMatcher.group(1),statement),
                        checkIdentifierStart(statementMatcher.group(2),statement)
                        );
                return true;
            }
            else if( PROCEDURE.equals(partitionee)) {

                // matches if it is
                //   PARTITION PROCEDURE <procedure>
                //      ON  TABLE <table> COLUMN <column> [PARAMETER <parameter-index-no>]
                statementMatcher = partitionProcedurePattern.matcher(statement);

                if( ! statementMatcher.matches()) {
                    throw m_compiler.new VoltCompilerException(String.format(
                            "Invalid PARTITION statement: \"%s\", " +
                            "expected syntax: PARTITION PROCEDURE <procedure> ON "+
                            "TABLE <table> COLUMN <column> [PARAMETER <parameter-index-no>]",
                            statement.substring(0,statement.length()-1))); // remove trailing semicolon
                }

                // check the table portion of the partition info
                String tableName = checkIdentifierStart(statementMatcher.group(2), statement);

                // check the column portion of the partition info
                String columnName = checkIdentifierStart(statementMatcher.group(3), statement);

                // if not specified default parameter index to 0
                String parameterNo = statementMatcher.group(4);
                if( parameterNo == null) {
                    parameterNo = "0";
                }

                String partitionInfo = String.format("%s.%s: %s", tableName, columnName, parameterNo);

                // procedureName -> group(1), partitionInfo -> group(2)
                m_tracker.addProcedurePartitionInfoTo(
                        checkIdentifierStart(statementMatcher.group(1), statement),
                        partitionInfo
                        );

                return true;
            }
            // can't get here as regex only matches for PROCEDURE or TABLE
        }

        // matches if it is REPLICATE TABLE <table-name>
        statementMatcher = replicatePattern.matcher(statement);
        if( statementMatcher.matches()) {
            // group(1) -> table
            m_tracker.put(
                    checkIdentifierStart(statementMatcher.group(1), statement),
                    null
                    );
            return true;
        }

        // matches if it is CREATE ROLE [WITH <permission> [, <permission> ...]]
        // group 1 is role name
        // group 2 is comma-separated permission list or null if there is no WITH clause
        statementMatcher = createRolePattern.matcher(statement);
        if( statementMatcher.matches()) {
            String roleName = statementMatcher.group(1);
            CatalogMap<Group> groupMap = db.getGroups();
            if (groupMap.get(roleName) != null) {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Role name \"%s\" in CREATE ROLE statement already exists.",
                        roleName));
            }
            org.voltdb.catalog.Group catGroup = groupMap.add(roleName);
            if (statementMatcher.group(2) != null) {
                for (String tokenRaw : StringUtils.split(statementMatcher.group(2), ',')) {
                    String token = tokenRaw.trim().toLowerCase();
                    Permission permission;
                    try {
                        permission = Permission.valueOf(token);
                    }
                    catch (IllegalArgumentException iaex) {
                        throw m_compiler.new VoltCompilerException(String.format(
                                "Invalid permission \"%s\" in CREATE ROLE statement: \"%s\", " +
                                "available permissions: %s", token,
                                statement.substring(0,statement.length()-1), // remove trailing semicolon
                                Permission.toListString()));
                    }
                    switch( permission) {
                    case adhoc:
                        catGroup.setAdhoc(true);
                        break;
                    case sysproc:
                        catGroup.setSysproc(true);
                        break;
                    case defaultproc:
                        catGroup.setDefaultproc(true);
                        break;
                    case export:
                        m_compiler.grantExportToGroup(roleName, db);
                        break;
                    }
                }
            }
            return true;
        }

        statementMatcher = exportPattern.matcher(statement);
        if( statementMatcher.matches()) {

            // check the table portion
            String tableName = checkIdentifierStart(statementMatcher.group(1), statement);
            m_tracker.addExportedTable(tableName);

            return true;
        }

        /*
         * if no correct syntax regex matched above then at this juncture
         * the statement is syntax incorrect
         */

        if( PARTITION.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid PARTITION statement: \"%s\", " +
                    "expected syntax: \"PARTITION TABLE <table> ON COLUMN <column>\" or " +
                    "\"PARTITION PROCEDURE <procedure> ON " +
                    "TABLE <table> COLUMN <column> [PARAMETER <parameter-index-no>]\"",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if( REPLICATE.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid REPLICATE statement: \"%s\", " +
                    "expected syntax: REPLICATE TABLE <table>",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if( PROCEDURE.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE PROCEDURE statement: \"%s\", " +
                    "expected syntax: \"CREATE PROCEDURE [ALLOW <role> [, <role> ...] FROM CLASS <class-name>\" " +
                    "or: \"CREATE PROCEDURE <name> [ALLOW <role> [, <role> ...] AS <single-select-or-dml-statement>\"",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if( ROLE.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE ROLE statement: \"%s\", " +
                    "expected syntax: CREATE ROLE <role>",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if( EXPORT.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid EXPORT TABLE statement: \"%s\", " +
                    "expected syntax: EXPORT TABLE <table>",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        // Not a VoltDB-specific DDL statement.
        return false;
    }

    public void compileToCatalog(Database db) throws VoltCompilerException {
        String hexDDL = Encoder.hexEncode(m_fullDDL);
        db.getCatalog().execute("set " + db.getPath() + " schema \"" + hexDDL + "\"");

        VoltXMLElement xmlCatalog;
        try
        {
            xmlCatalog = m_hsql.getXMLFromCatalog();
        }
        catch (HSQLParseException e)
        {
            String msg = "DDL Error: " + e.getMessage();
            throw m_compiler.new VoltCompilerException(msg);
        }

        // output the xml catalog to disk
        BuildDirectoryUtils.writeFile("schema-xml", "hsql-catalog-output.xml", xmlCatalog.toString(), true);

        // build the local catalog from the xml catalog
        fillCatalogFromXML(db, xmlCatalog);
    }

    /**
     * Read until the next newline
     * @throws IOException
     */
    String readToEndOfLine(FileReader reader) throws IOException {
        LineNumberReader lnr = new LineNumberReader(reader);
        String retval = lnr.readLine();
        m_currLineNo++;
        return retval;
    }



    // Parsing states. Start in kStateInvalid
    private static int kStateInvalid = 0;                         // have not yet found start of statement
    private static int kStateReading = 1;                         // normal reading state
    private static int kStateReadingCommentDelim = 2;             // dealing with first -
    private static int kStateReadingComment = 3;                  // parsing after -- for a newline
    private static int kStateReadingStringLiteralSpecialChar = 4; // dealing with one or more single quotes
    private static int kStateReadingStringLiteral = 5;            // in the middle of a string literal
    private static int kStateCompleteStatement = 6;               // found end of statement

    private int readingState(char[] nchar, DDLStatement retval) {
        if (nchar[0] == '-') {
            // remember that a possible '--' is being examined
            return kStateReadingCommentDelim;
        }
        else if (nchar[0] == '\n') {
            // normalize newlines to spaces
            m_currLineNo += 1;
            retval.statement += " ";
        }
        else if (nchar[0] == '\r') {
            // ignore carriage returns
        }
        else if (nchar[0] == ';') {
            // end of the statement
            retval.statement += nchar[0];
            return kStateCompleteStatement;
        }
        else if (nchar[0] == '\'') {
            retval.statement += nchar[0];
            return kStateReadingStringLiteral;
        }
        else {
            // accumulate and continue
            retval.statement += nchar[0];
        }

        return kStateReading;
    }

    private int readingStringLiteralState(char[] nchar, DDLStatement retval) {
        // all characters in the literal are accumulated. keep track of
        // newlines for error messages.
        retval.statement += nchar[0];
        if (nchar[0] == '\n') {
            m_currLineNo += 1;
        }

        // if we see a SINGLE_QUOTE, change states to check for terminating literal
        if (nchar[0] != '\'') {
            return kStateReadingStringLiteral;
        }
        else {
            return kStateReadingStringLiteralSpecialChar;
        }
    }


    private int readingStringLiteralSpecialChar(char[] nchar, DDLStatement retval) {

        // if this is an escaped quote, return kReadingStringLiteral.
        // otherwise, the string is complete. Parse nchar as a non-literal
        if (nchar[0] == '\'') {
            retval.statement += nchar[0];
            return kStateReadingStringLiteral;
        }
        else {
            return readingState(nchar, retval);
        }
    }

    private int readingCommentDelimState(char[] nchar, DDLStatement retval) {
        if (nchar[0] == '-') {
            // confirmed that a comment is being read
            return kStateReadingComment;
        }
        else {
            // need to append the previously skipped '-' to the statement
            // and process the current character
            retval.statement += '-';
            return readingState(nchar, retval);
        }
    }

    private int readingCommentState(char[] nchar, DDLStatement retval) {
        if (nchar[0] == '\n') {
            // a comment is continued until a newline is found.
            m_currLineNo += 1;
            return kStateReading;
        }
        return kStateReadingComment;
    }

    DDLStatement getNextStatement(FileReader reader, VoltCompiler compiler)
            throws VoltCompiler.VoltCompilerException {

        int state = kStateInvalid;

        char[] nchar = new char[1];
        @SuppressWarnings("synthetic-access")
        DDLStatement retval = new DDLStatement();
        retval.lineNo = m_currLineNo;

        try {

            // find the start of a statement and break out of the loop
            // or return null if there is no next statement to be found
            do {
                if (reader.read(nchar) == -1) {
                    return null;
                }

                // trim leading whitespace outside of a statement
                if (nchar[0] == '\n') {
                    m_currLineNo++;
                }
                else if (nchar[0] == '\r') {
                }
                else if (nchar[0] == ' ') {
                }

                // trim leading comments outside of a statement
                else if (nchar[0] == '-') {
                    // The next character must be a comment because no valid
                    // statement will start with "-<foo>". If a comment was
                    // found, read until the next newline.
                    if (reader.read(nchar) == -1) {
                        // garbage at the end of a file but easy to tolerable?
                        return null;
                    }
                    if (nchar[0] != '-') {
                        String msg = "Invalid content before or between DDL statements.";
                        throw compiler.new VoltCompilerException(msg, m_currLineNo);
                    }
                    else {
                        do {
                            if (reader.read(nchar) == -1) {
                                // a comment extending to EOF means no statement
                                return null;
                            }
                        } while (nchar[0] != '\n');

                        // process the newline and loop
                        m_currLineNo++;
                    }
                }

                // not whitespace or comment: start of a statement.
                else {
                    retval.statement += nchar[0];
                    state = kStateReading;
                    // Set the line number to the start of the real statement.
                    retval.lineNo = m_currLineNo;
                    break;
                }
            } while (true);

            while (state != kStateCompleteStatement) {
                if (reader.read(nchar) == -1) {
                    String msg = "Schema file ended mid-statement (no semicolon found).";
                    throw compiler.new VoltCompilerException(msg, retval.lineNo);
                }

                if (state == kStateReading) {
                    state = readingState(nchar, retval);
                }
                else if (state == kStateReadingCommentDelim) {
                    state = readingCommentDelimState(nchar, retval);
                }
                else if (state == kStateReadingComment) {
                    state = readingCommentState(nchar, retval);
                }
                else if (state == kStateReadingStringLiteral) {
                    state = readingStringLiteralState(nchar, retval);
                }
                else if (state == kStateReadingStringLiteralSpecialChar) {
                    state = readingStringLiteralSpecialChar(nchar, retval);
                }
                else {
                    throw compiler.new VoltCompilerException("Unrecoverable error parsing DDL.");
                }
            }

            return retval;
        }
        catch (IOException e) {
            throw compiler.new VoltCompilerException("Unable to read from file");
        }
    }

    public void fillCatalogFromXML(Database db, VoltXMLElement xml)
    throws VoltCompiler.VoltCompilerException {

        if (xml == null)
            throw m_compiler.new VoltCompilerException("Unable to parse catalog xml file from hsqldb");

        assert xml.name.equals("databaseschema");

        for (VoltXMLElement node : xml.children) {
            if (node.name.equals("table"))
                addTableToCatalog(db, node);
        }

        processMaterializedViews(db);
    }

    void addTableToCatalog(Database db, VoltXMLElement node) throws VoltCompilerException {
        assert node.name.equals("table");

        // clear these maps, as they're table specific
        columnMap.clear();
        indexMap.clear();

        String name = node.attributes.get("name");

        // create a table node in the catalog
        Table table = db.getTables().add(name);

        // add the original DDL to the table (or null if it's not there)
        TableAnnotation annotation = new TableAnnotation();
        table.setAnnotation(annotation);
        annotation.ddl = m_tableNameToDDL.get(name.toUpperCase());

        // handle the case where this is a materialized view
        String query = node.attributes.get("query");
        if (query != null) {
            assert(query.length() > 0);
            matViewMap.put(table, query);
        }

        // all tables start replicated
        // if a partition is found in the project file later,
        //  then this is reversed
        table.setIsreplicated(true);

        // map of index replacements for later constraint fixup
        Map<String, String> indexReplacementMap = new TreeMap<String, String>();

        ArrayList<VoltType> columnTypes = new ArrayList<VoltType>();
        for (VoltXMLElement subNode : node.children) {

            if (subNode.name.equals("columns")) {
                int colIndex = 0;
                for (VoltXMLElement columnNode : subNode.children) {
                    if (columnNode.name.equals("column"))
                        addColumnToCatalog(table, columnNode, colIndex++, columnTypes);
                }
                // limit the total number of columns in a table
                if (colIndex > MAX_COLUMNS) {
                    String msg = "Table " + name + " has " +
                        colIndex + " columns (max is " + MAX_COLUMNS + ")";
                    throw m_compiler.new VoltCompilerException(msg);
                }
            }

            if (subNode.name.equals("indexes")) {
                // do non-system indexes first so they get priority when the compiler
                // starts throwing out duplicate indexes
                for (VoltXMLElement indexNode : subNode.children) {
                    if (indexNode.name.equals("index") == false) continue;
                    String indexName = indexNode.attributes.get("name");
                    if (indexName.startsWith("SYS_IDX_SYS_") == false) {
                        addIndexToCatalog(db, table, indexNode, indexReplacementMap);
                    }
                }

                // now do system indexes
                for (VoltXMLElement indexNode : subNode.children) {
                    if (indexNode.name.equals("index") == false) continue;
                    String indexName = indexNode.attributes.get("name");
                    if (indexName.startsWith("SYS_IDX_SYS_") == true) {
                        addIndexToCatalog(db, table, indexNode, indexReplacementMap);
                    }
                }
            }

            if (subNode.name.equals("constraints")) {
                for (VoltXMLElement constraintNode : subNode.children) {
                    if (constraintNode.name.equals("constraint"))
                        addConstraintToCatalog(table, constraintNode, indexReplacementMap);
                }
            }
        }

        table.setSignature(VoltTypeUtil.getSignatureForTable(name, columnTypes));

        /*
         * Validate that the total size
         */
        int maxRowSize = 0;
        for (Column c : columnMap.values()) {
            VoltType t = VoltType.get((byte)c.getType());
            if ((t == VoltType.STRING) || (t == VoltType.VARBINARY)) {
                if (c.getSize() > VoltType.MAX_VALUE_LENGTH) {
                    throw m_compiler.new VoltCompilerException("Table name " + name + " column " + c.getName() +
                            " has a maximum size of " + c.getSize() + " bytes" +
                            " but the maximum supported size is " + VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH));
                }
                maxRowSize += 4 + c.getSize();
            } else {
                maxRowSize += t.getLengthInBytesForFixedTypes();
            }
        }
        if (maxRowSize > MAX_ROW_SIZE) {
            throw m_compiler.new VoltCompilerException("Table name " + name + " has a maximum row size of " + maxRowSize +
                    " but the maximum supported row size is " + MAX_ROW_SIZE);
        }
    }

    void addColumnToCatalog(Table table, VoltXMLElement node, int index, ArrayList<VoltType> columnTypes) throws VoltCompilerException {
        assert node.name.equals("column");

        String name = node.attributes.get("name");
        String typename = node.attributes.get("valuetype");
        String nullable = node.attributes.get("nullable");
        String sizeString = node.attributes.get("size");
        String defaultvalue = null;
        String defaulttype = null;

        // throws an exception if string isn't an int (i think)
        Integer.parseInt(sizeString);

        // Default Value
        for (VoltXMLElement child : node.children) {
            if (child.name.equals("default")) {
                for (VoltXMLElement inner_child : child.children) {
                    // Value
                    if (inner_child.name.equals("value")) {
                        assert(defaulttype == null); // There should be only one default value/type.
                        defaultvalue = inner_child.attributes.get("value");
                        defaulttype = inner_child.attributes.get("valuetype");
                        assert(defaulttype != null);
                    }
                }
            }
        }
        if (defaultvalue != null && defaultvalue.equals("NULL"))
            defaultvalue = null;
        if (defaulttype != null) {
            // fyi: Historically, VoltType class initialization errors get reported on this line (?).
            if (defaultvalue == null) {
                defaulttype = "NULL";
            }
            defaulttype = Integer.toString(VoltType.typeFromString(defaulttype).getValue());
        }

        // replace newlines in default values
        if (defaultvalue != null) {
            defaultvalue = defaultvalue.replace('\n', ' ');
            defaultvalue = defaultvalue.replace('\r', ' ');
        }

        // fyi: Historically, VoltType class initialization errors get reported on this line (?).
        VoltType type = VoltType.typeFromString(typename);
        columnTypes.add(type);
        if (defaultvalue != null && (type == VoltType.DECIMAL || type == VoltType.NUMERIC))
        {
            // Until we support deserializing scientific notation in the EE, we'll
            // coerce default values to plain notation here.  See ENG-952 for more info.
            BigDecimal temp = new BigDecimal(defaultvalue);
            defaultvalue = temp.toPlainString();
        }

        Column column = table.getColumns().add(name);
        // need to set other column data here (default, nullable, etc)
        column.setName(name);
        column.setIndex(index);

        column.setType(type.getValue());
        column.setNullable(nullable.toLowerCase().startsWith("t") ? true : false);
        int size = type.getMaxLengthInBytes();
        // Require a valid length if variable length is supported for a type
        if (type == VoltType.STRING || type == VoltType.VARBINARY) {
            size = Integer.parseInt(sizeString);
            if ((size == 0) || (size > VoltType.MAX_VALUE_LENGTH)) {
                String msg = type.toSQLString() + " column " + name + " in table " + table.getTypeName() + " has unsupported length " + sizeString;
                throw m_compiler.new VoltCompilerException(msg);
            }
        }
        column.setSize(size);

        column.setDefaultvalue(defaultvalue);
        if (defaulttype != null)
            column.setDefaulttype(Integer.parseInt(defaulttype));

        columnMap.put(name, column);
    }

    /**
     * Return true if the two indexes are identical with a different name.
     */
    boolean indexesAreDups(Index idx1, Index idx2) {
        // same attributes?
        if (idx1.getType() != idx2.getType()) {
            return false;
        }
        if (idx1.getCountable() != idx2.getCountable()) {
            return false;
        }
        if (idx1.getUnique() != idx2.getUnique()) {
            return false;
        }

        // same column count?
        if (idx1.getColumns().size() != idx2.getColumns().size()) {
            return false;
        }

        //TODO: For index types like HASH that support only random access vs. scanned ranges, indexes on different
        // permutations of the same list of columns/expressions could be considered dupes. This code skips that edge
        // case optimization in favor of using a simpler more exact permutation-sensitive algorithm for all indexes.

        if ( ! (idx1.getExpressionsjson().equals(idx2.getExpressionsjson()))) {
            return false;
        }

        // Simple column indexes have identical empty expression strings so need to be distinguished other ways.
        // More complex expression indexes that have the same expression strings always have the same set of (base)
        // columns referenced in the same order, but we fall through and check them, anyway.

        // sort in index order the columns of idx1, each identified by its index in the base table
        int[] idx1baseTableOrder = new int[idx1.getColumns().size()];
        for (ColumnRef cref : idx1.getColumns()) {
            int index = cref.getIndex();
            int baseTableIndex = cref.getColumn().getIndex();
            idx1baseTableOrder[index] = baseTableIndex;
        }

        // sort in index order the columns of idx2, each identified by its index in the base table
        int[] idx2baseTableOrder = new int[idx2.getColumns().size()];
        for (ColumnRef cref : idx2.getColumns()) {
            int index = cref.getIndex();
            int baseTableIndex = cref.getColumn().getIndex();
            idx2baseTableOrder[index] = baseTableIndex;
        }

        // Duplicate indexes have identical columns in identical order.
        return Arrays.equals(idx1baseTableOrder, idx2baseTableOrder);
    }

    void addIndexToCatalog(Database db, Table table, VoltXMLElement node, Map<String, String> indexReplacementMap)
            throws VoltCompilerException
    {
        assert node.name.equals("index");

        String name = node.attributes.get("name");
        boolean unique = Boolean.parseBoolean(node.attributes.get("unique"));
        AbstractParsedStmt dummy = new ParsedSelectStmt(null, db);
        dummy.setTable(table);

        // "parse" the expression trees for an expression-based index (vs. a simple column value index)
        AbstractExpression[] exprs = null;
        for (VoltXMLElement subNode : node.children) {
            if (subNode.name.equals("exprs")) {
                exprs = new AbstractExpression[subNode.children.size()];
                int j = 0;
                for (VoltXMLElement exprNode : subNode.children) {
                    exprs[j] = dummy.parseExpressionTree(exprNode);
                    exprs[j].resolveForTable(table);
                    exprs[j].finalizeValueTypes();
                    ++j;
                }
            }
        }

        String colList = node.attributes.get("columns");
        String[] colNames = colList.split(",");
        Column[] columns = new Column[colNames.length];
        boolean has_nonint_col = false;
        String nonint_col_name = null;

        for (int i = 0; i < colNames.length; i++) {
            columns[i] = columnMap.get(colNames[i]);
            if (columns[i] == null) {
                return;
            }
        }

        if (exprs == null) {
            for (int i = 0; i < colNames.length; i++) {
                VoltType colType = VoltType.get((byte)columns[i].getType());
                if (colType == VoltType.DECIMAL || colType == VoltType.FLOAT || colType == VoltType.STRING) {
                    has_nonint_col = true;
                    nonint_col_name = colNames[i];
                }
                // disallow columns from VARBINARYs
                if (colType == VoltType.VARBINARY) {
                    String msg = "VARBINARY values are not currently supported as index keys: '" + colNames[i] + "'";
                    throw this.m_compiler.new VoltCompilerException(msg);
                }
            }
        } else {
            for (AbstractExpression expression : exprs) {
                VoltType colType = expression.getValueType();
                if (colType == VoltType.DECIMAL || colType == VoltType.FLOAT || colType == VoltType.STRING) {
                    has_nonint_col = true;
                    nonint_col_name = "<expression>";
                }
                // disallow expressions of type VARBINARY
                if (colType == VoltType.VARBINARY) {
                    String msg = "VARBINARY expressions are not currently supported as index keys.";
                    throw this.m_compiler.new VoltCompilerException(msg);
                }
            }
        }

        Index index = table.getIndexes().add(name);
        index.setCountable(false);

        // set the type of the index based on the index name and column types
        // Currently, only int types can use hash or array indexes
        String indexNameNoCase = name.toLowerCase();
        if (indexNameNoCase.contains("tree"))
        {
            index.setType(IndexType.BALANCED_TREE.getValue());
            index.setCountable(true);
        }
        else if (indexNameNoCase.contains("hash"))
        {
            if (!has_nonint_col)
            {
                index.setType(IndexType.HASH_TABLE.getValue());
            }
            else
            {
                String msg = "Index " + name + " in table " + table.getTypeName() +
                             " uses a non-hashable column " + nonint_col_name;
                throw m_compiler.new VoltCompilerException(msg);
            }
        } else {
            index.setType(IndexType.BALANCED_TREE.getValue());
            index.setCountable(true);
        }

        // Countable is always on right now. Fix it when VoltDB can pack memory for TreeNode.
//        if (indexNameNoCase.contains("NoCounter")) {
//            index.setType(IndexType.BALANCED_TREE.getValue());
//            index.setCountable(false);
//        }

        // need to set other index data here (column, etc)
        // For expression indexes, the columns listed in the catalog do not correspond to the values in the index,
        // but they still represent the columns that will trigger an index update when their values change.
        for (int i = 0; i < columns.length; i++) {
            ColumnRef cref = index.getColumns().add(columns[i].getTypeName());
            cref.setColumn(columns[i]);
            cref.setIndex(i);
        }

        if (exprs != null) {
            try {
                index.setExpressionsjson(convertToJSONArray(exprs));
            } catch (JSONException e) {
                throw m_compiler.new VoltCompilerException("Unexpected error serializing non-column expressions for index '" +
                                                           name + "' on type '" + table.getTypeName() + "': " + e.toString());
            }
        }

        index.setUnique(unique);

        // check if an existing index duplicates another index (if so, drop it)
        // note that this is an exact dup... uniqueness, counting-ness and type
        // will make two indexes different
        for (Index existingIndex : table.getIndexes()) {
            // skip thineself
            if (existingIndex == index) {
                 continue;
            }

            if (indexesAreDups(existingIndex, index)) {
                // replace any constraints using one index with the other
                //for () TODO
                // get ready for replacements from constraints created later
                indexReplacementMap.put(index.getTypeName(), existingIndex.getTypeName());

                // if the index is a user-named index...
                if (index.getTypeName().startsWith("SYS_IDX_") == false) {
                    // on dup-detection, add a warning but don't fail
                    String msg = String.format("Dropping index %s on table %s because it duplicates index %s.",
                            index.getTypeName(), table.getTypeName(), existingIndex.getTypeName());
                    m_compiler.addWarn(msg);
                }

                // drop the index and GTFO
                table.getIndexes().delete(index.getTypeName());
                return;
            }
        }

        String msg = "Created index: " + name + " on table: " +
                    table.getTypeName() + " of type: " + IndexType.get(index.getType()).name();

        m_compiler.addInfo(msg);

        indexMap.put(name, index);
    }

    private static String convertToJSONArray(AbstractExpression[] exprs) throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.array();
        for (AbstractExpression abstractExpression : exprs) {
            stringer.object();
            abstractExpression.toJSONString(stringer);
            stringer.endObject();
        }
        stringer.endArray();
        return stringer.toString();
    }

    /**
     * Add a constraint on a given table to the catalog
     * @param table
     * @param node
     * @throws VoltCompilerException
     */
    void addConstraintToCatalog(Table table, VoltXMLElement node, Map<String, String> indexReplacementMap)
            throws VoltCompilerException
    {
        assert node.name.equals("constraint");

        String name = node.attributes.get("name");
        String typeName = node.attributes.get("constrainttype");
        ConstraintType type = ConstraintType.valueOf(typeName);
        if (type == ConstraintType.CHECK) {
            String msg = "VoltDB does not enforce check constraints. ";
            msg += "Constraint on table " + table.getTypeName() + " will be ignored.";
            m_compiler.addWarn(msg);
            return;
        }
        else if (type == ConstraintType.FOREIGN_KEY) {
            String msg = "VoltDB does not enforce foreign key references and constraints. ";
            msg += "Constraint on table " + table.getTypeName() + " will be ignored.";
            m_compiler.addWarn(msg);
            return;
        }
        else if (type == ConstraintType.MAIN) {
            // should never see these
            assert(false);
        }
        else if (type == ConstraintType.NOT_NULL) {
            // these get handled by table metadata inspection
            return;
        }
        else if (type != ConstraintType.PRIMARY_KEY &&  type != ConstraintType.UNIQUE) {
            throw m_compiler.new VoltCompilerException("Invalid constraint type '" + typeName + "'");
        }

        // else, create the unique index below
        // primary key code is in other places as well

        // The constraint is backed by an index, therefore we need to create it
        // TODO: We need to be able to use indexes for foreign keys. I am purposely
        //       leaving those out right now because HSQLDB just makes too many of them.
        Constraint catalog_const = table.getConstraints().add(name);
        String indexName = node.attributes.get("index");
        assert(indexName != null);
        // handle replacements from duplicate index pruning
        if (indexReplacementMap.containsKey(indexName)) {
            indexName = indexReplacementMap.get(indexName);
        }

        Index catalog_index = indexMap.get(indexName);

        if (catalog_index != null) {
            // if the constraint name contains index type hints, exercise them (giant hack)
            String constraintNameNoCase = name.toLowerCase();
            if (constraintNameNoCase.contains("tree"))
                catalog_index.setType(IndexType.BALANCED_TREE.getValue());
            if (constraintNameNoCase.contains("hash"))
                catalog_index.setType(IndexType.HASH_TABLE.getValue());

            catalog_const.setIndex(catalog_index);
            catalog_index.setUnique(true);
        }
        catalog_const.setType(type.getValue());
    }

    /**
     * Add materialized view info to the catalog for the tables that are
     * materialized views.
     */
    void processMaterializedViews(Database db) throws VoltCompiler.VoltCompilerException {
        for (Entry<Table, String> entry : matViewMap.entrySet()) {
            Table destTable = entry.getKey();
            String query = entry.getValue();

            // get the xml for the query
            VoltXMLElement xmlquery = null;
            try {
                xmlquery = m_hsql.getXMLCompiledStatement(query);
            }
            catch (HSQLParseException e) {
                e.printStackTrace();
            }
            assert(xmlquery != null);

            // parse the xml like any other sql statement
            ParsedSelectStmt stmt = null;
            try {
                stmt = (ParsedSelectStmt) AbstractParsedStmt.parse(query, xmlquery, null, db, null);
            }
            catch (Exception e) {
                throw m_compiler.new VoltCompilerException(e.getMessage());
            }
            assert(stmt != null);

            String viewName = destTable.getTypeName();
            // throw an error if the view isn't within voltdb's limited worldview
            checkViewMeetsSpec(viewName, stmt);

            // Allow only non-unique indexes other than the primary key index.
            // The primary key index is yet to be defined (below).
            for (Index destIndex : destTable.getIndexes()) {
                if (destIndex.getUnique()) {
                    String msg = "A UNIQUE index is not allowed on a materialized view. " +
                            "Remove the qualifier \"UNIQUE\" from the index " + destIndex.getTypeName() +
                            "defined on the materialized view \"" + viewName + "\".";
                    throw m_compiler.new VoltCompilerException(msg);
                }
            }

            // create the materializedviewinfo catalog node for the source table
            Table srcTable = stmt.tableList.get(0);
            MaterializedViewInfo matviewinfo = srcTable.getViews().add(viewName);
            matviewinfo.setDest(destTable);
            AbstractExpression where = stmt.getSingleTableFilterExpression();
            if (where != null) {
                String hex = Encoder.hexEncode(where.toJSONString());
                matviewinfo.setPredicate(hex);
            } else {
                matviewinfo.setPredicate("");
            }
            destTable.setMaterializer(srcTable);

            List<Column> srcColumnArray = CatalogUtil.getSortedCatalogItems(srcTable.getColumns(), "index");
            List<Column> destColumnArray = CatalogUtil.getSortedCatalogItems(destTable.getColumns(), "index");

            // add the group by columns from the src table
            for (int i = 0; i < stmt.groupByColumns.size(); i++) {
                ParsedSelectStmt.ParsedColInfo gbcol = stmt.groupByColumns.get(i);
                Column srcCol = srcColumnArray.get(gbcol.index);
                ColumnRef cref = matviewinfo.getGroupbycols().add(srcCol.getTypeName());
                // groupByColumns is iterating in order of groups. Store that grouping order
                // in the column ref index. When the catalog is serialized, it will, naturally,
                // scramble this order like a two year playing dominos, presenting the data
                // in a meaningless sequence.
                cref.setIndex(i);           // the column offset in the view's grouping order
                cref.setColumn(srcCol);     // the source column from the base (non-view) table
            }

            ParsedSelectStmt.ParsedColInfo countCol = stmt.displayColumns.get(stmt.groupByColumns.size());
            assert(countCol.expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR);
            assert(countCol.expression.getLeft() == null);
            processMaterializedViewColumn(matviewinfo, srcTable, destTable,
                    destColumnArray.get(stmt.groupByColumns.size()),
                    ExpressionType.AGGREGATE_COUNT_STAR, null);

            // create an index and constraint for the table
            Index pkIndex = destTable.getIndexes().add("MATVIEW_PK_INDEX");
            pkIndex.setType(IndexType.BALANCED_TREE.getValue());
            pkIndex.setUnique(true);
            // add the group by columns from the src table
            // assume index 1 throuh #grpByCols + 1 are the cols
            for (int i = 0; i < stmt.groupByColumns.size(); i++) {
                ColumnRef c = pkIndex.getColumns().add(String.valueOf(i));
                c.setColumn(destColumnArray.get(i));
                c.setIndex(i);
            }
            Constraint pkConstraint = destTable.getConstraints().add("MATVIEW_PK_CONSTRAINT");
            pkConstraint.setType(ConstraintType.PRIMARY_KEY.getValue());
            pkConstraint.setIndex(pkIndex);

            // parse out the group by columns into the dest table
            for (int i = 0; i < stmt.groupByColumns.size(); i++) {
                ParsedSelectStmt.ParsedColInfo col = stmt.displayColumns.get(i);
                Column destColumn = destColumnArray.get(i);

                processMaterializedViewColumn(matviewinfo, srcTable, destTable, destColumn,
                        ExpressionType.VALUE_TUPLE, (TupleValueExpression)col.expression);
            }

            // parse out the aggregation columns into the dest table
            for (int i = stmt.groupByColumns.size() + 1; i < stmt.displayColumns.size(); i++) {
                ParsedSelectStmt.ParsedColInfo col = stmt.displayColumns.get(i);
                Column destColumn = destColumnArray.get(i);

                AbstractExpression colExpr = col.expression.getLeft();
                assert(colExpr.getExpressionType() == ExpressionType.VALUE_TUPLE);
                processMaterializedViewColumn(matviewinfo, srcTable, destTable, destColumn,
                        col.expression.getExpressionType(), (TupleValueExpression)colExpr);

                // Correctly set the type of the column so that it's consistent.
                // Otherwise HSQLDB might promote types differently than Volt.
                destColumn.setType(col.expression.getValueType().getValue());
            }
        }
    }

    /**
     * Verify the materialized view meets our arcane rules about what can and can't
     * go in a materialized view. Throw hopefully helpful error messages when these
     * rules are inevitably borked.
     *
     * @param viewName The name of the view being checked.
     * @param stmt The output from the parser describing the select statement that creates the view.
     * @throws VoltCompilerException
     */
    private void checkViewMeetsSpec(String viewName, ParsedSelectStmt stmt) throws VoltCompilerException {
        int groupColCount = stmt.groupByColumns.size();
        int displayColCount = stmt.displayColumns.size();
        String msg = "Materialized view \"" + viewName + "\" ";

        if (stmt.tableList.size() != 1) {
            msg += "has " + String.valueOf(stmt.tableList.size()) + " sources. " +
            "Only one source view or source table is allowed.";
            throw m_compiler.new VoltCompilerException(msg);
        }

        if (displayColCount <= groupColCount) {
            msg += "has too few columns.";
            throw m_compiler.new VoltCompilerException(msg);
        }

        if (stmt.hasComplexGroupby()) {
            msg += "contains an expression involving a group by. " +
                    "Expressions with group by are not currently supported in views.";
            throw m_compiler.new VoltCompilerException(msg);
        }

        if (stmt.hasComplexAgg()) {
            msg += "contains an expression involving an aggregate function. " +
                    "Expressions with aggregate functions are not currently supported in views.";
            throw m_compiler.new VoltCompilerException(msg);
        }


        int i;
        for (i = 0; i < groupColCount; i++) {
            ParsedSelectStmt.ParsedColInfo gbcol = stmt.groupByColumns.get(i);
            ParsedSelectStmt.ParsedColInfo outcol = stmt.displayColumns.get(i);

            if (outcol.expression.getExpressionType() != ExpressionType.VALUE_TUPLE) {
                msg += "must have column at index " + String.valueOf(i) + " be " + gbcol.alias;
                throw m_compiler.new VoltCompilerException(msg);
            }

            TupleValueExpression expr = (TupleValueExpression) outcol.expression;
            if (expr.getColumnIndex() != gbcol.index) {
                msg += "must have column at index " + String.valueOf(i) + " be " + gbcol.alias;
                throw m_compiler.new VoltCompilerException(msg);
            }
        }

        AbstractExpression coli = stmt.displayColumns.get(i).expression;
        if (coli.getExpressionType() != ExpressionType.AGGREGATE_COUNT_STAR) {
            msg += "is missing count(*) as the column after the group by columns, a materialized view requirement.";
            throw m_compiler.new VoltCompilerException(msg);
        }

        for (i++; i < displayColCount; i++) {
            ParsedSelectStmt.ParsedColInfo outcol = stmt.displayColumns.get(i);
            if ((outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_COUNT) &&
                    (outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_SUM)) {
                msg += "must have non-group by columns aggregated by sum or count.";
                throw m_compiler.new VoltCompilerException(msg);
            }
            if (outcol.expression.getLeft().getExpressionType() != ExpressionType.VALUE_TUPLE) {
                msg += "must have non-group by columns use only one level of aggregation.";
                throw m_compiler.new VoltCompilerException(msg);
            }
        }
    }

    void processMaterializedViewColumn(MaterializedViewInfo info, Table srcTable, Table destTable,
            Column destColumn, ExpressionType type, TupleValueExpression colExpr)
            throws VoltCompiler.VoltCompilerException {

        if (colExpr != null) {
            assert(colExpr.getTableName().equalsIgnoreCase(srcTable.getTypeName()));
            String srcColName = colExpr.getColumnName();
            Column srcColumn = srcTable.getColumns().getIgnoreCase(srcColName);
            destColumn.setMatviewsource(srcColumn);
        }
        destColumn.setMatview(info);
        destColumn.setAggregatetype(type.getValue());
    }
}
