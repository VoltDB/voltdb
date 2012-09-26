/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
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
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
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
     * Regex Description:
     *
     *  if the statement starts with either create procedure, partition,
     *  replicate, or role the first match group is set to respectively procedure,
     *  partition, replicate, or role.
     * <pre>
     * (?i) -- ignore case
     * ((?<=\\ACREATE\\s{0,1024})PROCEDURE|\\APARTITION|\\AREPLICATE|\\ACREATE\\s{0,1024})ROLE) -- voltdb ddl
     *    [capture group 1]
     *      (?<=\\ACREATE\\s{1,1024})PROCEDURE -- create procedure ddl
     *          (?<=\\ACREATE\\s{0,1024}) -- CREATE zero-width positive lookbehind
     *              \\A -- beginning of statement
     *              CREATE -- token
     *              \\s{1,1024} -- one or up to 1024 spaces
     *              PROCEDURE -- procedure token
     *      | -- or
     *      \\A -- beginning of statement
     *      -- PARTITION token
     *      | -- or
     *      \\A -- beginning of statement
     *      REPLICATE -- token
     *      | -- or
     *      \\A -- beginning of statement
     *      CREATE\\s{0,1024})ROLE -- create role ddl
     * \\s -- one space
     * </pre>
     */
    static final Pattern voltdbStatementPrefixPattern = Pattern.compile(
            "(?i)((?<=\\ACREATE\\s{0,1024})PROCEDURE|\\APARTITION|\\AREPLICATE|\\ACREATE\\s{0,1024}ROLE)\\s"
            );

    static final String TABLE = "TABLE";
    static final String PROCEDURE = "PROCEDURE";
    static final String PARTITION = "PARTITION";
    static final String REPLICATE = "REPLICATE";

    HSQLInterface m_hsql;
    VoltCompiler m_compiler;
    String m_fullDDL = "";
    int m_currLineNo = 1;

    /// Partition descriptors parsed from DDL PARTITION or REPLICATE statements.
    final PartitionMap m_partitionMap;

    /// Groups parsed from CREATE ROLE statements.
    final CatalogMap<Group> m_groupMap;

    HashMap<String, Column> columnMap = new HashMap<String, Column>();
    HashMap<String, Index> indexMap = new HashMap<String, Index>();
    HashMap<Table, String> matViewMap = new HashMap<Table, String>();

    private class DDLStatement {
        public DDLStatement() {
        }
        String statement = "";
        int lineNo;
    }

    public DDLCompiler(VoltCompiler compiler, HSQLInterface hsql, PartitionMap partitionMap, CatalogMap<Group> groupMap) {
        assert(compiler != null);
        assert(hsql != null);
        assert(partitionMap != null);
        this.m_hsql = hsql;
        this.m_compiler = compiler;
        this.m_partitionMap = partitionMap;
        this.m_groupMap = groupMap;

    }

    /**
     * Compile a DDL schema from a file on disk
     * @param path
     * @throws VoltCompiler.VoltCompilerException
     */
    public void loadSchema(String path)
            throws VoltCompiler.VoltCompilerException {
        File inputFile = new File(path);
        FileReader reader = null;
        try {
            reader = new FileReader(inputFile);
        } catch (FileNotFoundException e) {
            throw m_compiler.new VoltCompilerException("Unable to open schema file for reading");
        }

        m_currLineNo = 1;
        this.loadSchema(path, reader);
    }

    /**
     * Compile a file from an open input stream
     * @param path
     * @param reader
     * @throws VoltCompiler.VoltCompilerException
     */
    public void loadSchema(String path, FileReader reader)
            throws VoltCompiler.VoltCompilerException {

        DDLStatement stmt = getNextStatement(reader, m_compiler);
        while (stmt != null) {
            // Some statements are processed by VoltDB and the rest are handled by HSQL.
            boolean processed = false;
            try {
                processed = processVoltDBStatement(stmt.statement);
            } catch (VoltCompilerException e) {
                // Reformat the message thrown by VoltDB DDL processing to have a line number.
                String msg = "VoltDB DDL Error: \"" + e.getMessage() + "\" in statement starting on lineno: " + stmt.lineNo;
                throw m_compiler.new VoltCompilerException(msg);
            }
            if (!processed) {
                try {
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
                String msg = "Bad indentifier in DDL: \"" +
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
     * @return true if statement was handled, otherwise it should be passed to HSQL
     * @throws VoltCompilerException
     */
    private boolean processVoltDBStatement(String statement) throws VoltCompilerException {
        if (statement == null || statement.trim().isEmpty()) {
            return false;
        }

        statement = statement.trim();

        // matches if it is the beginning of a voltDB statement
        Matcher statementMatcher = voltdbStatementPrefixPattern.matcher(statement);
        if( ! statementMatcher.find()) {
            return false;
        }

        // either PROCEDURE, REPLICATE, or PARTITION
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
                    descriptor.m_authGroups.add(roleName.trim().toLowerCase());
                }
            }

            // track the defined procedure
            m_partitionMap.add(descriptor);

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

            m_partitionMap.add(descriptor);

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
                            "Bad PARTITION DDL statement: \"%s\", " +
                            "expected syntax: PARTITION TABLE <table> ON COLUMN <column>",
                            statement.substring(0,statement.length()-1))); // remove trailing semicolon
                }
                // group(1) -> table, group(2) -> column
                m_partitionMap.put(
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
                            "Bad PARTITION DDL statement: \"%s\", " +
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
                m_partitionMap.addProcedurePartitionInfoTo(
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
            m_partitionMap.put(
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
            if (m_groupMap.get(roleName) != null) {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Role name \"%s\" in CREATE ROLE DDL statement already exists.",
                        roleName));
            }
            org.voltdb.catalog.Group catGroup = m_groupMap.add(roleName);
            if (statementMatcher.group(2) != null) {
                for (String tokenRaw : StringUtils.split(statementMatcher.group(2), ',')) {
                    String token = tokenRaw.trim().toLowerCase();
                    if (token.equals("adhoc")) {
                        catGroup.setAdhoc(true);
                    }
                    else if (token.equals("sysproc")) {
                        catGroup.setSysproc(true);
                    }
                    else if (token.equals("defaultproc")) {
                        catGroup.setDefaultproc(true);
                    }
                    else {
                        throw m_compiler.new VoltCompilerException(String.format(
                            "Bad flag \"%s\" to CREATE ROLE DDL statement: \"%s\", " +
                            "expected syntax: CREATE ROLE <role> [WITH adhoc|sysproc|defaultproc ...]",
                            token, statement.substring(0,statement.length()-1))); // remove trailing semicolon
                    }
                }
            }
            return true;
        }

        /*
         * if no correct syntax regex matched above then at this juncture
         * the statement is syntax incorrect
         */

        if( PARTITION.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Bad PARTITION DDL statement: \"%s\", " +
                    "expected syntax: \"PARTITION TABLE <table> ON COLUMN <column>\" or " +
                    "\"PARTITION PROCEDURE <procedure> ON " +
                    "TABLE <table> COLUMN <column> [PARAMETER <parameter-index-no>]\"",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if( REPLICATE.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Bad REPLICATE DDL statement: \"%s\", " +
                    "expected syntax: REPLICATE TABLE <table>",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if( PROCEDURE.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Bad CREATE PROCEDURE DDL statement: \"%s\", " +
                    "expected syntax: \"CREATE PROCEDURE [ALLOW <role> [, <role> ...] FROM CLASS <class-name>\" " +
                    "or: \"CREATE PROCEDURE <name> [ALLOW <role> [, <role> ...] AS <single-select-or-dml-statement>\"",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        // Not a VoltDB-specific DDL statement.
        return false;
    }

    public void compileToCatalog(Catalog catalog, Database db) throws VoltCompilerException {
        String hexDDL = Encoder.hexEncode(m_fullDDL);
        catalog.execute("set " + db.getPath() + " schema \"" + hexDDL + "\"");

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
        BuildDirectoryUtils.writeFile("schema-xml", "hsql-catalog-output.xml", xmlCatalog.toString());

        // build the local catalog from the xml catalog
        fillCatalogFromXML(catalog, db, xmlCatalog);
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

    public void fillCatalogFromXML(Catalog catalog, Database db, VoltXMLElement xml)
    throws VoltCompiler.VoltCompilerException {

        if (xml == null)
            throw m_compiler.new VoltCompilerException("Unable to parse catalog xml file from hsqldb");

        assert xml.name.equals("databaseschema");

        for (VoltXMLElement node : xml.children) {
            if (node.name.equals("table"))
                addTableToCatalog(catalog, db, node);
        }

        processMaterializedViews(db);
    }

    void addTableToCatalog(Catalog catalog, Database db, VoltXMLElement node) throws VoltCompilerException {
        assert node.name.equals("table");

        // clear these maps, as they're table specific
        columnMap.clear();
        indexMap.clear();

        String name = node.attributes.get("name");

        Table table = db.getTables().add(name);

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
                for (VoltXMLElement indexNode : subNode.children) {
                    if (indexNode.name.equals("index"))
                        addIndexToCatalog(table, indexNode);
                }
            }

            if (subNode.name.equals("constraints")) {
                for (VoltXMLElement constraintNode : subNode.children) {
                    if (constraintNode.name.equals("constraint"))
                        addConstraintToCatalog(table, constraintNode);
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
                            " but the maximum supported size is " + VoltType.MAX_VALUE_LENGTH_STR);
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
        String typename = node.attributes.get("type");
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
                        defaultvalue = inner_child.attributes.get("value");
                        defaulttype = inner_child.attributes.get("type");
                    }
                    // Function
                    /*else if (inner_child.name.equals("function")) {
                        defaultvalue = inner_child.attributes.get("name");
                        defaulttype = VoltType.VOLTFUNCTION.name();
                    }*/
                    if (defaultvalue != null) break;
                }
            }
        }
        if (defaultvalue != null && defaultvalue.equals("NULL"))
            defaultvalue = null;
        if (defaulttype != null) {
            // fyi: Historically, VoltType class initialization errors get reported on this line (?).
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
        int size = Integer.parseInt(sizeString);
        // check valid length if varchar
        if (type == VoltType.STRING) {
            if ((size == 0) || (size > VoltType.MAX_VALUE_LENGTH)) {
                String msg = "VARCHAR Column " + name + " in table " + table.getTypeName() + " has unsupported length " + sizeString;
                throw m_compiler.new VoltCompilerException(msg);
            }
        }
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
        column.setSize(size);

        column.setDefaultvalue(defaultvalue);
        if (defaulttype != null)
            column.setDefaulttype(Integer.parseInt(defaulttype));

        columnMap.put(name, column);
    }

    void addIndexToCatalog(Table table, VoltXMLElement node) throws VoltCompilerException {
        assert node.name.equals("index");

        String name = node.attributes.get("name");
        boolean unique = Boolean.parseBoolean(node.attributes.get("unique"));

        // "parse" the expression trees for an expression-based index (vs. a simple column value index)
        AbstractExpression[] exprs = null;
        for (VoltXMLElement subNode : node.children) {
            if (subNode.name.equals("exprs")) {
                exprs = new AbstractExpression[subNode.children.size()];
                int j = 0;
                for (VoltXMLElement exprNode : subNode.children) {
                    exprs[j] = AbstractParsedStmt.parseExpressionTree(null, exprNode);
                    exprs[j].resolveForTable(table);
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
    void addConstraintToCatalog(Table table, VoltXMLElement node) throws VoltCompilerException {
        assert node.name.equals("constraint");

        String name = node.attributes.get("name");
        String typeName = node.attributes.get("type");
        ConstraintType type = ConstraintType.valueOf(typeName);
        if (type == null) {
            throw m_compiler.new VoltCompilerException("Invalid constraint type '" + typeName + "'");
        }

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
        else if (type == ConstraintType.PRIMARY_KEY) {
            // create the unique index below
            // primary key code is in other places as well
        }
        else if (type == ConstraintType.UNIQUE) {
            // just create the unique index below
        }
        else if (type == ConstraintType.MAIN) {
            // should never see these
            assert(false);
        }
        else if (type == ConstraintType.NOT_NULL) {
            // these get handled by table metadata inspection
            return;
        }

        // The constraint is backed by an index, therefore we need to create it
        // TODO: We need to be able to use indexes for foreign keys. I am purposely
        //       leaving those out right now because HSQLDB just makes too many of them.
        Constraint catalog_const = null;
        if (node.attributes.get("index") != null) {
            String indexName = node.attributes.get("index");
            Index catalog_index = indexMap.get(indexName);

            // if the constraint name contains index type hints, exercise them (giant hack)
            if (catalog_index != null) {
                String constraintNameNoCase = name.toLowerCase();
                if (constraintNameNoCase.contains("tree"))
                    catalog_index.setType(IndexType.BALANCED_TREE.getValue());
                if (constraintNameNoCase.contains("hash"))
                    catalog_index.setType(IndexType.HASH_TABLE.getValue());
            }

            catalog_const = table.getConstraints().add(name);
            if (catalog_index != null) {
                catalog_const.setIndex(catalog_index);
                catalog_index.setUnique(type == ConstraintType.UNIQUE || type == ConstraintType.PRIMARY_KEY);
            }
        } else {
            catalog_const = table.getConstraints().add(name);
        }
        catalog_const.setType(type.getValue());

        // NO ADDITIONAL WORK
        // since we only really support unique constraints, setting up a
        // unique index above is all we need to do to make that work

        return;
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
                stmt = (ParsedSelectStmt) AbstractParsedStmt.parse(query, xmlquery, db, null);
            }
            catch (Exception e) {
                throw m_compiler.new VoltCompilerException(e.getMessage());
            }
            assert(stmt != null);

            // throw an error if the view isn't withing voltdb's limited worldview
            checkViewMeetsSpec(destTable.getTypeName(), stmt);

            // create the materializedviewinfo catalog node for the source table
            Table srcTable = stmt.tableList.get(0);
            MaterializedViewInfo matviewinfo = srcTable.getViews().add(destTable.getTypeName());
            matviewinfo.setDest(destTable);
            if (stmt.where == null)
                matviewinfo.setPredicate("");
            else {
                String hex = Encoder.hexEncode(stmt.where.toJSONString());
                matviewinfo.setPredicate(hex);
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
            assert(countCol.expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT);
            assert(countCol.expression.getLeft() == null);
            processMaterializedViewColumn(matviewinfo, srcTable, destTable, destColumnArray.get(stmt.groupByColumns.size()),
                    ExpressionType.AGGREGATE_COUNT, null);

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
        if ((coli.getExpressionType() != ExpressionType.AGGREGATE_COUNT) ||
                (coli.getLeft() != null) ||
                (coli.getRight() != null)) {
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
