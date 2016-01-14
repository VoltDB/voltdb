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

package org.voltdb.compiler;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Matcher;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.HSQLDDLInfo;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.hsqldb_voltpatches.VoltXMLElement.VoltXMLDiff;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.DatabaseConfiguration;
import org.voltdb.catalog.Group;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.IndexRef;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.common.Constants;
import org.voltdb.common.Permission;
import org.voltdb.compiler.ClassMatcher.ClassNameMatchStatus;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.ProcedureDescriptor;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractSubqueryExpression;
import org.voltdb.expressions.AggregateExpression;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.groovy.GroovyCodeBlockCompiler;
import org.voltdb.parser.HSQLLexer;
import org.voltdb.parser.SQLLexer;
import org.voltdb.parser.SQLParser;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.ParsedColInfo;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.planner.SubPlanAssembler;
import org.voltdb.planner.parseinfo.StmtTableScan;
import org.voltdb.planner.parseinfo.StmtTargetTableScan;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogSchemaTools;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.InMemoryJarfile;


/**
 * Compiles schema (SQL DDL) text files and stores the results in a given catalog.
 *
 */
public class DDLCompiler {

    private static final int MAX_COLUMNS = 1024; // KEEP THIS < MAX_PARAM_COUNT to enable default CRUD update.
    private static final int MAX_ROW_SIZE = 1024 * 1024 * 2;
    private static final int MAX_BYTES_PER_UTF8_CHARACTER = 4;

    private static final String TABLE = "TABLE";
    private static final String PROCEDURE = "PROCEDURE";
    private static final String PARTITION = "PARTITION";
    private static final String REPLICATE = "REPLICATE";
    private static final String EXPORT = "EXPORT";
    private static final String ROLE = "ROLE";
    private static final String DR = "DR";

    private final HSQLInterface m_hsql;
    private final VoltCompiler m_compiler;
    private String m_fullDDL = "";
    private int m_currLineNo = 1;

    // Partition descriptors parsed from DDL PARTITION or REPLICATE statements.
    private final VoltDDLElementTracker m_tracker;
    private final VoltXMLElement m_schema =
            new VoltXMLElement(HSQLInterface.XML_SCHEMA_NAME)
            .withValue("name", HSQLInterface.XML_SCHEMA_NAME);

    // used to match imported class with those in the classpath
    // For internal cluster compilation, this will point to the
    // InMemoryJarfile for the current catalog, so that we can
    // find classes provided as part of the application.
    private final ClassMatcher m_classMatcher = new ClassMatcher();

    private final HashMap<Table, String> m_matViewMap = new HashMap<>();

    /** A cache of the XML used to do validation on LIMIT DELETE statements
     * Preserved here to avoid having to re-parse for planning */
    private final Map<Statement, VoltXMLElement> m_limitDeleteStmtToXml = new HashMap<>();

    // Resolve classes using a custom loader. Needed for catalog version upgrade.
    private final ClassLoader m_classLoader;

    private final Set<String> tableLimitConstraintCounter = new HashSet<>();

    // Meta columns for DR conflicts table
    public static String DR_ROW_TYPE_COLUMN_NAME = "ROW_TYPE";
    public static String DR_LOG_ACTION_COLUMN_NAME = "ACTION_TYPE";
    public static String DR_CONFLICT_COLUMN_NAME = "CONFLICT_TYPE";
    public static String DR_CONFLICTS_ON_PK_COLUMN_NAME = "CONFLICTS_ON_PRIMARY_KEY";
    public static String DR_DECISION_COLUMN_NAME = "DECISION";
    public static String DR_CLUSTER_ID_COLUMN_NAME = "CLUSTER_ID";
    public static String DR_TIMESTAMP_COLUMN_NAME = "TIMESTAMP";
    public static String DR_DIVERGENCE_COLUMN_NAME = "DIVERGENCE";
    public static String DR_TABLE_NAME_COLUMN_NAME = "TABLE_NAME";
    // The varchar column contains JSON representation of original data
    public static String DR_TUPLE_COLUMN_NAME = "TUPLE";

    static final String [][] DR_CONFLICTS_EXPORT_TABLE_META_COLUMNS = {
        {DR_ROW_TYPE_COLUMN_NAME, "VARCHAR(3 BYTES) NOT NULL"},
        {DR_LOG_ACTION_COLUMN_NAME, "VARCHAR(1 BYTES) NOT NULL"},
        {DR_CONFLICT_COLUMN_NAME, "VARCHAR(4 BYTES)"},
        {DR_CONFLICTS_ON_PK_COLUMN_NAME, "TINYINT"},
        {DR_DECISION_COLUMN_NAME, "VARCHAR(1 BYTES) NOT NULL"},
        {DR_CLUSTER_ID_COLUMN_NAME, "TINYINT NOT NULL"},
        {DR_TIMESTAMP_COLUMN_NAME, "BIGINT NOT NULL"},
        {DR_DIVERGENCE_COLUMN_NAME, "VARCHAR(1 BYTES) NOT NULL"},
        {DR_TABLE_NAME_COLUMN_NAME, "VARCHAR(1024 BYTES)"},
        {DR_TUPLE_COLUMN_NAME, "VARCHAR(1048576 BYTES)"},
    };

    private static class DDLStatement {
        public DDLStatement() { }
        String statement = "";
        int lineNo;
    }

    public DDLCompiler(VoltCompiler compiler,
                       HSQLInterface hsql,
                       VoltDDLElementTracker tracker,
                       ClassLoader classLoader)  {
        assert(compiler != null);
        assert(hsql != null);
        assert(tracker != null);
        m_hsql = hsql;
        m_compiler = compiler;
        m_tracker = tracker;
        m_classLoader = classLoader;
    }

    private static boolean isVoltTypePresent(VoltXMLElement tableNode, VoltType lookupType, StringBuffer msg) {
        assert(tableNode.name.equals("table"));

        String valueTypeStr;
        VoltType type;
        String columnName;
        boolean typeDetected = false;
        String separatorStr;

        for (VoltXMLElement subNode : tableNode.children) {
            if (subNode.name.equals("columns")) {
                for (VoltXMLElement columnNode : subNode.children) {
                    if (columnNode.name.equals("column")) {
                        valueTypeStr = columnNode.attributes.get("valuetype");
                        columnName = columnNode.attributes.get("name");
                        type = VoltType.typeFromString(valueTypeStr);
                        if (lookupType == type) {
                            if (msg != null) {
                                separatorStr = (typeDetected == true)? ", " : " ";
                                msg.append(separatorStr + "column name: '" + columnName + "' type: '" + valueTypeStr + "'");
                            }
                            typeDetected = true;
                        }
                    }
                }
            } // if (subNode.name.equals("columns"))
        }

        return typeDetected;
    }

    // Function to check if the table - TableXML, supplied as VoltXMLElement, does not contain geo types.
    // If table does contain geo types, VoltCompilerException is generated with information about geo columns
    // found in table wrapped in exception message
    private void guardForGeoColumns(VoltXMLElement tableXML, String tableName, String tableVerb)
            throws VoltCompilerException {
        assert(tableXML.name.equals("table"));
        assert(tableVerb != null);
        assert(tableName != null);

        boolean pointColDetected = false;
        StringBuffer pointMsg = new StringBuffer();
        pointColDetected = isVoltTypePresent(tableXML, VoltType.GEOGRAPHY_POINT, pointMsg);

        boolean polygonColDetected = false;
        StringBuffer polygonMsg = new StringBuffer();
        polygonColDetected = isVoltTypePresent(tableXML, VoltType.GEOGRAPHY, polygonMsg);
        //
        if (pointColDetected || polygonColDetected) {
            String separatorStr = (pointColDetected && polygonColDetected) ? "," : "";
            throw m_compiler.new VoltCompilerException("Can't " + tableVerb + " table '" + tableName.toUpperCase() + "' containing geo type column(s) -" +
                    pointMsg + separatorStr + polygonMsg + ".");
        }
    }

    /**
     * Compile a DDL schema from an abstract reader
     * @param reader  abstract DDL reader
     * @param db  database
     * @param whichProcs  which type(s) of procedures to load
     * @throws VoltCompiler.VoltCompilerException
     */
    void loadSchema(Reader reader, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompiler.VoltCompilerException {
        m_currLineNo = 1;

        DDLStatement stmt = getNextStatement(reader, m_compiler);
        while (stmt != null) {
            // Some statements are processed by VoltDB and the rest are handled by HSQL.
            boolean processed = false;
            try {
                processed = processVoltDBStatement(stmt, db, whichProcs);
            } catch (VoltCompilerException e) {
                // Reformat the message thrown by VoltDB DDL processing to have a line number.
                String msg = "VoltDB DDL Error: \"" + e.getMessage() + "\" in statement starting on lineno: " + stmt.lineNo;
                throw m_compiler.new VoltCompilerException(msg);
            }
            if (!processed) {
                try {
                    //* enable to debug */ System.out.println("DEBUG: " + stmt.statement);
                    // kind of ugly.  We hex-encode each statement so we can
                    // avoid embedded newlines so we can delimit statements
                    // with newline.
                    m_fullDDL += Encoder.hexEncode(stmt.statement) + "\n";

                    // figure out what table this DDL might affect to minimize diff processing
                    HSQLDDLInfo ddlStmtInfo = HSQLLexer.preprocessHSQLDDL(stmt.statement);

                    // Get the diff that results from applying this statement and apply it
                    // to our local tree (with Volt-specific additions)
                    VoltXMLDiff thisStmtDiff = m_hsql.runDDLCommandAndDiff(ddlStmtInfo, stmt.statement);
                    // null diff means no change (usually drop if exists for non-existent thing)
                    if (thisStmtDiff != null) {
                        applyDiff(thisStmtDiff);
                    }
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

        // process extra classes
        m_tracker.addExtraClasses(m_classMatcher.getMatchedClassList());
        // possibly save some memory
        m_classMatcher.clear();
    }

    private void createDRConflictTables(StringBuilder sb, Database previousDBIfAny) {
        boolean hasPartitionedConflictTable;
        boolean hasReplicatedConflictTable;

        // Do DR conflicts export table exist already?
        if (previousDBIfAny != null) {
            hasPartitionedConflictTable = previousDBIfAny.getTables().get(CatalogUtil.DR_CONFLICTS_PARTITIONED_EXPORT_TABLE) != null;
            hasReplicatedConflictTable = previousDBIfAny.getTables().get(CatalogUtil.DR_CONFLICTS_REPLICATED_EXPORT_TABLE) != null;
        } else {
            hasPartitionedConflictTable = hasConflictTableInSchema(m_schema, CatalogUtil.DR_CONFLICTS_PARTITIONED_EXPORT_TABLE);
            hasReplicatedConflictTable = hasConflictTableInSchema(m_schema, CatalogUtil.DR_CONFLICTS_REPLICATED_EXPORT_TABLE);
        }

        if (!hasPartitionedConflictTable) {
            createOneDRConflictTable(sb, CatalogUtil.DR_CONFLICTS_PARTITIONED_EXPORT_TABLE, true);
        }
        if (!hasReplicatedConflictTable) {
            createOneDRConflictTable(sb, CatalogUtil.DR_CONFLICTS_REPLICATED_EXPORT_TABLE, false);
        }
    }

    private static void createOneDRConflictTable(StringBuilder sb, String name, boolean partitioned) {
        // If the conflict export table doesn't exist yet, create a new one.
        sb.append("CREATE TABLE ").append(name).append(" (");
        for (String[] column : DR_CONFLICTS_EXPORT_TABLE_META_COLUMNS) {
            sb.append(column[0]).append(" ").append(column[1]);
            if (!column[0].equals(DR_TUPLE_COLUMN_NAME)) {
                sb.append(", ");
            }
        }
        sb.append(");\n");
        sb.append("EXPORT TABLE ").append(name).append(" TO STREAM ").append(CatalogUtil.DR_CONFLICTS_TABLE_EXPORT_GROUP).append(";\n");

        // The partitioning here doesn't matter, it's only to trick the export system, not related to data placement.
        if (partitioned) {
            sb.append("PARTITION TABLE ").append(name).append(" ON COLUMN ").append(DR_TIMESTAMP_COLUMN_NAME).append(";\n");
        }
    }

    private static boolean hasConflictTableInSchema(VoltXMLElement m_schema, String name) {
        for (VoltXMLElement element : m_schema.children) {
            if (element.name.equals("table")
                    && element.attributes.containsKey("export")
                    && element.attributes.get("name").equals(name)) {
                return true;
            }
        }
        return false;
    }

    // Drop the dr conflicts table if A/A is disabled
    private void dropDRConflictTablesIfNeeded(StringBuilder sb) {
        if (hasConflictTableInSchema(m_schema, CatalogUtil.DR_CONFLICTS_PARTITIONED_EXPORT_TABLE)) {
            sb.append("DROP TABLE " + CatalogUtil.DR_CONFLICTS_PARTITIONED_EXPORT_TABLE + ";\n");
        }
        if (hasConflictTableInSchema(m_schema, CatalogUtil.DR_CONFLICTS_REPLICATED_EXPORT_TABLE)) {
            sb.append("DROP TABLE " + CatalogUtil.DR_CONFLICTS_REPLICATED_EXPORT_TABLE + ";\n");
        }
    }

    // Generate DDL to create or drop the DR conflict table
    private String generateDDLForDRConflictsTable(Database currentDB, Database previousDBIfAny) {
        StringBuilder sb = new StringBuilder();
        if (currentDB.getIsactiveactivedred()) {
            createDRConflictTables(sb, previousDBIfAny);
        } else {
            dropDRConflictTablesIfNeeded(sb);
        }
        return sb.toString();
    }

    /**
     * Load auto generated DR conflicts table into current schema.
     * DR conflict table will be generated by
     * 1) compiling the catalog if DR table exist and A/A DR is on,
     * 2) liveDDL to enable DR table and turn A/A DR on
     * 3) @UpdateApplicationCatalog to enable DR table and turn A/A DR on
     *
     * @param db  current database
     * @param previousDBIfAny  previous status of database, liveDDL needs it
     * @param whichProcs  which type(s) of procedures to load
     * @throws VoltCompilerException
     */
    void loadAutogenExportTableSchema(Database db, Database previousDBIfAny,
            DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        Reader reader = new VoltCompilerStringReader(null, generateDDLForDRConflictsTable(db, previousDBIfAny));
        loadSchema(reader, db, whichProcs);
    }

    private void applyDiff(VoltXMLDiff stmtDiff)
    {
        // record which tables changed
        for (String tableName : stmtDiff.getChangedNodes().keySet()) {
            assert(tableName.startsWith("table"));
            tableName = tableName.substring("table".length());
            m_compiler.markTableAsDirty(tableName);
        }
        for (VoltXMLElement tableXML : stmtDiff.getRemovedNodes()) {
            String tableName = tableXML.attributes.get("name");
            assert(tableName != null);
            m_compiler.markTableAsDirty(tableName);
        }
        for (VoltXMLElement tableXML : stmtDiff.getAddedNodes()) {
            String tableName = tableXML.attributes.get("name");
            assert(tableName != null);
            m_compiler.markTableAsDirty(tableName);
        }

        m_schema.applyDiff(stmtDiff);
        // now go back and clean up anything that wasn't resolvable just by applying the diff
        // For now, this is:
        // - ensuring that the partition columns on tables are correct.  The hard
        // case is when the partition column is dropped from the table

        // Each statement can change at most one table. Check to see if the table is listed in
        // the changed nodes
        if (stmtDiff.getChangedNodes().isEmpty()) {
            return;
        }
        assert(stmtDiff.getChangedNodes().size() == 1);
        Entry<String, VoltXMLDiff> tableEntry = stmtDiff.getChangedNodes().entrySet().iterator().next();
        VoltXMLDiff tableDiff = tableEntry.getValue();
        // need columns to be changed
        if (tableDiff.getChangedNodes().isEmpty() ||
            !tableDiff.getChangedNodes().containsKey("columnscolumns"))
        {
            return;
        }
        VoltXMLDiff columnsDiff = tableDiff.getChangedNodes().get("columnscolumns");
        assert(columnsDiff != null);
        // Need to have deleted columns
        if (columnsDiff.getRemovedNodes().isEmpty()) {
            return;
        }
        // Okay, get a list of deleted column names
        Set<String> removedColumns = new HashSet<String>();
        for (VoltXMLElement e : columnsDiff.getRemovedNodes()) {
            assert(e.attributes.get("name") != null);
            removedColumns.add(e.attributes.get("name"));
        }
        // go back and get our table name.  Use the uniquename ("table" + name) to get the element
        // from the schema
        VoltXMLElement tableElement = m_schema.findChild(tableEntry.getKey());
        assert(tableElement != null);
        String partitionCol = tableElement.attributes.get("partitioncolumn");
        // if we removed the partition column, then remove the attribute from the schema
        if (partitionCol != null && removedColumns.contains(partitionCol)) {
            m_compiler.addWarn(String.format("Partition column %s was dropped from table %s.  Attempting to change table to replicated.", partitionCol, tableElement.attributes.get("name")));
            tableElement.attributes.remove("partitioncolumn");
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
            if ( ! Character.isJavaIdentifierStart(identifier.charAt(loc))) {
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
     * Checks whether or not the start of the given identifier is java (and
     * thus DDL) compliant. An identifier may start with: _ [a-zA-Z] $ *
     * and contain subsequent characters including: _ [0-9a-zA-Z] $ *
     * @param identifier the identifier to check
     * @param statement the statement where the identifier is
     * @return the given identifier unmodified
     * @throws VoltCompilerException when it is not compliant
     */
    private String checkIdentifierWithWildcard(
            final String identifier, final String statement
            ) throws VoltCompilerException {

        assert identifier != null && ! identifier.trim().isEmpty();
        assert statement != null && ! statement.trim().isEmpty();

        int loc = 0;
        do {
            if ( ! Character.isJavaIdentifierStart(identifier.charAt(loc)) && identifier.charAt(loc)!= '*') {
                String msg = "Unknown indentifier in DDL: \"" +
                        statement.substring(0,statement.length()-1) +
                        "\" contains invalid identifier \"" + identifier + "\"";
                throw m_compiler.new VoltCompilerException(msg);
            }
            loc++;
            while (loc < identifier.length() && identifier.charAt(loc) != '.') {
                if (! Character.isJavaIdentifierPart(identifier.charAt(loc)) && identifier.charAt(loc)!= '*') {
                    String msg = "Unknown indentifier in DDL: \"" +
                            statement.substring(0,statement.length()-1) +
                            "\" contains invalid identifier \"" + identifier + "\"";
                    throw m_compiler.new VoltCompilerException(msg);
                }
                loc++;
            }
            if (loc < identifier.length() && identifier.charAt(loc) == '.') {
                loc++;
                if (loc >= identifier.length()) {
                    String msg = "Unknown indentifier in DDL: \"" +
                            statement.substring(0,statement.length()-1) +
                            "\" contains invalid identifier \"" + identifier + "\"";
                    throw m_compiler.new VoltCompilerException(msg);
                }
            }
        }
        while( loc > 0 && loc < identifier.length());

        return identifier;
    }

    /**
     * Check whether or not a procedure name is acceptible.
     * @param identifier the identifier to check
     * @param statement the statement where the identifier is
     * @return the given identifier unmodified
     * @throws VoltCompilerException
     */
    private String checkProcedureIdentifier(
            final String identifier, final String statement
            ) throws VoltCompilerException {
        String retIdent = checkIdentifierStart(identifier, statement);
        if (retIdent.contains(".")) {
            String msg = String.format(
                "Invalid procedure name containing dots \"%s\" in DDL: \"%s\"",
                identifier, statement.substring(0,statement.length()-1));
            throw m_compiler.new VoltCompilerException(msg);
        }
        return retIdent;
    }

   /**
     * Process a VoltDB-specific DDL statement, like PARTITION, REPLICATE,
     * CREATE PROCEDURE, and CREATE ROLE.
     * @param statement  DDL statement string
     * @param db
     * @param whichProcs
     * @return true if statement was handled, otherwise it should be passed to HSQL
     * @throws VoltCompilerException
     */
    private boolean processVoltDBStatement(DDLStatement ddlStatement, Database db,
                                           DdlProceduresToLoad whichProcs)
            throws VoltCompilerException
    {
        String statement = ddlStatement.statement;
        if (statement == null || statement.trim().isEmpty()) {
            return false;
        }

        statement = statement.trim();

        // matches if it is the beginning of a voltDB statement
        Matcher statementMatcher = SQLParser.matchAllVoltDBStatementPreambles(statement);
        if ( ! statementMatcher.find()) {
            return false;
        }

        // either PROCEDURE, REPLICATE, PARTITION, ROLE, EXPORT or DR
        String commandPrefix = statementMatcher.group(1).toUpperCase();

        // matches if it is CREATE PROCEDURE [ALLOW <role> ...] [PARTITION ON ...] FROM CLASS <class-name>;
        statementMatcher = SQLParser.matchCreateProcedureFromClass(statement);
        if (statementMatcher.matches()) {
            if (whichProcs != DdlProceduresToLoad.ALL_DDL_PROCEDURES) {
                return true;
            }
            String className = checkIdentifierStart(statementMatcher.group(2), statement);
            Class<?> clazz;
            try {
                clazz = Class.forName(className, true, m_classLoader);
            }
            catch (Throwable cause) {
                // We are here because either the class was not found or the class was found and
                // the initializer of the class threw an error we can't anticipate. So we will
                // wrap the error with a runtime exception that we can trap in our code.
                if (CoreUtils.isStoredProcThrowableFatalToServer(cause)) {
                    throw (Error)cause;
                }
                else {
                    throw m_compiler.new VoltCompilerException(String.format(
                            "Cannot load class for procedure: %s",
                            className), cause);
                }
            }

            ProcedureDescriptor descriptor = m_compiler.new ProcedureDescriptor(
                    new ArrayList<String>(), Language.JAVA, null, clazz);

            // Parse the ALLOW and PARTITION clauses.
            // Populate descriptor roles and returned partition data as needed.
            CreateProcedurePartitionData partitionData =
                    parseCreateProcedureClauses(descriptor, statementMatcher.group(1));

            // track the defined procedure
            String procName = m_tracker.add(descriptor);

            // add partitioning if specified
            addProcedurePartitionInfo(procName, partitionData, statement);

            return true;
        }

        // matches  if it is CREATE PROCEDURE <proc-name> [ALLOW <role> ...] [PARTITION ON ...] AS
        // ### <code-block> ### LANGUAGE <language-name>
        statementMatcher = SQLParser.matchCreateProcedureAsScript(statement);
        if (statementMatcher.matches()) {

            // Dots are okay in script procedures because they are a class name
            String className = checkIdentifierStart(statementMatcher.group(1), statement);
            String codeBlock = statementMatcher.group(3);
            String languageToken = statementMatcher.group(4);
            if (languageToken == null) {
                throw m_compiler.new VoltCompilerException("LANGUAGE clause is bad or missing.");
            }
            languageToken = languageToken.toUpperCase();

            Language language;
            try {
                language = Language.valueOf(languageToken);
            }
            catch (IllegalArgumentException e) {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Language \"%s\" is not a supported", languageToken));
            }

            Class<?> scriptClass = null;

            if (language == Language.GROOVY) {
                try {
                    scriptClass = GroovyCodeBlockCompiler.instance().parseCodeBlock(codeBlock, className);
                } catch (CodeBlockCompilerException ex) {
                    throw m_compiler.new VoltCompilerException(String.format(
                            "Procedure \"%s\" code block has syntax errors:\n%s",
                            className, ex.getMessage()));
                } catch (Exception ex) {
                    throw m_compiler.new VoltCompilerException(ex);
                }
            } else {
                // Not sure how to get here with exception handling above, but help yourself
                // to a belt with those suspenders!
                throw m_compiler.new VoltCompilerException(String.format(
                        "Language \"%s\" is not a supported", language.name()));
            }

            ProcedureDescriptor descriptor = m_compiler.new ProcedureDescriptor(
                    new ArrayList<String>(), language, codeBlock, scriptClass);

            // Parse the ALLOW and PARTITION clauses.
            // Populate descriptor roles and returned partition data as needed.
            CreateProcedurePartitionData partitionData =
                    parseCreateProcedureClauses(descriptor, statementMatcher.group(2));

            // track the defined procedure
            String procName = m_tracker.add(descriptor);

            // add partitioning if specified
            addProcedurePartitionInfo(procName, partitionData, statement);

            return true;
        }

        // matches if it is CREATE PROCEDURE <proc-name> [ALLOW <role> ...] [PARTITION ON ...] AS <select-or-dml-statement>
        statementMatcher = SQLParser.matchCreateProcedureAsSQL(statement);
        if (statementMatcher.matches()) {
            String clazz = checkProcedureIdentifier(statementMatcher.group(1), statement);
            String sqlStatement = statementMatcher.group(3) + ";";

            ProcedureDescriptor descriptor = m_compiler.new ProcedureDescriptor(
                    new ArrayList<String>(), clazz, sqlStatement, null, null, false, null, null, null);

            // Parse the ALLOW and PARTITION clauses.
            // Populate descriptor roles and returned partition data as needed.
            CreateProcedurePartitionData partitionData =
                    parseCreateProcedureClauses(descriptor, statementMatcher.group(2));

            m_tracker.add(descriptor);

            // add partitioning if specified
            addProcedurePartitionInfo(clazz, partitionData, statement);

            return true;
        }

        // Matches if it is DROP PROCEDURE <proc-name or classname>
        statementMatcher = SQLParser.matchDropProcedure(statement);
        if (statementMatcher.matches()) {
            String classOrProcName = checkIdentifierStart(statementMatcher.group(1), statement);
            // Extract the ifExists bool from group 2
            m_tracker.removeProcedure(classOrProcName, (statementMatcher.group(2) != null));

            return true;
        }

        // matches if it is the beginning of a partition statement
        statementMatcher = SQLParser.matchPartitionStatementPreamble(statement);
        if (statementMatcher.matches()) {

            // either TABLE or PROCEDURE
            String partitionee = statementMatcher.group(1).toUpperCase();
            if (TABLE.equals(partitionee)) {

                // matches if it is PARTITION TABLE <table> ON COLUMN <column>
                statementMatcher = SQLParser.matchPartitionTable(statement);

                if ( ! statementMatcher.matches()) {
                    throw m_compiler.new VoltCompilerException(String.format(
                            "Invalid PARTITION statement: \"%s\", " +
                            "expected syntax: PARTITION TABLE <table> ON COLUMN <column>",
                            statement.substring(0,statement.length()-1))); // remove trailing semicolon
                }
                // group(1) -> table, group(2) -> column
                String tableName = checkIdentifierStart(statementMatcher.group(1), statement);
                String columnName = checkIdentifierStart(statementMatcher.group(2), statement);
                VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());
                if (tableXML != null) {
                    tableXML.attributes.put("partitioncolumn", columnName.toUpperCase());
                    // Column validity check done by VoltCompiler in post-processing

                    // mark the table as dirty for the purposes of caching sql statements
                    m_compiler.markTableAsDirty(tableName);
                }
                else {
                    throw m_compiler.new VoltCompilerException(String.format(
                                "Invalid PARTITION statement: table %s does not exist", tableName));
                }
                return true;
            }
            else if (PROCEDURE.equals(partitionee)) {
                if (whichProcs != DdlProceduresToLoad.ALL_DDL_PROCEDURES) {
                    return true;
                }
                // matches if it is
                //   PARTITION PROCEDURE <procedure>
                //      ON  TABLE <table> COLUMN <column> [PARAMETER <parameter-index-no>]
                statementMatcher = SQLParser.matchPartitionProcedure(statement);

                if ( ! statementMatcher.matches()) {
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
                if (parameterNo == null) {
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
        statementMatcher = SQLParser.matchReplicateTable(statement);
        if (statementMatcher.matches()) {
            // group(1) -> table
            String tableName = checkIdentifierStart(statementMatcher.group(1), statement);
            VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());
            if (tableXML != null) {
                tableXML.attributes.remove("partitioncolumn");

                // mark the table as dirty for the purposes of caching sql statements
                m_compiler.markTableAsDirty(tableName);
            }
            else {
                throw m_compiler.new VoltCompilerException(String.format(
                            "Invalid REPLICATE statement: table %s does not exist", tableName));
            }
            return true;
        }

        // match IMPORT CLASS statements
        statementMatcher = SQLParser.matchImportClass(statement);
        if (statementMatcher.matches()) {
            if (whichProcs == DdlProceduresToLoad.ALL_DDL_PROCEDURES) {
                // Semi-hacky way of determining if we're doing a cluster-internal compilation.
                // Command-line compilation will never have an InMemoryJarfile.
                if (!(m_classLoader instanceof InMemoryJarfile.JarLoader)) {
                    // Only process the statement if this is not for the StatementPlanner
                    String classNameStr = statementMatcher.group(1);

                    // check that the match pattern is a valid match pattern
                    checkIdentifierWithWildcard(classNameStr, statement);

                    ClassNameMatchStatus matchStatus = m_classMatcher.addPattern(classNameStr);
                    if (matchStatus == ClassNameMatchStatus.NO_EXACT_MATCH) {
                        throw m_compiler.new VoltCompilerException(String.format(
                                    "IMPORT CLASS not found: '%s'",
                                    classNameStr)); // remove trailing semicolon
                    }
                    else if (matchStatus == ClassNameMatchStatus.NO_WILDCARD_MATCH) {
                        m_compiler.addWarn(String.format(
                                    "IMPORT CLASS no match for wildcarded class: '%s'",
                                    classNameStr), ddlStatement.lineNo);
                    }
                }
                else {
                    m_compiler.addInfo("Internal cluster recompilation ignoring IMPORT CLASS line: " +
                            statement);
                }
                // Need to track the IMPORT CLASS lines even on internal compiles so that
                // we don't lose them from the DDL source.  When the @UAC path goes away,
                // we could change this.
                m_tracker.addImportLine(statement);
            }

            return true;
        }

        // matches if it is CREATE ROLE [WITH <permission> [, <permission> ...]]
        // group 1 is role name
        // group 2 is comma-separated permission list or null if there is no WITH clause
        statementMatcher = SQLParser.matchCreateRole(statement);
        if (statementMatcher.matches()) {
            String roleName = statementMatcher.group(1).toLowerCase();
            CatalogMap<Group> groupMap = db.getGroups();
            if (groupMap.get(roleName) != null) {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Role name \"%s\" in CREATE ROLE statement already exists.",
                        roleName));
            }
            Group catGroup = groupMap.add(roleName);
            if (statementMatcher.group(2) != null) {
                try {
                    EnumSet<Permission> permset =
                            Permission.getPermissionsFromAliases(Arrays.asList(StringUtils.split(statementMatcher.group(2), ',')));
                    Permission.setPermissionsInGroup(catGroup, permset);
                } catch (IllegalArgumentException iaex) {
                    throw m_compiler.new VoltCompilerException(String.format(
                            "Invalid permission \"%s\" in CREATE ROLE statement: \"%s\", " +
                                    "available permissions: %s", iaex.getMessage(),
                            statement.substring(0,statement.length()-1), // remove trailing semicolon
                            Permission.toListString()));
                }
            }
            return true;
        }

        // matches if it is DROP ROLE
        // group 1 is role name
        statementMatcher = SQLParser.matchDropRole(statement);
        if (statementMatcher.matches()) {
            String roleName = statementMatcher.group(1).toUpperCase();
            boolean ifExists = (statementMatcher.group(2) != null);
            CatalogMap<Group> groupMap = db.getGroups();
            if (groupMap.get(roleName) == null) {
                if (!ifExists) {
                    throw m_compiler.new VoltCompilerException(String.format(
                                "Role name \"%s\" in DROP ROLE statement does not exist.",
                                roleName));
                }
                else {
                    return true;
                }
            }
            else {
                // Hand-check against the two default roles which shall not be
                // dropped.
                if (roleName.equals("ADMINISTRATOR") || roleName.equals("USER")) {
                    throw m_compiler.new VoltCompilerException(String.format(
                                "You may not drop the built-in role \"%s\".",
                                roleName));
                }
                // The constraint that there be no users with this role gets
                // checked by the deployment validation.  *HOWEVER*, right now
                // this ends up giving a confusing error message.
                groupMap.delete(roleName);
            }
            return true;
        }

        // matches if it is EXPORT TABLE
        statementMatcher = SQLParser.matchExportTable(statement);
        if (statementMatcher.matches()) {

            // check the table portion
            String tableName = checkIdentifierStart(statementMatcher.group(1), statement);

            // group names should be the third group captured
            String targetName = ((statementMatcher.groupCount() > 1) && (statementMatcher.group(2) != null)) ?
                    checkIdentifierStart(statementMatcher.group(2), statement) :
                    Constants.DEFAULT_EXPORT_CONNECTOR_NAME;

            VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());

            if (tableXML != null) {
                guardForGeoColumns(tableXML, tableName, EXPORT);

                if (tableXML.attributes.containsKey("drTable") && tableXML.attributes.get("drTable").equals("ENABLE")) {
                    throw m_compiler.new VoltCompilerException(String.format(
                            "Invalid EXPORT statement: table %s is a DR table.", tableName));
                }
                else {
                    tableXML.attributes.put("export", targetName);
                }
            }
            else {
                throw m_compiler.new VoltCompilerException(String.format(
                            "Invalid EXPORT statement: table %s was not present in the catalog.",
                            tableName));
            }

            return true;
        }

        // matches if it is DR TABLE <table-name> [DISABLE]
        // group 1 -- table name
        // group 2 -- NULL: enable dr
        //            NOT NULL: disable dr
        // TODO: maybe I should write one fit all regex for this.
        statementMatcher = SQLParser.matchDRTable(statement);
        if (statementMatcher.matches()) {
            String tableName;
            if (statementMatcher.group(1).equalsIgnoreCase("*")) {
                tableName = "*";
            } else {
                tableName = checkIdentifierStart(statementMatcher.group(1), statement);
            }

            //System.out.println("\n\n" + m_schema.toString());

            VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());
            if (tableXML != null) {
                guardForGeoColumns(tableXML, tableName, DR);

                if (tableXML.attributes.containsKey("export")) {
                    throw m_compiler.new VoltCompilerException(String.format(
                        "Invalid DR statement: table %s is an export table", tableName));
                }
                else {
                    if ((statementMatcher.group(2) != null)) {
                        tableXML.attributes.put("drTable", "DISABLE");
                    }
                    else {
                        tableXML.attributes.put("drTable", "ENABLE");
                    }
                }
            }
            else {
                throw m_compiler.new VoltCompilerException(String.format(
                        "While configuring dr, table %s was not present in the catalog.", tableName));
            }
            return true;
        }

        statementMatcher = SQLParser.matchSetGlobalParam(statement);
        if (statementMatcher.matches()) {
            String name = statementMatcher.group(1).toUpperCase();
            String value = statementMatcher.group(2).toUpperCase();
            switch (name) {
                case DatabaseConfiguration.DR_MODE_NAME:
                    switch (value) {
                        case DatabaseConfiguration.ACTIVE_ACTIVE: {
                            db.setIsactiveactivedred(true);
                        }
                        break;
                        case DatabaseConfiguration.ACTIVE_PASSIVE:
                        case "DEFAULT": {
                            db.setIsactiveactivedred(false);
                        }
                        break;
                        default: {
                            throw m_compiler.new VoltCompilerException(String.format(
                                    "Invalid parameter value for %s. Candidate values are %s, %s/DEFAULT",
                                    name, DatabaseConfiguration.ACTIVE_ACTIVE, DatabaseConfiguration.ACTIVE_PASSIVE));
                        }
                    }
                    break;
                default:
                    throw m_compiler.new VoltCompilerException(String.format(
                        "Unknown global parameter: %s. Candidate parameters are %s", name, DatabaseConfiguration.allNames));
            }
            return true;
        }

        /*
         * if no correct syntax regex matched above then at this juncture
         * the statement is syntax incorrect
         */

        if (PARTITION.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid PARTITION statement: \"%s\", " +
                    "expected syntax: \"PARTITION TABLE <table> ON COLUMN <column>\" or " +
                    "\"PARTITION PROCEDURE <procedure> ON " +
                    "TABLE <table> COLUMN <column> [PARAMETER <parameter-index-no>]\"",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if (REPLICATE.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid REPLICATE statement: \"%s\", " +
                    "expected syntax: REPLICATE TABLE <table>",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if (PROCEDURE.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE PROCEDURE statement: \"%s\", " +
                    "expected syntax: \"CREATE PROCEDURE [ALLOW <role> [, <role> ...] FROM CLASS <class-name>\" " +
                    "or: \"CREATE PROCEDURE <name> [ALLOW <role> [, <role> ...] AS <single-select-or-dml-statement>\" " +
                    "or: \"CREATE PROCEDURE <proc-name> [ALLOW <role> ...] AS ### <code-block> ### LANGUAGE GROOVY\"",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if (ROLE.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid CREATE ROLE statement: \"%s\", " +
                    "expected syntax: CREATE ROLE <role>",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if (EXPORT.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid EXPORT TABLE statement: \"%s\", " +
                    "expected syntax: EXPORT TABLE <table>",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        if (DR.equals(commandPrefix)) {
            throw m_compiler.new VoltCompilerException(String.format(
                    "Invalid DR TABLE statement: \"%s\", " +
                    "expected syntax: DR TABLE <table> [DISABLE]",
                    statement.substring(0,statement.length()-1))); // remove trailing semicolon
        }

        // Not a VoltDB-specific DDL statement.
        return false;
    }

    private class CreateProcedurePartitionData {
        String tableName = null;
        String columnName = null;
        String parameterNo = null;
    }

    /**
     * Parse and validate the substring containing ALLOW and PARTITION
     * clauses for CREATE PROCEDURE.
     * @param clauses  the substring to parse
     * @param descriptor  procedure descriptor populated with role names from ALLOW clause
     * @return  parsed and validated partition data or null if there was no PARTITION clause
     * @throws VoltCompilerException
     */
    private CreateProcedurePartitionData parseCreateProcedureClauses(
            ProcedureDescriptor descriptor,
            String clauses) throws VoltCompilerException {

        // Nothing to do if there were no clauses.
        // Null means there's no partition data to return.
        // There's also no roles to add.
        if (clauses == null || clauses.isEmpty()) {
            return null;
        }
        CreateProcedurePartitionData data = null;

        Matcher matcher = SQLParser.matchAnyCreateProcedureStatementClause(clauses);
        int start = 0;
        while (matcher.find(start)) {
            start = matcher.end();

            if (matcher.group(1) != null) {
                // Add roles if it's an ALLOW clause. More that one ALLOW clause is okay.
                for (String roleName : StringUtils.split(matcher.group(1), ',')) {
                    // Don't put the same role in the list more than once.
                   String roleNameFixed = roleName.trim().toLowerCase();
                    if (!descriptor.m_authGroups.contains(roleNameFixed)) {
                        descriptor.m_authGroups.add(roleNameFixed);
                    }
                }
            }
            else {
                // Add partition info if it's a PARTITION clause. Only one is allowed.
                if (data != null) {
                    throw m_compiler.new VoltCompilerException(
                        "Only one PARTITION clause is allowed for CREATE PROCEDURE.");
                }
                data = new CreateProcedurePartitionData();
                data.tableName = matcher.group(2);
                data.columnName = matcher.group(3);
                data.parameterNo = matcher.group(4);
            }
        }

        return data;
    }

    private void addProcedurePartitionInfo(
            String procName,
            CreateProcedurePartitionData data,
            String statement) throws VoltCompilerException {

        assert(procName != null);

        // Will be null when there is no optional partition clause.
        if (data == null) {
            return;
        }

        assert(data.tableName != null);
        assert(data.columnName != null);

        // Check the identifiers.
        checkIdentifierStart(procName, statement);
        checkIdentifierStart(data.tableName, statement);
        checkIdentifierStart(data.columnName, statement);

        // if not specified default parameter index to 0
        if (data.parameterNo == null) {
            data.parameterNo = "0";
        }

        String partitionInfo = String.format("%s.%s: %s", data.tableName, data.columnName, data.parameterNo);

        m_tracker.addProcedurePartitionInfoTo(procName, partitionInfo);
    }

    void compileToCatalog(Database db) throws VoltCompilerException {
        // note this will need to be decompressed to be used
        String binDDL = Encoder.compressAndBase64Encode(m_fullDDL);
        db.setSchema(binDDL);

        // output the xml catalog to disk
        //* enable to debug */ System.out.println("DEBUG: " + m_schema);
        BuildDirectoryUtils.writeFile("schema-xml", "hsql-catalog-output.xml", m_schema.toString(), true);

        // build the local catalog from the xml catalog
        for (VoltXMLElement node : m_schema.children) {
            if (node.name.equals("table")) {
                addTableToCatalog(db, node);
            }
        }

        processMaterializedViews(db);
        fillTrackerFromXML();
    }

    // Fill the table stuff in VoltDDLElementTracker from the VoltXMLElement tree at the end when
    // requested from the compiler
    private void fillTrackerFromXML()
    {
        for (VoltXMLElement e : m_schema.children) {
            if (e.name.equals("table")) {
                String tableName = e.attributes.get("name");
                String partitionCol = e.attributes.get("partitioncolumn");
                String export = e.attributes.get("export");
                String drTable = e.attributes.get("drTable");
                if (partitionCol != null) {
                    m_tracker.addPartition(tableName, partitionCol);
                }
                else {
                    m_tracker.removePartition(tableName);
                }
                if (export != null) {
                    m_tracker.addExportedTable(tableName, export);
                }
                else {
                    m_tracker.removeExportedTable(tableName);
                }
                if (drTable != null) {
                    m_tracker.addDRedTable(tableName, drTable);
                }
            }
        }
    }

    // Parsing states. Start in kStateInvalid
    private static int kStateInvalid = 0;                         // have not yet found start of statement
    private static int kStateReading = 1;                         // normal reading state
    private static int kStateReadingCommentDelim = 2;             // dealing with first -
    private static int kStateReadingComment = 3;                  // parsing after -- for a newline
    private static int kStateReadingStringLiteralSpecialChar = 4; // dealing with one or more single quotes
    private static int kStateReadingStringLiteral = 5;            // in the middle of a string literal
    private static int kStateCompleteStatement = 6;               // found end of statement
    private static int kStateReadingCodeBlockDelim = 7 ;          // dealing with code block delimiter ###
    private static int kStateReadingCodeBlockNextDelim = 8;       // dealing with code block delimiter ###
    private static int kStateReadingCodeBlock = 9;                // reading code block
    private static int kStateReadingEndCodeBlockDelim = 10 ;      // dealing with ending code block delimiter ###
    private static int kStateReadingEndCodeBlockNextDelim = 11;   // dealing with ending code block delimiter ###


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
        else if (SQLLexer.isBlockDelimiter(nchar[0])) {
            // we may be examining ### code block delimiters
            retval.statement += nchar[0];
            return kStateReadingCodeBlockDelim;
        }
        else {
            // accumulate and continue
            retval.statement += nchar[0];
        }

        return kStateReading;
    }

    private int readingCodeBlockStateDelim(char [] nchar, DDLStatement retval) {
        retval.statement += nchar[0];
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            return kStateReadingCodeBlockNextDelim;
        } else {
            return readingState(nchar, retval);
        }
    }

    private int readingEndCodeBlockStateDelim(char [] nchar, DDLStatement retval) {
        retval.statement += nchar[0];
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            return kStateReadingEndCodeBlockNextDelim;
        } else {
            return kStateReadingCodeBlock;
        }
    }

    private int readingCodeBlockStateNextDelim(char [] nchar, DDLStatement retval) {
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            retval.statement += nchar[0];
            return kStateReadingCodeBlock;
        }
        return readingState(nchar, retval);
    }

    private int readingEndCodeBlockStateNextDelim(char [] nchar, DDLStatement retval) {
        retval.statement += nchar[0];
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            return kStateReading;
        }
        return kStateReadingCodeBlock;
    }

    private int readingCodeBlock(char [] nchar, DDLStatement retval) {
        // all characters in the literal are accumulated. keep track of
        // newlines for error messages.
        retval.statement += nchar[0];
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            return kStateReadingEndCodeBlockDelim;
        }

        if (nchar[0] == '\n') {
            m_currLineNo += 1;
        }
        return kStateReadingCodeBlock;
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

    private DDLStatement getNextStatement(Reader reader, VoltCompiler compiler)
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
                else if (state == kStateReadingCodeBlockDelim) {
                    state = readingCodeBlockStateDelim(nchar, retval);
                }
                else if (state == kStateReadingCodeBlockNextDelim) {
                    state = readingCodeBlockStateNextDelim(nchar, retval);
                }
                else if (state == kStateReadingCodeBlock) {
                    state = readingCodeBlock(nchar, retval);
                }
                else if (state == kStateReadingEndCodeBlockDelim) {
                    state = readingEndCodeBlockStateDelim(nchar, retval);
                }
                else if (state == kStateReadingEndCodeBlockNextDelim) {
                    state = readingEndCodeBlockStateNextDelim(nchar, retval);
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

    private void addTableToCatalog(Database db, VoltXMLElement node)
            throws VoltCompilerException {
        assert node.name.equals("table");

        // Construct table-specific maps
        HashMap<String, Column> columnMap = new HashMap<String, Column>();
        HashMap<String, Index> indexMap = new HashMap<String, Index>();

        String name = node.attributes.get("name");

        // create a table node in the catalog
        Table table = db.getTables().add(name);
        // set max value before return for view table
        table.setTuplelimit(Integer.MAX_VALUE);

        // add the original DDL to the table (or null if it's not there)
        TableAnnotation annotation = new TableAnnotation();
        table.setAnnotation(annotation);

        // handle the case where this is a materialized view
        String query = node.attributes.get("query");
        if (query != null) {
            assert(query.length() > 0);
            m_matViewMap.put(table, query);
        }

        // all tables start replicated
        // if a partition is found in the project file later,
        //  then this is reversed
        table.setIsreplicated(true);

        // map of index replacements for later constraint fixup
        Map<String, String> indexReplacementMap = new TreeMap<String, String>();

        // Need the columnTypes sorted by column index.
        SortedMap<Integer, VoltType> columnTypes = new TreeMap<Integer, VoltType>();
        for (VoltXMLElement subNode : node.children) {

            if (subNode.name.equals("columns")) {
                int colIndex = 0;
                for (VoltXMLElement columnNode : subNode.children) {
                    if (columnNode.name.equals("column")) {
                        addColumnToCatalog(table, columnNode, columnTypes,
                                columnMap, m_compiler);
                        colIndex++;
                    }
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
                    if (indexName.startsWith(HSQLInterface.AUTO_GEN_IDX_PREFIX) == false) {
                        addIndexToCatalog(db, table, indexNode, indexReplacementMap,
                                indexMap, columnMap, m_compiler);
                    }
                }

                // now do system indexes
                for (VoltXMLElement indexNode : subNode.children) {
                    if (indexNode.name.equals("index") == false) continue;
                    String indexName = indexNode.attributes.get("name");
                    if (indexName.startsWith(HSQLInterface.AUTO_GEN_IDX_PREFIX) == true) {
                        addIndexToCatalog(db, table, indexNode, indexReplacementMap,
                                indexMap, columnMap, m_compiler);
                    }
                }
            }

            if (subNode.name.equals("constraints")) {
                for (VoltXMLElement constraintNode : subNode.children) {
                    if (constraintNode.name.equals("constraint")) {
                        addConstraintToCatalog(table, constraintNode,
                                indexReplacementMap, indexMap);
                    }
                }
            }
        }

        // Warn user if DR table don't have any unique index.
        if (db.getIsactiveactivedred() &&
                node.attributes.get("drTable") != null &&
                node.attributes.get("drTable").equalsIgnoreCase("ENABLE")) {
            boolean hasUniqueIndex = false;
            for (Index index : table.getIndexes()) {
                if (index.getUnique()) {
                    hasUniqueIndex = true;
                    break;
                }
            }
            if (!hasUniqueIndex) {
                String info = String.format("Table %s doesn't have any unique index, it will cause full table scans to update/delete DR record and may become slower as table grow.", table.getTypeName());
                m_compiler.addWarn(info);
            }
        }

        table.setSignature(CatalogUtil.getSignatureForTable(name, columnTypes));

        /*
         * Validate that the total size
         */
        int maxRowSize = 0;
        for (Column c : columnMap.values()) {
            VoltType t = VoltType.get((byte)c.getType());
            if (t.isVariableLength()) {
                if (c.getSize() > VoltType.MAX_VALUE_LENGTH) {
                    throw m_compiler.new VoltCompilerException("Column " + name + "." + c.getName() +
                            " specifies a maximum size of " + c.getSize() + " bytes" +
                            " but the maximum supported size is " + VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH));
                }
                maxRowSize += 4 + c.getSize();
            }
            else if (t == VoltType.STRING) {
                if (c.getSize() * MAX_BYTES_PER_UTF8_CHARACTER > VoltType.MAX_VALUE_LENGTH) {
                    throw m_compiler.new VoltCompilerException("Column " + name + "." + c.getName() +
                            " specifies a maximum size of " + c.getSize() + " characters" +
                            " but the maximum supported size is " +
                            VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH / MAX_BYTES_PER_UTF8_CHARACTER) +
                            " characters or " + VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH) + " bytes");
                }
                maxRowSize += 4 + c.getSize() * MAX_BYTES_PER_UTF8_CHARACTER;
            }
            else {
                maxRowSize += t.getLengthInBytesForFixedTypes();
            }
        }
        // Temporarily assign the view Query to the annotation so we can use when we build
        // the DDL statement for the VIEW
        if (query != null) {
            annotation.ddl = query;
        } else {
            // Get the final DDL for the table rebuilt from the catalog object
            // Don't need a real StringBuilder or export state to get the CREATE for a table
            annotation.ddl = CatalogSchemaTools.toSchema(new StringBuilder(),
                    table, query, null);
        }

        if (maxRowSize > MAX_ROW_SIZE) {
            throw m_compiler.new VoltCompilerException("Table name " + name + " has a maximum row size of " + maxRowSize +
                    " but the maximum supported row size is " + MAX_ROW_SIZE);
        }
    }

    private static void addColumnToCatalog(Table table,
                            VoltXMLElement node,
                            SortedMap<Integer, VoltType> columnTypes,
                            Map<String, Column> columnMap,
                            VoltCompiler compiler) throws VoltCompilerException
    {
        assert node.name.equals("column");

        String name = node.attributes.get("name");
        String typename = node.attributes.get("valuetype");
        String nullable = node.attributes.get("nullable");
        String sizeString = node.attributes.get("size");
        int index = Integer.valueOf(node.attributes.get("index"));
        String defaultvalue = null;
        String defaulttype = null;

        int defaultFuncID = -1;

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
                    } else if (inner_child.name.equals("function")) {
                        assert(defaulttype == null); // There should be only one default value/type.
                        defaultFuncID = Integer.parseInt(inner_child.attributes.get("function_id"));
                        defaultvalue = inner_child.attributes.get("name");
                        defaulttype = inner_child.attributes.get("valuetype");
                        assert(defaulttype != null);
                    }
                }
            }
        }
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
        columnTypes.put(index, type);
        if (defaultFuncID == -1) {
            if (defaultvalue != null && (type == VoltType.DECIMAL || type == VoltType.NUMERIC)) {
                // Until we support deserializing scientific notation in the EE, we'll
                // coerce default values to plain notation here.  See ENG-952 for more info.
                BigDecimal temp = new BigDecimal(defaultvalue);
                defaultvalue = temp.toPlainString();
            }
        } else {
            // Concat function name and function id, format: NAME:ID
            // Used by PlanAssembler:getNextInsertPlan().
            defaultvalue = defaultvalue + ":" + String.valueOf(defaultFuncID);
        }

        Column column = table.getColumns().add(name);
        // need to set other column data here (default, nullable, etc)
        column.setName(name);
        column.setIndex(index);
        column.setType(type.getValue());
        column.setNullable(Boolean.valueOf(nullable));
        int size = type.getMaxLengthInBytes();

        boolean inBytes = false;
        if (node.attributes.containsKey("bytes")) {
            inBytes = Boolean.valueOf(node.attributes.get("bytes"));
        }

        // Determine the length of columns with a variable-length type
        if (type.isVariableLength()) {
            int userSpecifiedSize = 0;
            if (sizeString != null) {
                userSpecifiedSize = Integer.parseInt(sizeString);
            }

            if (userSpecifiedSize == 0) {
                // So size specified in the column definition.  Either:
                // - the user-specified size is zero (unclear how this would happen---
                //   if someone types VARCHAR(0) HSQL will complain)
                // - or the sizeString was null, meaning that the size specifier was
                //   omitted.
                // Choose an appropriate default for the type.
                size = type.defaultLengthForVariableLengthType();
            }
            else {
                if (userSpecifiedSize < 0 || (inBytes && userSpecifiedSize > VoltType.MAX_VALUE_LENGTH)) {
                    String msg = type.toSQLString() + " column " + name +
                            " in table " + table.getTypeName() + " has unsupported length " + sizeString;
                    throw compiler.new VoltCompilerException(msg);
                }

                if (!inBytes && type == VoltType.STRING) {
                    if (userSpecifiedSize > VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS) {
                        String msg = String.format("The size of VARCHAR column %s in table %s greater than %d " +
                                "will be enforced as byte counts rather than UTF8 character counts. " +
                                "To eliminate this warning, specify \"VARCHAR(%d BYTES)\"",
                                name, table.getTypeName(),
                                VoltType.MAX_VALUE_LENGTH_IN_CHARACTERS, userSpecifiedSize);
                        compiler.addWarn(msg);
                        inBytes = true;
                    }
                }

                if (userSpecifiedSize < type.getMinLengthInBytes()) {
                    String msg = type.toSQLString() + " column " + name +
                            " in table " + table.getTypeName() + " has length of " + sizeString
                            + " which is shorter than " + type.getMinLengthInBytes() + ", "
                            + "the minimum allowed length for the type.";
                    throw compiler.new VoltCompilerException(msg);
                }


                size = userSpecifiedSize;
            }
        }

        column.setInbytes(inBytes);
        column.setSize(size);

        column.setDefaultvalue(defaultvalue);
        if (defaulttype != null)
            column.setDefaulttype(Integer.parseInt(defaulttype));

        columnMap.put(name, column);
    }

    /**
     * Return true if the two indexes are identical with a different name.
     */
    private static boolean indexesAreDups(Index idx1, Index idx2) {
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
        if (idx1.getAssumeunique() != idx2.getAssumeunique()) {
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
        if ( ! Arrays.equals(idx1baseTableOrder, idx2baseTableOrder) ) {
            return false;
        }

        // Check the predicates
        if (idx1.getPredicatejson().length() > 0) {
            return idx1.getPredicatejson().equals(idx2.getPredicatejson());
        }
        if (idx2.getPredicatejson().length() > 0) {
            return idx2.getPredicatejson().equals(idx1.getPredicatejson());
        }
        return true;
    }

    private static void addIndexToCatalog(Database db,
            Table table,
            VoltXMLElement node,
            Map<String, String> indexReplacementMap,
            HashMap<String, Index> indexMap,
            HashMap<String, Column> columnMap,
            VoltCompiler compiler)
            throws VoltCompilerException
    {
        assert node.name.equals("index");

        String name = node.attributes.get("name");
        boolean unique = Boolean.parseBoolean(node.attributes.get("unique"));
        boolean assumeUnique = Boolean.parseBoolean(node.attributes.get("assumeunique"));

        AbstractParsedStmt dummy = new ParsedSelectStmt(null, db);
        dummy.setTable(table);
        StringBuffer msg = new StringBuffer(String.format("Index \"%s\" ", name));
        // "parse" the expression trees for an expression-based index (vs. a simple column value index)
        List<AbstractExpression> exprs = null;
        // "parse" the WHERE expression for partial index if any
        AbstractExpression predicate = null;
        // Some expressions have special validation in indices.  Not all the expression
        // can be indexed. We scan for result type at first here and block those which
        // can't be indexed like boolean, geo ... We gather rest of expression into
        // checkExpressions list.  We will check on them all at once.
        List<AbstractExpression> checkExpressions = new ArrayList<AbstractExpression>();
        for (VoltXMLElement subNode : node.children) {
            if (subNode.name.equals("exprs")) {
                exprs = new ArrayList<AbstractExpression>();
                for (VoltXMLElement exprNode : subNode.children) {
                    AbstractExpression expr = dummy.parseExpressionTree(exprNode);
                    expr.resolveForTable(table);
                    expr.finalizeValueTypes();
                    // string will will be populated expression details whose value
                    // type are not indexable
                    StringBuffer exprMsg = new StringBuffer();
                    if (!expr.isValueTypeIndexable(exprMsg)) {
                        // indexing on expression with boolean result is not supported.
                        throw compiler.new VoltCompilerException("Cannot create index \""+ name +
                                "\" because it contains " + exprMsg + ", which is not supported.");
                    }
                    // rest of the validity gaurds will be evaluated after collecting all the expressions.
                    checkExpressions.add(expr);
                    exprs.add(expr);
                }
            }
            else if (subNode.name.equals("predicate")) {
                assert(subNode.children.size() == 1);
                VoltXMLElement predicateXML = subNode.children.get(0);
                assert(predicateXML != null);
                predicate = buildPartialIndexPredicate(dummy, name,
                        predicateXML, table, compiler);
            }
        }

        // Check all the subexpressions we gathered up.
        if (!AbstractExpression.validateExprsForIndexesAndMVs(checkExpressions, msg)) {
            // The error message will be in the StringBuffer msg.
            throw compiler.new VoltCompilerException(msg.toString());
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

                if (! colType.isIndexable()) {
                    String emsg = colType.getName() + " values are not currently supported as index keys: '" + colNames[i] + "'";
                    throw compiler.new VoltCompilerException(emsg);
                }

                if (! colType.isBackendIntegerType()) {
                    has_nonint_col = true;
                    nonint_col_name = colNames[i];
                }
            }
        }
        else {
            for (AbstractExpression expression : exprs) {
                VoltType colType = expression.getValueType();

                if (! colType.isBackendIntegerType()) {
                    has_nonint_col = true;
                    nonint_col_name = "<expression>";
                }
            }
        }

        Index index = table.getIndexes().add(name);
        index.setCountable(false);

        String indexNameNoCase = name.toLowerCase();
        // The index is a hash iff:
        //   1. it does not have "tree" in the name, and
        //   2. it does have "hash" in the name, and
        //   3. it does not have an autogenerated name.
        // We don't think about the column type here, but see
        // below.
        boolean makeHashable = ( ! indexNameNoCase.contains("tree") ) && indexNameNoCase.contains("hash") &&
                 ! indexNameNoCase.startsWith(HSQLInterface.AUTO_GEN_PRIMARY_KEY_PREFIX.toLowerCase());

        if (makeHashable) {
            // If the column type is not an integer, we cannot
            // make the index a hash.
            if (has_nonint_col) {
                String emsg = "Index " + name + " in table " + table.getTypeName() +
                             " uses a non-hashable column " + nonint_col_name;
                throw compiler.new VoltCompilerException(emsg);
            }
            index.setType(IndexType.HASH_TABLE.getValue());
        }
        else {
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
                throw compiler.new VoltCompilerException("Unexpected error serializing non-column expressions for index '" +
                                                           name + "' on type '" + table.getTypeName() + "': " + e.toString());
            }
        }

        index.setUnique(unique);
        if (assumeUnique) {
            index.setUnique(true);
        }
        index.setAssumeunique(assumeUnique);

        if (predicate != null) {
            try {
                index.setPredicatejson(convertToJSONObject(predicate));
            } catch (JSONException e) {
                throw compiler.new VoltCompilerException("Unexpected error serializing predicate for partial index '" +
                        name + "' on type '" + table.getTypeName() + "': " + e.toString());
            }
        }

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
                if (index.getTypeName().startsWith(HSQLInterface.AUTO_GEN_PREFIX) == false) {
                    // on dup-detection, add a warning but don't fail
                    String emsg = String.format("Dropping index %s on table %s because it duplicates index %s.",
                            index.getTypeName(), table.getTypeName(), existingIndex.getTypeName());
                    compiler.addWarn(emsg);
                }

                // drop the index and GTFO
                table.getIndexes().delete(index.getTypeName());
                return;
            }
        }

        String smsg = "Created index: " + name + " on table: " +
                   table.getTypeName() + " of type: " + IndexType.get(index.getType()).name();

        compiler.addInfo(smsg);

        indexMap.put(name, index);
    }

    private static String convertToJSONArray(List<AbstractExpression> exprs) throws JSONException {
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

    private static String convertToJSONObject(AbstractExpression expr) throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        expr.toJSONString(stringer);
        stringer.endObject();
        return stringer.toString();
    }

    /** Makes sure that the DELETE statement on a LIMIT PARTITION ROWS EXECUTE (DELETE ...)
     * - Contains no parse errors
     * - Is actually a DELETE statement
     * - Targets the table being constrained
     * Throws VoltCompilerException if any of these does not hold
     * @param catStmt     The catalog statement whose sql text field is the DELETE to be validated
     **/
    private void validateTupleLimitDeleteStmt(Statement catStmt) throws VoltCompilerException {
        String tableName = catStmt.getParent().getTypeName();
        String msgPrefix = "Error: Table " + tableName + " has invalid DELETE statement for LIMIT PARTITION ROWS constraint: ";
        VoltXMLElement deleteXml = null;
        try {
            // We parse the statement here and cache the XML below if the statement passes
            // validation.
            deleteXml = m_hsql.getXMLCompiledStatement(catStmt.getSqltext());
        }
        catch (HSQLInterface.HSQLParseException e) {
            throw m_compiler.new VoltCompilerException(msgPrefix + "parse error: " + e.getMessage());
        }

        if (! deleteXml.name.equals("delete")) {
            // Could in theory allow TRUNCATE TABLE here too.
            throw m_compiler.new VoltCompilerException(msgPrefix + "not a DELETE statement");
        }

        String deleteTarget = deleteXml.attributes.get("table");
        if (! deleteTarget.equals(tableName)) {
            throw m_compiler.new VoltCompilerException(msgPrefix + "target of DELETE must be " + tableName);
        }

        m_limitDeleteStmtToXml.put(catStmt, deleteXml);
    }

    /** Accessor */
    Collection<Map.Entry<Statement, VoltXMLElement>> getLimitDeleteStmtToXmlEntries() {
        return Collections.unmodifiableCollection(m_limitDeleteStmtToXml.entrySet());
    }

    /**
     * Add a constraint on a given table to the catalog
     * @param table                The table on which the constraint will be enforced
     * @param node                 The XML node representing the constraint
     * @param indexReplacementMap
     * @throws VoltCompilerException
     */
    private void addConstraintToCatalog(Table table,
            VoltXMLElement node,
            Map<String, String> indexReplacementMap,
            Map<String, Index> indexMap)
            throws VoltCompilerException
    {
        assert node.name.equals("constraint");

        String name = node.attributes.get("name");
        String typeName = node.attributes.get("constrainttype");
        ConstraintType type = ConstraintType.valueOf(typeName);
        String tableName = table.getTypeName();

        if (type == ConstraintType.LIMIT) {
            int tupleLimit = Integer.parseInt(node.attributes.get("rowslimit"));
            if (tupleLimit < 0) {
                throw m_compiler.new VoltCompilerException("Invalid constraint limit number '" + tupleLimit + "'");
            }
            if (tableLimitConstraintCounter.contains(tableName)) {
                throw m_compiler.new VoltCompilerException("Too many table limit constraints for table " + tableName);
            } else {
                tableLimitConstraintCounter.add(tableName);
            }

            table.setTuplelimit(tupleLimit);
            String deleteStmt = node.attributes.get("rowslimitdeletestmt");
            if (deleteStmt != null) {
                Statement catStmt = table.getTuplelimitdeletestmt().add("limit_delete");
                catStmt.setSqltext(deleteStmt);
                validateTupleLimitDeleteStmt(catStmt);
            }
            return;
        }

        if (type == ConstraintType.CHECK) {
            String msg = "VoltDB does not enforce check constraints. ";
            msg += "Constraint on table " + tableName + " will be ignored.";
            m_compiler.addWarn(msg);
            return;
        }
        else if (type == ConstraintType.FOREIGN_KEY) {
            String msg = "VoltDB does not enforce foreign key references and constraints. ";
            msg += "Constraint on table " + tableName + " will be ignored.";
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

        // Attach the index to the catalog constraint (catalog_const).
        if (catalog_index != null) {
            catalog_const.setIndex(catalog_index);
            // This may be redundant.
            catalog_index.setUnique(true);

            boolean assumeUnique = Boolean.parseBoolean(node.attributes.get("assumeunique"));
            catalog_index.setAssumeunique(assumeUnique);
        }
        catalog_const.setType(type.getValue());
    }

    // Compile the fallback query XMLs, add the plans into the catalog statement (ENG-8641).
    private void compileFallbackQueriesAndUpdateCatalog(Database db,
                                                List<VoltXMLElement> fallbackQueryXMLs,
                                                MaterializedViewInfo matviewinfo) throws VoltCompilerException {
        DatabaseEstimates estimates = new DatabaseEstimates();
        for (int i=0; i<fallbackQueryXMLs.size(); ++i) {
            String key = String.valueOf(i);
            Statement fallbackQueryStmt = matviewinfo.getFallbackquerystmts().add(key);
            VoltXMLElement fallbackQueryXML = fallbackQueryXMLs.get(i);
            // Use the uniqueName as the sqlText. This is easier for differentiating the queries?
            // Normally for select statements, the unique names will start with "Eselect".
            // Remove the first "E" to let QueryType.getFromSQL(stmt) generate correct query type value.
            // (StatementCompiler.java, line 159)
            fallbackQueryStmt.setSqltext( fallbackQueryXML.getUniqueName().substring(2) );
            // For debug:
            // System.out.println(fallbackQueryXML.toString());
            StatementCompiler.compileStatementAndUpdateCatalog(m_compiler,
                              m_hsql,
                              db.getCatalog(),
                              db,
                              estimates,
                              fallbackQueryStmt,
                              fallbackQueryXML,
                              fallbackQueryStmt.getSqltext(),
                              null, // no user-supplied join order
                              DeterminismMode.FASTER,
                              StatementPartitioning.forceSP());
        }
    }

    /**
     * Add materialized view info to the catalog for the tables that are
     * materialized views.
     */
    private void processMaterializedViews(Database db) throws VoltCompilerException {
        HashSet <String> viewTableNames = new HashSet<>();
        for (Entry<Table, String> entry : m_matViewMap.entrySet()) {
            viewTableNames.add(entry.getKey().getTypeName());
        }


        for (Entry<Table, String> entry : m_matViewMap.entrySet()) {
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
            checkViewMeetsSpec(viewName, stmt, m_compiler);

            // Allow only non-unique indexes other than the primary key index.
            // The primary key index is yet to be defined (below).
            for (Index destIndex : destTable.getIndexes()) {
                if (destIndex.getUnique() || destIndex.getAssumeunique()) {
                    String msg = "A UNIQUE or ASSUMEUNIQUE index is not allowed on a materialized view. " +
                            "Remove the qualifier from the index " + destIndex.getTypeName() +
                            "defined on the materialized view \"" + viewName + "\".";
                    throw m_compiler.new VoltCompilerException(msg);
                }
            }

            // create the materializedviewinfo catalog node for the source table
            Table srcTable = stmt.m_tableList.get(0);
            if (viewTableNames.contains(srcTable.getTypeName())) {
                String msg = String.format("A materialized view (%s) can not be defined on another view (%s).",
                        viewName, srcTable.getTypeName());
                throw m_compiler.new VoltCompilerException(msg);
            }

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
            List<AbstractExpression> groupbyExprs = null;

            if (stmt.hasComplexGroupby()) {
                groupbyExprs = new ArrayList<AbstractExpression>();
                for (ParsedColInfo col: stmt.m_groupByColumns) {
                    groupbyExprs.add(col.expression);
                }
                // Parse group by expressions to json string
                String groupbyExprsJson = null;
                try {
                    groupbyExprsJson = convertToJSONArray(groupbyExprs);
                } catch (JSONException e) {
                    throw m_compiler.new VoltCompilerException ("Unexpected error serializing non-column " +
                            "expressions for group by expressions: " + e.toString());
                }
                matviewinfo.setGroupbyexpressionsjson(groupbyExprsJson);

            } else {
                // add the group by columns from the src table
                for (int i = 0; i < stmt.m_groupByColumns.size(); i++) {
                    ParsedColInfo gbcol = stmt.m_groupByColumns.get(i);
                    Column srcCol = srcColumnArray.get(gbcol.index);
                    ColumnRef cref = matviewinfo.getGroupbycols().add(srcCol.getTypeName());
                    // groupByColumns is iterating in order of groups. Store that grouping order
                    // in the column ref index. When the catalog is serialized, it will, naturally,
                    // scramble this order like a two year playing dominos, presenting the data
                    // in a meaningless sequence.
                    cref.setIndex(i);           // the column offset in the view's grouping order
                    cref.setColumn(srcCol);     // the source column from the base (non-view) table
                }

                // parse out the group by columns into the dest table
                for (int i = 0; i < stmt.m_groupByColumns.size(); i++) {
                    ParsedColInfo col = stmt.m_displayColumns.get(i);
                    Column destColumn = destColumnArray.get(i);
                    processMaterializedViewColumn(matviewinfo, srcTable, destColumn,
                            ExpressionType.VALUE_TUPLE, (TupleValueExpression)col.expression);
                }
            }

            // Set up COUNT(*) column
            ParsedColInfo countCol = stmt.m_displayColumns.get(stmt.m_groupByColumns.size());
            assert(countCol.expression.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR);
            assert(countCol.expression.getLeft() == null);
            processMaterializedViewColumn(matviewinfo, srcTable,
                    destColumnArray.get(stmt.m_groupByColumns.size()),
                    ExpressionType.AGGREGATE_COUNT_STAR, null);

            // create an index and constraint for the table
            // After ENG-7872 is fixed if there is no group by column then we will not create any
            // index or constraint in order to avoid error and crash.
            if (stmt.m_groupByColumns.size() != 0) {
                Index pkIndex = destTable.getIndexes().add(HSQLInterface.AUTO_GEN_MATVIEW_IDX);
                pkIndex.setType(IndexType.BALANCED_TREE.getValue());
                pkIndex.setUnique(true);
                // add the group by columns from the src table
                // assume index 1 throuh #grpByCols + 1 are the cols
                for (int i = 0; i < stmt.m_groupByColumns.size(); i++) {
                    ColumnRef c = pkIndex.getColumns().add(String.valueOf(i));
                    c.setColumn(destColumnArray.get(i));
                    c.setIndex(i);
                }
                Constraint pkConstraint = destTable.getConstraints().add(HSQLInterface.AUTO_GEN_MATVIEW_CONST);
                pkConstraint.setType(ConstraintType.PRIMARY_KEY.getValue());
                pkConstraint.setIndex(pkIndex);
            }

            // prepare info for aggregation columns.
            List<AbstractExpression> aggregationExprs = new ArrayList<AbstractExpression>();
            boolean hasAggregationExprs = false;
            boolean hasMinOrMaxAgg = false;
            ArrayList<AbstractExpression> minMaxAggs = new ArrayList<AbstractExpression>();
            for (int i = stmt.m_groupByColumns.size() + 1; i < stmt.m_displayColumns.size(); i++) {
                ParsedColInfo col = stmt.m_displayColumns.get(i);
                AbstractExpression aggExpr = col.expression.getLeft();
                if (aggExpr.getExpressionType() != ExpressionType.VALUE_TUPLE) {
                    hasAggregationExprs = true;
                }
                aggregationExprs.add(aggExpr);
                if (col.expression.getExpressionType() ==  ExpressionType.AGGREGATE_MIN ||
                        col.expression.getExpressionType() == ExpressionType.AGGREGATE_MAX) {
                    hasMinOrMaxAgg = true;
                    minMaxAggs.add(aggExpr);
                }
            }

            // Generate query XMLs for min/max recalculation (ENG-8641)
            MatViewFallbackQueryXMLGenerator xmlGen = new MatViewFallbackQueryXMLGenerator(xmlquery, stmt.m_groupByColumns, stmt.m_displayColumns);
            List<VoltXMLElement> fallbackQueryXMLs = xmlGen.getFallbackQueryXMLs();
            compileFallbackQueriesAndUpdateCatalog(db, fallbackQueryXMLs, matviewinfo);

            // set Aggregation Expressions.
            if (hasAggregationExprs) {
                String aggregationExprsJson = null;
                try {
                    aggregationExprsJson = convertToJSONArray(aggregationExprs);
                } catch (JSONException e) {
                    throw m_compiler.new VoltCompilerException ("Unexpected error serializing non-column " +
                            "expressions for aggregation expressions: " + e.toString());
                }
                matviewinfo.setAggregationexpressionsjson(aggregationExprsJson);
            }

            if (hasMinOrMaxAgg) {
                // Find index for each min/max aggCol/aggExpr (ENG-6511 and ENG-8512)
                boolean needsWarning = false;
                for (Integer i=0; i<minMaxAggs.size(); ++i) {
                    Index found = findBestMatchIndexForMatviewMinOrMax(matviewinfo, srcTable, groupbyExprs, minMaxAggs.get(i));
                    IndexRef refFound = matviewinfo.getIndexforminmax().add(i.toString());
                    if (found != null) {
                        refFound.setName(found.getTypeName());
                    } else {
                        refFound.setName("");
                        needsWarning = true;
                    }
                }
                if (needsWarning) {
                    m_compiler.addWarn("No index found to support UPDATE and DELETE on some of the min() / max() columns in the Materialized View " +
                            matviewinfo.getTypeName() +
                            ", and a sequential scan might be issued when current min / max value is updated / deleted.");
                }
            }

            // parse out the aggregation columns into the dest table
            for (int i = stmt.m_groupByColumns.size() + 1; i < stmt.m_displayColumns.size(); i++) {
                ParsedColInfo col = stmt.m_displayColumns.get(i);
                Column destColumn = destColumnArray.get(i);

                AbstractExpression colExpr = col.expression.getLeft();
                TupleValueExpression tve = null;
                if (colExpr.getExpressionType() == ExpressionType.VALUE_TUPLE) {
                    tve = (TupleValueExpression)colExpr;
                }
                processMaterializedViewColumn(matviewinfo, srcTable, destColumn,
                        col.expression.getExpressionType(), tve);

                // Correctly set the type of the column so that it's consistent.
                // Otherwise HSQLDB might promote types differently than Volt.
                destColumn.setType(col.expression.getValueType().getValue());
            }
        }
    }

    private enum MatViewIndexMatchingGroupby {GB_COL_IDX_COL, GB_COL_IDX_EXP,  GB_EXP_IDX_EXP}

    // if the materialized view has MIN / MAX, try to find an index defined on the source table
    // covering all group by cols / exprs to avoid expensive tablescan.
    // For now, the only acceptable index is defined exactly on the group by columns IN ORDER.
    // This allows the same key to be used to do lookups on the grouped table index and the
    // base table index.
    // TODO: More flexible (but usually less optimal*) indexes may be allowed here and supported
    // in the EE in the future including:
    //   -- *indexes on the group keys listed out of order
    //   -- *indexes on the group keys as a prefix before other indexed values.
    //   -- (ENG-6511) indexes on the group keys PLUS the MIN/MAX argument value (to eliminate post-filtering)
    // This function is mostly re-written for the fix of ENG-6511. --yzhang
    private static Index findBestMatchIndexForMatviewMinOrMax(MaterializedViewInfo matviewinfo,
            Table srcTable, List<AbstractExpression> groupbyExprs, AbstractExpression minMaxAggExpr)
    {
        CatalogMap<Index> allIndexes = srcTable.getIndexes();
        StmtTableScan tableScan = new StmtTargetTableScan(srcTable, srcTable.getTypeName());

        // Candidate index. If we can find an index covering both group-by columns and aggExpr (optimal) then we will
        // return immediately.
        // If the index found covers only group-by columns (sub-optimal), we will first cache it here.
        Index candidate = null;
        for (Index index : allIndexes) {
            // indexOptimalForMinMax == true if the index covered both the group-by columns and the min/max aggExpr.
            boolean indexOptimalForMinMax = false;
            // If minMaxAggExpr is not null, the diff can be zero or one.
            // Otherwise, for a usable index, its number of columns must agree with that of the group-by columns.
            final int diffAllowance = minMaxAggExpr == null ? 0 : 1;

            // Get all indexed exprs if there is any.
            String expressionjson = index.getExpressionsjson();
            List<AbstractExpression> indexedExprs = null;
            if ( ! expressionjson.isEmpty() ) {
                try {
                    indexedExprs = AbstractExpression.fromJSONArrayString(expressionjson, tableScan);
                } catch (JSONException e) {
                    e.printStackTrace();
                    assert(false);
                    return null;
                }
            }
            // Get source table columns.
            List<Column> srcColumnArray = CatalogUtil.getSortedCatalogItems(srcTable.getColumns(), "index");
            MatViewIndexMatchingGroupby matchingCase = null;

            if (groupbyExprs == null) {
                // This means group-by columns are all simple columns.
                // It also means we can only access the group-by columns by colref.
                List<ColumnRef> groupbyColRefs =
                    CatalogUtil.getSortedCatalogItems(matviewinfo.getGroupbycols(), "index");
                if (indexedExprs == null) {
                    matchingCase = MatViewIndexMatchingGroupby.GB_COL_IDX_COL;

                    // All the columns in the index are also simple columns, EASY! colref vs. colref
                    List<ColumnRef> indexedColRefs =
                        CatalogUtil.getSortedCatalogItems(index.getColumns(), "index");
                    // The number of columns in index can never be less than that in the group-by column list.
                    // If minMaxAggExpr == null, they must be equal (diffAllowance == 0)
                    // Otherwise they may be equal (sub-optimal) or
                    // indexedColRefs.size() == groupbyColRefs.size() + 1 (optimal, diffAllowance == 1)
                    if (isInvalidIndexCandidate(indexedColRefs.size(), groupbyColRefs.size(), diffAllowance)) {
                        continue;
                    }

                    if (! isGroupbyMatchingIndex(matchingCase, groupbyColRefs, null, indexedColRefs, null, null)) {
                        continue;
                    }
                    if (isValidIndexCandidateForMinMax(indexedColRefs.size(), groupbyColRefs.size(), diffAllowance)) {
                        if(! isIndexOptimalForMinMax(matchingCase, minMaxAggExpr, indexedColRefs, null, srcColumnArray)) {
                            continue;
                        }
                        indexOptimalForMinMax = true;
                    }
                }
                else {
                    matchingCase = MatViewIndexMatchingGroupby.GB_COL_IDX_EXP;
                    // In this branch, group-by columns are simple columns, but the index contains complex columns.
                    // So it's only safe to access the index columns from indexedExprs.
                    // You can still get something from indexedColRefs, but they will be inaccurate.
                    // e.g.: ONE index column (a+b) will get you TWO separate entries {a, b} in indexedColRefs.
                    // In order to compare columns: for group-by columns: convert colref => col
                    //                              for    index columns: convert    tve => col
                    if (isInvalidIndexCandidate(indexedExprs.size(), groupbyColRefs.size(), diffAllowance)) {
                        continue;
                    }

                    if (! isGroupbyMatchingIndex(matchingCase, groupbyColRefs, null, null, indexedExprs, srcColumnArray)) {
                        continue;
                    }
                    if (isValidIndexCandidateForMinMax(indexedExprs.size(), groupbyColRefs.size(), diffAllowance)) {
                        if(! isIndexOptimalForMinMax(matchingCase, minMaxAggExpr, null, indexedExprs, null)) {
                            continue;
                        }
                        indexOptimalForMinMax = true;
                    }
                }
            }
            else {
                matchingCase = MatViewIndexMatchingGroupby.GB_EXP_IDX_EXP;
                // This means group-by columns have complex columns.
                // It's only safe to access the group-by columns from groupbyExprs.
                // AND, indexedExprs must not be null in this case. (yeah!)
                if ( indexedExprs == null ) {
                    continue;
                }
                if (isInvalidIndexCandidate(indexedExprs.size(), groupbyExprs.size(), diffAllowance)) {
                    continue;
                }

                if (! isGroupbyMatchingIndex(matchingCase, null, groupbyExprs, null, indexedExprs, null)) {
                    continue;
                }

                if (isValidIndexCandidateForMinMax(indexedExprs.size(), groupbyExprs.size(), diffAllowance)) {
                    if (! isIndexOptimalForMinMax(matchingCase, minMaxAggExpr, null, indexedExprs, null)) {
                        continue;
                    }
                    indexOptimalForMinMax = true;
                }
            }

            // NOW index at least covered all group-by columns (sub-optimal candidate)
            if (!index.getPredicatejson().isEmpty()) {
                // Additional check for partial indexes to make sure matview WHERE clause
                // covers the partial index predicate
                List<AbstractExpression> coveringExprs = new ArrayList<AbstractExpression>();
                List<AbstractExpression> exactMatchCoveringExprs = new ArrayList<AbstractExpression>();
                try {
                    String encodedPredicate = matviewinfo.getPredicate();
                    if (!encodedPredicate.isEmpty()) {
                        String predicate = Encoder.hexDecodeToString(encodedPredicate);
                        AbstractExpression matViewPredicate = AbstractExpression.fromJSONString(predicate, tableScan);
                        coveringExprs.addAll(ExpressionUtil.uncombineAny(matViewPredicate));
                    }
                } catch (JSONException e) {
                    e.printStackTrace();
                    assert(false);
                    return null;
                }
                if (! SubPlanAssembler.isPartialIndexPredicateIsCovered(tableScan, coveringExprs, index, exactMatchCoveringExprs)) {
                    // partial index does not match MatView where clause, give up this index
                    continue;
                }
            }
            // if the index already covered group by columns and the aggCol/aggExpr,
            // it is already the best index we can get, return immediately.
            if (indexOptimalForMinMax) {
                return index;
            }
            // otherwise wait to see if we can find something better!
            candidate = index;
        }
        return candidate;
    }

    private static boolean isInvalidIndexCandidate(int idxSize, int gbSize, int diffAllowance) {
        if ( idxSize < gbSize || idxSize > gbSize + diffAllowance ) {
            return true;
        }
        return false;
    }

    private static boolean isGroupbyMatchingIndex(
            MatViewIndexMatchingGroupby matchingCase,
            List<ColumnRef> groupbyColRefs, List<AbstractExpression> groupbyExprs,
            List<ColumnRef> indexedColRefs, List<AbstractExpression> indexedExprs,
            List<Column> srcColumnArray) {
        // Compare group-by columns/expressions for different cases
        switch(matchingCase) {
        case GB_COL_IDX_COL:
            for (int i = 0; i < groupbyColRefs.size(); ++i) {
                int groupbyColIndex = groupbyColRefs.get(i).getColumn().getIndex();
                int indexedColIndex = indexedColRefs.get(i).getColumn().getIndex();
                if (groupbyColIndex != indexedColIndex) {
                    return false;
                }
            }
            break;
        case GB_COL_IDX_EXP:
            for (int i = 0; i < groupbyColRefs.size(); ++i) {
                AbstractExpression indexedExpr = indexedExprs.get(i);
                if (! (indexedExpr instanceof TupleValueExpression)) {
                    // Group-by columns are all simple columns, so indexedExpr must be tve.
                    return false;
                }
                int indexedColIdx = ((TupleValueExpression)indexedExpr).getColumnIndex();
                Column indexedColumn = srcColumnArray.get(indexedColIdx);
                Column groupbyColumn = groupbyColRefs.get(i).getColumn();
                if ( ! indexedColumn.equals(groupbyColumn) ) {
                    return false;
                }
            }
            break;
        case GB_EXP_IDX_EXP:
            for (int i = 0; i < groupbyExprs.size(); ++i) {
                if (! indexedExprs.get(i).equals(groupbyExprs.get(i))) {
                   return false;
                }
            }
            break;
        default:
            assert(false);
            // invalid option
            return false;
        }

        // group-by columns/expressions are matched with the corresponding index
        return true;
    }

    private static boolean isValidIndexCandidateForMinMax(int idxSize, int gbSize, int diffAllowance) {
        return diffAllowance == 1 && idxSize == gbSize + 1;
    }

    private static boolean isIndexOptimalForMinMax(
            MatViewIndexMatchingGroupby matchingCase, AbstractExpression minMaxAggExpr,
            List<ColumnRef> indexedColRefs, List<AbstractExpression> indexedExprs,
            List<Column> srcColumnArray) {
        // We have minMaxAggExpr and the index also has one extra column
        switch(matchingCase) {
        case GB_COL_IDX_COL:
            if ( ! (minMaxAggExpr instanceof TupleValueExpression) ) {
                // Here because the index columns are all simple columns (indexedExprs == null)
                // so the minMaxAggExpr must be TupleValueExpression.
                return false;
            }
            int aggSrcColIdx = ((TupleValueExpression)minMaxAggExpr).getColumnIndex();
            Column aggSrcCol = srcColumnArray.get(aggSrcColIdx);
            Column lastIndexCol = indexedColRefs.get(indexedColRefs.size() - 1).getColumn();
            // Compare the two columns, if they are equal as well, then this is the optimal index! Congrats!
            if (aggSrcCol.equals(lastIndexCol)) {
                return true;
            }
            break;
        case GB_COL_IDX_EXP:
        case GB_EXP_IDX_EXP:
            if (indexedExprs.get(indexedExprs.size()-1).equals(minMaxAggExpr)) {
                return true;
            }
            break;
        default:
            assert(false);
        }

        // If the last part of the index does not match the MIN/MAX expression
        // this is not the optimal index candidate for now
        return false;
    }
    /**
     * Build the abstract expression representing the partial index predicate.
     * Verify it satisfies the rules. Throw error messages otherwise.
     *
     * @param dummy AbstractParsedStmt
     * @param indexName The name of the index being checked.
     * @param predicateXML The XML representing the predicate.
     * @param table Table
     * @throws VoltCompilerException
     * @return AbstractExpression
     */
    private static AbstractExpression buildPartialIndexPredicate(
            AbstractParsedStmt dummy, String indexName,
            VoltXMLElement predicateXML, Table table,
            VoltCompiler compiler) throws VoltCompilerException {
        // Make sure all column expressions refer to the same index table
        // before we can parse the XML to avoid the AbstractParsedStmt
        // exception/assertion
        String tableName = table.getTypeName();
        assert(tableName != null);
        String msg = "Partial index \"" + indexName + "\" ";

        // Make sure all column expressions refer the index table
        List<VoltXMLElement> columnRefs= predicateXML.findChildrenRecursively("columnref");
        for (VoltXMLElement columnRef : columnRefs) {
            String columnRefTableName = columnRef.attributes.get("table");
            if (columnRefTableName != null && !tableName.equals(columnRefTableName)) {
                msg += "with expression(s) involving other tables is not supported.";
                throw compiler.new VoltCompilerException(msg);
            }
        }
        // Now it safe to parse the expression tree
        AbstractExpression predicate = dummy.parseExpressionTree(predicateXML);

        if (!predicate.findAllSubexpressionsOfClass(AggregateExpression.class).isEmpty()) {
            msg += "with aggregate expression(s) is not supported.";
            throw compiler.new VoltCompilerException(msg);
        }
        if (!predicate.findAllSubexpressionsOfClass(AbstractSubqueryExpression.class).isEmpty()) {
            msg += "with subquery expression(s) is not supported.";
            throw compiler.new VoltCompilerException(msg);
        }
        return predicate;
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
    private static void checkViewMeetsSpec(String viewName, ParsedSelectStmt stmt,
            VoltCompiler compiler) throws VoltCompilerException {
        int groupColCount = stmt.m_groupByColumns.size();
        int displayColCount = stmt.m_displayColumns.size();
        StringBuffer msg = new StringBuffer();
        msg.append("Materialized view \"" + viewName + "\" ");

        List <AbstractExpression> checkExpressions = new ArrayList<AbstractExpression>();

        int i;
        // First, check the group by columns.  They are at
        // the beginning of the display list.
        for (i = 0; i < groupColCount; i++) {
            ParsedColInfo gbcol = stmt.m_groupByColumns.get(i);
            ParsedColInfo outcol = stmt.m_displayColumns.get(i);
            // The columns must be equal.
            if (!outcol.expression.equals(gbcol.expression)) {
                msg.append("must exactly match the GROUP BY clause at index " + String.valueOf(i) + " of SELECT list.");
                throw compiler.new VoltCompilerException(msg.toString());
            }
            // check if the expression return type is not indexable
            StringBuffer exprMsg = new StringBuffer();
            if (!outcol.expression.isValueTypeIndexable(exprMsg)) {
                msg.append("with " + exprMsg + " in GROUP BY clause not supported.");
                throw compiler.new VoltCompilerException(msg.toString());
            }
            // collect all the expressions and we will check
            // for other gaurds on all of them together
            checkExpressions.add(outcol.expression);
        }

        // check for count star in the display list
        boolean countStarFound = false;
        if (i < displayColCount) {
            AbstractExpression coli = stmt.m_displayColumns.get(i).expression;
            if (coli.getExpressionType() == ExpressionType.AGGREGATE_COUNT_STAR) {
                countStarFound = true;
            }
        }

        if (countStarFound == false) {
            msg.append("must have count(*) after the GROUP BY columns (if any) but before the aggregate functions (if any).");
            throw compiler.new VoltCompilerException(msg.toString());
        }

        // Finally, the display columns must have aggregate
        // calls.  But these are not any aggregate calls. They
        // must be count(), min(), max() or sum().
        for (i++; i < displayColCount; i++) {
            ParsedColInfo outcol = stmt.m_displayColumns.get(i);
            // Note that this expression does not catch all aggregates.
            // An instance of count(*) here, or avg() would cause the
            // exception.  We just required count(*) above, but a
            // second one would fail.
            if ((outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_COUNT) &&
                    (outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_SUM) &&
                    (outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_MIN) &&
                    (outcol.expression.getExpressionType() != ExpressionType.AGGREGATE_MAX)) {
                msg.append("must have non-group by columns aggregated by sum, count, min or max.");
                throw compiler.new VoltCompilerException(msg.toString());
            }
            // Don't push the expression, though.  Push the argument.
            // We will check for aggregate calls and fail, and we don't
            // want to fail on legal aggregate expressions.
            if (outcol.expression.getLeft() != null) {
                checkExpressions.add(outcol.expression.getLeft());
            }
            assert(outcol.expression.getRight() == null);
            assert(outcol.expression.getArgs() == null || outcol.expression.getArgs().size() == 0);
        }

        AbstractExpression where = stmt.getSingleTableFilterExpression();
        if (where != null) {
            checkExpressions.add(where);
        }

        // Check all the subexpressions we gathered up.
        if (!AbstractExpression.validateExprsForIndexesAndMVs(checkExpressions, msg)) {
            // The error message will be in the StringBuffer msg.
            throw compiler.new VoltCompilerException(msg.toString());
        }

        // Check some other materialized view specific things.
        if (stmt.hasSubquery()) {
            msg.append("with subquery sources is not supported.");
            throw compiler.new VoltCompilerException(msg.toString());
        }

        if (stmt.m_tableList.size() != 1) {
            msg.append("has " + String.valueOf(stmt.m_tableList.size()) + " sources. " +
                       "Only one source table is allowed.");
            throw compiler.new VoltCompilerException(msg.toString());
        }

        if (stmt.orderByColumns().size() != 0) {
            msg.append("with ORDER BY clause is not supported.");
            throw compiler.new VoltCompilerException(msg.toString());
        }

        if (stmt.hasLimitOrOffset()) {
            msg.append("with LIMIT or OFFSET clause is not supported.");
            throw compiler.new VoltCompilerException(msg.toString());
        }

        if (stmt.m_having != null) {
            msg.append("with HAVING clause is not supported.");
            throw compiler.new VoltCompilerException(msg.toString());
        }

        if (displayColCount <= groupColCount) {
            msg.append("has too few columns.");
            throw compiler.new VoltCompilerException(msg.toString());
        }

     }

    private static void processMaterializedViewColumn(MaterializedViewInfo info, Table srcTable,
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
