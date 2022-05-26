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

package org.voltdb.compiler;

import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.lang3.StringUtils;
import org.hsqldb_voltpatches.FunctionForVoltDB;
import org.hsqldb_voltpatches.HSQLDDLInfo;
import org.hsqldb_voltpatches.HSQLInterface;
import org.hsqldb_voltpatches.HSQLInterface.HSQLParseException;
import org.hsqldb_voltpatches.PersistentExport;
import org.hsqldb_voltpatches.TimeToLiveVoltDB;
import org.hsqldb_voltpatches.VoltXMLElement;
import org.hsqldb_voltpatches.VoltXMLElement.VoltXMLDiff;
import org.hsqldb_voltpatches.lib.StringUtil;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONStringer;
import org.voltdb.TableType;
import org.voltdb.VoltType;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Function;
import org.voltdb.catalog.FunctionParameter;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TimeToLive;
import org.voltdb.common.Constants;
import org.voltdb.compiler.VoltCompiler.DdlProceduresToLoad;
import org.voltdb.compiler.VoltCompiler.VoltCompilerException;
import org.voltdb.compiler.statements.AlterTask;
import org.voltdb.compiler.statements.CatchAllVoltDBStatement;
import org.voltdb.compiler.statements.CreateAggregateFunctionFromClass;
import org.voltdb.compiler.statements.CreateFunctionFromMethod;
import org.voltdb.compiler.statements.CreateProcedureAsSQL;
import org.voltdb.compiler.statements.CreateProcedureAsScript;
import org.voltdb.compiler.statements.CreateProcedureFromClass;
import org.voltdb.compiler.statements.CreateRole;
import org.voltdb.compiler.statements.CreateTask;
import org.voltdb.compiler.statements.DRTable;
import org.voltdb.compiler.statements.DropAggregateFunction;
import org.voltdb.compiler.statements.DropFunction;
import org.voltdb.compiler.statements.DropProcedure;
import org.voltdb.compiler.statements.DropRole;
import org.voltdb.compiler.statements.DropStream;
import org.voltdb.compiler.statements.DropTask;
import org.voltdb.compiler.statements.PartitionStatement;
import org.voltdb.compiler.statements.ReplicateTable;
import org.voltdb.compiler.statements.SetGlobalParam;
import org.voltdb.compiler.statements.VoltDBStatementProcessor;
import org.voltdb.compilereport.TableAnnotation;
import org.voltdb.e3.topics.TopicProperties;
import org.voltdb.exportclient.ExportRowSchema;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractExpression.UnsafeOperatorsForDDL;
import org.voltdb.expressions.ExpressionUtil;
import org.voltdb.expressions.FunctionExpression;
import org.voltdb.expressions.OperatorExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.parser.HSQLLexer;
import org.voltdb.parser.SQLLexer;
import org.voltdb.parser.SQLParser;
import org.voltdb.planner.AbstractParsedStmt;
import org.voltdb.planner.ParsedSelectStmt;
import org.voltdb.plannerv2.utils.DropTableUtils;
import org.voltdb.sysprocs.AdHocNTBase;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.IndexType;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogSchemaTools;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.CompressionService;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.LineReaderAdapter;
import org.voltdb.utils.SQLCommand;

/**
 * Compiles schema (SQL DDL) text files and stores the results in a given catalog.
 *
 */
public class DDLCompiler {

    public static final int MAX_COLUMNS = 1024; // KEEP THIS < MAX_PARAM_COUNT to enable default CRUD update.
    public static final int MAX_ROW_SIZE = 1024 * 1024 * 2;
    // These constants should be consistent with the definitions in VoltType.java
    protected static final int MAX_VALUE_LENGTH = 1024 * 1024;
    public static final int MAX_BYTES_PER_UTF8_CHARACTER = 4;

    private final HSQLInterface m_hsql;
    private final VoltCompiler m_compiler;
    private final MaterializedViewProcessor m_mvProcessor;
    private static final AbstractExpression NOT_MIGRATING =
            new OperatorExpression(ExpressionType.OPERATOR_NOT,
                    new FunctionExpression("migrating", null, null,   // MIGRATING() function
                            FunctionForVoltDB.FunctionDescriptor.FUNC_VOLT_MIGRATING),
                    null);

    static {
        final AbstractExpression migrating_node = NOT_MIGRATING.getLeft();
        migrating_node.setValueType(VoltType.BOOLEAN);
        migrating_node.setArgs(Collections.emptyList());
        migrating_node.setValueSize(1);
    }

    private String m_fullDDL = "";

    private final VoltDBStatementProcessor m_voltStatementProcessor;

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

    // Resolve classes using a custom loader. Needed for catalog version upgrade.
    private final ClassLoader m_classLoader;

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
    public static String DR_CURRENT_CLUSTER_ID_COLUMN_NAME = "CURRENT_CLUSTER_ID";
    public static String DR_CURRENT_TIMESTAMP_COLUMN_NAME = "CURRENT_TIMESTAMP";
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
        {DR_CURRENT_CLUSTER_ID_COLUMN_NAME, "TINYINT NOT NULL"},
        {DR_CURRENT_TIMESTAMP_COLUMN_NAME, "BIGINT NOT NULL"},
        {DR_TUPLE_COLUMN_NAME, "VARCHAR(1048576 BYTES)"},
    };

    public static class DDLStatement {
        public DDLStatement(boolean newDdl) {
            this.newDdl = newDdl;
        }

        public DDLStatement(String statement, int lineNo) {
            this.statement = statement;
            this.lineNo = lineNo;
            newDdl = false;
        }

        public String statement = "";
        public int lineNo; // beginning of statement line number
        public int endLineNo; // end of statement line number
        public final boolean newDdl;
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
        m_mvProcessor = new MaterializedViewProcessor(m_compiler, m_hsql);
        m_voltStatementProcessor = new VoltDBStatementProcessor(this);
        m_voltStatementProcessor.addNextProcessor(new CreateProcedureFromClass(this))
                                .addNextProcessor(new CreateProcedureAsScript(this))
                                .addNextProcessor(new CreateProcedureAsSQL(this))
                                .addNextProcessor(new CreateAggregateFunctionFromClass(this))
                                .addNextProcessor(new CreateFunctionFromMethod(this))
                                .addNextProcessor(new DropAggregateFunction(this))
                                .addNextProcessor(new DropFunction(this))
                                .addNextProcessor(new DropProcedure(this))
                                .addNextProcessor(new PartitionStatement(this))
                                .addNextProcessor(new ReplicateTable(this))
                                .addNextProcessor(new CreateRole(this))
                                .addNextProcessor(new DropRole(this))
                                .addNextProcessor(new DropStream(this))
                                .addNextProcessor(new DRTable(this))
                                .addNextProcessor(new SetGlobalParam(this))
                                .addNextProcessor(new CreateTask(this))
                                .addNextProcessor(new DropTask(this))
                                .addNextProcessor(new AlterTask(this))
                                // CatchAllVoltDBStatement needs to be the last processor in the chain.
                                .addNextProcessor(new CatchAllVoltDBStatement(this, m_voltStatementProcessor));
    }

    /**
     * Processors for different types of DDL statements will inherit from this class.
     * Together they will build a processing chain in a chain of responsibility (CoR) pattern.
     */
    public static abstract class StatementProcessor {
        //
        private StatementProcessor m_next;

        protected HSQLInterface m_hsql;
        protected VoltCompiler m_compiler;
        protected VoltDDLElementTracker m_tracker;
        protected ClassLoader m_classLoader;
        protected VoltXMLElement m_schema;
        protected ClassMatcher m_classMatcher;
        protected boolean m_returnAfterThis = false;

        public StatementProcessor(DDLCompiler ddlCompiler) {
            if (ddlCompiler != null) {
                m_hsql = ddlCompiler.m_hsql;
                m_compiler = ddlCompiler.m_compiler;
                m_tracker = ddlCompiler.m_tracker;
                m_classLoader = ddlCompiler.m_classLoader;
                m_schema = ddlCompiler.m_schema;
                m_classMatcher = ddlCompiler.m_classMatcher;
            }
        }

        public final StatementProcessor addNextProcessor(StatementProcessor next) {
            m_next = next;
            return m_next;
        }

        protected abstract boolean processStatement(
                  DDLStatement ddlStatement,
                  Database db,
                  DdlProceduresToLoad whichProcs) throws VoltCompilerException;

        public final boolean process(
                DDLStatement ddlStatement,
                Database db,
                DdlProceduresToLoad whichProcs) throws VoltCompilerException {
            m_returnAfterThis = false;
            if (! processStatement(ddlStatement, db, whichProcs)) {
                if (m_returnAfterThis || m_next == null) {
                    return false;
                }
                return m_next.process(ddlStatement, db, whichProcs);
            }
            return true;
        }

        /**
         * Checks whether or not the start of the given identifier is java (and
         * thus DDL) compliant. An identifier may start with: _ [a-zA-Z] $
         * @param identifier the identifier to check
         * @param statement the statement where the identifier is
         * @return the given identifier unmodified
         * @throws VoltCompilerException when it is not compliant
         */
        protected final String checkIdentifierStart(
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

        protected static final String TABLE = "TABLE";
        protected static final String PROCEDURE = "PROCEDURE";
        protected static final String FUNCTION = "FUNCTION";
        protected static final String PARTITION = "PARTITION";
        protected static final String REPLICATE = "REPLICATE";
        protected static final String ROLE = "ROLE";
        protected static final String DR = "DR";
        protected static final String TASK = "TASK";
        protected static final String AGGREGATE = "AGGREGATE";
        protected static final String TOPIC = "TOPIC";
        protected static final String OPAQUE = "OPAQUE";
    }

    public void loadSchemaWithFiltering(Reader reader, final Database db, final DdlProceduresToLoad whichProcs, SQLParser.FileInfo fileInfo)
            throws VoltCompiler.VoltCompilerException {

        final LineReaderAdapter lineReader = new LineReaderAdapter(reader);

        DDLParserCallback callback = new DDLParserCallback() {

            @Override
            public void statement(String statement, int lineNum) throws VoltCompiler.VoltCompilerException {
                try {
                    if (statement == null || statement.trim().isEmpty()) {
                        return;
                    }
                    doExecuteStatement(statement.trim() + ";", lineNum);
                }
                catch (Exception e) {
                    throw m_compiler.new VoltCompilerException(e);
                }
            }

            @Override
            public void batch(String batch, int batchEndLineNumber) throws VoltCompiler.VoltCompilerException {
                try {
                    String[] parts = batch.split(";");

                    int currLine = batchEndLineNumber - parts.length;
                    for (String s : parts) {
                        if (!s.trim().isEmpty()) {
                            doExecuteStatement(s.trim() + ";", currLine++);
                        }
                    }
                }
                catch (Exception e) {
                    throw m_compiler.new VoltCompilerException(e);
                }
            }

            private void doExecuteStatement(String statement, int lineNum) throws VoltCompiler.VoltCompilerException {

                // Filter out any statements that we don't want to deal with
                if (statement.toUpperCase().startsWith("LOAD")) {
                    return;
                }

                DDLStatement stmt = new DDLStatement(statement, lineNum);
                processVoltDBStatements(db, whichProcs, stmt);
            }
        };

        try {
            SQLCommand.executeScriptFromReader(fileInfo, lineReader, callback);
        }
        catch (Exception e) {
            throw m_compiler.new VoltCompilerException(e);
        }
        finally {
            // NEEDSWORK (ENG-12255): This really should be done outside the loop, by the caller. The exception propagation, at this point, is pretty messy, and
            // changing it would be a pretty big effort that's really not relevant to this ticket. Fix this sometime.
            lineReader.close();
        }

        // process extra classes
        m_tracker.addExtraClasses(m_classMatcher.getMatchedClassList());
        // possibly save some memory
        m_classMatcher.clear();
    }

    /**
     * Compile a DDL schema from an abstract reader
     *
     * @param reader     abstract DDL reader
     * @param db         database
     * @param whichProcs which type(s) of procedures to load
     * @param newDdl     Whether or not this DDL has been compiled before or not
     * @throws VoltCompiler.VoltCompilerException
     */
    void loadSchema(Reader reader, Database db, Database prevDb, DdlProceduresToLoad whichProcs, boolean newDdl)
            throws VoltCompilerException {
        int currLineNo = 1;

        DDLStatement stmt = getNextStatement(reader, m_compiler, currLineNo, newDdl);
        final StringBuilder ddls = new StringBuilder();
        boolean isBatch = false;        // When reader contains multiple stmts, set it to indicate we are in batch mode.
        while (stmt != null) {
            if (isBatch || ! AdHocNTBase.USING_CALCITE) {      // We cannot query previous database for existing tables in batch mode, as
                // current DDL batch may contain CREATE statements, that is later dropped.
                processVoltDBStatements(db, whichProcs, stmt);
            } else {        // ENG-17075: Until Calcite can handle all DDL statements,
                // this could throw SqlParserException, that gets printed and transformed into FallbackException.
                final String s = DropTableUtils.run(prevDb, stmt.statement, m_schema, m_compiler);
                if (s == null) {
                    processVoltDBStatements(db, whichProcs, stmt);
                } else {
                    ddls.append(s);
                }
            }
            stmt = getNextStatement(reader, m_compiler, stmt.endLineNo, newDdl);
            isBatch = true;
        }
        m_fullDDL += ddls;

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
        sb.append("CREATE STREAM ").append(name).append(" ");
        // The partitioning here doesn't matter, it's only to trick the export system, not related to data placement.
        if (partitioned) {
            sb.append("PARTITION ON COLUMN ").append(DR_TIMESTAMP_COLUMN_NAME).append(" ");
        }
        sb.append("EXPORT TO TARGET ").append(CatalogUtil.DR_CONFLICTS_TABLE_EXPORT_GROUP).append(" (");
        for (String[] column : DR_CONFLICTS_EXPORT_TABLE_META_COLUMNS) {
            sb.append(column[0]).append(" ").append(column[1]);
            if (!column[0].equals(DR_TUPLE_COLUMN_NAME)) {
                sb.append(", ");
            }
        }
        sb.append(");\n");
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
            sb.append("DROP STREAM " + CatalogUtil.DR_CONFLICTS_PARTITIONED_EXPORT_TABLE + ";\n");
        }
        if (hasConflictTableInSchema(m_schema, CatalogUtil.DR_CONFLICTS_REPLICATED_EXPORT_TABLE)) {
            sb.append("DROP STREAM " + CatalogUtil.DR_CONFLICTS_REPLICATED_EXPORT_TABLE + ";\n");
        }
    }

    // Generate DDL to create or drop the DR conflict table
    private String generateDDLForDRConflictsTable(Database currentDB, Database previousDBIfAny, boolean isCurrentXDCR) {
        StringBuilder sb = new StringBuilder();
        if (isCurrentXDCR) {
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
     * @param isCurrentXDCR Does the current catalog has XDCR enabled
     * @throws VoltCompilerException
     */
    void loadAutogenExportTableSchema(Database db, Database previousDBIfAny,
            DdlProceduresToLoad whichProcs, boolean isCurrentXDCR)
            throws VoltCompilerException {
        Reader reader = new VoltCompilerStringReader(null, generateDDLForDRConflictsTable(db, previousDBIfAny, isCurrentXDCR));
        loadSchema(reader, db, previousDBIfAny, whichProcs, false);
    }

    private void applyDiff(VoltXMLDiff stmtDiff, final Database db) throws VoltCompilerException
    {
        // record which tables changed
        for (Map.Entry<String, VoltXMLDiff> entry : stmtDiff.getChangedNodes().entrySet()) {
            String tableName = entry.getKey();
            assert(tableName.startsWith("table"));
            tableName = tableName.substring("table".length());
            validatePersistentExport(tableName, entry.getValue());
            m_compiler.markTableAsDirty(tableName);
        }
        for (VoltXMLElement tableXML : stmtDiff.getRemovedNodes()) {
            String tableName = tableXML.attributes.get("name");
            assert(tableName != null);
            m_compiler.markTableAsDirty(tableName);
            Table table = db.getTables().get(tableName);
            if (table != null && StringUtils.isNotBlank(table.getTopicname())) {
                throw m_compiler.new VoltCompilerException(String.format("The table %s is used by topic, cannot be dropped", tableName));
            }
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
        Set<String> removedColumns = new HashSet<>();
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

    private void validatePersistentExport(String tableName, VoltXMLDiff stmtdiff) throws VoltCompilerException {
        VoltXMLElement added = findPersistentExport(stmtdiff.getAddedNodes());
        VoltXMLElement removed = findPersistentExport(stmtdiff.getRemovedNodes());
        if (added == null || removed == null) {
            return; // accepted
        }

        boolean addedTopic = Boolean.parseBoolean(added.attributes.get("isTopic"));
        boolean removedTopic = Boolean.parseBoolean(removed.attributes.get("isTopic"));
        String addedName =  added.attributes.get("target");
        String removedName = removed.attributes.get("target");
        if (addedTopic ^ removedTopic || !addedName.equalsIgnoreCase(removedName)) {
            String type = removedTopic ? "topic" : "target";
            throw m_compiler.new VoltCompilerException(String.format("The export %s on table %s cannot be altered",
                                                                     type, tableName));
        }
    }

    private VoltXMLElement findPersistentExport(List<VoltXMLElement> nodes) {
        for (VoltXMLElement node : nodes) {
            if (PersistentExport.PERSISTENT_EXPORT.equals(node.name)) {
                return node;
            }
        }
        return null;
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
     * Process a VoltDB-specific create stream DDL statement
     *
     * @param stmt
     *            DDL statement string
     * @param db
     * @param whichProcs
     * @throws VoltCompilerException
     */
    private void processCreateStreamStatement(DDLStatement stmt, Database db, DdlProceduresToLoad whichProcs)
            throws VoltCompilerException {
        String statement = stmt.statement;
        Matcher statementMatcher = SQLParser.matchCreateStream(statement);
        if (statementMatcher.matches()) {
            // check the table portion
            String tableName = checkIdentifierStart(statementMatcher.group(1), statement);
            String targetName = null;
            String topicName = null;
            String keyColumnNames = null;
            String valueColumnNames = null;
            String columnName = null;

            // Parse the EXPORT and PARTITION captures.
            if ((statementMatcher.groupCount() > 1) &&
                (statementMatcher.group(2) != null) &&
                (!statementMatcher.group(2).isEmpty())) {
                String clauses = statementMatcher.group(2);
                Matcher matcher = SQLParser.matchAnyCreateStreamStatementClause(clauses);
                int start = 0;
                while ( matcher.find(start)) {
                    start = matcher.end();

                    if (matcher.group(SQLParser.CAPTURE_EXPORT_TARGET) != null) {
                        // Add target info if it's an Export clause. Only one is allowed
                        if (targetName != null || topicName != null) {
                            throw m_compiler.new VoltCompilerException(
                                "Only one Export clause is allowed for CREATE STREAM.");
                        }
                        targetName = matcher.group(SQLParser.CAPTURE_EXPORT_TARGET);
                    }
                    else if (matcher.group(SQLParser.CAPTURE_EXPORT_TOPIC) != null) {
                        // Add target info if it's an Export clause. Only one is allowed
                        if (targetName != null || topicName != null) {
                            throw m_compiler.new VoltCompilerException(
                                "Only one Export clause is allowed for CREATE STREAM.");
                        }
                        topicName = matcher.group(SQLParser.CAPTURE_EXPORT_TOPIC);

                        // Add optional clauses
                        keyColumnNames = matcher.group(SQLParser.CAPTURE_TOPIC_KEY_COLUMNS);
                        valueColumnNames = matcher.group(SQLParser.CAPTURE_TOPIC_VALUE_COLUMNS);
                    }
                    else if (matcher.group(SQLParser.CAPTURE_STREAM_PARTITION_COLUMN) != null) {
                        // Add partition info if it's a PARTITION clause. Only one is allowed.
                        if (columnName != null) {
                            throw m_compiler.new VoltCompilerException(
                                "Only one PARTITION clause is allowed for CREATE STREAM.");
                        }
                        columnName = matcher.group(SQLParser.CAPTURE_STREAM_PARTITION_COLUMN);
                    }
                }
            }

            // Update table
            VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());
            if (tableXML != null) {
                tableXML.attributes.put("stream", "true");
            } else {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Invalid STREAM statement: table %s does not exist", tableName));
            }

            // process partition if specified
            if (columnName != null) {
                tableXML.attributes.put("partitioncolumn", columnName.toUpperCase());
                // Column validity check done by VoltCompiler in post-processing

                // mark the table as dirty for the purposes of caching sql statements
                m_compiler.markTableAsDirty(tableName);
            }

            // process export to target
            targetName = (targetName != null) ? checkIdentifierStart(
                    targetName, statement) : Constants.CONNECTORLESS_STREAM_TARGET_NAME;

            if (tableXML.attributes.containsKey("drTable") && "ENABLE".equals(tableXML.attributes.get("drTable"))) {
                throw m_compiler.new VoltCompilerException(String.format(
                        "Invalid CREATE STREAM statement: table %s is a DR table.", tableName));
            } else {
                tableXML.attributes.put("export", targetName);
            }

            // process export to topic - note it erases the connector information
            if (topicName != null) {
                tableXML.attributes.put("topicName", topicName);
                tableXML.attributes.remove("export");

                if (keyColumnNames != null) {
                    tableXML.attributes.put("topicKeyColumnNames", keyColumnNames);
                }
                if (valueColumnNames != null) {
                    tableXML.attributes.put("topicValueColumnNames", valueColumnNames);
                }
            }
        } else {
            throw m_compiler.new VoltCompilerException(String.format("Invalid CREATE STREAM statement: \"%s\", "
                    + "expected syntax: CREATE STREAM <table> [PARTITION ON COLUMN <column-name>] [EXPORT TO TARGET <target>] (column datatype, ...); "
                    + "or CREATE STREAM <table> PARTITION ON COLUMN <column-name> (column datatype, ...);",
                    statement.substring(0, statement.length() - 1)));
        }
    }

    /**
     * Process a VoltDB-specific 'create table ... migrate/export to target/topic ...' DDL statement.
     * @param stmt
     * @throws VoltCompilerException
     */
    private void processCreateTableStatement(DDLStatement stmt) throws VoltCompilerException {
        final String statement = stmt.statement;
        // If a plain 'create table' without migrate or export clause
        Matcher matcher = SQLParser.matchCreateTablePlain(statement);
        if (matcher.matches()) {
            // Nothing needed here
            return;
        }
        // If we have 'migrate to target' clause
        matcher = SQLParser.matchCreateTableMigrateToTarget(statement);
        if (matcher.matches()) {
            VoltXMLElement tableXML = matchedTableXML(matcher, statement);
            String targetName = checkIdentifierStart(matcher.group(2), statement);
            tableXML.attributes.put("migrateExport", targetName);
            return;
        }
        // If we have 'migrate to topic' clause
        matcher = SQLParser.matchCreateTableMigrateToTopic(statement);
        if (matcher.matches()) {
            VoltXMLElement tableXML = matchedTableXML(matcher, statement);
            String topicName = checkIdentifierStart(matcher.group(2), statement);
            tableXML.attributes.put("migrateExport", topicName);
            tableXML.attributes.put("topicName", topicName);
            saveKeysAndValues(matcher, tableXML);
            return;
        }
        // If we have 'export to target' clause
        matcher = SQLParser.matchCreateTableExportToTarget(statement);
        if (matcher.matches()) {
            VoltXMLElement tableXML = matchedTableXML(matcher, statement);
            String targetName = checkIdentifierStart(matcher.group(2), statement);
            // Nothing stored here - syntax check only
            // Triggers are available through the PersistentExport node
            return;
        }
        // If we have 'export to topic' clause
        matcher = SQLParser.matchCreateTableExportToTopic(statement);
        if (matcher.matches()) {
            VoltXMLElement tableXML = matchedTableXML(matcher, statement);
            String topicName = checkIdentifierStart(matcher.group(2), statement);
            tableXML.attributes.put("topicName", topicName);
            saveKeysAndValues(matcher, tableXML);
            // Triggers are available through the PersistentExport node
            return;
        }
        // No matching variant: try and refine the error message (a little hacky but it seems
        // better than spitting out a message with the entire syntax)
        String syntax = "CREATE TABLE name";
        List<String> words = Arrays.asList(statement.toUpperCase().split("\\s+"));
        boolean migrate = words.contains("MIGRATE");
        boolean export = words.contains("EXPORT");
        boolean target = words.contains("TARGET");
        boolean topic = words.contains("TOPIC");
        if (migrate ^ export && target ^ topic) { // if intent is clearly one type
            syntax += (export ? " EXPORT TO" : " MIGRATE TO") +
                      (topic ? " TOPIC name" : " TARGET name") +
                      (export ? " [ON trigger,...]" : "") +
                      (topic ? " [WITH KEY (key,...) VALUE (value,...)]" : "");
        } else { // abbreviated complete syntax
            syntax += " [EXPORT|MIGRATE TO TARGET|TOPIC name] [options]";
        }
        syntax += " (column datatype, ...);";
        throw m_compiler.new VoltCompilerException(String.format("Invalid CREATE TABLE statement: \"%s\" expected syntax: %s",
                                                                  statement.substring(0, statement.length() - 1), syntax));
    }

    private VoltXMLElement matchedTableXML(Matcher matcher, String statement) throws VoltCompilerException {
        String tableName = checkIdentifierStart(matcher.group(1), statement);
        VoltXMLElement tableXML = m_schema.findChild("table", tableName.toUpperCase());
        if (tableXML == null) {
            throw m_compiler.new VoltCompilerException(String.format("Invalid DDL statement: table %s does not exist",
                                                                     tableName));
        }
        return tableXML;
    }

    private void saveKeysAndValues(Matcher matcher, VoltXMLElement tableXML) {
        String keys = matcher.group(SQLParser.CAPTURE_TOPIC_KEY_COLUMNS);
        String values= matcher.group(SQLParser.CAPTURE_TOPIC_VALUE_COLUMNS);
        if (keys != null) {
            tableXML.attributes.put("topicKeyColumnNames", keys);
        }
        if (values != null) {
            tableXML.attributes.put("topicValueColumnNames", values);
        }
    }

    /**
     * Process a VoltDB-specific 'create view ... migrate to target/topic ...' DDL statement.
     * @param stmt
     * @throws VoltCompilerException
     */
    private void processCreateViewStatement(DDLStatement stmt) throws VoltCompilerException {
        final String statement = stmt.statement;
        // If we have 'migrate to target' clause
        Matcher matcher = SQLParser.matchCreateViewMigrateToTarget(statement);
        if (matcher.matches()) {
            VoltXMLElement tableXML = matchedTableXML(matcher, statement);
            String targetName = checkIdentifierStart(matcher.group(2), statement);
            tableXML.attributes.put("migrateExport", targetName);
            return;
        }
        // If we have 'migrate to topic' clause
        matcher = SQLParser.matchCreateViewMigrateToTopic(statement);
        if (matcher.matches()) {
            VoltXMLElement tableXML = matchedTableXML(matcher, statement);
            String topicName = checkIdentifierStart(matcher.group(2), statement);
            tableXML.attributes.put("migrateExport", topicName);
            tableXML.attributes.put("topicName", topicName);
            saveKeysAndValues(matcher, tableXML);
            return;
        }
        // No matching MIGRATE variant. Do nothing more here if not MIGRATE,
        // else try and come up with a useful error message.
        List<String> words = Arrays.asList(statement.toUpperCase().split("\\s+"));
        if (!words.contains("MIGRATE")) {
            return;
        }
        String syntax = "CREATE VIEW name";
        boolean target = words.contains("TARGET");
        boolean topic = words.contains("TOPIC");
        if (target ^ topic) { // if intent is clearly one type
            if (target) {
                syntax += " MIGRATE TO TARGET name";
            } else {
                syntax += " MIGRATE TO TOPIC name [WITH KEY (key,...) VALUE (value,...)]";
            }
        } else { // abbreviated complete syntax
            syntax += " [MIGRATE TO TARGET|TOPIC name]";
        }
        syntax += " (column datatype, ...) ...";
        throw m_compiler.new VoltCompilerException(String.format("Invalid CREATE VIEW statement: \"%s\" expected syntax: %s",
                                                                  statement.substring(0, statement.length() - 1), syntax));
    }

    private void checkValidPartitionTableIndex(Index index, Column partitionCol, String tableName)
            throws VoltCompilerException {
        // skip checking for non-unique indexes.
        if (!index.getUnique()) {
            return;
        }

        boolean containsPartitionColumn = false;
        String jsonExpr = index.getExpressionsjson();
        // if this is a pure-column index...
        if (jsonExpr.isEmpty()) {
            for (ColumnRef cref : index.getColumns()) {
                Column col = cref.getColumn();
                // unique index contains partitioned column
                if (col.equals(partitionCol)) {
                    containsPartitionColumn = true;
                    break;
                }
            }
        } else { // if this is a fancy expression-based index...
            try {
                int partitionColIndex = partitionCol.getIndex();
                List<AbstractExpression> indexExpressions = AbstractExpression.fromJSONArrayString(jsonExpr, null);
                for (AbstractExpression expr: indexExpressions) {
                    if (expr instanceof TupleValueExpression &&
                            ((TupleValueExpression) expr).getColumnIndex() == partitionColIndex ) {
                        containsPartitionColumn = true;
                        break;
                    }
                }
            } catch (JSONException e) {
                e.printStackTrace(); // danger will robinson
                assert(false);
            }
        }

        if (containsPartitionColumn) {
            if (index.getAssumeunique()) {
                String exceptionMsg = String.format("ASSUMEUNIQUE is not valid " +
                "for an index that includes the partitioning column. Please use UNIQUE instead.");
                throw m_compiler.new VoltCompilerException(exceptionMsg);
            }
        } else if (! index.getAssumeunique()) {
            // Throw compiler exception.
            String indexName = index.getTypeName();
            String keyword = "";
            if (indexName.startsWith(HSQLInterface.AUTO_GEN_PRIMARY_KEY_PREFIX)) {
                indexName = "PRIMARY KEY";
                keyword = "PRIMARY KEY";
            } else {
                indexName = "UNIQUE INDEX " + indexName;
                keyword = "UNIQUE";
            }
            throw m_compiler.new VoltCompilerException(
                    "Invalid use of " + keyword + ". The " + indexName + " on the partitioned table " + tableName +
                            " does not include the partitioning column " + partitionCol.getName() +
                            ". See the documentation for the 'CREATE TABLE' and 'CREATE INDEX' commands and the 'ASSUMEUNIQUE' keyword.");
        }

    }

    void handlePartitions(Database db) throws VoltCompilerException {
        // Actually parse and handle all the partitions
        // this needs to happen before procedures are compiled
        String msg = "In database, ";
        final CatalogMap<Table> tables = db.getTables();
        for (Table table : tables) {
            String tableName = table.getTypeName();
            // Here, we restore that the table is always replicated, and set it to be partitioned if
            // the partition makes sense.
            table.setIsreplicated(true);

            if (m_tracker.m_partitionMap.containsKey(tableName.toLowerCase())) {
                String colName = m_tracker.m_partitionMap.get(tableName.toLowerCase());
                // A null column name indicates a replicated table. Ignore it here
                // because it defaults to replicated in the catalog.
                if (colName != null) {
                    assert(tables.getIgnoreCase(tableName) != null);
                    if (m_matViewMap.containsKey(table)) {
                        msg += "the materialized view is automatically partitioned based on its source table. "
                                + "Invalid PARTITION statement on view table " + tableName + ".";
                        throw m_compiler.new VoltCompilerException(msg);
                    }

                    final Column partitionCol = table.getColumns().getIgnoreCase(colName);
                    // make sure the column exists
                    if (partitionCol == null) {
                        msg += "PARTITION has unknown COLUMN '" + colName + "'";
                        throw m_compiler.new VoltCompilerException(msg);
                    }
                    // make sure the column is marked not-nullable
                    if (partitionCol.getNullable()) {
                        msg += "Partition column '" + tableName + "." + colName + "' is nullable. " +
                            "Partition columns must be constrained \"NOT NULL\".";
                        throw m_compiler.new VoltCompilerException(msg);
                    }
                    // verify that the partition column is a supported type
                    VoltType pcolType = VoltType.get((byte) partitionCol.getType());
                    switch (pcolType) {
                        case TINYINT:
                        case SMALLINT:
                        case INTEGER:
                        case BIGINT:
                        case STRING:
                        case VARBINARY:
                            break;
                        default:
                            msg += "Partition column '" + tableName + "." + colName + "' is not a valid type. " +
                            "Partition columns must be an integer, varchar or varbinary type.";
                            throw m_compiler.new VoltCompilerException(msg);
                    }

                    table.setPartitioncolumn(partitionCol);
                    table.setIsreplicated(false);

                    // Check valid indexes, whether they contain the partition column or not.
                    for (Index index : table.getIndexes()) {
                        checkValidPartitionTableIndex(index, partitionCol, tableName);
                    }
                }
            }
            else if (!StringUtils.isBlank(table.getTopicname()) && TableType.isStream(table.getTabletype())) {
                // Reject out of hand non-partitioned streams exporting to topics,
                // even if the topic isn't declared yet in the deployment
                throw m_compiler.new VoltCompilerException(String.format(
                        "Invalid topic %s: stream %s must be partitioned", table.getTopicname(), table.getTypeName()));
            }
        }
    }

    private void handleTTL(Database db) throws VoltCompilerException {
        for (Table table : db.getTables()) {
            TimeToLive ttl = table.getTimetolive().get(TimeToLiveVoltDB.TTL_NAME);
            if (ttl != null) {
                Column ttlColumn = ttl.getTtlcolumn();

                // Accept nullable TTL columns in views
                if (ttlColumn.getNullable() && !m_matViewMap.containsKey(table)) {
                    throw m_compiler.new VoltCompilerException(
                            "Column '" + table.getTypeName() + "." + ttlColumn.getName() +
                                    "' cannot be nullable for TTL.");
                }

                VoltType ttlColumnType = VoltType.get((byte) ttlColumn.getType());
                if (ttlColumnType != VoltType.TIMESTAMP) {
                    throw m_compiler.new VoltCompilerException(
                            "TTL column '" + table.getTypeName() + "." + ttlColumn.getName() +
                                    "' must be of type " + VoltType.TIMESTAMP.getName());
                }
            }
        }
    }

    private TreeSet<String> getExportTableNames() {
        TreeSet<String> exportTableNames = new TreeSet<>();
        NavigableMap<String, NavigableSet<String>> exportsByTargetName = m_tracker.getExportedTables();
        for (Entry<String, NavigableSet<String>> e : exportsByTargetName.entrySet()) {
            for (String tableName : e.getValue()) {
                exportTableNames.add(tableName);
            }
        }
        return exportTableNames;
    }

    void compileToCatalog(Database db, boolean isXDCR) throws VoltCompilerException {
        // note this will need to be decompressed to be used
        String binDDL = CompressionService.compressAndBase64Encode(m_fullDDL);
        db.setSchema(binDDL);
        // output the xml catalog to disk
        //* enable to debug */ System.out.println("DEBUG: " + m_schema);
        BuildDirectoryUtils.writeFile("schema-xml", "hsql-catalog-output.xml", m_schema.toString(), true);

        // Build the local catalog from the xml catalog.  Note that we've already
        // saved the previous user defined function set, so we don't need
        // to save it here.
        // 1.) Add the user defined functions.  We want
        //     these first in case something depends on them.
        // 2.) Add the tables.  This will add indexes as well,
        //     since the indexes are stored with the tables.
        // 3.) Amend the tracker with all the artifacts that
        //     we can't actually add to the catalog right now.
        //     This includes partitioning information, and whether
        //     tables are export or DR tables.
        // 4.) Add partitioning information from the tracker
        //     into the catalog.
        // 5.) Start processing materialized views.
        for (VoltXMLElement node : m_schema.children) {
            if (node.name.equals("ud_function")) {
                addUserDefinedFunctionToCatalog(db, node);
            }
        }
        // push down parsing schema for catalog
        db.getCatalog().parse(m_schema.toXML());
        for (VoltXMLElement node : m_schema.children) {
            if (node.name.equals("table")) {
                addTableToCatalog(db, node, isXDCR);        // Inside the function, it skips when a table catalog had already been created.
            }
        }

        fillTrackerFromXML();
        handlePartitions(db);
        handleTTL(db);
        m_mvProcessor.startProcessing(db, m_matViewMap, getExportTableNames());
    }

    private void addUserDefinedFunctionToCatalog(Database db, VoltXMLElement XMLfunc) {
        // Fetch out the functions, find the function name and define
        // the function object.
        CatalogMap<Function> catalogFunctions = db.getFunctions();
        String functionName = XMLfunc.attributes.get("name");
        Function func = catalogFunctions.add(functionName);

        //
        // Set the attributes of the function object.
        //
        func.setFunctionname(functionName);
        func.setClassname(XMLfunc.attributes.get("className"));
        func.setMethodname(XMLfunc.attributes.get("methodName"));

        // Calculate types.  In the XML the types are (stringified) integers
        // which are the values of VoltType enumerals.  We use the same thing
        // in the catalog.  But in the compiler types are enumerals of the type
        // Type.  We need to construct these from the integer values
        // using getDefaultTypeWithSize.
        //
        // Filling in the catalog and compiler types for paraemeters is
        // kind of mushed together in this loop.
        byte returnTypeByte = Byte.parseByte(XMLfunc.attributes.get("returnType"));
        func.setReturntype(returnTypeByte);

        int nparams = XMLfunc.children.size();
        CatalogMap<FunctionParameter> params = func.getParameters();

        for (int pidx = 0; pidx < nparams; pidx++) {
            VoltXMLElement ptype = XMLfunc.children.get(pidx);
            assert("udf_ptype".equals(ptype.name));
            FunctionParameter param = params.add(String.valueOf(pidx));
            byte ptypeno = Byte.parseByte(ptype.attributes.get("type"));
            param.setParametertype(ptypeno);
        }
        // Ascertain that the function id in the xml is the same as the
        // function id returned from the registration.  We don't really
        // care if this is newly defined or not.
        int functionId = Integer.parseInt(XMLfunc.attributes.get("functionid"));
        func.setFunctionid(functionId);
    }

    // Fill the table stuff in VoltDDLElementTracker from the VoltXMLElement tree at the end when
    // requested from the compiler
    private void fillTrackerFromXML() {
        for (VoltXMLElement e : m_schema.children) {
            if (e.name.equals("table")) {
                String tableName = e.attributes.get("name");
                String partitionCol = e.attributes.get("partitioncolumn");
                String export = e.attributes.get("export");
                String drTable = e.attributes.get("drTable");
                String migrateTarget = e.attributes.get("migrateExport");
                export = StringUtil.isEmpty(export) ? migrateTarget : export;
                boolean isTopic = !StringUtils.isBlank(e.attributes.get("topicName"));
                final boolean isStream = (e.attributes.get("stream") != null);
                if (partitionCol != null) {
                    m_tracker.addPartition(tableName, partitionCol);
                } else {
                    m_tracker.removePartition(tableName);
                }
                if (!StringUtil.isEmpty(export) && !isTopic) {
                    m_tracker.addExportedTable(tableName, export, isStream);
                } else {
                    m_tracker.removeExportedTable(tableName, isStream);
                }
                if (drTable != null) {
                    m_tracker.addDRedTable(tableName, drTable);
                }

                if (isTopic) {
                    continue;
                }

                for (VoltXMLElement subNode : e.children) {
                    if (subNode.name.equals(PersistentExport.PERSISTENT_EXPORT)) {
                        String streamTarget = subNode.attributes.get("target");
                        m_tracker.addExportedTable(tableName, streamTarget, false);
                        break;
                    }
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

    // To indicate if inside multi statement procedure
    private static boolean inAsBegin = false;
    // To indicate if inside CASE .. WHEN .. END
    private static int inCaseWhen = 0;
    // BEGIN should follow AS for create procedure
    // added this case since 'begin' can be table or column name
    private static boolean checkForNextBegin = false;
    // inBegin and checkForNextBegin are not k-state values since we cannot have BEGIN within a BEGIN
    // also we check for next BEGIN only after AS. If the next token after AS is not BEGIN,
    // then we need not store the state (i.e, checkForNextBegin)

    private static int readingState(char[] nchar, DDLStatement retval) {

        if ( ! Character.isUnicodeIdentifierPart(nchar[0])) {
            char prev = prevChar(retval.statement);
            /* Since we only have access to the current character and the characters we have seen so far,
             * we can check for the token only after its completed and then look if it matches the required token. */
            if (checkForNextBegin) {
                if (prev == 'n' || prev == 'N') {
                    if (SQLLexer.matchToken(retval.statement, retval.statement.length() - 5, "begin") ) {
                        inAsBegin = true;
                    }
                }
                checkForNextBegin = false;
            }
            if (prev == 'd' || prev == 'D') {
                if (SQLLexer.matchToken(retval.statement, retval.statement.length() - 3, "end") ) {
                    if (inCaseWhen > 0) {
                        inCaseWhen--;
                    } else {
                        // we can terminate BEGIN ... END for multi stmt proc
                        // after all CASE ... END stmts are completed
                        inAsBegin = false;
                    }
                }
            } else if (prev == 'e' || prev == 'E') {
                if (SQLLexer.matchToken(retval.statement, retval.statement.length() - 4, "case") ) {
                    inCaseWhen++;
                }
            } else if (prev == 's' || prev == 'S') {
                if (SQLLexer.matchToken(retval.statement, retval.statement.length() - 2, "as") ) {
                    checkForNextBegin = true;
                }
            }
        }
        if (nchar[0] == '-') {
            // remember that a possible '--' is being examined
            return kStateReadingCommentDelim;
        } else if (nchar[0] == '\n') {
            // normalize newlines to spaces
            retval.endLineNo += 1;
            retval.statement += " ";
        } else if (nchar[0] == '\r') {
            // ignore carriage returns
        } else if (nchar[0] == ';') {
            // end of the statement
            retval.statement += nchar[0];
            // statement completed only if outside of begin..end
            if(!inAsBegin) {
                return kStateCompleteStatement;
            }
        } else if (nchar[0] == '\'') {
            retval.statement += nchar[0];
            return kStateReadingStringLiteral;
        } else if (SQLLexer.isBlockDelimiter(nchar[0])) {
            // we may be examining ### code block delimiters
            retval.statement += nchar[0];
            return kStateReadingCodeBlockDelim;
        } else {
            // accumulate and continue
            retval.statement += nchar[0];
        }

        return kStateReading;
    }

    private static char prevChar(String str) {
        return str.charAt(str.length() - 1);
    }

    private static int readingCodeBlockStateDelim(char [] nchar, DDLStatement retval) {
        retval.statement += nchar[0];
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            return kStateReadingCodeBlockNextDelim;
        } else {
            return readingState(nchar, retval);
        }
    }

    private static int readingEndCodeBlockStateDelim(char [] nchar, DDLStatement retval) {
        retval.statement += nchar[0];
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            return kStateReadingEndCodeBlockNextDelim;
        } else {
            return kStateReadingCodeBlock;
        }
    }

    private static int readingCodeBlockStateNextDelim(char [] nchar, DDLStatement retval) {
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            retval.statement += nchar[0];
            return kStateReadingCodeBlock;
        } else {
            return readingState(nchar, retval);
        }
    }

    private static int readingEndCodeBlockStateNextDelim(char [] nchar, DDLStatement retval) {
        retval.statement += nchar[0];
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            return kStateReading;
        } else {
            return kStateReadingCodeBlock;
        }
    }

    private static int readingCodeBlock(char [] nchar, DDLStatement retval) {
        // all characters in the literal are accumulated. keep track of
        // newlines for error messages.
        retval.statement += nchar[0];
        if (SQLLexer.isBlockDelimiter(nchar[0])) {
            return kStateReadingEndCodeBlockDelim;
        }

        if (nchar[0] == '\n') {
            retval.endLineNo += 1;
        }
        return kStateReadingCodeBlock;
    }

    private static int readingStringLiteralState(char[] nchar, DDLStatement retval) {
        // all characters in the literal are accumulated. keep track of
        // newlines for error messages.
        retval.statement += nchar[0];
        if (nchar[0] == '\n') {
            retval.endLineNo += 1;
        }

        // if we see a SINGLE_QUOTE, change states to check for terminating literal
        if (nchar[0] != '\'') {
            return kStateReadingStringLiteral;
        } else {
            return kStateReadingStringLiteralSpecialChar;
        }
    }


    private static int readingStringLiteralSpecialChar(char[] nchar, DDLStatement retval) {

        // if this is an escaped quote, return kReadingStringLiteral.
        // otherwise, the string is complete. Parse nchar as a non-literal
        if (nchar[0] == '\'') {
            retval.statement += nchar[0];
            return kStateReadingStringLiteral;
        } else {
            return readingState(nchar, retval);
        }
    }

    private static int readingCommentDelimState(char[] nchar, DDLStatement retval) {
        if (nchar[0] == '-') {
            // confirmed that a comment is being read
            return kStateReadingComment;
        } else {
            // need to append the previously skipped '-' to the statement
            // and process the current character
            retval.statement += '-';
            return readingState(nchar, retval);
        }
    }

    private static int readingCommentState(char[] nchar, DDLStatement retval) {
        if (nchar[0] == '\n') {
            // a comment is continued until a newline is found.
            retval.endLineNo += 1;
            return kStateReading;
        } else {
            return kStateReadingComment;
        }
    }

    public static DDLStatement getNextStatement(Reader reader, VoltCompiler compiler, int currLineNo, boolean newDdl)
            throws VoltCompiler.VoltCompilerException {

        int state = kStateInvalid;

        char[] nchar = new char[1];
        @SuppressWarnings("synthetic-access")
        DDLStatement retval = new DDLStatement(newDdl);

        try {

            // find the start of a statement and break out of the loop
            // or return null if there is no next statement to be found
            do {
                if (reader.read(nchar) == -1) {
                    return null;
                }

                // trim leading whitespace outside of a statement
                if (nchar[0] == '\n') {
                    currLineNo++;
                } else if (nchar[0] == '\r') {
                } else if (nchar[0] == ' ') {
                } else if (nchar[0] == '-') { // trim leading comments outside of a statement
                    // The next character must be a comment because no valid
                    // statement will start with "-<foo>". If a comment was
                    // found, read until the next newline.
                    if (reader.read(nchar) == -1) {
                        // garbage at the end of a file but easy to tolerable?
                        return null;
                    }
                    if (nchar[0] != '-') {
                        String msg = "Invalid content before or between DDL statements.";
                        throw compiler.new VoltCompilerException(msg, currLineNo);
                    } else {
                        do {
                            if (reader.read(nchar) == -1) {
                                // a comment extending to EOF means no statement
                                return null;
                            }
                        } while (nchar[0] != '\n');

                        // process the newline and loop
                        currLineNo++;
                    }
                } else { // not whitespace or comment: start of a statement.
                    retval.statement += nchar[0];
                    state = kStateReading;
                    break;
                }
            } while (true);

            // Set the line number to the start of the real statement.
            retval.lineNo = currLineNo;
            retval.endLineNo = currLineNo;
            inAsBegin = false;
            inCaseWhen = 0;
            checkForNextBegin = false;

            while (state != kStateCompleteStatement) {
                if (reader.read(nchar) == -1) {
                    // might be useful for the users for debugging if we include the statement which caused the error
                    String msg = "Schema file ended mid-statement (no semicolon found) for statment: " + retval.statement;
                    throw compiler.new VoltCompilerException(msg, retval.lineNo);
                }

                if (state == kStateReading) {
                    state = readingState(nchar, retval);
                } else if (state == kStateReadingCommentDelim) {
                    state = readingCommentDelimState(nchar, retval);
                } else if (state == kStateReadingComment) {
                    state = readingCommentState(nchar, retval);
                } else if (state == kStateReadingStringLiteral) {
                    state = readingStringLiteralState(nchar, retval);
                } else if (state == kStateReadingStringLiteralSpecialChar) {
                    state = readingStringLiteralSpecialChar(nchar, retval);
                } else if (state == kStateReadingCodeBlockDelim) {
                    state = readingCodeBlockStateDelim(nchar, retval);
                } else if (state == kStateReadingCodeBlockNextDelim) {
                    state = readingCodeBlockStateNextDelim(nchar, retval);
                } else if (state == kStateReadingCodeBlock) {
                    state = readingCodeBlock(nchar, retval);
                } else if (state == kStateReadingEndCodeBlockDelim) {
                    state = readingEndCodeBlockStateDelim(nchar, retval);
                } else if (state == kStateReadingEndCodeBlockNextDelim) {
                    state = readingEndCodeBlockStateNextDelim(nchar, retval);
                } else {
                    throw compiler.new VoltCompilerException("Unrecoverable error parsing DDL.");
                }
            }

            return retval;
        } catch (IOException e) {
            throw compiler.new VoltCompilerException("Unable to read from file");
        }
    }

    private void addTableToCatalog(Database db, VoltXMLElement node, boolean isXDCR) throws VoltCompilerException {
        assert node.name.equals("table");

        // Construct table-specific maps
        HashMap<String, Column> columnMap = new HashMap<>();
        HashMap<String, Index> indexMap = new HashMap<>();

        final String name = node.attributes.get("name");
        if (AdHocNTBase.USING_CALCITE &&    // Skip "CREATE TABLE" if we have already added it using Calcite parser.
                StreamSupport.stream(((Iterable<Table>) () -> db.getTables().iterator()).spliterator(), false)
                        .anyMatch(tbl -> tbl.getTypeName().equals(name))) {
            // Code below this point is not executed any more. See VoltCompiler#compileDatabase() for
            // how CREATE TABLE statement is processed by Calcite.
            return;
        }

        // create a table node in the catalog, persistent until we find otherwise
        final Table table = db.getTables().add(name);
        table.setTabletype(TableType.PERSISTENT.get());

        // add the original DDL to the table (or null if it's not there)
        TableAnnotation annotation = new TableAnnotation();
        table.setAnnotation(annotation);

        // handle the case where this is a materialized view
        final String query = node.attributes.get("query");
        if (query != null) {
            assert(query.length() > 0);
            m_matViewMap.put(table, query);
        }

        // get the stream-related information
        final boolean isStream = Boolean.parseBoolean(node.attributes.get("stream"));
        String streamTarget = node.attributes.get("export");
        final String streamPartitionColumn = node.attributes.get("partitioncolumn");

        /*
         * FIXME: the check below is disabled because it breaks auto-generated EE tests
         * See ENG-18737
         *
         * if (!VoltDB.instance().getConfig().m_isEnterprise) {
         *   throw m_compiler.new VoltCompilerException(
         *          String.format("STREAM %s cannot be declared TOPIC in community edition", name));
         * }
         */

        // all tables start replicated
        // if a partition is found in the project file later,
        //  then this is reversed;
        // But the index creation needs to know if the table is replicated, and coerce
        // any ASSUMEUNIQUE index to be UNIQUE index on replicated table. Therefore, we
        // set it according to current DDL state, then recheck table.m_isreplicated in handlePartitions().
        table.setIsreplicated(!node.attributes.containsKey("partitioncolumn"));

        // map of index replacements for later constraint fixup
        final Map<String, String> indexReplacementMap = new TreeMap<>();

        // Need the columnTypes sorted by column index.
        SortedMap<Integer, VoltType> columnTypes = new TreeMap<>();
        VoltXMLElement ttlNode = null;
        for (VoltXMLElement subNode : node.children) {

            if (subNode.name.equals(PersistentExport.PERSISTENT_EXPORT)) {
                // indicates EXPORT TO TARGET|TOPIC
                boolean isTopic = Boolean.parseBoolean(subNode.attributes.get("isTopic"));
                if (isTopic && node.attributes.get("topicName") == null) {
                    throw m_compiler.new VoltCompilerException("XML error, EXPORT TO TOPIC but no topic name");
                }
                streamTarget = subNode.attributes.get("target");
                List<String> items= Stream.of(subNode.attributes.get("triggers").split(","))
                        .map(String::trim)
                        .collect(Collectors.toList());
                int tblType = TableType.getPersistentExportTrigger(items);
                table.setTabletype(tblType);
            }

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

                // Do the system indexes first, since we don't want to
                // drop them: there are constraint objects in the catalog
                // that refer to them.
                for (VoltXMLElement indexNode : subNode.children) {
                    if (!indexNode.name.equals("index")) {
                        continue;
                    }
                    String indexName = indexNode.attributes.get("name");
                    if (!indexName.startsWith(HSQLInterface.AUTO_GEN_IDX_PREFIX)) {
                        addIndexToCatalog(db, table, indexNode, indexReplacementMap,
                                indexMap, columnMap, m_compiler);
                    }
                }

                for (VoltXMLElement indexNode : subNode.children) {
                    if (!indexNode.name.equals("index")) {
                        continue;
                    }
                    String indexName = indexNode.attributes.get("name");
                    if (indexName.startsWith(HSQLInterface.AUTO_GEN_IDX_PREFIX)) {
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
            if (subNode.name.equalsIgnoreCase(TimeToLiveVoltDB.TTL_NAME)) {
                ttlNode = subNode;
            }
        }

        // If table is a stream, initialize stream type
        // If stream is a topic, it is created as a CONNECTOR_LESS_STREAM and will be changed to STREAM,
        // if the topic is found to exist (see CatalogUtil.setTopics).
        if (isStream) {
            if(streamTarget != null && !Constants.CONNECTORLESS_STREAM_TARGET_NAME.equals(streamTarget)) {
                table.setTabletype(TableType.STREAM.get());
            } else {
                table.setTabletype(TableType.CONNECTOR_LESS_STREAM.get());
            }
        }

        final String migratingIndexName =
                StreamSupport.stream(((Iterable<Index>) () -> table.getIndexes().iterator()).spliterator(), false)
                        .filter(Index::getMigrating)
                        .map(Index::getTypeName)
                        .findAny()
                        .orElse(null);

        // Warn user if DR table don't have any unique index.
        if (isXDCR &&
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
                String info = String.format("Table %s doesn't have any unique index, it will cause full table scans " +
                                "to update/delete DR record and may become slower as table grow.", table.getTypeName());
                m_compiler.addWarn(info);
            }
        }

        table.setSignature(CatalogUtil.getSignatureForTable(name, columnTypes));

        /*
         * Validate that each variable-length column is below the max value length,
         * and that the maximum size for the row is below the max row length.
         */
        boolean hasTimestamp = false;
        int maxRowSize = 0;
        for (Column c : columnMap.values()) {
            VoltType t = VoltType.get((byte)c.getType());
            if (t == VoltType.STRING && (! c.getInbytes())) {
                // A VARCHAR column whose size is defined in characters.

                if (c.getSize() * MAX_BYTES_PER_UTF8_CHARACTER > VoltType.MAX_VALUE_LENGTH) {
                    throw m_compiler.new VoltCompilerException("Column " + name + "." + c.getName() +
                            " specifies a maximum size of " + c.getSize() + " characters" +
                            " but the maximum supported size is " +
                            VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH / MAX_BYTES_PER_UTF8_CHARACTER) +
                            " characters or " + VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH) + " bytes");
                }
                maxRowSize += 4 + c.getSize() * MAX_BYTES_PER_UTF8_CHARACTER;
            } else if (t.isVariableLength()) {
                // A VARCHAR(<n> bytes) column, VARBINARY or GEOGRAPHY column.

                if (c.getSize() > VoltType.MAX_VALUE_LENGTH) {
                    throw m_compiler.new VoltCompilerException("Column " + name + "." + c.getName() +
                            " specifies a maximum size of " + c.getSize() + " bytes" +
                            " but the maximum supported size is " + VoltType.humanReadableSize(VoltType.MAX_VALUE_LENGTH));
                }
                maxRowSize += 4 + c.getSize();
            } else {
                maxRowSize += t.getLengthInBytesForFixedTypes();
            }
            hasTimestamp |= t == VoltType.TIMESTAMP;
        }

        if (maxRowSize > MAX_ROW_SIZE) {
            throw m_compiler.new VoltCompilerException("Error: Table " + name + " has a maximum row size of " + maxRowSize +
                    " but the maximum supported row size is " + MAX_ROW_SIZE);
        }

        // MIGRATE TO TARGET|TOPIC
        final String migrationTarget = node.attributes.get("migrateExport");
        if (!StringUtil.isEmpty(migrationTarget)) {
            table.setMigrationtarget(migrationTarget);
            table.setTabletype(TableType.PERSISTENT_MIGRATE.get());
        } else if (migratingIndexName != null) {
            throw m_compiler.new VoltCompilerException(
                    String.format("Cannot create migrating index \"%s\" on non-migrating table \"%s\"",
                            migratingIndexName, name));
        }

        // WITH KEY/VALUE
        String topicName = node.attributes.get("topicName");
        if (!StringUtils.isBlank(topicName)) {
            table.setTopicname(topicName);

            // Set the optional clauses
            Map<String, String> columnProperties = new HashMap<>();
            String keyColumns = node.attributes.get("topicKeyColumnNames");
            if (!StringUtils.isBlank(keyColumns)) {
                table.setTopickeycolumnnames(keyColumns);
                columnProperties.put(TopicProperties.Key.CONSUMER_KEY.name(), keyColumns);
            }
            String valueColumns = node.attributes.get("topicValueColumnNames");
            if (!StringUtils.isBlank(valueColumns)) {
                table.setTopicvaluecolumnnames(valueColumns);
                columnProperties.put(TopicProperties.Key.CONSUMER_VALUE.name(), valueColumns);
            }

            // Validate the topic columns by creating a TopicProperties instance parsing the column lists
            if (!columnProperties.isEmpty()) {
                TopicProperties topicProperties = new TopicProperties(columnProperties);
                validateTopicColumns(topicName, table.getTypeName(), isStream,
                        columnMap, topicProperties.get(TopicProperties.Key.CONSUMER_KEY));
                validateTopicColumns(topicName, table.getTypeName(), isStream,
                        columnMap, topicProperties.get(TopicProperties.Key.CONSUMER_VALUE));
            }
        }

        // USING TTL
        if (ttlNode != null) {
            TimeToLive ttl = table.getTimetolive().add(TimeToLiveVoltDB.TTL_NAME);
            String column = ttlNode.attributes.get("column");
            int ttlValue = Integer.parseInt(ttlNode.attributes.get("value"));
            ttl.setTtlunit(ttlNode.attributes.get("unit"));
            ttl.setTtlvalue(ttlValue);
            ttlValue = Integer.parseInt(ttlNode.attributes.get("batchSize"));
            ttl.setBatchsize(ttlValue);
            ttlValue = Integer.parseInt(ttlNode.attributes.get("maxFrequency"));
            ttl.setMaxfrequency(ttlValue);
            for (Column col : table.getColumns()) {
                if (column.equalsIgnoreCase(col.getName())) {
                    ttl.setTtlcolumn(col);
                    break;
                }
            }
        }

        // Temporarily assign the view Query to the annotation so we can use when we build
        // the DDL statement for the VIEW
        if (query != null) {
            annotation.ddl = query;
        } else {
            // Get the final DDL for the table rebuilt from the catalog object
            // Don't need a real StringBuilder or export state to get the CREATE for a table
            annotation.ddl = CatalogSchemaTools.toSchema(new StringBuilder(), table, query, isStream, streamPartitionColumn,
                    streamTarget, topicName);
        }
    }

    /**
     * Validate that the topic columns correspond to a existing columns in the {@link Table}
     * <p>
     * Note: Moved here from {@link TopicsValidator} to catch errors on CREATE STREAM
     * even if no topics are defined in deployment.
     *
     * @param topicName         name of topic
     * @param tableName         name of table
     * @param isStream          {@code true} if table is stream
     * @param tableColumns      map of columns defined in {@link Table}
     * @param selectedColumns   list of selected column names in topic
     * @throws VoltCompilerException
     */
    private void validateTopicColumns(String topicName, String tableName, boolean isStream,
            Map<String, Column> tableColumns, List<String> selectedColumns) throws VoltCompilerException {
        if (selectedColumns != null && !selectedColumns.isEmpty()) {
            // If there is only * in the list, go for all columns.
            if (TopicProperties.ALL_COLUMNS.equals(selectedColumns)) {
                return;
            }

            for (String columnName : selectedColumns) {
                if (tableColumns.get(columnName) == null) {
                    if (ExportRowSchema.isMetadataColumn(columnName)) {
                        // Accept metadata column names. TopicsValidator will complement
                        // the checks once topic and table/stream are connected
                        continue;
                    }
                    String tableType = isStream ? "stream" : "table";
                     throw m_compiler.new VoltCompilerException(String.format(
                            "Invalid topic %s: column %s specified in WITH clause is not defined in %s %s",
                            topicName, columnName, tableType, tableName));
                 }
            }
        }
    }


    private static void addColumnToCatalog(Table table,
                            VoltXMLElement node,
                            SortedMap<Integer, VoltType> columnTypes,
                            Map<String, Column> columnMap,
                            VoltCompiler compiler) throws VoltCompilerException {
        assert node.name.equals("column");

        String name = node.attributes.get("name");
        String typename = node.attributes.get("valuetype");
        String nullable = node.attributes.get("nullable");
        String sizeString = node.attributes.get("size");
        int index = Integer.parseInt(node.attributes.get("index"));
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
            } else {
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
        if (defaulttype != null) {
            column.setDefaulttype(Integer.parseInt(defaulttype));
        }

        columnMap.put(name, column);
    }

    /**
     * Return true if the two indexes are identical with a different name.
     */
    private static boolean indexesAreDups(Index idx1, Index idx2) {
        // same attributes?
        if (idx1.getType() != idx2.getType() ||
                idx1.getCountable() != idx2.getCountable() ||
                idx1.getUnique() != idx2.getUnique() ||
                idx1.getAssumeunique() != idx2.getAssumeunique()) {
            return false;
        } else if (idx1.getColumns().size() != idx2.getColumns().size()) { // same column count?
            return false;
        } else if ( ! idx1.getExpressionsjson().equals(idx2.getExpressionsjson())) {
            //TODO: For index types like HASH that support only random access vs. scanned ranges, indexes on different
            // permutations of the same list of columns/expressions could be considered dupes. This code skips that edge
            // case optimization in favor of using a simpler more exact permutation-sensitive algorithm for all indexes.
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
        } else if (idx1.getPredicatejson().length() > 0) {
            return idx1.getPredicatejson().equals(idx2.getPredicatejson());
        } else if (idx2.getPredicatejson().length() > 0) {
            return idx2.getPredicatejson().equals(idx1.getPredicatejson());
        } else {
            return true;
        }
    }

    private static void addIndexToCatalog(Database db,
            Table table,
            VoltXMLElement node,
            Map<String, String> indexReplacementMap,
            HashMap<String, Index> indexMap,
            HashMap<String, Column> columnMap,
            VoltCompiler compiler)
            throws VoltCompilerException {
        assert node.name.equals("index");

        String name = node.attributes.get("name");
        final boolean unique = Boolean.parseBoolean(node.attributes.get("unique"));
        boolean assumeUnique = Boolean.parseBoolean(node.attributes.get("assumeunique"));
        boolean isMigrating = Boolean.parseBoolean(node.getStringAttribute("migrating", "false"));

        AbstractParsedStmt dummy = new ParsedSelectStmt(null, null, db);
        dummy.setDDLIndexedTable(table);
        StringBuffer msg = new StringBuffer(String.format("Index \"%s\" ", name));
        // "parse" the expression trees for an expression-based index (vs. a simple column value index)
        List<AbstractExpression> exprs = null;
        // "parse" the WHERE expression for partial index if any
        AbstractExpression predicate = null;
        // Some expressions have special validation in indices.  Not all the expression
        // can be indexed. We scan for result type at first here and block those which
        // can't be indexed like boolean, geo ... We gather rest of expression into
        // checkExpressions list.  We will check on them all at once.
        List<AbstractExpression> checkExpressions = new ArrayList<>();
        final UnsafeOperatorsForDDL unsafeOps = new UnsafeOperatorsForDDL();
        for (VoltXMLElement subNode : node.children) {
            if (subNode.name.equals("exprs")) {
                exprs = new ArrayList<>();
                for (VoltXMLElement exprNode : subNode.children) {
                    AbstractExpression expr = dummy.parseExpressionTree(exprNode);
                    expr.resolveForTable(table);
                    expr.finalizeValueTypes();
                    // string will be populated with an expression's details when
                    // its value type is not indexable
                    StringBuffer exprMsg = new StringBuffer();
                    if (!expr.isValueTypeIndexable(exprMsg)) {
                        // indexing on expression with boolean result is not supported.
                        throw compiler.new VoltCompilerException("Cannot create index \""+ name +
                                "\" because it contains " + exprMsg + ", which is not supported.");
                    } else if ((unique || assumeUnique) && !expr.isValueTypeUniqueIndexable(exprMsg)) {
                        // indexing on expression with boolean result is not supported.
                        throw compiler.new VoltCompilerException("Cannot create unique index \""+ name +
                                "\" because it contains " + exprMsg + ", which is not supported.");
                    }
                    // rest of the validity guards will be evaluated after collecting all the expressions.
                    checkExpressions.add(expr);
                    exprs.add(expr);
                }
            } else if (subNode.name.equals("predicate")) {
                assert(subNode.children.size() == 1);
                VoltXMLElement predicateXML = subNode.children.get(0);
                assert(predicateXML != null);
                predicate = buildPartialIndexPredicate(dummy, name,
                        predicateXML, table, compiler);
                predicate.findUnsafeOperatorsForDDL(unsafeOps);
            }
        }

        // Check all the subexpressions we gathered up.
        if (!AbstractExpression.validateExprsForIndexesAndMVs(checkExpressions, msg, false)) {
            // The error message will be in the StringBuffer msg.
            throw compiler.new VoltCompilerException(msg.toString());
        }
        String colList = node.attributes.get("columns");
        String[] colNames = colList.split(",");
        Column[] columns = new Column[colNames.length];
        boolean has_nonint_col = false;
        boolean has_geo_col = false;
        String nonint_col_name = null;

        for (int i = 0; i < colNames.length; i++) {
            columns[i] = columnMap.get(colNames[i]);
            //  Allow creation of empty migrating index which has no explicit columns.
            if (!isMigrating && columns[i] == null) {
                return;
            }
        }

        // Empty columns of migrating index ==> colNames = {""}
        if (exprs == null && (!isMigrating || colNames.length > 1)) {
            for (int i = 0; i < colNames.length; i++) {
                VoltType colType = VoltType.get((byte) columns[i].getType());

                if (!colType.isIndexable()) {
                    String emsg = "Cannot create index \"" + name + "\" because " +
                            colType.getName() + " values are not currently supported as index keys: \"" + colNames[i] + "\"";
                    throw compiler.new VoltCompilerException(emsg);
                } else if ((unique || assumeUnique) && !colType.isUniqueIndexable()) {
                    String emsg = "Cannot create index \"" + name + "\" because " +
                            colType.getName() + " values are not currently supported as unique index keys: \"" + colNames[i] + "\"";
                    throw compiler.new VoltCompilerException(emsg);
                } else if (!colType.isBackendIntegerType()) {
                    has_nonint_col = true;
                    nonint_col_name = colNames[i];
                    has_geo_col = colType.equals(VoltType.GEOGRAPHY);
                    if (has_geo_col && colNames.length > 1) {
                        String emsg = "Cannot create index \"" + name + "\" because " +
                                colType.getName() + " values must be the only component of an index key: \"" + nonint_col_name + "\"";
                        throw compiler.new VoltCompilerException(emsg);
                    }
                }
            }
        } else if (exprs != null) {
            for (AbstractExpression expression : exprs) {
                VoltType colType = expression.getValueType();

                if (! colType.isIndexable()) {
                    String emsg = "Cannot create index \""+ name + "\" because " +
                                colType.getName() + " valued expressions are not currently supported as index keys.";
                    throw compiler.new VoltCompilerException(emsg);
                } else if ((unique || assumeUnique) && ! colType.isUniqueIndexable()) {
                    String emsg = "Cannot create index \""+ name + "\" because " +
                                colType.getName() + " valued expressions are not currently supported as unique index keys.";
                    throw compiler.new VoltCompilerException(emsg);
                } else if (! colType.isBackendIntegerType()) {
                    has_nonint_col = true;
                    nonint_col_name = "<expression>";
                    has_geo_col = colType.equals(VoltType.GEOGRAPHY);
                    if (has_geo_col) {
                        if (exprs.size() > 1) {
                            String emsg = "Cannot create index \""+ name + "\" because " +
                                        colType.getName() + " values must be the only component of an index key.";
                            throw compiler.new VoltCompilerException(emsg);
                        } else if (!(expression instanceof TupleValueExpression)) {
                            String emsg = "Cannot create index \"" + name + "\" because " +
                                        colType.getName() + " expressions must be simple column expressions.";
                            throw compiler.new VoltCompilerException(emsg);
                        }
                    }
                }
                expression.findUnsafeOperatorsForDDL(unsafeOps);
            }
        } else {
            assert isMigrating && colNames.length == 1 :
                    "Encountered with an index that does not include columns or expressions";
        }

        Index index = table.getIndexes().add(name);
        index.setCountable(false);
        index.setIssafewithnonemptysources(! unsafeOps.isUnsafe());

        // Set the index type.  It will be one of:
        // - Covering cell index (geo index for CONTAINS predicates)
        // - HASH index (set in HSQL because "hash" is in the name of the
        //   constraint or the index
        // - TREE index, which is the default
        boolean isHashIndex = node.attributes.get("ishashindex").equals("true");
        if (has_geo_col) {
            index.setType(IndexType.COVERING_CELL_INDEX.getValue());
        } else if (isHashIndex) {
            // warn user that hash index will be deprecated
            compiler.addWarn("Hash indexes are deprecated. In a future release, VoltDB will only support tree indexes, even if the index name contains the string \"hash\"");

            // If the column type is not an integer, we cannot
            // make the index a hash.
            if (has_nonint_col) {
                String emsg = "Index " + name + " in table " + table.getTypeName() +
                             " uses a non-hashable column " + nonint_col_name;
                throw compiler.new VoltCompilerException(emsg);
            }
            index.setType(IndexType.HASH_TABLE.getValue());
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
        if (columns.length > 1 || columns[0] != null) {
            for (int i = 0; i < columns.length; i++) {
                ColumnRef cref = index.getColumns().add(columns[i].getTypeName());
                cref.setColumn(columns[i]);
                cref.setIndex(i);
            }
        }

        if (exprs != null) {
            try {
                index.setExpressionsjson(convertToJSONArray(exprs));
            } catch (JSONException e) {
                throw compiler.new VoltCompilerException("Unexpected error serializing non-column expressions for index '" +
                        name + "' on type '" + table.getTypeName() + "': " + e.toString());
            }
        }

        // For non-migrating index, we need to check whether the predicate contains a 'NOT MIGRATING' piece, and
        // set the isMigrating flag in the catalog if it does. We only check the leaf node for simplicity, meaning that
        // there could possibly be misses/false alarms for complex predicate like `WHERE NOT (MIGRATING AND a > 0)`,
        // `WHERE NOT MIGRATING OR a > 0`, or `WHERE NOT NOT MIGRATING`, etc.
        isMigrating |= ExpressionUtil.reduce(predicate, NOT_MIGRATING::equals);
        index.setUnique(unique);
        if (! index.getTypeName().startsWith(HSQLInterface.AUTO_GEN_PREFIX) && table.getIsreplicated() && assumeUnique) {
            // Warn and convert AssumeUnique -> Unique index only on
            // non-auto-generated index (i.e. from "CREATE ASSUMEUNIQUE INDEX ..." statement rather than "CREATE TABLE(...)" statement),
            // because "PARTITION TABLE ..." statement cannot be specified together with "CREATE TABLE" statement!
            final String warn = String.format(
                    "Converting ASSUMEUNIQUE index %s to UNIQUE because table %s is a replicated table.",
                    index.getTypeName(), table.getTypeName());
            compiler.addWarn(warn);
            System.err.println(warn);
            assumeUnique = false;
        } else if (assumeUnique) {
            index.setUnique(true);
        }
        index.setAssumeunique(assumeUnique);
        index.setMigrating(isMigrating);

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
                if (! index.getTypeName().startsWith(HSQLInterface.AUTO_GEN_PREFIX)) {
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

    public static String convertToJSONArray(List<AbstractExpression> exprs) throws JSONException {
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

    public static String convertToJSONObject(AbstractExpression expr) throws JSONException {
        JSONStringer stringer = new JSONStringer();
        stringer.object();
        expr.toJSONString(stringer);
        stringer.endObject();
        return stringer.toString();
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
            Map<String, Index> indexMap) throws VoltCompilerException {
        assert node.name.equals("constraint");

        String name = node.attributes.get("name");
        String typeName = node.attributes.get("constrainttype");
        ConstraintType type = ConstraintType.valueOf(typeName);
        String tableName = table.getTypeName();

        switch (type) {
            case CHECK:
                m_compiler.addWarn("VoltDB does not enforce check constraints. " +
                        "Constraint on table " + tableName + " will be ignored.");
                return;
            case FOREIGN_KEY:
                m_compiler.addWarn("VoltDB does not enforce foreign key references and constraints. " +
                        "Constraint on table " + tableName + " will be ignored.");
                return;
            case MAIN:
                // should never see these
                assert(false);
            case NOT_NULL:
                // these get handled by table metadata inspection
                return;
            default:
                if (type != ConstraintType.PRIMARY_KEY &&  type != ConstraintType.UNIQUE) {
                    throw m_compiler.new VoltCompilerException("Invalid constraint type '" + typeName + "'");
                }
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
        StringBuffer msg = new StringBuffer("Partial index \"" + indexName + "\" ");

        // Make sure all column expressions refer the index table
        List<VoltXMLElement> columnRefs= predicateXML.findChildrenRecursively("columnref");
        for (VoltXMLElement columnRef : columnRefs) {
            String columnRefTableName = columnRef.attributes.get("table");
            if (columnRefTableName != null && !tableName.equals(columnRefTableName)) {
                msg.append("with expression(s) involving other tables is not supported.");
                throw compiler.new VoltCompilerException(msg.toString());
            }
        }
        // Now it safe to parse the expression tree
        AbstractExpression predicate = dummy.parseExpressionTree(predicateXML);
        if ( ! predicate.isValidExprForIndexesAndMVs(msg, false) ) {
            throw compiler.new VoltCompilerException(msg.toString());
        } else {
            return predicate;
        }
    }

    public void processMaterializedViewWarnings(Database db) throws VoltCompilerException {
            m_mvProcessor.processMaterializedViewWarnings(db, m_matViewMap);
    }

    private void processVoltDBStatements(
            final Database db, final DdlProceduresToLoad whichProcs,DDLStatement stmt) throws VoltCompilerException {
        boolean processed = false;
        try {
            // Process a VoltDB-specific DDL statement, like PARTITION, REPLICATE,
            // CREATE PROCEDURE, CREATE FUNCTION, CREATE ROLE,
            // and CREATE AGGREGATE FUNCTION.
            processed = m_voltStatementProcessor.process(stmt, db, whichProcs);
        } catch (VoltCompilerException e) {
            // Reformat the message thrown by VoltDB DDL processing to have a line number.
            String msg = "VoltDB DDL Error: \"" + e.getMessage() + "\" in statement starting on lineno: " + stmt.lineNo;
            throw m_compiler.new VoltCompilerException(msg);
        }

        if (! processed) {
            try {
                // figure out what table this DDL might affect to minimize diff processing
                HSQLDDLInfo ddlStmtInfo = HSQLLexer.preprocessHSQLDDL(stmt.statement);

                //* enable to debug */ System.out.println("DEBUG: " + stmt.statement);
                // kind of ugly.  We hex-encode each statement so we can
                // avoid embedded newlines so we can delimit statements
                // with newline.
                m_fullDDL += Encoder.hexEncode(stmt.statement) + "\n";

                // Get the diff that results from applying this statement and apply it
                // to our local tree (with Volt-specific additions)
                VoltXMLDiff thisStmtDiff = m_hsql.runDDLCommandAndDiff(ddlStmtInfo, stmt.statement);
                // null diff means no change (usually drop if exists for non-existent thing)
                if (thisStmtDiff != null) {
                    applyDiff(thisStmtDiff, db);
                }
                // special treatment for stream syntax
                if (ddlStmtInfo.creatStream) {
                    processCreateStreamStatement(stmt, db, whichProcs);
                }

                boolean createTable = ddlStmtInfo.verb.equals(HSQLDDLInfo.Verb.CREATE) &&
                        ddlStmtInfo.noun.equals(HSQLDDLInfo.Noun.TABLE);
                if (createTable) {
                    processCreateTableStatement(stmt);
                }

                boolean createView = ddlStmtInfo.verb.equals(HSQLDDLInfo.Verb.CREATE) &&
                        ddlStmtInfo.noun.equals(HSQLDDLInfo.Noun.VIEW);
                if (createView) {
                    processCreateViewStatement(stmt);
                }

            } catch (HSQLParseException e) {
                String msg = "DDL Error: \"" + e.getMessage() + "\" in statement starting on lineno: " + stmt.lineNo;
                throw m_compiler.new VoltCompilerException(msg, stmt.lineNo);
            }
        }
    }

    /**
     * Load the udfs from the previous DB into the m_oldFunctions.  These are not
     * actually defined, but they are where we can find them if we need them.  If
     * we create a UDF which is equal to the old UDF from the the catalog we will
     * get the function id from the catalog in this way, and reuse the old signature.
     *
     * This should really be in FunctionForVoltDB.  But we can't use the catalog
     * functions in HSQLDB.
     */
    public void saveDefinedFunctions() {
        FunctionForVoltDB.saveDefinedFunctions();
    }

    public void restoreSavedFunctions() {
        FunctionForVoltDB.restoreSavedFunctions();
    }

    public void clearSavedFunctions() {
        FunctionForVoltDB.clearSavedFunctions();
    }
}
