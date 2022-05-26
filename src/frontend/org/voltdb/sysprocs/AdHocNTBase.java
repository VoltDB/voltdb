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

package org.voltdb.sysprocs;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.catalog.Database;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.exceptions.PlanningErrorException;
import org.voltdb.parser.SQLLexer;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.plannerv2.SqlBatch;
import org.voltdb.plannerv2.guards.PlannerFallbackException;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTrace;

import com.google_voltpatches.common.base.Charsets;

/**
 * Base class for non-transactional sysprocs AdHoc, AdHocSPForTest and SwapTables
 *
 *
 */
public abstract class AdHocNTBase extends UpdateApplicationBase {

    protected static final VoltLogger adhocLog = new VoltLogger("ADHOC");

    public static final String AdHocErrorResponseMessage =
            "The @AdHoc stored procedure when called with more than one parameter "
                    + "must be passed a single parameterized SQL statement as its first parameter. "
                    + "Pass each parameterized SQL statement to a separate callProcedure invocation.";

    // When DEBUG_MODE is true this (valid) DDL string triggers an exception.
    // Public visibility allows it to be used from a unit test.
    public final static String DEBUG_EXCEPTION_DDL =
            "create table DEBUG_MODE_ENG_7653_crash_me_now (die varchar(7654) not null)";

    // Enable debug hooks when the "asynccompilerdebug" sys prop is set to "true" or "yes".
    protected final static MiscUtils.BooleanSystemProperty DEBUG_MODE =
            new MiscUtils.BooleanSystemProperty("asynccompilerdebug");

    /**
     * There are two ways to set planning with calcite: either by setting the JAVA properties; or by setting the
     * environment variable.
     *
     * The first way is used by build.xml to make Jenkins be Git branch aware, and only use Calcite planner when the
     * branch it is in contains "calcite-" string (case sensitive). You don't need to do anything to make it work that
     * way.
     *
     * The second way is needed for developer that run some quick test in sqlcmd, or need to run `startdb' locally.
     * The first way does not work here, because this is the easiest way to by pass Python scripts for starting
     * `bin/voltdb' without passing JVM variable down. To use it in `sqlcmd':
     *
     * $ export plan_with_calcite=true
     * $ startdb ... # or use developer script `kvdb', `vdb'
     * $ sqlcmd # the planner is using Calcite
     *
     * To use it within an IDE (IntelliJ for example), remember to add
     * plan_with_calcite=true
     * in Run -> Edit Configurations -> (VoltDB) -> Edit environment variables.
     *
     * Note that changes made to enviroment variable masks the mechanism how build.xml decides to pick planner based on
     * the Git branch name.
     */
    public static final boolean USING_CALCITE =
            Boolean.parseBoolean(System.getProperty("plan_with_calcite", "false")) ||
            Boolean.parseBoolean(System.getenv("plan_with_calcite"));

    BackendTarget m_backendTargetType = VoltDB.instance().getBackendTargetType();
    private final boolean m_isConfiguredForNonVoltDBBackend =
        m_backendTargetType == BackendTarget.HSQLDB_BACKEND ||
        m_backendTargetType == BackendTarget.POSTGRESQL_BACKEND ||
        m_backendTargetType == BackendTarget.POSTGIS_BACKEND;

    abstract protected CompletableFuture<ClientResponse> runUsingCalcite(ParameterSet params) throws SqlParseException;
    abstract protected CompletableFuture<ClientResponse> runUsingLegacy(ParameterSet params);
    // NOTE!!! Because we use reflection to find the run method of each concrete AdHocNTBase class, each of those
    // concrete methods must declare and implement the run() method. If instead we simply using run() from base class,
    // the reflection would not find it. Each overriding run() method that matches this signature can safely just
    // call runInternal() method.
    abstract public CompletableFuture<ClientResponse> run(ParameterSet params);
    protected CompletableFuture<ClientResponse> runInternal(ParameterSet params) {
        if (USING_CALCITE) {
            try {
                return runUsingCalcite(params);
            } catch (PlannerFallbackException | SqlParseException ex) { // Use the legacy planner to run this.
                return runUsingLegacy(params);
            }
        } else {
            return runUsingLegacy(params);
        }
    }

    /**
     * Log ad hoc batch info
     * @param batch planned statement batch
     */
    void logBatch(final CatalogContext context, final AdHocPlannedStmtBatch batch, final Object[] userParams) {
        final int numStmts = batch.getPlannedStatementCount();
        final int numParams = userParams == null ? 0 : userParams.length;
        final String readOnly = batch.readOnly ? "yes" : "no";
        final String singlePartition = batch.isSinglePartitionCompatible() ? "yes" : "no";
        final String user = getUsername();
        final List<String> groupNames = context.authSystem.getGroupNamesForUser(user);

        //String[] stmtArray = batch.stmts.stream().map(s -> new String(s.sql, Charsets.UTF_8)).toArray(String[]::new);

        adhocLog.debug(String.format(
            "=== statements=%d parameters=%d read-only=%s single-partition=%s user=%s groups=%s",
            numStmts, numParams, readOnly, singlePartition, user, groupNames));
        for (int i = 0; i < batch.getPlannedStatementCount(); i++) {
            AdHocPlannedStatement stmt = batch.getPlannedStatement(i);
            String sql = stmt.sql == null ? "SQL_UNKNOWN" : new String(stmt.sql, Charsets.UTF_8);
            adhocLog.debug(String.format("Statement #%d: %s", i + 1, sql));
        }
        if (userParams != null) {
            for (int i = 0; i < userParams.length; ++i) {
                Object value = userParams[i];
                final String valueString = (value != null ? value.toString() : "NULL");
                adhocLog.debug(String.format("Parameter #%d: %s", i + 1, valueString));
            }
        }
    }

    public enum AdHocSQLMix {
        EMPTY,
        ALL_DML_OR_DQL,
        ALL_DDL,
        MIXED
    }

    /**
     * Split a set of one or more semi-colon delimited sql statements into a list.
     * Store the list in validatedHomogeonousSQL.
     * Return whether the SQL is empty, all dml or dql, all ddl, or an invalid mix (AdHocSQLMix)
     *
     * Used by the NT adhoc procs, but also by the experimental in-proc adhoc.
     */
    public static AdHocSQLMix processAdHocSQLStmtTypes(String sql, List<String> validatedHomogeonousSQL) {
        assert(validatedHomogeonousSQL != null);
        assert(validatedHomogeonousSQL.size() == 0);

        List<String> sqlStatements = SQLLexer.splitStatements(sql).getCompletelyParsedStmts();

        // do initial naive scan of statements for DDL, forbid mixed DDL and (DML|DQL)
        Boolean hasDDL = null;

        for (String stmt : sqlStatements) {
            // Simulate an unhandled exception? (ENG-7653)
            if (DEBUG_MODE.isTrue() && stmt.equals(DEBUG_EXCEPTION_DDL)) {
                throw new IndexOutOfBoundsException(DEBUG_EXCEPTION_DDL);
            }

            String ddlToken = SQLLexer.extractDDLToken(stmt);

            if (hasDDL == null) {
                hasDDL = ddlToken != null;
            } else if ((hasDDL && ddlToken == null) || (!hasDDL && ddlToken != null)) {
                return AdHocSQLMix.MIXED;
            }

            validatedHomogeonousSQL.add(stmt);
        }

        if (validatedHomogeonousSQL.isEmpty()) {
            return AdHocSQLMix.EMPTY;
        } else {
            assert(hasDDL != null);
            return hasDDL ? AdHocSQLMix.ALL_DDL : AdHocSQLMix.ALL_DML_OR_DQL;
        }
    }

    /**
     * Compile a batch of one or more SQL statements into a set of plans.
     * Parameters are valid iff there is exactly one DML/DQL statement.
     */
    private static AdHocPlannedStatement compileAdHocSQL(
            PlannerTool plannerTool, String sqlStatement, boolean inferPartitioning,
            Object userPartitionKey, ExplainMode explainMode, boolean isLargeQuery,
            boolean isSwapTables, Object[] userParamSet) throws PlanningErrorException {
        assert(plannerTool != null);
        assert(sqlStatement != null);
        final PlannerTool ptool = plannerTool;

        // Take advantage of the planner optimization for inferring single partition work
        // when the batch has one statement.
        StatementPartitioning partitioning = null;

        if (inferPartitioning) {
            partitioning = StatementPartitioning.inferPartitioning();
        } else if (userPartitionKey == null) {
            partitioning = StatementPartitioning.forceMP();
        } else {
            partitioning = StatementPartitioning.forceSP();
        }

        try {
            return ptool.planSql(sqlStatement, partitioning, explainMode != ExplainMode.NONE,
                    userParamSet, isSwapTables, isLargeQuery);
        } catch (Exception e) {
            throw new PlanningErrorException(e.getMessage());
        } catch (StackOverflowError error) {
            // Overly long predicate expressions can cause a
            // StackOverflowError in various code paths that may be
            // covered by different StackOverflowError/Error/Throwable
            // catch blocks. The factors that determine which code path
            // and catch block get activated appears to be platform
            // sensitive for reasons we do not entirely understand.
            // To generate a deterministic error message regardless of
            // these factors, purposely defer StackOverflowError handling
            // for as long as possible, so that it can be handled
            // consistently by a minimum number of high level callers like
            // this one.
            // This eliminates the need to synchronize error message text
            // in multiple catch blocks, which becomes a problem when some
            // catch blocks lead to re-wrapping of exceptions which tends
            // to adorn the final error text in ways that are hard to track
            // and replicate.
            // Deferring StackOverflowError handling MAY mean ADDING
            // explicit StackOverflowError catch blocks that re-throw
            // the error to bypass more generic catch blocks
            // for Error or Throwable on the same try block.
            throw new PlanningErrorException("Encountered stack overflow error. " +
                    "Try reducing the number of predicate expressions in the query.");
        } catch (AssertionError ae) {
            String msg = "An unexpected internal error occurred when planning a statement issued via @AdHoc.  "
                    + "Please contact VoltDB at support@voltactivedata.com with your log files.";
            StringWriter stringWriter = new StringWriter();
            PrintWriter writer = new PrintWriter(stringWriter);
            ae.printStackTrace(writer);
            String stackTrace = stringWriter.toString();

            adhocLog.error(msg + "\n" + stackTrace);
            throw new PlanningErrorException(msg);
        }
    }

    /**
     * Plan and execute a batch of DML/DQL. Any DDL has been filtered out at this point.
     */
    protected CompletableFuture<ClientResponse> runNonDDLAdHoc(
            CatalogContext context, List<String> sqlStatements, boolean inferPartitioning,
            Object userPartitionKey, ExplainMode explainMode, boolean isLargeQuery,
            boolean isSwapTables, Object[] userParamSet) {
        // record the catalog version the query is planned against to
        // catch races vs. updateApplicationCatalog.
        if (context == null) {
            context = VoltDB.instance().getCatalogContext();
        }

        List<String> errorMsgs = new ArrayList<>();
        List<AdHocPlannedStatement> stmts = new ArrayList<>();
        int partitionParamIndex = -1;
        VoltType partitionParamType = null;
        Object partitionParamValue = null;
        assert(sqlStatements != null);

        boolean inferSP = (sqlStatements.size() == 1) && inferPartitioning;

        if (userParamSet != null && userParamSet.length > 0) {
            if (sqlStatements.size() != 1) {
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                                         AdHocErrorResponseMessage);
            }
        }

        for (final String sqlStatement : sqlStatements) {
            try {
                AdHocPlannedStatement result = compileAdHocSQL(
                        context.m_ptool, sqlStatement, inferSP, userPartitionKey, explainMode, isLargeQuery,
                        isSwapTables, userParamSet);
                // The planning tool may have optimized for the single partition case
                // and generated a partition parameter.
                if (inferSP) {
                    partitionParamIndex = result.getPartitioningParameterIndex();
                    partitionParamType = result.getPartitioningParameterType();
                    partitionParamValue = result.getPartitioningParameterValue();
                }
                stmts.add(result);
            } catch (PlanningErrorException e) {
                errorMsgs.add(e.getMessage());
            }
        }

        if (!errorMsgs.isEmpty()) {
            String errorSummary = StringUtils.join(errorMsgs, "\n");
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, errorSummary);
        }

        AdHocPlannedStmtBatch plannedStmtBatch =
                new AdHocPlannedStmtBatch(userParamSet, stmts, partitionParamIndex, partitionParamType,
                        partitionParamValue, userPartitionKey == null ? null : new Object[] { userPartitionKey });

        if (adhocLog.isDebugEnabled()) {
            logBatch(context, plannedStmtBatch, userParamSet);
        }

        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.CI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.endAsync("planadhoc", getClientHandle()));
        }

        if (explainMode == ExplainMode.EXPLAIN_ADHOC) {
            return processExplainPlannedStmtBatch(plannedStmtBatch);
        } else if (explainMode == ExplainMode.EXPLAIN_DEFAULT_PROC) {
            return processExplainDefaultProc(plannedStmtBatch);
        } else if (explainMode == ExplainMode.EXPLAIN_JSON) {
            return processExplainPlannedStmtBatchInJSON(plannedStmtBatch);
        } else {
            try {
                return createAdHocTransaction(plannedStmtBatch, isSwapTables);
            } catch (VoltTypeException vte) {
                String msg = "Unable to execute adhoc sql statement(s): " + vte.getMessage();
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, msg);
            }
        }
    }

    static CompletableFuture<ClientResponse> processExplainPlannedStmtBatch(AdHocPlannedStmtBatch planBatch) {
        /**
         * Take the response from the async ad hoc planning process and put the explain
         * plan in a table with the right format.
         */
        Database db = VoltDB.instance().getCatalogContext().database;
        int size = planBatch.getPlannedStatementCount();

        VoltTable[] vt = new VoltTable[ size ];
        for (int i = 0; i < size; ++i) {
            vt[i] = new VoltTable(new VoltTable.ColumnInfo("EXECUTION_PLAN", VoltType.STRING));
            String str = planBatch.explainStatement(i, db, false);
            vt[i].addRow(str);
        }

        ClientResponseImpl response =
                new ClientResponseImpl(ClientResponseImpl.SUCCESS, ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null, vt, null);

        CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        fut.complete(response);
        return fut;
    }

    static CompletableFuture<ClientResponse> processExplainPlannedStmtBatchInJSON(AdHocPlannedStmtBatch planBatch) {
        /**
         * Take the response from the async ad hoc planning process and put the explain
         * plan in a table with the right format.
         */
        Database db = VoltDB.instance().getCatalogContext().database;
        int size = planBatch.getPlannedStatementCount();

        VoltTable[] vt = new VoltTable[ size ];
        for (int i = 0; i < size; ++i) {
            vt[i] = new VoltTable(new VoltTable.ColumnInfo("JSON_PLAN", VoltType.STRING));
            String str = planBatch.explainStatement(i, db, true);
            vt[i].addRow(str);
        }

        ClientResponseImpl response =
                new ClientResponseImpl( ClientResponseImpl.SUCCESS, ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null, vt, null);

        CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        fut.complete(response);
        return fut;
    }

    /**
     * Explain Proc for a default proc is routed through the regular Explain
     * path using ad hoc planning and all. Take the result from that async
     * process and format it like other explains for procedures.
     */
    static CompletableFuture<ClientResponse> processExplainDefaultProc(AdHocPlannedStmtBatch planBatch) {
        Database db = VoltDB.instance().getCatalogContext().database;

        // there better be one statement if this is really SQL
        // from a default procedure
        assert(planBatch.getPlannedStatementCount() == 1);
        AdHocPlannedStatement ahps = planBatch.getPlannedStatement(0);
        String sql = new String(ahps.sql, StandardCharsets.UTF_8);
        String explain = planBatch.explainStatement(0, db, false);

        VoltTable vt = new VoltTable(new VoltTable.ColumnInfo("STATEMENT_NAME", VoltType.STRING),
                new VoltTable.ColumnInfo( "SQL_STATEMENT", VoltType.STRING),
                new VoltTable.ColumnInfo( "EXECUTION_PLAN", VoltType.STRING));
        vt.addRow("sql0", sql, explain);

        ClientResponseImpl response =
                new ClientResponseImpl(ClientResponseImpl.SUCCESS, ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null, new VoltTable[] { vt }, null);

        CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        fut.complete(response);
        return fut;
    }

    /**
     * Take a set of adhoc plans and pass them off to the right transactional
     * adhoc variant.
     */
    final CompletableFuture<ClientResponse> createAdHocTransaction(
            final AdHocPlannedStmtBatch plannedStmtBatch,
            final boolean isSwapTables) throws VoltTypeException {
        ByteBuffer buf = null;
        try {
            buf = plannedStmtBatch.flattenPlanArrayToBuffer();
        } catch (IOException e) {
            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
        }
        assert(buf.hasArray());

        // create the execution site task
        String procedureName = null;
        Object[] params = null;

        // pick the sysproc based on the presence of partition info
        // HSQL (or PostgreSQL) does not specifically implement AdHoc SP
        // -- instead, use its always-SP implementation of AdHoc
        boolean isSinglePartition = plannedStmtBatch.isSinglePartitionCompatible() || m_isConfiguredForNonVoltDBBackend;

        if (isSwapTables) {
            procedureName = "@SwapTablesCore";
            params = new Object[] { buf.array() };
        } else if (isSinglePartition) {
            if (plannedStmtBatch.isReadOnly()) {
                procedureName = "@AdHoc_RO_SP";
            } else {
                procedureName = "@AdHoc_RW_SP";
            }
            int type = VoltType.NULL.getValue();
            // replicated table read is single-part without a partitioning param
            // I copied this from below, but I'm not convinced that the above statement is correct
            // or that the null behavior here either (a) ever actually happens or (b) has the
            // desired intent.
            Object partitionParam = plannedStmtBatch.partitionParam();
            byte[] param = null;
            if (partitionParam != null) {
                type = VoltType.typeFromClass(partitionParam.getClass()).getValue();
                param = VoltType.valueToBytes(partitionParam);
            }

            // Send the partitioning parameter and its type along so that the site can check if
            // it's mis-partitioned. Type is needed to re-hashinate for command log re-init.
            params = new Object[] { param, (byte)type, buf.array() };
        } else {
            if (plannedStmtBatch.isReadOnly()) {
                procedureName = "@AdHoc_RO_MP";
            } else {
                procedureName = "@AdHoc_RW_MP";
            }
            params = new Object[] { buf.array() };
        }

        return callProcedure(procedureName, params);
    }

    public static AdHocPlannedStmtBatch plan(PlannerTool ptool, String sql, Object[] userParams, boolean singlePartition)
            throws PlanningErrorException {
        List<String> sqlStatements = new ArrayList<>();
        AdHocSQLMix mix = processAdHocSQLStmtTypes(sql, sqlStatements);

        switch (mix) {
        case EMPTY:
            throw new PlanningErrorException("No valid SQL found.");
        case ALL_DDL:
        case MIXED:
            throw new PlanningErrorException("DDL not supported in stored procedures.");
        default:
            break;
        }

        if (sqlStatements.size() != 1) {
            throw new PlanningErrorException("Only one statement is allowed in stored procedure, but received " + sqlStatements.size());
        }

        sql = sqlStatements.get(0);

        // any object will signify SP
        Object partitionKey = singlePartition ? "1" : null;

        List<AdHocPlannedStatement> stmts = new ArrayList<>();
        AdHocPlannedStatement result = compileAdHocSQL(ptool, sql, false, partitionKey,
                ExplainMode.NONE, false, false, userParams);
        stmts.add(result);

        return new AdHocPlannedStmtBatch(userParams, stmts, -1, null, null, userParams);
    }

    /**
     * The {@link org.voltdb.plannerv2.SqlBatch} was designed to be
     * self-contained. However, this is not entirely true due to the way that
     * the legacy code was organized. Until I have further reshaped the legacy
     * code path, I will leave this interface to call back into the private
     * methods of {@link org.voltdb.sysprocs.AdHocNTBase}.
     */
    class AdHocNTBaseContext implements SqlBatch.Context {

        @Override public CompletableFuture<ClientResponse> runDDLBatch(
        List<String> sqlStatements, List<SqlNode> sqlNodes) {
            throw new UnsupportedOperationException("Not Implemented.");
        }

        @Override public void logBatch(CatalogContext context, AdHocPlannedStmtBatch batch, Object[] userParams) {
            AdHocNTBase.this.logBatch(context, batch, userParams);
        }

        @Override public VoltLogger getLogger() {
            return adhocLog;
        }

        @Override public long getClientHandle() {
            return AdHocNTBase.this.getClientHandle();
        }

        @Override public CompletableFuture<ClientResponse> createAdHocTransaction(
                AdHocPlannedStmtBatch plannedStmtBatch) throws VoltTypeException {
            return AdHocNTBase.this.createAdHocTransaction(plannedStmtBatch, false);
        }

        @Override public CompletableFuture<ClientResponse> processExplainPlannedStmtBatch(AdHocPlannedStmtBatch plannedStmtBatch) {
            return AdHocNTBase.processExplainPlannedStmtBatch(plannedStmtBatch);
        }
    }
}
