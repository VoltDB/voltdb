/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltdb.BackendTarget;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ClientResponseImpl;
import org.voltdb.TheHashinator;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.VoltTypeException;
import org.voltdb.catalog.Database;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.AdHocPlannedStatement;
import org.voltdb.compiler.AdHocPlannedStmtBatch;
import org.voltdb.compiler.PlannerTool;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTrace;

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

    BackendTarget m_backendTargetType = VoltDB.instance().getBackendTargetType();
    boolean m_isConfiguredForNonVoltDBBackend = (m_backendTargetType == BackendTarget.HSQLDB_BACKEND ||
                                                 m_backendTargetType == BackendTarget.POSTGRESQL_BACKEND ||
                                                 m_backendTargetType == BackendTarget.POSTGIS_BACKEND);

    /**
     * Log ad hoc batch info
     * @param batch  planned statement batch
     */
    /*private void logBatch(final
                          final String sqlStatements,
                          final Object[] userParams,
                          final AdHocPlannedStmtBatch batch)
    {
        final int numStmts = batch.work.getStatementCount();
        final int numParams = batch.work.getParameterCount();
        final String readOnly = batch.readOnly ? "yes" : "no";
        final String singlePartition = batch.isSinglePartitionCompatible() ? "yes" : "no";
        final String user = batch.work.user.m_name;
        final CatalogContext context = (batch.work.catalogContext != null
                                            ? batch.work.catalogContext
                                            : VoltDB.instance().getCatalogContext());
        final String[] groupNames = context.authSystem.getGroupNamesForUser(user);
        final String groupList = StringUtils.join(groupNames, ',');

        adhocLog.debug(String.format(
            "=== statements=%d parameters=%d read-only=%s single-partition=%s user=%s groups=[%s]",
            numStmts, numParams, readOnly, singlePartition, user, groupList));
        if (batch.work.sqlStatements != null) {
            for (int i = 0; i < batch.work.sqlStatements.length; ++i) {
                adhocLog.debug(String.format("Statement #%d: %s", i + 1, batch.work.sqlStatements[i]));
            }
        }
        if (batch.work.userParamSet != null) {
            for (int i = 0; i < batch.work.userParamSet.length; ++i) {
                Object value = batch.work.userParamSet[i];
                final String valueString = (value != null ? value.toString() : "NULL");
                adhocLog.debug(String.format("Parameter #%d: %s", i + 1, valueString));
            }
        }
    }*/

    protected CompletableFuture<ClientResponse> runNonDDLAdHoc(CatalogContext context,
                                                               List<String> sqlStatements,
                                                               boolean inferPartitioning,
                                                               Object userPartitionKey,
                                                               ExplainMode explainMode,
                                                               Object[] userParamSet)
    {
        // record the catalog version the query is planned against to
        // catch races vs. updateApplicationCatalog.
        if (context == null) {
            context = VoltDB.instance().getCatalogContext();
        }

        final PlannerTool ptool = context.m_ptool;

        List<String> errorMsgs = new ArrayList<>();
        List<AdHocPlannedStatement> stmts = new ArrayList<>();
        int partitionParamIndex = -1;
        VoltType partitionParamType = null;
        Object partitionParamValue = null;
        assert(sqlStatements != null);
        // Take advantage of the planner optimization for inferring single partition work
        // when the batch has one statement.
        StatementPartitioning partitioning = null;
        boolean inferSP = (sqlStatements.size() == 1) && inferPartitioning;

        if (userParamSet != null && userParamSet.length > 0) {
            if (sqlStatements.size() != 1) {
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                                         AdHocErrorResponseMessage);
            }
        }

        for (final String sqlStatement : sqlStatements) {
            if (inferSP) {
                partitioning = StatementPartitioning.inferPartitioning();
            }
            else if (userPartitionKey == null) {
                partitioning = StatementPartitioning.forceMP();
            }
            else {
                partitioning = StatementPartitioning.forceSP();
            }
            try {
                AdHocPlannedStatement result = ptool.planSql(sqlStatement, partitioning,
                        explainMode != ExplainMode.NONE, userParamSet);
                // The planning tool may have optimized for the single partition case
                // and generated a partition parameter.
                if (inferSP) {
                    partitionParamIndex = result.getPartitioningParameterIndex();
                    partitionParamType = result.getPartitioningParameterType();
                    partitionParamValue = result.getPartitioningParameterValue();
                }
                stmts.add(result);
            }
            catch (Exception e) {
                errorMsgs.add("Unexpected Ad Hoc Planning Error: " + e);
            }
            catch (StackOverflowError error) {
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
                errorMsgs.add("Encountered stack overflow error. " +
                        "Try reducing the number of predicate expressions in the query.");
            }
            catch (AssertionError ae) {
                errorMsgs.add("Assertion Error in Ad Hoc Planning: " + ae);
            }
        }
        String errorSummary = null;
        if (!errorMsgs.isEmpty()) {
            errorSummary = StringUtils.join(errorMsgs, "\n");
        }

        AdHocPlannedStmtBatch plannedStmtBatch =
                new AdHocPlannedStmtBatch(userParamSet,
                                          stmts,
                                          partitionParamIndex,
                                          partitionParamType,
                                          partitionParamValue,
                                          null,
                                          errorSummary);

        // TODO: re-enable this
        /*if (adhocLog.isDebugEnabled()) {
            logBatch(plannedStmtBatch);
        }*/

        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.CI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.endAsync("planadhoc", getClientHandle()));
        }

        if (explainMode == ExplainMode.EXPLAIN_ADHOC) {
            return processExplainPlannedStmtBatch(plannedStmtBatch);
        }
        else if (explainMode == ExplainMode.EXPLAIN_DEFAULT_PROC) {
            return processExplainDefaultProc(plannedStmtBatch);
        }
        else {
            try {
                return createAdHocTransaction(plannedStmtBatch);
            }
            catch (VoltTypeException vte) {
                String msg = "Unable to execute adhoc sql statement(s): " + vte.getMessage();
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, msg);
            }
        }
    }

    CompletableFuture<ClientResponse> processExplainPlannedStmtBatch(AdHocPlannedStmtBatch planBatch) {
        /**
         * Take the response from the async ad hoc planning process and put the explain
         * plan in a table with the right format.
         */
        Database db = VoltDB.instance().getCatalogContext().database;
        int size = planBatch.getPlannedStatementCount();

        VoltTable[] vt = new VoltTable[ size ];
        for (int i = 0; i < size; ++i) {
            vt[i] = new VoltTable(new VoltTable.ColumnInfo("EXECUTION_PLAN", VoltType.STRING));
            String str = planBatch.explainStatement(i, db);
            vt[i].addRow(str);
        }

        ClientResponseImpl response =
                new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null,
                        vt,
                        null);
        // TODO: check if I need this
        //response.setClientHandle( planBatch.clientHandle );

        CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        fut.complete(response);
        return fut;
    }

    /**
     * Explain Proc for a default proc is routed through the regular Explain
     * path using ad hoc planning and all. Take the result from that async
     * process and format it like other explains for procedures.
     */
    CompletableFuture<ClientResponse> processExplainDefaultProc(AdHocPlannedStmtBatch planBatch) {
        Database db = VoltDB.instance().getCatalogContext().database;

        // there better be one statement if this is really sql
        // from a default procedure
        assert(planBatch.getPlannedStatementCount() == 1);
        AdHocPlannedStatement ahps = planBatch.getPlannedStatement(0);
        String sql = new String(ahps.sql, StandardCharsets.UTF_8);
        String explain = planBatch.explainStatement(0, db);

        VoltTable vt = new VoltTable(new VoltTable.ColumnInfo( "SQL_STATEMENT", VoltType.STRING),
                new VoltTable.ColumnInfo( "EXECUTION_PLAN", VoltType.STRING));
        vt.addRow(sql, explain);

        ClientResponseImpl response =
                new ClientResponseImpl(
                        ClientResponseImpl.SUCCESS,
                        ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                        null,
                        new VoltTable[] { vt },
                        null);
        // TODO: check if I need this
        //response.setClientHandle( planBatch.clientHandle );

        CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        fut.complete(response);
        return fut;
    }

    /**
     * A simplified variant of compileAdHoc that translates a
     * pseudo-statement generated internally from a system stored proc
     * invocation into a multi-part EE statement plan.
     * @param work
     * @return
     */
    /*AsyncCompilerResult compileSysProcPlan(AdHocPlannerWork work) {
        // record the catalog version the query is planned against to
        // catch races vs. updateApplicationCatalog.
        CatalogContext context = work.catalogContext;
        if (context == null) {
            context = VoltDB.instance().getCatalogContext();
        }

        final PlannerTool ptool = context.m_ptool;

        List<String> errorMsgs = new ArrayList<>();
        List<AdHocPlannedStatement> stmts = new ArrayList<>();
        assert(work.sqlStatements != null);
        assert(work.sqlStatements.length == 1);

        String sqlStatement = work.sqlStatements[0];
        StatementPartitioning partitioning = StatementPartitioning.forceMP();
        try {
            AdHocPlannedStatement result = ptool.planSql(sqlStatement, partitioning,
                    false, work.userParamSet);
            stmts.add(result);
        }
        catch (Exception ex) {
            errorMsgs.add("Unexpected System Stored Procedure Planning Error: " + ex);
        }
        catch (AssertionError ae) {
            errorMsgs.add("Assertion Error in System Stored Procedure Planning: " + ae);
        }

        String errorSummary = null;
        if ( ! errorMsgs.isEmpty()) {
            errorSummary = StringUtils.join(errorMsgs, "\n");
        }

        AdHocPlannedStmtBatch plannedStmtBatch = new AdHocPlannedStmtBatch(work,
                stmts, -1, null, null, errorSummary);

        if (adhocLog.isDebugEnabled()) {
            logBatch(plannedStmtBatch);
        }

        return plannedStmtBatch;
    }*/






    private final CompletableFuture<ClientResponse> createAdHocTransaction(final AdHocPlannedStmtBatch plannedStmtBatch)
            throws VoltTypeException
    {
        ByteBuffer buf = null;
        try {
            buf = plannedStmtBatch.flattenPlanArrayToBuffer();
        }
        catch (IOException e) {
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
        int partition = -1;

        if (isSinglePartition) {
            if (plannedStmtBatch.isReadOnly()) {
                procedureName = "@AdHoc_RO_SP";
            }
            else {
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
            partition = TheHashinator.getPartitionForParameter(type, partitionParam);

            // Send the partitioning parameter and its type along so that the site can check if
            // it's mis-partitioned. Type is needed to re-hashinate for command log re-init.
            params = new Object[] { param, (byte)type, buf.array() };
        }
        else {
            if (plannedStmtBatch.isReadOnly()) {
                procedureName = "@AdHoc_RO_MP";
            }
            else {
                procedureName = "@AdHoc_RW_MP";
            }
            params = new Object[] { buf.array() };
        }

        return callProcedure(procedureName, params);
    }
}
