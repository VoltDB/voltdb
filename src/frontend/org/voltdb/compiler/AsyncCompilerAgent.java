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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.OperationMode;
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.licensetool.LicenseApi;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.parser.SQLLexer;
import org.voltdb.planner.StatementPartitioning;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class AsyncCompilerAgent {

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    private static final VoltLogger adhocLog = new VoltLogger("ADHOC");

    // if more than this amount of work is queued, reject new work
    static public final int MAX_QUEUE_DEPTH = 250;

    // accept work via this mailbox
    Mailbox m_mailbox;

    public AsyncCompilerAgent(LicenseApi licenseApi) {
        m_helper = new AsyncCompilerAgentHelper(licenseApi);
    }

    // The helper for catalog updates, back after its exclusive three year tour
    // of Europe, Scandinavia, and the sub-continent.
    final AsyncCompilerAgentHelper m_helper;

    // do work in this executor service
    final ListeningExecutorService m_es =
        CoreUtils.getBoundedSingleThreadExecutor("Ad Hoc Planner", MAX_QUEUE_DEPTH);

    // Enable debug hooks when the "asynccompilerdebug" sys prop is set to "true" or "yes".
    private final static MiscUtils.BooleanSystemProperty DEBUG_MODE =
            new MiscUtils.BooleanSystemProperty("asynccompilerdebug");

    // When DEBUG_MODE is true this (valid) DDL string triggers an exception.
    // Public visibility allows it to be used from a unit test.
    public final static String DEBUG_EXCEPTION_DDL =
            "create table DEBUG_MODE_ENG_7653_crash_me_now (die varchar(7654) not null)";

    // intended for integration test use. finish planning what's in
    // the queue and terminate the TPE.
    public void shutdown() throws InterruptedException {
        if (m_es != null) {
            m_es.shutdown();
            m_es.awaitTermination(120, TimeUnit.SECONDS);
        }
    }

    public void createMailbox(final HostMessenger hostMessenger, final long hsId) {
        m_mailbox = new LocalMailbox(hostMessenger) {

            @Override
            public void send(long destinationHSId, VoltMessage message) {
                message.m_sourceHSId = hsId;
                hostMessenger.send(destinationHSId, message);
            }

            @Override
            public void deliver(final VoltMessage message) {
                try {
                    m_es.submit(new Runnable() {
                        @Override
                        public void run() {
                            handleMailboxMessage(message);
                        }
                    });
                } catch (RejectedExecutionException rejected) {
                    final LocalObjectMessage wrapper = (LocalObjectMessage)message;
                    AsyncCompilerWork work = (AsyncCompilerWork)(wrapper.payload);
                    generateErrorResult("Ad Hoc Planner task queue is full. Try again.", work);
                }
            }
        };
        hostMessenger.createMailbox(hsId, m_mailbox);
    }

    void generateErrorResult(String errorMsg, AsyncCompilerWork work) {
        AsyncCompilerResult retval = new AsyncCompilerResult();
        retval.clientHandle = work.clientHandle;
        retval.errorMsg = errorMsg;
        retval.connectionId = work.connectionId;
        retval.hostname = work.hostname;
        retval.adminConnection = work.adminConnection;
        retval.clientData = work.clientData;
        work.completionHandler.onCompletion(retval);
    }

    void handleMailboxMessage(final VoltMessage message) {
        final LocalObjectMessage wrapper = (LocalObjectMessage)message;
        if (wrapper.payload instanceof AsyncCompilerWork) {
            AsyncCompilerWork compilerWork = (AsyncCompilerWork)wrapper.payload;
            // Don't let exceptions escape
            try {
                if (compilerWork instanceof AdHocPlannerWork) {
                    handleAdHocPlannerWork((AdHocPlannerWork)(compilerWork));
                }
                else if (compilerWork instanceof CatalogChangeWork) {
                    handleCatalogChangeWork((CatalogChangeWork)(compilerWork));
                }
                else {
                    // Definitely shouldn't happen since we should be handling all possible
                    // AsyncCompilerWork derivative classes above.
                    AsyncCompilerResult errResult =
                        AsyncCompilerResult.makeErrorResult(compilerWork,
                            String.format("Unexpected compiler work class: %s %s: %s",
                                    compilerWork.getClass().getName(),
                                    "Please contact VoltDB support with this message and the contents:",
                                    message.toString()));
                    compilerWork.completionHandler.onCompletion(errResult);
                }
            }
            catch (RuntimeException e) {
                AsyncCompilerResult errResult =
                    AsyncCompilerResult.makeErrorResult(compilerWork,
                        String.format("Unexpected async compiler exception for %s: %s: %s: %s",
                                compilerWork.getClass().getName(),
                                e.getLocalizedMessage(),
                                "Please contact VoltDB support with this message and the contents:",
                                message.toString()));
                compilerWork.completionHandler.onCompletion(errResult);
            }
        }
        else {
            hostLog.error("Unexpected message received by AsyncCompilerAgent.  " +
                    "Please contact VoltDB support with this message and the contents: " +
                    message.toString());
        }
    }

    void handleAdHocPlannerWork(final AdHocPlannerWork w) {
        // do initial naive scan of statements for DDL, forbid mixed DDL and (DML|DQL)
        Boolean hasDDL = null;
        // conflictTables tracks dropped tables before removing the ones that don't have CREATEs.
        SortedSet<String> conflictTables = new TreeSet<String>();
        Set<String> createdTables = new HashSet<String>();
        for (String stmt : w.sqlStatements) {
            // Simulate an unhandled exception? (ENG-7653)
            if (DEBUG_MODE.isTrue() && stmt.equals(DEBUG_EXCEPTION_DDL)) {
                throw new IndexOutOfBoundsException(DEBUG_EXCEPTION_DDL);
            }
            if (SQLLexer.isComment(stmt) || stmt.trim().isEmpty()) {
                continue;
            }
            String ddlToken = SQLLexer.extractDDLToken(stmt);
            if (hasDDL == null) {
                hasDDL = (ddlToken != null) ? true : false;
            }
            else if ((hasDDL && ddlToken == null) || (!hasDDL && ddlToken != null))
            {
                AsyncCompilerResult errResult =
                    AsyncCompilerResult.makeErrorResult(w,
                            "DDL mixed with DML and queries is unsupported.");
                // No mixing DDL and DML/DQL.  Turn this into an error returned to client.
                w.completionHandler.onCompletion(errResult);
                return;
            }
            // do a couple of additional checks if it's DDL
            if (hasDDL) {
                // check that the DDL is allowed
                String rejectionExplanation = SQLLexer.checkPermitted(stmt);
                if (rejectionExplanation != null) {
                    AsyncCompilerResult errResult =
                            AsyncCompilerResult.makeErrorResult(w, rejectionExplanation);
                    w.completionHandler.onCompletion(errResult);
                    return;
                }
                // make sure not to mix drop and create in the same batch for the same table
                if (ddlToken.equals("drop")) {
                    String tableName = SQLLexer.extractDDLTableName(stmt);
                    if (tableName != null) {
                        conflictTables.add(tableName);
                    }
                }
                else if (ddlToken.equals("create")) {
                    String tableName = SQLLexer.extractDDLTableName(stmt);
                    if (tableName != null) {
                        createdTables.add(tableName);
                    }
                }
            }
        }
        if (hasDDL == null) {
            // we saw neither DDL or DQL/DML.  Make sure that we get a
            // response back to the client
            AsyncCompilerResult errResult =
                AsyncCompilerResult.makeErrorResult(w,
                        "Failed to plan, no SQL statement provided.");
            w.completionHandler.onCompletion(errResult);
            return;
        }
        else if (!hasDDL) {
            final AsyncCompilerResult result = compileAdHocPlan(w);
            w.completionHandler.onCompletion(result);
        }
        else {
            // We have adhoc DDL.  Is it okay to run it?

            // check for conflicting DDL create/drop table statements.
            // unhappy if the intersection is empty
            conflictTables.retainAll(createdTables);
            if (!conflictTables.isEmpty()) {
                StringBuilder sb = new StringBuilder();
                sb.append("AdHoc DDL contains both DROP and CREATE statements for the following table(s):");
                for (String tableName : conflictTables) {
                    sb.append(" ");
                    sb.append(tableName);
                }
                sb.append("\nYou cannot DROP and ADD a table with the same name in a single batch "
                        + "(via @AdHoc). Issue the DROP and ADD statements as separate commands.");
                AsyncCompilerResult errResult =
                        AsyncCompilerResult.makeErrorResult(w, sb.toString());
                    w.completionHandler.onCompletion(errResult);
                    return;
            }

            // Is it forbidden by the replication role and configured schema change method?
            // master and UAC method chosen:
            if (!w.useAdhocDDL) {
                AsyncCompilerResult errResult =
                    AsyncCompilerResult.makeErrorResult(w,
                            "Cluster is configured to use @UpdateApplicationCatalog " +
                            "to change application schema.  AdHoc DDL is forbidden.");
                w.completionHandler.onCompletion(errResult);
                return;
            }

            if (!allowPausedModeWork(w)) {
                AsyncCompilerResult errResult =
                    AsyncCompilerResult.makeErrorResult(w,
                            "Server is paused and is available in read-only mode - please try again later.",
                            ClientResponse.SERVER_UNAVAILABLE);
                w.completionHandler.onCompletion(errResult);
                return;
            }
            final CatalogChangeWork ccw = new CatalogChangeWork(w);
            dispatchCatalogChangeWork(ccw);
        }
    }

    private boolean allowPausedModeWork(AsyncCompilerWork w) {
         return (VoltDB.instance().getMode() != OperationMode.PAUSED ||
                 w.isServerInitiated() ||
                 w.adminConnection);
    }

    void handleCatalogChangeWork(final CatalogChangeWork w) {
        if (!allowPausedModeWork(w)) {
            AsyncCompilerResult errResult =
                    AsyncCompilerResult.makeErrorResult(w,
                            "Server is paused and is available in read-only mode - please try again later.",
                            ClientResponse.SERVER_UNAVAILABLE);
            w.completionHandler.onCompletion(errResult);
            return;
        }
        // We have an @UAC.  Is it okay to run it?
        // If we weren't provided operationBytes, it's a deployment-only change and okay to take
        // master and adhoc DDL method chosen
        if (w.invocationName.equals("@UpdateApplicationCatalog") &&
            w.operationBytes != null && w.useAdhocDDL)
        {
            AsyncCompilerResult errResult =
                AsyncCompilerResult.makeErrorResult(w,
                        "Cluster is configured to use AdHoc DDL to change application " +
                        "schema.  Use of @UpdateApplicationCatalog is forbidden.");
            w.completionHandler.onCompletion(errResult);
            return;
        }
        else if (w.invocationName.equals("@UpdateClasses") && !w.useAdhocDDL) {
            AsyncCompilerResult errResult =
                AsyncCompilerResult.makeErrorResult(w,
                        "Cluster is configured to use @UpdateApplicationCatalog " +
                        "to change application schema.  Use of @UpdateClasses is forbidden.");
            w.completionHandler.onCompletion(errResult);
            return;
        }
        dispatchCatalogChangeWork(w);
    }

    public void compileAdHocPlanForProcedure(final AdHocPlannerWork apw) {
        m_es.submit(new Runnable() {
            @Override
            public void run(){
                apw.completionHandler.onCompletion(compileAdHocPlan(apw));
            }
        });
    }

    private void dispatchCatalogChangeWork(CatalogChangeWork work)
    {
        final AsyncCompilerResult result = m_helper.prepareApplicationCatalogDiff(work);
        if (result.errorMsg != null) {
            hostLog.info("A request to update the database catalog and/or deployment settings has been rejected. More info returned to client.");
        }
        // Log something useful about catalog upgrades when they occur.
        if (result instanceof CatalogChangeResult) {
            CatalogChangeResult ccr = (CatalogChangeResult)result;
            if (ccr.upgradedFromVersion != null) {
                hostLog.info(String.format(
                            "In order to update the application catalog it was "
                            + "automatically upgraded from version %s.",
                            ccr.upgradedFromVersion));
            }
        }
        work.completionHandler.onCompletion(result);
    }

    public static final String AdHocErrorResponseMessage =
            "The @AdHoc stored procedure when called with more than one parameter "
                    + "must be passed a single parameterized SQL statement as its first parameter. "
                    + "Pass each parameterized SQL statement to a separate callProcedure invocation.";

    AsyncCompilerResult compileAdHocPlan(AdHocPlannerWork work) {

        // record the catalog version the query is planned against to
        // catch races vs. updateApplicationCatalog.
        CatalogContext context = work.catalogContext;
        if (context == null) {
            context = VoltDB.instance().getCatalogContext();
        }

        final PlannerTool ptool = context.m_ptool;

        List<String> errorMsgs = new ArrayList<String>();
        List<AdHocPlannedStatement> stmts = new ArrayList<AdHocPlannedStatement>();
        int partitionParamIndex = -1;
        VoltType partitionParamType = null;
        Object partitionParamValue = null;
        assert(work.sqlStatements != null);
        // Take advantage of the planner optimization for inferring single partition work
        // when the batch has one statement.
        StatementPartitioning partitioning = null;
        boolean inferSP = (work.sqlStatements.length == 1) && work.inferPartitioning;

        if (work.userParamSet != null && work.userParamSet.length > 0) {
            if (work.sqlStatements.length != 1) {
                return AsyncCompilerResult.makeErrorResult(work, AdHocErrorResponseMessage);
            }
        }

        for (final String sqlStatement : work.sqlStatements) {
            if (inferSP) {
                partitioning = StatementPartitioning.inferPartitioning();
            }
            else if (work.userPartitionKey == null) {
                partitioning = StatementPartitioning.forceMP();
            } else {
                partitioning = StatementPartitioning.forceSP();
            }
            try {
                AdHocPlannedStatement result = ptool.planSql(sqlStatement, partitioning,
                        work.explainMode != ExplainMode.NONE, work.userParamSet);
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

        AdHocPlannedStmtBatch plannedStmtBatch = new AdHocPlannedStmtBatch(work,
                                                                           stmts,
                                                                           partitionParamIndex,
                                                                           partitionParamType,
                                                                           partitionParamValue,
                                                                           errorSummary);

        if (adhocLog.isDebugEnabled()) {
            logBatch(plannedStmtBatch);
        }

        return plannedStmtBatch;
    }

    /**
     * Log ad hoc batch info
     * @param batch  planned statement batch
     */
    private void logBatch(final AdHocPlannedStmtBatch batch)
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
    }
}
