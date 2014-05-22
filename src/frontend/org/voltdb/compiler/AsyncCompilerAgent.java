/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.util.List;
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
import org.voltdb.VoltDB;
import org.voltdb.VoltType;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.planner.PartitioningForStatement;
import org.voltdb.utils.SQLLexer;

import com.google_voltpatches.common.util.concurrent.ListeningExecutorService;

public class AsyncCompilerAgent {

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    static final VoltLogger ahpLog = new VoltLogger("ADHOCPLANNERTHREAD");

    // if more than this amount of work is queued, reject new work
    static public final int MAX_QUEUE_DEPTH = 250;

    // accept work via this mailbox
    Mailbox m_mailbox;

    // The helper for catalog updates, back after its exclusive three year tour
    // of Europe, Scandinavia, and the sub-continent.
    AsyncCompilerAgentHelper m_helper = new AsyncCompilerAgentHelper();

    // do work in this executor service
    final ListeningExecutorService m_es =
        CoreUtils.getBoundedSingleThreadExecutor("Ad Hoc Planner", MAX_QUEUE_DEPTH);

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
        if (wrapper.payload instanceof AdHocPlannerWork) {
            final AdHocPlannerWork w = (AdHocPlannerWork)(wrapper.payload);
            // do initial naive scan of statements for DDL, forbid mixed DDL and (DML|DQL)
            // This is not currently robust to comment, multi-line statments,
            // multiple statements on a line, etc.
            Boolean hasDDL = null;
            for (String stmt : w.sqlStatements) {
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
            }
            if (!hasDDL) {
                final AsyncCompilerResult result = compileAdHocPlan(w);
                w.completionHandler.onCompletion(result);
            }
            else {
                final CatalogChangeWork ccw = new CatalogChangeWork(w);
                dispatchCatalogChangeWork(ccw);
            }
        }
        else if (wrapper.payload instanceof CatalogChangeWork) {
            final CatalogChangeWork w = (CatalogChangeWork)(wrapper.payload);
            dispatchCatalogChangeWork(w);
        }
        else {
            hostLog.warn("Unexpected message received by AsyncCompilerAgent.  " +
                    "Please contact VoltDB support with this message and the contents: " +
                    message.toString());
        }
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

    AdHocPlannedStmtBatch compileAdHocPlan(AdHocPlannerWork work) {

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
        PartitioningForStatement partitioning = null;
        boolean inferSP = (work.sqlStatements.length == 1) && work.inferPartitioning;
        for (final String sqlStatement : work.sqlStatements) {
            if (inferSP) {
                partitioning = PartitioningForStatement.inferPartitioning();
            }
            else if (work.userPartitionKey == null) {
                partitioning = PartitioningForStatement.forceMP();
            } else {
                partitioning = PartitioningForStatement.forceSP();
            }
            try {
                AdHocPlannedStatement result = ptool.planSql(sqlStatement, partitioning);
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

        return plannedStmtBatch;
    }
}
