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

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.LocalObjectMessage;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.messaging.LocalMailbox;

import com.google.common.util.concurrent.ListeningExecutorService;

public class AsyncCompilerAgent {

    private static final VoltLogger hostLog = new VoltLogger("HOST");
    static final VoltLogger ahpLog = new VoltLogger("ADHOCPLANNERTHREAD");

    // if more than this amount of work is queued, reject new work
    static public final int MAX_QUEUE_DEPTH = 250;

    // accept work via this mailbox
    Mailbox m_mailbox;

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
            final AsyncCompilerResult result = compileAdHocPlan(w);
            w.completionHandler.onCompletion(result);
        }
        else if (wrapper.payload instanceof CatalogChangeWork) {
            final CatalogChangeWork w = (CatalogChangeWork)(wrapper.payload);
            if (VoltDB.instance().getConfig().m_isEnterprise) {
                try {
                    Class<?> acahClz = getClass().getClassLoader().loadClass("org.voltdb.compiler.AsyncCompilerAgentHelper");
                    Object acah = acahClz.newInstance();
                    Method acahPrepareMethod = acahClz.getMethod(
                            "prepareApplicationCatalogDiff", new Class<?>[] { CatalogChangeWork.class });
                    hostLog.info("Asynchronously preparing to update the application catalog and/or deployment settings.");
                    final AsyncCompilerResult result = (AsyncCompilerResult) acahPrepareMethod.invoke(acah, w);
                    if (result.errorMsg != null) {
                        hostLog.info("A request to update the application catalog and/or deployment settings has been rejected. More info returned to client.");
                    }
                    w.completionHandler.onCompletion(result);
                }
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Error preparing catalog diff.", true, e);
                }
            }
            assert(false); // shouldn't get here in community edition
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

    AdHocPlannedStmtBatch compileAdHocPlan(AdHocPlannerWork work) {

        // record the catalog version the query is planned against to
        // catch races vs. updateApplicationCatalog.
        CatalogContext context = work.catalogContext;
        if (context == null) {
            context = VoltDB.instance().getCatalogContext();
        }

        final PlannerTool ptool = context.m_ptool;

        AdHocPlannedStmtBatch plannedStmtBatch =
                new AdHocPlannedStmtBatch(work.sqlBatchText,
                                          work.partitionParam,
                                          work.clientHandle,
                                          work.connectionId,
                                          work.hostname,
                                          work.adminConnection,
                                          work.type,
                                          work.originalTxnId,
                                          work.originalUniqueId,
                                          work.clientData);

        List<String> errorMsgs = new ArrayList<String>();
        assert(work.sqlStatements != null);
        // Take advantage of the planner optimization for inferring single partition work
        // when the batch has one statement.
        if (work.sqlStatements.length == 1) {
            // Single statement batch.
            try {
                String sqlStatement = work.sqlStatements[0];
                AdHocPlannedStatement result = ptool.planSql(sqlStatement, work.partitionParam,
                                                             work.inferSinglePartition, work.allowParameterization);
                // The planning tool may have optimized for the single partition case
                // and generated a partition parameter.
                plannedStmtBatch.partitionParam = result.partitionParam;
                plannedStmtBatch.addStatement(result);
            }
            catch (Exception e) {
                errorMsgs.add("Unexpected Ad Hoc Planning Error: " + e);
            }
        }
        else {
            // Multi-statement batch.
            for (final String sqlStatement : work.sqlStatements) {
                try {
                    AdHocPlannedStatement result = ptool.planSql(sqlStatement, work.partitionParam,
                                                                 false, work.allowParameterization);
                    plannedStmtBatch.addStatement(result);
                }
                catch (Exception e) {
                    errorMsgs.add("Unexpected Ad Hoc Planning Error: " + e);
                }
            }
        }
        if (!errorMsgs.isEmpty()) {
            plannedStmtBatch.errorMsg = StringUtils.join(errorMsgs, "\n");
        }
        if( work.isExplainWork() ) {
            plannedStmtBatch.setIsExplainWork();
        }
        return plannedStmtBatch;
    }
}
