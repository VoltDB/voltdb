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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
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
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.messaging.LocalMailbox;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Encoder;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

public class AsyncCompilerAgent {

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
                    AsyncCompilerResult retval = new AsyncCompilerResult();
                    retval.clientHandle = work.clientHandle;
                    retval.errorMsg = "Ad Hoc Planner is not available. Try again.";
                    retval.connectionId = work.connectionId;
                    retval.hostname = work.hostname;
                    retval.adminConnection = work.adminConnection;
                    retval.clientData = work.clientData;
                    // XXX: need client interface mailbox id.
                    m_mailbox.send(message.m_sourceHSId, new LocalObjectMessage(retval));
                }
            }
        };
        hostMessenger.createMailbox(hsId, m_mailbox);
    }

    void handleMailboxMessage(final VoltMessage message) {
        final LocalObjectMessage wrapper = (LocalObjectMessage)message;
        if (wrapper.payload instanceof AdHocPlannerWork) {
            final AdHocPlannerWork w = (AdHocPlannerWork)(wrapper.payload);
            final AsyncCompilerResult result = compileAdHocPlan(w);
            // XXX: need client interface mailbox id.
            m_mailbox.send(message.m_sourceHSId, new LocalObjectMessage(result));
        }
        else if (wrapper.payload instanceof CatalogChangeWork) {
            final CatalogChangeWork w = (CatalogChangeWork)(wrapper.payload);
            final AsyncCompilerResult result = prepareApplicationCatalogDiff(w);
            // XXX: need client interface mailbox id.
            m_mailbox.send(message.m_sourceHSId, new LocalObjectMessage(result));
        }
    }

    public ListenableFuture<AdHocPlannedStmtBatch> compileAdHocPlanFuture(final AdHocPlannerWork apw) {
        return m_es.submit(new Callable<AdHocPlannedStmtBatch>() {
            @Override
            public AdHocPlannedStmtBatch call() throws Exception {
                return compileAdHocPlan(apw);
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
                                          context.catalogVersion,
                                          work.clientHandle,
                                          work.connectionId,
                                          work.hostname,
                                          work.adminConnection,
                                          work.clientData);

        List<String> errorMsgs = new ArrayList<String>();
        assert(work.sqlStatements != null);
        // Take advantage of the planner optimization for inferring single partition work
        // when the batch has one statement.
        if (work.sqlStatements.length == 1) {
            // Single statement batch.
            try {
                String sqlStatement = work.sqlStatements[0];
                PlannerTool.Result result = ptool.planSql(sqlStatement, work.partitionParam,
                                                            work.inferSinglePartition, work.allowParameterization);
                // The planning tool may have optimized for the single partition case
                // and generated a partition parameter.
                plannedStmtBatch.partitionParam = result.partitionParam;
                plannedStmtBatch.addStatement(sqlStatement,
                                              result.onePlan,
                                              result.allPlan,
                                              result.replicatedDML,
                                              result.params);
            }
            catch (Exception e) {
                errorMsgs.add("Unexpected Ad Hoc Planning Error: " + e.getMessage());
            }
        }
        else {
            // Multi-statement batch.
            for (final String sqlStatement : work.sqlStatements) {
                try {
                    PlannerTool.Result result = ptool.planSql(sqlStatement, work.partitionParam,
                                                                false, work.allowParameterization);

                    plannedStmtBatch.addStatement(sqlStatement,
                                                  result.onePlan,
                                                  result.allPlan,
                                                  result.replicatedDML,
                                                  result.params);
                }
                catch (Exception e) {
                    errorMsgs.add("Unexpected Ad Hoc Planning Error: " + e.getMessage());
                }
            }
        }
        if (!errorMsgs.isEmpty()) {
            plannedStmtBatch.errorMsg = StringUtils.join(errorMsgs, "\n");
        }

        return plannedStmtBatch;
    }

    private AsyncCompilerResult prepareApplicationCatalogDiff(CatalogChangeWork work) {
        // create the change result and set up all the boiler plate
        CatalogChangeResult retval = new CatalogChangeResult();
        retval.clientData = work.clientData;
        retval.clientHandle = work.clientHandle;
        retval.connectionId = work.connectionId;
        retval.adminConnection = work.adminConnection;
        retval.hostname = work.hostname;

        // catalog change specific boiler plate
        retval.catalogBytes = work.catalogBytes;
        retval.deploymentString = work.deploymentString;

        // get the diff between catalogs
        try {
            // try to get the new catalog from the params
            String newCatalogCommands = CatalogUtil.loadCatalogFromJar(work.catalogBytes, null);
            if (newCatalogCommands == null) {
                retval.errorMsg = "Unable to read from catalog bytes";
                return retval;
            }
            Catalog newCatalog = new Catalog();
            newCatalog.execute(newCatalogCommands);

            // If VoltProjectBuilder was used, work.deploymentURL will be null. No deployment.xml file was
            // given to the server in this case because its deployment info has already been added to the catalog.
            if (work.deploymentString != null) {
                retval.deploymentCRC =
                        CatalogUtil.compileDeploymentStringAndGetCRC(newCatalog, work.deploymentString, false);
                if (retval.deploymentCRC < 0) {
                    retval.errorMsg = "Unable to read from deployment file string";
                    return retval;
                }
            }

            // get the current catalog
            CatalogContext context = VoltDB.instance().getCatalogContext();

            // store the version of the catalog the diffs were created against.
            // verified when / if the update procedure runs in order to verify
            // catalogs only move forward
            retval.expectedCatalogVersion = context.catalogVersion;

            // compute the diff in StringBuilder
            CatalogDiffEngine diff = new CatalogDiffEngine(context.catalog, newCatalog);
            if (!diff.supported()) {
                throw new Exception("The requested catalog change is not a supported change at this time. " + diff.errors());
            }

            // since diff commands can be stupidly big, compress them here
            retval.encodedDiffCommands = Encoder.compressAndBase64Encode(diff.commands());
            /*
            // check if the resulting string is small enough to fit in our parameter sets (about 2mb)
            if (retval.encodedDiffCommands.length() > (2 * 1000 * 1000)) {
                throw new Exception("The requested catalog change is too large for this version of VoltDB. " +
                                    "Try a series of smaller updates.");
            }
            */
        }
        catch (Exception e) {
            e.printStackTrace();
            retval.encodedDiffCommands = null;
            retval.errorMsg = e.getMessage();
        }

        return retval;
    }

}
