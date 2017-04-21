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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.deploymentfile.DrRoleType;
import org.voltdb.parser.SQLLexer;

public class AdHoc extends AdHocNTBase {

    /*final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.CI);
    if (traceLog != null) {
        traceLog.add(() -> VoltTrace.beginAsync("planadhoc", task.getClientHandle(),
                                                "clientHandle", Long.toString(task.getClientHandle()),
                                                "sql", sql));
    }

    List<String> sqlStatements = SQLLexer.splitStatements(sql);
    String[] stmtsArray = sqlStatements.toArray(new String[sqlStatements.size()]);

    AdHocPlannerWork ahpw = new AdHocPlannerWork(
            m_siteId,
            task.clientHandle, handler.connectionId(),
            handler.isAdmin(), ccxn,
            sql, stmtsArray, userParams, null, explainMode,
            userPartitionKey == null, userPartitionKey,
            task.getProcName(),
            task.getBatchTimeout(),
            DrRoleType.fromValue(VoltDB.instance().getCatalogContext().getCluster().getDrrole()),
            VoltDB.instance().getCatalogContext().cluster.getUseddlschema(),
            m_adhocCompletionHandler, user);
    LocalObjectMessage work = new LocalObjectMessage( ahpw );

    m_mailbox.send(m_plannerSiteId, work);*/

    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        final String invocationName = "@AdHoc";

        Object[] paramArray = params.toArray();
        String sql = (String) paramArray[0];
        Object[] userParams = null;
        if (params.size() > 1) {
            userParams = Arrays.copyOfRange(paramArray, 1, paramArray.length);
        }

        List<String> sqlStatements = SQLLexer.splitStatements(sql);
        String[] stmtsArray = sqlStatements.toArray(new String[sqlStatements.size()]);

        // do initial naive scan of statements for DDL, forbid mixed DDL and (DML|DQL)
        Boolean hasDDL = null;
        // conflictTables tracks dropped tables before removing the ones that don't have CREATEs.
        SortedSet<String> conflictTables = new TreeSet<String>();
        Set<String> createdTables = new HashSet<String>();
        for (String stmt : sqlStatements) {
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
                // No mixing DDL and DML/DQL.  Turn this into an error returned to client.
                return makeQuickResponse(
                        ClientResponse.GRACEFUL_FAILURE,
                        "DDL mixed with DML and queries is unsupported.");
            }
            // do a couple of additional checks if it's DDL
            if (hasDDL) {
                // check that the DDL is allowed
                String rejectionExplanation = SQLLexer.checkPermitted(stmt);
                if (rejectionExplanation != null) {
                    return makeQuickResponse(
                            ClientResponse.GRACEFUL_FAILURE,
                            rejectionExplanation);
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

            // TODO: this is where the @SwapTables needs to be made to work
            /*if (invocationName.equals("@SwapTables")) {
                final AsyncCompilerResult result = compileSysProcPlan(w);
                w.completionHandler.onCompletion(result);
                return;
            }*/

            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "Failed to plan, no SQL statement provided.");
        }
        else if (!hasDDL) {
            // this is where we run non-DDL sql statements
            return runNonDDLAdHoc(VoltDB.instance().getCatalogContext(),
                                  sqlStatements,
                                  true,
                                  null,
                                  ExplainMode.NONE,
                                  userParams);
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
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, sb.toString());
            }

            // Is it forbidden by the replication role and configured schema change method?
            // master and UAC method chosen:
            //CatalogContext context = VoltDB.instance().getCatalogContext();
            //VoltDB.instance().get
            boolean useAdhocDDL = true;
            // TODO fix this hack
            if (!useAdhocDDL) {
                return makeQuickResponse(
                        ClientResponse.GRACEFUL_FAILURE,
                        "Cluster is configured to use @UpdateApplicationCatalog " +
                        "to change application schema.  AdHoc DDL is forbidden.");
            }

            // TODO re-enable this check
            /*if (!allowPausedModeWork(internalCall, adminConnection)) {
                return makeQuickResponse(
                        ClientResponse.SERVER_UNAVAILABLE,
                        "Server is paused and is available in read-only mode - please try again later.");
            }*/

            ChangeDescription ccr = null;
            try {
                ccr = prepareApplicationCatalogDiff(invocationName,
                                                    null,
                                                    null,
                                                    sqlStatements.toArray(new String[0]),
                                                    null,
                                                    false,
                                                    DrRoleType.NONE,
                                                    true,
                                                    false,
                                                    "hostname.server.com",
                                                    "NOUSER");
            }
            catch (Exception e) {
                hostLog.info("A request to update the database catalog and/or deployment settings has been rejected. More info returned to client.");
                // TODO: return proper error from exception
                return makeQuickResponse(ClientResponse.UNEXPECTED_FAILURE, "ALL IS LOST");
            }

            // case for @CatalogChangeResult
            if (ccr.encodedDiffCommands.trim().length() == 0) {
                return makeQuickResponse(ClientResponseImpl.SUCCESS, "Catalog update with no changes was skipped.");
            }

            // initiate the transaction.
            return callProcedure("@UpdateCore",
                                 ccr.encodedDiffCommands,
                                 ccr.catalogHash,
                                 ccr.catalogBytes,
                                 ccr.expectedCatalogVersion,
                                 ccr.deploymentString,
                                 ccr.tablesThatMustBeEmpty,
                                 ccr.reasonsForEmptyTables,
                                 ccr.requiresSnapshotIsolation ? 1 : 0,
                                 ccr.worksWithElastic ? 1 : 0,
                                 ccr.deploymentHash,
                                 ccr.hasSchemaChange ?  1 : 0);
        }
    }
}
