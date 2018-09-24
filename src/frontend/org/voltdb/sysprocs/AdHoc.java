/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.newplanner.SqlBatch;
import org.voltdb.newplanner.SqlTask;
import org.voltdb.parser.SQLLexer;

public class AdHoc extends AdHocNTBase {

    /**
     * Run an AdHoc query batch through the Calcite planner.
     * @param params the user parameters. The first parameter is always the query text.
     * The rest parameters are the ones used in the queries. </br>
     * Some notes:
     * <ul>
     *   <li>AdHoc DDLs do not take parameters ("?" will be treated as an unexpected token);</li>
     *   <li>Currently, a non-DDL batch can take parameters only if the batch has one query.</li>
     *   <li>We do not handle large query mode now. The special flag for swap tables is also
     *       eliminated. They both need to be re-designed in the new Calcite framework.</li>
     * </ul>
     * @return the client response.
     * @since 8.4
     * @author Yiqun Zhang
     */
    public CompletableFuture<ClientResponse> runThroughCalcite(ParameterSet params) {
        // TRAIL [Calcite:1] [entry] AdHoc.runThroughCalcite()
        SqlBatch batch;
        try {
            // We do not need to worry about the ParameterSet,
            // AdHocAcceptancePolicy will sanitize the parameters ahead of time.
            batch = SqlBatch.fromParameterSet(params);
            if (batch.isDDLBatch()) {
                return runDDLBatchThroughCalcite(batch);
            } else {
                // Large query mode should be set to m_backendTargetType.isLargeTempTableTarget
                // But for now let's just disable it.
                return runNonDDLBatchThroughCalcite(batch);
            }
        } catch (Exception ex) {
            // For now, let's just fail the batch if any error happens.
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                                     ex.getLocalizedMessage());
        }
    }

    private boolean shouldBeUsingCalcite() {
        if (m_backendTargetType.isLargeTempTableTarget) {
            return false;
        }
        return true;
    }

    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        // TRAIL [Calcite:0] [entry] AdHoc.run()
        if (shouldBeUsingCalcite()) {
            return runThroughCalcite(params);
        }
        if (params.size() == 0) {
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                    "Adhoc system procedure requires at least the query parameter.");
        }

        Object[] paramArray = params.toArray();
        String sql = (String) paramArray[0];
        Object[] userParams = null;
        if (params.size() > 1) {
            userParams = Arrays.copyOfRange(paramArray, 1, paramArray.length);
        }

        List<String> sqlStatements = new ArrayList<>();
        AdHocSQLMix mix = processAdHocSQLStmtTypes(sql, sqlStatements);

        if (mix == AdHocSQLMix.EMPTY) {
            // we saw neither DDL or DQL/DML.  Make sure that we get a
            // response back to the client
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "Failed to plan, no SQL statement provided.");
        }

        else if (mix == AdHocSQLMix.MIXED) {
            // No mixing DDL and DML/DQL.  Turn this into an error returned to client.
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "DDL mixed with DML and queries is unsupported.");
        }

        else if (mix == AdHocSQLMix.ALL_DML_OR_DQL) {
            // this is where we run non-DDL sql statements
            return runNonDDLAdHoc(VoltDB.instance().getCatalogContext(),
                                  sqlStatements,
                                  true, // infer partitioning
                                  null, // no partition key
                                  ExplainMode.NONE,
                                  m_backendTargetType.isLargeTempTableTarget, // back end dependent.
                                  false, // is not swap tables
                                  userParams);
        }

        // at this point assume all DDL
        assert(mix == AdHocSQLMix.ALL_DDL);

        return runDDLBatch(sqlStatements);
    }

    /**
     * Run a DDL batch through Calcite.
     * @param batch the batch to run.
     * @return the client response.
     * @since 8.4
     * @author Yiqun Zhang
     */
    private CompletableFuture<ClientResponse> runDDLBatchThroughCalcite(SqlBatch batch) {
        // For now let's keep using the pre-existing parser for DDL.
        List<String> sqlStatements = new ArrayList<>(batch.getTaskCount());
        for (SqlTask task : batch) {
            sqlStatements.add(task.getSQL());
        }
        return runDDLBatch(sqlStatements);
    }

    private CompletableFuture<ClientResponse> runDDLBatch(List<String> sqlStatements) {
        // conflictTables tracks dropped tables before removing the ones that don't have CREATEs.
        SortedSet<String> conflictTables = new TreeSet<String>();
        Set<String> createdTables = new HashSet<String>();

        for (String stmt : sqlStatements) {
            // check that the DDL is allowed
            String rejectionExplanation = SQLLexer.checkPermitted(stmt);
            if (rejectionExplanation != null) {
                return makeQuickResponse(
                        ClientResponse.GRACEFUL_FAILURE,
                        rejectionExplanation);
            }

            String ddlToken = SQLLexer.extractDDLToken(stmt);
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

        if (!allowPausedModeWork(false, isAdminConnection())) {
            return makeQuickResponse(
                    ClientResponse.SERVER_UNAVAILABLE,
                    "Server is paused and is available in read-only mode - please try again later.");
        }

        boolean useAdhocDDL = VoltDB.instance().getCatalogContext().cluster.getUseddlschema();
        if (!useAdhocDDL) {
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "Cluster is configured to use @UpdateApplicationCatalog " +
                    "to change application schema.  AdHoc DDL is forbidden.");
        }

        logCatalogUpdateInvocation("@AdHoc");

        return updateApplication("@AdHoc",
                                null,
                                null,
                                sqlStatements.toArray(new String[0]),
                                null,
                                false,
                                true);
    }
}
