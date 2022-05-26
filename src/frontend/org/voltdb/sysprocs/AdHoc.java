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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.parser.SQLLexer;
import org.voltdb.plannerv2.SqlBatch;
import org.voltdb.plannerv2.guards.PlannerFallbackException;

public class AdHoc extends AdHocNTBase {

    @Override
    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        return runInternal(params);
    }

    /**
     * Run an AdHoc query batch through Calcite parser and planner. If there is
     * anything that Calcite cannot handle, we will let it fall back to the
     * legacy parser and planner.
     *
     * @param params
     *            the user parameters. The first parameter is always the query
     *            text. The rest parameters are the ones used in the queries.
     *            </br>
     * @return the client response.
     * @since 9.0
     * @author Yiqun Zhang
     */
    @Override
    protected CompletableFuture<ClientResponse> runUsingCalcite(ParameterSet params) throws SqlParseException {
        // TRAIL [Calcite-AdHoc-DQL/DML:0] AdHoc.run()
        // TRAIL [Calcite-AdHoc-DDL:0] AdHoc.run()
        /**
         * Some notes: 1. AdHoc DDLs do not take parameters - "?" will be
         * treated as an unexpected token; 2. Currently, a DML/DQL batch can
         * take parameters only if the batch has one query. 3. We do not handle
         * large query mode now. The special flag for swap tables is also
         * eliminated. They both need to be re-designed in the new Calcite
         * framework.
         */
        try { // We do not need to worry about the ParameterSet;
            // AdHocAcceptancePolicy will sanitize the parameters ahead of time.
            return SqlBatch.from(params, m_context, ExplainMode.NONE).execute();
        } catch (PlannerFallbackException | SqlParseException ex) { // Use the legacy planner to run this.
            throw ex;
        } catch (Exception ex) {        // For any other non-calcite related exceptions, fail the batch.
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, ex.getMessage());
        }
    }

    @Override
    protected CompletableFuture<ClientResponse> runUsingLegacy(ParameterSet params) {
        if (params.size() == 0) {
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                    "Adhoc system procedure requires at least the query parameter.");
        }

        final Object[] paramArray = params.toArray();
        final String sql = (String) paramArray[0];
        final Object[] userParams = params.size() > 1 ? Arrays.copyOfRange(paramArray, 1, paramArray.length) : null;

        final List<String> sqlStatements = new ArrayList<>();
        switch (processAdHocSQLStmtTypes(sql, sqlStatements)) {
            case EMPTY:                 // we saw neither DDL or DQL/DML. Make sure that we get a response back to the client
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, "Failed to plan, no SQL statement provided.");
            case MIXED:                 // No mixing DDL and DML/DQL. Turn this into an error returned to client.
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, "DDL mixed with DML and queries is unsupported.");
            case ALL_DML_OR_DQL:        // this is where we run non-DDL sql statements
                return runNonDDLAdHoc(VoltDB.instance().getCatalogContext(), sqlStatements, true,
                        null, // no partition key
                        ExplainMode.NONE, m_backendTargetType.isLargeTempTableTarget, // back end dependent
                        false, // is not swap tables
                        userParams);
            case ALL_DDL:               // Since we are not going through Calcite, there is no need to update CalciteSchema.
                return runDDLBatch(sqlStatements, Collections.emptyList());
            default:                    // should never reach here
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, "Unsupported/unknown SQL statement type.");
        }
    }

    private CompletableFuture<ClientResponse> runDDLBatch(List<String> sqlStatements, List<SqlNode> sqlNodes) {
        // conflictTables tracks dropped tables before removing the ones that don't have CREATEs.
        final SortedSet<String> conflictTables = new TreeSet<>();
        final Set<String> createdTables = new HashSet<>();

        for (String stmt : sqlStatements) {         // check that the DDL is allowed
            String rejectionExplanation = SQLLexer.checkPermitted(stmt);
            if (rejectionExplanation != null) {
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, rejectionExplanation);
            }

            final String ddlToken = SQLLexer.extractDDLToken(stmt);
            // make sure not to mix drop and create in the same batch for the
            // same table
            if (ddlToken.equals("drop")) {
                String tableName = SQLLexer.extractDDLTableName(stmt);
                if (tableName != null) {
                    conflictTables.add(tableName);
                }
            } else if (ddlToken.equals("create")) {
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
        } else if (!allowPausedModeWork(false, isAdminConnection())) {
            return makeQuickResponse(ClientResponse.SERVER_UNAVAILABLE,
                    "Server is paused and is available in read-only mode - please try again later.");
        } else {
            final boolean useAdhocDDL = VoltDB.instance().getCatalogContext().cluster.getUseddlschema();
            if (!useAdhocDDL) {
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                        "Cluster is configured to use @UpdateApplicationCatalog "
                                + "to change application schema.  AdHoc DDL is forbidden.");
            } else {
                logCatalogUpdateInvocation("@AdHoc");
                return updateApplication("@AdHoc", null, /* operationBytes */
                        null, sqlStatements.toArray(new String[0]), /* adhocDDLStmts */
                        sqlNodes, false);
            }
        }
    }

    /**
     * The {@link org.voltdb.plannerv2.SqlBatch} was designed to be
     * self-contained. However, this is not entirely true due to the way that
     * the legacy code was organized. Until I have further reshaped the legacy
     * code path, I will leave this interface to call back into the private
     * methods of {@link org.voltdb.sysprocs.AdHoc}.
     *
     * @author Yiqun Zhang
     * @since 9.0
     */
    private class AdHocContext extends AdHocNTBaseContext {
        @Override public CompletableFuture<ClientResponse> runDDLBatch(List<String> sqlStatements,
                List<SqlNode> sqlNodes) {
            return AdHoc.this.runDDLBatch(sqlStatements, sqlNodes);
        }
    }

    private AdHocContext m_context = new AdHocContext();
}
