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

import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.parser.ParserFactory;
import org.voltdb.parser.SQLLexer;

public class AdHoc extends AdHocNTBase {

    /**
     * Turn this to true to enable the Calcite parser.
     */
    private final static boolean USE_CALCITE = true;

    /**
     * Run the AdHoc query through the Calcite parser & planner.
     * @param params The user parameters. The first parameter is always the query text.
     * The rest parameters are the ones used in the query.
     * @return The client response.
     */
    public CompletableFuture<ClientResponse> runThroughCalcite(ParameterSet params) {
        // AdHocAcceptancePolicy will sanitize the parameters ahead of time.
        Object[] paramArray = params.toArray();
        String sqlBlock = (String) paramArray[0];
        Object[] userParams = null;
        // AdHoc query can have parameters, see TestAdHocQueries.testAdHocWithParams.
        if (params.size() > 1) {
            userParams = Arrays.copyOfRange(paramArray, 1, paramArray.length);
        }

        // We can process batches with either all DDL or all DML/DQL, no mixed batch can be accepted.
        // Split the SQL statements, and run them through SqlParser.
        // Currently (1.17.0), SqlParser only supports parsing single SQL statement.
        // https://issues.apache.org/jira/browse/CALCITE-2310

        // TODO: Calcite's error message will contain line and column numbers, this information is lost
        // during the split. It will be helpful to develop a way to preserve that information.
        List<String> sqlList = SQLLexer.splitStatements(sqlBlock).getCompletelyParsedStmts();
        List<SqlNode> rootNodesOfParsedQueries = new ArrayList<>(sqlList.size());
        // Are all the queries in this input batch DDL? (null means unknown)
        Boolean isDDLBatch = null;

        for (String sql : sqlList) {
            SqlParser parser = ParserFactory.create(sql);
            SqlNode sqlNode;
            try {
                sqlNode = parser.parseStmt();
            } catch (SqlParseException e) {
                // For now, let's just fail the batch if any parsing error happens.
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                        e.getLocalizedMessage());
            }
            boolean isDDL = sqlNode.isA(SqlKind.DDL);
            if (isDDLBatch == null) {
                isDDLBatch = isDDL;
            } else if (isDDLBatch ^ isDDL) { // True if isDDLBatch is different from isDDL
                // No mixing DDL and DML/DQL. Turn this into an error and return it to the client.
                return makeQuickResponse(
                        ClientResponse.GRACEFUL_FAILURE,
                        "DDL mixed with DML and queries is unsupported.");
            }
            rootNodesOfParsedQueries.add(sqlNode);
        }

        if (isDDLBatch) {
            // Should use runDDLBatchThroughCalcite when it's ready.
            return runDDLBatch(sqlList);
        } else {
            // This is where we run non-DDL SQL statements
            // Should use runNonDDLAdHocThroughCalcite when it's ready.
            return runNonDDLAdHoc(VoltDB.instance().getCatalogContext(),
                                  sqlList,
                                  true, // infer partitioning
                                  null, // no partition key
                                  ExplainMode.NONE,
                                  m_backendTargetType.isLargeTempTableTarget, // back end dependent.
                                  false, // is not swap tables
                                  userParams);
        }
    }

    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        if (USE_CALCITE) {
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

    private CompletableFuture<ClientResponse> runDDLBatchThroughCalcite(
            List<String> sqlStatements, List<SqlNode> sqlNodes) {
        return null;
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
