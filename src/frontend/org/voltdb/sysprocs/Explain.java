/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.apache.calcite.sql.parser.SqlParseException;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.plannerv2.SqlBatch;
import org.voltdb.plannerv2.guards.PlannerFallbackException;

public class Explain extends AdHocNTBase {
    private AdHocNTBaseContext m_context = new AdHocNTBaseContext();

    /**
     * Run an AdHoc explain batch through Calcite parser and planner. If there is
     * anything that Calcite cannot handle, we will let it fall back to the
     * legacy parser and planner.
     *
     * @param params
     *            the user parameters. The first parameter is always the query
     *            text. The rest parameters are the ones used in the queries.
     *            </br>
     * @return the client response.
     * @since 9.0
     * @author Chao Zhou
     */
    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        SqlBatch batch;
        try {
            // We do not need to worry about the ParameterSet,
            // AdHocAcceptancePolicy will sanitize the parameters ahead of time.
            batch = SqlBatch.from(params, m_context, ExplainMode.EXPLAIN_ADHOC);
            return batch.execute();
        } catch (PlannerFallbackException | SqlParseException ex) {
            // Use the legacy planner to run this.
            return runFallback(params);
        } catch (Exception ex) {
            // For now, let's just fail the batch if any error happens.
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, ex.getMessage());
        }
    }

    public CompletableFuture<ClientResponse> runFallback(ParameterSet params) {
        // dispatch common
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

        else if (mix == AdHocSQLMix.ALL_DDL) {
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "Explain doesn't support DDL.");
        }

        // assume all DML/DQL at this point
        return runNonDDLAdHoc(VoltDB.instance().getCatalogContext(),
                sqlStatements,
                true, // infer partitioning
                null, // no partition key
                ExplainMode.EXPLAIN_ADHOC,
                false, // not a large query
                false, // not swap tables
                userParams);
    }
}
