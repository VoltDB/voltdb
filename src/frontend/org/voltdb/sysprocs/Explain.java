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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.plannerv2.SqlBatch;
import org.voltdb.plannerv2.guards.PlannerFallbackException;

public class Explain extends AdHocNTBase {
    private final AdHocNTBaseContext m_context = new AdHocNTBaseContext();

    @Override
    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        return runInternal(params);
    }

    /**
     * Run an AdHoc explain batch through Calcite parser and planner. If there is
     * anything that Calcite cannot handle, we will let it fall back to the
     * legacy parser and planner.
     *
     * @param params the user parameters. The first parameter is always the query
     *               text. The rest parameters are the ones used in the queries.
     *               </br>
     * @return the client response.
     * @author Chao Zhou
     * @since 9.1
     */
    @Override
    protected CompletableFuture<ClientResponse> runUsingCalcite(ParameterSet params) {
        try {
            // We do not need to worry about the ParameterSet,
            // AdHocAcceptancePolicy will sanitize the parameters ahead of time.
            return SqlBatch.from(params, m_context, ExplainMode.EXPLAIN_ADHOC).execute();
        } catch (PlannerFallbackException e) {
            return runUsingLegacy(params);
        } catch (Exception ex) { // For now, let's just fail the batch if any error happens.
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, ex.getMessage());
        }
    }

    @Override
    protected CompletableFuture<ClientResponse> runUsingLegacy(ParameterSet params) {
        // dispatch common
        final Object[] paramArray = params.toArray();
        final String sql = (String) paramArray[0];
        final Object[] userParams;
        if (params.size() > 1) {
            userParams = Arrays.copyOfRange(paramArray, 1, paramArray.length);
        } else {
            userParams = null;
        }
        final List<String> sqlStatements = new ArrayList<>();
        switch (processAdHocSQLStmtTypes(sql, sqlStatements)) {
            case EMPTY: // we saw neither DDL or DQL/DML.  Make sure that we get a response back to the client
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, "Failed to plan, no SQL statement provided.");
            case MIXED: // No mixing DDL and DML/DQL.  Turn this into an error returned to client.
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, "DDL mixed with DML and queries is unsupported.");
            case ALL_DDL:
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, "Explain doesn't support DDL.");
            case ALL_DML_OR_DQL:
                return runNonDDLAdHoc(VoltDB.instance().getCatalogContext(), sqlStatements,
                        true, null, ExplainMode.EXPLAIN_ADHOC, false, false,
                        userParams);
            default:
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, "Unsupported/unknown SQL statement type.");
        }
    }
}
