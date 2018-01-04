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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.parser.SQLLexer;

/**
 * A flavor of AdHoc that is only used in VoltDB tests. It allows you to force
 * DQL or DML to be single-partition by including a single partition key object
 * just after the sql statement (and before optional parameters).
 *
 */
public class AdHocSpForTest extends AdHocNTBase {
    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        if (params.size() < 2) {
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    String.format("@AdHocSpForTest expects at least 2 paramters but got %d.", params.size()));
        }

        Object[] paramArray = params.toArray();
        String sql = (String) paramArray[0];
        Object userPartitionKey = paramArray[1];

        // get any user params from the end
        Object[] userParams = null;
        if (params.size() > 2) {
            userParams = Arrays.copyOfRange(paramArray, 2, paramArray.length);
        }

        List<String> sqlStatements = SQLLexer.splitStatements(sql).getCompletelyParsedStmts();
        if (sqlStatements.size() != 1) {
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "@AdHocSpForTest expects precisely one statement (no batching).");
        }

        // do initial naive scan of statements for DDL, forbid mixed DDL and (DML|DQL)
        for (String stmt : sqlStatements) {
            if (SQLLexer.isComment(stmt) || stmt.trim().isEmpty()) {
                continue;
            }
            String ddlToken = SQLLexer.extractDDLToken(stmt);
            if (ddlToken != null) {
                return makeQuickResponse(
                        ClientResponse.GRACEFUL_FAILURE,
                        "@AdHocSpForTest doesn't support DDL.");
            }
        }

        return runNonDDLAdHoc(VoltDB.instance().getCatalogContext(),
                              sqlStatements,
                              false, // Do not infer partitioning
                              userPartitionKey,
                              ExplainMode.NONE,
                              false, // not a large query
                              false, // not swap tables
                              userParams);
    }
}
