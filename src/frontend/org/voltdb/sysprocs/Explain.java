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
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.parser.SQLLexer;

public class Explain extends AdHocNTBase {

    //dispatchAdHoc(task, handler, ccxn, ExplainMode.EXPLAIN_ADHOC, user);

    public CompletableFuture<ClientResponse> run(ParameterSet params) {

        // dipatch common

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
                                  ExplainMode.EXPLAIN_ADHOC,
                                  false,
                                  userParams);
        }
        else {
            // We have adhoc DDL.  Is it okay to run it?
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "Explain doesn't support DDL.");
        }
    }

}
