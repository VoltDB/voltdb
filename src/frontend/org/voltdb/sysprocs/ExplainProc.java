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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ClientResponseImpl;
import org.voltdb.DefaultProcedureManager;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.client.ClientResponse;
import org.voltdb.parser.SQLLexer;
import org.voltdb.utils.Encoder;

public class ExplainProc extends AdHocNTBase {

    public CompletableFuture<ClientResponse> run(String procedureNames) {
        // Go to the catalog and fetch all the "explain plan" strings of the queries in the procedure.

        CatalogContext context = VoltDB.instance().getCatalogContext();

        /*
         * TODO: We don't actually support multiple proc names in an ExplainProc call,
         * so I THINK that the string is always a single procname symbol and all this
         * splitting and iterating is a no-op.
         */
        List<String> procNames = SQLLexer.splitStatements(procedureNames).getCompletelyParsedStmts();
        int size = procNames.size();
        VoltTable[] vt = new VoltTable[size];

        for (int i = 0; i < size; i++) {
            String procName = procNames.get(i);

            // look in the catalog
            Procedure proc = context.procedures.get(procName);
            if (proc == null) {
                // check default procs and send them off to be explained using the regular
                // adhoc explain process
                proc = context.m_defaultProcs.checkForDefaultProcedure(procName);
                if (proc != null) {
                    String sql = DefaultProcedureManager.sqlForDefaultProc(proc);
                    List<String> sqlStatements = new ArrayList<>(1);
                    sqlStatements.add(sql);
                    return runNonDDLAdHoc(context,
                            sqlStatements,
                            true, // infer partitioning
                            null, // no partition key
                            ExplainMode.EXPLAIN_DEFAULT_PROC,
                            false, // not a large query
                            false, // not swap tables
                            new Object[0]);
                }

                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                        "Procedure " + procName + " not in catalog");
            }

            vt[i] = new VoltTable(new VoltTable.ColumnInfo("STATEMENT_NAME", VoltType.STRING),
                                  new VoltTable.ColumnInfo("SQL_STATEMENT", VoltType.STRING),
                                  new VoltTable.ColumnInfo("EXECUTION_PLAN", VoltType.STRING));

            for(Statement stmt : proc.getStatements()) {
                vt[i].addRow(stmt.getTypeName(), stmt.getSqltext(), Encoder.hexDecodeToString(stmt.getExplainplan()));
            }
        }

        ClientResponseImpl response = new ClientResponseImpl(ClientResponseImpl.SUCCESS,
                                                             ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                                                             null,
                                                             vt,
                                                             null);

        CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        fut.complete(response);
        return fut;
    }
}
