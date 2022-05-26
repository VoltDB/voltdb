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
import java.util.List;
import java.util.concurrent.CompletableFuture;

import org.voltdb.*;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.compilereport.ViewExplainer;
import org.voltdb.parser.SQLLexer;

//Go to the catalog and fetch all the "explain plan" strings of the queries in the view.
public class ExplainView extends AdHocNTExplain {

    @Override
    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        return runInternal(params);
    }

    @Override
    protected CompletableFuture<ClientResponse> runUsingLegacy(ParameterSet params) {
        return run(stringOf(params));
    }

    @Override
    public CompletableFuture<ClientResponse> run(String fullViewNames) {
        CatalogContext context = VoltDB.instance().getCatalogContext();
        /*
         * TODO: We don't actually support multiple view names in an ExplainView call,
         * so I THINK that the string is always a single view symbol and all this
         * splitting and iterating is a no-op.
         */
        List<String> viewNames = SQLLexer.splitStatements(fullViewNames).getCompletelyParsedStmts();
        int size = viewNames.size();
        VoltTable[] vt = new VoltTable[size];
        CatalogMap<Table> tables = context.database.getTables();

        for (int i = 0; i < size; i++) {
            String viewName = viewNames.get(i);

            // look in the catalog
            // get the view table from the catalog
            Table viewTable = tables.getIgnoreCase(viewName);
            if (viewTable == null) {
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, "View " + viewName + " does not exist.");
            }

            vt[i] = new VoltTable(new VoltTable.ColumnInfo("TASK",           VoltType.STRING),
                                  new VoltTable.ColumnInfo("EXECUTION_PLAN", VoltType.STRING));
            try {
                ArrayList<String[]> viewExplanation = ViewExplainer.explain(viewTable);
                if (viewExplanation.size() == 0) {
                    vt[i].addRow("", "No query plan is being used.");
                }
                for (String[] row : viewExplanation) {
                    vt[i].addRow(row[0], row[1]);
                }
            } catch (Exception e) {
                return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE, e.getMessage());
            }
        }

        final ClientResponseImpl response = new ClientResponseImpl(
                ClientResponseImpl.SUCCESS, ClientResponse.UNINITIALIZED_APP_STATUS_CODE,
                null, vt, null);
        CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        fut.complete(response);
        return fut;
    }
}
