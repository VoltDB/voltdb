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

import org.hsqldb_voltpatches.lib.StringUtil;
import org.voltdb.CatalogContext;
import org.voltdb.ClientInterface.ExplainMode;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.catalog.Table;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.CatalogUtil;

/**
 * Non-transactional procedure that turns around and creates a SQL statement
 * string "@SwapTables TableA TableB" and passes it on to AdHoc.
 *
 */
public class SwapTables extends AdHocNTBase {

    @Override
    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        return runInternal(params);
    }

    @Override
    protected CompletableFuture<ClientResponse> runUsingCalcite(ParameterSet params) {
        return runUsingLegacy(params);
    }
    @Override
    protected CompletableFuture<ClientResponse> runUsingLegacy(ParameterSet params) {
        assert params.size() == 2;
        assert params.getParam(0) instanceof String &&
                params.getParam(1) instanceof String;
        return run((String) params.getParam(0), (String) params.getParam(1));
    }

    private CompletableFuture<ClientResponse> run(String theTable, String otherTable) {
        final String sql = "@SwapTables " + theTable + " " + otherTable;
        final Object[] userParams = null;
        final CatalogContext context = VoltDB.instance().getCatalogContext();
        final List<String> sqlStatements = new ArrayList<>(1);
        sqlStatements.add(sql);
        final Table ttlTable = CatalogUtil.getTimeToLiveTables(context.database).get(theTable.toUpperCase());
        if (ttlTable != null && !StringUtil.isEmpty(ttlTable.getMigrationtarget())) {
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                    String.format("Table %s cannot be swapped since it uses TTL.",theTable));
        } else if (CatalogUtil.hasShadowStream(theTable) || CatalogUtil.hasShadowStream(otherTable)) {
            return makeQuickResponse(ClientResponse.GRACEFUL_FAILURE,
                    String.format("Table %s cannot be swapped since it is used for exporting.",theTable));
        } else {
            return runNonDDLAdHoc(context, sqlStatements, true, null, ExplainMode.NONE,
                    false, true, userParams);
        }
    }
}
