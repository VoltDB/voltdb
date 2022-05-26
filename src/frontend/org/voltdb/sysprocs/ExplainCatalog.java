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

import java.util.concurrent.CompletableFuture;

import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.client.ClientResponse;
import org.voltdb.exceptions.PlanningErrorException;

public class ExplainCatalog extends AdHocNTBase {

    @Override
    public CompletableFuture<ClientResponse> run(ParameterSet params) {
        return runUsingLegacy(params);
    }

    @Override
    public CompletableFuture<ClientResponse> runUsingCalcite(ParameterSet params) {
        throw new PlanningErrorException("Unsupported operation");
    }

    @Override
    protected CompletableFuture<ClientResponse> runUsingLegacy(ParameterSet params) {
        VoltTable[] ret = new VoltTable[] { new VoltTable(new VoltTable.ColumnInfo("CATALOG", VoltType.STRING)) };
        ret[0].addRow(VoltDB.instance().getCatalogContext().catalog.serialize());
        ClientResponseImpl response = new ClientResponseImpl(ClientResponseImpl.SUCCESS, ret, null);
        CompletableFuture<ClientResponse> fut = new CompletableFuture<>();
        fut.complete(response);
        return fut;
    }
}
