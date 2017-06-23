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

import java.util.concurrent.CompletableFuture;

import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;

public class WriteCatalog extends UpdateApplicationBase {
    // Write the new catalog to a temporary jar file
    public CompletableFuture<ClientResponse> run(byte[] catalogBytes) throws Exception
    {
        // This should only be called once on each host
        try {
            VoltDB.instance().writeCatalogJar(catalogBytes);
        } catch (RuntimeException e) {
            return makeQuickResponse(ClientResponseImpl.GRACEFUL_FAILURE, e.getMessage());
        }

        return makeQuickResponse(ClientResponseImpl.SUCCESS, "Catalog update finished locally.");
    }
}
