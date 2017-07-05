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

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;

public class WriteCatalog extends UpdateApplicationBase {

    // TODO: maybe we can add another option to clean up the
    // temporary jar files on all hosts, in case some operations
    // failed in the middle of the way ?

    public static final byte WRITE = 0;
    public static final byte CLEAN_UP = 1;
    public static final byte VERIFY = 2;

    VoltLogger log = new VoltLogger("HOST");

    // Write the new catalog to a temporary jar file
    public CompletableFuture<ClientResponse> run(byte[] catalogBytes, byte mode) throws Exception
    {
        // This should only be called once on each host
        if (mode == WRITE) {
            try {
                VoltDB.instance().writeCatalogJar(catalogBytes);
            } catch (IOException e) {
                // Catalog disk write failed, include the message
                return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE, e.getMessage());
            }
        } else if (mode == CLEAN_UP) {
            VoltDB.instance().cleanUpTempCatalogJar();
        } else if (mode == VERIFY) {
            if (!VoltDB.instance().verifyZKCatalog()) {
                return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE, "Catalog verification failed.");
            }
        } else {
            return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE, "The mode " + Byte.toString(mode) +
                                     " is not supported in @WriteCatalog operation.");
        }

        return makeQuickResponse(ClientResponseImpl.SUCCESS, "Catalog update work finished locally with mode " +
                                 Byte.toString(mode));
    }
}
