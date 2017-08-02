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

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;

/**
 *
 * This system NT-procedure is used by UpdateCore related work to verify and write
 * the catalog bytes. The purpose of this procedure is to put the catalog verification and write
 * work on the asynchronous thread and reduce its blocking time.
 */
public class VerifyCatalogAndWriteJar extends UpdateApplicationBase {

    public final static HashMap<Integer, String> SupportedJavaVersionMap = new HashMap<>();
    static {
        SupportedJavaVersionMap.put(45, "Java 1.1");
        SupportedJavaVersionMap.put(46, "Java 1.2");
        SupportedJavaVersionMap.put(47, "Java 1.3");
        SupportedJavaVersionMap.put(48, "Java 1.4");
        SupportedJavaVersionMap.put(49, "Java 5");
        SupportedJavaVersionMap.put(50, "Java 6");
        SupportedJavaVersionMap.put(51, "Java 7");
        SupportedJavaVersionMap.put(52, "Java 8");
    }

    VoltLogger log = new VoltLogger("HOST");

    public CompletableFuture<ClientResponse> run(byte[] catalogBytes, String diffCommands,
            byte[] catalogHash, byte[] deploymentBytes)
    {
        log.info("Precheck and prepare catalog update on non-blocking asynchronous threads");

        // This should only be called once on each host
        String err = VoltDB.instance().verifyJarAndPrepareProcRunners(
                catalogBytes, diffCommands, catalogHash, deploymentBytes);
        if (err != null) {
            return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE, err);
        }

        // Write the new catalog to a temporary jar file
        try {
            VoltDB.instance().writeCatalogJar(catalogBytes);
        } catch (Exception e) {
            // Catalog disk write failed, include the message
            VoltDB.instance().cleanUpTempCatalogJar();
            return makeQuickResponse(ClientResponseImpl.UNEXPECTED_FAILURE, e.getMessage());
        }

        return makeQuickResponse(ClientResponseImpl.SUCCESS, "");
    }
}
