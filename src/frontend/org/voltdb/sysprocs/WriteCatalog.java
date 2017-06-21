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

import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;
import org.voltdb.utils.Encoder;

public class WriteCatalog extends UpdateApplicationBase {
    VoltLogger log = new VoltLogger("HOST");

    // Write the new catalog to a temporary jar file
    public CompletableFuture<ClientResponse> run(String catalogDiffCommands,
                                                 byte[] catalogHash,
                                                 byte[] catalogBytes,
                                                 int expectedCatalogVersion,
                                                 String deploymentString,
                                                 String[] tablesThatMustBeEmpty,
                                                 String[] reasonsForEmptyTables,
                                                 byte requiresSnapshotIsolation,
                                                 byte worksWithElastic,
                                                 byte[] deploymentHash,
                                                 byte requireCatalogDiffCmdsApplyToEE,
                                                 byte hasSchemaChange,
                                                 byte requiresNewExportGeneration)
                                                    throws Exception
    {
        assert(tablesThatMustBeEmpty != null);

        String commands = Encoder.decodeBase64AndDecompress(catalogDiffCommands);
        byte[] deploymentBytes = deploymentString.getBytes("UTF-8");

//        CatalogAndIds catalogStuff = null;
//        try {
//            catalogStuff = CatalogUtil.getCatalogFromZK(VoltDB.instance().getHostMessenger().getZK());
//        } catch (Exception e) {
//            Throwables.propagate(e);
//        }

//        log.warn("=================== WriteCatalog ===================");
//        log.warn("expected cat version: " +  expectedCatalogVersion);
//        log.warn(catalogDiffCommands);
//        log.warn(deploymentString);
//        log.warn("====================================================");

        // This should only be called once on each host
        VoltDB.instance().writeCatalogJar(
                  commands,
                  catalogBytes,
                  catalogHash,
                  expectedCatalogVersion,   // not used, though
                  getID(),  // The transaction ID and the unique does not really matter for writing the catalog jar
                  Long.MAX_VALUE,
                  deploymentBytes,
                  deploymentHash,
                  requireCatalogDiffCmdsApplyToEE != 0,
                  hasSchemaChange != 0,
                  requiresNewExportGeneration != 0);

        return makeQuickResponse(ClientResponseImpl.SUCCESS, "Catalog update finished locally.");
    }
}
