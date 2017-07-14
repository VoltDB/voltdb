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

import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.client.ClientResponse;
import org.voltdb.compiler.CatalogChangeResult;

/**
 * Non-transactional procedure to implement public @UpdateClasses system procedure.
 *
 */
public class UpdateClasses extends UpdateApplicationBase {
    VoltLogger log = new VoltLogger("HOST");

    public CompletableFuture<ClientResponse> run(byte[] jarfileBytes, String classesToDeleteSelector) throws Exception {
        if (!allowPausedModeWork(false, isAdminConnection())) {
            return makeQuickResponse(
                    ClientResponse.SERVER_UNAVAILABLE,
                    "Server is paused and is available in read-only mode - please try again later.");
        }
        boolean useDDLSchema = VoltDB.instance().getCatalogContext().cluster.getUseddlschema();
        if (!useDDLSchema) {
            return makeQuickResponse(
                    ClientResponse.GRACEFUL_FAILURE,
                    "Cluster is configured to use @UpdateApplicationCatalog " +
                    "to change application schema.  Use of @UpdateClasses is forbidden.");
        }

        final String invocationName = "@UpdateClasses";

        ZooKeeper zk = VoltDB.instance().getHostMessenger().getZK();
        String blockerError = VoltZK.createCatalogUpdateBlocker(zk, VoltZK.uacActiveBlocker, hostLog, invocationName);
        if (blockerError != null) {
            return makeQuickResponse(ClientResponse.USER_ABORT, blockerError);
        }

        CatalogChangeResult ccr = prepareApplicationCatalogDiff(invocationName,
                                                                jarfileBytes,
                                                                classesToDeleteSelector,
                                                                new String[0],
                                                                null,
                                                                false, /* isPromotion */
                                                                useDDLSchema,
                                                                false,
                                                                getHostname(),
                                                                getUsername());
        if (ccr.errorMsg != null) {
            compilerLog.error(invocationName + " has been rejected: " + ccr.errorMsg);
            return cleanupAndMakeResponse(ClientResponse.USER_ABORT, ccr.errorMsg);
        }

        // Log something useful about catalog upgrades when they occur.
        if (ccr.upgradedFromVersion != null) {
            compilerLog.info(String.format("catalog was automatically upgraded from version %s.", ccr.upgradedFromVersion));
        }

        if (ccr.encodedDiffCommands.trim().length() == 0) {
            return cleanupAndMakeResponse(ClientResponseImpl.SUCCESS, invocationName +" with no catalog changes was skipped.");
        }

        return updateApplication(ccr);
    }
}
