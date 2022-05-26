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

import java.util.Collections;
import java.util.concurrent.CompletableFuture;

import org.voltcore.logging.VoltLogger;
import org.voltdb.VoltDB;
import org.voltdb.client.ClientResponse;

/**
 * Non-transactional procedure to implement public @UpdateClasses system procedure.
 *
 */
public class UpdateClasses extends UpdateApplicationBase {
    VoltLogger log = new VoltLogger("HOST");

    public CompletableFuture<ClientResponse> run(byte[] jarfileBytes, String classesToDeleteSelector) {
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

        logCatalogUpdateInvocation("@UpdateClasses");
        // NOTE/TODO: this is not from AdHoc code branch. We use the old code path here, and don't update CalciteSchema from VoltDB catalog.
        return updateApplication("@UpdateClasses",
                                jarfileBytes,
                                classesToDeleteSelector,
                                new String[0],
                                Collections.emptyList(),
                                false /* isPromotion */
                                );
    }
}
