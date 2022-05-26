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

package org.voltdb.client;

import java.io.File;
import java.io.IOException;

import org.voltdb.common.Constants;

/**
 * Previously part of the Client interface, but deprecated there,
 * so reincarnated as a standalone utility for use by test programs
 * and by SQLCommand.
 */
public class UpdateApplicationCatalog {

    /**
     * Synchronously invokes the UpdateApplicationCatalog procedure. Blocks
     * until a result is available. A {@link ProcCallException} is thrown if the
     * response is anything other than success.
     *
     * @param client connected {@link Client} instance.
     * @param catalogPath Path to the catalog jar file.
     * @param deploymentPath Path to the deployment file.
     * @return {@link ClientResponse} instance of procedure call results.
     * @throws IOException If the files cannot be serialized or if there is a Java network error.
     * @throws ProcCallException on any VoltDB specific failure.
     */
    public static ClientResponse update(Client client, File catalogPath, File deploymentPath)
        throws IOException, ProcCallException {
        Object[] params = makeParams(catalogPath, deploymentPath);
        return client.callProcedure("@UpdateApplicationCatalog", params);
    }

    /**
     * Asynchronously invokes the UpdateApplicationCatalog procedure. Does not
     * guarantee that the invocation is actually queued. Check the return value
     * to determine if queuing actually took place.
     *
     * @param client connected {@link Client} instance.
     * @param callback {@link ProicedureCallback} object.
     * @param catalogPath Path to the catalog jar file.
     * @param deploymentPath Path to the deployment file.
     * @return indication of whether call was successfully queued.
     * @throws IOException If the files cannot be serialized or if there is a Java network error.
     */
    public static boolean update(Client client, ProcedureCallback callback,
                                 File catalogPath, File deploymentPath) throws IOException {
        Object[] params = makeParams(catalogPath, deploymentPath);
        return client.callProcedure(callback, "@UpdateApplicationCatalog", params);
    }

    /*
     * Serializes catalog and deployment file for UpdateApplicationCatalog.
     * Catalog is serialized into byte array, deployment file is serialized into
     * string.
     */
    private static Object[] makeParams(File catalogPath, File deploymentPath)
    throws IOException {
        Object[] params = new Object[] { null, null };
        if (catalogPath != null) {
            params[0] = ClientUtils.fileToBytes(catalogPath);
        }
        if (deploymentPath != null) {
            params[1] = new String(ClientUtils.fileToBytes(deploymentPath), Constants.UTF8ENCODING);
        }
        return params;
    }
}
