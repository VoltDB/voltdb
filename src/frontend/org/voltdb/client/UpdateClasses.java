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
import java.util.concurrent.CompletableFuture;

import org.voltdb.common.Constants;

/**
 * Utility class, providing methods to load updated classes
 * into a VoltDB database.
 * <p>
 * Each such method is a convenience method that is equivalent to reading
 * a jarfile, containing classes to be added/updated, into a byte array,
 * then using the client API to execute a call to the <code>@UpdateClasses</code>
 * system procedure. Classes can be removed from the database by
 * giving their names in a separate argument.
 */
public class UpdateClasses {

    /**
     * Synchronously updates class definitions. Blocks until a
     * result is available. A {@link ProcCallException} is thrown if the
     * response is anything other than success.
     *
     * @param client A connected {@link Client}.
     * @param jarPath Path to the jar file containing new/updated classes.
     * @param classesToDelete comma-separated list of classes to delete.
     *
     * @return {@link ClientResponse} instance of procedure call result.
     * @throws IOException If the file cannot be serialized or if there is a Java network error.
     * @throws ProcCallException on any VoltDB-specific failure.
     */
    public static ClientResponse update(Client client,
                                        File jarPath,
                                        String classesToDelete)
        throws IOException, ProcCallException {

        byte[] jarbytes = null;
        if (jarPath != null) {
            jarbytes = ClientUtils.fileToBytes(jarPath);
        }

        return client.callProcedure("@UpdateClasses", jarbytes, classesToDelete);
    }

    /**
     * Asynchronously updates class definitions. Does not guarantee that
     * the invocation was actually queued: check the return value to determine
     * if queuing actually took place.
     *
     * @param client A connected {@link Client}.
     * @param callback The {@link ProcedureCallback} that will be invoked with the result.
     * @param jarPath Path to the jar file containing new/updated classes.
     * @param classesToDelete comma-separated list of classes to delete.
     *
     * @return <code>true</code> if the procedure was queued and <code>false</code> otherwise.
     * @throws IOException If the file cannot be serialized or if there is a Java network error.
     */
    public static boolean update(Client client,
                                 ProcedureCallback callback,
                                 File jarPath,
                                 String classesToDelete) throws IOException {

        byte[] jarbytes = null;
        if (jarPath != null) {
            jarbytes = ClientUtils.fileToBytes(jarPath);
        }

        return client.callProcedure(callback, "@UpdateClasses", jarbytes, classesToDelete);
    }

    /**
     * Synchronously updates class definitions via a "version 2" client.
     * Blocks until a  result is available. A {@link ProcCallException}
     * is thrown if the response is anything other than success.
     *
     * @param client A connected {@link Client2}.
     * @param jarPath Path to the jar file containing new/updated classes.
     * @param classesToDelete comma-separated list of classes to delete.
     *
     * @return {@link ClientResponse} instance of procedure call result.
     * @throws IOException If the file cannot be serialized or if there is a Java network error.
     * @throws ProcCallException on any VoltDB-specific failure.
     */
    public static ClientResponse updateSync(Client2 client,
                                            File jarPath,
                                            String classesToDelete)
        throws IOException, ProcCallException {

        byte[] jarbytes = null;
        if (jarPath != null) {
            jarbytes = ClientUtils.fileToBytes(jarPath);
        }

        return client.callProcedureSync("@UpdateClasses", jarbytes, classesToDelete);
    }

    /**
     * Asynchronously updates class definitions via a "version 2" client.
     * Completion is communicated via a returned <code>CompletableFuture</code>.
     *
     * @param client A connected {@link Client2}.
     * @param jarPath Path to the jar file containing new/updated classes.
     * @param classesToDelete comma-separated list of classes to delete.
     *
     * @return a {@code CompletableFuture} that completes when the procedure call completes.
     * @throws IOException If the file cannot be serialized.
     */
    public static CompletableFuture<ClientResponse> updateAsync(Client2 client,
                                                                File jarPath,
                                                                String classesToDelete)
        throws IOException {

        byte[] jarbytes = null;
        if (jarPath != null) {
            jarbytes = ClientUtils.fileToBytes(jarPath);
        }

        return client.callProcedureAsync("@UpdateClasses", jarbytes, classesToDelete);
    }
}
