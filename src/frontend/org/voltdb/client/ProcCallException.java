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

/**
 * Exception thrown by the {@link SyncCallback} and {@link Client#callProcedure(String, Object...)} when a status code
 * that is not {@link ClientResponse#SUCCESS} is returned in the {@link ClientResponse}.
 * <p>
 * The message returned by {@link #getMessage()} will be the same as the status string returned by
 * {@link ClientResponse#getStatusString()}
 *
 */
public class ProcCallException extends Exception {
    private static final long serialVersionUID = 1L;
    final ClientResponse m_response;

    ProcCallException(ClientResponse response) {
        super(response.getStatusString());
        m_response = response;
    }

    /**
     * When a ProcCallException has a response from the server, retrieve it with this method.
     *
     * @return A {@link ClientResponse} associated with this exception or null.
     */
    public ClientResponse getClientResponse() {
        return m_response;
    }
}
