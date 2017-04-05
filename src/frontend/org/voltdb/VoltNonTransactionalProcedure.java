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

package org.voltdb;

import java.util.concurrent.CompletableFuture;

import org.voltdb.client.ClientResponse;

/**
 * Base class for any user provided non-transactional procedures.
 * To be a valid procedure, there muse exist a run(..) method that
 * accepts compatible parameters and returns one of:
 * long, VoltTable, VoltTable[], CompletableFuture<ClientResponse>
 *
 */
public class VoltNonTransactionalProcedure {

    ProcedureRunnerNT m_runner = null;

    /**
     * Calls a procedure (either transactional or not) and returns a CompletableFuture that can
     * be returned from the procedure.
     *
     * To add a callback to the future, try:
     * return callProcedure(procname, params).thenApply(javafunction);
     *
     * Where "javafunction" takes a ClientResponse and returns an acceptable procedure return type.
     *
     * @return
     */
    public CompletableFuture<ClientResponse> callProcedure(String procName, Object... params) {
        return m_runner.callProcedure(procName, params);
    }

    /**
     * Get the ID of cluster that the client connects to.
     * @return An ID that identifies the VoltDB cluster
     */
    public int getClusterId() {
        return m_runner.getClusterId();
    }

    /**
     * Set the status code that will be returned to the client. This is not the same as the status
     * code returned by the server. If a procedure sets the status code and then rolls back or causes an error
     * the status code will still be propagated back to the client so it is always necessary to check
     * the server status code first.
     *
     * @param statusCode Byte-long application-specific status code.
     */
    public void setAppStatusCode(byte statusCode) {
        m_runner.setAppStatusCode(statusCode);
    }

    /**
     * Set the string that will be turned to the client. This is not the same as the status string
     * returned by the server. If a procedure sets the status string and then rolls back or causes an error
     * the status string will still be propagated back to the client so it is always necessary to check
     * the server status code first.
     *
     * @param statusString Application specific status string.
     */
    public void setAppStatusString(String statusString) {
        m_runner.setAppStatusString(statusString);
    }
}
