/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
 * Listener that a client application can provide to a {@link Client} in order to receive notifications
 * when a connection is lost or backpressure occurs
 *
 * @deprecated Use {@link ClientStatusListenerExt} instead.
 */
@Deprecated
public interface ClientStatusListener {
    /**
     * Notify listeners that a connection to a host was lost.
     * @param hostname Name of the host the connect was lost to
     * @param connectionsLeft Number of remaining connections this client has to the DB
     */
    void connectionLost(String hostname, int connectionsLeft);

    /**
     * Called by the client API whenever backpressure starts/stops. Backpressure is a condition
     * where all TCP connections to the servers are full and the {@link Client} will no longer
     * queue invocations.
     * @param status <code>true</code> if there is backpressure and <code>false</code> otherwise.
     */
    void backpressure(boolean status);

    /**
     * Called when a {@link ProcedureCallback#clientCallback(ClientResponse)} invocation throws
     * an exception.
     * @param callback The callback that threw an exception.
     * @param r The response object passed to the callback.
     * @param e The exception thrown by the callback.
     */
    void uncaughtException(ProcedureCallback callback, ClientResponse r, Throwable e);
}
