/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.client;

/**
 * Abstract base class for callbacks that are invoked when an asynchronously invoked transaction receives a response.
 * Extend this class and provide an implementation of {@link #clientCallback} to receive a response to a
 * stored procedure invocation.
 */
public interface ProcedureCallback {
    /**
     * Implementation of callback to be provided by client applications
     * @param clientResponse Response to the stored procedure invocation this callback is associated with
     */
    abstract public void clientCallback(ClientResponse clientResponse);
}
