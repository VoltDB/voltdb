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

package org.voltdb.client;

/**
 * interface for callbacks that are invoked when an asynchronously invoked transaction receives a response.
 * Extend this class and provide an implementation of {@link #clientCallback} to receive a response to a
 * stored procedure invocation on all partitions.
 */
public interface AllPartitionProcedureCallback {
    /**
     * Implementation of callback to be provided by client applications.
     *
     * @param responses Responses for each partition to the stored procedure invocation this callback is associated with
     * @throws Exception on any Exception.
     */
     public void clientCallback(ClientResponseWithPartitionKey[] responses) throws Exception;
}
