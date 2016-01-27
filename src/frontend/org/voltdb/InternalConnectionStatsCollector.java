/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltdb.client.ClientResponse;


/**
 * Methods to report success, failure, retries etc. of internal connection transactions.
 */
public interface InternalConnectionStatsCollector
{
    /**
     * Used to report that a request completion details.
     *
     * @param callerName a name identifying the request invoker
     * @param procName name of the procedure that is used in the transaction request.
     * @param response ClientResponse with response details
     */
    public void reportCompletion(String callerName, String procName, ClientResponse response);
}
