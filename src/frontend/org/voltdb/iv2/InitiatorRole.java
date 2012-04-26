/* This file is part of VoltDB.
 * Copyright (C) 2008-2012 VoltDB Inc.
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

package org.voltdb.iv2;


import org.voltcore.utils.Pair;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.InitiateTaskMessage;

public interface InitiatorRole
{
    /**
     * Forward a new transaction.
     *
     * If this is the primary initiator, the caller must call this in a
     * synchronized block to guarantee that the transactions will be queued in
     * order.
     */
    public void offerInitiateTask(InitiateTaskMessage message);

    /**
     * Forward a transaction completion.
     *
     * @return A pair of HSId and response message if the response message needs
     *         to be forwarded to the site designated by the HSId, or null if no
     *         forwarding is required.
     */
    public Pair<Long, InitiateResponseMessage> offerResponse(InitiateResponseMessage message);

    /**
     * Request the next new transaction to execute.
     *
     * @return Next transaction to execute, or null if none is ready.
     */
    public InitiateTaskMessage poll();
}
