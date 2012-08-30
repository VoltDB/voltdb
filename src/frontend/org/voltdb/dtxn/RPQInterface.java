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

package org.voltdb.dtxn;

public interface RPQInterface {
    /**
     * Only return transaction state objects that are ready to run.
     */
    public OrderableTransaction poll();

    public boolean add(OrderableTransaction txnState);

    public long noteTransactionRecievedAndReturnLastSeen(long initiatorHSId, long txnId,
            boolean isHeartbeat, long lastSafeTxnIdFromInitiator);

    public Long getNewestSafeTransactionForInitiator(long initiator);

    public void gotFaultForInitiator(long initiatorId);

    public int ensureInitiatorIsKnown(long initiatorId);

    public boolean isEmpty();
}
