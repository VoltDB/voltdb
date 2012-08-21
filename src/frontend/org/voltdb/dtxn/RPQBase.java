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

import java.util.PriorityQueue;

import org.voltdb.dtxn.OrderableTransaction;

/**
 * <p>Extends a PriorityQueue such that is only stores transaction state
 * objects, and it only releases them (to a poll() call) if they are
 * ready to be processed.</p>
 *
 * For IV2/command logging purposes, this RPQ base class just provides the
 * original RPQ interface around a stock PriorityQueue.
 * RestrictedPriorityQueue is unchanged, but inherits from this class rather
 * than PriorityQueue.
 *
 * <p>This class manages all that state.</p>
 */
public class RPQBase extends PriorityQueue<OrderableTransaction> {
    private static final long serialVersionUID = 1L;

    public RPQBase() {
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public OrderableTransaction poll() {
        return super.poll();
    }

    /**
     * Only return transaction state objects that are ready to run.
     */
    @Override
    public OrderableTransaction peek() {
        return super.peek();
    }

    /**
     * Drop data for unknown initiators. This is the only valid add interface.
     */
    @Override
    public boolean add(OrderableTransaction txnState) {
        return super.add(txnState);
    }

    @Override
    public boolean remove(Object txnState) {
        return super.remove(txnState);
    }

    public long noteTransactionRecievedAndReturnLastSeen(long initiatorHSId, long txnId,
            boolean isHeartbeat, long lastSafeTxnIdFromInitiator)
    {
        return txnId;
    }

    public long getEarliestSeenTxnIdAcrossInitiatorsWhenEmpty() {
        return Long.MIN_VALUE;
    }

    public Long getNewestSafeTransactionForInitiator(long initiator) {
        return null;
    }

    public void gotFaultForInitiator(long initiatorId) {
    }

    public int ensureInitiatorIsKnown(long initiatorId) {
        return 0;
    }
}
