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

package org.voltdb.dtxn;

import org.voltdb.StoredProcedureInvocation;

/**
 * <p>A <code>TransactionInitiator</code> is the center of the distributed
 * transaction system on each host. It is the only way to create new
 * transactions and it is the only way to internally access the results
 * of those transactions.</p>
 *
 * <p><code>TransactionInitiator</code> is an abstract base class which is an
 * interface to the pluggable transaction system. Different transaction
 * systems with provide their own subclasses.</p>
 *
 */
public abstract class TransactionInitiator {

    /**
     * <p>Create a new transaction, which will result in one or more
     * <code>WorkUnit</code>'s being generated for worker partitions.</p>
     *
     * <p>Does not need to be synchronized as the scheduler will ensure
     * that only one thread is ever accepting connections at a time.</p>
     *
     * @param connectionId A unique integer identifying which TCP/IP connection
     * spawned this transaction.
     * @param connectionHostname hostname associated with this connection
     * @param invocation The data describing the work to be done.
     * @param partitions The partitions (from the catalog) involved in this
     * transaction (Errs on the side of too many).
     * @param numPartitions Number of relevant partitions in the array (allows
     * for the array to be oversized).
     * @param clientData Client data returned with the completed transaction
     * @param messageSize Size in bytes of the message that created this invocation
     */
    public abstract void createTransaction(
            long connectionId,
            final String connectionHostname,
            StoredProcedureInvocation invocation,
            boolean isReadOnly,
            boolean isSinglePartition,
            boolean isEverySite,
            int partitions[],
            int numPartitions,
            Object clientData,
            int messageSize,
            long now);

    /**
     * This method should be called every X ms or so, where X is probably
     * about 5, but that's somewhat fungible. The goal is to let the initiator
     * evaluate whether it's been too long since it's had contact with any
     * execution sites, and to send a heartbeat message if needed.
     */
    public abstract long tick();

    /**
     * @return The id of the most recently used transaction id.
     * @deprecated
     */
    @Deprecated
    public abstract long getMostRecentTxnId();

    /**
     * Increase or reduce the amount of backpressure from this initiator.
     *
     * @param messageSize
     */
    abstract void increaseBackpressure(int messageSize);
    abstract void reduceBackpressure(int messageSize);
}
