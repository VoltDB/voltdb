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

package org.voltdb.dtxn;

import java.util.ArrayList;
import java.util.Map;

import org.voltdb.CreateTransactionResult;
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
public abstract class TransactionInitiator implements TransactionCreator {

    /**
     * <p>Create a new transaction, which will result in one or more
     * <code>WorkUnit</code>'s being generated for worker partitions.</p>
     *
     * <p>Does not need to be synchronized as the scheduler will ensure
     * that only one thread is ever accepting connections at a time.</p>
     *
     * @param connectionId A unique integer identifying which TCP/IP connection
     * spawned this transaction.
     * @param invocation The data describing the work to be done.
     * @param partition The partition for routing if it is a single part
     * @param messageSize Size in bytes of the message that created this invocation
     */
    @Override
    public abstract CreateTransactionResult createTransaction(
            long connectionId,
            StoredProcedureInvocation invocation,
            boolean isReadOnly,
            boolean isSinglePartition,
            boolean isEverySite,
            int partition,
            int messageSize,
            long now);

    /**
     * <p>
     * Create a new transaction with a specified transaction ID, which will
     * result in one or more <code>WorkUnit</code>'s being generated for worker
     * partitions.
     * </p>
     *
     * <p>
     * Does not need to be synchronized as the scheduler will ensure that only
     * one thread is ever accepting connections at a time.
     * </p>
     *
     * @param connectionId
     *            A unique integer identifying which TCP/IP connection spawned
     *            this transaction.
     * @param txnId
     *            The transaction ID to assign to this initiation
     * @param invocation
     *            The data describing the work to be done.
     * @param partition
     *            The partition for routing if it is a single part
     * @param messageSize
     *            Size in bytes of the message that created this invocation
     */
    @Override
    public abstract CreateTransactionResult createTransaction(
            long connectionId,
            long txnId,
            long timestamp,
            StoredProcedureInvocation invocation,
            boolean isReadOnly,
            boolean isSinglePartition,
            boolean isEverySite,
            int partition,
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
    protected abstract void increaseBackpressure(int messageSize);
    protected abstract void reduceBackpressure(int messageSize);

    /**
     * Called to notify an initiator that it is safe to send work
     * to rejoined sites.
     * @param executorSiteIds The ids of the sites that joined.
     */
    public abstract void notifyExecutionSiteRejoin(ArrayList<Long> executorSiteIds);

    public abstract Map<Long, long[]> getOutstandingTxnStats();

    /**
     * Removes client connection statistics when the connection dies
     * @param connectionId
     */
    public abstract void removeConnectionStats(long connectionId);
}
