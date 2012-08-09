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

import org.voltdb.StoredProcedureInvocation;

/**
 * Provide an interface that both Iv2 and simple dtxn can implement.
 */
public interface TransactionCreator
{
    // create a new transaction.
    public boolean createTransaction(
            long connectionId,
            final String connectionHostname,
            boolean adminConnection,
            StoredProcedureInvocation invocation,
            boolean isReadOnly,
            boolean isSinglePartition,
            boolean isEverySite,
            int partitions[],
            int numPartitions,
            Object clientData,
            int messageSize,
            long now,
            boolean allowMismatchedResults);

    // Create a transaction using the provided txnId.
    public boolean createTransaction(
            long connectionId,
            final String connectionHostname,
            boolean adminConnection,
            long txnId,
            StoredProcedureInvocation invocation,
            boolean isReadOnly,
            boolean isSinglePartition,
            boolean isEverySite,
            int partitions[],
            int numPartitions,
            Object clientData,
            int messageSize,
            long now,
            boolean allowMismatchedResults);

    public void setSendHeartbeats(boolean val);
    public void sendHeartbeat(long txnId);

    /**
     * Whether or not the initiator is on back pressure.
     * @return
     */
    public abstract boolean isOnBackPressure();
}
