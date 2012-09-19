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

package org.voltdb;

import org.voltdb.dtxn.TransactionCreator;

/**
 * Provide the Iv2 transaction routing/creation required by
 * RestoreAgent in a way consumable by the agent.
 */
public class Iv2TransactionCreator implements TransactionCreator
{
    private final ClientInterface m_ci;

    Iv2TransactionCreator(ClientInterface ci)
    {
        m_ci = ci;
    }

    @Override
    public final boolean createTransaction(long connectionId,
            String connectionHostname, boolean adminConnection,
            StoredProcedureInvocation invocation, boolean isReadOnly,
            boolean isSinglePartition, boolean isEverySite, int[] partitions,
            int numPartitions, Object clientData, int messageSize, long now,
            boolean allowMismatchedResults)
    {
        return m_ci.createTransaction(connectionId,
                connectionHostname,
                adminConnection,
                invocation,
                isReadOnly,
                isSinglePartition,
                isEverySite,
                partitions,
                numPartitions,
                clientData,
                messageSize,
                now,
                allowMismatchedResults);
    }

    @Override
    public final boolean createTransaction(long connectionId,
            String connectionHostname, boolean adminConnection, long txnId,
            long timestamp,
            StoredProcedureInvocation invocation, boolean isReadOnly,
            boolean isSinglePartition, boolean isEverySite, int[] partitions,
            int numPartitions, Object clientData, int messageSize, long now,
            boolean allowMismatchedResults)
    {
        return m_ci.createTransaction(connectionId,
                connectionHostname,
                adminConnection,
                txnId,
                timestamp,
                invocation,
                isReadOnly,
                isSinglePartition,
                isEverySite,
                partitions,
                numPartitions,
                clientData,
                messageSize,
                now,
                allowMismatchedResults,
                true);
    }

    @Override
    public void sendSentinel(long txnId, int partitionId) {
        m_ci.sendSentinel( txnId, partitionId);
    }

    @Override
    public void setSendHeartbeats(boolean val)
    {
        // Iv2 does not require heartbeating.
    }

    @Override
    public void sendHeartbeat(long txnId)
    {
        // Iv2 does not require heartbeating.
    }

    @Override
    public boolean isOnBackPressure()
    {
        // TODO Auto-generated method stub
        return false;
    }


}

