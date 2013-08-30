/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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
            Object clientData, int messageSize, long now)
    {
        return m_ci.createTransaction(connectionId,
                connectionHostname,
                adminConnection,
                invocation,
                isReadOnly,
                isSinglePartition,
                isEverySite,
                partitions,
                clientData,
                messageSize,
                now);
    }

    @Override
    public final boolean createTransaction(long connectionId,
            String connectionHostname, boolean adminConnection, long txnId,
            long uniqueId,
            StoredProcedureInvocation invocation, boolean isReadOnly,
            boolean isSinglePartition, boolean isEverySite, int[] partitions,
            Object clientData, int messageSize, long now)
    {
        return m_ci.createTransaction(connectionId,
                connectionHostname,
                adminConnection,
                txnId,
                uniqueId,
                invocation,
                isReadOnly,
                isSinglePartition,
                isEverySite,
                partitions,
                clientData,
                messageSize,
                now,
                true);
    }

    @Override
    public void sendSentinel(long txnId, int partitionId) {
        m_ci.sendSentinel(txnId, partitionId);
    }

    @Override
    public void sendEOLMessage(int partitionId)
    {
        m_ci.sendEOLMessage(partitionId);
    }
}

