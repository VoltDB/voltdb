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

package org.voltdb;

import org.voltcore.network.Connection;
import org.voltdb.AuthSystem.AuthUser;
import org.voltdb.InvocationDispatcher.OverrideCheck;
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
    public final CreateTransactionResult createTransaction(long connectionId,
            StoredProcedureInvocation invocation, boolean isReadOnly,
            boolean isSinglePartition, boolean isEverySite, int partition, int messageSize, long nowNanos)
    {
        return m_ci.getDispatcher().createTransaction(connectionId,
                invocation,
                isReadOnly,
                isSinglePartition,
                isEverySite,
                new int[] { partition },
                messageSize,
                nowNanos);
    }

    @Override
    public final CreateTransactionResult createTransaction(long connectionId,
            long txnId,
            long uniqueId,
            StoredProcedureInvocation invocation, boolean isReadOnly,
            boolean isSinglePartition, boolean isEverySite, int partition, int messageSize, long nowNanos)
    {
        return m_ci.getDispatcher().createTransaction(connectionId,
                txnId,
                uniqueId,
                invocation,
                isReadOnly,
                isSinglePartition,
                isEverySite,
                new int[] { partition },
                messageSize,
                nowNanos,
                true);
    }

    @Override
    public ClientResponseImpl dispatch(StoredProcedureInvocation invocation,
            Connection connection, boolean isAdmin, OverrideCheck bypass) {

        final InvocationClientHandler handler = new InvocationClientHandler() {

            final boolean adminFlag = isAdmin;
            final long connectionId = connection.connectionId();

            @Override
            public boolean isAdmin() {
                return adminFlag;
            }

            @Override
            public long connectionId() {
                return connectionId;
            }
        };

        AuthUser admin = m_ci.getCatalogContext().authSystem.getInternalAdminUser();
        return m_ci.getDispatcher().dispatch(invocation, handler, connection, admin, bypass, false);
    }

    @Override
    public void sendSentinel(long uniqueId, int partitionId) {
        m_ci.sendSentinel(uniqueId, partitionId);
    }

    @Override
    public void sendEOLMessage(int partitionId)
    {
        m_ci.sendEOLMessage(partitionId);
    }

    @Override
    public void bindAdapter(Connection adapter) {
        m_ci.bindAdapter(adapter, null);
    }
}

