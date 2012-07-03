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

import java.util.HashSet;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.dtxn.TransactionState;

import org.voltdb.StoredProcedureInvocation;

public class ParticipantTransactionState extends TransactionState
{
    ParticipantTransactionState(long txnId, TransactionInfoBaseMessage notice)
    {
        super(txnId, null, notice);
    }

    @Override
    public boolean isSinglePartition()
    {
        return false;
    }

    @Override
    public boolean isCoordinator()
    {
        return false;
    }

    @Override
    public boolean isBlocked()
    {
        return false;
    }

    @Override
    public boolean hasTransactionalWork()
    {
        return false;
    }

    @Override
    public boolean doWork(boolean recovering)
    {
        return false;
    }

    @Override
    public StoredProcedureInvocation getInvocation()
    {
        return null;
    }

    @Override
    public void handleSiteFaults(HashSet<Long> failedSites)
    {
    }
}
