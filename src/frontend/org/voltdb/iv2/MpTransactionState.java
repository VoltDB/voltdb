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
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class MpTransactionState extends TransactionState
{
    final Iv2InitiateTaskMessage m_task;

    MpTransactionState(long txnId, TransactionInfoBaseMessage notice)
    {
        super(txnId, notice);
        m_task = (Iv2InitiateTaskMessage)notice;
    }

    @Override
    public boolean isSinglePartition()
    {
        return false;
    }

    @Override
    public boolean isCoordinator()
    {
        return true;
    }

    @Override
    public boolean isBlocked()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean hasTransactionalWork()
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public boolean doWork(boolean recovering)
    {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public StoredProcedureInvocation getInvocation()
    {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void handleSiteFaults(HashSet<Long> failedSites)
    {
        // TODO Auto-generated method stub

    }

    // Overrides needed by MpProcedureRunner
    @Override
    public void setupProcedureResume(boolean isFinal, int[] dependencies)
    {
    }

    @Override
    public void createLocalFragmentWork(FragmentTaskMessage task, boolean nonTransactional)
    {
    }

    @Override
    public void createAllParticipatingFragmentWork(FragmentTaskMessage task)
    {
    }
}