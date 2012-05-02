package org.voltdb.iv2;

import java.util.HashSet;

import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.FragmentTaskMessage;

public class Iv2MPTransactionState extends TransactionState
{
    final FragmentTaskMessage m_task;

    Iv2MPTransactionState(long txnId, TransactionInfoBaseMessage notice)
    {
        super(txnId, notice);
        m_task = (FragmentTaskMessage)notice;
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
}
