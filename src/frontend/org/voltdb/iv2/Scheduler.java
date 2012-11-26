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

import java.io.IOException;
import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StarvationTracker;
import org.voltdb.VoltDB;
import org.voltdb.rejoin.TaskLog;

/**
 * Scheduler's rough current responsibility is to take appropriate local action
 * based on a received message.
 *
 * For new work (InitiateTask, FragmentTask, CompleteTransactionTask):
 *   - Create new TransactionStates for previously unseen transactions
 *   - Look up TransactionStates for in-progress multi-part transactions
 *   - Create appropriate TransactionTasks and offer them to the Site (via
 *   TransactionTaskQueue)
 * For responses (InitiateResponse, FragmentResponse):
 *   - Perform response de-duping
 *   - Offer responses to the corresponding TransactionState for MP dependency tracking
 *
 * Currently, Single- and Multi-partition schedulers extend this class and
 * provide the specific message handling necessary for the different
 * transaction types.
 * IZZY: This class maybe folds into InitiatorMessageHandler nicely; let's see
 * how it looks once partition replicas are implemented.
 */
abstract public class Scheduler implements InitiatorMessageHandler
{
    protected VoltLogger hostLog = new VoltLogger("HOST");

    // A null task that unblocks the site task queue, used during shutdown
    protected static final SiteTasker m_nullTask = new SiteTasker() {
        @Override
        public void run(SiteProcedureConnection siteConnection)
        {
        }

        @Override
        public void runForRejoin(SiteProcedureConnection siteConnection, TaskLog taskLog)
        throws IOException
        {
        }
    };

    // The queue which the Site's runloop is going to poll for new work.  This
    // is fronted here by the TransactionTaskQueue and should not be directly
    // offered work.
    // IZZY: We should refactor this to be inviolable in the future.
    final protected SiteTaskerQueue m_tasks;
    protected Mailbox m_mailbox;
    final protected TransactionTaskQueue m_pendingTasks;
    protected boolean m_isLeader = false;
    private TxnEgo m_txnEgo;
    final protected int m_partitionId;
    protected Object m_lock;

    Scheduler(int partitionId, SiteTaskerQueue taskQueue)
    {
        m_tasks = taskQueue;
        m_pendingTasks = new TransactionTaskQueue(m_tasks);
        m_partitionId = partitionId;
        m_txnEgo = TxnEgo.makeZero(partitionId);
    }

    public void setMaxSeenTxnId(long maxSeenTxnId)
    {
        final TxnEgo ego = new TxnEgo(maxSeenTxnId);
        if (m_txnEgo.getPartitionId() != ego.getPartitionId()) {
            VoltDB.crashLocalVoltDB(
                    "Received a transaction id at partition " + m_txnEgo.getPartitionId() +
                    " for partition " + ego.getPartitionId() + ". The partition ids should match.", true, null);
        }
        m_txnEgo = ego;
    }

    final protected TxnEgo advanceTxnEgo()
    {
        m_txnEgo = m_txnEgo.makeNext();
        return m_txnEgo;
    }

    final protected long getCurrentTxnId()
    {
        return m_txnEgo.getTxnId();
    }

    @Override
    public void setMailbox(Mailbox mailbox)
    {
        m_mailbox = mailbox;
    }

    public void setLeaderState(boolean isLeader)
    {
        m_isLeader = isLeader;
    }

    public SiteTaskerQueue getQueue()
    {
        return m_tasks;
    }

    public void setStarvationTracker(StarvationTracker tracker) {
        m_tasks.setStarvationTracker(tracker);
    }

    public void setLock(Object o) {
        m_lock = o;
    }

    abstract public void shutdown();

    @Override
    abstract public void updateReplicas(List<Long> replicas);

    @Override
    abstract public void deliver(VoltMessage message);

    abstract public void enableWritingIv2FaultLog();
}
