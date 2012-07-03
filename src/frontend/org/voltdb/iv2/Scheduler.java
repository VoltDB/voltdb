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

import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.LoadedProcedureSet;

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
    // The queue which the Site's runloop is going to poll for new work.  This
    // is fronted here by the TransactionTaskQueue and should not be directly
    // offered work.
    // IZZY: We should refactor this to be inviolable in the future.
    final protected SiteTaskerQueue m_tasks;
    protected LoadedProcedureSet m_loadedProcs;
    protected Mailbox m_mailbox;
    final protected TransactionTaskQueue m_pendingTasks;
    protected boolean m_isLeader = false;

    Scheduler(SiteTaskerQueue taskQueue)
    {
        m_tasks = taskQueue;
        m_pendingTasks = new TransactionTaskQueue(m_tasks);
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

    void setProcedureSet(LoadedProcedureSet loadedProcs)
    {
        m_loadedProcs = loadedProcs;
    }

    public SiteTaskerQueue getQueue()
    {
        return m_tasks;
    }

    @Override
    abstract public void updateReplicas(List<Long> replicas);

    @Override
    abstract public void deliver(VoltMessage message);
}
