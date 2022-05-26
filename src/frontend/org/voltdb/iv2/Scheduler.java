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

package org.voltdb.iv2;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.LoadedProcedureSet;
import org.voltdb.QueueDepthTracker;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.StarvationTracker;
import org.voltdb.VoltDB;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.iv2.SpScheduler.DurableUniqueIdListener;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
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
    protected static final VoltLogger hostLog = new VoltLogger("HOST");

    // A null task that unblocks the site task queue, used during shutdown
    static final SiteTasker m_nullTask = new SiteTasker() {
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
    protected boolean m_isLeader = false;
    private TxnEgo m_txnEgo;
    final protected int m_partitionId;
    protected LoadedProcedureSet m_procSet;
    protected boolean m_isLowestSiteId;

    // helper class to put command log work in order
    protected final ReplaySequencer m_replaySequencer = new ReplaySequencer();

    /*
     * This lock is extremely dangerous to use without known the pattern.
     * It is the intrinsic lock on the InitiatorMailbox. For an SpInitiator
     * this is a real thing, but for the MpInitiator the intrinsic lock isn't used
     * because it uses MpInitiatorMailbox (as subclass of InitiatorMailbox)
     * which uses a dedicated thread instead of locking.
     *
     * In the MpInitiator case locking on this will not provide any isolation because
     * the InitiatorMailbox thread doesn't use the lock.
     *
     * Right now this lock happens to only be used to gain isolation for
     * command logging while submitting durable tasks. Only SpInitiators log
     * so this is fine.
     *
     * Think twice and ask around before using it for anything else.
     * You should probably be going through InitiatorMailbox.deliver which automatically
     * handles the transition between locking vs. submitting to the MpInitiatorMailbox task queue.
     */
    protected Object m_lock;
    // Lock shared by all schedulers to de-conflict the first dump message to log a stacktrace of all site threads
    private static final Object s_threadDumpLock = new Object();
    private static long s_txnIdForSiteThreadDump = 0;

    protected boolean m_isEnterpriseLicense = false;

    Scheduler(int partitionId, SiteTaskerQueue taskQueue)
    {
        m_tasks = taskQueue;
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
        if (m_txnEgo.getTxnId() < ego.getTxnId()) {
            m_txnEgo = ego;
        }
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

    public boolean isLeader() {
        return m_isLeader;
    }

    public SiteTaskerQueue getQueue()
    {
        return m_tasks;
    }

    public void setStarvationTracker(StarvationTracker tracker) {
        m_tasks.setStarvationTracker(tracker);
    }

    public QueueDepthTracker setupQueueDepthTracker(long siteId) {
        return m_tasks.setupQueueDepthTracker(siteId);
    }

    public void setLock(Object o) {
        m_lock = o;
    }

    public void configureDurableUniqueIdListener(final DurableUniqueIdListener listener, final boolean install) {
        // Durability Listeners should never be assigned to the MP Scheduler
        assert false;
    }

    public void setProcedureSet(LoadedProcedureSet procSet) {
        m_procSet = procSet;
    }

    public void setIsLowestSiteId(Boolean isLowestSiteId) {
        m_isLowestSiteId = isLowestSiteId;
    }

    /**
     * Update last seen uniqueIds in the replay sequencer. This is used on MPI repair.
     * @param message
     */
    public void updateLastSeenUniqueIds(VoltMessage message)
    {
        long sequenceWithUniqueId = Long.MIN_VALUE;

        boolean commandLog = (message instanceof TransactionInfoBaseMessage &&
                (((TransactionInfoBaseMessage)message).isForReplay()));

        boolean sentinel = message instanceof MultiPartitionParticipantMessage;

        // if replay
        if (commandLog || sentinel) {
            sequenceWithUniqueId = ((TransactionInfoBaseMessage)message).getUniqueId();
            // Update last seen and last polled txnId for replicas
            m_replaySequencer.updateLastSeenUniqueId(sequenceWithUniqueId,
                    (TransactionInfoBaseMessage) message);
            m_replaySequencer.updateLastPolledUniqueId(sequenceWithUniqueId,
                    (TransactionInfoBaseMessage) message);
        }
    }

    // Dumps the content of the scheduler for debugging
    public void dump() {}

    abstract public void shutdown();

    @Override
    abstract public long[] updateReplicas(List<Long> replicas, Map<Integer, Long> partitionMasters,
            TransactionState snapshotTransactionState);

    @Override
    abstract public void deliver(VoltMessage message);

    abstract public void enableWritingIv2FaultLog();

    abstract public boolean sequenceForReplay(VoltMessage m);

    //flush out read only transactions upon host failure
    public void cleanupTransactionBacklogOnRepair() {}

    //flush out transactions for the site to be removed
    public void cleanupTransactionBacklogs() {}

    protected static void generateSiteThreadDump(StringBuilder threadDumps) {
        ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        ThreadInfo[] threadInfos = threadMXBean.dumpAllThreads(true, true);
        for (ThreadInfo t : threadInfos) {
            if (t.getThreadName().startsWith("SP") || t.getThreadName().startsWith("MP Site") || t.getThreadName().startsWith("RO MP Site")) {
                threadDumps.append(t);
            }
        }
    }

    protected static void dumpStackTraceOnFirstSiteThread(DumpMessage message, StringBuilder threadDumps) {
        synchronized(s_threadDumpLock) {
            if (message.getTxnId() > s_txnIdForSiteThreadDump) {
                s_txnIdForSiteThreadDump = message.getTxnId();
            } else {
                return;
            }
        }
        threadDumps.append("\nSITE THREAD DUMP FROM TXNID:" + TxnEgo.txnIdToString(message.getTxnId()) +"\n");
        generateSiteThreadDump(threadDumps);
        threadDumps.append("\nEND OF SITE THREAD DUMP FROM TXNID:" + TxnEgo.txnIdToString(message.getTxnId()));
    }
}
