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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.Pair;
import org.voltdb.CommandLog;
import org.voltdb.ProcedureRunner;
import org.voltdb.SiteProcedureConnection;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;

public class MpScheduler extends Scheduler
{
    VoltLogger tmLog = new VoltLogger("TM");

    private final Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();
    private final Map<Long, DuplicateCounter> m_duplicateCounters =
        new HashMap<Long, DuplicateCounter>();

    private final List<Long> m_iv2Masters;
    private final long m_buddyHSId;

    // the current not-needed-any-more point of the repair log.
    long m_repairLogTruncationHandle = Long.MIN_VALUE;
    private CommandLog m_cl;

    MpScheduler(int partitionId, long buddyHSId, SiteTaskerQueue taskQueue)
    {
        super(partitionId, taskQueue);
        m_buddyHSId = buddyHSId;
        m_iv2Masters = new ArrayList<Long>();
    }

    @Override
    public void shutdown()
    {
        // cancel any in-progress transaction by creating a fragement
        // response to roll back. This function must be called with
        // the deliver lock held to be correct. The null task should
        // never run; the site thread is expected to be told to stop.
        SiteTasker nullTask = new SiteTasker() {
            @Override
            public void run(SiteProcedureConnection siteConnection) {
            }

            @Override
            public void runForRejoin(SiteProcedureConnection siteConnection) {
            }

            @Override
            public int priority() {
                return 0;
            }
        };
        m_pendingTasks.repair(nullTask);
    }


    @Override
    public void updateReplicas(final List<Long> replicas)
    {
        // Handle startup and promotion semi-gracefully
        if (!m_isLeader) {
            m_iv2Masters.clear();
            m_iv2Masters.addAll(replicas);
            return;
        }

        final List<Long> replicaCopy = new ArrayList<Long>(replicas);

        // Must run the repair while pausing the site task queue;
        // Otherwise, a new MP might immediately be blocked in a
        // confused world of semi-repair. So just do the repair
        // work on the site thread....
        SiteTasker repairTask = new SiteTasker() {
            @Override
            public void run(SiteProcedureConnection connection) {
                try {
                    String whoami = "MP leader repair " +
                        CoreUtils.hsIdToString(m_mailbox.getHSId()) + " ";
                    InitiatorMailbox initiatorMailbox =
                        (InitiatorMailbox)m_mailbox;
                    RepairAlgo algo = new MpPromoteAlgo(replicas,
                            initiatorMailbox, whoami);
                    initiatorMailbox.setRepairAlgo(algo);
                    Pair<Boolean, Long> result = algo.start().get();
                    boolean success = result.getFirst();
                    if (success) {
                        tmLog.info(whoami + "finished repair.");
                        // We need to update the replicas with the InitiatorMailbox's
                        // deliver() lock held.  Since we're not calling
                        // InitiatorMailbox.updateReplicas() here, grab the lock manually
                        synchronized (initiatorMailbox) {
                            m_iv2Masters.clear();
                            m_iv2Masters.addAll(replicaCopy);
                        }
                    }
                    else {
                        tmLog.info(whoami + "interrupted during repair.  Retrying.");
                    }
                }
                catch (InterruptedException ie) {}
                catch (Exception e) {
                    VoltDB.crashLocalVoltDB("Terminally failed MPI repair.", true, e);
                }
            }

            @Override
            public void runForRejoin(SiteProcedureConnection siteConnection)
            {
                throw new RuntimeException("Rejoin while repairing the MPI should be impossible.");
            }

            @Override
            public int priority() {
                return 0;
            }
        };
        m_pendingTasks.repair(repairTask);
    }

    @Override
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2InitiateTaskMessage) {
            handleIv2InitiateTaskMessage((Iv2InitiateTaskMessage)message);
        }
        else if (message instanceof InitiateResponseMessage) {
            handleInitiateResponseMessage((InitiateResponseMessage)message);
        }
        else if (message instanceof FragmentResponseMessage) {
            handleFragmentResponseMessage((FragmentResponseMessage)message);
        }
        else {
            throw new RuntimeException("UNKNOWN MESSAGE TYPE, BOOM!");
        }
    }

    // MpScheduler expects to see initiations for multipartition procedures and
    // system procedures which are "every-partition", meaning that they run as
    // single-partition procedures at every partition, and the results are
    // aggregated/deduped here at the MPI.
    public void handleIv2InitiateTaskMessage(Iv2InitiateTaskMessage message)
    {
        final String procedureName = message.getStoredProcedureName();
        final ProcedureRunner runner = m_loadedProcs.getProcByName(procedureName);

        /*
         * If this is CL replay, use the txnid from the CL and use it to update the current txnid
         */
        long mpTxnId;
        if (message.isForReplay()) {
            mpTxnId = message.getTxnId();
            setMaxSeenTxnId(mpTxnId);
        } else {
            mpTxnId = advanceTxnEgo();;
        }

        // advanceTxnEgo();
        // final long mpTxnId = currentTxnEgoSequence();

        // Don't have an SP HANDLE at the MPI, so fill in the unused value
        Iv2Trace.logIv2InitiateTaskMessage(message, m_mailbox.getHSId(), mpTxnId, Long.MIN_VALUE);
        // Handle every-site system procedures (at the MPI)
        if (runner.isEverySite()) {
            // Send an SP initiate task to all remote sites
            final Long localId = m_mailbox.getHSId();
            Iv2InitiateTaskMessage sp = new Iv2InitiateTaskMessage(
                    localId, // make the MPI the initiator.
                    message.getCoordinatorHSId(),
                    m_repairLogTruncationHandle,
                    mpTxnId,
                    message.isReadOnly(),
                    true, // isSinglePartition
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());
            DuplicateCounter counter = new DuplicateCounter(
                    message.getInitiatorHSId(),
                    mpTxnId,
                    m_iv2Masters);
            m_duplicateCounters.put(mpTxnId, counter);
            EveryPartitionTask eptask =
                new EveryPartitionTask(m_mailbox, mpTxnId, m_pendingTasks, sp,
                        m_iv2Masters);
            m_pendingTasks.offer(eptask);
            return;
        }
        // Create a copy so we can overwrite the txnID so the InitiateResponse will be
        // correctly tracked.
        Iv2InitiateTaskMessage mp =
            new Iv2InitiateTaskMessage(
                    message.getInitiatorHSId(),
                    message.getCoordinatorHSId(),
                    m_repairLogTruncationHandle,
                    mpTxnId,
                    message.isReadOnly(),
                    message.isSinglePartition(),
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());
        // Multi-partition initiation (at the MPI)
        final MpProcedureTask task =
            new MpProcedureTask(m_mailbox, m_loadedProcs.getProcByName(procedureName),
                    mpTxnId, m_pendingTasks, mp, m_iv2Masters, m_buddyHSId);
        m_outstandingTxns.put(task.m_txn.txnId, task.m_txn);
        m_pendingTasks.offer(task);
    }

    @Override
    public void handleIv2InitiateTaskMessageRepair(List<Long> needsRepair, Iv2InitiateTaskMessage message) {
        // MP initiate tasks are never repaired.
        throw new RuntimeException("Impossible code path through MPI repair.");
    }

    // The MpScheduler will see InitiateResponseMessages from the Partition masters when
    // performing an every-partition system procedure.  A consequence of this deduping
    // is that the MpScheduler will also need to forward the final InitiateResponseMessage
    // for a normal multipartition procedure back to the client interface since it must
    // see all of these messages and control their transmission.
    public void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        DuplicateCounter counter = m_duplicateCounters.get(message.getTxnId());
        if (counter != null) {
            int result = counter.offer(message);
            if (result == DuplicateCounter.DONE) {
                m_duplicateCounters.remove(message.getTxnId());
                m_repairLogTruncationHandle = message.getTxnId();
                m_outstandingTxns.remove(message.getTxnId());
                m_mailbox.send(counter.m_destinationId, message);
            }
            else if (result == DuplicateCounter.MISMATCH) {
                VoltDB.crashLocalVoltDB("HASH MISMATCH running every-site system procedure.", true, null);
            }
            // doing duplicate suppresion: all done.
            return;
        }

        // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
        m_repairLogTruncationHandle = message.getTxnId();
        m_outstandingTxns.remove(message.getTxnId());
        m_mailbox.send(message.getInitiatorHSId(), message);
    }

    public void handleFragmentTaskMessage(FragmentTaskMessage message,
                                          Map<Integer, List<VoltTable>> inputDeps)
    {
        throw new RuntimeException("MpScheduler should never see a FragmentTaskMessage");
    }

    // MpScheduler will receive FragmentResponses from the partition masters, and needs
    // to offer them to the corresponding TransactionState so that the TransactionTask in
    // the runloop which is awaiting these responses can do dependency tracking and eventually
    // unblock.
    public void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        TransactionState txn = m_outstandingTxns.get(message.getTxnId());
        // We could already have received the CompleteTransactionMessage from
        // the local site and the transaction is dead, despite FragmentResponses
        // in flight from remote sites.  Drop those on the floor.
        // IZZY: After implementing BorrowTasks, I'm not sure that the above sequence
        // can actually happen any longer, but leaving this and logging it for now.
        // RTB: Didn't we decide early rollback can do this legitimately.
        if (txn != null) {
            ((MpTransactionState)txn).offerReceivedFragmentResponse(message);
        }
        else {
            hostLog.info("MpScheduler received a FragmentResponseMessage for a null TXN ID: " + message);
        }
    }

    public void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        throw new RuntimeException("MpScheduler should never see a CompleteTransactionMessage");
    }

    @Override
    public void setCommandLog(CommandLog cl) {
        m_cl = cl;
    }
}
