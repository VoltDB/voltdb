/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltdb.CatalogContext;
import org.voltdb.CatalogSpecificPlanner;
import org.voltdb.CommandLog;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.SystemProcedureCatalog.Config;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2EndOfLogMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.sysprocs.BalancePartitionsRequest;
import org.voltdb.utils.MiscUtils;

public class MpScheduler extends Scheduler
{
    static VoltLogger tmLog = new VoltLogger("TM");

    // null if running community, fallback to MpProcedureTask
    private static final Constructor<?> NpProcedureTaskConstructor = loadNpProcedureTaskClass();

    private final Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();
    private final Map<Long, DuplicateCounter> m_duplicateCounters =
        new HashMap<Long, DuplicateCounter>();

    private final List<Long> m_iv2Masters;
    private final Map<Integer, Long> m_partitionMasters;
    private final List<Long> m_buddyHSIds;
    private int m_nextBuddy = 0;
    //Generator of pre-IV2ish timestamp based unique IDs
    private final UniqueIdGenerator m_uniqueIdGenerator;
    final private MpTransactionTaskQueue m_pendingTasks;

    // the current not-needed-any-more point of the repair log.
    long m_repairLogTruncationHandle = Long.MIN_VALUE;
    // We need to lag the current MP execution point by at least two committed TXN ids
    // since that's the first point we can be sure is safely agreed on by all nodes.
    // Let the one we can't be sure about linger here.  See ENG-4211 for more.
    long m_repairLogAwaitingCommit = Long.MIN_VALUE;

    MpScheduler(int partitionId, List<Long> buddyHSIds, SiteTaskerQueue taskQueue)
    {
        super(partitionId, taskQueue);
        m_pendingTasks = new MpTransactionTaskQueue(m_tasks, getCurrentTxnId());
        m_buddyHSIds = buddyHSIds;
        m_iv2Masters = new ArrayList<Long>();
        m_partitionMasters = Maps.newHashMap();
        m_uniqueIdGenerator = new UniqueIdGenerator(partitionId, 0);
    }

    void setMpRoSitePool(MpRoSitePool sitePool)
    {
        m_pendingTasks.setMpRoSitePool(sitePool);
    }

    void updateCatalog(String diffCmds, CatalogContext context, CatalogSpecificPlanner csp)
    {
        m_pendingTasks.updateCatalog(diffCmds, context, csp);
    }

    @Override
    public void shutdown()
    {
        // cancel any in-progress transaction by creating a fragement
        // response to roll back. This function must be called with
        // the deliver lock held to be correct. The null task should
        // never run; the site thread is expected to be told to stop.
        m_pendingTasks.shutdown();
        m_pendingTasks.repair(m_nullTask, m_iv2Masters, m_partitionMasters);
    }


    @Override
    public void updateReplicas(final List<Long> replicas, final Map<Integer, Long> partitionMasters)
    {
        // Handle startup and promotion semi-gracefully
        m_iv2Masters.clear();
        m_iv2Masters.addAll(replicas);
        m_partitionMasters.clear();
        m_partitionMasters.putAll(partitionMasters);
        if (!m_isLeader) {
            return;
        }

        // Stolen from SpScheduler.  Need to update the duplicate counters associated with any EveryPartitionTasks
        // Cleanup duplicate counters and collect DONE counters
        // in this list for further processing.
        List<Long> doneCounters = new LinkedList<Long>();
        for (Entry<Long, DuplicateCounter> entry : m_duplicateCounters.entrySet()) {
            DuplicateCounter counter = entry.getValue();
            int result = counter.updateReplicas(m_iv2Masters);
            if (result == DuplicateCounter.DONE) {
                doneCounters.add(entry.getKey());
            }
        }

        // Maintain the CI invariant that responses arrive in txnid order.
        Collections.sort(doneCounters);
        for (Long key : doneCounters) {
            DuplicateCounter counter = m_duplicateCounters.remove(key);
            VoltMessage resp = counter.getLastResponse();
            if (resp != null && resp instanceof InitiateResponseMessage) {
                InitiateResponseMessage msg = (InitiateResponseMessage)resp;
                if (msg.shouldCommit()) {
                    m_repairLogTruncationHandle = m_repairLogAwaitingCommit;
                    m_repairLogAwaitingCommit = msg.getTxnId();
                }
                m_outstandingTxns.remove(msg.getTxnId());
                m_mailbox.send(counter.m_destinationId, resp);
            }
            else {
                hostLog.warn("TXN " + counter.getTxnId() + " lost all replicas and " +
                        "had no responses.  This should be impossible?");
            }
        }

        MpRepairTask repairTask = new MpRepairTask((InitiatorMailbox)m_mailbox, replicas);
        m_pendingTasks.repair(repairTask, replicas, partitionMasters);
    }

    /**
     * Sequence the message for replay if it's for DR.
     * @return true if the message can be delivered directly to the scheduler,
     * false if the message was a duplicate
     */
    @Override
    public boolean sequenceForReplay(VoltMessage message)
    {
        boolean canDeliver = true;
        long sequenceWithTxnId = Long.MIN_VALUE;

        boolean dr = ((message instanceof TransactionInfoBaseMessage &&
                ((TransactionInfoBaseMessage)message).isForDR()));

        if (dr) {
            sequenceWithTxnId = ((TransactionInfoBaseMessage)message).getOriginalTxnId();
            InitiateResponseMessage dupe = m_replaySequencer.dedupe(sequenceWithTxnId,
                    (TransactionInfoBaseMessage) message);
            if (dupe != null) {
                canDeliver = false;
                // Duplicate initiate task message, send response
                m_mailbox.send(dupe.getInitiatorHSId(), dupe);
            }
            else {
                m_replaySequencer.updateLastSeenTxnId(sequenceWithTxnId,
                        (TransactionInfoBaseMessage) message);
                canDeliver = true;
            }
        }
        return canDeliver;
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
        else if (message instanceof Iv2EndOfLogMessage) {
            handleEOLMessage();
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

        /*
         * If this is CL replay, use the txnid from the CL and use it to update the current txnid
         */
        long mpTxnId;
        //Timestamp is actually a pre-IV2ish style time based transaction id
        long timestamp = Long.MIN_VALUE;

        // Update UID if it's for replay
        if (message.isForReplay()) {
            timestamp = message.getUniqueId();
            m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(timestamp);
        } else if (message.isForDR()) {
            timestamp = message.getStoredProcedureInvocation().getOriginalUniqueId();
            // @LoadMultipartitionTable does not have a valid uid
            if (UniqueIdGenerator.getPartitionIdFromUniqueId(timestamp) == m_partitionId) {
                m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(timestamp);
            }
        }

        if (message.isForReplay()) {
            mpTxnId = message.getTxnId();
            setMaxSeenTxnId(mpTxnId);
        } else {
            TxnEgo ego = advanceTxnEgo();
            mpTxnId = ego.getTxnId();
            timestamp = m_uniqueIdGenerator.getNextUniqueId();
        }

        // Don't have an SP HANDLE at the MPI, so fill in the unused value
        Iv2Trace.logIv2InitiateTaskMessage(message, m_mailbox.getHSId(), mpTxnId, Long.MIN_VALUE);

        // Handle every-site system procedures (at the MPI)
        final Config sysprocConfig = SystemProcedureCatalog.listing.get(procedureName);
        if (sysprocConfig != null &&  sysprocConfig.getEverysite()) {
            // Send an SP initiate task to all remote sites
            final Long localId = m_mailbox.getHSId();
            Iv2InitiateTaskMessage sp = new Iv2InitiateTaskMessage(
                    localId, // make the MPI the initiator.
                    message.getCoordinatorHSId(),
                    m_repairLogTruncationHandle,
                    mpTxnId,
                    timestamp,
                    message.isReadOnly(),
                    true, // isSinglePartition
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());
            DuplicateCounter counter = new DuplicateCounter(
                    message.getInitiatorHSId(),
                    mpTxnId,
                    m_iv2Masters, message.getStoredProcedureName());
            m_duplicateCounters.put(mpTxnId, counter);
            EveryPartitionTask eptask =
                new EveryPartitionTask(m_mailbox, m_pendingTasks, sp,
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
                    timestamp,
                    message.isReadOnly(),
                    message.isSinglePartition(),
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());
        // Multi-partition initiation (at the MPI)
        MpProcedureTask task = null;
        if (isNpTxn(message) && NpProcedureTaskConstructor != null) {
            Set<Integer> involvedPartitions = getBalancePartitions(message);
            if (involvedPartitions != null) {
                HashMap<Integer, Long> involvedPartitionMasters = Maps.newHashMap(m_partitionMasters);
                involvedPartitionMasters.keySet().retainAll(involvedPartitions);

                task = instantiateNpProcedureTask(m_mailbox, procedureName,
                        m_pendingTasks, mp, involvedPartitionMasters,
                        m_buddyHSIds.get(m_nextBuddy), false);
            }

            // if cannot figure out the involved partitions, run it as an MP txn
        }

        if (task == null) {
            task = new MpProcedureTask(m_mailbox, procedureName,
                    m_pendingTasks, mp, m_iv2Masters, m_partitionMasters,
                    m_buddyHSIds.get(m_nextBuddy), false);
        }

        m_nextBuddy = (m_nextBuddy++) % m_buddyHSIds.size();
        m_outstandingTxns.put(task.m_txnState.txnId, task.m_txnState);
        m_pendingTasks.offer(task);
    }

    /**
     * Hacky way to only run @BalancePartitions as n-partition transactions for now.
     * @return true if it's an n-partition transaction
     */
    private boolean isNpTxn(Iv2InitiateTaskMessage msg)
    {
        return msg.getStoredProcedureName().startsWith("@") &&
                msg.getStoredProcedureName().equalsIgnoreCase("@BalancePartitions") &&
                (byte) msg.getParameters()[1] != 1; // clearIndex is MP, normal rebalance is NP
    }

    /**
     * Extract the two involved partitions from the @BalancePartitions request.
     */
    private Set<Integer> getBalancePartitions(Iv2InitiateTaskMessage msg)
    {
        try {
            JSONObject jsObj = new JSONObject((String) msg.getParameters()[0]);
            BalancePartitionsRequest request = new BalancePartitionsRequest(jsObj);

            return Sets.newHashSet(request.partitionPairs.get(0).srcPartition,
                    request.partitionPairs.get(0).destPartition);
        } catch (JSONException e) {
            hostLog.warn("Unable to determine partitions for @BalancePartitions", e);
            return null;
        }
    }

    @Override
    public void handleMessageRepair(List<Long> needsRepair, VoltMessage message)
    {
        if (message instanceof Iv2InitiateTaskMessage) {
            handleIv2InitiateTaskMessageRepair(needsRepair, (Iv2InitiateTaskMessage)message);
        }
        else {
            // MpInitiatorMailbox should throw RuntimeException for unhandled types before we could get here
            throw new RuntimeException("MpScheduler.handleMessageRepair() received unhandled message type." +
                    " This should be impossible");
        }
    }

    private void handleIv2InitiateTaskMessageRepair(List<Long> needsRepair, Iv2InitiateTaskMessage message)
    {
        // just reforward the Iv2InitiateTaskMessage for the txn being restarted
        // this copy may be unnecessary
        final String procedureName = message.getStoredProcedureName();
        Iv2InitiateTaskMessage mp =
            new Iv2InitiateTaskMessage(
                    message.getInitiatorHSId(),
                    message.getCoordinatorHSId(),
                    message.getTruncationHandle(),
                    message.getTxnId(),
                    message.getUniqueId(),
                    message.isReadOnly(),
                    message.isSinglePartition(),
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());
        m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(message.getUniqueId());
        // Multi-partition initiation (at the MPI)
        MpProcedureTask task = null;
        if (isNpTxn(message) && NpProcedureTaskConstructor != null) {
            Set<Integer> involvedPartitions = getBalancePartitions(message);
            if (involvedPartitions != null) {
                HashMap<Integer, Long> involvedPartitionMasters = Maps.newHashMap(m_partitionMasters);
                involvedPartitionMasters.keySet().retainAll(involvedPartitions);

                task = instantiateNpProcedureTask(m_mailbox, procedureName,
                        m_pendingTasks, mp, involvedPartitionMasters,
                        m_buddyHSIds.get(m_nextBuddy), true);
            }

            // if cannot figure out the involved partitions, run it as an MP txn
        }

        if (task == null) {
            task = new MpProcedureTask(m_mailbox, procedureName,
                    m_pendingTasks, mp, m_iv2Masters, m_partitionMasters,
                    m_buddyHSIds.get(m_nextBuddy), true);
        }

        m_nextBuddy = (m_nextBuddy++) % m_buddyHSIds.size();
        m_outstandingTxns.put(task.m_txnState.txnId, task.m_txnState);
        m_pendingTasks.offer(task);
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
                // Only advance the truncation point on committed transactions.  See ENG-4211
                if (message.shouldCommit()) {
                    m_repairLogTruncationHandle = m_repairLogAwaitingCommit;
                    m_repairLogAwaitingCommit = message.getTxnId();
                }
                m_outstandingTxns.remove(message.getTxnId());

                m_mailbox.send(counter.m_destinationId, message);
            }
            else if (result == DuplicateCounter.MISMATCH) {
                VoltDB.crashLocalVoltDB("HASH MISMATCH running every-site system procedure.", true, null);
            }
            // doing duplicate suppresion: all done.
        }
        else {
            // Only advance the truncation point on committed transactions.
            if (message.shouldCommit()) {
                m_repairLogTruncationHandle = m_repairLogAwaitingCommit;
                m_repairLogAwaitingCommit = message.getTxnId();
            }
            m_outstandingTxns.remove(message.getTxnId());
            // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
            m_mailbox.send(message.getInitiatorHSId(), message);
            // We actually completed this MP transaction.  Create a fake CompleteTransactionMessage
            // to send to our local repair log so that the fate of this transaction is never forgotten
            // even if all the masters somehow die before forwarding Complete on to their replicas.
            CompleteTransactionMessage ctm = new CompleteTransactionMessage(m_mailbox.getHSId(),
                    message.m_sourceHSId, message.getTxnId(), message.isReadOnly(), 0,
                    !message.shouldCommit(), false, false, false);
            ctm.setTruncationHandle(m_repairLogTruncationHandle);
            // dump it in the repair log
            // hacky castage
            ((MpInitiatorMailbox)m_mailbox).deliverToRepairLog(ctm);
        }
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
            hostLog.debug("MpScheduler received a FragmentResponseMessage for a null TXN ID: " + message);
        }
    }

    public void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        throw new RuntimeException("MpScheduler should never see a CompleteTransactionMessage");
    }

    /**
     * Inject a task into the transaction task queue to flush it. When it
     * executes, it will send out MPI end of log messages to all partition
     * initiators.
     */
    public void handleEOLMessage()
    {
        Iv2EndOfLogMessage msg = new Iv2EndOfLogMessage(m_partitionId);
        MPIEndOfLogTransactionState txnState = new MPIEndOfLogTransactionState(msg);
        MPIEndOfLogTask task = new MPIEndOfLogTask(m_mailbox, m_pendingTasks,
                                                   txnState, m_iv2Masters);
        m_pendingTasks.offer(task);
    }

    @Override
    public void setCommandLog(CommandLog cl) {
        // the MPI currently doesn't do command logging.  Don't have a reference to one.
    }

    @Override
    public void enableWritingIv2FaultLog() {
        // This is currently a no-op for the MPI
    }

    /**
     * Load the pro class for n-partition transactions.
     * @return null if running in community or failed to load the class
     */
    private static Constructor<?> loadNpProcedureTaskClass()
    {
        Class<?> klass = MiscUtils.loadProClass("org.voltdb.iv2.NpProcedureTask", "N-Partition", !MiscUtils.isPro());
        if (klass != null) {
            try {
                return klass.getConstructor(Mailbox.class, String.class, TransactionTaskQueue.class,
                        Iv2InitiateTaskMessage.class, Map.class, long.class, boolean.class);
            } catch (NoSuchMethodException e) {
                hostLog.error("Unabled to get the constructor for pro class NpProcedureTask", e);
                return null;
            }
        } else {
            return null;
        }
    }

    private static MpProcedureTask instantiateNpProcedureTask(Object...params)
    {
        if (NpProcedureTaskConstructor != null) {
            try {
                return (MpProcedureTask) NpProcedureTaskConstructor.newInstance(params);
            } catch (Exception e) {
                tmLog.error("Unable to instantiate NpProcedureTask", e);
            }
        }
        return null;
    }
}
