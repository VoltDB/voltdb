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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.CommandLog;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.VoltZK;
import org.voltdb.client.Priority;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.TransactionRestartException;
import org.voltdb.iv2.DuplicateCounter.HashResult;
import org.voltdb.iv2.MpTerm.RepairType;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.DummyTransactionTaskMessage;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2EndOfLogMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.sysprocs.BalancePartitionsRequest;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.ProClass;
import org.voltdb.utils.VoltTrace;

import com.google_voltpatches.common.base.Supplier;
import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.collect.Maps;
import com.google_voltpatches.common.collect.Sets;

public class MpScheduler extends Scheduler
{
    static VoltLogger tmLog = new VoltLogger("TM");
    static final VoltLogger repairLogger = new VoltLogger("REPAIR");

    // null if running community, fallback to MpProcedureTask
    private static final ProClass<MpProcedureTask> NP_PROCEDURE_CLASS = loadNpProcedureTaskClass();

    private final Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();
    private final Map<Long, DuplicateCounter> m_duplicateCounters =
        new HashMap<Long, DuplicateCounter>();

    private final List<Long> m_iv2Masters;
    private final Map<Integer, Long> m_partitionMasters;
    // Todo: update buddyHSIds
    private ImmutableList<Long> m_buddyHSIds;
    // Leader migrated from one site to another
    private final Map<Long, Long> m_leaderMigrationMap;

    private volatile int m_nextBuddy = 0;
    //Generator of pre-IV2ish timestamp based unique IDs
    private final UniqueIdGenerator m_uniqueIdGenerator;
    final private MpTransactionTaskQueue m_pendingTasks;
    private final int m_leaderId;

    // the current not-needed-any-more point of the repair log.
    long m_repairLogTruncationHandle =TransactionInfoBaseMessage.UNUSED_TRUNC_HANDLE;
    // We need to lag the current MP execution point by at least two committed TXN ids
    // since that's the first point we can be sure is safely agreed on by all nodes.
    // Let the one we can't be sure about linger here.  See ENG-4211 for more.
    long m_repairLogAwaitingTruncate = TransactionInfoBaseMessage.UNUSED_TRUNC_HANDLE;
    private Object m_lock = new Object();
    MpScheduler(int partitionId, List<Long> buddyHSIds, SiteTaskerQueue taskQueue, int leaderId)
    {
        super(partitionId, taskQueue);
        m_pendingTasks = new MpTransactionTaskQueue(m_tasks);
        m_buddyHSIds = ImmutableList.<Long>builder().addAll(buddyHSIds).build();
        m_iv2Masters = new ArrayList<Long>();
        m_partitionMasters = Maps.newHashMap();
        m_uniqueIdGenerator = new UniqueIdGenerator(partitionId, 0);
        m_leaderId = leaderId;
        m_leaderMigrationMap = Maps.newHashMap();
    }

    private final Supplier<Long> m_buddySupplier = new Supplier<Long>() {
        @Override
        public Long get() {
            //MP reads can be concurrently executed
            synchronized(m_lock) {
                Long buddy = m_buddyHSIds.get(m_nextBuddy);
                m_nextBuddy = (m_nextBuddy + 1) % m_buddyHSIds.size();
                return buddy;
            }
        }
    };

    // reset buddy Hsid list if local sites decommissioned
    public void updateBuddyHSIds(List<Long> buddyHSIds) {
        synchronized(m_lock) {
            m_nextBuddy = 0;
            m_buddyHSIds = ImmutableList.<Long>builder().addAll(buddyHSIds).build();
            // We need at least one available sites on the MPI host
            assert(m_buddyHSIds.size() > 0);
        }
    }

    void setMpRoSitePool(MpRoSitePool sitePool)
    {
        m_pendingTasks.setMpRoSitePool(sitePool);
    }

    void updateCatalog(String diffCmds, CatalogContext context)
    {
        m_pendingTasks.updateCatalog(diffCmds, context);
    }

    void updateSettings(CatalogContext context)
    {
        m_pendingTasks.updateSettings(context);
    }

    @Override
    public void shutdown()
    {
        // cancel any in-progress transaction by creating a fragement
        // response to roll back. This function must be called with
        // the deliver lock held to be correct. The null task should
        // never run; the site thread is expected to be told to stop.
        m_pendingTasks.shutdown();
        m_pendingTasks.repair(m_nullTask, m_iv2Masters, m_partitionMasters, RepairType.NORMAL);
        m_tasks.offer(m_nullTask);
    }

    @Override
    public long[] updateReplicas(final List<Long> replicas, final Map<Integer, Long> partitionMasters,
            TransactionState snapshotTransactionState)
    {
        return updateReplicas(replicas, partitionMasters, RepairType.NORMAL);
    }

    public long[] updateReplicas(final List<Long> replicas, final Map<Integer, Long> partitionMasters,
            RepairType repairType)
    {
        applyLeaderMigration(replicas, repairType.isMigrate());

        // Handle startup and promotion semi-gracefully
        m_iv2Masters.clear();
        m_iv2Masters.addAll(replicas);
        m_partitionMasters.clear();
        m_partitionMasters.putAll(partitionMasters);

        if (!m_isLeader) {
            return new long[0];
        }

        // Stolen from SpScheduler.  Need to update the duplicate counters associated with any EveryPartitionTasks
        // Cleanup duplicate counters and collect DONE counters
        // in this list for further processing.

        // Do not update DuplicateCounter upon leader migration
        if (!repairType.isMigrate()) {
            List<Long> doneCounters = new LinkedList<Long>();
            for (Entry<Long, DuplicateCounter> entry : m_duplicateCounters.entrySet()) {
                DuplicateCounter counter = entry.getValue();
                HashResult result = counter.updateReplicas(m_iv2Masters);
                if (result.isDone()) {
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
                    advanceRepairTruncationHandle(msg);
                    m_outstandingTxns.remove(msg.getTxnId());
                    m_mailbox.send(counter.m_destinationId, resp);
                }
                else {
                    hostLog.warn("TXN " + counter.getTxnId() + " lost all replicas and " +
                            "had no responses.  This should be impossible?");
                }
            }
        }
        // This is a non MPI Promotion (but SPI Promotion) path for repairing outstanding MP Txns
        final InitiatorMailbox mailbox = (InitiatorMailbox)m_mailbox;
        MpRepairTask repairTask = new MpRepairTask(mailbox, replicas, repairType);
        m_pendingTasks.repair(repairTask, replicas, partitionMasters, repairType);

        // At this point, all the repairs are completed. Remove the mp repair blocker if there exists
        if (mailbox.m_messenger != null) {
            Set<Integer> liveHostIds = mailbox.m_messenger.getLiveHostIds();
            if (m_iv2Masters.stream().map(CoreUtils::getHostIdFromHSId).allMatch(liveHostIds::contains)) {
                VoltZK.removeActionBlocker(mailbox.m_messenger.getZK(), VoltZK.mpRepairInProgress, tmLog);
            }
        }
        return new long[0];
    }

    private void applyLeaderMigration(final List<Long> updatedReplicas, boolean balanceSPI) {

        if (!balanceSPI || !m_isLeader) {
            m_leaderMigrationMap.clear();
            return;
        }
         // Find the old leader
        Set<Long> previousLeaders = Sets.newHashSet();
        previousLeaders.addAll(m_iv2Masters);
        previousLeaders.removeAll(updatedReplicas);
        // Update is triggered by manufactured callback
        if (previousLeaders.isEmpty()) {
            return;
        }
         // Find the new leader
        Set<Long> currentLeaders = Sets.newHashSet();
        currentLeaders.addAll(updatedReplicas);
        currentLeaders.removeAll(m_iv2Masters);
         // Leader migration moves partition leader from a host to another, one at a time
        assert(previousLeaders.size() == 1 && currentLeaders.size() == 1);
        m_leaderMigrationMap.put(previousLeaders.iterator().next(), currentLeaders.iterator().next());
    }

    /**
     * Sequence the message for replay if it's for DR.
     * @return true if the message can be delivered directly to the scheduler,
     * false if the message was a duplicate
     */
    @Override
    public boolean sequenceForReplay(VoltMessage message)
    {
        return true;
    }

    @Override
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2InitiateTaskMessage) {
            if (tmLog.isDebugEnabled()) {
                // Protect against race in string conversion of VoltTable parameter on deliver and site threads
                StringBuilder sb = new StringBuilder("DELIVER: ");
                ((Iv2InitiateTaskMessage)message).toShortString(sb);
                tmLog.debug(sb.toString());
            }
            handleIv2InitiateTaskMessage((Iv2InitiateTaskMessage)message);
        }
        else {
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("DELIVER: " + message.toString());
            }
            if (message instanceof InitiateResponseMessage) {
                handleInitiateResponseMessage((InitiateResponseMessage)message);
            }
            else if (message instanceof FragmentResponseMessage) {
                handleFragmentResponseMessage((FragmentResponseMessage)message);
            }
            else if (message instanceof Iv2EndOfLogMessage) {
                handleEOLMessage();
            }
            else if (message instanceof DummyTransactionTaskMessage) {
                // leave empty to ignore it on purpose
            }
            else if (message instanceof DumpMessage) {
                handleDumpMessage((DumpMessage)message);
            }
            else {
                throw new RuntimeException("UNKNOWN MESSAGE TYPE, BOOM!");
            }
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
         * Reset invocation priority to system (highest) priority: this initiate message has been
         * taken from the queue and will be assigned transaction numbers. The original priority
         * should be forgotten when this message is copied to downstream destinations: repair log,
         * command log, replicas...
         *
         * Also check for a timed-out request, and unconditionally remove the timeout indicators
         * from the procedure invocation. We must make the check before we reset the priority,
         * since SYSTEM_PRIORITY tasks are considered to never time out.
         *
         * There is an abundance of caution here:
         * - Only external clients set the request-timeout indicator
         * - We don't check timeouts for replay requests
         * - No timeout is ever reported if priority is SYSTEM_PRIORITY
         * - We immediately clear the request-timeout indicator here
         * - Any every-site duplicate requests or repair logs we create will
         *   have SYSTEM_PRIORITY and not have request-timeout indication
         */
        boolean hasTimedOut = false;
        StoredProcedureInvocation spi = message.getStoredProcedureInvocation();
        if (spi != null) {
            if (spi.hasRequestTimeout()) {
                if (!message.isForReplay()) {
                    hasTimedOut = spi.requestHasTimedOut();
                }
                spi.clearRequestTimeout();
            }
            spi.setRequestPriority(Priority.SYSTEM_PRIORITY);
        }

        if (hasTimedOut) {
            sendTimeoutResponse(message, spi);
            return;
        }

        /*
         * If this is CL replay, use the txnid from the CL and use it to update the current txnid
         * Timestamp is actually a pre-IV2ish style time based transaction id
         */
        long timestamp = Long.MIN_VALUE;

        // Update UID if it's for replay
        if (message.isForReplay()) {
            timestamp = message.getUniqueId();
            m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(timestamp);
        } else  {
            timestamp = m_uniqueIdGenerator.getNextUniqueId();
        }

        TxnEgo ego = advanceTxnEgo();
        long mpTxnId = ego.getTxnId();

        final String threadName = Thread.currentThread().getName(); // Thread name has to be materialized here
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.MPI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.meta("process_name", "name", CoreUtils.getHostnameOrAddress()))
                    .add(() -> VoltTrace.meta("thread_name", "name", threadName))
                    .add(() -> VoltTrace.meta("thread_sort_index", "sort_index", Integer.toString(100)))
                    .add(() -> VoltTrace.beginAsync("initmp", mpTxnId,
                                                    "txnId", TxnEgo.txnIdToString(mpTxnId),
                                                    "ciHandle", message.getClientInterfaceHandle(),
                                                    "name", procedureName,
                                                    "read", message.isReadOnly()));
        }

        // Don't have an SP HANDLE at the MPI, so fill in the unused value
        Iv2Trace.logIv2InitiateTaskMessage(message, m_mailbox.getHSId(), mpTxnId, Long.MIN_VALUE);

        // Handle every-site system procedures (at the MPI)
        if (message.isEveryPartition()) {
            assert(SystemProcedureCatalog.listing.get(procedureName) != null &&
                    SystemProcedureCatalog.listing.get(procedureName).getEverysite());
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
                    true, // isEveryPartition
                    null, // nPartitions
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());
            DuplicateCounter counter = new DuplicateCounter(
                    message.getInitiatorHSId(),
                    mpTxnId,
                    m_iv2Masters,
                    message,
                    localId);
            safeAddToDuplicateCounterMap(mpTxnId, counter);
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
                    false, // isEveryPartition
                    null,  // nPartitions
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());

        // Multi-partition initiation (at the MPI)
        MpProcedureTask task = null;
        if (isNpTxn(message) && NP_PROCEDURE_CLASS.hasProClass()) {
            Set<Integer> involvedPartitions = getBalancePartitions(message);
            if (involvedPartitions != null) {
                HashMap<Integer, Long> involvedPartitionMasters = Maps.newHashMap(m_partitionMasters);
                involvedPartitionMasters.keySet().retainAll(involvedPartitions);

                task = instantiateNpProcedureTask(m_mailbox, procedureName,
                        m_pendingTasks, mp, involvedPartitionMasters,
                        m_buddySupplier, false, m_leaderId);
            }

            // if cannot figure out the involved partitions, run it as an MP txn
        }

        int[] nPartitionIds = message.getNPartitionIds();
        if (nPartitionIds != null) {
            HashMap<Integer, Long> involvedPartitionMasters = new HashMap<>();
            for (int partitionId : nPartitionIds) {
                involvedPartitionMasters.put(partitionId, m_partitionMasters.get(partitionId));
            }

            task = instantiateNpProcedureTask(m_mailbox, procedureName,
                    m_pendingTasks, mp, involvedPartitionMasters,
                    m_buddySupplier, false, m_leaderId);
        }


        if (task == null) {
            task = MpProcedureTask.create(m_mailbox, procedureName,
                    m_pendingTasks, mp, m_iv2Masters, m_partitionMasters,
                    m_buddySupplier, false, m_leaderId, false);
        }

        m_outstandingTxns.put(task.m_txnState.txnId, task.m_txnState);
        m_pendingTasks.offer(task);
    }

    /*
     * Responds to a client request that has been found to have timed out while it
     * was loitering in the site queue. Execution has not been started.
     */
    private void sendTimeoutResponse(Iv2InitiateTaskMessage task, StoredProcedureInvocation spi) {
        ClientResponseImpl clientResp = new ClientResponseImpl(ClientResponseImpl.GRACEFUL_FAILURE,
                                                               new VoltTable[0],
                                                               "Request timed out at server before execution",
                                                               spi.getClientHandle());
        InitiateResponseMessage response = new InitiateResponseMessage(task);
        response.setResults(clientResp);
        m_mailbox.send(response.getInitiatorHSId(), response);
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
            handleIv2InitiateTaskMessageRepair((Iv2InitiateTaskMessage)message);
        }
        else {
            // MpInitiatorMailbox should throw RuntimeException for unhandled types before we could get here
            throw new RuntimeException("MpScheduler.handleMessageRepair() received unhandled message type." +
                    " This should be impossible");
        }
    }

    private void handleIv2InitiateTaskMessageRepair(Iv2InitiateTaskMessage message)
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
                    message.isEveryPartition(),
                    null,
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());
        m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(message.getUniqueId());
        // Multi-partition initiation (at the MPI)
        MpProcedureTask task = null;
        if (isNpTxn(message) && NP_PROCEDURE_CLASS.hasProClass()) {
            Set<Integer> involvedPartitions = getBalancePartitions(message);
            if (involvedPartitions != null) {
                HashMap<Integer, Long> involvedPartitionMasters = Maps.newHashMap(m_partitionMasters);
                involvedPartitionMasters.keySet().retainAll(involvedPartitions);

                task = instantiateNpProcedureTask(m_mailbox, procedureName,
                        m_pendingTasks, mp, involvedPartitionMasters,
                        m_buddySupplier, true, m_leaderId);
            }

            // if cannot figure out the involved partitions, run it as an MP txn
        }

        if (task == null) {
            task = MpProcedureTask.create(m_mailbox, procedureName,
                    m_pendingTasks, mp, m_iv2Masters, m_partitionMasters,
                    m_buddySupplier, true, m_leaderId, false);
        }

        m_outstandingTxns.put(task.m_txnState.txnId, task.m_txnState);
        m_pendingTasks.offer(task);
        if (repairLogger.isDebugEnabled()) {
            repairLogger.debug("TXN repair:" + message );
        }
    }

    // The MpScheduler will see InitiateResponseMessages from the Partition masters when
    // performing an every-partition system procedure.  A consequence of this deduping
    // is that the MpScheduler will also need to forward the final InitiateResponseMessage
    // for a normal multipartition procedure back to the client interface since it must
    // see all of these messages and control their transmission.
    public void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.MPI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.endAsync("initmp", message.getTxnId()));
        }

        DuplicateCounter counter = m_duplicateCounters.get(message.getTxnId());

        // A transaction may be routed back here for EveryPartitionTask via leader migration
        if (counter != null && message.isMisrouted()) {
            tmLog.info("The message on the partition is misrouted. TxnID: " + TxnEgo.txnIdToString(message.getTxnId()));
            Long newLeader = m_leaderMigrationMap.get(message.m_sourceHSId);
            if (newLeader != null) {
                // Update the DuplicateCounter with new replica
                counter.updateReplica(message.m_sourceHSId, newLeader);
                m_leaderMigrationMap.remove(message.m_sourceHSId);

                // Leader migration has updated the leader, send the request to the new leader
                m_mailbox.send(newLeader, counter.getOpenMessage());
            } else {
                // Leader migration not done yet.
                m_mailbox.send(message.m_sourceHSId, counter.getOpenMessage());
            }
            return;
        }
        if (counter != null) {
            HashResult result = counter.offer(message);
            if (result.isDone()) {
                m_duplicateCounters.remove(message.getTxnId());
                advanceRepairTruncationHandle(message);
                m_outstandingTxns.remove(message.getTxnId());
                m_mailbox.send(counter.m_destinationId, message);
            } else if (result.isAbort() || result.isMismatch()) {
                VoltDB.crashLocalVoltDB("HASH MISMATCH running every-site system procedure.", true, null);
            }
        } else {
            advanceRepairTruncationHandle(message);
            MpTransactionState txn = (MpTransactionState)m_outstandingTxns.remove(message.getTxnId());
            if (txn == null) {
                // The thread (updateReplicas) could wipe out duplicate counters for run-everywhere system procedures
                // if the duplicate counters contain only the partition masters from failed hosts.
                // A response could get here after the transaction has been declared completed from a failed partition master could get here or
                // from a previous partition master which handles the transaction upon leader migration:
                // partition master has been moved to a host but the host fails.
                tmLog.info(String.format("Received InitiateResponseMessage after the transaction %s is completed from %s",
                        TxnEgo.txnIdToString(message.getTxnId()), CoreUtils.hsIdToString(message.m_sourceHSId)));
                return;
            }
            // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
            m_mailbox.send(message.getInitiatorHSId(), message);
            // We actually completed this MP transaction.  Create a fake CompleteTransactionMessage
            // to send to our local repair log so that the fate of this transaction is never forgotten
            // even if all the masters somehow die before forwarding Complete on to their replicas.
            CompleteTransactionMessage ctm = new CompleteTransactionMessage(m_mailbox.getHSId(),
                    message.m_sourceHSId, message.getTxnId(), message.isReadOnly(), 0,
                    !message.shouldCommit(), false, false, false, txn.isNPartTxn(),
                    message.m_isFromNonRestartableSysproc, false);
            ctm.setTruncationHandle(m_repairLogTruncationHandle);
            // dump it in the repair log
            // hacky castage
            ((MpInitiatorMailbox)m_mailbox).deliverToRepairLog(ctm);
        }
    }

    private void advanceRepairTruncationHandle(InitiateResponseMessage msg) {
        // Only advance the truncation point on completed transactions that sent fragments to SPIs.
        // See ENG-4211 & ENG-14563
        if(msg.shouldCommit() && msg.haveSentMpFragment()) {
            m_repairLogTruncationHandle = m_repairLogAwaitingTruncate;
            m_pendingTasks.setRepairLogTruncationHandle(m_repairLogTruncationHandle);
            m_repairLogAwaitingTruncate = msg.getTxnId();
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
            SerializableException ex = message.getException();
            if (ex instanceof TransactionRestartException && ((TransactionRestartException)ex).isMisrouted()) {
                tmLog.debug("MpScheduler received misroute FragmentResponseMessage");
                ((TransactionRestartException)ex).updateReplicas(m_iv2Masters, m_partitionMasters);
            }
            ((MpTransactionState)txn).offerReceivedFragmentResponse(message);
        }
        else if (tmLog.isDebugEnabled()){
            tmLog.debug("MpScheduler received a FragmentResponseMessage for a null TXN ID: " + message);
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
    private static ProClass<MpProcedureTask> loadNpProcedureTaskClass()
    {
        return ProClass
                .<MpProcedureTask>load("org.voltdb.iv2.NpProcedureTask", "N-Partition",
                        MiscUtils.isPro() ? ProClass.HANDLER_LOG : ProClass.HANDLER_IGNORE)
                .errorHandler(tmLog::error)
                .useConstructorFor(Mailbox.class, String.class, TransactionTaskQueue.class,
                        Iv2InitiateTaskMessage.class, Map.class, Supplier.class, boolean.class, int.class);
    }

    /**
     * Just using "put" on the dup counter map is unsafe.
     * It won't detect the case where keys collide from two different transactions.
     */
    void safeAddToDuplicateCounterMap(long dpKey, DuplicateCounter counter) {
        DuplicateCounter existingDC = m_duplicateCounters.get(dpKey);
        if (existingDC != null) {
            // this is a collision and is bad
            existingDC.logWithCollidingDuplicateCounters(counter);
            VoltDB.crashGlobalVoltDB("DUPLICATE COUNTER MISMATCH: two duplicate counter keys collided.", true, null);
        }
        else {
            m_duplicateCounters.put(dpKey, counter);
        }
    }

    private static MpProcedureTask instantiateNpProcedureTask(Object...params)
    {
        return NP_PROCEDURE_CLASS.newInstance(params);
    }

    public int getLeaderId() {
        return m_leaderId;
    }

    @Override
    public void dump()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("[dump] current truncation handle: ").append(TxnEgo.txnIdToString(m_repairLogTruncationHandle)).append("\n");
        m_pendingTasks.toString(sb);
        hostLog.warn(sb.toString());
    }

    private void handleDumpMessage(DumpMessage message)
    {
        StringBuilder sb = new StringBuilder();
        dumpStackTraceOnFirstSiteThread(message, sb);
        hostLog.warn(sb.toString());
    }
}
