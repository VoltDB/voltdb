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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.zookeeper_voltpatches.CreateMode;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;
import org.voltdb.CommandLog;
import org.voltdb.CommandLog.DurabilityListener;
import org.voltdb.LogEntryType;
import org.voltdb.RealVoltDB;
import org.voltdb.SnapshotCompletionInterest;
import org.voltdb.SnapshotCompletionMonitor;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.VoltDB;
import org.voltdb.VoltDBInterface;
import org.voltdb.VoltTable;
import org.voltdb.VoltZK;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.Priority;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.SerializableException;
import org.voltdb.exceptions.TransactionRestartException;
import org.voltdb.iv2.DuplicateCounter.HashResult;
import org.voltdb.iv2.SiteTasker.SiteTaskerRunnable;
import org.voltdb.iv2.SpInitiator.ServiceState;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.CompleteTransactionResponseMessage;
import org.voltdb.messaging.DummyTransactionResponseMessage;
import org.voltdb.messaging.DummyTransactionTaskMessage;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.DumpPlanThenExitMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.HashMismatchMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2LogFaultMessage;
import org.voltdb.messaging.MPBacklogFlushMessage;
import org.voltdb.messaging.MigratePartitionLeaderMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;
import org.voltdb.messaging.RepairLogTruncationMessage;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.MiscUtils;
import org.voltdb.utils.VoltTrace;

import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.primitives.Ints;
import com.google_voltpatches.common.primitives.Longs;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;

public class SpScheduler extends Scheduler implements SnapshotCompletionInterest
{
    static final VoltLogger tmLog = new VoltLogger("TM");
    static final VoltLogger hostLog = new VoltLogger("HOST");
    static class DuplicateCounterKey implements Comparable<DuplicateCounterKey> {
        private final long m_txnId;
        private final long m_spHandle;

        DuplicateCounterKey(long txnId, long spHandle) {
            m_txnId = txnId;
            m_spHandle = spHandle;
        }

        @Override
        public boolean equals(Object o) {
            try {
                DuplicateCounterKey other = (DuplicateCounterKey) o;
                return (m_txnId == other.m_txnId && m_spHandle == other.m_spHandle);
            }
            catch (Exception e) {
                return false;
            }
        }

        // Only care about comparing TXN ID part for sorting in updateReplicas
        @Override
        public int compareTo(DuplicateCounterKey o) {
            if (m_txnId < o.m_txnId) {
                return -1;
            } else if (m_txnId > o.m_txnId) {
                return 1;
            } else {
                if (m_spHandle < o.m_spHandle) {
                    return -1;
                }
                else if (m_spHandle > o.m_spHandle) {
                    return 1;
                }
                else {
                    return 0;
                }
            }
        }

        @Override
        public int hashCode() {
            assert(false) : "Hashing this is unsafe as it can't promise no collisions.";
            throw new UnsupportedOperationException(
                    "Hashing this is unsafe as it can't promise no collisions.");
        }

        @Override
        public String toString() {
            return "[txn:" + TxnEgo.txnIdToString(m_txnId) + ", spHandle:" + TxnEgo.txnIdToString(m_spHandle) + "]";
        }

        public boolean isSpTransaction() {
            return (TxnEgo.getPartitionId(m_txnId) != MpInitiator.MP_INIT_PID);
        }
    };

    public interface DurableUniqueIdListener {
        /**
         * Notify listener of last durable Single-Part and Multi-Part uniqueIds
         */
        public void lastUniqueIdsMadeDurable(long spUniqueId, long mpUniqueId);
    }

    private List<Long> m_replicaHSIds = new ArrayList<>();
    long m_sendToHSIds[] = new long[0];
    private final TransactionTaskQueue m_pendingTasks;
    private final Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();
    private final TreeMap<DuplicateCounterKey, DuplicateCounter> m_duplicateCounters =
        new TreeMap<DuplicateCounterKey, DuplicateCounter>();
    // MP fragment tasks or completion tasks pending durability
    private final Map<Long, Queue<TransactionTask>> m_mpsPendingDurability =
        new HashMap<Long, Queue<TransactionTask>>();
    private CommandLog m_cl;
    private final SnapshotCompletionMonitor m_snapMonitor;
    private BufferedReadLog m_bufferedReadLog = null;

    // Need to track when command log replay is complete (even if not performed) so that
    // we know when we can start writing viable replay sets to the fault log.
    boolean m_replayComplete = false;
    // The DurabilityListener is not thread-safe. Access it only on the Site thread.
    private final DurabilityListener m_durabilityListener;
    // Generator of pre-IV2ish timestamp based unique IDs
    private final UniqueIdGenerator m_uniqueIdGenerator;

    // the current not-needed-any-more point of the repair log.
    long m_repairLogTruncationHandle = Long.MIN_VALUE;
    // the truncation handle last sent to the replicas
    long m_lastSentTruncationHandle = Long.MIN_VALUE;
    // the max schedule transaction sphandle, multi-fragments mp txn counts one
    long m_maxScheduledTxnSpHandle = Long.MIN_VALUE;

    // the checkpoint transaction sphandle upon MigratePartitionLeader is initiated
    long m_migratePartitionLeaderCheckPoint = Long.MIN_VALUE;

    //The RepairLog is the same instance as the one initialized in InitiatorMailbox.
    //Iv2IniatiateTaskMessage, FragmentTaskMessage and CompleteTransactionMessage
    //are to be added to the repair log when these messages get updated transaction ids.
    protected RepairLog m_repairLog;

    private final boolean IS_KSAFE_CLUSTER;

    private ServiceState m_serviceState;

    SpScheduler(int partitionId, SiteTaskerQueue taskQueue, SnapshotCompletionMonitor snapMonitor, boolean scoreboardEnabled)
    {
        super(partitionId, taskQueue);
        m_pendingTasks = new TransactionTaskQueue(m_tasks, scoreboardEnabled);
        m_snapMonitor = snapMonitor;
        m_durabilityListener = new SpDurabilityListener(this, m_pendingTasks);
        m_uniqueIdGenerator = new UniqueIdGenerator(partitionId, 0);
        m_bufferedReadLog = new BufferedReadLog();
        m_repairLogTruncationHandle = getCurrentTxnId();
        // initialized as current txn id in order to release the initial reads into the system
        m_maxScheduledTxnSpHandle = getCurrentTxnId();
        IS_KSAFE_CLUSTER = VoltDB.instance().getKFactor() > 0;
    }

    public void initializeScoreboard(int siteId) {
        m_pendingTasks.initializeScoreboard(siteId);
    }

    @Override
    public void setLeaderState(boolean isLeader)
    {
        super.setLeaderState(isLeader);
        m_snapMonitor.addInterest(this);
        VoltDBInterface db = VoltDB.instance();
        if (isLeader && db instanceof RealVoltDB ) {
            SpInitiator init = (SpInitiator)((RealVoltDB)db).getInitiator(m_partitionId);
            if (init.m_term != null) {
                ((SpTerm)init.m_term).setPromoting(false);
            }
        }
    }

    @Override
    public void setMaxSeenTxnId(long maxSeenTxnId)
    {
        super.setMaxSeenTxnId(maxSeenTxnId);
        writeIv2ViableReplayEntry();
    }

    @Override
    public void configureDurableUniqueIdListener(final DurableUniqueIdListener listener, final boolean install) {
        SiteTaskerRunnable task = new SiteTaskerRunnable() {
            @Override
            void run()
            {
                m_durabilityListener.configureUniqueIdListener(listener, install);
            }
            private SiteTaskerRunnable init(DurableUniqueIdListener listener){
                taskInfo = listener.getClass().getSimpleName();
                return this;
            }
        }.init(listener);

        Iv2Trace.logSiteTaskerQueueOffer(task);
        m_tasks.offer(task);
    }

    @Override
    public void shutdown()
    {
        m_tasks.offer(m_nullTask);
    }

    // This is going to run in the BabySitter's thread.  This and deliver are synchronized by
    // virtue of both being called on InitiatorMailbox and not directly called.
    // (That is, InitiatorMailbox's API, used by BabySitter, is synchronized on the same
    // lock deliver() is synchronized on.)
    @Override
    public long[] updateReplicas(List<Long> replicas, Map<Integer, Long> partitionMasters,
            TransactionState snapshotTransactionState)
    {
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("[SpScheduler.updateReplicas] replicas to " + CoreUtils.hsIdCollectionToString(replicas) +
                    " on " + CoreUtils.hsIdToString(m_mailbox.getHSId())
             + " from " + CoreUtils.hsIdCollectionToString(m_replicaHSIds));
        }
        long[] replicasAdded = new long[0];
        if (m_replicaHSIds.size() > 0 && replicas.size() > m_replicaHSIds.size()) {
            // Remember the rejoin sites before update replicas set
            Set<Long> rejoinHSIds = Sets.difference(new HashSet<Long>(replicas),
                                                  new HashSet<Long>(m_replicaHSIds));
            replicasAdded = Longs.toArray(rejoinHSIds);
        }
        // First - correct the official replica set.
        m_replicaHSIds = replicas;
        // Update the list of remote replicas that we'll need to send to
        List<Long> sendToHSIds = new ArrayList<Long>(m_replicaHSIds);
        sendToHSIds.remove(m_mailbox.getHSId());
        m_sendToHSIds = Longs.toArray(sendToHSIds);

        // A new site joins in, forward the current txn (stream snapshot save) message to new site
        if (m_isLeader && snapshotTransactionState != null) {
            // Look up the DuplicateCounter for this snapshots fragment
            DuplicateCounterKey key = new DuplicateCounterKey(snapshotTransactionState.txnId,
                    snapshotTransactionState.m_spHandle);
            DuplicateCounter duplicateCounter = m_duplicateCounters.get(key);
            assert (duplicateCounter != null);
            duplicateCounter.addReplicas(replicasAdded);
            // Forward fragment message to new replica
            m_mailbox.send(replicasAdded, duplicateCounter.getOpenMessage());
        }

        // Cleanup duplicate counters and collect DONE counters
        // in this list for further processing.
        List<DuplicateCounterKey> doneCounters = new LinkedList<DuplicateCounterKey>();
        for (Entry<DuplicateCounterKey, DuplicateCounter> entry : m_duplicateCounters.entrySet()) {
            DuplicateCounter counter = entry.getValue();
            HashResult result = counter.updateReplicas(m_replicaHSIds);
            if (result.isDone()) {
                doneCounters.add(entry.getKey());
            }
        }

        //notify the new partition leader that the old leader has completed the Txns if needed
        //after duplicate counters are cleaned. m_mailbox can be MockMailBox which is used for
        //unit test
        if (!m_isLeader && m_mailbox instanceof InitiatorMailbox) {
            ((InitiatorMailbox)m_mailbox).notifyNewLeaderOfTxnDoneIfNeeded();
        }

        // Maintain the CI invariant that responses arrive in txnid order.
        Collections.sort(doneCounters);
        for (DuplicateCounterKey key : doneCounters) {
            DuplicateCounter counter = m_duplicateCounters.remove(key);

            final TransactionState txn = m_outstandingTxns.get(key.m_txnId);
            if (txn == null || txn.isDone()) {
                m_outstandingTxns.remove(key.m_txnId);
                // for MP write txns, we should use it's first SpHandle in the TransactionState
                // for SP write txns, we can just use the SpHandle from the DuplicateCounterKey
                long safeSpHandle = txn == null ? key.m_spHandle: txn.m_spHandle;
                setRepairLogTruncationHandle(safeSpHandle, false);
            }

            VoltMessage resp = counter.getLastResponse();
            if (resp != null) {
                // MPI is tracking deps per partition HSID.  We need to make
                // sure we write ours into the message getting sent to the MPI
                if (resp instanceof FragmentResponseMessage) {
                    FragmentResponseMessage fresp = (FragmentResponseMessage)resp;
                    fresp.setExecutorSiteId(m_mailbox.getHSId());
                }
                m_mailbox.send(counter.m_destinationId, resp);
            }
            else {
                hostLog.warn("TXN " + counter.getTxnId() + " lost all replicas and " +
                        "had no responses.  This should be impossible?");
            }
        }

        long uniqueId;
        if (snapshotTransactionState == null) {
            uniqueId = m_uniqueIdGenerator.getLastUniqueId();
        } else {
            // When there is a snapshot transaction state use the lastSpUniqueId from that
            uniqueId = snapshotTransactionState.m_lastSpUniqueId;
        }
        SettableFuture<Boolean> written = writeIv2ViableReplayEntry(uniqueId);

        // Get the fault log status here to ensure the leader has written it to disk
        // before initiating transactions again.
        blockFaultLogWriteStatus(written);

        return replicasAdded;
    }

    /**
     * Poll the replay sequencer and process the messages until it returns null
     */
    private void deliverReadyTxns() {
        // First, pull all the sequenced messages, if any.
        VoltMessage m = m_replaySequencer.poll();
        while(m != null) {
            deliver(m);
            m = m_replaySequencer.poll();
        }
        // Then, try to pull all the drainable messages, if any.
        m = m_replaySequencer.drain();
        while (m != null) {
            if (m instanceof Iv2InitiateTaskMessage) {
                // Send IGNORED response for all SPs
                Iv2InitiateTaskMessage task = (Iv2InitiateTaskMessage) m;
                final InitiateResponseMessage response = new InitiateResponseMessage(task);
                response.setResults(new ClientResponseImpl(ClientResponse.UNEXPECTED_FAILURE,
                            new VoltTable[0],
                            ClientResponseImpl.IGNORED_TRANSACTION));
                m_mailbox.send(response.getInitiatorHSId(), response);
            }
            m = m_replaySequencer.drain();
        }
    }

    /**
     * Sequence the message for replay if it's for CL or DR.
     *
     * @param message
     * @return true if the message can be delivered directly to the scheduler,
     * false if the message is queued
     */
    @Override
    public boolean sequenceForReplay(VoltMessage message)
    {
        boolean canDeliver = false;
        long sequenceWithUniqueId = Long.MIN_VALUE;

        boolean commandLog = (message instanceof TransactionInfoBaseMessage &&
                (((TransactionInfoBaseMessage)message).isForReplay()));

        boolean sentinel = message instanceof MultiPartitionParticipantMessage;

        boolean replay = commandLog || sentinel;
        boolean sequenceForReplay = m_isLeader && replay;

        if (replay) {
            sequenceWithUniqueId = ((TransactionInfoBaseMessage)message).getUniqueId();
        }

        if (sequenceForReplay) {
            InitiateResponseMessage dupe = m_replaySequencer.dedupe(sequenceWithUniqueId,
                    (TransactionInfoBaseMessage) message);
            if (dupe != null) {
                // Duplicate initiate task message, send response
                m_mailbox.send(dupe.getInitiatorHSId(), dupe);
            }
            else if (!m_replaySequencer.offer(sequenceWithUniqueId, (TransactionInfoBaseMessage) message)) {
                canDeliver = true;
            }
            else {
                deliverReadyTxns();
            }

            // If it's a DR sentinel, send an acknowledgement
            if (sentinel && !commandLog) {
                MultiPartitionParticipantMessage mppm = (MultiPartitionParticipantMessage) message;
                final InitiateResponseMessage response = new InitiateResponseMessage(mppm);
                ClientResponseImpl clientResponse =
                        new ClientResponseImpl(ClientResponseImpl.UNEXPECTED_FAILURE,
                                new VoltTable[0], ClientResponseImpl.IGNORED_TRANSACTION);
                response.setResults(clientResponse);
                m_mailbox.send(response.getInitiatorHSId(), response);
            }
        }
        else {
            if (replay) {
                // Update last seen and last polled uniqueId for replicas
                m_replaySequencer.updateLastSeenUniqueId(sequenceWithUniqueId,
                        (TransactionInfoBaseMessage) message);
                m_replaySequencer.updateLastPolledUniqueId(sequenceWithUniqueId,
                        (TransactionInfoBaseMessage) message);
            }

            canDeliver = true;
        }

        return canDeliver;
    }

    // SpInitiators will see every message type.  The Responses currently come
    // from local work, but will come from replicas when replication is
    // implemented
    @Override
    public void deliver(VoltMessage message)
    {
        if (message instanceof Iv2InitiateTaskMessage) {
            handleIv2InitiateTaskMessage((Iv2InitiateTaskMessage)message);
        }
        else if (message instanceof InitiateResponseMessage) {
            handleInitiateResponseMessage((InitiateResponseMessage)message);
        }
        else if (message instanceof FragmentTaskMessage) {
            handleFragmentTaskMessage((FragmentTaskMessage)message);
        }
        else if (message instanceof FragmentResponseMessage) {
            handleFragmentResponseMessage((FragmentResponseMessage)message);
        }
        else if (message instanceof CompleteTransactionMessage) {
            handleCompleteTransactionMessage((CompleteTransactionMessage)message);
        }
        else if (message instanceof CompleteTransactionResponseMessage) {
            handleCompleteTransactionResponseMessage((CompleteTransactionResponseMessage) message);
        }
        else if (message instanceof BorrowTaskMessage) {
            handleBorrowTaskMessage((BorrowTaskMessage)message);
        }
        else if (message instanceof Iv2LogFaultMessage) {
            handleIv2LogFaultMessage((Iv2LogFaultMessage)message);
        }
        else if (message instanceof DumpMessage) {
            handleDumpMessage((DumpMessage)message);
        } else if (message instanceof DumpPlanThenExitMessage) {
            handleDumpPlanMessage((DumpPlanThenExitMessage)message);
        }
        else if (message instanceof DummyTransactionTaskMessage) {
            handleDummyTransactionTaskMessage((DummyTransactionTaskMessage) message);
        }
        else if (message instanceof DummyTransactionResponseMessage) {
            handleDummyTransactionResponseMessage((DummyTransactionResponseMessage)message);
        }
        else if (message instanceof MPBacklogFlushMessage) {
            cleanupTransactionBacklogOnRepair();
        }
        else {
            throw new RuntimeException("UNKNOWN MESSAGE TYPE, BOOM!");
        }
    }

    // SpScheduler expects to see InitiateTaskMessages corresponding to single-partition
    // procedures only.
    private void handleIv2InitiateTaskMessage(Iv2InitiateTaskMessage message)
    {
        if (!message.isSinglePartition()) {
            VoltDB.crashLocalVoltDB("SpScheduler.handleIv2InitiateTaskMessage " +
                    "should never receive multi-partition initiations. Invocation: " + message, true, null);
        }
        final String procedureName = message.getStoredProcedureName();
        long newSpHandle;
        long uniqueId = Long.MIN_VALUE;
        Iv2InitiateTaskMessage msg = message;

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
         * - We don't check timeouts for replicas or replay
         * - No timeout is ever reported if priority is SYSTEM_PRIORITY
         * - We immediately clear the request-timeout indicator here
         * - Any replica requests or repair logs we create will have
         *   SYSTEM_PRIORITY and not have request-timeout indication
         */
        boolean hasTimedOut = false;
        StoredProcedureInvocation spi = msg.getStoredProcedureInvocation();
        if (spi != null) {
            if (spi.hasRequestTimeout()) {
                if (m_isLeader && !message.isForReplay()) {
                    hasTimedOut = spi.requestHasTimedOut();
                }
                spi.clearRequestTimeout();
            }
            spi.setRequestPriority(Priority.SYSTEM_PRIORITY);
        }

        if (hasTimedOut) {
            sendTimeoutResponse(msg, spi);
            return;
        }

        if (m_isLeader || message.isReadOnly()) {
            /*
             * If this is for CL replay or DR, update the unique ID generator
             */
            if (message.isForReplay()) {
                uniqueId = message.getUniqueId();
                try {
                    m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(uniqueId);
                }
                catch (Exception e) {
                    hostLog.fatal(e.getMessage());
                    hostLog.fatal("Invocation: " + message);
                    VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                }
            }
            /*
             * If this is CL replay use the txnid from the CL and also
             * update the txnid to match the one from the CL
             */
            if (message.isForReplay()) {
                TxnEgo ego = advanceTxnEgo();
                newSpHandle = ego.getTxnId();
                updateMaxScheduledTransactionSpHandle(newSpHandle);
            } else if (m_isLeader && !message.isReadOnly()) {
                TxnEgo ego = advanceTxnEgo();
                newSpHandle = ego.getTxnId();
                updateMaxScheduledTransactionSpHandle(newSpHandle);
                uniqueId = m_uniqueIdGenerator.getNextUniqueId();
            } else {
                /*
                 * The SPI read or the short circuit read case. Since we are read only,
                 * do not create new transaction IDs but reuse the last seen
                 * txnid. For a timestamp, might as well give a reasonable one
                 * for a read heavy workload so time isn't bursty.
                 */
                uniqueId = UniqueIdGenerator.makeIdFromComponents(
                        Math.max(System.currentTimeMillis(), m_uniqueIdGenerator.lastUsedTime),
                        0,
                        m_uniqueIdGenerator.partitionId);

                newSpHandle = getMaxScheduledTxnSpHandle();
            }

            // Need to set the SP handle on the received message
            // Need to copy this or the other local sites handling
            // the same initiate task message will overwrite each
            // other's memory -- the message isn't copied on delivery
            // to other local mailboxes. This copy refers to the original
            // SPI but we have reset the priority and request timeout.
            msg = new Iv2InitiateTaskMessage(
                    message.getInitiatorHSId(),
                    message.getCoordinatorHSId(),
                    getRepairLogTruncationHandleForReplicas(),
                    message.getTxnId(),
                    message.getUniqueId(),
                    message.isReadOnly(),
                    message.isSinglePartition(),
                    message.isEveryPartition(),
                    null, // nPartitions
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());
            msg.setShouldReturnResultTables(message.shouldReturnResultTables());
            msg.setSpHandle(newSpHandle);
            logRepair(msg);
            // Also, if this is a vanilla single-part procedure, make the TXNID
            // be the SpHandle (for now)
            // Only system procedures are every-site, so we'll check through the SystemProcedureCatalog
            if (msg.isEveryPartition()) {
                assert(SystemProcedureCatalog.listing.get(procedureName) != null &&
                        SystemProcedureCatalog.listing.get(procedureName).getEverysite());
            }
            else {
                msg.setTxnId(newSpHandle);
                msg.setUniqueId(uniqueId);
            }

            // The leader will be responsible to replicate messages to replicas.
            // Don't replicate reads, no matter FAST or SAFE.
            if (m_isLeader && !msg.isReadOnly() && IS_KSAFE_CLUSTER) {
                for (long hsId : m_sendToHSIds) {
                    Iv2InitiateTaskMessage finalMsg = msg;
                    final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
                    if (traceLog != null) {
                        traceLog.add(() -> VoltTrace.beginAsync("replicateSP",
                                                                MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), hsId, finalMsg.getSpHandle(), finalMsg.getClientInterfaceHandle()),
                                                                "txnId", TxnEgo.txnIdToString(finalMsg.getTxnId()),
                                                                "dest", CoreUtils.hsIdToString(hsId)));
                    }
                }
                Iv2InitiateTaskMessage replmsg =
                    new Iv2InitiateTaskMessage(m_mailbox.getHSId(),
                            m_mailbox.getHSId(),
                            getRepairLogTruncationHandleForReplicas(),
                            msg.getTxnId(),
                            msg.getUniqueId(),
                            msg.isReadOnly(),
                            msg.isSinglePartition(),
                            msg.isEveryPartition(),
                            msg.getStoredProcedureInvocation(),
                            msg.getClientInterfaceHandle(),
                            msg.getConnectionId(),
                            msg.isForReplay(),
                            true);
                replmsg.setShouldReturnResultTables(false);
                replmsg.getStoredProcedureInvocation().setKeepParamsImmutable(true);

                // Update the handle in the copy since the constructor doesn't set it
                replmsg.setSpHandle(newSpHandle);
                // K-safety cluster doesn't always mean partition has replicas,
                // node failure may reduce the number of replicas for each partition
                if (m_sendToHSIds.length > 0) {
                    m_mailbox.send(m_sendToHSIds, replmsg);
                }

                DuplicateCounter counter = new DuplicateCounter(
                        msg.getInitiatorHSId(),
                        msg.getTxnId(),
                        m_replicaHSIds,
                        replmsg,
                        m_mailbox.getHSId());

                safeAddToDuplicateCounterMap(new DuplicateCounterKey(msg.getTxnId(), newSpHandle), counter);
            }
        }
        else { // !(m_isLeader || message.isReadOnly())
            setMaxSeenTxnId(msg.getSpHandle());
            newSpHandle = msg.getSpHandle();
            logRepair(msg);
            // Don't update the uniqueID if this is a run-everywhere txn, because it has an MPI unique ID.
            if (UniqueIdGenerator.getPartitionIdFromUniqueId(msg.getUniqueId()) == m_partitionId) {
                m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(msg.getUniqueId());
            }
        }
        Iv2Trace.logIv2InitiateTaskMessage(message, m_mailbox.getHSId(), msg.getTxnId(), newSpHandle);
        doLocalInitiateOffer(msg);
    }

    /**
     * Do the work necessary to turn the Iv2InitiateTaskMessage into a
     * TransactionTask which can be queued to the TransactionTaskQueue.
     * This is reused by both the normal message handling path and the repair
     * path, and assumes that the caller has dealt with or ensured that the
     * necessary ID, SpHandles, and replication issues are resolved.
     */
    private void doLocalInitiateOffer(Iv2InitiateTaskMessage msg)
    {
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
        if (traceLog != null) {
            final String threadName = Thread.currentThread().getName(); // Thread name has to be materialized here
            traceLog.add(() -> VoltTrace.meta("process_name", "name", CoreUtils.getHostnameOrAddress()))
                    .add(() -> VoltTrace.meta("thread_name", "name", threadName))
                    .add(() -> VoltTrace.meta("thread_sort_index", "sort_index", Integer.toString(10000)))
                    .add(() -> VoltTrace.beginAsync("localSp",
                                                    MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), m_mailbox.getHSId(), msg.getSpHandle(), msg.getClientInterfaceHandle()),
                                                    "ciHandle", msg.getClientInterfaceHandle(),
                                                    "txnId", TxnEgo.txnIdToString(msg.getTxnId()),
                                                    "partition", m_partitionId,
                                                    "read", msg.isReadOnly(),
                                                    "name", msg.getStoredProcedureName(),
                                                    "hsId", CoreUtils.hsIdToString(m_mailbox.getHSId())));
        }

        final String procedureName = msg.getStoredProcedureName();
        final SpProcedureTask task = SpProcedureTask.create(m_mailbox, procedureName, m_pendingTasks, msg);

        ListenableFuture<Object> durabilityBackpressureFuture =
                m_cl.log(msg, msg.getSpHandle(), null, m_durabilityListener, task);

        if (traceLog != null && durabilityBackpressureFuture != null) {
            traceLog.add(() -> VoltTrace.beginAsync("durability",
                                                    MiscUtils.hsIdTxnIdToString(m_mailbox.getHSId(), msg.getSpHandle()),
                                                    "txnId", TxnEgo.txnIdToString(msg.getTxnId()),
                                                    "partition", Integer.toString(m_partitionId)));
        }

        //Durability future is always null for sync command logging
        //the transaction will be delivered again by the CL for execution once durable
        //Async command logging has to offer the task immediately with a Future for backpressure
        if (m_cl.canOfferTask()) {
            m_pendingTasks.offer(task.setDurabilityBackpressureFuture(durabilityBackpressureFuture));
        }
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

    @Override
    public void handleMessageRepair(List<Long> needsRepair, VoltMessage message)
    {
        if (message instanceof Iv2InitiateTaskMessage) {
            handleIv2InitiateTaskMessageRepair(needsRepair, (Iv2InitiateTaskMessage)message);
        }
        else if (message instanceof FragmentTaskMessage) {
            handleFragmentTaskMessageRepair(needsRepair, (FragmentTaskMessage)message);
        }
        else if (message instanceof CompleteTransactionMessage) {
            // It should be safe to just send CompleteTransactionMessages to everyone.
            //if it gets here, the message is for the leader to repair from MpScheduler
            ((CompleteTransactionMessage) message).setForReplica(false);
            handleCompleteTransactionMessage((CompleteTransactionMessage)message);
        }
        else {
            throw new RuntimeException("SpScheduler.handleMessageRepair received unexpected message type: " +
                    message);
        }
    }

    private void handleIv2InitiateTaskMessageRepair(List<Long> needsRepair, Iv2InitiateTaskMessage message)
    {
        if (!message.isSinglePartition()) {
            throw new RuntimeException("SpScheduler.handleIv2InitiateTaskMessageRepair " +
                    "should never receive multi-partition initiations.");
        }

        // set up duplicate counter. expect exactly the responses corresponding
        // to needsRepair. These may, or may not, include the local site.
        List<Long> expectedHSIds = new ArrayList<Long>(needsRepair);
        DuplicateCounter counter = new DuplicateCounter(
                HostMessenger.VALHALLA,
                message.getTxnId(),
                expectedHSIds,
                message,
                m_mailbox.getHSId());

        final DuplicateCounterKey dcKey = new DuplicateCounterKey(message.getTxnId(), message.getSpHandle());
        updateOrAddDuplicateCounter(dcKey, counter);
        m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(message.getUniqueId());
        // is local repair necessary?
        if (needsRepair.contains(m_mailbox.getHSId())) {
            needsRepair.remove(m_mailbox.getHSId());
            // make a copy because handleIv2 non-repair case does?
            Iv2InitiateTaskMessage localWork =
                new Iv2InitiateTaskMessage(message.getInitiatorHSId(),
                    message.getCoordinatorHSId(), message);
            doLocalInitiateOffer(localWork);
        }

        // is remote repair necessary?
        if (!needsRepair.isEmpty()) {
            //to replica for repair
            Iv2InitiateTaskMessage replmsg =
                new Iv2InitiateTaskMessage(m_mailbox.getHSId(), m_mailbox.getHSId(), message, true);
            m_mailbox.send(Longs.toArray(needsRepair), replmsg);
        }
    }

    private void handleFragmentTaskMessageRepair(List<Long> needsRepair, FragmentTaskMessage message)
    {
        if (needsRepair.contains(m_mailbox.getHSId()) && m_outstandingTxns.get(message.getTxnId()) != null) {
            // Sanity check that we really need repair.
            hostLog.warn("SPI repair attempted to repair a fragment which it has already seen. " +
                    "This shouldn't be possible.");
            // Not sure what to do in this event.  Crash for now
            throw new RuntimeException("Attempted to repair with a fragment we've already seen.");
        }
        // set up duplicate counter. expect exactly the responses corresponding
        // to needsRepair. These may, or may not, include the local site.
        List<Long> expectedHSIds = new ArrayList<Long>(needsRepair);
        DuplicateCounter counter = new DuplicateCounter(
                message.getCoordinatorHSId(), // Assume that the MPI's HSID hasn't changed
                message.getTxnId(),
                expectedHSIds,
                message,
                m_mailbox.getHSId());

        final DuplicateCounterKey dcKey = new DuplicateCounterKey(message.getTxnId(), message.getSpHandle());
        updateOrAddDuplicateCounter(dcKey, counter);
        // is local repair necessary?
        if (needsRepair.contains(m_mailbox.getHSId())) {
            needsRepair.remove(m_mailbox.getHSId());
            // make a copy because handleIv2 non-repair case does?
            FragmentTaskMessage localWork =
                new FragmentTaskMessage(message.getInitiatorHSId(),
                    message.getCoordinatorHSId(), message);
            doLocalFragmentOffer(localWork);
        }

        // is remote repair necessary?
        if (!needsRepair.isEmpty()) {
            FragmentTaskMessage replmsg =
                new FragmentTaskMessage(m_mailbox.getHSId(), m_mailbox.getHSId(), message);
            m_mailbox.send(Longs.toArray(needsRepair), replmsg);
        }
    }

    private void updateOrAddDuplicateCounter(final DuplicateCounterKey dcKey, DuplicateCounter counter) {
        DuplicateCounter theCounter = m_duplicateCounters.get(dcKey);
        if (theCounter == null) {
            counter.setTransactionRepair(true);
            safeAddToDuplicateCounterMap(dcKey, counter);
        } else {
            // The partition leader on the local site is being migrated away, but the migration fails. The local site
            // can be elected again as leader. In this case, update the duplicate counter.

            // If local site is already in the duplicate counter, retain it.
            List<Long> expectedHSIDs = new ArrayList<Long>(counter.m_expectedHSIds);
            if (!expectedHSIDs.contains(m_mailbox.getHSId())) {
                expectedHSIDs.add(m_mailbox.getHSId());
            }
            theCounter.setTransactionRepair(true);
            theCounter.updateReplicas(expectedHSIDs);
        }
    }

    // Pass a response through the duplicate counters.
    private void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        //For mis-routed transactions, no update for truncation handle or duplicated counter
        if (message.isMisrouted()){
            m_mailbox.send(message.getInitiatorHSId(), message);
            return;
        }

        /**
         * A shortcut read is a read operation sent to any replica and completed with no
         * confirmation or communication with other replicas. In a partition scenario, it's
         * possible to read an unconfirmed transaction's writes that will be lost.
         */
        final long spHandle = message.getSpHandle();
        final DuplicateCounterKey dcKey = new DuplicateCounterKey(message.getTxnId(), spHandle);
        DuplicateCounter counter = m_duplicateCounters.get(dcKey);
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);

        // All reads will have no duplicate counter.
        // Avoid all the lookup below.
        // Also, don't update the truncation handle, since it won't have meaning for anyone.
        if (message.isReadOnly()) {
            if (traceLog != null) {
                traceLog.add(() -> VoltTrace.endAsync("localSp", MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), message.m_sourceHSId, message.getSpHandle(), message.getClientInterfaceHandle())));
            }

            // InvocationDispatcher routes SAFE reads to SPI only
            assert(m_bufferedReadLog != null);
            m_bufferedReadLog.offer(m_mailbox, message, m_repairLogTruncationHandle);
            return;
        }

        if (traceLog != null && message.m_sourceHSId == m_mailbox.getHSId()) {
            traceLog.add(() -> VoltTrace.endAsync("localSp", MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), message.m_sourceHSId, message.getSpHandle(), message.getClientInterfaceHandle())/*,
                                                      "hash", message.getClientResponseData().getHashes()[0]*/));
        }
        if (traceLog != null && message.m_sourceHSId != m_mailbox.getHSId()) {
            traceLog.add(() -> VoltTrace.endAsync("replicateSP",
                                                    MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), message.m_sourceHSId, message.getSpHandle(), message.getClientInterfaceHandle())/*,
                                                  "hash", message.getClientResponseData().getHashes()[0]*/));
        }
        if (counter != null) {
            HashResult result = counter.offer(message);
            if (result.isDone()) {
                if (counter.isSuccess() || (!counter.isSuccess() && m_isEnterpriseLicense)) {
                    m_duplicateCounters.remove(dcKey);
                    final TransactionState txn = m_outstandingTxns.get(message.getTxnId());
                    setRepairLogTruncationHandle(spHandle, (txn != null && txn.isLeaderMigrationInvolved()));
                    if (!counter.isSuccess()) {
                        sendServiceStateUpdateRequest(counter);
                    }
                    m_mailbox.send(counter.m_destinationId, counter.m_lastResponse);
                } else {
                    if (m_isLeader && m_sendToHSIds.length > 0) {
                        StringBuilder sb = new StringBuilder();
                        for (long hsId : m_sendToHSIds) {
                            sb.append(CoreUtils.hsIdToString(hsId)).append(" ");
                        }
                        hostLog.info("Send dump plan message to other replicas: " + sb.toString());
                        m_mailbox.send(m_sendToHSIds, new DumpPlanThenExitMessage(counter.getStoredProcedureName()));
                    }
                    if (tmLog.isDebugEnabled()) {
                        RealVoltDB.printDiagnosticInformation(VoltDB.instance().getCatalogContext(),
                                counter.getStoredProcedureName(), m_procSet);
                    }
                    VoltDB.crashLocalVoltDB("Hash mismatch: replicas produced different results.", true, null);
                }
            }
        } else {
            // the initiatorHSId is the ClientInterface mailbox.
            // this will be on SPI without k-safety or replica only with k-safety
            assert(!message.isReadOnly());
            setRepairLogTruncationHandle(spHandle, false);

            //BabySitter's thread (updateReplicas) could clean up a duplicate counter and send a transaction response to ClientInterface
            //if the duplicate counter contains only the replica's HSIDs from failed hosts. That is, a response from a replica could get here
            //AFTER the transaction is completed. Such a response message should not be further propagated.
            if (m_mailbox.getHSId() != message.getInitiatorHSId()) {
                m_mailbox.send(message.getInitiatorHSId(), message);
            }
        }

        //notify the new partition leader that the old leader has completed the Txns if needed.
        //m_mailbox can be MockMailBox for unit test.
        if (!m_isLeader && m_mailbox instanceof InitiatorMailbox) {
            ((InitiatorMailbox)m_mailbox).notifyNewLeaderOfTxnDoneIfNeeded();
        }
    }

    private void sendServiceStateUpdateRequest(DuplicateCounter counter){
        m_mailbox.send(Longs.toArray(counter.getMisMatchedReplicas()), new HashMismatchMessage());
        tmLog.warn("Hash mismatch is detected on replicas:" + CoreUtils.hsIdCollectionToString(counter.getMisMatchedReplicas()));

        final HostMessenger hostMessenger = VoltDB.instance().getHostMessenger();
        VoltZK.addHashMismatchedSite(hostMessenger.getZK(), m_mailbox.getHSId());

        VoltZK.createActionBlocker(hostMessenger.getZK(), VoltZK.reducedClusterSafety,
                CreateMode.PERSISTENT, tmLog, "Transfer to Reduced Safety Mode");
        Set<Integer> liveHids = hostMessenger.getLiveHostIds();
        boolean isClusterComplete = VoltDB.instance().isClusterComplete();
        MigratePartitionLeaderMessage message = new MigratePartitionLeaderMessage(m_mailbox.getHSId(), Integer.MIN_VALUE);
        for (Integer hostId : liveHids) {
            final long ciHsid = CoreUtils.getHSIdFromHostAndSite(hostId, HostMessenger.CLIENT_INTERFACE_SITE_ID);
            if (isClusterComplete) {
                m_mailbox.send(ciHsid, message);
            }
        }
    }

    // BorrowTaskMessages encapsulate a FragmentTaskMessage along with
    // input dependency tables. The MPI issues borrows to a local site
    // to perform replicated reads or aggregation fragment work.
    private void handleBorrowTaskMessage(BorrowTaskMessage message) {
        // borrows do not advance the sp handle. The handle would
        // move backwards anyway once the next message is received
        // from the SP leader.
        long newSpHandle = getMaxScheduledTxnSpHandle();
        Iv2Trace.logFragmentTaskMessage(message.getFragmentTaskMessage(),
                m_mailbox.getHSId(), newSpHandle, true);
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.beginAsync("recvfragment",
                                                    MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), m_mailbox.getHSId(), newSpHandle, 0),
                                                    "txnId", TxnEgo.txnIdToString(message.getTxnId()),
                                                    "partition", m_partitionId,
                                                    "hsId", CoreUtils.hsIdToString(m_mailbox.getHSId())));
        }

        TransactionState txn = m_outstandingTxns.get(message.getTxnId());

        if (txn == null) {
            // If the borrow is the first fragment for a transaction, run it as
            // a single partition fragment; Must not  engage/pause this
            // site on a MP transaction before the SP instructs to do so.
            // Do not track the borrow task as outstanding - it completes
            // immediately and is not a valid transaction state for
            // full MP participation (it claims everything can run as SP).
            txn = new BorrowTransactionState(newSpHandle, message);
        }

        // BorrowTask is a read only task embedded in a MP transaction
        // and its response (FragmentResponseMessage) should not be buffered
        if (message.getFragmentTaskMessage().isSysProcTask()) {
            final SysprocBorrowedTask task =
                new SysprocBorrowedTask(m_mailbox, (ParticipantTransactionState)txn,
                                        m_pendingTasks, message.getFragmentTaskMessage(),
                                        message.getInputDepMap());
            task.setResponseNotBufferable();
            m_pendingTasks.offer(task);
        }
        else {
            final BorrowedTask task =
                new BorrowedTask(m_mailbox, (ParticipantTransactionState)txn,
                        m_pendingTasks, message.getFragmentTaskMessage(),
                        message.getInputDepMap());
            task.setResponseNotBufferable();
            m_pendingTasks.offer(task);
        }
    }

    // SpSchedulers will see FragmentTaskMessage for:
    // - The scatter fragment(s) of a multi-part transaction (normal or sysproc)
    // - Borrow tasks to do the local fragment work if this partition is the
    //   buddy of the MPI.  Borrow tasks may include input dependency tables for
    //   aggregation fragments, or not, if it's a replicated table read.
    // For multi-batch MP transactions, we'll need to look up the transaction state
    // that gets created when the first batch arrives.
    // During command log replay a new SP handle is going to be generated, but it really
    // doesn't matter, it isn't going to be used for anything.
    void handleFragmentTaskMessage(FragmentTaskMessage message)
    {
        FragmentTaskMessage msg = message;
        long newSpHandle;
        //The site has been marked as non-leader. The follow-up batches or fragments are processed here
        if (!message.isForReplica() && (m_isLeader || message.isExecutedOnPreviousLeader())) {
            // message processed on leader
            // Quick hack to make progress...we need to copy the FragmentTaskMessage
            // before we start mucking with its state (SPHANDLE).  We need to revisit
            // all the messaging mess at some point.
            msg = new FragmentTaskMessage(message.getInitiatorHSId(),
                    message.getCoordinatorHSId(), message);
            //Not going to use the timestamp from the new Ego because the multi-part timestamp is what should be used
            msg.setTimestamp(message.getTimestamp());
            msg.setExecutedOnPreviousLeader(message.isExecutedOnPreviousLeader());
            if (!message.isReadOnly()) {
                TxnEgo ego = advanceTxnEgo();
                newSpHandle = ego.getTxnId();

                if (m_outstandingTxns.get(msg.getTxnId()) == null) {
                    updateMaxScheduledTransactionSpHandle(newSpHandle);
                }
            } else {
                newSpHandle = getMaxScheduledTxnSpHandle();
                // use the same sphandle for all readonly fragments
                final TransactionState txn = m_outstandingTxns.get(message.getTxnId());
                if (txn != null) {
                    newSpHandle = txn.m_spHandle;
                }
            }

            msg.setSpHandle(newSpHandle);
            msg.setLastSpUniqueId(m_uniqueIdGenerator.getLastUniqueId());
            logRepair(msg);
            if (msg.getInitiateTask() != null) {
                msg.getInitiateTask().setSpHandle(newSpHandle);//set the handle
                //Trigger reserialization so the new handle is used
                msg.setStateForDurability(msg.getInitiateTask(), msg.getInvolvedPartitions());
            }

            /*
             * If there a replicas to send it to, forward it!
             * Unless... it's read only AND not a sysproc. Read only sysprocs may expect to be sent
             * everywhere.
             * In that case don't propagate it to avoid a determinism check and extra messaging overhead
             */
            if (IS_KSAFE_CLUSTER && (!message.isReadOnly() || msg.isSysProcTask())) {
                for (long hsId : m_sendToHSIds) {
                    FragmentTaskMessage finalMsg = msg;
                    final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
                    if (traceLog != null) {
                        traceLog.add(() -> VoltTrace.beginAsync("replicatefragment",
                                                                MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), hsId, finalMsg.getSpHandle(), finalMsg.getTxnId()),
                                                                "txnId", TxnEgo.txnIdToString(finalMsg.getTxnId()),
                                                                "dest", CoreUtils.hsIdToString(hsId)));
                    }
                }

                FragmentTaskMessage replmsg =
                    new FragmentTaskMessage(m_mailbox.getHSId(),
                            m_mailbox.getHSId(), msg);
                replmsg.setForReplica(true);
                replmsg.setTimestamp(msg.getTimestamp());
                // K-safety cluster doesn't always mean partition has replicas,
                // node failure may reduce the number of replicas for each partition.
                if (m_sendToHSIds.length > 0) {
                    m_mailbox.send(m_sendToHSIds,replmsg);
                }
                DuplicateCounter counter;
                /*
                 * Non-determinism should be impossible to happen with MP fragments.
                 * if you see "MP_DETERMINISM_ERROR" as procedure name in the crash logs
                 * something has horribly gone wrong.
                 */
                if (message.getFragmentTaskType() != FragmentTaskMessage.SYS_PROC_PER_SITE) {
                    counter = new DuplicateCounter(
                            msg.getCoordinatorHSId(),
                            msg.getTxnId(),
                            m_replicaHSIds,
                            replmsg,
                            m_mailbox.getHSId());
                }
                else {
                    counter = new SysProcDuplicateCounter(
                            msg.getCoordinatorHSId(),
                            msg.getTxnId(),
                            m_replicaHSIds,
                            replmsg,
                            m_mailbox.getHSId());
                }
                safeAddToDuplicateCounterMap(new DuplicateCounterKey(message.getTxnId(), newSpHandle), counter);
            }
        } else {
            // message processed on replica
            logRepair(msg);
            newSpHandle = msg.getSpHandle();
            setMaxSeenTxnId(newSpHandle);
        }

        Iv2Trace.logFragmentTaskMessage(message, m_mailbox.getHSId(), newSpHandle, false);
        doLocalFragmentOffer(msg);
    }

    /**
     * Do the work necessary to turn the FragmentTaskMessage into a
     * TransactionTask which can be queued to the TransactionTaskQueue.
     * This is reused by both the normal message handling path and the repair
     * path, and assumes that the caller has dealt with or ensured that the
     * necessary ID, SpHandles, and replication issues are resolved.
     */
    private void doLocalFragmentOffer(FragmentTaskMessage msg)
    {
        final String threadName = Thread.currentThread().getName(); // Thread name has to be materialized here
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.meta("process_name", "name", CoreUtils.getHostnameOrAddress()))
                    .add(() -> VoltTrace.meta("thread_name", "name", threadName))
                    .add(() -> VoltTrace.meta("thread_sort_index", "sort_index", Integer.toString(10000)))
                    .add(() -> VoltTrace.beginAsync("recvfragment",
                                                    MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), m_mailbox.getHSId(), msg.getSpHandle(), msg.getTxnId()),
                                                    "txnId", TxnEgo.txnIdToString(msg.getTxnId()),
                                                    "partition", m_partitionId,
                                                    "hsId", CoreUtils.hsIdToString(m_mailbox.getHSId()),
                                                    "final", msg.isFinalTask()));
        }

        TransactionState txn = m_outstandingTxns.get(msg.getTxnId());
        boolean logThis = false;
        // bit of a hack...we will probably not want to create and
        // offer FragmentTasks for txn ids that don't match if we have
        // something in progress already
        if (txn == null) {
            //FIXME: replay transaction doesn't have initiate task,
            txn = new ParticipantTransactionState(msg.getSpHandle(), msg, msg.isReadOnly(),
                    msg.isNPartTxn());
            m_outstandingTxns.put(msg.getTxnId(), txn);
            // Only want to send things to the command log if it satisfies this predicate
            // AND we've never seen anything for this transaction before.  We can't
            // actually log until we create a TransactionTask, though, so just keep track
            // of whether it needs to be done.

            // Like SP, we should log writes and safe reads.
            logThis = true;
        }

        // Check to see if this is the final task for this txn, and if so, if we can close it out early
        // Right now, this just means read-only.
        // NOTE: this overlaps slightly with CompleteTransactionMessage handling completion.  It's so tiny
        // that for now, meh, but if this scope grows then it should get refactored out
        if (msg.isFinalTask() && txn.isReadOnly()) {
            m_outstandingTxns.remove(msg.getTxnId());
        }

        TransactionTask task;
        Iv2InitiateTaskMessage clMessage = msg.getInitiateTask();
        if (msg.isSysProcTask()) {
            // inject catalog bytes into c/l
            if (logThis && clMessage != null && ("@UpdateCore").equalsIgnoreCase(msg.getProcedureName())) {
                // Only one site writes the real UAC initiate task, other sites write dummy tasks, to command log
                Iv2InitiateTaskMessage uac = clMessage;
                StoredProcedureInvocation invocation = new StoredProcedureInvocation();
                invocation.setProcName("@UpdateCore");
                if (m_isLowestSiteId) {
                    // First find the expected catalog version in the parameters
                    CatalogUtil.copyUACParameters(invocation, uac.getParameters());
                    invocation.setClientHandle(uac.getStoredProcedureInvocation().getClientHandle());
                    if (invocation.getSerializedParams() == null) {
                        try {
                            invocation = MiscUtils.roundTripForCL(invocation);
                        } catch (IOException e) {
                            hostLog.fatal("Failed to serialize invocation @UpdateCore: " + e.getMessage());
                            VoltDB.crashLocalVoltDB(e.getMessage(), true, e);
                        }
                    }
                }
                clMessage = new Iv2InitiateTaskMessage(
                        uac.getInitiatorHSId(),
                        uac.getCoordinatorHSId(),
                        uac.getTruncationHandle(),
                        uac.getTxnId(),
                        uac.getUniqueId(),
                        uac.isReadOnly(),
                        uac.isSinglePartition(),
                        uac.isEveryPartition(),
                        uac.getNPartitionIds(),
                        invocation,
                        uac.getClientInterfaceHandle(),
                        uac.getConnectionId(),
                        uac.isForReplay());
            }
            task =
                new SysprocFragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                        m_pendingTasks, msg, null);
        }
        else {
            task =
                new FragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                 m_pendingTasks, msg, null);
        }
        if (logThis) {
            ListenableFuture<Object> durabilityBackpressureFuture =
                    m_cl.log(clMessage, msg.getSpHandle(), Ints.toArray(msg.getInvolvedPartitions()),
                             m_durabilityListener, task);

            if (traceLog != null && durabilityBackpressureFuture != null) {
                traceLog.add(() -> VoltTrace.beginAsync("durability",
                                                        MiscUtils.hsIdTxnIdToString(m_mailbox.getHSId(), msg.getSpHandle()),
                                                        "txnId", TxnEgo.txnIdToString(msg.getTxnId()),
                                                        "partition", Integer.toString(m_partitionId)));
            }

            //Durability future is always null for sync command logging
            //the transaction will be delivered again by the CL for execution once durable
            //Async command logging has to offer the task immediately with a Future for backpressure
            if (m_cl.canOfferTask()) {
                m_pendingTasks.offer(task.setDurabilityBackpressureFuture(durabilityBackpressureFuture));
                if( hostLog.isTraceEnabled() ) {
                    hostLog.trace("SpScheduler.doLocalFragmentOffer() add " + (msg.isSysProcTask() ? "SysprocFragmentTask" : "FragmentTask")  +
                            " with sync logging P" + this.m_partitionId + " mpId=" + msg.getUniqueId());
                }

            } else {
                /* Getting here means that the task is the first fragment of an MP txn and
                 * synchronous command logging is on, so create a backlog for future tasks of
                 * this MP arrived before it's marked durable.
                 *
                 * This is important for synchronous command logging and MP txn restart. Without
                 * this, a restarted MP txn may not be gated by logging of the first fragment.
                 */
                assert !m_mpsPendingDurability.containsKey(task.getTxnId());
                m_mpsPendingDurability.put(task.getTxnId(), new ArrayDeque<TransactionTask>());
                if( hostLog.isTraceEnabled() ) {
                    hostLog.trace("SpScheduler.doLocalFragmentOffer() add " + (msg.isSysProcTask() ? "SysprocFragmentTask" : "FragmentTask") +
                            " with async logging P" + this.m_partitionId + " mpId=" + msg.getUniqueId());
                }
            }
        } else {
            queueOrOfferMPTask(task);
            if( hostLog.isTraceEnabled() ) {
                hostLog.trace("SpScheduler.doLocalFragmentOffer() add " + (msg.isSysProcTask() ? "SysprocFragmentTask" : "FragmentTask") +
                        " P" + this.m_partitionId + " mpId=" + msg.getUniqueId());
            }
        }
    }

    /**
     * Offer all fragment tasks and complete transaction tasks queued for durability for the given
     * MP transaction, and remove the entry from the pending map so that future ones won't be
     * queued.
     *
     * @param txnId    The MP transaction ID.
     */
    public void offerPendingMPTasks(long txnId)
    {
        Queue<TransactionTask> pendingTasks = m_mpsPendingDurability.get(txnId);
        if (pendingTasks != null) {
            for (TransactionTask task : pendingTasks) {
                if (task instanceof SpProcedureTask) {
                    final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
                    if (traceLog != null) {
                        traceLog.add(() -> VoltTrace.endAsync("durability",
                                                              MiscUtils.hsIdTxnIdToString(m_mailbox.getHSId(), task.getSpHandle())));
                    }
                } else if (task instanceof FragmentTask) {
                    final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
                    if (traceLog != null) {
                        traceLog.add(() -> VoltTrace.endAsync("durability",
                                                              MiscUtils.hsIdTxnIdToString(m_mailbox.getHSId(), ((FragmentTask) task).m_fragmentMsg.getSpHandle())));
                    }
                }

                m_pendingTasks.offer(task);
            }
            m_mpsPendingDurability.remove(txnId);
        }
    }

    /**
     * Check if the MP task has to be queued because the first fragment is still being logged
     * synchronously to the command log. If not, offer it to the transaction task queue.
     *
     * @param task    A fragment task or a complete transaction task
     */
    private void queueOrOfferMPTask(TransactionTask task)
    {
        // The pending map will only have an entry for the transaction if the first fragment is
        // still pending durability.
        Queue<TransactionTask> pendingTasks = m_mpsPendingDurability.get(task.getTxnId());
        if (pendingTasks != null) {
            pendingTasks.offer(task);
        } else {
            m_pendingTasks.offer(task);
        }
    }

    private boolean isFragmentMisrouted(FragmentResponseMessage message) {
        SerializableException ex = message.getException();
        if (ex != null && ex instanceof TransactionRestartException) {
            return (((TransactionRestartException)ex).isMisrouted());
        }
        return false;
    }

    // Eventually, the master for a partition set will need to be able to dedupe
    // FragmentResponses from its replicas.
    private void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        if (isFragmentMisrouted(message)){
            m_mailbox.send(message.getDestinationSiteId(), message);
            return;
        }
        final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);

        // Send the message to the duplicate counter, if any
        DuplicateCounter counter =
            m_duplicateCounters.get(new DuplicateCounterKey(message.getTxnId(), message.getSpHandle()));
        final TransactionState txn = m_outstandingTxns.get(message.getTxnId());
        if (counter != null) {
            String traceName = "recvfragment";
            if (message.m_sourceHSId != m_mailbox.getHSId()) {
                traceName = "replicatefragment";
            }
            String finalTraceName = traceName;
            if (traceLog != null) {
                traceLog.add(() -> VoltTrace.endAsync(finalTraceName, MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), message.m_sourceHSId, message.getSpHandle(), message.getTxnId()),
                                                      "status", message.getStatusCode()));
            }
            HashResult result = counter.offer(message);
            if (result.isDone()) {
                if (counter.isSuccess() || (!counter.isSuccess() && m_isEnterpriseLicense)) {
                    if (txn != null && txn.isDone()) {
                        setRepairLogTruncationHandle(txn.m_spHandle, txn.isLeaderMigrationInvolved());
                    }

                    m_duplicateCounters.remove(new DuplicateCounterKey(message.getTxnId(), message.getSpHandle()));
                    FragmentResponseMessage resp = (FragmentResponseMessage)counter.getLastResponse();
                    // MPI is tracking deps per partition HSID.  We need to make
                    // sure we write ours into the message getting sent to the MPI
                    resp.setExecutorSiteId(m_mailbox.getHSId());
                    if (!counter.isSuccess()) {
                        sendServiceStateUpdateRequest(counter);
                    }
                    m_mailbox.send(counter.m_destinationId, resp);
                } else {
                    VoltDB.crashGlobalVoltDB("Hash mismatch running multi-part procedure.", true, null);
                }
            }
            // doing duplicate suppression: all done.
            return;
        }

        // No k-safety means no replica: read/write queries on master.
        // K-safety: read-only queries (on master) or write queries (on replica).
        if ( (m_isLeader || (!m_isLeader && message.isExecutedOnPreviousLeader()))
                && m_sendToHSIds.length > 0 && message.getRespBufferable()
                && (txn == null || txn.isReadOnly()) ) {
            // on k-safety leader with safe reads configuration: one shot reads + normal multi-fragments MP reads
            // we will have to buffer these reads until previous writes acked in the cluster.
            long readTxnId = txn == null ? message.getSpHandle() : txn.m_spHandle;
            m_bufferedReadLog.offer(m_mailbox, message, readTxnId, m_repairLogTruncationHandle);
            return;
        }

        // for complete writes txn, we will advance the transaction point
        if (txn != null && !txn.isReadOnly() && txn.isDone()) {
            setRepairLogTruncationHandle(txn.m_spHandle, message.isExecutedOnPreviousLeader());
        }

        if (traceLog != null) {
            traceLog.add(() -> VoltTrace.endAsync("recvfragment", MiscUtils.hsIdPairTxnIdToString(m_mailbox.getHSId(), message.m_sourceHSId, message.getSpHandle(), message.getTxnId()),
                                                  "status", message.getStatusCode()));
        }

        // Message arrives after duplicate counter is cleaned/removed, do not send to itself
        if (message.m_sourceHSId != message.getDestinationSiteId()) {
            m_mailbox.send(message.getDestinationSiteId(), message);
        }
    }

    private void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        CompleteTransactionMessage msg = message;
        TransactionState txn = m_outstandingTxns.get(msg.getTxnId());

        // 1) The site is not a leader any more, thanks to MigratePartitionLeader but the message is intended for leader.
        //    action: advance TxnEgo, send it to all original replicas (before MigratePartitionLeader)
        // 2) The site is the new leader but the message is intended for replica
        //    action: no TxnEgo advance
        if (!message.isForReplica()) {
            msg = new CompleteTransactionMessage(m_mailbox.getHSId(), m_mailbox.getHSId(), message);
            msg.setTimestamp(message.getTimestamp());
            // Set the spHandle so that on repair the new master will set the max seen spHandle
            // correctly
            advanceTxnEgo();
            msg.setSpHandle(getCurrentTxnId());
            msg.setForReplica(true);
            msg.setRequireAck(true);
            if (m_sendToHSIds.length > 0 && !msg.isReadOnly()) {
                m_mailbox.send(m_sendToHSIds, msg);
            }
        } else if(!m_isLeader) {
            setMaxSeenTxnId(msg.getSpHandle());
        }
        logRepair(msg);
        // We can currently receive CompleteTransactionMessages for multipart procedures
        // which only use the buddy site (replicated table read).  Ignore them for
        // now, fix that later.
        if (txn != null)
        {
            CompleteTransactionMessage finalMsg = msg;
            final VoltTrace.TraceEventBatch traceLog = VoltTrace.log(VoltTrace.Category.SPI);
            if (traceLog != null) {
                traceLog.add(() -> VoltTrace.instant("recvCompleteTxn",
                                                     "txnId", TxnEgo.txnIdToString(finalMsg.getTxnId()),
                                                     "partition", Integer.toString(m_partitionId),
                                                     "hsId", CoreUtils.hsIdToString(m_mailbox.getHSId())));
            }

            final boolean isSysproc = ((FragmentTaskMessage) txn.getNotice()).isSysProcTask();
            if (IS_KSAFE_CLUSTER && !msg.isRestart() && (!msg.isReadOnly() || isSysproc) && !message.isForReplica()) {

                DuplicateCounter counter;
                counter = new DuplicateCounter(msg.getCoordinatorHSId(),
                                               msg.getTxnId(),
                                               m_replicaHSIds,
                                               msg,
                                               m_mailbox.getHSId());
                safeAddToDuplicateCounterMap(new DuplicateCounterKey(msg.getTxnId(), msg.getSpHandle()), counter);
            }

            Iv2Trace.logCompleteTransactionMessage(msg, m_mailbox.getHSId());
            final CompleteTransactionTask task =
                new CompleteTransactionTask(m_mailbox, txn, m_pendingTasks, msg);
            queueOrOfferMPTask(task);
        } else {
            if (msg.needsCoordination()) {
                final CompleteTransactionTask missingTxnCompletion =
                        new CompleteTransactionTask(m_mailbox, null, m_pendingTasks, msg);
                m_pendingTasks.handleCompletionForMissingTxn(missingTxnCompletion);
            }
            // Generate a dummy response message when this site has not seen previous FragmentTaskMessage,
            // the leader may have started to wait for replicas' response messages.
            // This can happen in the early phase of site rejoin before replica receiving the snapshot initiation,
            // it also means this CompleteTransactionMessage message will be dropped because it's after snapshot.
            final CompleteTransactionResponseMessage resp = new CompleteTransactionResponseMessage(msg);
            resp.m_sourceHSId = m_mailbox.getHSId();
            handleCompleteTransactionResponseMessage(resp);
        }
    }

    private void handleCompleteTransactionResponseMessage(CompleteTransactionResponseMessage msg)
    {
        final DuplicateCounterKey duplicateCounterKey = new DuplicateCounterKey(msg.getTxnId(), msg.getSpHandle());
        DuplicateCounter counter = m_duplicateCounters.get(duplicateCounterKey);
        boolean txnDone = true;
        if (msg.isRestart()) {
            // Don't mark txn done for restarts
            txnDone = false;
        }
        if (msg.isAborted() && counter != null) {
            // The last completion was an abort due to a repair/abort or restart/abort so we need to remove duplicate counters
            // for stale versions of the restarted Txn that never made it past the scoreboard
            final DuplicateCounterKey lowestPossible = new DuplicateCounterKey(msg.getTxnId(), 0);
            DuplicateCounterKey staleMatch = m_duplicateCounters.ceilingKey(lowestPossible);
            while (staleMatch != null && staleMatch.compareTo(duplicateCounterKey) == -1) {
                m_duplicateCounters.remove(staleMatch);
                staleMatch = m_duplicateCounters.ceilingKey(lowestPossible);
            };
        }

        if (counter != null) {
            txnDone = counter.offer(msg).isDone();
        }

        if (txnDone) {
            final TransactionState txn = m_outstandingTxns.remove(msg.getTxnId());
            m_duplicateCounters.remove(duplicateCounterKey);
            if (txn != null && !txn.isReadOnly()) {
                // Set the truncation handle here instead of when processing
                // FragmentResponseMessage to avoid letting replicas think a
                // fragment is done before the MP txn is fully committed.
                if (!txn.isDone() && tmLog.isDebugEnabled()) {
                    tmLog.debug("Transaction " + TxnEgo.txnIdToString(msg.getTxnId()) + " is not completed.");
                }
                assert txn.isDone() : "Counter " + counter + ", leader " + m_isLeader + ", " + msg;
                setRepairLogTruncationHandle(txn.m_spHandle, txn.isLeaderMigrationInvolved());
            }
        }

        // The CompleteTransactionResponseMessage ends at the SPI. It is not
        // sent to the MPI because it doesn't care about it.
        // The SPI uses this response message to track if all replicas have
        // committed the transaction. avoid sending to itself from some stale message.

        // During partition leader migration, the leadership of the site is being moved away. The site
        // has been marked as not a leader and then receives the responses from replicas.
        // These responses from replicas end here---do not send the message to itself
        // committed the transaction. avoid sending to itself from some stale message.
        if (!m_isLeader && msg.requireAck() && msg.getSPIHSId() != m_mailbox.getHSId()) {
            m_mailbox.send(msg.getSPIHSId(), msg);
        }
    }

    /**
     * Should only receive these messages at replicas, when told by the leader
     */
    private void handleIv2LogFaultMessage(Iv2LogFaultMessage message)
    {
        //call the internal log write with the provided SP handle and wait for the fault log IO to complete
        SettableFuture<Boolean> written = writeIv2ViableReplayEntryInternal(message.getSpHandle());

        // Get the Fault Log Status here to ensure the replica completes the log fault task is finished before
        // it starts processing transactions again
        blockFaultLogWriteStatus(written);

        setMaxSeenTxnId(message.getSpHandle());

        // Also initialize the unique ID generator and the last durable unique ID using
        // the value sent by the master
        m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(message.getSpUniqueId());
        m_cl.initializeLastDurableUniqueId(m_durabilityListener, m_uniqueIdGenerator.getLastUniqueId());
    }

    /**
     * Wait to get the status of a fault log write
     */
    private void blockFaultLogWriteStatus(SettableFuture<Boolean> written) {
        boolean logWritten = false;

        if (written != null) {
            try {
                logWritten = written.get();
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug("Could not determine fault log state for partition: " + m_partitionId, e);
                }
            }
            if (!logWritten) {
                tmLog.warn("Attempted fault log not written for partition: " + m_partitionId);
            }
        }
    }

    private void handleDumpMessage(DumpMessage message)
    {
        String who = CoreUtils.hsIdToString(m_mailbox.getHSId());
        StringBuilder builder = new StringBuilder();
        builder.append("START OF STATE DUMP FOR SITE: ").append(who);
        if (message.getTxnId() > 0) {
            builder.append(" FROM TXNID:" + TxnEgo.txnIdToString(message.getTxnId()));
        }
        builder.append("\n  partition: ").append(m_partitionId).append(", isLeader: ").append(m_isLeader);
        if (m_isLeader) {
            builder.append("  replicas: ").append(CoreUtils.hsIdCollectionToString(m_replicaHSIds));
            if (m_sendToHSIds.length > 0) {
                m_mailbox.send(m_sendToHSIds, new DumpMessage());
            }
        }
        builder.append("\n  most recent SP handle: ").append(TxnEgo.txnIdToString(getCurrentTxnId()));
        builder.append("\n  outstanding txns: ").append(TxnEgo.txnIdCollectionToString(m_outstandingTxns.keySet()));
        builder.append("\n  ");
        m_pendingTasks.toString(builder);
        if (m_duplicateCounters.size() > 0) {
            builder.append("\n  DUPLICATE COUNTERS:\n ");
            for (Entry<DuplicateCounterKey, DuplicateCounter> e : m_duplicateCounters.entrySet()) {
                builder.append("  ").append(e.getKey().toString()).append(": ");
                e.getValue().dumpCounter(builder);
            }
        }
        builder.append("END of STATE DUMP FOR SITE: ").append(who);
        dumpStackTraceOnFirstSiteThread(message, builder);
        hostLog.warn(builder.toString());
    }

    private void handleDummyTransactionTaskMessage(DummyTransactionTaskMessage message)
    {
        DummyTransactionTaskMessage msg = message;
        if (m_isLeader) {
            TxnEgo ego = advanceTxnEgo();
            long newSpHandle = ego.getTxnId();
            updateMaxScheduledTransactionSpHandle(newSpHandle);
            // this uniqueId is needed as the command log tracks it (uniqueId has to advance)
            long uniqueId = m_uniqueIdGenerator.getNextUniqueId();
            msg = new DummyTransactionTaskMessage(m_mailbox.getHSId(), newSpHandle, uniqueId);

            if (m_sendToHSIds.length > 0) {
                DummyTransactionTaskMessage replmsg = new DummyTransactionTaskMessage(m_mailbox.getHSId(), newSpHandle, uniqueId);
                replmsg.setForReplica(true);
                m_mailbox.send(m_sendToHSIds, replmsg);

                DuplicateCounter counter = new DuplicateCounter(
                        HostMessenger.VALHALLA,
                        msg.getTxnId(),
                        m_replicaHSIds,
                        msg,m_mailbox.getHSId());
                safeAddToDuplicateCounterMap(new DuplicateCounterKey(msg.getTxnId(), newSpHandle), counter);
            }
        } else {
            setMaxSeenTxnId(msg.getSpHandle());
        }
        Iv2Trace.logDummyTransactionTaskMessage(msg, m_mailbox.getHSId());
        logRepair(msg);
        DummyTransactionTask task = new DummyTransactionTask(m_mailbox,
                new SpTransactionState(msg), m_pendingTasks);
        // This read only DummyTransactionTask is to help flushing the task queue,
        // including tasks in command log queue as well.
        ListenableFuture<Object> durabilityBackpressureFuture =
                m_cl.log(null, msg.getSpHandle(), null,  m_durabilityListener, task);
        // Durability future is always null for sync command logging
        // the transaction will be delivered again by the CL for execution once durable
        // Async command logging has to offer the task immediately with a Future for backpressure
        if (m_cl.canOfferTask()) {
            m_pendingTasks.offer(task.setDurabilityBackpressureFuture(durabilityBackpressureFuture));
        }
    }

    private void handleDummyTransactionResponseMessage(DummyTransactionResponseMessage message) {
        final long spHandle = message.getSpHandle();
        final DuplicateCounterKey dcKey = new DuplicateCounterKey(message.getTxnId(), spHandle);
        DuplicateCounter counter = m_duplicateCounters.get(dcKey);
        if (counter == null) {
            // this will be on SPI without k-safety or replica only with k-safety
            setRepairLogTruncationHandle(spHandle, false);
            if (!m_isLeader) {
                m_mailbox.send(message.getSPIHSId(), message);
            }
            return;
        }

        HashResult result = counter.offer(message);
        if (result.isDone()) {
            // DummyTransactionResponseMessage ends on SPI
            m_duplicateCounters.remove(dcKey);
            setRepairLogTruncationHandle(spHandle, false);
        }
    }

    public void handleDumpPlanMessage(DumpPlanThenExitMessage msg)
    {
        hostLog.error("This node is going to shutdown because a hash mismatch error was detected on " +
                       CoreUtils.getHostIdFromHSId(msg.m_sourceHSId) + ":" + CoreUtils.getSiteIdFromHSId(msg.m_sourceHSId));
        if (tmLog.isDebugEnabled()) {
            RealVoltDB.printDiagnosticInformation(VoltDB.instance().getCatalogContext(),
                    msg.getProcName(), m_procSet);
        }
        VoltDB.crashLocalVoltDB("Hash mismatch", true, null);
    }

    @Override
    public void setCommandLog(CommandLog cl) {
        m_cl = cl;
        m_durabilityListener.createFirstCompletionCheck(cl.isSynchronous(), cl.isEnabled());
        m_cl.registerDurabilityListener(m_durabilityListener);
    }

    @Override
    public void enableWritingIv2FaultLog()
    {
        m_replayComplete = true;
        writeIv2ViableReplayEntry();
    }

    /**
     * If appropriate, cause the initiator to write the viable replay set to the command log
     * Use when it's unclear whether the caller is the leader or a replica; the right thing will happen.
     *
     * This will return a future to block on for the write on the fault log. If the attempt to write
     * the replay entry was never followed through due to conditions, it will be null. If the attempt
     * to write the replay entry went through but could not be done internally, the future will be false.
     */
    private SettableFuture<Boolean> writeIv2ViableReplayEntry()
    {
        return writeIv2ViableReplayEntry(m_uniqueIdGenerator.getLastUniqueId());
    }

    private SettableFuture<Boolean> writeIv2ViableReplayEntry(long lastUniqueId) {
        SettableFuture<Boolean> written = null;
        if (m_replayComplete && m_isLeader) {
            // write the viable set locally
            long faultSpHandle = advanceTxnEgo().getTxnId();
            written = writeIv2ViableReplayEntryInternal(faultSpHandle);
            // Generate Iv2LogFault message and send it to replicas
            Iv2LogFaultMessage faultMsg = new Iv2LogFaultMessage(faultSpHandle, lastUniqueId);
            m_mailbox.send(m_sendToHSIds, faultMsg);
        }
        return written;
    }

    public SettableFuture<Boolean> logMasterMode() {
        SettableFuture<Boolean> written = null;
        if (m_replayComplete) {
            long faultSpHandle = advanceTxnEgo().getTxnId();
            Set<Long> master = Sets.newHashSet();
            master.add(m_mailbox.getHSId());
            written = m_cl.logIv2Fault(m_mailbox.getHSId(),
                    master, m_partitionId, faultSpHandle, LogEntryType.MASTERMODE);
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("Log master only mode on site " + CoreUtils.hsIdToString(m_mailbox.getHSId()) + " partition:" + m_partitionId);
            }
        }
        return written;
    }

    /**
     * Write the viable replay set to the command log with the provided SP Handle.
     * Pass back the future that is set after the fault log is written to disk.
     */
    SettableFuture<Boolean> writeIv2ViableReplayEntryInternal(long spHandle)
    {
        SettableFuture<Boolean> written = null;
        if (m_replayComplete) {
            Set<Long> replicas = new HashSet<>();
            if (isLeader()) {
                replicas.addAll(m_replicaHSIds);
            } else {
                replicas.addAll(VoltDB.instance().getCartographer().getReplicasForPartition(m_partitionId));
            }
            written = m_cl.logIv2Fault(m_mailbox.getHSId(), replicas, m_partitionId, spHandle);
        }
        return written;
    }

    @Override
    public CountDownLatch snapshotCompleted(SnapshotCompletionEvent event)
    {
        if (event.truncationSnapshot && event.didSucceed) {
            synchronized(m_lock) {
                writeIv2ViableReplayEntry();
            }
        }
        return new CountDownLatch(0);
    }

    public void processDurabilityChecks(final CommandLog.CompletionChecks currentChecks) {
        final SiteTaskerRunnable r = new SiteTasker.SiteTaskerRunnable() {
            @Override
            void run() {
                assert(currentChecks != null);
                synchronized (m_lock) {
                    currentChecks.processChecks();
                }
            }
        };
        if (InitiatorMailbox.SCHEDULE_IN_SITE_THREAD) {
            if (hostLog.isDebugEnabled() || Iv2Trace.IV2_QUEUE_TRACE_ENABLED) {
                r.taskInfo = currentChecks.getClass().getSimpleName();
            }

            Iv2Trace.logSiteTaskerQueueOffer(r);
            m_tasks.offer(r);
        } else {
            r.run();
        }
    }

    /**
     * Just using "put" on the dup counter map is unsafe.
     * It won't detect the case where keys collide from two different transactions.
     */
    void safeAddToDuplicateCounterMap(DuplicateCounterKey dpKey, DuplicateCounter counter) {
        DuplicateCounter existingDC = m_duplicateCounters.get(dpKey);
        if (existingDC == null) {
            m_duplicateCounters.put(dpKey, counter);
        } else {
            existingDC.logWithCollidingDuplicateCounters(counter);
            VoltDB.crashGlobalVoltDB("DUPLICATE COUNTER MISMATCH: two duplicate counter keys collided.", true, null);
        }
    }


    @Override
    public void dump()
    {
        StringBuilder sb = new StringBuilder();
        m_replaySequencer.dump(m_mailbox.getHSId(), sb);
        sb.append("\n    current truncation handle: " + TxnEgo.txnIdToString(m_repairLogTruncationHandle) + " "
                + m_bufferedReadLog.toString());
        m_repairLog.indentedString(sb, 5);
        hostLog.warn(sb.toString());
    }

    private void updateMaxScheduledTransactionSpHandle(long newSpHandle) {
        m_maxScheduledTxnSpHandle = Math.max(m_maxScheduledTxnSpHandle, newSpHandle);
    }

    long getMaxScheduledTxnSpHandle() {
        return m_maxScheduledTxnSpHandle;
    }

    private long getRepairLogTruncationHandleForReplicas()
    {
        m_lastSentTruncationHandle = m_repairLogTruncationHandle;
        return m_repairLogTruncationHandle;
    }

    private void setRepairLogTruncationHandle(long newHandle, boolean isExecutedOnOldLeader)
    {
        if (newHandle > m_repairLogTruncationHandle) {
            m_repairLogTruncationHandle = newHandle;
            // ENG-14553: release buffered reads regardless of leadership status
            m_bufferedReadLog.releaseBufferedReads(m_mailbox, m_repairLogTruncationHandle);
            // We have to advance the local truncation point on the replica. It's important for
            // node promotion when there are no missing repair log transactions on the replica.
            // Because we still want to release the reads if no following writes will come to this replica.
            // Also advance the truncation point if this is not a leader but the response message is for leader.
            if (m_isLeader || isExecutedOnOldLeader) {
                scheduleRepairLogTruncateMsg(m_repairLogTruncationHandle);
            }
        } else {
            // As far as I know, they are cases that will move truncation handle backwards.
            // These include node failures (promotion phase) and node rejoin (early rejoin phase).
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("Skipping trucation handle update " + TxnEgo.txnIdToString(m_repairLogTruncationHandle) +
                        "to" + TxnEgo.txnIdToString(newHandle)+ " isLeader:" + m_isLeader + " isExecutedOnOldLeader:" + isExecutedOnOldLeader);
            }
        }
    }

    /**
     * Schedules a task to be run on the site to send the latest truncation
     * handle to the replicas. This should be called whenever the local
     * truncation handle advances on the leader to guarantee that the replicas
     * will hear about the new handle in case there is no more transactions to
     * carry the information over.
     *
     * The truncation handle is not sent immediately when this method is called
     * to avoid sending a message for every committed transaction. In most cases
     * when there is sufficient load on the system, there will always be a new
     * transaction that this information can piggy-back on. In that case, by the
     * time this task runs on the site, the last sent truncation handle has
     * already advanced, so there is no need to send the message. This has the
     * benefit of sending more truncation messages when the throughput is low,
     * which makes the replicas see committed transactions faster.
     */
    private void scheduleRepairLogTruncateMsg(long newHandle)
    {
        // skip schedule jobs if no TxnCommitInterests need to be notified
        if (m_sendToHSIds.length == 0 && m_repairLog.hasNoTxnCommitInterests()) {
            return;
        }

        SiteTaskerRunnable r = new SiteTaskerRunnable() {
            @Override
            void run()
            {
                synchronized (m_lock) {
                    if (m_lastSentTruncationHandle < newHandle) {
                        m_lastSentTruncationHandle = newHandle;

                        final RepairLogTruncationMessage truncMsg = new RepairLogTruncationMessage(newHandle);
                        // Also keep the local repair log's truncation point up-to-date
                        // so that it can trigger the callbacks.
                        truncMsg.m_sourceHSId = m_mailbox.getHSId();
                        m_repairLog.deliver(truncMsg);
                        if (m_sendToHSIds.length > 0) {
                            m_mailbox.send(m_sendToHSIds, truncMsg);
                        }
                    }
                }
            }
        };
        if (hostLog.isDebugEnabled() || Iv2Trace.IV2_QUEUE_TRACE_ENABLED) {
            r.taskInfo = "Repair Log Truncate Message Handle:" + TxnEgo.txnIdToString(m_repairLogTruncationHandle);
        }

        Iv2Trace.logSiteTaskerQueueOffer(r);
        m_tasks.offer(r);
    }


    public TransactionState getTransactionState(long txnId) {
        return m_outstandingTxns.get(txnId);
    }

    private void logRepair(VoltMessage message) {
        //null check for unit test
        if (m_repairLog != null) {
            m_repairLog.deliver(message);
        }
    }

    public void checkPointMigratePartitionLeader() {
        m_migratePartitionLeaderCheckPoint = getMaxScheduledTxnSpHandle();
        tmLog.info("MigratePartitionLeader checkpoint on " + CoreUtils.hsIdToString(m_mailbox.getHSId()) +
                    " sphandle: " + TxnEgo.txnIdToString(m_migratePartitionLeaderCheckPoint));
    }

    public boolean txnDoneBeforeCheckPoint() {
        if (m_migratePartitionLeaderCheckPoint < 0) {
            return false;
        }
        List<DuplicateCounterKey> keys = m_duplicateCounters.keySet().stream()
                .filter(k->k.m_spHandle < m_migratePartitionLeaderCheckPoint && k.isSpTransaction()).collect(Collectors.toList());
        if (!keys.isEmpty()) {
            if (tmLog.isDebugEnabled()) {
                StringBuilder builder = new StringBuilder();
                for (DuplicateCounterKey dc : keys) {
                    builder.append(TxnEgo.txnIdToString(dc.m_txnId) + "(" + dc.m_spHandle + "),");
                    DuplicateCounter counter = m_duplicateCounters.get(dc);
                    builder.append(counter.m_openMessage + "\n");
                }
                tmLog.debug("Duplicate counters on " + CoreUtils.hsIdToString(m_mailbox.getHSId()) + " have keys smaller than the sphandle:" +
                        TxnEgo.txnIdToString(m_migratePartitionLeaderCheckPoint) + "\n" + builder.toString());
            }
            return false;
        }
        tmLog.info("MigratePartitionLeader previous leader " + CoreUtils.hsIdToString(m_mailbox.getHSId()) +
                " has completed transactions before sphandle: " + TxnEgo.txnIdToString(m_migratePartitionLeaderCheckPoint));
        m_migratePartitionLeaderCheckPoint = Long.MIN_VALUE;
        return true;
    }

    // Because now in rejoin we rely on first fragment of stream snapshot to update the replica
    // set of every partition, it creates a window that may cause task log on rejoin node miss sp txns.
    // To fix it, leader forwards to rejoin node any sp txn that are queued in backlog between leader receives the
    // first fragment of stream snapshot and site runs the first fragment.
    public void forwardPendingTaskToRejoinNode(long[] replicasAdded, long snapshotSpHandle) {
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("Forward pending tasks in backlog to rejoin node: " + Arrays.toString(replicasAdded));
        }
        if (replicasAdded.length == 0) {
            return;
        }
        boolean sentAny = false;
        for (Map.Entry<DuplicateCounterKey, DuplicateCounter> entry : m_duplicateCounters.entrySet()) {
            if (snapshotSpHandle < entry.getKey().m_spHandle) {
                if (!sentAny) {
                    sentAny = true;
                    if (tmLog.isDebugEnabled()) {
                        tmLog.debug("Start forwarding pending tasks to rejoin node.");
                    }
                }

                // Then forward any message after the MP txn, I expect them are all Iv2InitiateMessages
                if (tmLog.isDebugEnabled()) {
                    tmLog.debug(entry.getValue().getOpenMessage().getMessageInfo());
                }
                m_mailbox.send(replicasAdded, entry.getValue().getOpenMessage());
            }
        }
        if (sentAny && tmLog.isDebugEnabled()) {
            tmLog.debug("Finish forwarding pending tasks to rejoin node.");
        }
    }

    // Since repair log doesn't include any MP RO transaction, MPI needs to clean up
    // possible in-progress RO transaction. Note that if RO transaction is a multi-fragment read, some sites may have
    // not received the fragment while some others have received. MPI starts the next transaction without cleaning up
    // prior RO transaction, on the sites that have received fragment, the backlogs of transaction task queue are still blocked on the fragment, it causes MP deadlock for the next transaction.
    // Fix: ask site leaders to clean their backlogs and duplicate counters when they receives repair log request
    // site leaders also forward the message to its replicas.
    @Override
    public void cleanupTransactionBacklogOnRepair() {
        if (m_isLeader && m_sendToHSIds.length > 0) {
            m_mailbox.send(m_sendToHSIds, new MPBacklogFlushMessage());
        }
        Iterator<Entry<Long, TransactionState>> iter = m_outstandingTxns.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Long, TransactionState> entry = iter.next();
            TransactionState txnState = entry.getValue();
            if (TxnEgo.getPartitionId(entry.getKey()) == MpInitiator.MP_INIT_PID ) {
                if (txnState.isReadOnly()) {
                    txnState.setDone();
                    m_duplicateCounters.entrySet().removeIf((e) -> e.getKey().m_txnId == entry.getKey());
                    iter.remove();
                }
            }
        }

        // flush all RO transactions out of backlog
        m_pendingTasks.removeMPReadTransactions();
    }

    public ServiceState getServiceState() {
        return m_serviceState;
    }

    public void setServiceState(ServiceState serviceState) {
        m_serviceState = serviceState;
    }

    @Override
    public void cleanupTransactionBacklogs() {
        if (m_serviceState.isRemoved()) {
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("Clean up transaction backlogs");
            }
            m_duplicateCounters.clear();
            m_outstandingTxns.clear();
            m_pendingTasks.m_taskQueue.clear();
            m_repairLog.clear();
        }
    }
}
