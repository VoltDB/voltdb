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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.TransactionInfoBaseMessage;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.ClientResponseImpl;
import org.voltdb.CommandLog;
import org.voltdb.CommandLog.DurabilityListener;
import org.voltdb.PartitionDRGateway;
import org.voltdb.SnapshotCompletionInterest;
import org.voltdb.SnapshotCompletionMonitor;
import org.voltdb.SystemProcedureCatalog;
import org.voltdb.VoltDB;
import org.voltdb.VoltTable;
import org.voltdb.client.ClientResponse;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.iv2.SiteTasker.SiteTaskerRunnable;
import org.voltdb.messaging.BorrowTaskMessage;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2LogFaultMessage;
import org.voltdb.messaging.MultiPartitionParticipantMessage;

import com.google_voltpatches.common.primitives.Ints;
import com.google_voltpatches.common.primitives.Longs;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

public class SpScheduler extends Scheduler implements SnapshotCompletionInterest
{
    static final VoltLogger tmLog = new VoltLogger("TM");

    static class DuplicateCounterKey implements Comparable<DuplicateCounterKey>
    {
        private final long m_txnId;
        private final long m_spHandle;
        transient final int m_hash;

        DuplicateCounterKey(long txnId, long spHandle)
        {
            m_txnId = txnId;
            m_spHandle = spHandle;
            m_hash = (37 * (int)(m_txnId ^ (m_txnId >>> 32))) +
                ((int)(m_spHandle ^ (m_spHandle >>> 32)));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || !(getClass().isInstance(o))) {
                return false;
            }

            DuplicateCounterKey other = (DuplicateCounterKey)o;

            return (m_txnId == other.m_txnId && m_spHandle == other.m_spHandle);
        }

        // Only care about comparing TXN ID part for sorting in updateReplicas
        @Override
        public int compareTo(DuplicateCounterKey o)
        {
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
        public int hashCode()
        {
            return m_hash;
        }

        @Override
        public String toString()
        {
            return "<" + m_txnId + ", " + m_spHandle + ">";
        }
    };

    List<Long> m_replicaHSIds = new ArrayList<Long>();
    long m_sendToHSIds[] = new long[0];

    private final TransactionTaskQueue m_pendingTasks;
    private final Map<Long, TransactionState> m_outstandingTxns =
        new HashMap<Long, TransactionState>();
    private final Map<DuplicateCounterKey, DuplicateCounter> m_duplicateCounters =
        new HashMap<DuplicateCounterKey, DuplicateCounter>();
    // MP fragment tasks or completion tasks pending durability
    private final Map<Long, Queue<TransactionTask>> m_mpsPendingDurability =
        new HashMap<Long, Queue<TransactionTask>>();
    private CommandLog m_cl;
    private PartitionDRGateway m_drGateway = new PartitionDRGateway();
    private final SnapshotCompletionMonitor m_snapMonitor;
    // Need to track when command log replay is complete (even if not performed) so that
    // we know when we can start writing viable replay sets to the fault log.
    boolean m_replayComplete = false;
    private final DurabilityListener m_durabilityListener;
    //Generator of pre-IV2ish timestamp based unique IDs
    private final UniqueIdGenerator m_uniqueIdGenerator;


    // the current not-needed-any-more point of the repair log.
    long m_repairLogTruncationHandle = Long.MIN_VALUE;

    SpScheduler(int partitionId, SiteTaskerQueue taskQueue, SnapshotCompletionMonitor snapMonitor)
    {
        super(partitionId, taskQueue);
        m_pendingTasks = new TransactionTaskQueue(m_tasks,getCurrentTxnId());
        m_snapMonitor = snapMonitor;
        m_durabilityListener = new DurabilityListener() {
            @Override
            public void onDurability(final ArrayList<Object> durableThings) {
                final SiteTaskerRunnable r = new SiteTasker.SiteTaskerRunnable() {
                    @Override
                    void run() {
                        synchronized (m_lock) {
                            for (Object o : durableThings) {
                                m_pendingTasks.offer((TransactionTask)o);

                                // Make sure all queued tasks for this MP txn are released
                                if (!((TransactionTask) o).getTransactionState().isSinglePartition()) {
                                    offerPendingMPTasks(((TransactionTask) o).getTxnId());
                                }
                            }
                        }
                    }
                };
                if (InitiatorMailbox.SCHEDULE_IN_SITE_THREAD) {
                    m_tasks.offer(r);
                } else {
                    r.run();
                }
            }
        };
        m_uniqueIdGenerator = new UniqueIdGenerator(partitionId, 0);
    }

    @Override
    public void setLeaderState(boolean isLeader)
    {
        super.setLeaderState(isLeader);
        m_snapMonitor.addInterest(this);
    }

    @Override
    public void setMaxSeenTxnId(long maxSeenTxnId)
    {
        super.setMaxSeenTxnId(maxSeenTxnId);
        writeIv2ViableReplayEntry();
    }

    public void setDRGateway(PartitionDRGateway gateway)
    {
        m_drGateway = gateway;
        if (m_drGateway != null) {
            // Schedules to be fired every 5ms
            VoltDB.instance().schedulePriorityWork(new Runnable() {
                @Override
                public void run() {
                    // Send a DR task to the site
                    m_tasks.offer(new DRTask(m_drGateway));
                }
            }, 0, 5, TimeUnit.MILLISECONDS);
        }
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
    public void updateReplicas(List<Long> replicas, Map<Integer, Long> partitionMasters)
    {
        // First - correct the official replica set.
        m_replicaHSIds = replicas;
        // Update the list of remote replicas that we'll need to send to
        List<Long> sendToHSIds = new ArrayList<Long>(m_replicaHSIds);
        sendToHSIds.remove(m_mailbox.getHSId());
        m_sendToHSIds = Longs.toArray(sendToHSIds);

        // Cleanup duplicate counters and collect DONE counters
        // in this list for further processing.
        List<DuplicateCounterKey> doneCounters = new LinkedList<DuplicateCounterKey>();
        for (Entry<DuplicateCounterKey, DuplicateCounter> entry : m_duplicateCounters.entrySet()) {
            DuplicateCounter counter = entry.getValue();
            int result = counter.updateReplicas(m_replicaHSIds);
            if (result == DuplicateCounter.DONE) {
                doneCounters.add(entry.getKey());
            }
        }

        // Maintain the CI invariant that responses arrive in txnid order.
        Collections.sort(doneCounters);
        for (DuplicateCounterKey key : doneCounters) {
            DuplicateCounter counter = m_duplicateCounters.remove(key);
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
        writeIv2ViableReplayEntry();
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
        long sequenceWithTxnId = Long.MIN_VALUE;

        boolean commandLog = (message instanceof TransactionInfoBaseMessage &&
                (((TransactionInfoBaseMessage)message).isForReplay()));

        boolean dr = ((message instanceof TransactionInfoBaseMessage &&
                ((TransactionInfoBaseMessage)message).isForDR()));

        boolean sentinel = message instanceof MultiPartitionParticipantMessage;

        boolean replay = commandLog || sentinel || dr;
        boolean sequenceForReplay = m_isLeader && replay;

        assert(!(commandLog && dr));

        if (commandLog || sentinel) {
            sequenceWithTxnId = ((TransactionInfoBaseMessage)message).getTxnId();
        }
        else if (dr) {
            sequenceWithTxnId = ((TransactionInfoBaseMessage)message).getOriginalTxnId();
        }

        if (sequenceForReplay) {
            InitiateResponseMessage dupe = m_replaySequencer.dedupe(sequenceWithTxnId,
                    (TransactionInfoBaseMessage) message);
            if (dupe != null) {
                // Duplicate initiate task message, send response
                m_mailbox.send(dupe.getInitiatorHSId(), dupe);
            }
            else if (!m_replaySequencer.offer(sequenceWithTxnId, (TransactionInfoBaseMessage) message)) {
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
                // Update last seen and last polled txnId for replicas
                m_replaySequencer.updateLastSeenTxnId(sequenceWithTxnId,
                        (TransactionInfoBaseMessage) message);
                m_replaySequencer.updateLastPolledTxnId(sequenceWithTxnId,
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
        else if (message instanceof BorrowTaskMessage) {
            handleBorrowTaskMessage((BorrowTaskMessage)message);
        }
        else if (message instanceof Iv2LogFaultMessage) {
            handleIv2LogFaultMessage((Iv2LogFaultMessage)message);
        }
        else if (message instanceof DumpMessage) {
            handleDumpMessage();
        }
        else {
            throw new RuntimeException("UNKNOWN MESSAGE TYPE, BOOM!");
        }
    }

    private long getMaxTaskedSpHandle() {
        return m_pendingTasks.getMaxTaskedSpHandle();
    }

    // SpScheduler expects to see InitiateTaskMessages corresponding to single-partition
    // procedures only.
    public void handleIv2InitiateTaskMessage(Iv2InitiateTaskMessage message)
    {
        if (!message.isSinglePartition()) {
            throw new RuntimeException("SpScheduler.handleIv2InitiateTaskMessage " +
                    "should never receive multi-partition initiations.");
        }

        final String procedureName = message.getStoredProcedureName();
        long newSpHandle;
        long uniqueId = Long.MIN_VALUE;
        Iv2InitiateTaskMessage msg = message;
        if (m_isLeader || message.isReadOnly()) {
            /*
             * A short circuit read is a read where the client interface is local to
             * this node. The CI will let a replica perform a read in this case and
             * it does looser tracking of client handles since it can't be
             * partitioned from the local replica.
             */
            if (!m_isLeader &&
                    CoreUtils.getHostIdFromHSId(msg.getInitiatorHSId()) !=
                    CoreUtils.getHostIdFromHSId(m_mailbox.getHSId())) {
                VoltDB.crashLocalVoltDB("Only allowed to do short circuit reads locally", true, null);
                    }

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
            } else if (message.isForDR()) {
                uniqueId = message.getStoredProcedureInvocation().getOriginalUniqueId();
                // @LoadSinglepartitionTable does not have a valid uid
                if (UniqueIdGenerator.getPartitionIdFromUniqueId(uniqueId) == m_partitionId) {
                    m_uniqueIdGenerator.updateMostRecentlyGeneratedUniqueId(uniqueId);
                }
            }

            /*
             * If this is CL replay use the txnid from the CL and also
             * update the txnid to match the one from the CL
             */
            if (message.isForReplay()) {
                newSpHandle = message.getTxnId();
                setMaxSeenTxnId(newSpHandle);
            } else if (m_isLeader && !message.isReadOnly()) {
                TxnEgo ego = advanceTxnEgo();
                newSpHandle = ego.getTxnId();
                uniqueId = m_uniqueIdGenerator.getNextUniqueId();
            } else {
                /*
                 * The short circuit read case. Since we are not a master
                 * we can't create new transaction IDs, so reuse the last seen
                 * txnid. For a timestamp, might as well give a reasonable one
                 * for a read heavy workload so time isn't bursty.
                 */
                uniqueId = UniqueIdGenerator.makeIdFromComponents(
                        Math.max(System.currentTimeMillis(), m_uniqueIdGenerator.lastUsedTime),
                        0,
                        m_uniqueIdGenerator.partitionId);
                //Don't think it wise to make a new one for a short circuit read
                newSpHandle = getMaxTaskedSpHandle();
            }

            // Need to set the SP handle on the received message
            // Need to copy this or the other local sites handling
            // the same initiate task message will overwrite each
            // other's memory -- the message isn't copied on delivery
            // to other local mailboxes.
            msg = new Iv2InitiateTaskMessage(
                    message.getInitiatorHSId(),
                    message.getCoordinatorHSId(),
                    m_repairLogTruncationHandle,
                    message.getTxnId(),
                    message.getUniqueId(),
                    message.isReadOnly(),
                    message.isSinglePartition(),
                    message.getStoredProcedureInvocation(),
                    message.getClientInterfaceHandle(),
                    message.getConnectionId(),
                    message.isForReplay());

            msg.setSpHandle(newSpHandle);

            // Also, if this is a vanilla single-part procedure, make the TXNID
            // be the SpHandle (for now)
            // Only system procedures are every-site, so we'll check through the SystemProcedureCatalog
            if (SystemProcedureCatalog.listing.get(procedureName) == null ||
                    !SystemProcedureCatalog.listing.get(procedureName).getEverysite()) {
                msg.setTxnId(newSpHandle);
                msg.setUniqueId(uniqueId);
                    }

            //Don't replicate reads, this really assumes that DML validation
            //is going to be integrated soonish
            if (m_isLeader && !msg.isReadOnly() && m_sendToHSIds.length > 0) {
                Iv2InitiateTaskMessage replmsg =
                    new Iv2InitiateTaskMessage(m_mailbox.getHSId(),
                            m_mailbox.getHSId(),
                            m_repairLogTruncationHandle,
                            msg.getTxnId(),
                            msg.getUniqueId(),
                            msg.isReadOnly(),
                            msg.isSinglePartition(),
                            msg.getStoredProcedureInvocation(),
                            msg.getClientInterfaceHandle(),
                            msg.getConnectionId(),
                            msg.isForReplay());
                // Update the handle in the copy since the constructor doesn't set it
                replmsg.setSpHandle(newSpHandle);
                m_mailbox.send(m_sendToHSIds, replmsg);
                DuplicateCounter counter = new DuplicateCounter(
                        msg.getInitiatorHSId(),
                        msg.getTxnId(), m_replicaHSIds, msg.getStoredProcedureName());
                m_duplicateCounters.put(new DuplicateCounterKey(msg.getTxnId(), newSpHandle), counter);
            }
        }
        else {
            setMaxSeenTxnId(msg.getSpHandle());
            newSpHandle = msg.getSpHandle();
            uniqueId = msg.getUniqueId();
        }
        Iv2Trace.logIv2InitiateTaskMessage(message, m_mailbox.getHSId(), msg.getTxnId(), newSpHandle);
        doLocalInitiateOffer(msg);
        return;
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
        final String procedureName = msg.getStoredProcedureName();
        final SpProcedureTask task =
            new SpProcedureTask(m_mailbox, procedureName, m_pendingTasks, msg, m_drGateway);
        if (!msg.isReadOnly()) {
            ListenableFuture<Object> durabilityBackpressureFuture =
                    m_cl.log(msg, msg.getSpHandle(), null, m_durabilityListener, task);
            //Durability future is always null for sync command logging
            //the transaction will be delivered again by the CL for execution once durable
            //Async command logging has to offer the task immediately with a Future for backpressure
            if (durabilityBackpressureFuture != null) {
                m_pendingTasks.offer(task.setDurabilityBackpressureFuture(durabilityBackpressureFuture));
            }
        } else {
            m_pendingTasks.offer(task);
        }
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

        // We currently send the final response into the ether, since we don't
        // have the original ClientInterface HSID stored.  It would be more
        // useful to have the original ClienInterface HSId somewhere handy.

        List<Long> expectedHSIds = new ArrayList<Long>(needsRepair);
        DuplicateCounter counter = new DuplicateCounter(
                HostMessenger.VALHALLA,
                message.getTxnId(), expectedHSIds, message.getStoredProcedureName());
        m_duplicateCounters.put(new DuplicateCounterKey(message.getTxnId(), message.getSpHandle()), counter);

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
            Iv2InitiateTaskMessage replmsg =
                new Iv2InitiateTaskMessage(m_mailbox.getHSId(), m_mailbox.getHSId(), message);
            m_mailbox.send(com.google_voltpatches.common.primitives.Longs.toArray(needsRepair), replmsg);
        }
    }

    private void handleFragmentTaskMessageRepair(List<Long> needsRepair, FragmentTaskMessage message)
    {
        // set up duplicate counter. expect exactly the responses corresponding
        // to needsRepair. These may, or may not, include the local site.

        List<Long> expectedHSIds = new ArrayList<Long>(needsRepair);
        DuplicateCounter counter = new DuplicateCounter(
                message.getCoordinatorHSId(), // Assume that the MPI's HSID hasn't changed
                message.getTxnId(), expectedHSIds, "MP_DETERMINISM_ERROR");
        m_duplicateCounters.put(new DuplicateCounterKey(message.getTxnId(), message.getSpHandle()), counter);

        // is local repair necessary?
        if (needsRepair.contains(m_mailbox.getHSId())) {
            // Sanity check that we really need repair.
            if (m_outstandingTxns.get(message.getTxnId()) != null) {
                hostLog.warn("SPI repair attempted to repair a fragment which it has already seen. " +
                        "This shouldn't be possible.");
                // Not sure what to do in this event.  Crash for now
                throw new RuntimeException("Attempted to repair with a fragment we've already seen.");
            }
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
            m_mailbox.send(com.google_voltpatches.common.primitives.Longs.toArray(needsRepair), replmsg);
        }
    }

    // Pass a response through the duplicate counters.
    public void handleInitiateResponseMessage(InitiateResponseMessage message)
    {
        // All single-partition reads are short-circuit reads and will have no duplicate counter.
        // SpScheduler will only see InitiateResponseMessages for SP transactions, so if it's
        // read-only here, it's short-circuited.  Avoid all the lookup below.  Also, don't update
        // the truncation handle, since it won't have meaning for anyone.
        if (message.isReadOnly()) {
            // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
            m_mailbox.send(message.getInitiatorHSId(), message);
            return;
        }

        final long spHandle = message.getSpHandle();
        final DuplicateCounterKey dcKey = new DuplicateCounterKey(message.getTxnId(), spHandle);
        DuplicateCounter counter = m_duplicateCounters.get(dcKey);
        if (counter != null) {
            int result = counter.offer(message);
            if (result == DuplicateCounter.DONE) {
                m_duplicateCounters.remove(dcKey);
                m_repairLogTruncationHandle = spHandle;
                m_mailbox.send(counter.m_destinationId, counter.getLastResponse());
            }
            else if (result == DuplicateCounter.MISMATCH) {
                VoltDB.crashGlobalVoltDB("HASH MISMATCH: replicas produced different results.", true, null);
            }
        }
        else {
            // the initiatorHSId is the ClientInterface mailbox. Yeah. I know.
            m_repairLogTruncationHandle = spHandle;
            m_mailbox.send(message.getInitiatorHSId(), message);
        }
    }

    // BorrowTaskMessages encapsulate a FragmentTaskMessage along with
    // input dependency tables. The MPI issues borrows to a local site
    // to perform replicated reads or aggregation fragment work.
    private void handleBorrowTaskMessage(BorrowTaskMessage message) {
        // borrows do not advance the sp handle. The handle would
        // move backwards anyway once the next message is received
        // from the SP leader.
        long newSpHandle = getMaxTaskedSpHandle();
        Iv2Trace.logFragmentTaskMessage(message.getFragmentTaskMessage(),
                m_mailbox.getHSId(), newSpHandle, true);
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


        if (message.getFragmentTaskMessage().isSysProcTask()) {
            final SysprocFragmentTask task =
                new SysprocFragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                                        m_pendingTasks, message.getFragmentTaskMessage(),
                                        message.getInputDepMap());
            m_pendingTasks.offer(task);
        }
        else {
            final FragmentTask task =
                new FragmentTask(m_mailbox, (ParticipantTransactionState)txn,
                        m_pendingTasks, message.getFragmentTaskMessage(),
                        message.getInputDepMap());
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
        if (m_isLeader) {
            // Quick hack to make progress...we need to copy the FragmentTaskMessage
            // before we start mucking with its state (SPHANDLE).  We need to revisit
            // all the messaging mess at some point.
            msg = new FragmentTaskMessage(message.getInitiatorHSId(),
                    message.getCoordinatorHSId(), message);
            //Not going to use the timestamp from the new Ego because the multi-part timestamp is what should be used
            if (!message.isReadOnly()) {
                TxnEgo ego = advanceTxnEgo();
                newSpHandle = ego.getTxnId();
            } else {
                newSpHandle = getMaxTaskedSpHandle();
            }
            msg.setSpHandle(newSpHandle);
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
            if (m_sendToHSIds.length > 0 && (!msg.isReadOnly() || msg.isSysProcTask())) {
                FragmentTaskMessage replmsg =
                    new FragmentTaskMessage(m_mailbox.getHSId(),
                            m_mailbox.getHSId(), msg);
                m_mailbox.send(m_sendToHSIds,
                        replmsg);
                DuplicateCounter counter;
                /*
                 * Non-determinism should be impossible to happen with MP fragments.
                 * if you see "MP_DETERMINISM_ERROR" as procedure name in the crash logs
                 * something has horribly gone wrong.
                 */
                if (message.getFragmentTaskType() != FragmentTaskMessage.SYS_PROC_PER_SITE) {
                    counter = new DuplicateCounter(
                            msg.getCoordinatorHSId(),
                            msg.getTxnId(), m_replicaHSIds, "MP_DETERMINISM_ERROR");
                }
                else {
                    counter = new SysProcDuplicateCounter(
                            msg.getCoordinatorHSId(),
                            msg.getTxnId(), m_replicaHSIds, "MP_DETERMINISM_ERROR");
                }
                m_duplicateCounters.put(new DuplicateCounterKey(msg.getTxnId(), newSpHandle), counter);
            }
        }
        else {
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
        TransactionState txn = m_outstandingTxns.get(msg.getTxnId());
        boolean logThis = false;
        // bit of a hack...we will probably not want to create and
        // offer FragmentTasks for txn ids that don't match if we have
        // something in progress already
        if (txn == null) {
            txn = new ParticipantTransactionState(msg.getSpHandle(), msg);
            m_outstandingTxns.put(msg.getTxnId(), txn);
            // Only want to send things to the command log if it satisfies this predicate
            // AND we've never seen anything for this transaction before.  We can't
            // actually log until we create a TransactionTask, though, so just keep track
            // of whether it needs to be done.
            logThis = (msg.getInitiateTask() != null && !msg.getInitiateTask().isReadOnly());
        }

        // Check to see if this is the final task for this txn, and if so, if we can close it out early
        // Right now, this just means read-only.
        // NOTE: this overlaps slightly with CompleteTransactionMessage handling completion.  It's so tiny
        // that for now, meh, but if this scope grows then it should get refactored out
        if (msg.isFinalTask() && txn.isReadOnly()) {
            m_outstandingTxns.remove(msg.getTxnId());
        }

        TransactionTask task;
        if (msg.isSysProcTask()) {
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
                    m_cl.log(msg.getInitiateTask(), msg.getSpHandle(), Ints.toArray(msg.getInvolvedPartitions()),
                             m_durabilityListener, task);
            //Durability future is always null for sync command logging
            //the transaction will be delivered again by the CL for execution once durable
            //Async command logging has to offer the task immediately with a Future for backpressure
            if (durabilityBackpressureFuture != null) {
                m_pendingTasks.offer(task.setDurabilityBackpressureFuture(durabilityBackpressureFuture));
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
            }
        } else {
            queueOrOfferMPTask(task);
        }
    }

    /**
     * Offer all fragment tasks and complete transaction tasks queued for durability for the given
     * MP transaction, and remove the entry from the pending map so that future ones won't be
     * queued.
     *
     * @param txnId    The MP transaction ID.
     */
    private void offerPendingMPTasks(long txnId)
    {
        Queue<TransactionTask> pendingTasks = m_mpsPendingDurability.get(txnId);
        if (pendingTasks != null) {
            for (TransactionTask task : pendingTasks) {
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

    // Eventually, the master for a partition set will need to be able to dedupe
    // FragmentResponses from its replicas.
    public void handleFragmentResponseMessage(FragmentResponseMessage message)
    {
        // Send the message to the duplicate counter, if any
        DuplicateCounter counter =
            m_duplicateCounters.get(new DuplicateCounterKey(message.getTxnId(), message.getSpHandle()));
        if (counter != null) {
            int result = counter.offer(message);
            if (result == DuplicateCounter.DONE) {
                m_duplicateCounters.remove(new DuplicateCounterKey(message.getTxnId(), message.getSpHandle()));
                m_repairLogTruncationHandle = message.getSpHandle();
                FragmentResponseMessage resp = (FragmentResponseMessage)counter.getLastResponse();
                // MPI is tracking deps per partition HSID.  We need to make
                // sure we write ours into the message getting sent to the MPI
                resp.setExecutorSiteId(m_mailbox.getHSId());
                m_mailbox.send(counter.m_destinationId, resp);
            }
            else if (result == DuplicateCounter.MISMATCH) {
                VoltDB.crashGlobalVoltDB("HASH MISMATCH running multi-part procedure.", true, null);
            }
            // doing duplicate suppresion: all done.
            return;
        }

        m_mailbox.send(message.getDestinationSiteId(), message);
    }

    public void handleCompleteTransactionMessage(CompleteTransactionMessage message)
    {
        CompleteTransactionMessage msg = message;
        if (m_isLeader) {
            msg = new CompleteTransactionMessage(message);
            // Set the spHandle so that on repair the new master will set the max seen spHandle
            // correctly
            if (!msg.isForReplay()) advanceTxnEgo();
            msg.setSpHandle(getCurrentTxnId());
            if (m_sendToHSIds.length > 0) {
                m_mailbox.send(m_sendToHSIds, msg);
            }
        } else {
            setMaxSeenTxnId(msg.getSpHandle());
        }
        TransactionState txn = m_outstandingTxns.get(msg.getTxnId());
        // We can currently receive CompleteTransactionMessages for multipart procedures
        // which only use the buddy site (replicated table read).  Ignore them for
        // now, fix that later.
        if (txn != null)
        {
            Iv2Trace.logCompleteTransactionMessage(msg, m_mailbox.getHSId());
            final CompleteTransactionTask task =
                new CompleteTransactionTask(txn, m_pendingTasks, msg, m_drGateway);
            queueOrOfferMPTask(task);
            // If this is a restart, then we need to leave the transaction state around
            if (!msg.isRestart()) {
                m_outstandingTxns.remove(msg.getTxnId());
            }
        }
    }

    public void handleIv2LogFaultMessage(Iv2LogFaultMessage message)
    {
        // Should only receive these messages at replicas, call the internal log write with
        // the provided SP handle
        writeIv2ViableReplayEntryInternal(message.getSpHandle());
        setMaxSeenTxnId(message.getSpHandle());
    }

    public void handleDumpMessage()
    {
        String who = CoreUtils.hsIdToString(m_mailbox.getHSId());
        hostLog.warn("State dump for site: " + who);
        hostLog.warn("" + who + ": partition: " + m_partitionId + ", isLeader: " + m_isLeader);
        if (m_isLeader) {
            hostLog.warn("" + who + ": replicas: " + CoreUtils.hsIdCollectionToString(m_replicaHSIds));
            if (m_sendToHSIds.length > 0) {
                m_mailbox.send(m_sendToHSIds, new DumpMessage());
            }
        }
        hostLog.warn("" + who + ": most recent SP handle: " + getCurrentTxnId() + " " +
                TxnEgo.txnIdToString(getCurrentTxnId()));
        hostLog.warn("" + who + ": outstanding txns: " + m_outstandingTxns.keySet() + " " +
                TxnEgo.txnIdCollectionToString(m_outstandingTxns.keySet()));
        hostLog.warn("" + who + ": TransactionTaskQueue: " + m_pendingTasks.toString());
        if (m_duplicateCounters.size() > 0) {
            hostLog.warn("" + who + ": duplicate counters: ");
            for (Entry<DuplicateCounterKey, DuplicateCounter> e : m_duplicateCounters.entrySet()) {
                hostLog.warn("\t" + who + ": " + e.getKey().toString() + ": " + e.getValue().toString());
            }
        }
    }

    @Override
    public void setCommandLog(CommandLog cl) {
        m_cl = cl;
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
     */
    void writeIv2ViableReplayEntry()
    {
        if (m_replayComplete) {
            if (m_isLeader) {
                // write the viable set locally
                long faultSpHandle = advanceTxnEgo().getTxnId();
                writeIv2ViableReplayEntryInternal(faultSpHandle);
                // Generate Iv2LogFault message and send it to replicas
                Iv2LogFaultMessage faultMsg = new Iv2LogFaultMessage(faultSpHandle);
                m_mailbox.send(m_sendToHSIds,
                        faultMsg);
            }
        }
    }

    /**
     * Write the viable replay set to the command log with the provided SP Handle
     */
    void writeIv2ViableReplayEntryInternal(long spHandle)
    {
        if (m_replayComplete) {
            m_cl.logIv2Fault(m_mailbox.getHSId(), new HashSet<Long>(m_replicaHSIds), m_partitionId,
                    spHandle);
        }
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

    @Override
    public void dump()
    {
        m_replaySequencer.dump(m_mailbox.getHSId());
        tmLog.info(String.format("%s: %s", CoreUtils.hsIdToString(m_mailbox.getHSId()), m_pendingTasks));
    }
}
