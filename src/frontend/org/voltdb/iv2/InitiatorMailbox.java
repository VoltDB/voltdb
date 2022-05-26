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
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.apache.zookeeper_voltpatches.ZooKeeper;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.RealVoltDB;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.dtxn.TransactionState;
import org.voltdb.exceptions.TransactionRestartException;
import org.voltdb.iv2.SpInitiator.ServiceState;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.DummyTransactionTaskMessage;
import org.voltdb.messaging.DumpMessage;
import org.voltdb.messaging.FragmentResponseMessage;
import org.voltdb.messaging.FragmentTaskMessage;
import org.voltdb.messaging.HashMismatchMessage;
import org.voltdb.messaging.InitiateResponseMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;
import org.voltdb.messaging.MigratePartitionLeaderMessage;
import org.voltdb.messaging.RejoinMessage;
import org.voltdb.messaging.RepairLogTruncationMessage;

import com.google_voltpatches.common.base.Supplier;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 *
 * If you add public synchronized methods that will be used on the MpInitiator then
 * you need to override them in MpInitiator mailbox so that they
 * occur in the correct thread instead of using synchronization
 */
public class InitiatorMailbox implements Mailbox
{
    static final boolean LOG_TX = false;
    public static final boolean SCHEDULE_IN_SITE_THREAD;
    static {
        SCHEDULE_IN_SITE_THREAD = Boolean.valueOf(System.getProperty("SCHEDULE_IN_SITE_THREAD", "true"));
    }

    public static enum LeaderMigrationState {
        STARTED(1),            //@MigratePartitionLeader on old master has been started
        TXN_RESTART(2),        //new master needs txn restart before old master drains txns
        TXN_DRAINED(3),        //new master is notified that old master has drained
        NONE(4);                //no or complete MigratePartitionLeader
        final int state;
        LeaderMigrationState(int s) {
            this.state = s;
        }
        public int get() {
            return state;
        }
    }

    final VoltLogger hostLog = new VoltLogger("HOST");
    final VoltLogger tmLog = new VoltLogger("TM");

    protected final int m_partitionId;
    protected final Scheduler m_scheduler;
    protected final HostMessenger m_messenger;
    protected final RepairLog m_repairLog;
    private final JoinProducerBase m_joinProducer;
    private final LeaderCacheReader m_masterLeaderCache;
    private long m_hsId;
    protected RepairAlgo m_algo;

    //Queue all the transactions on the new master after MigratePartitionLeader till it receives a message
    //from its older master which has drained all the transactions.
    private AtomicLong m_newLeaderHSID = new AtomicLong(Long.MIN_VALUE);
    private AtomicReference<LeaderMigrationState> m_leaderMigrationState =
            new AtomicReference<LeaderMigrationState>();

    private final Object m_leaderMigrationStateLock = new Object();

    // Both message delivery and initiator promotion threads can update migration status.
    // The flag is used to help to set the status correctly when the status is first updated by the delivery thread
    // followed by the initiator promotion thread.
    private volatile boolean m_leaderPromotedUponMigration = false;
    /*
     * Hacky global map of initiator mailboxes to support assertions
     * that verify the locking is kosher
     */
    public static final CopyOnWriteArrayList<InitiatorMailbox> m_allInitiatorMailboxes
                                                                         = new CopyOnWriteArrayList<InitiatorMailbox>();

    synchronized public void setLeaderState(long maxSeenTxnId)
    {
        setLeaderStateInternal(maxSeenTxnId);
    }

    public synchronized void setMaxLastSeenMultipartTxnId(long txnId) {
        setMaxLastSeenMultipartTxnIdInternal(txnId);
    }


    synchronized public void setMaxLastSeenTxnId(long txnId) {
        setMaxLastSeenTxnIdInternal(txnId);
    }

    synchronized public void enableWritingIv2FaultLog() {
        enableWritingIv2FaultLogInternal();
    }

    synchronized public RepairAlgo constructRepairAlgo(Supplier<List<Long>> survivors, int deadHost, String whoami, boolean isMigratePartitionLeader) {
        RepairAlgo ra = new SpPromoteAlgo(survivors.get(), deadHost, this, whoami, m_partitionId, isMigratePartitionLeader);
        if (hostLog.isDebugEnabled()) {
            hostLog.debug("[InitiatorMailbox:constructRepairAlgo] whoami: " + whoami + ", partitionId: " +
                    m_partitionId + ", survivors: " + CoreUtils.hsIdCollectionToString(survivors.get()));
        }
        setRepairAlgoInternal(ra);
        return ra;
    }

    protected void setRepairAlgoInternal(RepairAlgo algo)
    {
        assert(lockingVows());
        m_algo = algo;
    }

    protected void setLeaderStateInternal(long maxSeenTxnId)
    {
        assert(lockingVows());
        m_repairLog.setLeaderState(true);
        m_scheduler.setLeaderState(true);
        m_scheduler.setMaxSeenTxnId(maxSeenTxnId);

        // After SP leader promotion, a DummyTransactionTaskMessage is generated from the new leader.
        // This READ ONLY message will serve as a synchronization point on all replicas of this
        // partition, like normal SP write transaction that has to finish executing on all replicas.
        // In this way, the leader can make sure all replicas have finished replaying
        // all their repair logs entries.
        // From now on, the new leader is safe to accept new transactions. See ENG-11110.
        // Deliver here is to make sure it's the first message on the new leader.
        // On MP scheduler, this DummyTransactionTaskMessage will be ignored.
        deliver(new DummyTransactionTaskMessage());
    }

    protected void setMaxLastSeenMultipartTxnIdInternal(long txnId) {
        assert(lockingVows());
        m_repairLog.m_lastMpHandle = txnId;
    }


    protected void setMaxLastSeenTxnIdInternal(long txnId) {
        assert(lockingVows());
        m_scheduler.setMaxSeenTxnId(txnId);
    }

    protected void enableWritingIv2FaultLogInternal() {
        assert(lockingVows());
        m_scheduler.enableWritingIv2FaultLog();
    }

    public InitiatorMailbox(int partitionId,
            Scheduler scheduler,
            HostMessenger messenger, RepairLog repairLog,
            JoinProducerBase joinProducer)
    {
        m_partitionId = partitionId;
        m_scheduler = scheduler;
        m_messenger = messenger;
        m_repairLog = repairLog;
        m_joinProducer = joinProducer;

        m_masterLeaderCache = new LeaderCache(m_messenger.getZK(),
                "InitiatorMailbox-masterLeaderCache-" + m_partitionId, VoltZK.iv2masters);
        try {
            m_masterLeaderCache.start(false);
        } catch (InterruptedException ignored) {
            // not blocking. shouldn't interrupt.
        } catch (ExecutionException crashme) {
            // this on the other hand seems tragic.
            VoltDB.crashLocalVoltDB("Error constructiong InitiatorMailbox.", false, crashme);
        }

        /*
         * Leaking this from a constructor, real classy.
         * Only used for an assertion on locking.
         */
        m_allInitiatorMailboxes.add(this);
        m_leaderMigrationState.set(LeaderMigrationState.NONE);
    }

    public JoinProducerBase getJoinProducer()
    {
        return m_joinProducer;
    }

    // enforce restriction on not allowing promotion during rejoin.
    public boolean acceptPromotion()
    {
        return m_joinProducer == null || m_joinProducer.acceptPromotion();
    }

    /*
     * Thou shalt not lock two initiator mailboxes from the same thread, lest ye be deadlocked.
     */
    public static boolean lockingVows() {
        List<InitiatorMailbox> lockedMailboxes = new ArrayList<InitiatorMailbox>();
        for (InitiatorMailbox im : m_allInitiatorMailboxes) {
            if (im.m_scheduler != null && im.m_scheduler instanceof SpScheduler) {
                if (((SpScheduler)im.m_scheduler).getServiceState().isRemoved()) {
                    continue;
                }
            }
            if (Thread.holdsLock(im)) {
                lockedMailboxes.add(im);
            }
        }
        if (lockedMailboxes.size() > 1) {
            String msg = "Unexpected concurrency error, a thread locked two initiator mailboxes. ";
            msg += "Mailboxes for site id/partition ids ";
            boolean first = true;
            for (InitiatorMailbox m : lockedMailboxes) {
                msg += CoreUtils.hsIdToString(m.m_hsId) + "/" + m.m_partitionId;
                if (!first) {
                    msg += ", ";
                }
                first = false;
            }
            VoltDB.crashLocalVoltDB(msg, true, null);
        }
        return true;
    }

    synchronized public void shutdown() throws InterruptedException
    {
        shutdownInternal();
    }

    protected void shutdownInternal() throws InterruptedException {
        assert(lockingVows());
        m_masterLeaderCache.shutdown();
        if (m_algo != null) {
            m_algo.cancel();
        }
        m_scheduler.shutdown();
    }

    // Change the replica set configuration (during or after promotion)
    public synchronized long[] updateReplicas(List<Long> replicas, Map<Integer, Long> partitionMasters) {
        return updateReplicasInternal(replicas, partitionMasters, null);
    }

    public synchronized long[] updateReplicas(List<Long> replicas, Map<Integer, Long> partitionMasters,
            TransactionState snapshotTransactionState)
    {
        return updateReplicasInternal(replicas, partitionMasters, snapshotTransactionState);
    }

    protected long[] updateReplicasInternal(List<Long> replicas, Map<Integer, Long> partitionMasters,
            TransactionState snapshotTransactionState) {
        assert(lockingVows());
        Iv2Trace.logTopology(getHSId(), replicas, m_partitionId);
        // If a replica set has been configured and it changed during
        // promotion, must cancel the term
        if (m_algo != null) {
            m_algo.cancel();
        }
        return m_scheduler.updateReplicas(replicas, partitionMasters, snapshotTransactionState);
    }

    public long getMasterHsId(int partitionId)
    {
        long masterHSId = m_masterLeaderCache.get(partitionId);
        return masterHSId;
    }

    @Override
    public void send(long destHSId, VoltMessage message)
    {
        logTxMessage(message);
        message.m_sourceHSId = this.m_hsId;
        m_messenger.send(destHSId, message);
    }

    @Override
    public void send(long[] destHSIds, VoltMessage message)
    {
        logTxMessage(message);
        message.m_sourceHSId = this.m_hsId;
        m_messenger.send(destHSIds, message);
    }

    @Override
    public void deliver(final VoltMessage message)
    {
        if (SCHEDULE_IN_SITE_THREAD) {
            SiteTasker.SiteTaskerRunnable task = new SiteTasker.SiteTaskerRunnable() {
                @Override
                void run() {
                    synchronized (InitiatorMailbox.this) {
                        deliverInternal(message);
                    }
                }
                private SiteTaskerRunnable init(VoltMessage message) {
                    if (message instanceof Iv2InitiateTaskMessage && ((Iv2InitiateTaskMessage) message).getStoredProcedureInvocation() != null) {
                        setPriority(((Iv2InitiateTaskMessage) message).getStoredProcedureInvocation().getRequestPriority());
                    }
                    return this;
                }
            }.init(message);

            Iv2Trace.logSiteTaskerQueueOffer(task, message, m_hsId, hostLog.isDebugEnabled());
            m_scheduler.getQueue().offer(task);
        } else {
            synchronized (this) {
                deliverInternal(message);
            }
        }
    }

    protected void deliverInternal(VoltMessage message) {
        assert(lockingVows());
        logRxMessage(message);
        boolean canDeliver = m_scheduler.sequenceForReplay(message);
        if (message instanceof Iv2InitiateTaskMessage) {
            if (checkMisroutedIv2IntiateTaskMessage((Iv2InitiateTaskMessage)message)) {
                return;
            }
            initiateSPIMigrationIfRequested((Iv2InitiateTaskMessage)message);
        } else if (message instanceof FragmentTaskMessage) {
            if (checkMisroutedFragmentTaskMessage((FragmentTaskMessage)message)) {
                return;
            }
        } else if (message instanceof DumpMessage) {
            hostLog.warn("Received DumpMessage at " + CoreUtils.hsIdToString(m_hsId));
            try {
                m_scheduler.dump();
            } catch (Throwable ignore) {
                hostLog.warn("Failed to dump the content of the scheduler", ignore);
            }
        } else if (message instanceof Iv2RepairLogRequestMessage) {
            handleLogRequest(message);
            return;
        } else if (message instanceof Iv2RepairLogResponseMessage) {
            m_algo.deliver(message);
            return;
        } else if (message instanceof RejoinMessage) {
            m_joinProducer.deliver((RejoinMessage) message);
            return;
        } else if (message instanceof RepairLogTruncationMessage) {
            m_repairLog.deliver(message);
            return;
        } else if (message instanceof MigratePartitionLeaderMessage) {
            setLeaderMigrationState((MigratePartitionLeaderMessage)message);
            return;
        } else if (message instanceof HashMismatchMessage) {
            updateServiceState();
            return;
        }

        if (canDeliver) {
            //For a message delivered to partition leaders, the message may not have the updated transaction id yet.
            //The scheduler of partition leader will advance the transaction id, update the message and add it to repair log.
            //so that the partition leader and replicas have the consistent items in their repair logs.
            m_scheduler.deliver(message);
        } else {
            m_repairLog.deliver(message);
        }
    }

    // If @MigratePartitionLeader comes in, set up new partition leader selection and
    // mark this site as non-leader. All the transactions (sp and mp) which are sent to partition leader will be
    // rerouted from this moment on until the transactions are correctly routed to new leader.
    private void initiateSPIMigrationIfRequested(Iv2InitiateTaskMessage msg) {
        if (!"@MigratePartitionLeader".equals(msg.getStoredProcedureName())) {
            return;
        }

        final Object[] params = msg.getParameters();
        int pid = Integer.parseInt(params[1].toString());
        if (pid != m_partitionId) {
            tmLog.warn(String.format("@MigratePartitionLeader executed at a wrong partition %d for partition %d.", m_partitionId, pid));
            return;
        }

        RealVoltDB db = (RealVoltDB)VoltDB.instance();
        int hostId = Integer.parseInt(params[2].toString());
        Long newLeaderHSId = db.getCartographer().getHSIDForPartitionHost(hostId, pid);
        if (newLeaderHSId == null || newLeaderHSId == m_hsId) {
            tmLog.warn(String.format("@MigratePartitionLeader the partition leader is already on the host %d or the host id is invalid.", hostId));
            return;
        }


        LeaderCache leaderAppointee = new LeaderCache(m_messenger.getZK(),
                "initiateSPIMigrationIfRequested-" + m_partitionId, VoltZK.iv2appointees);
        try {
            leaderAppointee.start(true);
            if (!m_messenger.getLiveHostIds().contains(hostId)) {
                tmLog.info("Can not move partition leader to dead host: " + CoreUtils.hsIdToString(newLeaderHSId));
                return;
            }
            SpScheduler scheduler = (SpScheduler)m_scheduler;
            scheduler.checkPointMigratePartitionLeader();
            scheduler.m_isLeader = false;
            m_newLeaderHSID.set(newLeaderHSId);
            m_leaderMigrationState.set(LeaderMigrationState.STARTED);

            leaderAppointee.put(pid, LeaderCache.suffixHSIdsWithMigratePartitionLeaderRequest(newLeaderHSId));
        } catch (InterruptedException | ExecutionException | KeeperException e) {
            VoltDB.crashLocalVoltDB("fail to start MigratePartitionLeader",true, e);
        } finally {
            try {
                leaderAppointee.shutdown();
            } catch (InterruptedException e) {
            }
        }

        tmLog.info("MigratePartitionLeader for partition " + pid + " to " + CoreUtils.hsIdToString(newLeaderHSId));

        //notify the new leader right away if the current leader has drained all transactions.
        notifyNewLeaderOfTxnDoneIfNeeded();
    }

    // After the MigratePartitionLeader has been requested, all the sp requests will be sent back to the sender
    // if these requests are intended for leader. Client interface will restart these transactions.
    private boolean checkMisroutedIv2IntiateTaskMessage(Iv2InitiateTaskMessage message) {
        if (message.isForReplica()) {
            return false;
        }

        if (m_scheduler.isLeader() && m_leaderMigrationState.get() != LeaderMigrationState.TXN_RESTART) {
            //At this point, the message is sent to partition leader
            return false;
        }

        //At this point, the message is misrouted.
        //(1) If a site has been demoted via @MigratePartitionLeader, the messages which are sent to the leader will be restarted.
        //(2) If a site becomes new leader via @MigratePartitionLeader. Transactions will be restarted before it gets notification from old
        //    leader that transactions on older leader have been drained.
        InitiateResponseMessage response = new InitiateResponseMessage(message);
        response.setMisrouted(message.getStoredProcedureInvocation());
        response.m_sourceHSId = getHSId();
        deliver(response);
        if (tmLog.isDebugEnabled()) {
            tmLog.debug("Sending message back on:" + CoreUtils.hsIdToString(m_hsId) + " isLeader:" + m_scheduler.isLeader() +
                    " status:" + m_leaderMigrationState + "\n" + message);
        }
        //notify the new partition leader that the old leader has completed the Txns if needed.
        notifyNewLeaderOfTxnDoneIfNeeded();
        return true;
    }

    // After MigratePartitionLeader has been requested, the fragments which are sent to leader site should be restarted.
    private boolean checkMisroutedFragmentTaskMessage(FragmentTaskMessage message) {
        if (m_scheduler.isLeader() || message.isForReplica()) {
            return false;
        }

        TransactionState txnState = (((SpScheduler)m_scheduler).getTransactionState(message.getTxnId()));

        // If a fragment is part of a transaction which have not been seen on this site, restart it.
        if (txnState == null) {
            FragmentResponseMessage response = new FragmentResponseMessage(message, getHSId());
            TransactionRestartException restart = new TransactionRestartException(
                    "Transaction being restarted due to MigratePartitionLeader.", message.getTxnId());
            restart.setMisrouted(true);
            response.setStatus(FragmentResponseMessage.UNEXPECTED_ERROR, restart);
            response.m_sourceHSId = getHSId();
            response.setPartitionId(m_partitionId);
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("misRoutedFragMsg on site:" + CoreUtils.hsIdToString(getHSId()) + "\n" + message);
            }
            deliver(response);
            return true;
        }

        // A transaction may have multiple batches or fragments. If the first batch or fragment has already been
        // processed, the follow-up batches or fragments should also be processed on this site.
        if (!m_scheduler.isLeader() && !message.isForReplica()) {
            message.setExecutedOnPreviousLeader(true);
            txnState.setLeaderMigrationInvolved();
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("Follow-up fragment will be processed on " + CoreUtils.hsIdToString(getHSId()) + "\n" + message);
            }
        }
        return false;
    }

    @Override
    public VoltMessage recv()
    {
        return null;
    }

    @Override
    public void deliverFront(VoltMessage message)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking()
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(long timeout)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recv(Subject[] s)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s, long timeout)
    {
        throw new UnsupportedOperationException("unimplemented");
    }

    @Override
    public long getHSId()
    {
        return m_hsId;
    }

    @Override
    public void setHSId(long hsId)
    {
        this.m_hsId = hsId;
    }

    // Mark this site as eligible to be removed
    private void updateServiceState() {
        final RealVoltDB db = (RealVoltDB) VoltDB.instance();
        if (db.rejoining() || db.isJoining()) {
            VoltDB.crashLocalVoltDB("Hash mismatch found before this node could finish " + (db.rejoining() ? "rejoin" : "join") +
                    "As a result, the rejoin operation has been canceled.");
            return;
        }
        final SpInitiator init = (SpInitiator) db.getInitiator(m_partitionId);
        if (init.getServiceState().isNormal()) {
            init.updateServiceState(ServiceState.ELIGIBLE_REMOVAL);
            if (tmLog.isDebugEnabled()) {
                tmLog.debug("Add hash mismatch site:" + CoreUtils.hsIdToString(getHSId()));
            }
        }
    }

    /** Produce the repair log. This is idempotent. */
    private void handleLogRequest(VoltMessage message)
    {
        Iv2RepairLogRequestMessage req = (Iv2RepairLogRequestMessage)message;
        // It is possible for a dead host to queue messages after a repair request is processed
        // so make sure this can't happen by re-queuing this message after we know the dead host is gone
        // Since we are not checking validateForeignHostId on the PicoNetwork thread, it is possible for
        // the PicoNetwork thread to validateForeignHostId and queue a message behind this repair message.
        // Further, we loose visibility to the ForeignHost as soon as HostMessenger marks the host invalid
        // even though the PicoNetwork thread could still be alive so we will skeptically
        int deadHostId = req.getDeadHostId();
        if (deadHostId != Integer.MAX_VALUE) {
            // In busy and (often) resource-limited environment, we've observed extremely long delay to
            // close the connection to dead host. This may leads to MP deadlock because the SP leader
            // promotion is blocked, in progress MP transaction couldn't proceed on partitions without
            // leader. We introduce a timeout as a last resort to break the loop.
            if (req.getRepairRetryCount() == 60 * 100) {
                hostLog.warn("Connection to dead host " + deadHostId +
                        " has not been closed for 60 seconds, stop blocking repair request.");
                m_messenger.markPicoZombieHost(deadHostId);
            }
            if (m_messenger.canCompleteRepair(deadHostId)) {
                // Make sure we are the last in the task queue when we know the ForeignHost is gone
                req.disableDeadHostCheck();
                deliver(message);
            }
            else {
                if (req.getRepairRetryCount() > 100 && req.getRepairRetryCount() % 100 == 0) {
                    hostLog.warn("Repair Request for dead host " + deadHostId +
                            " has not been processed yet because connection has not closed");

                }
                Runnable retryRepair = new Runnable() {
                    @Override
                    public void run() {
                        InitiatorMailbox.this.deliver(message);
                    }
                };
                VoltDB.instance().scheduleWork(retryRepair, 10, -1, TimeUnit.MILLISECONDS);
                // the repair message will be resubmitted shortly when the ForeignHosts to the dead host have been removed
            }
            return;
        }

        List<Iv2RepairLogResponseMessage> logs = m_repairLog.contents(req.getRequestId(),
                req.isMPIRequest());

        if (req.isMPIRequest()) {
            m_scheduler.cleanupTransactionBacklogOnRepair();
        }
        for (Iv2RepairLogResponseMessage log : logs) {
            send(message.m_sourceHSId, log);
        }
    }

    /**
     * Create a real repair message from the msg repair log contents and
     * instruct the message handler to execute a repair. Single partition
     * work needs to do duplicate counting; MPI can simply broadcast the
     * repair to the needs repair units -- where the SP will do the rest.
     */
    void repairReplicasWith(List<Long> needsRepair, VoltMessage repairWork)
    {
        //For an SpInitiator the lock should already have been acquire since
        //this method is reach via SpPromoteAlgo.deliver which is reached by InitiatorMailbox.deliver
        //which should already have acquire the lock
        assert(Thread.holdsLock(this));
        repairReplicasWithInternal(needsRepair, repairWork);
    }

    private void repairReplicasWithInternal(List<Long> needsRepair, VoltMessage repairWork) {
        assert(lockingVows());
        if (repairWork instanceof Iv2InitiateTaskMessage) {
            Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)repairWork;
            Iv2InitiateTaskMessage work = new Iv2InitiateTaskMessage(m.getInitiatorHSId(), getHSId(), m);
            m_scheduler.handleMessageRepair(needsRepair, work);
        }
        else if (repairWork instanceof FragmentTaskMessage) {
            // We need to get this into the repair log in case we've never seen it before.  Adding fragment
            // tasks to the repair log is safe; we'll never overwrite the first fragment if we've already seen it.
            ((FragmentTaskMessage)repairWork).setExecutedOnPreviousLeader(false);
            m_repairLog.deliver(repairWork);
            m_scheduler.handleMessageRepair(needsRepair, repairWork);
        }
        else if (repairWork instanceof CompleteTransactionMessage) {
            // CompleteTransactionMessages should always be safe to handle.  Either the work was done, and we'll
            // ignore it, or we need to clean up, or we'll be restarting and it doesn't matter.  Make sure they
            // get into the repair log and then let them run their course.
            ((CompleteTransactionMessage)repairWork).setRequireAck(false);
            m_repairLog.deliver(repairWork);
            m_scheduler.handleMessageRepair(needsRepair, repairWork);
        }
        else {
            throw new RuntimeException("Invalid repair message type: " + repairWork);
        }
    }

    private void logRxMessage(VoltMessage message)
    {
        Iv2Trace.logInitiatorRxMsg(message, m_hsId);
    }

    private void logTxMessage(VoltMessage message)
    {
        if (LOG_TX) {
            hostLog.info("TX HSID: " + CoreUtils.hsIdToString(m_hsId) +
                    ": " + message);
        }
    }

    public void notifyOfSnapshotNonce(String nonce, long snapshotSpHandle) {
        if (m_joinProducer == null) {
            return;
        }
        m_joinProducer.notifyOfSnapshotNonce(nonce, snapshotSpHandle);
    }

    // The new partition leader is notified by previous partition leader
    // that previous partition leader has drained its txns
    private void setLeaderMigrationState(MigratePartitionLeaderMessage message) {

        // Synchronize the leader migration state since the state updates are async
        synchronized(m_leaderMigrationStateLock) {
            // The host with old partition leader is down.
            if (message.isStatusReset()) {
                boolean updated =  m_leaderMigrationState.compareAndSet(LeaderMigrationState.TXN_RESTART ,LeaderMigrationState.NONE);
                if (!updated && !m_leaderPromotedUponMigration){

                    // Update the status only if the site has not been promoted.
                    m_leaderMigrationState.set(LeaderMigrationState.TXN_DRAINED);
                }
                m_newLeaderHSID.set(Long.MIN_VALUE);
                m_leaderPromotedUponMigration = false;
                tmLog.info("MigratePartitionLeader " +
                        CoreUtils.hsIdToString(m_hsId) + " is reset to state:" + m_leaderMigrationState);
                return;
            }

            if (!m_leaderMigrationState.compareAndSet(LeaderMigrationState.NONE ,LeaderMigrationState.TXN_DRAINED)) {
                m_leaderMigrationState.compareAndSet(LeaderMigrationState.TXN_RESTART ,LeaderMigrationState.NONE);
            }

            m_leaderPromotedUponMigration = false;
            tmLog.info("MigratePartitionLeader new leader " +
                    CoreUtils.hsIdToString(m_hsId) + " is notified by previous leader " +
                    CoreUtils.hsIdToString(message.getPriorLeaderHSID()) + ". state:" + m_leaderMigrationState);
        }
    }

    // The site for new partition leader
    public void setLeaderMigrationState(boolean migratePartitionLeader, int deadSPIHost) {
        synchronized(m_leaderMigrationStateLock) {
            if (!migratePartitionLeader) {
                m_leaderMigrationState.set(LeaderMigrationState.NONE);
                m_newLeaderHSID.set(Long.MIN_VALUE);
                return;
            }

            // The previous leader has already drained all txns
            if (m_leaderMigrationState.compareAndSet(LeaderMigrationState.TXN_DRAINED ,LeaderMigrationState.NONE)) {
                tmLog.info("MigratePartitionLeader transactions on previous partition leader are drained. New leader:" +
                        CoreUtils.hsIdToString(m_hsId) + " state:" + m_leaderMigrationState);
                return;
            }

            m_leaderPromotedUponMigration = true;
            if (!m_messenger.getLiveHostIds().contains(deadSPIHost)) {
                // Wait for the notification from old partition leader
                m_leaderMigrationState.set(LeaderMigrationState.TXN_RESTART);
            }
            tmLog.info("MigratePartitionLeader restart txns on new leader:" + CoreUtils.hsIdToString(m_hsId) + " state:" + m_leaderMigrationState);
        }
    }

    // Old master notifies new master that the transactions before the checkpoint on old master have been drained.
    // Then new master can proceed to process transactions.
    public void notifyNewLeaderOfTxnDoneIfNeeded() {
        // return quickly to avoid performance hit
        if (m_newLeaderHSID.get() == Long.MIN_VALUE ) {
            return;
        }

        SpScheduler scheduler = (SpScheduler)m_scheduler;
        if (!scheduler.txnDoneBeforeCheckPoint()) {
            return;
        }

        MigratePartitionLeaderMessage message = new MigratePartitionLeaderMessage(m_hsId, m_newLeaderHSID.get());
        send(message.getNewLeaderHSID(), message);

        // reset status on the old partition leader
        m_leaderMigrationState.set(LeaderMigrationState.NONE);
        m_repairLog.setLeaderState(false);
        tmLog.info("MigratePartitionLeader previous leader " + CoreUtils.hsIdToString(m_hsId) + " notifies new leader " +
                CoreUtils.hsIdToString(m_newLeaderHSID.get()) + " transactions are drained." + " state:" + m_leaderMigrationState);
        m_newLeaderHSID.set(Long.MIN_VALUE);
    }

    public ZooKeeper getZK() {
        return m_messenger.getZK();
    }
}
