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
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;
import org.voltcore.utils.CoreUtils;
import org.voltdb.VoltDB;
import org.voltdb.VoltZK;
import org.voltdb.messaging.CompleteTransactionMessage;
import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;
import org.voltdb.messaging.RejoinMessage;

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
    static boolean LOG_TX = false;
    static boolean LOG_RX = false;

    VoltLogger hostLog = new VoltLogger("HOST");
    VoltLogger tmLog = new VoltLogger("TM");

    private final int m_partitionId;
    private final Scheduler m_scheduler;
    private final HostMessenger m_messenger;
    private final RepairLog m_repairLog;
    private final RejoinProducer m_rejoinProducer;
    private final LeaderCacheReader m_masterLeaderCache;
    private long m_hsId;
    private RepairAlgo m_algo;

    /*
     * Hacky global map of initiator mailboxes to support assertions
     * that verify the locking is kosher
     */
    public static final CopyOnWriteArrayList<InitiatorMailbox> m_allInitiatorMailboxes
                                                                         = new CopyOnWriteArrayList<InitiatorMailbox>();

    synchronized public void setRepairAlgo(RepairAlgo algo)
    {
        setRepairAlgoInternal(algo);
    }

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
            RejoinProducer rejoinProducer)
    {
        m_partitionId = partitionId;
        m_scheduler = scheduler;
        m_messenger = messenger;
        m_repairLog = repairLog;
        m_rejoinProducer = rejoinProducer;

        m_masterLeaderCache = new LeaderCache(m_messenger.getZK(), VoltZK.iv2masters);
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
    }

    // enforce restriction on not allowing promotion during rejoin.
    public boolean acceptPromotion()
    {
        return m_rejoinProducer == null || m_rejoinProducer.acceptPromotion();
    }

    /*
     * Thou shalt not lock two initiator mailboxes from the same thread, lest ye be deadlocked.
     */
    public static boolean lockingVows() {
        List<InitiatorMailbox> lockedMailboxes = new ArrayList<InitiatorMailbox>();
        for (InitiatorMailbox im : m_allInitiatorMailboxes) {
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
    public synchronized void updateReplicas(List<Long> replicas)
    {
        updateReplicasInternal(replicas);
    }

    protected void updateReplicasInternal(List<Long> replicas) {
        assert(lockingVows());
        Iv2Trace.logTopology(getHSId(), replicas, m_partitionId);
        // If a replica set has been configured and it changed during
        // promotion, must cancel the term
        if (m_algo != null) {
            m_algo.cancel();
        }
        m_scheduler.updateReplicas(replicas);
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
    public synchronized void deliver(VoltMessage message)
    {
        deliverInternal(message);
    }

    protected void deliverInternal(VoltMessage message) {
        assert(lockingVows());
        logRxMessage(message);
        if (message instanceof Iv2RepairLogRequestMessage) {
            handleLogRequest(message);
            return;
        }
        else if (message instanceof Iv2RepairLogResponseMessage) {
            m_algo.deliver(message);
            return;
        }
        else if (message instanceof RejoinMessage) {
            m_rejoinProducer.deliver((RejoinMessage)message);
            return;
        }
        m_repairLog.deliver(message);
        m_scheduler.deliver(message);
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

    /** Produce the repair log. This is idempotent. */
    private void handleLogRequest(VoltMessage message)
    {
        Iv2RepairLogRequestMessage req = (Iv2RepairLogRequestMessage)message;
        List<Iv2RepairLogResponseMessage> logs = m_repairLog.contents(req.getRequestId(),
                req.isMPIRequest());

        tmLog.debug(""
            + CoreUtils.hsIdToString(getHSId())
            + " handling repair log request id " + req.getRequestId()
            + " for " + CoreUtils.hsIdToString(message.m_sourceHSId) + ". ");

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

    protected  void repairReplicasWithInternal(List<Long> needsRepair, VoltMessage repairWork) {
        assert(lockingVows());
        if (repairWork instanceof Iv2InitiateTaskMessage) {
            Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)repairWork;
            Iv2InitiateTaskMessage work = new Iv2InitiateTaskMessage(m.getInitiatorHSId(), getHSId(), m);
            m_scheduler.handleIv2InitiateTaskMessageRepair(needsRepair, work);
        }
        else if (repairWork instanceof CompleteTransactionMessage) {
            send(com.google.common.primitives.Longs.toArray(needsRepair), repairWork);
        }
        else {
            throw new RuntimeException("Invalid repair message type: " + repairWork);
        }
    }

    private void logRxMessage(VoltMessage message)
    {
        Iv2Trace.logInitiatorRxMsg(message, m_hsId);
        if (LOG_RX) {
            hostLog.info("RX HSID: " + CoreUtils.hsIdToString(m_hsId) +
                    ": " + message);
        }
    }

    private void logTxMessage(VoltMessage message)
    {
        if (LOG_TX) {
            hostLog.info("TX HSID: " + CoreUtils.hsIdToString(m_hsId) +
                    ": " + message);
        }
    }
}
