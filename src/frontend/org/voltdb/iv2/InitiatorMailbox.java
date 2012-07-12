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

import java.util.concurrent.atomic.AtomicLong;

import java.util.concurrent.ExecutionException;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.json_voltpatches.JSONObject;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

import org.voltcore.utils.CoreUtils;

import org.voltcore.zk.MapCache;
import org.voltcore.zk.MapCacheReader;

import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;
import org.voltdb.messaging.RejoinMessage;

import org.voltdb.VoltDB;
import org.voltdb.VoltZK;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class InitiatorMailbox implements Mailbox
{
    static boolean LOG_TX = false;
    static boolean LOG_RX = false;

    VoltLogger hostLog = new VoltLogger("HOST");
    VoltLogger tmLog = new VoltLogger("TM");

    private final int m_partitionId;
    private final InitiatorMessageHandler m_msgHandler;
    private final HostMessenger m_messenger;
    private final RepairLog m_repairLog;
    private final RejoinProducer m_rejoinProducer;
    private final MapCacheReader m_masterMapCache;
    private long m_hsId;
    private Term m_term;

    private Set<Long> m_replicas = null;

    // hacky temp txnid
    AtomicLong m_txnId = new AtomicLong(0);

    synchronized public void setTerm(Term term)
    {
        m_term = term;
    }

    public InitiatorMailbox(int partitionId,
            InitiatorMessageHandler msgHandler,
            HostMessenger messenger, RepairLog repairLog,
            RejoinProducer rejoinProducer)
    {
        m_partitionId = partitionId;
        m_msgHandler = msgHandler;
        m_messenger = messenger;
        m_repairLog = repairLog;
        m_rejoinProducer = rejoinProducer;

        m_masterMapCache = new MapCache(m_messenger.getZK(), VoltZK.iv2masters);
        try {
            m_masterMapCache.start(false);
        } catch (InterruptedException ignored) {
            // not blocking. shouldn't interrupt.
        } catch (ExecutionException crashme) {
            // this on the other hand seems tragic.
            VoltDB.crashLocalVoltDB("Error constructiong InitiatorMailbox.", false, crashme);
        }
    }

    public void shutdown() throws InterruptedException
    {
        m_masterMapCache.shutdown();
    }

    // Provide the starting replica configuration (for startup)
    public synchronized void setReplicas(List<Long> replicas)
    {
        Iv2Trace.logTopology(getHSId(), replicas, m_partitionId);
        m_msgHandler.updateReplicas(replicas);
    }

    // Change the replica set configuration (during or after promotion)
    public synchronized void updateReplicas(List<Long> replicas)
    {
        Iv2Trace.logTopology(getHSId(), replicas, m_partitionId);
        // If a replica set has been configured and it changed during
        // promotion, must cancel the term
        if (m_replicas != null && m_term != null) {
            m_term.cancel();
        }
        m_replicas = new TreeSet<Long>();
        m_replicas.addAll(replicas);
        m_msgHandler.updateReplicas(replicas);
    }

    public long getMasterHsId(int partitionId)
    {
        try {
            JSONObject master = m_masterMapCache.get(Integer.toString(partitionId));
            long masterHsId = Long.valueOf(master.getLong("hsid"));
            return masterHsId;
        } catch (Exception e) {
            VoltDB.crashLocalVoltDB("Failed to deserialize map cache reader object.", false, e);
        }
        // unreachable.
        return Long.MIN_VALUE;
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
        logRxMessage(message);
        if (message instanceof Iv2RepairLogRequestMessage) {
            handleLogRequest(message);
            return;
        }
        else if (message instanceof Iv2RepairLogResponseMessage) {
            m_term.deliver(message);
            return;
        }
        else if (message instanceof RejoinMessage) {
            m_rejoinProducer.deliver((RejoinMessage)message);
            return;
        }
        m_repairLog.deliver(message);
        m_msgHandler.deliver(message);
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
    void handleLogRequest(VoltMessage message)
    {
        List<RepairLog.Item> logs = m_repairLog.contents();
        int ofTotal = logs.size();
        int seq = 1;

        Iv2RepairLogRequestMessage req = (Iv2RepairLogRequestMessage)message;
        tmLog.info("SP " +  CoreUtils.hsIdToString(getHSId())
                + " handling repair log request id " + req.getRequestId()
                + " for " + CoreUtils.hsIdToString(message.m_sourceHSId)
                + ". Responding with " + ofTotal + " repair log parts.");

        if (logs.isEmpty()) {
            // respond with an ack that the log is empty.
            // maybe better if seq 0 is always the ack with null payload?
            Iv2RepairLogResponseMessage response =
                new Iv2RepairLogResponseMessage(
                        req.getRequestId(),
                        0, // sequence
                        0, // total expected
                        m_repairLog.getLastSpHandle(), // spHandle
                        null); // no payload. just an ack.
            send(message.m_sourceHSId, response);
        }
        else {
            for (RepairLog.Item log : logs) {
                Iv2RepairLogResponseMessage response =
                    new Iv2RepairLogResponseMessage(
                            req.getRequestId(),
                            seq,
                            ofTotal,
                            log.getSpHandle(),
                            log.getMessage());
                send(message.m_sourceHSId, response);
                seq++;
            }
        }
        return;
    }

    /**
     * Create a real repair message from the msg repair log contents and
     * instruct the message handler to execute a repair.
     */
    void repairReplicasWith(List<Long> needsRepair, Iv2RepairLogResponseMessage msg)
    {
        VoltMessage repairWork = msg.getPayload();
        if (repairWork instanceof Iv2InitiateTaskMessage) {
            Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)repairWork;
            Iv2InitiateTaskMessage work = new Iv2InitiateTaskMessage(getHSId(), getHSId(), m);
            m_msgHandler.handleIv2InitiateTaskMessageRepair(needsRepair, work);
        }
    }

    void logRxMessage(VoltMessage message)
    {
        Iv2Trace.logInitiatorRxMsg(message, m_hsId);
        if (LOG_RX) {
            hostLog.info("RX HSID: " + CoreUtils.hsIdToString(m_hsId) +
                    ": " + message);
        }
    }

    void logTxMessage(VoltMessage message)
    {
        if (LOG_TX) {
            hostLog.info("TX HSID: " + CoreUtils.hsIdToString(m_hsId) +
                    ": " + message);
        }
    }
}
