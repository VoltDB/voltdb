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

import java.util.List;

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

import org.voltcore.utils.CoreUtils;

import org.voltdb.messaging.Iv2InitiateTaskMessage;
import org.voltdb.messaging.Iv2RepairLogRequestMessage;
import org.voltdb.messaging.Iv2RepairLogResponseMessage;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class InitiatorMailbox implements Mailbox
{
    static boolean LOG_TX = false;
    static boolean LOG_RX = false;

    VoltLogger hostLog = new VoltLogger("HOST");
    private final InitiatorMessageHandler m_msgHandler;
    private final HostMessenger m_messenger;
    private final RepairLog m_repairLog;
    private long m_hsId;
    private Term m_term;

    // hacky temp txnid
    AtomicLong m_txnId = new AtomicLong(0);

    synchronized public void setTerm(Term term)
    {
        m_term = term;
    }

    public InitiatorMailbox(InitiatorMessageHandler msgHandler,
            HostMessenger messenger, RepairLog repairLog)
    {
        m_msgHandler = msgHandler;
        m_messenger = messenger;
        m_repairLog = repairLog;
        m_messenger.createMailbox(null, this);
        m_msgHandler.setMailbox(this);
    }

    public synchronized void updateReplicas(List<Long> replicas)
    {
        // first cancel any ongoing repair work. must do this with
        // the deliver lock held.
        if (m_term != null) {
            m_term.cancel();
        }
        m_msgHandler.updateReplicas(replicas);
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
        int seq = 0;
        Iv2RepairLogRequestMessage req = (Iv2RepairLogRequestMessage)message;
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
        return;
    }

    /** Make and send a repair message upon request. */
    void repairReplicaWith(long replicaHSId, Iv2RepairLogResponseMessage msg)
    {
        VoltMessage repairWork = msg.getPayload();
        if (repairWork instanceof Iv2InitiateTaskMessage) {
            Iv2InitiateTaskMessage m = (Iv2InitiateTaskMessage)repairWork;
            Iv2InitiateTaskMessage work =
                new Iv2InitiateTaskMessage(getHSId(), getHSId(), m);
            send(replicaHSId, work);
        }
    }

    void logRxMessage(VoltMessage message)
    {
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
