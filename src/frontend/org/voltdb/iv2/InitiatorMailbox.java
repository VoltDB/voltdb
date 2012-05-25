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

import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.MessagingException;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

/**
 * InitiatorMailbox accepts initiator work and proxies it to the
 * configured InitiationRole.
 */
public class InitiatorMailbox implements Mailbox
{
    VoltLogger hostLog = new VoltLogger("HOST");
    private final InitiatorMessageHandler m_msgHandler;
    private final HostMessenger m_messenger;
    private long m_hsId;

    // hacky temp txnid
    AtomicLong m_txnId = new AtomicLong(0);

    public InitiatorMailbox(InitiatorMessageHandler msgHandler, HostMessenger messenger,
            PartitionClerk partitionClerk)
    {
        m_msgHandler = msgHandler;
        m_messenger = messenger;
        m_messenger.createMailbox(null, this);
        m_msgHandler.setMailbox(this);
    }

    public synchronized void updateReplicas(long[] hsids)
    {
        m_msgHandler.updateReplicas(hsids);
    }

    @Override
    public void send(long destHSId, VoltMessage message) throws MessagingException
    {
        message.m_sourceHSId = this.m_hsId;
        m_messenger.send(destHSId, message);
    }

    @Override
    public void send(long[] destHSIds, VoltMessage message) throws MessagingException
    {
        message.m_sourceHSId = this.m_hsId;
        m_messenger.send(destHSIds, message);
    }

    @Override
    public synchronized void deliver(VoltMessage message)
    {
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
}
