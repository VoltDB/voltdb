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

package org.voltdb.messaging;

import org.voltcore.messaging.HostMessenger;
import org.voltcore.messaging.Mailbox;
import org.voltcore.messaging.Subject;
import org.voltcore.messaging.VoltMessage;

/**
 * A base class for local mailboxes that always uses m_messenger.send
 * and requires custom implementation of deliver.
 */
public abstract class LocalMailbox implements Mailbox {

    protected final HostMessenger m_messenger;
    private long hsId;

    public LocalMailbox(HostMessenger messenger) {
        this(messenger, Long.MIN_VALUE);
    }

    public LocalMailbox(HostMessenger messenger, long hsId) {
        this.m_messenger = messenger;
        this.hsId = hsId;
    }

    @Override
    public void send(long hsId, VoltMessage message)
    {
        assert(message != null);
        message.m_sourceHSId = this.hsId;
        m_messenger.send(hsId, message);
    }

    @Override
    public void send(long[] hsIds, VoltMessage message)
    {
        assert(message != null);
        assert(hsIds != null);
        message.m_sourceHSId = this.hsId;
        m_messenger.send(hsIds, message);
    }

    @Override
    abstract public void deliver(final VoltMessage message);

    @Override
    public void deliverFront(VoltMessage message) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recv() {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recvBlocking() {
       throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recvBlocking(long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recv(Subject[] s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s) {
        throw new UnsupportedOperationException();
    }

    @Override
    public VoltMessage recvBlocking(Subject[] s, long timeout) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getHSId() {
        return hsId;
    }

    @Override
    public void setHSId(long hsId) {
        this.hsId = hsId;
    }

}
