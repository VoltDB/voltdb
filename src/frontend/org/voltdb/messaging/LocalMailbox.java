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

package org.voltdb.messaging;

/**
 * A base class for local mailboxes that always uses hostMessenger.send
 * and requires custom implementation of deliver.
 */
public abstract class LocalMailbox implements Mailbox {

    private final HostMessenger hostMessenger;
    private final int siteId;

    public LocalMailbox(HostMessenger hostMessenger, int siteId) {
        this.hostMessenger = hostMessenger;
        this.siteId = siteId;
    }

    @Override
    public void send(int siteId, int mailboxId, VoltMessage message)
        throws MessagingException {
        assert(message != null);
        hostMessenger.send(siteId, mailboxId, message);
    }

    @Override
    public void send(int[] siteIds, int mailboxId, VoltMessage message)
        throws MessagingException {
        assert(message != null);
        assert(siteIds != null);
        hostMessenger.send(siteIds, mailboxId, message);
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
    public int getSiteId() {
        return siteId;
    }

}
