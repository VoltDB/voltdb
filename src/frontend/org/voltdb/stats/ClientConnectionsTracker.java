/* This file is part of VoltDB.
 * Copyright (C) 2022 Volt Active Data Inc.
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
package org.voltdb.stats;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Counter of the number of client connections. Used to enforce a limit on the maximum number of connections
 * and count rejected ones.
 */
public class ClientConnectionsTracker {

    static final int FILE_DESCRIPTOR_HEADROOM = 300;

    private final FileDescriptorsTracker m_maxFileDescriptorTracker;

    private final AtomicInteger m_connectionsCount = new AtomicInteger(0);
    private final AtomicInteger m_droppedConnectionsCount = new AtomicInteger(0);
    private final AtomicInteger m_acceptedConnectionsCount = new AtomicInteger(0);

    public ClientConnectionsTracker(FileDescriptorsTracker maxFileDescriptorTracker) {
        this.m_maxFileDescriptorTracker = maxFileDescriptorTracker;
    }

    public boolean isConnectionsLimitReached() {
        return m_connectionsCount.get() >= getMaxNumberOfAllowedConnections();
    }

    public void connectionClosed() {
        m_connectionsCount.decrementAndGet();
    }

    public void connectionOpened() {
        m_connectionsCount.incrementAndGet();
        m_acceptedConnectionsCount.incrementAndGet();
    }

    public void connectionDropped() {
        m_droppedConnectionsCount.incrementAndGet();
    }

    public int getConnectionsCount() {
        return m_connectionsCount.get();
    }

    public int getAcceptedConnectionsCount() {
        return m_acceptedConnectionsCount.get();
    }

    public int getDroppedConnectionsCount() {
        return m_droppedConnectionsCount.get();
    }

    public int getMaxNumberOfAllowedConnections() {
        return m_maxFileDescriptorTracker.getOpenFileDescriptorLimit() - FILE_DESCRIPTOR_HEADROOM;
    }
}
