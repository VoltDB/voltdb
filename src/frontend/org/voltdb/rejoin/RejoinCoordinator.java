/* This file is part of VoltDB.
 * Copyright (C) 2008-2013 VoltDB Inc.
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

package org.voltdb.rejoin;

import org.apache.zookeeper_voltpatches.KeeperException;
import org.json_voltpatches.JSONException;
import org.voltcore.messaging.HostMessenger;
import org.voltdb.ClientInterface;
import org.voltdb.messaging.LocalMailbox;

/**
 * Coordinates the sites to perform rejoin
 */
public abstract class RejoinCoordinator extends LocalMailbox {
    protected final HostMessenger m_messenger;

    public RejoinCoordinator(HostMessenger hostMessenger) {
        super(hostMessenger, hostMessenger.generateMailboxId(null));
        m_messenger = hostMessenger;
    }

    public void setClientInterface(ClientInterface ci) {}

    /**
     * Starts the rejoin process.
     */
    public abstract boolean startJoin(Database catalog, Cartographer cartographer)
            throws KeeperException, InterruptedException, JSONException;

    /**
     * Discard the mailbox.
     */
    public void close() {
        m_messenger.removeMailbox(getHSId());
    }
}
