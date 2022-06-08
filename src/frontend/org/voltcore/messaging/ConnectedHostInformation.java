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
package org.voltcore.messaging;

import javax.net.ssl.SSLEngine;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

class ConnectedHostInformation {
    private final int hostId;
    private final String hostDisplayName;
    private final SocketChannel socket;
    private final SSLEngine sslEngine;
    private final InetSocketAddress listeningAddress;

    public ConnectedHostInformation(
            int hostId,
            String hostDisplayName,
            SocketChannel socket,
            SSLEngine sslEngine,
            InetSocketAddress listeningAddress
    ) {
        this.hostId = hostId;
        this.hostDisplayName = hostDisplayName;
        this.socket = socket;
        this.sslEngine = sslEngine;
        this.listeningAddress = listeningAddress;
    }

    public int getHostId() {
        return hostId;
    }

    public String getHostDisplayName() {
        return hostDisplayName;
    }

    public SocketChannel getSocket() {
        return socket;
    }

    public SSLEngine getSslEngine() {
        return sslEngine;
    }

    public InetSocketAddress getListeningAddress() {
        return listeningAddress;
    }
}
