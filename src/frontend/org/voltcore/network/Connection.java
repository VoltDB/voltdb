/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

package org.voltcore.network;

import java.util.concurrent.Future;
import java.net.InetSocketAddress;

public interface Connection {
    /**
     * Retrieve the write stream for this connection
     * @return Reference to a writable stream for outgoing data
     */
    WriteStream writeStream();

    /**
     * Retrieve the read stream for this connection.
     * @return Reference to a stream of incoming data
     */
    NIOReadStream readStream();

    void disableReadSelection();
    void enableReadSelection();

    /**
     * If the hostname has been resolved this will return the hostname and the IP + port of the remote connection.
     * If the hostname was not resolved this will return just the IP + port.
     * The format is hostname/127.0.0.1:21212 if resolution is done, /127.0.0.1:21212 otherwise
     * When logged from the server the remote port # will allow you to identify individual connections to the server
     * by the ephemeral port number.
     *
     * @return hostname and IP and port as a string
     */
    String getHostnameAndIPAndPort();

    /**
     * Returns the hostname if it was resolved, otherwise it returns the IP. Doesn't do a reverse DNS lookup
     */
    String getHostnameOrIP();
    int getRemotePort();
    InetSocketAddress getRemoteSocketAddress();

    long connectionId();

    void queueTask(Runnable r);

    /**
     * Schedule the connection to be unregistered and closed
     */
    Future<?> unregister();
}
