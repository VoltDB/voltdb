/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.network;

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
     * Get the hostname of a host if it's available, otherwise return the IP
     * address.
     *
     * @return hostname or IP as a string
     */
    String getHostnameOrIP();
    long connectionId();

    /**
     * Schedule an action to be invoked in a network thread
     * that has exclusive access to this connection
     * @param r Runnable to execute
     **/
    void scheduleRunnable(Runnable r);

    /**
     * Schedule the connection to be unregistered and closed
     */
    void unregister();
}
