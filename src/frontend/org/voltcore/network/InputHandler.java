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

import java.io.IOException;
import java.nio.ByteBuffer;

public interface InputHandler {

    /**
     * Retrieve the maximum number of bytes this input handler is willing to
     * read from the connection. If set to zero, no data will be read.
     */
    int getMaxRead();

    /**
     * Retrieve the next message from the connection. Examine the connection's
     * read stream. If a discrete unit of work is available, copy those bytes
     * (via NIOReadStream.getBytes()) to a new byte buffer and return them.
     *
     * @param inputStream
     * @return ByteBuffer containing the message data
     */
    ByteBuffer retrieveNextMessage(NIOReadStream inputStream) throws IOException;

    /**
     * Handle the incoming message produced by retrieve next message
     *
     * @param message
     * @param channelFacade
     * @throws IOException
     */
    void handleMessage(ByteBuffer message, Connection c) throws IOException;

    /**
     * Notify the input handler that the Connection will start receiving work
     * soon (Channel is registering with Selector)
     *
     * @param c
     */
    public void starting(Connection c);

    /**
     * Notify the input handler that the Connection will now receive incoming
     * messages (Channel is registered with Selector)
     *
     * @param c
     */
    public void started(Connection c);

    /**
     * Notify the input handler that the Connection is shut down. (Channel is
     * being unregistered by application, IOException reading or writing, EOF
     * reading )
     *
     * @param c
     */
    public void stopping(Connection c);

    /**
     * Notify the input handler that the Connection has been shut down.
     *
     * @param c
     */
    public void stopped(Connection c);

    public Runnable onBackPressure();
    public Runnable offBackPressure();
    public QueueMonitor writestreamMonitor();
    public long connectionId();
}
