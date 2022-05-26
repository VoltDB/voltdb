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

package org.voltcore.network;

import java.io.IOException;
import java.nio.ByteBuffer;

import io.netty.buffer.CompositeByteBuf;

public interface InputHandler
{

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
     * Retrieves the next message bytes from the input CompositeByteBuf,
     * if the input contains all the bytes required for next message.
     *
     * Only one form of <code>retrieveNextMessage</code> may be used with one input handler.
     * It is not safe to switch between the two different <code>retrieveNextMessage</code>.
     *
     * @param inputBB
     * @return ByteBuffer containing the message data
     * @throws IOException
     */
    ByteBuffer retrieveNextMessage(CompositeByteBuf inputBB) throws IOException;

    /**
     * Returns the number of bytes that need to be read to get the next full message.
     * Returns 0 if the next message length is not known, which is the case
     * before the length part of the message is read from the stream.
     * @return
     */
    int getNextMessageLength();

    /**
     * Handle the incoming message produced by retrieve next message
     *
     * @param message
     *            ByteBuffer containing the message data
     * @param c
     *            connection
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
