/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

    /** The distinct exception class allows better logging of these unexpected errors. */
    class BadMessageLength extends IOException {
        private static final long serialVersionUID = 8547352379044459911L;
        public BadMessageLength(String string) {
            super(string);
        }
    }

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
     * Retrieves a message header into the given buffer from the connection,
     * through the given inputStream.
     * @param inputStream
     * @param header
     * @return  True if the header buffer was filled, false if not.
     */
    boolean retrieveNextMessageHeader(NIOReadStream inputStream, ByteBuffer header);

    /**
     * Fill the buffer from the read stream.  Writes into the buffer
     * until either the buffer is full of the read stream is empty.
     * @param inputStream  The inputStream
     * @param buffer       The buffer
     * @return the number of bytes copied into the buffer.
     */
    int fillBuffer(NIOReadStream inputStream, ByteBuffer buffer);

    /**
     * Checks the validity of the given message length.
     * @param messageLength
     * @throws BadMessageLength
     */
    void checkMessageLength(int messageLength) throws BadMessageLength;

    /**
     * Get the length of the next message.
     * @return The length of the next message.
     */
    int getNextMessageLength();

    /**
     * Set the length of the next message.
     * @param nextMessageLength
     */
    void setNextMessageLength(int nextMessageLength);

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
