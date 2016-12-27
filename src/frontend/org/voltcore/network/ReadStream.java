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
import java.nio.channels.ReadableByteChannel;

public interface ReadStream {

    /** @returns the number of bytes available to be read. */
    int dataAvailable();

    /**
     * Move all bytes in current read buffers to output array, free read buffers
     * back to thread local memory pool.
     * @param output
     */
    void getBytes(byte[] output);

    /**
     * Move all bytes in current read buffers to output buffer, free read buffers
     * back to thread local memory pool.
     * @param output
     */
    int getBytes(ByteBuffer output);

    /** return the number of bytes read into the stream.  If interval is true,
     * returns the number of byres read since the last call.  Returns the total
     * otherwise.
     * @param interval
     * @return
     */
    long getBytesRead(boolean interval);

    /** @returns an integer from the read stream. */
    int getInt();

    /**
     * Read at most maxBytes from the network. Will read until the network would
     * block, the stream is closed or the maximum bytes to read is reached.
     * @param maxBytes
     * @return -1 if closed otherwise total buffered bytes. In all cases,
     * data may be buffered in the stream - even when the channel is closed.
     */
    int read(ReadableByteChannel channel, int maxBytes, NetworkDBBPool pool) throws IOException;

    /** shut down the read stream */
    void shutdown();
}
