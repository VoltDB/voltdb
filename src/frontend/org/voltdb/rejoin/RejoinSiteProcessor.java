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

package org.voltdb.rejoin;

import java.nio.ByteBuffer;
import java.util.List;

import org.voltcore.utils.Pair;

public interface RejoinSiteProcessor {

    /**
     * Initialize the snapshot sink, bind to a socket and wait for incoming
     * connection.
     *
     * @return A list of local addresses and port that remote node can connect
     *         to.
     */
    public abstract Pair<List<byte[]>, Integer> initialize();

    /**
     * Whether or not all snapshot blocks are polled
     *
     * @return true if no more blocks to come, false otherwise
     */
    public abstract boolean isEOF();

    /**
     * Closes all connections
     */
    public abstract void close();

    /**
     * Poll the next block to be sent to EE.
     *
     * @return The next block along with its table ID, or null if there's none.
     */
    public abstract Pair<Integer, ByteBuffer> poll();

    /**
     * Poll the next block to be sent to EE, wait for the next available block
     * if necessary. This method blocks.
     *
     * @return The next block of snapshot data. null indicates there's no more
     *         data to come.
     * @throws InterruptedException
     */
    public abstract Pair<Integer, ByteBuffer> take() throws InterruptedException;

    /**
     * Return the number of bytes transferred so far.
     * @return
     */
    public abstract long bytesTransferred();

}