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

import java.nio.ByteBuffer;

import org.voltdb.messaging.FastSerializable;
import org.voltdb.utils.DeferredSerialization;
import org.voltdb.utils.DBBPool.BBContainer;

public interface WriteStream {
    /**
     * Returns true when a drainTo invocation was unable to completely drain all queued bytes
     */
    public boolean hadBackPressure();

    /**
     * Queued a container for writing. This isn't the ideal API to use since the serialization has been done
     * outside of a network thread
     * @param c
     */
    public boolean enqueue(final BBContainer c);

    /**
     * Queue a FastSerializable object for writing. This is 3rd best way to serialize and queue messages.
     * Since no expected message size is provided the default one for this port is used which may not be accurate
     * for this particular message. Because FastSerializer is used to serialize the object there is some
     * overhead incurred if the FastSerializer has to resize and every time the FastSerializer has to check
     * if it needs to grow.
     * @param f
     */
    public boolean enqueue(final FastSerializable f);

    /**
     * Queue a FastSerializable object for writing. This is 2nd best way to serialize and queue messages.
     * The expected message size is used to size the initial allocation for the FastSerializer.
     * Because FastSerializer is used to serialize the object there is some over head incurred
     * when the FastSerializer has to check if it needs to grow, but the cost is pretty minor compared
     * to the cost of actually growing the FastSerializer.
     * @param f
     */
    public boolean enqueue(final FastSerializable f, final int expectedSize);

    /**
     * Queue a message and defer the serialization of the message until later. This is the ideal mechanism
     * for serializing and queueing network writes. It allows the sender to define an efficient serialization
     * mechanism that performs a single allocation of the correct size without the overhead of FastSerializer
     * which has to constantly check if it needs to grow.
     * @param ds A deferred serialization task that will generate the message
     */
    public boolean enqueue(final DeferredSerialization ds);
    /**
     * Queue a ByteBuffer for writing to the network. If the ByteBuffer is not direct then it will
     * be copied to a DirectByteBuffer if it is less then DBBPool.MAX_ALLOCATION_SIZE. This method
     * is a backup for code that isn't able to defer its serialization to a network thread
     * for whatever reason. It is reasonably efficient if a DirectByteBuffer is passed in,
     * but it would be better to keep allocations of DirectByteBuffers inside the network pools.
     * @param b
     */
    public boolean enqueue(final ByteBuffer b);

    /**
     * Calculate how long the oldest write has been waiting to go onto the wire. This allows dead connections
     * to be detected and closed.
     * @param now The current time in milliseconds
     * @return Delta between the current time and the time when calculatePendingWriteDelta was called when the oldest
     * write entered the queue.
     */
    public int calculatePendingWriteDelta(final long now);

    /**
     * Determine if the WriteStream contains data.
     * @return true if the stream has no data to write.
     */
    boolean isEmpty();

    /**
     * Return the number of messages waiting to be written to the network
     */
    int getOutstandingMessageCount();
}
