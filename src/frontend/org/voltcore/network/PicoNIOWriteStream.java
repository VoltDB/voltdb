/* This file is part of VoltDB.
 * Copyright (C) 2008-2014 VoltDB Inc.
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
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.SelectionKey;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.RateLimitedLogger;

/**
*
*  Provide a queue for ByteBuffers and DeferredSerializations and drain them to gathering ByteChannel.
*  Uses a thread local memory pool for serializing messages that are < MAX_GATHERING_WRITE size and HeapByteBuffers
*  otherwise. Jumps through serious hoops to avoid ever writing large HeapByteBuffers to the channel
*  because Java will allocate a DirectByteBuffer and copy ALL the data into the DirectByteBuffer even if only
*  a small fraction can reasonably be written to the channel. This wastes time in copying data that can never possibly
*  make it into the channel in non blocking mode and space because the DirectByteBuffer is never released unlike
*  the pool which shrinks after a long time without usage.
*
*  The value m_port.m_expectedOutgoingMessageSize is used to set the initial storage a FastSerializer will
*  allocate for when doing deferred serialization of FastSerializables. FastSerializable + enqueue is the
*  best way to serialize data unless you can't pick a good value for m_port.m_expectedOutgoingMessageSize.
*  In most cases you are optimizing for the bulk of your message and it is fine to guess a little high as the memory
*  allocation works well.
*/
public class PicoNIOWriteStream extends NIOWriteStreamBase {
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    /**
     * Contains messages waiting to be serialized and written to the socket
     */
    private final ArrayDeque<DeferredSerialization> m_queuedWrites = new ArrayDeque<DeferredSerialization>();

    /*
     * Return the number of messages waiting to be written to the network
     */
    @Override
    public int getOutstandingMessageCount()
    {
        return m_queuedWrites.size() + super.getOutstandingMessageCount();
    }

    @Override
    public boolean isEmpty()
    {
        return super.isEmpty() && m_queuedWrites.isEmpty();
    }

    protected ArrayDeque<DeferredSerialization> getQueuedWrites() {
        return m_queuedWrites;
    }

    /**
     * Free the pool resources that are held by this WriteStream. The pool itself is thread local
     * and will be freed when the thread terminates.
     */
    synchronized void shutdown() {
        super.shutdown();
        DeferredSerialization ds = null;
        while ((ds = m_queuedWrites.poll()) != null) {
            ds.cancel();
        }
    }

    protected void updateQueued(int queued, boolean noBackpressureSignal) {}

    /**
     * Does the work of queueing addititional buffers that have been serialized
     * and choosing between gathering and regular writes to the channel. Also splits up very large
     * writes of HeapByteBuffers into many smaller writes so Java doesn't allocate a monster DirectByteBuffer
     * that will never be freed
     * @param channel
     * @return
     * @throws IOException
     */
    int drainTo (final GatheringByteChannel channel) throws IOException {
        int bytesWritten = 0;
        long rc = 0;
        do {
            /*
             * Nothing to write
             */
            if (m_currentWriteBuffer == null && m_queuedBuffers.isEmpty()) {
                m_bytesWritten += bytesWritten;
                return bytesWritten;
            }

            ByteBuffer buffer = null;
            if (m_currentWriteBuffer == null) {
                m_currentWriteBuffer = m_queuedBuffers.poll();
                buffer = m_currentWriteBuffer.b;
                buffer.flip();
            } else {
                buffer = m_currentWriteBuffer.b;
            }

            rc = channel.write(buffer);

            //Discard the buffer back to a pool if no data remains
            if (!buffer.hasRemaining()) {
                m_currentWriteBuffer.discard();
                m_currentWriteBuffer = null;
                m_messagesWritten++;
            }
            bytesWritten += rc;

        } while (rc > 0);

        m_bytesWritten += bytesWritten;
        return bytesWritten;
    }

    public void enqueue(DeferredSerialization ds) {
        if (m_isShutdown) {
            ds.cancel();
            return;
        }
        m_queuedWrites.offer(ds);
    }

    public void enqueue(final ByteBuffer buf) {
        assert(!buf.isDirect());//Don't queue direct buffers, they leak memory without a container
        if (buf.remaining() == 0) {
            throw new IllegalArgumentException("Attempted to queue a zero length buffer");
        }
        if (m_isShutdown) return;
        m_queuedWrites.offer(new DeferredSerialization() {

            @Override
            public ByteBuffer[] serialize() throws IOException {
                return new ByteBuffer[] { buf };
            }

            @Override
            public void cancel() {}
        });
    }
}
