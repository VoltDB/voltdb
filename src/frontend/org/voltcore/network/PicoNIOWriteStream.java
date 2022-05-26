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
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayDeque;

import org.voltcore.utils.DeferredSerialization;

/**
*
* Write stream implementation optimized for use on the server. Does no locking and
* backpressure tracking
*
*/
public class PicoNIOWriteStream extends NIOWriteStreamBase {

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

    @Override
    protected ArrayDeque<DeferredSerialization> getQueuedWrites() {
        return m_queuedWrites;
    }

    /**
     * Free the pool resources that are held by this WriteStream. The pool itself is thread local
     * and will be freed when the thread terminates.
     */
    @Override
    synchronized void shutdown() {
        super.shutdown();
        DeferredSerialization ds = null;
        while ((ds = m_queuedWrites.poll()) != null) {
            ds.cancel();
        }
    }

    @Override
    protected void updateQueued(int queued, boolean noBackpressureSignal) {}

    /**
     * Drain pending buffers one at a time into the socket
     * @param channel
     * @return
     * @throws IOException
     */
    @Override
    int drainTo (final GatheringByteChannel channel) throws IOException {
        int bytesWritten = 0;
        long rc = 0;
        do {
            /*
             * Nothing to write
             */
            if (m_currentWriteBuffer == null && m_queuedBuffers.isEmpty()) {
                break;
            }

            ByteBuffer buffer = null;
            if (m_currentWriteBuffer == null) {
                m_currentWriteBuffer = m_queuedBuffers.poll();
                buffer = m_currentWriteBuffer.b();
                buffer.flip();
            } else {
                buffer = m_currentWriteBuffer.b();
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
            public void serialize(final ByteBuffer outbuf) throws IOException {
                outbuf.put(buf);
            }

            @Override
            public void cancel() {}

            @Override
            public int getSerializedSize() {
                return buf.remaining();
            }
        });
    }
}
