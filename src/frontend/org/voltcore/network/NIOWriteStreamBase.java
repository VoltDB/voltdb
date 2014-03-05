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
public abstract class NIOWriteStreamBase {
    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    protected boolean m_isShutdown = false;

    protected BBContainer m_currentWriteBuffer = null;

    /**
     * Contains serialized buffers ready to write to the socket
     */
    protected final ArrayDeque<BBContainer> m_queuedBuffers = new ArrayDeque<BBContainer>();

    protected long m_bytesWritten = 0;
    protected long m_messagesWritten = 0;

    /*
     * Used to provide incremental reads of the amount of
     * data written.
     * Stats now use a separate lock and risk reading dirty data
     * rather than risk deadlock by acquiring the lock of the writestream.
     */
    private long m_lastBytesWritten = 0;
    private long m_lastMessagesWritten = 0;

    long[] getBytesAndMessagesWritten(boolean interval) {
        if (interval) {
            final long bytesWrittenThisTime = m_bytesWritten - m_lastBytesWritten;
            m_lastBytesWritten = m_bytesWritten;

            final long messagesWrittenThisTime = m_messagesWritten - m_lastMessagesWritten;
            m_lastMessagesWritten = m_messagesWritten;
            return new long[] { bytesWrittenThisTime, messagesWrittenThisTime };
        } else {
            return new long[] {m_bytesWritten, m_messagesWritten};
        }
    }

    /*
     * Return the number of messages waiting to be written to the network
     */
    public int getOutstandingMessageCount()
    {
        return m_queuedBuffers.size();
    }

    public boolean isEmpty()
    {
        return m_queuedBuffers.isEmpty() && m_currentWriteBuffer == null;
    }

    abstract int drainTo (final GatheringByteChannel channel) throws IOException;

    protected abstract ArrayDeque<DeferredSerialization> getQueuedWrites();

    /**
     * Swap the two queues of DeferredSerializations and serialize everything in the queue
     * and return the resulting ByteBuffers as an array.
     * @return
     * @throws IOException
     */
    final void swapAndSerializeQueuedWrites(final NetworkDBBPool pool) throws IOException {
        final ArrayDeque<DeferredSerialization> oldlist = getQueuedWrites();
        if (oldlist.isEmpty()) return;

        DeferredSerialization ds = null;
        int bytesQueued = 0;
        while ((ds = oldlist.poll()) != null) {
            final int serializedSize = ds.getSerializedSize();
            if (serializedSize == -1) continue;
            BBContainer outCont = m_queuedBuffers.peekLast();
            if (outCont == null || !outCont.b.hasRemaining()) {
                outCont = pool.acquire();
                outCont.b.clear();
                m_queuedBuffers.offer(outCont);
            }
            //Fastpath, serialize to direct buffer creating no garbage
            if (outCont.b.remaining() >= serializedSize) {
                final ByteBuffer outbuf = outCont.b;
                final int oldLimit = outbuf.limit();
                outbuf.limit(outbuf.position() + serializedSize);
                final ByteBuffer slice = outbuf.slice();
                ds.serialize(slice);
                checkSloppySerialization(slice, ds);
                slice.position(0);
                bytesQueued += slice.remaining();
                outbuf.position(outbuf.limit());
                outbuf.limit(oldLimit);
            } else {
                //Slow path serialize to heap, and then put in buffers
                ByteBuffer buf = ByteBuffer.allocate(ds.getSerializedSize());
                ds.serialize(buf);
                checkSloppySerialization(buf, ds);
                buf.position(0);
                bytesQueued += buf.remaining();
                while (buf.hasRemaining()) {
                    if (!outCont.b.hasRemaining()) {
                        outCont = pool.acquire();
                        outCont.b.clear();
                        m_queuedBuffers.offer(outCont);
                    }
                    if (outCont.b.remaining() >= buf.remaining()) {
                        outCont.b.put(buf);
                    } else {
                        final int oldLimit = buf.limit();
                        buf.limit(buf.position() + outCont.b.remaining());
                        outCont.b.put(buf);
                        buf.limit(oldLimit);
                    }
                }
            }
        }
        updateQueued(bytesQueued, true);
    }

    private void checkSloppySerialization(ByteBuffer buf, DeferredSerialization ds) {
        if (buf.limit() != buf.capacity()) {
            boolean assertOn = false;
            assert (assertOn = true);
            if (assertOn) {
                networkLog.fatal("Sloppy serialization size for message class " + ds);
                System.exit(-1);
            }
            RateLimitedLogger.tryLogForMessage(
                    "Sloppy serialization size for message class " + ds,
                    System.currentTimeMillis(),
                    1, TimeUnit.HOURS,
                    networkLog,
                    Level.WARN);
        }
    }

    /**
     * Free the pool resources that are held by this WriteStream. The pool itself is thread local
     * and will be freed when the thread terminates.
     */
    void shutdown() {
        int bytesReleased = 0;
        m_isShutdown = true;
        BBContainer c = null;
        if (m_currentWriteBuffer != null) {
            bytesReleased += m_currentWriteBuffer.b.remaining();
            m_currentWriteBuffer.discard();
        }
        while ((c = m_queuedBuffers.poll()) != null) {
            //Buffer is not flipped after being written to in swap and serialize, need to do it here
            c.b.flip();
            bytesReleased += c.b.remaining();
            c.discard();
        }
        updateQueued(-bytesReleased, false);
    }

    protected abstract void updateQueued(int queued, boolean noBackpressureSignal);
}
