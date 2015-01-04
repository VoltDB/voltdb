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
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayDeque;
import java.util.concurrent.TimeUnit;

import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.DBBPool.BBContainer;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.RateLimitedLogger;

/**
*
* Base class for tracking pending writes to a socket and dealing with serializing them
* to a series of buffers. Actually draining to a socket is delegated to subclasses as
* is queuing writes which have different locking and backpressure policies
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
     * Swap the two queues of DeferredSerializations and serialize everything into the queue
     * of pending buffers
     * @return
     * @throws IOException
     */
    final int swapAndSerializeQueuedWrites(final NetworkDBBPool pool) throws IOException {
        int processedWrites = 0;
        final ArrayDeque<DeferredSerialization> oldlist = getQueuedWrites();
        if (oldlist.isEmpty()) return 0;

        DeferredSerialization ds = null;
        int bytesQueued = 0;
        while ((ds = oldlist.poll()) != null) {
            processedWrites++;
            final int serializedSize = ds.getSerializedSize();
            if (serializedSize == DeferredSerialization.EMPTY_MESSAGE_LENGTH) continue;
            BBContainer outCont = m_queuedBuffers.peekLast();
            ByteBuffer outbuf = null;
            if (outCont == null || !outCont.b().hasRemaining()) {
                outCont = pool.acquire();
                outCont.b().clear();
                m_queuedBuffers.offer(outCont);
            }

            outbuf = outCont.b();

            //Fastpath, serialize to direct buffer creating no garbage
            if (outbuf.remaining() >= serializedSize) {
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
                ByteBuffer buf = ByteBuffer.allocate(serializedSize);
                ds.serialize(buf);
                checkSloppySerialization(buf, ds);
                buf.position(0);
                bytesQueued += buf.remaining();
                while (buf.hasRemaining()) {
                    if (!outbuf.hasRemaining()) {
                        outCont = pool.acquire();
                        outbuf = outCont.b();
                        outbuf.clear();
                        m_queuedBuffers.offer(outCont);
                    }
                    if (outbuf.remaining() >= buf.remaining()) {
                        outbuf.put(buf);
                    } else {
                        final int oldLimit = buf.limit();
                        buf.limit(buf.position() + outbuf.remaining());
                        outbuf.put(buf);
                        buf.limit(oldLimit);
                    }
                }
            }
        }
        updateQueued(bytesQueued, true);
        return processedWrites;
    }

    private static final boolean ASSERT_ON;
    static {
        boolean assertOn = false;
        assert (assertOn = true);
        ASSERT_ON = assertOn;
    }

    /*
     * Validate that serialization is accurately reporting the amount of data necessary
     * to serialize the message
     */
    private void checkSloppySerialization(ByteBuffer buf, DeferredSerialization ds) {
        if (buf.limit() != buf.capacity()) {
            if (ASSERT_ON) {
                networkLog.fatal("Sloppy serialization size for message class " + ds);
                System.exit(-1);
            }
            RateLimitedLogger.tryLogForMessage(
                    System.currentTimeMillis(),
                    1, TimeUnit.HOURS,
                    networkLog,
                    Level.WARN, "Sloppy serialization size for message class %s", ds);
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
            bytesReleased += m_currentWriteBuffer.b().remaining();
            m_currentWriteBuffer.discard();
        }
        while ((c = m_queuedBuffers.poll()) != null) {
            //Buffer is not flipped after being written to in swap and serialize, need to do it here
            c.b().flip();
            bytesReleased += c.b().remaining();
            c.discard();
        }
        updateQueued(-bytesReleased, false);
    }

    /*
     * Track bytes queued for backpressure
     */
    protected abstract void updateQueued(int queued, boolean noBackpressureSignal);
}
