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
public class NIOWriteStream implements WriteStream {

    /**
     * Reference to the port for changing interest ops
     */
    private final VoltPort m_port;

    private static final VoltLogger networkLog = new VoltLogger("NETWORK");

    private boolean m_isShutdown = false;

    private BBContainer m_currentWriteBuffer = null;

    /**
     * Contains serialized buffers ready to write to the socket
     */
    private final ArrayDeque<BBContainer> m_queuedBuffers = new ArrayDeque<BBContainer>();

    /**
     * Contains messages waiting to be serialized and written to the socket
     */
    private final ArrayDeque<DeferredSerialization> m_queuedWrites1 =
        new ArrayDeque<DeferredSerialization>();

    private final ArrayDeque<DeferredSerialization> m_queuedWrites2 =
        new ArrayDeque<DeferredSerialization>();

    private ArrayDeque<DeferredSerialization> m_queuedWrites = m_queuedWrites1;

    private final int m_maxQueuedWritesBeforeBackpressure = 100;

    private final Runnable m_offBackPressureCallback;
    private final Runnable m_onBackPressureCallback;

    private final QueueMonitor m_monitor;

    private long m_bytesWritten = 0;
    private long m_messagesWritten = 0;

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

    /**
     * Set to -1 when there are no pending writes. If there is a pending write it is set to the time
     * of the last successful write or the time the oldest pending write was queued.
     */
    private long m_lastPendingWriteTime = -1;

    NIOWriteStream(VoltPort port) {
        this(port, null, null, null);
    }

    NIOWriteStream (
            VoltPort port,
            Runnable offBackPressureCallback,
            Runnable onBackPressureCallback,
            QueueMonitor monitor)
    {
        m_port = port;
        m_offBackPressureCallback = offBackPressureCallback;
        m_onBackPressureCallback = onBackPressureCallback;
        m_monitor = monitor;

    }

    /*
     * Return the number of messages waiting to be written to the network
     */
    @Override
    synchronized public int getOutstandingMessageCount()
    {
        return m_queuedWrites.size() + m_queuedBuffers.size();
    }

    @Override
    synchronized public boolean isEmpty()
    {
        return m_queuedBuffers.isEmpty() && m_queuedWrites.isEmpty() && m_currentWriteBuffer == null;
    }

    /**
     * Returns true when a drainTo invocation was unable to completely drain all queued bytes ||
     * > 1000 writes are queued
     */
    @Override
    public boolean hadBackPressure() {
        return m_hadBackPressure;
    }



    /**
     * Called when not all queued data could be flushed to the channel
     */
    private final void backpressureStarted() {
        if (networkLog.isTraceEnabled()) {
            networkLog.trace("Backpressure started for client " + m_port);
        }
        if (m_hadBackPressure == false) {
            m_hadBackPressure = true;
            if (m_onBackPressureCallback != null) {
                m_onBackPressureCallback.run();
            }
        }
    }

    /**
     * Called when all queued data is flushed to the channel
     */
    private final void backpressureEnded() {
        if (networkLog.isTraceEnabled()) {
            networkLog.trace("Backpressure ended for client " + m_port);
        }
        if (m_hadBackPressure == true) {
            m_hadBackPressure = false;
            if (m_offBackPressureCallback != null) {
                m_offBackPressureCallback.run();
            }
        }
    }
    /**
     * Boolean used to store the latest back pressure state
     * Does this need to be volatile?
     */
    private volatile boolean m_hadBackPressure = false;

    /**
     * Does the work of queueing addititional buffers that have been serialized
     * and choosing between gathering and regular writes to the channel. Also splits up very large
     * writes of HeapByteBuffers into many smaller writes so Java doesn't allocate a monster DirectByteBuffer
     * that will never be freed
     * @param channel
     * @param additional
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
                if (m_hadBackPressure && m_queuedWrites.size() <= m_maxQueuedWritesBeforeBackpressure) {
                    backpressureEnded();
                }
                m_lastPendingWriteTime = -1;
                updateQueued(-bytesWritten, false);
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

            rc = 0;
            rc = channel.write(buffer);

            //Discard the buffer back to a pool if no data remains
            if (buffer.hasRemaining()) {
                if (!m_hadBackPressure) {
                    backpressureStarted();
                }
            } else {
                m_currentWriteBuffer.discard();
                m_currentWriteBuffer = null;
                m_messagesWritten++;
            }
            bytesWritten += rc;

        } while (rc > 0);

        //This extra check is necessary because sometimes a buffer with nothing remaining
        //has to be queued in the above loop resulting in rc == 0. Since rc == 0
        //it won't loop around a last time and see that there are no more queued buffers
        //and thus no backpressure
        if (m_queuedBuffers.isEmpty() && m_hadBackPressure && m_queuedWrites.size() <= m_maxQueuedWritesBeforeBackpressure) {
            backpressureEnded();
        }

        if (!isEmpty()) {
            if (bytesWritten > 0) {
                m_lastPendingWriteTime = EstTime.currentTimeMillis();
            }
        } else {
            m_lastPendingWriteTime = -1;
        }
        updateQueued(-bytesWritten, false);
        m_bytesWritten += bytesWritten;
        return bytesWritten;
    }


    /**
     * Queue a message and defer the serialization of the message until later. This is the ideal mechanism
     * for serializing and queueing network writes. It allows the sender to define an efficient serialization
     * mechanism that performs a single allocation of the correct size without the overhead of FastSerializer
     * which has to constantly check if it needs to grow.
     * @param ds A deferred serialization task that will generate the message
     */
    @Override
    public void enqueue(final DeferredSerialization ds) {
        synchronized (this) {
            if (m_isShutdown) {
                ds.cancel();
                return;
            }
            updateLastPendingWriteTimeAndQueueBackpressure();
            m_queuedWrites.offer(ds);
            m_port.setInterests( SelectionKey.OP_WRITE, 0);
        }
        return;
    }

    @Override
    public void enqueue(final ByteBuffer b) {
        enqueue(new ByteBuffer[] { b });
    }

    /**
     * Queue a ByteBuffer for writing to the network. If the ByteBuffer is not direct then it will
     * be copied to a DirectByteBuffer if it is less then DBBPool.MAX_ALLOCATION_SIZE. This method
     * is a backup for code that isn't able to defer its serialization to a network thread
     * for whatever reason. It is reasonably efficient if a DirectByteBuffer is passed in,
     * but it would be better to keep allocations of DirectByteBuffers inside the network pools.
     * @param b
     */
    @Override
    public void enqueue(final ByteBuffer b[]) {
        assert(b != null);
        for (ByteBuffer buf : b) {
            assert(!buf.isDirect());//Don't queue direct buffers, they leak memory without a container
            if (buf.remaining() == 0) {
                throw new IllegalArgumentException("Attempted to queue a zero length buffer");
            }
        }

        synchronized (this) {
            if (m_isShutdown) {
                return;
            }

            updateLastPendingWriteTimeAndQueueBackpressure();

            m_queuedWrites.offer(new DeferredSerialization() {
                @Override
                public ByteBuffer[] serialize() {
                    return b;
                }

                @Override
                public void cancel() {}
            });
            m_port.setInterests( SelectionKey.OP_WRITE, 0);
        }
        return;
    }

    /**
     * Swap the two queues of DeferredSerializations and serialize everything in the queue
     * and return the resulting ByteBuffers as an array.
     * @return
     * @throws IOException
     */
    final void swapAndSerializeQueuedWrites(final NetworkDBBPool pool) throws IOException {
        ArrayDeque<DeferredSerialization> oldlist;
        synchronized (this) {
            if (m_queuedWrites.isEmpty()) {
                return;
            } else {
                if (m_queuedWrites == m_queuedWrites1) {
                    oldlist = m_queuedWrites1;
                    m_queuedWrites = m_queuedWrites2;
                }
                else {
                    oldlist = m_queuedWrites2;
                    m_queuedWrites = m_queuedWrites1;
                }
            }
        }

        DeferredSerialization ds = null;
        int bytesQueued = 0;
        while ((ds = oldlist.poll()) != null) {
            ByteBuffer data[] = ds.serialize();
            for (ByteBuffer buf : data) {
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
                buf.position(0);
                bytesQueued += buf.remaining();
                while (buf.hasRemaining()) {
                    BBContainer outCont = m_queuedBuffers.peekLast();
                    if (outCont == null || !outCont.b.hasRemaining()) {
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

    /**
     * Free the pool resources that are held by this WriteStream. The pool itself is thread local
     * and will be freed when the thread terminates.
     */
    synchronized void shutdown() {
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
        DeferredSerialization ds = null;
        while ((ds = m_queuedWrites.poll()) != null) {
            ds.cancel();
        }
    }

    @Override
    public synchronized int calculatePendingWriteDelta(final long now) {
        if (m_lastPendingWriteTime == -1) {
            return 0;
        }
        return (int)(now - m_lastPendingWriteTime);
    }

    private void updateLastPendingWriteTimeAndQueueBackpressure() {
        if (m_lastPendingWriteTime == -1) {
            m_lastPendingWriteTime = EstTime.currentTimeMillis();
        }
        if (m_queuedWrites.size() > m_maxQueuedWritesBeforeBackpressure && !m_hadBackPressure) {
            backpressureStarted();
        }
    }

    private void updateQueued(int queued, boolean noBackpressureSignal) {
        if (m_monitor != null) {
            boolean shouldSignalBackpressure = m_monitor.queue(queued);
            if (!noBackpressureSignal && shouldSignalBackpressure) {
                if (!m_hadBackPressure) {
                    backpressureStarted();
                }
            }
        }
    }
}
