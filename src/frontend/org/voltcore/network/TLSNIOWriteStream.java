/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.EstTime;
import org.voltcore.utils.FlexibleSemaphore;
import org.voltcore.utils.ssl.SSLBufferEncrypter;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import io.netty_voltpatches.buffer.ByteBuf;
import io.netty_voltpatches.buffer.CompositeByteBuf;
import io.netty_voltpatches.buffer.Unpooled;

public class TLSNIOWriteStream extends NIOWriteStream {

    private final ConcurrentLinkedDeque<ExecutionException> m_exceptions = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<EncryptFrame> m_encrypted = new ConcurrentLinkedDeque<>();
    private final FlexibleSemaphore m_inFlight = new FlexibleSemaphore(1);

    private final CompositeByteBuf m_outbuf;
    private final CipherExecutor m_ce;
    private final SSLEngine m_sslEngine;
    private final SSLBufferEncrypter m_encrypter;
    private final EncryptionGateway m_ecryptgw = new EncryptionGateway();
    private int m_ledger = 0;

    public TLSNIOWriteStream(VoltPort port, Runnable offBackPressureCallback,
            Runnable onBackPressureCallback, QueueMonitor monitor,
            SSLEngine engine, CipherExecutor cipherExecutor) {
        super(port, offBackPressureCallback, onBackPressureCallback, monitor);
        m_sslEngine = engine;
        m_ce = cipherExecutor;
        m_outbuf = Unpooled.compositeBuffer();
        m_encrypter = new SSLBufferEncrypter(engine);
    }

    private int applicationBufferSize() {
        return m_sslEngine.getSession().getApplicationBufferSize();
    }

    private int packetBufferSize() {
        return m_sslEngine.getSession().getPacketBufferSize();
    }

    @Override
    int serializeQueuedWrites(NetworkDBBPool pool) throws IOException {
        checkForGatewayExceptions();

        final int frameMax = Math.min(CipherExecutor.PAGE_SIZE, applicationBufferSize());
        int processedWrites = 0;
        final Deque<DeferredSerialization> oldlist = getQueuedWrites();
        if (oldlist.isEmpty()) return 0;

        ByteBuf accum = m_ce.allocator().buffer(frameMax).clear();

        DeferredSerialization ds = null;
        int bytesQueued = 0;
        int frameMsgs = 0;
        while ((ds = oldlist.poll()) != null) {
            ++processedWrites;
            final int serializedSize = ds.getSerializedSize();
            if (serializedSize == DeferredSerialization.EMPTY_MESSAGE_LENGTH) continue;
            if (serializedSize > frameMax) {
                // frames may contain only one or more whole messages, or only
                // partial parts of one message. a message may not contain whole
                // messages and an incomplete partial fragment of one
                if (accum.writerIndex() > 0) {
                    m_ecryptgw.offer(new EncryptFrame(accum, frameMsgs));
                    frameMsgs = 0;
                    bytesQueued += accum.writerIndex();
                    accum = m_ce.allocator().buffer(frameMax).clear();
                }
                ByteBuf big = m_ce.allocator().buffer(serializedSize).writerIndex(serializedSize);
                ByteBuffer jbb = big.nioBuffer();
                ds.serialize(jbb);
                checkSloppySerialization(jbb, ds);
                bytesQueued += big.writerIndex();
                m_ecryptgw.offer(new EncryptFrame(big, 1));
                frameMsgs = 0;
                continue;
            } else if (accum.writerIndex() + serializedSize > frameMax) {
                m_ecryptgw.offer(new EncryptFrame(accum, frameMsgs));
                frameMsgs = 0;
                bytesQueued += accum.writerIndex();
                accum = m_ce.allocator().buffer(frameMax).clear();
            }
            ByteBuf packet = accum.slice(accum.writerIndex(), serializedSize);
            ByteBuffer jbb = packet.nioBuffer();
            ds.serialize(jbb);
            checkSloppySerialization(jbb, ds);
            accum.writerIndex(accum.writerIndex()+serializedSize);
            ++frameMsgs;
        }
        if (accum.writerIndex() > 0) {
            m_ecryptgw.offer(new EncryptFrame(accum, frameMsgs));
            bytesQueued += accum.writerIndex();
        } else {
            accum.release();
        }
        updateQueued(bytesQueued, true);
        return processedWrites;
    }

    @Override
    public void updateQueued(int queued, boolean noBackpressureSignal) {
        super.updateQueued(queued, noBackpressureSignal);
        m_ledger += queued;
    }

    void waitForPendingEncrypts() throws IOException {
        boolean acquired;

        do {
            int waitFor = 1 - m_inFlight.availablePermits();
            acquired = waitFor == 0;
            for (int i = 0; i < waitFor && !acquired; ++i) {
                checkForGatewayExceptions();
                try {
                    acquired = m_inFlight.tryAcquire(1, TimeUnit.SECONDS);
                    if (acquired) {
                        m_inFlight.release();
                    } else if (!m_ce.isActive()) {
                        while (!m_ecryptgw.isEmpty()) {
                            m_ecryptgw.run();
                            checkForGatewayExceptions();
                        }
                    }
                } catch (InterruptedException e) {
                    throw new IOException("interrupted while waiting for pending encrypts", e);
                }
            }
        } while (!acquired);
    }

    private final List<EncryptFrame> m_partial = new ArrayList<>();
    private volatile int m_partialSize = 0;
    private final static int NONE_ADDED = Integer.MIN_VALUE;

    private int addFramesForCompleteMessage() {
        boolean added = false;
        EncryptFrame frame = null;
        int delta = 0;

        while (!added && (frame = m_encrypted.poll()) != null) {
            if (!frame.isLast()) {
                synchronized(m_partial) {
                    m_partial.add(frame);
                    ++m_partialSize;
                }
                continue;
            }

            final int partialSize = m_partialSize;
            if (partialSize > 0) {
                assert frame.chunks == partialSize + 1
                        : "partial frame buildup has wrong number of preceeding pieces";

                synchronized(m_partial) {
                    for (EncryptFrame frm: m_partial) {
                        delta += frm.delta;
                        m_outbuf.addComponent(true, frm.frame);
                    }
                    m_partial.clear();
                    m_partialSize = 0;
                }
            }
            delta += frame.delta;
            m_outbuf.addComponent(true, frame.frame);
            m_messagesInOutBuf += frame.msgs;
            added = true;
        }

        return added ? delta : NONE_ADDED;
    }

    @Override
    synchronized public boolean isEmpty() {
        return m_queuedWrites.isEmpty()
            && m_ecryptgw.isEmpty()
            && m_encrypted.isEmpty()
            && m_partialSize == 0
            && !m_outbuf.isReadable();
    }

    private void checkForGatewayExceptions() throws IOException {
        ExecutionException ee = m_exceptions.poll();
        if (ee != null) {
            IOException ioe = TLSException.ioCause(ee.getCause());
            if (ioe == null) {
                ioe = new IOException("encrypt task failed", ee.getCause());
            }
            throw ioe;
        }
    }

    private int m_messagesInOutBuf = 0;

    @Override
    int drainTo(final GatheringByteChannel channel) throws IOException {
        int bytesWritten = 0;
        int accumdelta = 0;
        try {
            long rc = 0;
            do {
                checkForGatewayExceptions();
                int delta = 0;
                // add to output buffer frames that contain whole messages
                while ((delta=addFramesForCompleteMessage()) != NONE_ADDED) {
                    accumdelta += delta;
                }

                rc = m_outbuf.readBytes(channel, m_outbuf.readableBytes());
                m_outbuf.discardReadComponents();
                bytesWritten += rc;

                if (m_outbuf.isReadable()) {
                    if (!m_hadBackPressure) {
                        backpressureStarted();
                    }
                } else if (rc > 0) {
                    m_messagesWritten += m_messagesInOutBuf;
                    m_messagesInOutBuf = 0;
                }

            } while (rc > 0);
        } finally {
            if (m_outbuf.numComponents() <= 1 && m_hadBackPressure && m_queuedWrites.size() <= m_maxQueuedWritesBeforeBackpressure) {
                backpressureEnded();
            }
            if (bytesWritten > 0 && !isEmpty()) {
                m_lastPendingWriteTime = EstTime.currentTimeMillis();
            } else {
                m_lastPendingWriteTime = -1L;
            }
            if (bytesWritten > 0) {
                updateQueued(accumdelta-bytesWritten, false);
                m_bytesWritten += bytesWritten;
            }
        }
        return bytesWritten;
    }

    String dumpState() {
        return new StringBuilder(256).append("TLSNIOWriteStream[")
                .append("isEmpty()=").append(isEmpty())
                .append(", encrypted.isEmpty()=").append(m_encrypted.isEmpty())
                .append(", exceptions.isEmpty()=").append(m_exceptions.isEmpty())
                .append(", gateway=").append(m_ecryptgw.dumpState())
                .append(", inFligth=").append(m_inFlight.availablePermits())
                .append(", outbuf.readableBytes()=").append(m_outbuf.readableBytes())
                .append("]").toString();
    }

    @Override
    public synchronized int getOutstandingMessageCount() {
        return m_encrypted.size()
             + m_queuedWrites.size()
             + m_partialSize
             + m_outbuf.numComponents(); // > 1 ? m_outbuf.numComponents() - 1 : 0;
    }

    @Override
    synchronized void shutdown() {
        m_isShutdown = true;
        try {
            DeferredSerialization ds = null;
            while ((ds = m_queuedWrites.poll()) != null) {
                ds.cancel();
            }

            int waitFor = 1 - Math.min(m_inFlight.availablePermits(), -4);
            for (int i = 0; i < waitFor; ++i) {
                try {
                    if (m_inFlight.tryAcquire(1, TimeUnit.SECONDS)) {
                        m_inFlight.release();
                        break;
                    }
                } catch (InterruptedException e) {
                    break;
                }
            }

            m_ecryptgw.die();

            EncryptFrame frame = null;
            while ((frame = m_encrypted.poll()) != null) {
                frame.frame.release();
            }

            for (EncryptFrame ef: m_partial) {
                ef.frame.release();
            }
            m_partial.clear();

            m_outbuf.release();

            // we have to use ledger because we have no idea how much encrypt delta
            // corresponds to what is left in the output buffer
            final int unqueue = -m_ledger;
            updateQueued(unqueue, false);
        } finally {
            m_inFlight.drainPermits();
            m_inFlight.release();
        }
    }

    /**
     * Construct used to serialize all the encryption tasks for this stream.
     * it takes an encryption request offer, divides it into chunks that
     * can be handled wholly by SSLEngine wrap, and queues all the
     * encrypted frames to the m_encrypted queue. All faults are queued
     * to the m_exceptions queue
     */
    class EncryptionGateway implements Runnable {
        private final ConcurrentLinkedDeque<EncryptFrame> m_q = new ConcurrentLinkedDeque<>();
        private long m_offered = 0L;

        synchronized void offer(EncryptFrame frame) throws IOException {
            final boolean wasEmpty = m_q.isEmpty();
            List<EncryptFrame> chunks = frame.chunked(Math.min(CipherExecutor.PAGE_SIZE, applicationBufferSize()));

            m_q.addAll(chunks);
            ++m_offered;
            m_inFlight.reducePermits(chunks.size());

            if (wasEmpty && m_ce.isActive()) {
                try {
                    ListenableFuture<?> fut = m_ce.getES().submit(this);
                    fut.addListener(new ExceptionListener(fut), CoreUtils.SAMETHREADEXECUTOR);
                } catch (RejectedExecutionException executorIsShuttingDown) {
                    run();
                    checkForGatewayExceptions();
                }
            } else if (wasEmpty && !m_ce.isActive()) {
                while (!isEmpty()) {
                    run();
                    checkForGatewayExceptions();
                }
            }
        }

        void runTaskSynchronously() throws IOException {
            run();
            checkForGatewayExceptions();
        }

        synchronized int die() {
            int toUnqueue = 0;
            EncryptFrame ef = null;
            while ((ef = m_q.poll()) != null) {
                toUnqueue += ef.frame.readableBytes();
                if (ef.isLast()) {
                    ef.bb.release();
                }
            }
            return toUnqueue;
        }

        String dumpState() {
            return new StringBuilder(256).append("EncryptionGateway[")
                    .append("q.isEmpty()=").append(m_q.isEmpty())
                    .append(", partialSize=").append(m_partialSize)
                    .append(", offered=").append(m_offered)
                    .append("]").toString();
        }

        public Iterator<EncryptFrame> iterator() {
            return ImmutableList.copyOf(m_q).iterator();
        }

        @Override
        public void run() {
            EncryptFrame frame = m_q.peek();
            if (frame == null) return;

            ByteBuffer src = frame.frame.nioBuffer();
            ByteBuf encr = m_ce.allocator().ioBuffer(packetBufferSize()).writerIndex(packetBufferSize());
            ByteBuffer dest = encr.nioBuffer();

            try {
                m_encrypter.tlswrap(src, dest);
            } catch (TLSException e) {
                m_inFlight.release();
                encr.release();
                m_exceptions.offer(new ExecutionException("failed to encrypt frame", e));
                networkLog.error("failed to encrypt frame", e);
                m_port.enableWriteSelection();
                return;
            }
            assert !src.hasRemaining() : "encryption wrap did not consume the whole source buffer";
            int delta = dest.limit() - frame.frame.readableBytes();
            encr.writerIndex(dest.limit());

            if (!m_isShutdown) {
                m_encrypted.offer(frame.encrypted(delta, encr));
                /*
                 * All interactions with write stream must be protected
                 * with a lock to ensure that interests ops are consistent with
                 * the state of writes queued to the stream. This prevent
                 * lost queued writes where the write is queued
                 * but the write interest op is not set.
                 */
                if (frame.isLast()) {
                    m_port.enableWriteSelection();
                }
            } else {
                encr.release();
                return;
            }
            synchronized(this) {
                m_q.poll();
                if (frame.isLast()) {
                    frame.bb.release();
                }
                m_inFlight.release();
                if (m_q.peek() != null && !m_isShutdown && m_ce.isActive()) {
                    ListenableFuture<?> fut = m_ce.getES().submit(this);
                    fut.addListener(new ExceptionListener(fut), CoreUtils.SAMETHREADEXECUTOR);
                }
            }
        }

        boolean isEmpty() {
            return m_q.isEmpty();
        }
    }

    class ExceptionListener implements Runnable {
        private final ListenableFuture<?> m_fut;
        private ExceptionListener(ListenableFuture<?> fut) {
            m_fut = fut;
        }
        @Override
        public void run() {
            if (!m_isShutdown) return;
            try {
                m_fut.get();
            } catch (InterruptedException notPossible) {
            } catch (ExecutionException e) {
                m_inFlight.release();
                networkLog.error("unexpect fault occurred in encrypt task", e.getCause());
                m_exceptions.offer(e);
            }
        }
    }

}
