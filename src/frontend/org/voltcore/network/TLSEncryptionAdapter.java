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
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.FlexibleSemaphore;
import org.voltcore.utils.Pair;
import org.voltcore.utils.ssl.SSLBufferEncrypter;

import com.google_voltpatches.common.collect.ImmutableList;
import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import io.netty_voltpatches.buffer.ByteBuf;

public class TLSEncryptionAdapter {
    private static final VoltLogger s_networkLog = new VoltLogger("NETWORK");

    private final ConcurrentLinkedDeque<ExecutionException> m_exceptions = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<EncryptFrame> m_encrypted;
    private final FlexibleSemaphore m_inFlight = new FlexibleSemaphore(1);

    private final Connection m_connection;
    private final CipherExecutor m_ce;
    private final SSLEngine m_sslEngine;
    private final SSLBufferEncrypter m_encrypter;
    private final EncryptionGateway m_ecryptgw = new EncryptionGateway();
    private volatile boolean m_isShutdown;

    public TLSEncryptionAdapter(Connection connection,
                                SSLEngine engine,
                                CipherExecutor cipherExecutor,
                                ConcurrentLinkedDeque<EncryptFrame> encryptedDeq) {
        m_connection = connection;
        m_sslEngine = engine;
        m_ce = cipherExecutor;
        m_encrypter = new SSLBufferEncrypter(engine);
        m_encrypted = encryptedDeq;
    }

    /**
     * this values may change if a TLS session renegotiates its cipher suite
     */
    public int applicationBufferSize() {
        return m_sslEngine.getSession().getApplicationBufferSize();
    }

    /**
     * this values may change if a TLS session renegotiates its cipher suite
     */
    public int packetBufferSize() {
        return m_sslEngine.getSession().getPacketBufferSize();
    }

    public Pair<Integer, Integer> encryptBuffers(Deque<DeferredSerialization> buffersToEncrypt, int frameMax) throws IOException {
        ByteBuf accum = m_ce.allocator().buffer(frameMax).clear();

        int processedWrites = 0;
        DeferredSerialization ds = null;
        int bytesQueued = 0;
        int frameMsgs = 0;
        while ((ds = buffersToEncrypt.poll()) != null) {
            ++processedWrites;
            final int serializedSize = ds.getSerializedSize();
            if (serializedSize == DeferredSerialization.EMPTY_MESSAGE_LENGTH) continue;
            // pack as messages you can inside a TLS frame before you send it to
            // the encryption gateway
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
                NIOWriteStreamBase.checkSloppySerialization(jbb, ds);
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
            NIOWriteStreamBase.checkSloppySerialization(jbb, ds);
            accum.writerIndex(accum.writerIndex()+serializedSize);
            ++frameMsgs;
        }
        if (accum.writerIndex() > 0) {
            m_ecryptgw.offer(new EncryptFrame(accum, frameMsgs));
            bytesQueued += accum.writerIndex();
        } else {
            accum.release();
        }

        return new Pair<Integer, Integer>(processedWrites, bytesQueued);
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
                    }
                } catch (InterruptedException e) {
                    throw new IOException("interrupted while waiting for pending encrypts", e);
                }
            }
        } while (!acquired);
    }

    public boolean isEmpty() {
        return m_ecryptgw.isEmpty();
    }

    public void checkForGatewayExceptions() throws IOException {
        ExecutionException ee = m_exceptions.poll();
        if (ee != null) {
            IOException ioe = TLSException.ioCause(ee.getCause());
            if (ioe == null) {
                ioe = new IOException("encrypt task failed", ee.getCause());
            }
            throw ioe;
        }
    }

    String dumpState() {
        return new StringBuilder(256).append("TLSEncryptionAdapter[")
                .append("isEmpty()=").append(isEmpty())
                .append(", exceptions.isEmpty()=").append(m_exceptions.isEmpty())
                .append(", gateway=").append(m_ecryptgw.dumpState())
                .append(", inFlight=").append(m_inFlight.availablePermits())
                .append("]").toString();
    }

    // Only called from synchronized block
    void shutdown() {
        m_isShutdown = true;
        try {
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
        private final int COALESCE_THRESHOLD = CipherExecutor.FRAME_SIZE - 4096;

        synchronized void offer(EncryptFrame frame) throws IOException {
            final boolean wasEmpty = m_q.isEmpty();

            List<EncryptFrame> chunks = frame.chunked(
                    Math.min(CipherExecutor.FRAME_SIZE, applicationBufferSize()));

            m_q.addAll(chunks);
            m_inFlight.reducePermits(chunks.size());

            if (wasEmpty) {
                submitSelf();
            }
        }

        /**
         * Encryption takes time and the likelihood that some or more encrypt frames
         * are queued up behind the recently completed frame encryption is high. This
         * takes queued up small frames and coalesces them into a bigger frame
         */
        //Called from synchronized block only
        private void coalesceEncryptFrames() {
            EncryptFrame head = m_q.peek();
            if (head == null || head.chunks > 1 || head.bb.readableBytes() > COALESCE_THRESHOLD) {
                return;
            }

            m_q.poll();
            ByteBuf bb = head.bb;
            int msgs = head.msgs;
            int released = 0;

            head = m_q.peek();
            while (head != null && head.chunks == 1 && head.bb.readableBytes() <= bb.writableBytes()) {
                m_q.poll();
                bb.writeBytes(head.bb, head.bb.readableBytes());
                head.bb.release();
                ++released;
                msgs += head.msgs;
                head = m_q.peek();
            }
            m_q.push(new EncryptFrame(bb, 0, msgs));
            if (released > 0) {
                m_inFlight.release(released);
            }
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
                s_networkLog.error("failed to encrypt frame", e);
                m_connection.enableWriteSelection();
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
                    m_connection.enableWriteSelection();
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
                coalesceEncryptFrames();
                if (m_q.peek() != null && !m_isShutdown) {
                    submitSelf();
                }
            }
        }

        boolean isEmpty() {
            return m_q.isEmpty();
        }

        void submitSelf() {
            ListenableFuture<?> fut = m_ce.submit(this);
            fut.addListener(new ExceptionListener(fut), CoreUtils.LISTENINGSAMETHREADEXECUTOR);
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
                s_networkLog.error("unexpect fault occurred in encrypt task", e.getCause());
                m_exceptions.offer(e);
            }
        }
    }

}
