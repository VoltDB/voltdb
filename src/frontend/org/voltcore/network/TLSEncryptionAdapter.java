/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.GatheringByteChannel;
import java.util.ArrayList;
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
import io.netty_voltpatches.buffer.CompositeByteBuf;
import io.netty_voltpatches.buffer.Unpooled;

public class TLSEncryptionAdapter {
    private static final VoltLogger s_networkLog = new VoltLogger("NETWORK");

    private final ConcurrentLinkedDeque<ExecutionException> m_exceptions = new ConcurrentLinkedDeque<>();
    // Input frames encrypted as they came in
    private final ConcurrentLinkedDeque<EncryptFrame> m_encryptedFrames = new ConcurrentLinkedDeque<>();
    // Frames that form full messages
    private final CompositeByteBuf m_encryptedMessages;
    // Number of full messages in encryptedMessages
    private int m_numEncryptedMessages = 0;
    private final List<EncryptFrame> m_partialMessages = new ArrayList<>();
    private volatile int m_partialSize = 0;

    private final FlexibleSemaphore m_inFlight = new FlexibleSemaphore(1);

    private final Connection m_connection;
    private final CipherExecutor m_ce;
    private final SSLEngine m_sslEngine;
    private final SSLBufferEncrypter m_encrypter;
    private final EncryptionGateway m_ecryptgw = new EncryptionGateway();
    private volatile boolean m_isShutdown;

    public TLSEncryptionAdapter(Connection connection,
                                SSLEngine engine,
                                CipherExecutor cipherExecutor) {
        m_connection = connection;
        m_sslEngine = engine;
        m_ce = cipherExecutor;
        m_encrypter = new SSLBufferEncrypter(engine);
        m_encryptedMessages = Unpooled.compositeBuffer();
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

    CompositeByteBuf getEncryptedMessagesBuffer() {
        return m_encryptedMessages;
    }

    static final class EncryptLedger {
        final int encryptedBytesDelta;
        final long bytesWritten;
        final int messagesWritten;

        public EncryptLedger(int delta, long bytesWritten, int messagesWritten) {
            this.encryptedBytesDelta = delta;
            this.bytesWritten = bytesWritten;
            this.messagesWritten = messagesWritten;
        }
    }

    public EncryptLedger drainEncryptedMessages(final GatheringByteChannel channel) throws IOException {
        checkForGatewayExceptions();

        int delta = 0;
        int queued;
        // add to output buffer frames that contain whole messages
        while ((queued=addFramesForCompleteMessage()) >= 0) {
            delta += queued;
        }

        long bytesWritten = m_encryptedMessages.readBytes(channel, m_encryptedMessages.readableBytes());
        m_encryptedMessages.discardReadComponents();

        int messagesWritten = 0;
        if (!m_encryptedMessages.isReadable() && bytesWritten > 0) {
            messagesWritten += m_numEncryptedMessages;
            m_numEncryptedMessages = 0;
        }

        return new EncryptLedger(delta, bytesWritten, messagesWritten);
    }

    /**
     * Gather all the frames that comprise a whole Volt Message
     * Returns the delta between the original message byte count and encrypted message byte count.
     */
    private int addFramesForCompleteMessage() {
        boolean added = false;
        EncryptFrame frame = null;
        int delta = 0;

        while (!added && (frame = m_encryptedFrames.poll()) != null) {
            if (!frame.isLast()) {
                //TODO: Review - I don't think this synchronized(m_partialMessages) is required.
                // This is the only method with synchronized(m_partialMessages) and
                // it doesn't look like this method will be called from multiple threads concurrently.
                // Take this out 8.0 release.
                synchronized(m_partialMessages) {
                    m_partialMessages.add(frame);
                    ++m_partialSize;
                }
                continue;
            }

            final int partialSize = m_partialSize;
            if (partialSize > 0) {
                assert frame.chunks == partialSize + 1
                        : "partial frame buildup has wrong number of preceding pieces";

                //TODO: Review - I don't think this synchronized(m_partialMessages) is required.
                // See comment above.
                // Take this out 8.0 release.
                synchronized(m_partialMessages) {
                    for (EncryptFrame frm: m_partialMessages) {
                        m_encryptedMessages.addComponent(true, frm.frame);
                        delta += frm.delta;
                    }
                    m_partialMessages.clear();
                    m_partialSize = 0;
                }
            }
            m_encryptedMessages.addComponent(true, frame.frame);
            delta += frame.delta;

            m_numEncryptedMessages += frame.msgs;
            added = true;
        }
        return added ? delta : -1;
    }

    // Called from synchronized block only
    // (Except from dumpState, which doesn't appear to be used).
    public boolean isEmpty() {
        return m_ecryptgw.isEmpty()
            && m_encryptedFrames.isEmpty()
            && m_partialSize == 0
            && !m_encryptedMessages.isReadable();

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
                .append(", encryptedFrames.isEmpty()=").append(m_encryptedFrames.isEmpty())
                .append(", encryptedMessages.readableBytes()=").append(m_encryptedMessages.readableBytes())
                .append(", gateway=").append(m_ecryptgw.dumpState())
                .append(", inFlight=").append(m_inFlight.availablePermits())
                .append("]").toString();
    }

    // Called from synchronized block only
    public int getOutstandingMessageCount() {
        return m_encryptedFrames.size()
             + m_partialSize
             + m_encryptedMessages.numComponents();
    }

    // Called from synchronized block only
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

            EncryptFrame frame = null;
            while ((frame = m_encryptedFrames.poll()) != null) {
                frame.frame.release();
            }

            for (EncryptFrame ef: m_partialMessages) {
                ef.frame.release();
            }
            m_partialMessages.clear();

            if (m_encryptedMessages.refCnt() > 0) m_encryptedMessages.release();
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
                m_encryptedFrames.offer(frame.encrypted(delta, encr));
                /*
                 * All interactions with write stream must be protected
                 * with a lock to ensure that interests ops are consistent with
                 * the state of writes queued to the stream. This prevent
                 * lost queued writes where the write is queued
                 * but the write interest op is not set.
                 */
                if (frame.isLast()) {
                    try {
                        m_connection.enableWriteSelection();
                    } catch(CancelledKeyException e) {
                        // If the connection gets closed for some reason we will get this error.
                        // OK to ignore and return immediately
                        s_networkLog.debug("CancelledKeyException while trying to enable write", e);
                        return;
                    }
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
