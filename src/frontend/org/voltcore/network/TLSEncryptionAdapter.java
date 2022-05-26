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
import java.nio.channels.CancelledKeyException;
import java.nio.channels.GatheringByteChannel;
import java.util.Deque;
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

import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import io.netty.buffer.ByteBuf;

public class TLSEncryptionAdapter {
    private static final VoltLogger s_networkLog = new VoltLogger("NETWORK");

    private final ConcurrentLinkedDeque<ExecutionException> m_exceptions = new ConcurrentLinkedDeque<>();
    // Input frames encrypted as they came in
    private final ConcurrentLinkedDeque<EncryptedMessages> m_encryptedQueue = new ConcurrentLinkedDeque<>();

    // Encrypted data which is in the process of being written out
    private EncryptedMessages m_inflightMessages;

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
            if (serializedSize == DeferredSerialization.EMPTY_MESSAGE_LENGTH) {
                continue;
            }
            // pack as messages you can inside a TLS frame before you send it to
            // the encryption gateway
            if (serializedSize > frameMax) {
                // frames may contain only one or more whole messages, or only
                // partial parts of one message. a message may not contain whole
                // messages and an incomplete partial fragment of one
                if (accum.writerIndex() > 0) {
                    m_ecryptgw.offer(new SerializedMessages(accum, frameMsgs));
                    frameMsgs = 0;
                    bytesQueued += accum.writerIndex();
                    accum = m_ce.allocator().buffer(frameMax).clear();
                }
                ByteBuf big = m_ce.allocator().buffer(serializedSize).writerIndex(serializedSize);
                ByteBuffer jbb = big.nioBuffer();
                ds.serialize(jbb);
                NIOWriteStreamBase.checkSloppySerialization(jbb, ds);
                bytesQueued += big.writerIndex();
                m_ecryptgw.offer(new SerializedMessages(big, 1));
                frameMsgs = 0;
                continue;
            } else if (accum.writerIndex() + serializedSize > frameMax) {
                m_ecryptgw.offer(new SerializedMessages(accum, frameMsgs));
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
            m_ecryptgw.offer(new SerializedMessages(accum, frameMsgs));
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

    boolean hasOutstandingData() {
        return !(m_inflightMessages == null && m_encryptedQueue.isEmpty());
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
        int bytesWritten = 0;
        int messagesWritten = 0;

        while (true) {
            if (m_inflightMessages == null) {
                m_inflightMessages = m_encryptedQueue.poll();
                if (m_inflightMessages == null) {
                    break;
                }
                delta += m_inflightMessages.m_delta;
            }

            bytesWritten += m_inflightMessages.write(channel);
            if (m_inflightMessages.m_messages.isReadable()) {
                break;
            }

            messagesWritten += m_inflightMessages.m_count;
            m_inflightMessages.m_messages.release();
            m_inflightMessages = null;
        }

        return new EncryptLedger(delta, bytesWritten, messagesWritten);
    }

    // Called from synchronized block only
    // (Except from dumpState, which doesn't appear to be used).
    public boolean isEmpty() {
        return m_ecryptgw.isEmpty() && m_encryptedQueue.isEmpty() && m_inflightMessages == null;
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
                .append(", encryptedFrames.isEmpty()=").append(m_encryptedQueue.isEmpty())
                .append(", m_inflightMessages.readableBytes()=")
                .append(m_inflightMessages == null ? 0 : m_inflightMessages.m_messages.readableBytes())
                .append(", gateway=").append(m_ecryptgw.dumpState())
                .append(", inFlight=").append(m_inFlight.availablePermits())
                .append("]").toString();
    }

    // Called from synchronized block only
    public int getOutstandingMessageCount() {
        return m_encryptedQueue.size()
                + (m_inflightMessages == null ? 0 : m_inflightMessages.m_count);
    }

    // Called from synchronized block only
    void shutdown() {
        if (m_isShutdown) { // make sure we only shutdown once.
            return;
        }
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

            EncryptedMessages messages = null;
            while ((messages = m_encryptedQueue.poll()) != null) {
                messages.m_messages.release();
            }

            if (m_inflightMessages != null) {
                assert (m_inflightMessages.m_messages != null && m_inflightMessages.m_messages.refCnt() > 0);
                m_inflightMessages.m_messages.release();
            }
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
        private final ConcurrentLinkedDeque<SerializedMessages> m_q = new ConcurrentLinkedDeque<>();

        synchronized void offer(SerializedMessages frame) {
            final boolean wasEmpty = m_q.isEmpty();

            m_q.add(frame);
            m_inFlight.reducePermits(1);

            if (wasEmpty) {
                submitSelf();
            }
        }

        synchronized int die() {
            int toUnqueue = 0;
            SerializedMessages ef = null;
            while ((ef = m_q.poll()) != null) {
                toUnqueue += ef.m_messages.readableBytes();
            }
            return toUnqueue;
        }

        String dumpState() {
            return new StringBuilder(256).append("EncryptionGateway[")
                    .append("q.isEmpty()=").append(m_q.isEmpty())
                    .append("]").toString();
        }

        @Override
        public void run() {
            SerializedMessages messages = m_q.peek();
            if (messages == null) {
                return;
            }

            try {
                int clearTextSize = messages.m_messages.readableBytes();
                ByteBuf encr;
                try {
                    encr = m_encrypter.tlswrap(messages.m_messages, m_ce.allocator());
                } catch (TLSException e) {
                    m_exceptions.offer(new ExecutionException("failed to encrypt frame", e));
                    m_connection.enableWriteSelection();
                    return;
                }

                if (m_isShutdown) {
                    encr.release();
                    return;
                }

                m_encryptedQueue.offer(new EncryptedMessages(encr, messages.m_count, clearTextSize));

                /*
                 * All interactions with write stream must be protected with a lock to ensure that interests ops are
                 * consistent with the state of writes queued to the stream. This prevent lost queued writes where the
                 * write is queued but the write interest op is not set.
                 */
                try {
                    m_connection.enableWriteSelection();
                } catch (CancelledKeyException e) {
                    // If the connection gets closed for some reason we will get this error.
                    // OK to ignore and return immediately
                    s_networkLog.debug("CancelledKeyException while trying to enable write", e);
                    return;
                }
            } finally {
                messages.m_messages.release();
                m_inFlight.release();
            }

            synchronized(this) {
                m_q.poll();
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
            if (!m_isShutdown) {
                return;
            }
            try {
                m_fut.get();
            } catch (InterruptedException notPossible) {
            } catch (ExecutionException e) {
                s_networkLog.error("unexpect fault occurred in encrypt task", e.getCause());
                m_exceptions.offer(e);
            }
        }
    }

    /**
     * Simple class to hold the plain text bytes of 1 one or more messages
     */
    private static class SerializedMessages {
        // Plain text data of messages
        final ByteBuf m_messages;
        // Number of individual messages in m_messages
        final int m_count;

        SerializedMessages(ByteBuf messages, int count) {
            super();
            m_messages = messages;
            m_count = count;
        }
    }

    /**
     * Simple class to hold a fully encrypted messages
     */
    private static final class EncryptedMessages extends SerializedMessages {
        // Difference in size of encrypted data to clear text data
        final int m_delta;

        EncryptedMessages(ByteBuf messages, int count, int clearTextSize) {
            super(messages, count);
            m_delta = m_messages.readableBytes() - clearTextSize;
        }

        /**
         * Write the contents of these messages to {@code channel}
         *
         * @param channel {@link GatheringByteChannel} to write to
         * @return Number of bytes written
         * @throws IOException If an error occurs
         * @see GatheringByteChannel#write(ByteBuffer[])
         */
        long write(GatheringByteChannel channel) throws IOException {
            return m_messages.readBytes(channel, m_messages.readableBytes());
        }
    }
}
