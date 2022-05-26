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

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLEngine;

import org.voltcore.logging.VoltLogger;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.FlexibleSemaphore;
import org.voltcore.utils.Pair;
import org.voltcore.utils.ssl.SSLBufferDecrypter;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.IllegalReferenceCountException;

public class TLSDecryptionAdapter {
    public final static int TLS_HEADER_SIZE = 5;
    private final static int MAX_READ = CipherExecutor.FRAME_SIZE << 1; //32 KB
    private final static int NOT_AVAILABLE = -1;


    protected static final VoltLogger networkLog = new VoltLogger("NETWORK");

    private final SSLBufferDecrypter m_decrypter;

    private final ConcurrentLinkedDeque<ExecutionException> m_exceptions = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<ByteBuffer> m_decrypted = new ConcurrentLinkedDeque<>();
    private final FlexibleSemaphore m_inFlight = new FlexibleSemaphore(1);
    private final CipherExecutor m_ce;
    private final DecryptionGateway m_dcryptgw;
    private final Connection m_connection;
    private final InputHandler m_inputHandler;
    private volatile boolean m_isDead;

    private int m_needed = NOT_AVAILABLE;


    public TLSDecryptionAdapter(Connection connection, InputHandler handler, SSLEngine sslEngine, CipherExecutor cipherExecutor) {
        m_connection = connection;
        m_inputHandler = handler;
        m_ce = cipherExecutor;
        m_decrypter = new SSLBufferDecrypter(sslEngine);
        m_dcryptgw = new DecryptionGateway();
    }

    void die() {
        m_isDead = true;
        m_dcryptgw.die();

        int waitFor = 1 - m_inFlight.availablePermits();
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

        m_inFlight.drainPermits();
        m_inFlight.release();
    }

    private boolean isDead() {
        return m_isDead;
    }

    public Pair<Integer, Integer> handleInputStreamMessages(boolean doRead, NIOReadStream readStream, SocketChannel fromChannel, NetworkDBBPool toPool)
            throws IOException {

        checkForGatewayExceptions();

        int readBytes = 0;
        /* Have the read stream fill from the network */
        if (doRead) {
            final int maxRead = getMaxRead(readStream);
            if (maxRead > 0) {
                readBytes = readStream.read(fromChannel, maxRead, toPool);
                if (readBytes == -1) {
                    throw new EOFException();
                }
                if (readBytes > 0) {
                    ByteBuf frameHeader = Unpooled.wrappedBuffer(new byte[TLS_HEADER_SIZE]);
                    while (readStream.dataAvailable() >= TLS_HEADER_SIZE) {
                        readStream.peekBytes(frameHeader.array());
                        m_needed = frameHeader.getShort(3) + TLS_HEADER_SIZE;
                        if (readStream.dataAvailable() < m_needed) {
                            break;
                        }
                        m_dcryptgw.offer(readStream.getSlice(m_needed));
                        m_needed = NOT_AVAILABLE;
                    }
                }
            }
        }

        /**
         TODO: Moving this stopping check to TLSVoltPort after this method is called.
         This means that handleMessages will be done before this check. Any adverse effects?
        if (m_network.isStopping() || m_isShuttingDown) {
            waitForPendingDecrypts();
        }
        */

        int numMessages = 0;
        ByteBuffer message = null;
        while ((message = pollDecryptedQueue()) != null) {
            ++numMessages;
            m_inputHandler.handleMessage(message, m_connection);
        }

        return new Pair<Integer, Integer>(readBytes, numMessages);
    }

    private final int getMaxRead(NIOReadStream readStream) {
        return m_inputHandler.getMaxRead() == 0 ? 0 // in back pressure
                : m_needed == NOT_AVAILABLE ? MAX_READ :
                    readStream.dataAvailable() > m_needed ? 0 : m_needed - readStream.dataAvailable();
    }

    ByteBuffer pollDecryptedQueue() {
        return m_decrypted.poll();
    }

    void releaseDecryptedBuffer() {
        m_dcryptgw.releaseDecryptedBuffer();
    }

    void checkForGatewayExceptions() throws IOException {
        ExecutionException ee = m_exceptions.poll();
        if (ee != null) {
            IOException ioe = TLSException.ioCause(ee.getCause());
            if (ioe == null) {
                ioe = new IOException("decrypt task failed", ee.getCause());
            }
            throw ioe;
        }
    }

    void waitForPendingDecrypts() throws IOException {
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
                    throw new IOException("interrupted while waiting for pending decrypts", e);
                }
            }
        } while (!acquired);
    }

    String dumpState() {
        return new StringBuilder(256).append("TLSPortAdapter[")
                .append("gateway=").append(m_dcryptgw.dumpState())
                .append(", decrypted.isEmpty()= ").append(m_decrypted.isEmpty())
                .append(", exceptions.isEmpty()= ").append(m_exceptions.isEmpty())
                .append(", inFlight=").append(m_inFlight.availablePermits())
                .append("]").toString();
    }

    class ExceptionListener implements Runnable {
        private final ListenableFuture<?> m_fut;
        private ExceptionListener(ListenableFuture<?> fut) {
            m_fut = fut;
        }
        @Override
        public void run() {
            if (isDead()) {
                return;
            }
            try {
                m_fut.get();
            } catch (InterruptedException notPossible) {
            } catch (ExecutionException e) {
                m_inFlight.release();
                networkLog.error("unexpect fault occurred in decrypt task", e.getCause());
                m_exceptions.offer(e);
            }
        }
    }

    /**
     * Construct used to serialize all the decryption tasks for this port.
     * it takes a view of the incoming queued buffers (that may span two BBContainers)
     * and decrypts them. It uses the assembler to gather all frames that comprise
     * a frame spanning message, otherwise it will enqueue decrypted messages to
     * the m_descrypted queue.
     */
    class DecryptionGateway implements Runnable {

        private final byte[] m_overlap = new byte[m_decrypter.getPacketBufferSize()];
        private final ConcurrentLinkedDeque<NIOReadStream.Slice> m_q = new ConcurrentLinkedDeque<>();
        private final CompositeByteBuf m_msgbb = Unpooled.compositeBuffer();

        synchronized void offer(NIOReadStream.Slice slice) {
            if (isDead()) {
                slice.markConsumed().discard();
                return;
            }
            final boolean wasEmpty = m_q.isEmpty();
            m_q.offer(slice);
            if (wasEmpty) {
                submitSelf();
            }
            m_inFlight.reducePermits(1);
        }

        synchronized void die() {
            NIOReadStream.Slice slice = null;
            while ((slice=m_q.poll()) != null) {
                slice.markConsumed().discard();
            }
            releaseDecryptedBuffer();
        }

        synchronized boolean isEmpty() {
            return m_q.isEmpty();
        }

        String dumpState() {
            return new StringBuilder(256).append("DecryptionGateway[isEmpty()=").append(isEmpty())
              .append(", isDead()=").append(isDead())
              .append(", msgbb=").append(m_msgbb)
              .append("]").toString();
        }

        void releaseDecryptedBuffer() {
            if (m_msgbb.refCnt() > 0) {
                try {
                    m_msgbb.release();
                } catch (IllegalReferenceCountException ignoreIt) {
                }
            }
        }

        @Override
        public void run() {
            final NIOReadStream.Slice slice = m_q.peek();
            if (slice == null) {
                return;
            }

            ByteBuf src = slice.bb;

            if (isDead()) {
                synchronized(this) {
                    slice.markConsumed().discard();
                    m_q.poll();
                    releaseDecryptedBuffer();
                    return;
                }
            }

            ByteBuffer [] slicebbarr = slice.bb.nioBuffers();
            // if frame overlaps two buffers then copy it to the overlap buffer
            // and use that instead for the unwrap src buffer
            if (slicebbarr.length > 1) {
                src = Unpooled.wrappedBuffer(m_overlap).clear();
                slice.bb.readBytes(src, slice.bb.readableBytes());
                slicebbarr[0] = src.nioBuffer();
            }

            ByteBuf dest ;
            int srcBBLength = slicebbarr[0].remaining();
            try {
                dest = m_decrypter.tlsunwrap(slicebbarr[0], m_ce.allocator());
            } catch (TLSException e) {
                m_inFlight.release();
                m_exceptions.offer(new ExecutionException("fragment decrypt task failed", e));
                networkLog.error("fragment decrypt task failed", e);
                networkLog.error("isDead()=" + isDead() + ", Src buffer original length: " + srcBBLength +
                        ", Length after decrypt operation: " + slicebbarr[0].remaining());
                m_connection.enableWriteSelection();
                return;
            }
            assert !slicebbarr[0].hasRemaining() : "decrypter did not wholly consume the source buffer";

            // src buffer is wholly consumed
            if (!isDead()) {
                if (dest.isReadable()) {
                    m_msgbb.addComponent(true, dest);
                } else {
                    // the TLS frame was consumed by the call to engines unwrap but it
                    // did not yield any content
                    dest.release();
                }

                int read = 0;
                while (m_msgbb.readableBytes() >= getNeededBytes()) {
                    ByteBuffer bb = null;
                    try {
                        bb = m_inputHandler.retrieveNextMessage(m_msgbb);
                        // All of the message bytes are not available yet
                        if (bb==null) {
                            continue;
                        }
                    } catch(IOException e) {
                        m_inFlight.release(); m_msgbb.release();
                        m_exceptions.offer(new ExecutionException("failed message length check", e));
                        networkLog.error("failed message length check", e);
                        m_connection.enableWriteSelection();
                        continue;
                    }

                    m_decrypted.offer((ByteBuffer)bb.flip());
                    ++read;
                }
                if (read > 0) {
                    m_msgbb.discardReadComponents();
                    m_connection.enableWriteSelection();
                }
            } else { // it isDead()
                dest.release();
                releaseDecryptedBuffer();
            }
            synchronized(this) {
                m_q.poll();
                slice.markConsumed().discard();
                m_inFlight.release();
                if (m_q.peek() != null) {
                    submitSelf();
                }
            }
        }

        void submitSelf() {
            ListenableFuture<?> fut = m_ce.submit(this);
            fut.addListener(new ExceptionListener(fut), CoreUtils.LISTENINGSAMETHREADEXECUTOR);
        }

        private int getNeededBytes() {
            int nextLength = m_inputHandler.getNextMessageLength();
            return nextLength == 0 ? 4 : nextLength;
        }
    }

    /** The distinct exception class allows better logging of these unexpected errors. */
    static class BadMessageLength extends IOException {
        private static final long serialVersionUID = 8547352379044459911L;
        public BadMessageLength(String string) {
            super(string);
        }
    }
}
