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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.FlexibleSemaphore;
import org.voltcore.utils.ssl.SSLBufferDecrypter;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import io.netty_voltpatches.buffer.ByteBuf;
import io.netty_voltpatches.buffer.CompositeByteBuf;
import io.netty_voltpatches.buffer.Unpooled;
import io.netty_voltpatches.util.IllegalReferenceCountException;

public class TLSVoltPort extends VoltPort  {
    public final static int TLS_HEADER_SIZE = 5;

    private final SSLEngine m_sslEngine;
    private final SSLBufferDecrypter m_decrypter;

    private final ConcurrentLinkedDeque<ExecutionException> m_exceptions = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<ByteBuffer> m_decrypted = new ConcurrentLinkedDeque<>();
    private final FlexibleSemaphore m_inFlight = new FlexibleSemaphore(1);
    private final CipherExecutor m_ce;
    private final DecryptionGateway m_dcryptgw;

    public TLSVoltPort(VoltNetwork network, InputHandler handler,
            InetSocketAddress remoteAddress, NetworkDBBPool pool,
            SSLEngine sslEngine, CipherExecutor cipherExecutor) {
        super(network, handler, remoteAddress, pool);
        m_ce = cipherExecutor;
        m_sslEngine = sslEngine;
        m_decrypter = new SSLBufferDecrypter(sslEngine);
        m_dcryptgw = new DecryptionGateway();
    }

    /**
     * this values may change if a TLS session renegotiates its cipher suite
     */
    private int applicationBufferSize() {
        return m_sslEngine.getSession().getApplicationBufferSize();
    }

    @Override
    protected void setKey(SelectionKey key) {
        m_selectionKey = key;
        m_channel = (SocketChannel)key.channel();
        m_readStream = new NIOReadStream();
        m_writeStream = new TLSNIOWriteStream(
                this,
                m_handler.offBackPressure(),
                m_handler.onBackPressure(),
                m_handler.writestreamMonitor(),
                m_sslEngine, m_ce);
        m_interestOps = key.interestOps();
    }

    @Override
    void die() {
        super.die();
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

    private void checkForGatewayExceptions() throws IOException {
        ExecutionException ee = m_exceptions.poll();
        if (ee != null) {
            IOException ioe = TLSException.ioCause(ee.getCause());
            if (ioe == null) {
                ioe = new IOException("decrypt task failed", ee.getCause());
            }
            throw ioe;
        }
    }

    private void waitForPendingDecrypts() throws IOException {
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
        return new StringBuilder(256).append("TLSVoltPort[")
                .append("availableBytes=").append(readStream().dataAvailable())
                .append(", gateway=").append(m_dcryptgw.dumpState())
                .append(", decrypted.isEmpty()= ").append(m_decrypted.isEmpty())
                .append(", exceptions.isEmpty()= ").append(m_exceptions.isEmpty())
                .append(", inFlight=").append(m_inFlight.availablePermits())
                .append("]").toString();
    }

    private void waitForPendingEncrypts() throws IOException {
        ((TLSNIOWriteStream)m_writeStream).waitForPendingEncrypts();
    }

    private final static int MAX_READ = CipherExecutor.FRAME_SIZE << 1; //32 KB
    private final static int NOT_AVAILABLE = -1;

    private int m_needed = NOT_AVAILABLE;

    private final int getMaxRead() {
        return m_handler.getMaxRead() == 0 ? 0 // in back pressure
                : m_needed == NOT_AVAILABLE ? MAX_READ :
                    readStream().dataAvailable() > m_needed ? 0 : m_needed - readStream().dataAvailable();
    }

    @Override
    public void run() throws IOException {
        try {
            do {
                checkForGatewayExceptions();
                /*
                 * Have the read stream fill from the network
                 */
                if (readyForRead()) {
                    final int maxRead = getMaxRead();
                    if (maxRead > 0) {
                        int read = fillReadStream(maxRead);
                        if (read > 0) {
                            ByteBuf frameHeader = Unpooled.wrappedBuffer(new byte[TLS_HEADER_SIZE]);
                            while (readStream().dataAvailable() >= TLS_HEADER_SIZE) {
                                NIOReadStream rdstrm = readStream();
                                rdstrm.peekBytes(frameHeader.array());
                                m_needed = frameHeader.getShort(3) + TLS_HEADER_SIZE;
                                if (rdstrm.dataAvailable() < m_needed) break;
                                m_dcryptgw.offer(rdstrm.getSlice(m_needed));
                                m_needed = NOT_AVAILABLE;
                            }
                        }
                    }
                }

                if (m_network.isStopping() || m_isShuttingDown) {
                    waitForPendingDecrypts();
                }

                ByteBuffer message = null;
                while ((message = m_decrypted.poll()) != null) {
                    ++m_messagesRead;
                    m_handler.handleMessage(message, this);
                }

                /*
                 * On readiness selection, optimistically assume that write will succeed,
                 * in the common case it will
                 */
                drainEncryptedStream();
                /*
                 * some encrypt or decrypt task may have finished while this port is running
                 * so enabling write interest would have been muted. Signal is there to
                 * reconsider finished decrypt or encrypt tasks.
                 */
            } while (m_signal.compareAndSet(true, false));
        } finally {
            synchronized(m_lock) {
                assert(m_running == true);
                m_running = false;
            }
        }
    }

    private void drainEncryptedStream() throws IOException {
        TLSNIOWriteStream writeStream = (TLSNIOWriteStream)m_writeStream;

        writeStream.serializeQueuedWrites(m_pool /* unused and ignored */);
        if (m_network.isStopping()) {
            waitForPendingEncrypts();
        }
        synchronized (writeStream) {
            if (!writeStream.isEmpty()) {
                writeStream.drainTo(m_channel);
            }
            if (writeStream.isEmpty()) {
                disableWriteSelection();
                if (m_isShuttingDown) {
                    m_channel.close();
                    unregistered();
                }
            }
        }
    }
    /**
     * if this port is running calls to enableWriteSelection are
     * ignored by the super class. This signaling artifacts tells
     * this port run() method to keep polling for finished encrypt
     * and decrypt taks
     */
    private final AtomicBoolean m_signal = new AtomicBoolean(false);

    @Override
    protected void enableWriteSelection() {
        m_signal.set(true);
        super.enableWriteSelection();
    }

    @Override
    void unregistered() {
        try {
            waitForPendingDecrypts();
        } catch (IOException e) {
            networkLog.warn("unregistered port had an decryption task drain fault", e);
        }
        try {
            waitForPendingEncrypts();
        } catch (IOException e) {
            networkLog.warn("unregistered port had an encryption task drain fault", e);
        }
        m_dcryptgw.releaseDecryptedBuffer();
        super.unregistered();
    }

    class ExceptionListener implements Runnable {
        private final ListenableFuture<?> m_fut;
        private ExceptionListener(ListenableFuture<?> fut) {
            m_fut = fut;
        }
        @Override
        public void run() {
            if (isDead()) return;
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

        private final byte [] m_overlap = new byte[CipherExecutor.FRAME_SIZE + 2048];
        private final ConcurrentLinkedDeque<NIOReadStream.Slice> m_q = new ConcurrentLinkedDeque<>();
        private final CompositeByteBuf m_msgbb = Unpooled.compositeBuffer();
        private volatile int m_needed = NOT_AVAILABLE;

        synchronized void offer(NIOReadStream.Slice slice) throws IOException {
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

        private final IOException validateMessageLength(int msgLength) {
            IOException ioe = null;
            if (msgLength < 1) {
                ioe = new BadMessageLength(
                        "Next message length is " + msgLength + " which is less than 1 and is nonsense");
            }
            if (msgLength > MAX_MESSAGE_LENGTH) {
                ioe = new BadMessageLength(
                        "Next message length is " + msgLength + " which is greater then the hard coded " +
                        "max of " + MAX_MESSAGE_LENGTH + ". Break up the work into smaller chunks (2 megabytes is reasonable) " +
                        "and send as multiple messages or stored procedure invocations");
            }
            assert msgLength > 1 : "invalid negative or zero message length header value";
            return ioe;
        }

        void releaseDecryptedBuffer() {
            if (m_msgbb.refCnt() > 0) try {
                m_msgbb.release();
            } catch (IllegalReferenceCountException ignoreIt) {
            }
        }

        @Override
        public void run() {
            final NIOReadStream.Slice slice = m_q.peek();
            if (slice == null) return;

            ByteBuf src = slice.bb;

            if (isDead()) synchronized(this) {
                slice.markConsumed().discard();
                m_q.poll();
                releaseDecryptedBuffer();
                return;
            }

            ByteBuffer [] slicebbarr = slice.bb.nioBuffers();
            // if frame overlaps two buffers then copy it to the overlap buffer
            // and use that instead for the unwrap src buffer
            if (slicebbarr.length > 1) {
                src = Unpooled.wrappedBuffer(m_overlap).clear();
                slice.bb.readBytes(src, slice.bb.readableBytes());
                slicebbarr[0] = src.nioBuffer();
            }

            final int appBuffSz = applicationBufferSize();
            ByteBuf dest = m_ce.allocator().buffer(appBuffSz).writerIndex(appBuffSz);
            ByteBuffer destjbb = dest.nioBuffer();
            int decryptedBytes = 0;
            try {
                decryptedBytes = m_decrypter.tlsunwrap(slicebbarr[0], destjbb);
            } catch (TLSException e) {
                m_inFlight.release(); dest.release();
                m_exceptions.offer(new ExecutionException("fragment decrypt task failed", e));
                networkLog.error("fragment decrypt task failed", e);
                enableWriteSelection();
                return;
            }
            assert !slicebbarr[0].hasRemaining() : "decrypter did not wholly consume the source buffer";

            // src buffer is wholly consumed
            if (!isDead()) {
                if (decryptedBytes > 0) {
                    dest.writerIndex(destjbb.limit());
                    m_msgbb.addComponent(true, dest);
                } else {
                    // the TLS frame was consumed by the call to engines unwrap but it
                    // did not yield any content
                    dest.release();
                }

                int read = 0;
                while (m_msgbb.readableBytes() >= getNeededBytes()) {
                    if (m_needed == NOT_AVAILABLE) {
                        m_needed = m_msgbb.readInt();
                        IOException ioe = validateMessageLength(m_needed);
                        if (ioe != null) {
                            m_inFlight.release(); m_msgbb.release();
                            m_exceptions.offer(new ExecutionException("failed message length check", ioe));
                            networkLog.error("failed message length check", ioe);
                            enableWriteSelection();
                        }
                        continue;
                    }
                    ByteBuffer bb = ByteBuffer.allocate(m_needed);
                    m_msgbb.readBytes(bb);
                    m_decrypted.offer((ByteBuffer)bb.flip());

                    ++read;
                    m_needed = NOT_AVAILABLE;
                }
                if (read > 0) {
                    m_msgbb.discardReadComponents();
                    enableWriteSelection();
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
            return m_needed == NOT_AVAILABLE ? 4 : m_needed;
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
