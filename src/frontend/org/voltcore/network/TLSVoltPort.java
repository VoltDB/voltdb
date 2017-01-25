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

import javax.net.ssl.SSLEngine;

import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.FlexibleSemaphore;
import org.voltcore.utils.ssl.SSLBufferDecrypter;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import io.netty_voltpatches.buffer.ByteBuf;
import io.netty_voltpatches.buffer.CompositeByteBuf;
import io.netty_voltpatches.buffer.Unpooled;

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
        if (!m_ce.isActive()) {
            while (!m_dcryptgw.isEmpty()) {
                m_dcryptgw.run();
            }
        }
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

    @Override
    public void run() throws IOException {
        try {
            checkForGatewayExceptions();
            /*
             * Have the read stream fill from the network
             */
            if (readyForRead()) {
                final int maxRead = m_handler.getMaxRead();
                if (maxRead > 0) {
                    int read = fillReadStream(maxRead);
                    if (read > 0) {
                        ByteBuf frameHeader = Unpooled.wrappedBuffer(new byte[TLS_HEADER_SIZE]);
                        while (readStream().dataAvailable() >= TLS_HEADER_SIZE) {
                            NIOReadStream rdstrm = (NIOReadStream)readStream();
                            rdstrm.peekBytes(frameHeader.array());
                            int framesz = frameHeader.getShort(3) + TLS_HEADER_SIZE;
                            if (rdstrm.dataAvailable() < framesz) break;
                            m_dcryptgw.offer(rdstrm.getSlice(framesz));
                        }
                    }
                }
            }

            if (m_network.isStopping()) {
                waitForPendingDecrypts();
            }

            ByteBuffer message = null;
            while ((message = m_decrypted.poll()) != null) {
                m_handler.handleMessage(message, this);
            }

            /*
             * On readiness selection, optimistically assume that write will succeed,
             * in the common case it will
             */
            drainEncryptedStream();
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

    /*
     * All interactions with write stream must be protected
     * with a lock to ensure that interests ops are consistent with
     * the state of writes queued to the stream. This prevent
     * lost queued writes where the write is queued
     * but the write interest op is not set.
     */
    @Override
    protected void enableWriteSelection() {
        synchronized (m_writeStream) {
            super.enableWriteSelection();
        }
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

        private final byte [] m_overlap = new byte[CipherExecutor.PAGE_SIZE + 2048];
        private final ConcurrentLinkedDeque<NIOReadStream.Slice> m_q = new ConcurrentLinkedDeque<>();
        private final CompositeByteBuf m_msgbb = Unpooled.compositeBuffer();
        private volatile int m_needed = NOT_AVAILABLE;

        synchronized void offer(NIOReadStream.Slice slice) {
            if (isDead()) {
                slice.bb.readerIndex(slice.bb.writerIndex());
                slice.discard();
                return;
            }
            final boolean wasEmpty = m_q.isEmpty();
            m_q.offer(slice);
            if (wasEmpty) {
                ListenableFuture<?> fut = m_ce.getES().submit(this);
                fut.addListener(new ExceptionListener(fut), CoreUtils.SAMETHREADEXECUTOR);
            }
            m_inFlight.reducePermits(1);
        }

        synchronized void die() {
            NIOReadStream.Slice slice = null;
            while ((slice=m_q.poll()) != null) {
                slice.bb.readerIndex(slice.bb.writerIndex());
                slice.discard();
            }
            m_msgbb.release();
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

        @Override
        public void run() {
            final NIOReadStream.Slice slice = m_q.peek();
            if (slice == null) return;

            ByteBuf src = slice.bb;

            if (isDead()) {
                slice.bb.readerIndex(slice.bb.writerIndex());
                slice.discard();
                return;
            }

            ByteBuffer [] slicebbarr = slice.bb.nioBuffers();
            // if frame overlaps two buffers then copy it to the ovelap buffer
            // and use that instead for the unwrap src buffer
            if (slicebbarr.length > 1) {
                src = Unpooled.wrappedBuffer(m_overlap).clear();
                slice.bb.readBytes(src, slice.bb.readableBytes());
                slicebbarr[0] = src.nioBuffer();
            }

            final int appBuffSz = applicationBufferSize();
            ByteBuf dest = m_ce.allocator().buffer(appBuffSz).writerIndex(appBuffSz);
            ByteBuffer destjbb = dest.nioBuffer();

            try {
                m_decrypter.tlsunwrap(slicebbarr[0], destjbb);
            } catch (TLSException e) {
                m_inFlight.release(); dest.release(); m_msgbb.release();
                m_exceptions.offer(new ExecutionException("fragment decrypt task failed", e));
                networkLog.error("fragment decrypt task failed", e);
                enableWriteSelection();
                return;
            }
            assert !slicebbarr[0].hasRemaining() : "decrypter did not wholly consume the source buffer";

            // src buffer is wholly consumed
            dest.writerIndex(destjbb.limit());
            m_msgbb.addComponent(true, dest);

            int read = 0;
            while (m_msgbb.readableBytes() >= getNeededBytes()) {
                if (m_needed == NOT_AVAILABLE) {
                    IOException ioe = null;
                    m_needed = m_msgbb.readInt();
                    if (m_needed < 1) {
                        ioe = new BadMessageLength(
                                "Next message length is " + m_needed + " which is less than 1 and is nonsense");
                    }
                    if (m_needed > MAX_MESSAGE_LENGTH) {
                        ioe = new BadMessageLength(
                                "Next message length is " + m_needed + " which is greater then the hard coded " +
                                "max of " + MAX_MESSAGE_LENGTH + ". Break up the work into smaller chunks (2 megabytes is reasonable) " +
                                "and send as multiple messages or stored procedure invocations");
                    }
                    assert m_needed > 1 : "invalid negative or zero message length header value";
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
                bb.flip();
                if (!isDead()) {
                    m_decrypted.offer(bb);
                }
                ++read;
                m_needed = NOT_AVAILABLE;
            }
            if (read > 0) {
                m_msgbb.discardReadComponents();
                enableWriteSelection();
            }
            synchronized(this) {
                m_q.poll();
                src.readerIndex(src.writerIndex());
                slice.discard();
                m_inFlight.release();
                if (m_q.peek() != null && m_ce.isActive()) {
                    ListenableFuture<?> fut = m_ce.getES().submit(this);
                    fut.addListener(new ExceptionListener(fut), CoreUtils.SAMETHREADEXECUTOR);
                }
            }
        }

        private final static int NOT_AVAILABLE = -1;

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
