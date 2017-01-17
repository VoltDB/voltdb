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

import org.voltcore.utils.FlexibleSemaphore;
import org.voltcore.utils.ssl.SSLBufferDecrypter;

import com.google_voltpatches.common.util.concurrent.ListenableFuture;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;

public class TLSVoltPort extends VoltPort  {
    private final static int TLS_HEADER_SIZE = 5;

    private final SSLEngine m_sslEngine;
    private final SSLBufferDecrypter m_decrypter;

    private final ConcurrentLinkedDeque<ExecutionException> m_exceptions = new ConcurrentLinkedDeque<>();
    private final ConcurrentLinkedDeque<ByteBuffer> m_decrypted = new ConcurrentLinkedDeque<>();
    private final FlexibleSemaphore m_inFlight = new FlexibleSemaphore(1);
    private final CipherExecutor m_ce;
    private final DecryptionGateway m_dcryptgw = new DecryptionGateway();

    public TLSVoltPort(VoltNetwork network, InputHandler handler,
            InetSocketAddress remoteAddress, NetworkDBBPool pool,
            SSLEngine sslEngine, CipherExecutor cipherExecutor) {
        super(network, handler, remoteAddress, pool);
        m_ce = cipherExecutor;
        m_sslEngine = sslEngine;
        m_decrypter = new SSLBufferDecrypter(sslEngine);
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
        boolean acquired = false;
        while (!acquired) {
            int waitFor = 1 - m_inFlight.availablePermits();
            for (int i = 0; i < waitFor && !acquired; ++i) {
                checkForGatewayExceptions();
                try {
                    acquired = m_inFlight.tryAcquire(1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new IOException("interrupted while waiting for pending decrypts", e);
                }
            }
        }
        m_inFlight.release();
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
                        byte [] frameHeader = new byte[TLS_HEADER_SIZE];
                        while (readStream().dataAvailable() >= frameHeader.length) {
                            NIOReadStream rdstrm = (NIOReadStream)readStream();
                            rdstrm.peekBytes(frameHeader);
                            int framesz = ByteBuffer.wrap(frameHeader,3,2).getShort() + frameHeader.length;
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

    abstract class Assembler {

        protected final CompositeByteBuf m_bb;
        protected volatile int m_pieces;

        protected Assembler(int pieces) {
            assert pieces > 1 : "gatherer must have gather 2 or more pieces";
            m_pieces = pieces;
            m_bb = m_ce.allocator().compositeBuffer(m_pieces);
        }

        abstract void complete();

        Assembler add(ByteBuf bb) {
            final int remaining = --m_pieces;
            if (isDead()) {
                m_pieces = -1;
                m_bb.release();
                return this;
            }
            if (remaining < 0) {
                throw new IllegalStateException("gatherer alredy got all its pieces");
            }
            m_bb.addComponent(true, bb);
            if (remaining == 0) {
                assert m_bb.readerIndex() == 0 : "operating on an already consumed decrypted buffer";
                complete();
            }
            return this;
        }

        boolean done() {
            return m_pieces <= 0;
        }
    }

    /**
     * Class used to gather decrypted tls frames that comprise one large volt message
     * It operates on the assumption that a frame contains one or more whole volt messages
     * or it contains a fragment of one message. A volt message cannot contain whole messages
     * and fragments of another message
     */
    class InAssembler extends Assembler {

        private InAssembler(int pieces) {
            super(pieces);
        }

        @Override
        void complete() {
            try {
                ByteBuffer jbb = ByteBuffer.allocate(m_bb.writerIndex());
                m_bb.readBytes(jbb);
                jbb.flip();
                m_decrypted.offer(jbb);
                enableWriteSelection();
            } finally {
                m_bb.release();
            }
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
        private volatile Assembler m_assembler = null;

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
                fut.addListener(new ExceptionListener(fut), m_ce.getES());
            }
            m_inFlight.reducePermits(1);
        }

        synchronized void die() {
            NIOReadStream.Slice slice = null;
            while ((slice=m_q.poll()) != null) {
                slice.bb.readerIndex(slice.bb.writerIndex());
                slice.discard();
            }
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
                m_inFlight.release();
                dest.release();
                m_exceptions.offer(new ExecutionException("fragment decrypt task failed", e));
                enableWriteSelection();
                return;
            }
            assert !slicebbarr[0].hasRemaining() : "decrypter did not wholly consume the source buffer";

            // src buffer is wholly consumed
            dest.writerIndex(destjbb.limit());
            if (m_assembler == null) {
                int msgsz = dest.getInt(0);
                final int frames = CipherExecutor.framesFor(msgsz);
                // frames may contain only one or more whole messages, or only
                // partial parts of one message. a message may not contain whole
                // messages and an incomplete partial fragment of one
                if (frames == 1) {
                    while (dest.isReadable()) {
                        msgsz = dest.readInt();
                        ByteBuffer bb = ByteBuffer.allocate(msgsz);
                        dest.readBytes(bb);
                        bb.flip();
                        if (!isDead()) {
                            m_decrypted.offer(bb);
                        }
                    }
                    dest.release();
                    if (!isDead()) {
                        enableWriteSelection();
                    }
                } else {
                    dest.readInt();
                    m_assembler = new InAssembler(frames).add(dest);
                }
            } else {
                if (m_assembler.add(dest).done()) {
                    m_assembler = null;
                }
            }
            synchronized(this) {
                m_q.poll();
                src.readerIndex(src.writerIndex());
                slice.discard();
                m_inFlight.release();
                if (m_q.peek() != null) {
                    ListenableFuture<?> fut = m_ce.getES().submit(this);
                    fut.addListener(new ExceptionListener(fut), m_ce.getES());
                }
            }
        }
    }
}
