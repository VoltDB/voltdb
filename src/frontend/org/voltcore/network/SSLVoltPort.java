/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import org.voltcore.utils.DBBPool;
import org.voltcore.utils.ssl.SSLBufferDecrypter;
import org.voltcore.utils.ssl.SSLBufferEncrypter;
import org.voltcore.utils.ssl.SSLEncryptionService;
import org.voltcore.utils.ssl.SSLMessageParser;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.voltcore.utils.DeferredSerialization;

public class SSLVoltPort extends VoltPort {

    private final SSLEngine m_sslEngine;
    private final SSLBufferDecrypter m_sslBufferDecrypter;
    private final SSLBufferEncrypter m_sslBufferEncrypter;
    private DBBPool.BBContainer m_dstBufferCont;
    private final ByteBuffer m_dstBuffer;
    private final SSLMessageParser m_sslMessageParser;

    private final ByteBuffer m_frameHeader;
    private DBBPool.BBContainer m_frameCont;

    private int m_nextFrameLength = 0;

    private final DecryptionGateway m_decryptionGateway;
    private final EncryptionGateway m_encryptionGateway;
    private final WriteGateway m_writeGateway;

    public SSLVoltPort(VoltNetwork network, InputHandler handler, InetSocketAddress remoteAddress, NetworkDBBPool readPool, SSLEngine sslEngine) {
        super(network, handler, remoteAddress, readPool);
        this.m_sslEngine = sslEngine;
        this.m_sslBufferDecrypter = new SSLBufferDecrypter(sslEngine);
        int appBufferSize = m_sslEngine.getSession().getApplicationBufferSize();
        // the app buffer size will sometimes be greater than 16k, but the ssl engine won't
        // encrypt more than 16k bytes at a time.  So it's simpler to not go over 16k.
        int packetBufferSize = m_sslEngine.getSession().getPacketBufferSize();
        this.m_sslBufferEncrypter = new SSLBufferEncrypter(sslEngine, appBufferSize, packetBufferSize);
        this.m_writeGateway = new WriteGateway();
        this.m_encryptionGateway = new EncryptionGateway(m_sslBufferEncrypter, m_writeGateway);
        this.m_sslMessageParser = new SSLMessageParser();
        this.m_frameHeader = ByteBuffer.allocate(5);
        this.m_dstBufferCont = DBBPool.allocateDirect(packetBufferSize);
        this.m_dstBuffer = m_dstBufferCont.b();
        this.m_dstBuffer.clear();
        this.m_decryptionGateway = new DecryptionGateway(m_sslBufferDecrypter, this, handler, m_sslMessageParser, m_dstBuffer);
    }

    @Override
    public void run() throws IOException {

        if (m_isShuttingDown) {
            unregistered();
            return;
        }

        try {
            processReads();
            processWrites();

            checkBackPressure();
        } catch (IOException ioe) {
            throw ioe;
        } finally {
            synchronized (m_lock) {
                assert (m_running == true);
                m_running = false;
            }
        }
    }

    private void checkBackPressure() {
        writeStream().checkBackpressureEnded();
    }

    private void processReads() throws IOException {
        final int maxRead = m_handler.getMaxRead();
        int nRead = fillReadStream(maxRead);
        if (nRead > 0) {
            m_decryptionGateway.submitDecryptionTasks();
        }
    }

    private void processWrites() throws IOException {
        if (writeStream().serializeQueuedWrites(m_pool) > 0) {
            m_encryptionGateway.submitEncryptionTasks();
        }
    }

    private DBBPool.BBContainer getDecryptionFrame() {
        if (m_nextFrameLength == 0) {
            readStream().getBytes(m_frameHeader);
            if (m_frameHeader.hasRemaining()) {
                return null;
            } else {
                m_frameHeader.flip();
                m_frameHeader.position(3);
                m_nextFrameLength = m_frameHeader.getShort();
                m_frameHeader.flip();
                m_frameCont = DBBPool.allocateDirectAndPool(m_nextFrameLength + 5);
                m_frameCont.b().clear();
                m_frameCont.b().limit(m_nextFrameLength + 5);
                m_frameCont.b().put(m_frameHeader);
                m_frameHeader.clear();
            }
        }

        readStream().getBytes(m_frameCont.b());
        if (m_frameCont.b().hasRemaining()) {
            return null;
        } else {
            m_nextFrameLength = 0;
            DBBPool.BBContainer frame = m_frameCont;
            m_frameCont = null;
            return frame;
        }
    }


    @Override
    void unregistered() {
        m_writeGateway.shutdown();
        m_encryptionGateway.shutdown();
        m_decryptionGateway.shutdown();
        m_dstBufferCont.discard();
        m_dstBufferCont = null;
        super.unregistered();
    }

    private class DecryptionGateway {
        private final SSLBufferDecrypter m_sslBufferDecrypter;
        private final SSLMessageParser m_sslMessageParser;
        private final ByteBuffer m_dstBuffer;
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final VoltPort m_port;
        private final InputHandler m_handler;
        protected AtomicBoolean m_isShuttingDown = new AtomicBoolean(false);

        public DecryptionGateway(SSLBufferDecrypter m_sslBufferDecrypter, SSLVoltPort port, InputHandler handler, SSLMessageParser sslMessageParser, ByteBuffer dstBuffer) {
            this.m_sslBufferDecrypter = m_sslBufferDecrypter;
            this.m_sslMessageParser = sslMessageParser;
            this.m_dstBuffer = dstBuffer;
            this.m_port = port;
            this.m_handler = handler;
        }

        synchronized void submitDecryptionTasks() {
            if (m_hasOutstandingTask.compareAndSet(false, true)) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        DBBPool.BBContainer srcC = null;
                        try {
                            while ((srcC = getDecryptionFrame()) != null) {
                                if (m_isShuttingDown.get()) return;
                                final List<ByteBuffer> messages = new ArrayList<>();
                                srcC.b().flip();
                                m_dstBuffer.limit(m_dstBuffer.capacity());
                                m_sslBufferDecrypter.unwrap(srcC.b(), m_dstBuffer);
                                if (m_dstBuffer.hasRemaining()) {
                                    ByteBuffer message;
                                    while ((message = m_sslMessageParser.message(m_dstBuffer)) != null) {
                                        messages.add(message);
                                    }
                                }
                                srcC.discard();
                                m_dstBuffer.clear();
                                if (!messages.isEmpty()) {
                                    m_network.queueTask(new Runnable() {
                                        @Override
                                        public void run() {
                                            for (ByteBuffer m : messages) {
                                                try {
                                                    m_handler.handleMessage(m, m_port);
                                                } catch (IOException e) {
                                                    // TODO: log
                                                    return;
                                                }
                                            }
                                        }
                                    });
                                }
                            }
                        } catch (IOException ioe) {
                            //TODO: Add logging.
                            m_dstBuffer.clear();
                            return;
                        } finally {
                            if (srcC != null) {
                                srcC.discard();
                            }
                            m_hasOutstandingTask.set(false);
                        }
                    }
                };
                SSLEncryptionService.instance().submitForDecryption(task);
            }
        }
        public void shutdown() {
            m_isShuttingDown.set(true);
        }
    }

    private class EncryptionGateway {

        private final SSLBufferEncrypter m_sslBufferEncrypter;
        private final WriteGateway m_writeGateway;
        protected AtomicBoolean m_isShuttingDown = new AtomicBoolean(false);
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);

        public EncryptionGateway(SSLBufferEncrypter m_sslBufferEncrypter, WriteGateway writeGateway) {
            this.m_sslBufferEncrypter = m_sslBufferEncrypter;
            this.m_writeGateway = writeGateway;
        }

        synchronized void submitEncryptionTasks() {
            if (m_hasOutstandingTask.compareAndSet(false, true)) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        DBBPool.BBContainer fragCont = null;
                        try {
                            while ((fragCont = ((SSLNIOWriteStream) writeStream()).getWriteBuffer()) != null) {
                                fragCont.b().flip();
                                if (m_isShuttingDown.get()) return;
                                int nBytesClear = fragCont.b().remaining();
                                DBBPool.BBContainer encCont = m_sslBufferEncrypter.encryptBuffer(fragCont.b().slice());
                                EncryptionResult er = new EncryptionResult(encCont, nBytesClear);
                                m_writeGateway.enque(er);
                                fragCont.discard();
                                fragCont = null;
                            }
                        } catch (IOException ioe) {
                            //TODO: Log
                        } finally {
                            if (fragCont != null) {
                                fragCont.discard();
                            }
                            m_hasOutstandingTask.set(false);
                        }
                    }
                };
                SSLEncryptionService.instance().submitForEncryption(task);
        }
        }
        public void shutdown() {
            m_isShuttingDown.set(true);
        }
    }

    private class WriteGateway {

        private final Deque<EncryptionResult> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final AtomicBoolean m_isShuttingDown = new AtomicBoolean(false);

        public WriteGateway() {
        }

        void enque(EncryptionResult encRes) {
            synchronized (m_q) {
                m_q.offer(encRes);
                if (m_hasOutstandingTask.compareAndSet(false, true)) {
                    Runnable task = new Runnable() {
                        @Override
                        public void run() {
                            EncryptionResult er = null;
                            int bytesQueued = 0;
                            boolean wroteAll = true;
                            try {
                                while ((er = m_q.peek()) != null) {
                                    int rc = 1;
                                    while (er.m_encCont.b().hasRemaining() && rc > 0) {
                                        rc = m_channel.write(er.m_encCont.b());
                                    }

                                    if (er.m_encCont.b().hasRemaining()) {
                                        er = null;
                                        SSLEncryptionService.instance().submitForEncryption(this);
                                        wroteAll = false;
                                        return;
                                    } else {
                                        bytesQueued += er.m_nBytesClear;
                                        er = m_q.poll();
                                        er.m_encCont.discard();
                                        er = null;
                                    }
                                }
                            } catch (IOException ioe) {
                                // TODO: log
                            } finally {
                                if (er != null) {
                                    er.m_encCont.discard();
                                }

                                final int finalBytesQueued = bytesQueued;
                                final boolean finalWroteAll = wroteAll;
                                m_network.queueTask(new Runnable() {
                                    @Override
                                    public void run() {
                                        m_writeStream.updateQueued(-finalBytesQueued, false);
                                        if (finalWroteAll) {
                                            m_writeStream.checkBackpressureEnded();
                                        } else {
                                            m_writeStream.backpressureStarted();
                                        }
                                    }
                                });
                                m_hasOutstandingTask.set(false);

                            }
                        }
                    };
                    SSLEncryptionService.instance().submitForEncryption(task);
                }
            }
        }

        public boolean isEmpty() {
            return !m_hasOutstandingTask.get();
        }

        public void shutdown() {
            m_isShuttingDown.set(true);
        }
    }

    protected synchronized int fillReadStream(int maxBytes) throws IOException {
        if ( maxBytes == 0 || m_isShuttingDown)
            return 0;

        // read from network, copy data into read buffers, which from thread local memory pool
        final int read = readStream().read(m_channel, maxBytes, m_pool);

        if (read == -1) {
            disableReadSelection();

            if (m_channel.socket().isConnected()) {
                try {
                    m_channel.socket().shutdownInput();
                } catch (SocketException e) {
                    //Safe to ignore to these
                }
            }

            m_isShuttingDown = true;
            m_handler.stopping(this);

            /*
             * Allow the write queue to drain if possible
             */
            enableWriteSelection();
        }
        return read;
    }

    protected void setKey (SelectionKey key) {
        m_selectionKey = key;
        m_channel = (SocketChannel)key.channel();
        m_readStream = new SSLNIOReadStream();
        m_writeStream = new SSLNIOWriteStream(
                this,
                m_handler.offBackPressure(),
                m_handler.onBackPressure(),
                m_handler.writestreamMonitor());
        m_interestOps = key.interestOps();
    }

    public static class EncryptionResult {
        public final DBBPool.BBContainer m_encCont;
        public final int m_nBytesClear;
        public EncryptionResult(DBBPool.BBContainer encCont, int nBytesClear) {
            this.m_encCont = encCont;
            this.m_nBytesClear = nBytesClear;
        }
    }

    public class WriteResult {
        public final int m_bytesQueued;
        public final int m_bytesWritten;
        public WriteResult(int bytesQueued, int bytesWritten) {
            this.m_bytesQueued = bytesQueued;
            this.m_bytesWritten = bytesWritten;
        }
    }
}
