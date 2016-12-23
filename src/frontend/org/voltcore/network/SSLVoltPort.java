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

import com.google_voltpatches.common.util.concurrent.SettableFuture;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.ssl.SSLBufferDecrypter;
import org.voltcore.utils.ssl.SSLBufferEncrypter;
import org.voltcore.utils.ssl.SSLEncryptionService;
import org.voltcore.utils.ssl.SSLMessageParser;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

public class SSLVoltPort extends VoltPort {

    private final SSLEngine m_sslEngine;
    private final SSLEncryptionService m_sslEncryptionService;
    private final SSLBufferDecrypter m_sslBufferDecrypter;
    private final SSLBufferEncrypter m_sslBufferEncrypter;
    private DBBPool.BBContainer m_dstBufferCont;
    private final ByteBuffer m_dstBuffer;
    private final SSLMessageParser m_sslMessageParser;

    private final DecryptionGateway m_decryptionGateway;
    private final EncryptionGateway m_encryptionGateway;
    private final Deque<EncryptionResult> m_encryptedBuffers = new ConcurrentLinkedDeque<>();
    private final Deque<DecryptionResult> m_decryptedMessages = new ConcurrentLinkedDeque<>();
    private final Deque<SettableFuture<Void>> m_encryptionResults = new ArrayDeque<>();
    private final Deque<SettableFuture<Void>> m_decryptionResults = new ArrayDeque<>();

    public SSLVoltPort(VoltNetwork network, InputHandler handler, InetSocketAddress remoteAddress, NetworkDBBPool readPool, SSLEncryptionService sslEncryptionService, SSLEngine sslEngine) {
        super(network, handler, remoteAddress, readPool);
        this.m_sslEncryptionService = sslEncryptionService;
        this.m_sslEngine = sslEngine;
        this.m_sslBufferDecrypter = new SSLBufferDecrypter(sslEngine);
        int appBufferSize = m_sslEngine.getSession().getApplicationBufferSize();
        // the app buffer size will sometimes be greater than 16k, but the ssl engine won't
        // encrypt more than 16k bytes at a time.  So it's simpler to not go over 16k.
        int packetBufferSize = m_sslEngine.getSession().getPacketBufferSize();
        this.m_sslBufferEncrypter = new SSLBufferEncrypter(sslEngine, appBufferSize, packetBufferSize);
        this.m_encryptionGateway = new EncryptionGateway(m_sslBufferEncrypter, this);
        this.m_sslMessageParser = new SSLMessageParser();
        this.m_dstBufferCont = DBBPool.allocateDirect(packetBufferSize);
        this.m_dstBuffer = m_dstBufferCont.b();
        this.m_dstBuffer.clear();
        this.m_decryptionGateway = new DecryptionGateway(m_sslBufferDecrypter, this, m_sslMessageParser, m_dstBuffer);
    }

    @Override
    public void run() throws IOException {

        try {
            if (m_isShuttingDown) {
                unregistered();
                return;
            }
            processReads();
            handleDecryptedMessages();
            processWrites();
            handleEncryptedBuffers();
            writeStream().checkBackpressureEnded();
            cleanup();
        } catch (IOException ioe) {
            networkLog.error("Exception in SSLVoltPort.run", ioe);
            throw ioe;
        } finally {
            synchronized (m_lock) {
                assert (m_running == true);
                m_running = false;
            }
        }
    }

    private void processReads() throws IOException {
        // if the read stream is empty  (dataAvailable == 0), then fill it.
        if (readStream().dataAvailable() == 0) {
            final int maxRead = m_handler.getMaxRead();
            int nRead = fillReadStream(maxRead);
            if (nRead > 0) {
                m_decryptionGateway.submitDecryptionTasks();
            }
        } else {
            m_decryptionGateway.submitDecryptionTasks();
        }
    }

    private void processWrites() throws IOException {
        if (writeStream().serializeQueuedWrites(m_pool) > 0) {
            m_encryptionGateway.submitEncryptionTasks();
        }
    }

    @Override
    public SSLNIOReadStream readStream() {
        assert(m_readStream != null);
        return (SSLNIOReadStream) m_readStream;
    }

    @Override
    public SSLNIOWriteStream writeStream() {
        assert(m_writeStream != null);
        return (SSLNIOWriteStream) m_writeStream;
    }

    @Override
    void unregistered() {
        drainGateways();
        m_encryptionGateway.shutdown();
        m_decryptionGateway.shutdown();
        if (m_dstBufferCont != null) {
            m_dstBufferCont.discard();
            m_dstBufferCont = null;
        }
        super.unregistered();
    }

    private class DecryptionGateway {
        private final SSLBufferDecrypter m_sslBufferDecrypter;
        private final SSLMessageParser m_sslMessageParser;
        private final ByteBuffer m_dstBuffer;
        private boolean m_hasOutstandingTask = false;
        private final Object m_decLock = new Object();
        private final VoltPort m_port;
        final AtomicBoolean m_isShuttingDown = new AtomicBoolean(false);
        private int m_nextFrameLength = 0;
        private final ByteBuffer m_frameHeader = ByteBuffer.allocate(5);
        private DBBPool.BBContainer m_frameCont;


        private final Runnable m_decTask = new Runnable() {
            @Override
            public void run() {
                DBBPool.BBContainer srcC = null;
                boolean queuedMessages = false;
                SettableFuture<Void> sf = null;
                while (true) {
                    synchronized (m_decLock) {
                        if ((srcC = getDecryptionFrame()) == null) {
                            m_hasOutstandingTask = false;
                            if (!m_isShuttingDown.get() && queuedMessages) {
                                m_network.addToChangeList(m_port, true);
                            }
                            break;
                        }
                    }

                    try {
                        sf = SettableFuture.create();
                        m_decryptionResults.add(sf);
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
                        srcC = null;
                        m_dstBuffer.clear();
                        m_decryptedMessages.offer(new DecryptionResult(messages, sf));
                        sf = null;
                        queuedMessages = true;
                    } catch (IOException ioe) {
                        networkLog.error("Exception unwrapping an SSL frame", ioe);
                        m_dstBuffer.clear();
                    } finally {
                        if (srcC != null) {
                            srcC.discard();
                        }
                        if (sf != null) {
                            sf.setException(new Throwable("Decryption of buffer failed."));
                        }
                    }
                }
            }
        };

        public DecryptionGateway(SSLBufferDecrypter m_sslBufferDecrypter, SSLVoltPort port, SSLMessageParser sslMessageParser, ByteBuffer dstBuffer) {
            this.m_sslBufferDecrypter = m_sslBufferDecrypter;
            this.m_sslMessageParser = sslMessageParser;
            this.m_dstBuffer = dstBuffer;
            this.m_port = port;
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

        void submitDecryptionTasks() {
            synchronized (m_decLock) {
                if (! m_hasOutstandingTask) {
                    try {
                        m_sslEncryptionService.submitForDecryption(m_decTask);
                        m_hasOutstandingTask = true;
                    } catch (RejectedExecutionException e) {
                        // the thread pool has been shutdown.
                    }
                }
            }
        }
        public void shutdown() {
            m_isShuttingDown.set(true);
        }
    }

    private void handleDecryptedMessages() throws IOException {
        DecryptionResult dr = null;
        while ((dr = m_decryptedMessages.poll()) != null) {
            try {
                for (ByteBuffer message : dr.m_messages) {
                    m_handler.handleMessage(message, this);
                }
            } finally {
                dr.m_sf.set(null);
            }
        }
    }

    private class EncryptionGateway {

        private final SSLBufferEncrypter m_sslBufferEncrypter;
        private final SSLVoltPort m_port;
        final AtomicBoolean m_isShuttingDown = new AtomicBoolean(false);
        private boolean m_hasOutstandingTask = false;
        private final Object m_encLock = new Object();
        private final Runnable m_encTask = new Runnable() {
            @Override
            public void run() {
                DBBPool.BBContainer fragCont = null;
                boolean queuedEncBuffers = false;
                SettableFuture<Void> sf = null;
                while (true) {
                    synchronized (m_encLock) {
                        if ((fragCont = writeStream().getWriteBuffer()) == null) {
                            m_hasOutstandingTask = false;
                            if (!m_isShuttingDown.get() && queuedEncBuffers) {
                                m_network.addToChangeList(m_port, true);
                            }
                            break;
                        }
                    }

                    try {
                        sf = SettableFuture.create();
                        m_encryptionResults.add(sf);
                        fragCont.b().flip();
                        int nBytesClear = fragCont.b().remaining();
                        final DBBPool.BBContainer encCont = m_sslBufferEncrypter.encryptBuffer(fragCont.b().slice());
                        fragCont.discard();
                        fragCont = null;
                        m_encryptedBuffers.offer(new EncryptionResult(encCont, nBytesClear, sf));
                        sf = null;
                        queuedEncBuffers = true;
                    } catch (IOException ioe) {
                        networkLog.error("Exception wrapping an SSL frame", ioe);
                    } finally {
                        if (fragCont != null) {
                            fragCont.discard();
                        }
                        if (sf != null) {
                            sf.setException(new Throwable("Encryption of buffer failed."));
                        }
                    }
                }
            }
        };

        public EncryptionGateway(SSLBufferEncrypter m_sslBufferEncrypter, SSLVoltPort port) {
            this.m_sslBufferEncrypter = m_sslBufferEncrypter;
            this.m_port = port;
        }

        void submitEncryptionTasks() {
            synchronized (m_encLock) {
                if (! m_hasOutstandingTask) {
                    try {
                        m_sslEncryptionService.submitForEncryption(m_encTask);
                        m_hasOutstandingTask = true;
                    } catch (RejectedExecutionException e) {
                        // the thread pool has been shutdown.
                    }
                }
            }
        }
        public void shutdown() {
            m_isShuttingDown.set(true);
        }
    }

    private boolean handleEncryptedBuffers() throws IOException {
        if (m_encryptedBuffers.isEmpty()) {
            return false;
        }

        EncryptionResult er = null;
        int bytesWritten = 0;
        try {
            while (true) {
                er = m_encryptedBuffers.peek();
                if (er == null) {
                    break;
                }
                int rc = 1;
                while (er.m_encCont.b().hasRemaining() && rc > 0) {
                    rc = m_channel.write(er.m_encCont.b());
                }

                if (er.m_encCont.b().hasRemaining()) {
                    m_writeStream.backpressureStarted();
                    m_network.addToChangeList(this, true);
                    return true;  // there are remaining writes to process
                } else {
                    m_encryptedBuffers.poll();
                    er.m_encCont.discard();
                    bytesWritten += er.m_nClearBytes;
                    er.m_sf.set(null);
                    m_writeStream.backpressureEnded();
                }
            }
        } finally {
            writeStream().checkBackPressureAndUpdateWriteStats(bytesWritten);
        }
        return false;
    }

    @Override
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

    private void cleanup() {
        SettableFuture<Void> sf;
        while ((sf = m_encryptionResults.peek()) != null) {
            if (sf.isDone()) {
                try {
                    sf.get();
                } catch (InterruptedException e) {
                } catch (ExecutionException e) {
                    networkLog.error("SSLVoltPort task failed", e);
                }
                m_encryptionResults.poll();
            } else {
                break;
            }
        }
        while ((sf = m_decryptionResults.peek()) != null) {
            if (sf.isDone()) {
                try {
                    sf.get();
                } catch (InterruptedException e) {
                } catch (ExecutionException e) {
                    networkLog.error("SSLVoltPort task failed", e);
                }
                m_decryptionResults.poll();
            } else {
                break;
            }
        }
    }

    private void drainGateways() {
        SettableFuture<Void> sf;
        while ((sf = m_encryptionResults.poll()) != null) {
            try {
                sf.get();
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
                networkLog.error("SSLVoltPort task failed", e);
            }
        }
        while ((sf = m_decryptionResults.poll()) != null) {
            try {
                sf.get();
            } catch (InterruptedException e) {
            } catch (ExecutionException e) {
                networkLog.error("SSLVoltPort task failed", e);
            }
        }
    }

    private static class EncryptionResult {
        private final DBBPool.BBContainer m_encCont;
        private final int m_nClearBytes;
        private final SettableFuture<Void> m_sf;

        EncryptionResult(DBBPool.BBContainer encCont, int m_nClearBytes, SettableFuture<Void> sf) {
            this.m_encCont = encCont;
            this.m_nClearBytes = m_nClearBytes;
            this.m_sf = sf;
        }
    }

    private static class DecryptionResult {
        private final List<ByteBuffer> m_messages;
        private final SettableFuture<Void> m_sf;

        DecryptionResult(List<ByteBuffer> m_messages, SettableFuture<Void> m_sf) {
            this.m_messages = m_messages;
            this.m_sf = m_sf;
        }
    }
}
