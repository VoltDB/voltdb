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
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.ssl.SSLBufferDecrypter;
import org.voltcore.utils.ssl.SSLBufferEncrypter;
import org.voltcore.utils.ssl.SSLEncryptionService;
import org.voltcore.utils.ssl.SSLMessageParser;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

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
    private final ReadGateway m_readGateway;
    private final WriteGateway m_writeGateway;

    private final int m_appBufferSize;
    private VoltPort m_port;

    private AtomicBoolean m_processingReads = new AtomicBoolean(false);
    private AtomicBoolean m_processingWrites = new AtomicBoolean(false);
    private final NetworkDBBPool m_writePool;
    private ByteBuffer m_heapBuffer;
    private final AtomicInteger m_bytesQueued = new AtomicInteger(0);

    public SSLVoltPort(VoltNetwork network, InputHandler handler, InetSocketAddress remoteAddress, NetworkDBBPool readPool, NetworkDBBPool writePool, SSLEngine sslEngine) {
        super(network, handler, remoteAddress, readPool);
        this.m_writePool = writePool;
        this.m_sslEngine = sslEngine;
        this.m_sslBufferDecrypter = new SSLBufferDecrypter(sslEngine);
        int appBufferSize = m_sslEngine.getSession().getApplicationBufferSize();
        // the app buffer size will sometimes be greater than 16k, but the ssl engine won't
        // encrypt more than 16k bytes at a time.  So it's simpler to not go over 16k.
        this.m_appBufferSize = Math.min(appBufferSize, 16 * 1024);
        int packetBufferSize = m_sslEngine.getSession().getPacketBufferSize();
        this.m_sslBufferEncrypter = new SSLBufferEncrypter(sslEngine, appBufferSize, packetBufferSize);
        this.m_writeGateway = new WriteGateway(network, this);
        this.m_encryptionGateway = new EncryptionGateway(m_sslBufferEncrypter, m_writeGateway, this);
        this.m_sslMessageParser = new SSLMessageParser();
        this.m_frameHeader = ByteBuffer.allocate(5);
        this.m_dstBufferCont = DBBPool.allocateDirect(packetBufferSize);
        this.m_dstBuffer = m_dstBufferCont.b();
        this.m_dstBuffer.clear();
        this.m_readGateway = new ReadGateway(network, this, handler);
        this.m_decryptionGateway = new DecryptionGateway(m_sslBufferDecrypter, m_readGateway, m_sslMessageParser, m_dstBuffer, this);
    }

    @Override
    public void run() throws IOException {

        if (m_isShuttingDown) {
            unregistered();
            return;
        }

        try {
            if (!m_processingReads.get() && !m_processingWrites.get()) {
                if (!processReads()) {
                    processWrites();
                }
            }

            checkBackPressure();
        } catch (IOException ioe) {
            while (!gatewaysEmpty()) {
            }
            throw ioe;
        } finally {
            synchronized (m_lock) {
                assert (m_running == true);
                m_running = false;
            }
        }
    }

    private void checkBackPressure() {
        int bytesQueued = m_bytesQueued.getAndSet(0);
        if (bytesQueued != 0) {
            writeStream().updateQueued(bytesQueued, false);
        }
        writeStream().checkBackpressureEnded();
    }

    private boolean processReads() throws IOException {
        if (m_processingReads.compareAndSet(false, true)) {
            final int maxRead = m_handler.getMaxRead();
            int nRead = fillReadStream(maxRead);
            if (nRead > 0) {
                m_decryptionGateway.start();
                return true;
            } else {
                m_processingReads.set(false);
                return false;
            }
        } else {
            return true;
        }
    }

    private void processWrites() {
        if (writeStream().hasQueuedWrites() && m_processingWrites.compareAndSet(false,true)) {
            m_encryptionGateway.start();
        }
    }

    private void doneProcessingWrites() {
        m_processingWrites.set(false);
    }

    public void doneProcessingReads() {
        m_processingReads.set(false);
    }

    private boolean gatewaysEmpty() {
        return isWriteStreamProcessed() && isReadStreamProcessed();
    }

    private boolean isWriteStreamProcessed() {
        return m_encryptionGateway.isEmpty() && m_writeGateway.isEmpty();
    }

    private boolean isReadStreamProcessed() {
        return m_decryptionGateway.isEmpty() && m_readGateway.isEmpty();
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

    private DBBPool.BBContainer getChunkToEncrypt() throws IOException {
        if (m_heapBuffer != null) {
            return getEncChunkFromHeapBuffer();
        }

        Queue<DeferredSerialization> writes = m_writeStream.getQueuedWrites();
        if (writes.isEmpty()) return null;

        DBBPool.BBContainer outCont = m_writePool.acquire();
        outCont.b().clear();
        DeferredSerialization ds = null;
        while (true) {
            ds = writes.poll();
            if (ds == null) {
                if (outCont.b().position() == 0) {
                    outCont.discard();
                    return null;
                } else {
                    outCont.b().flip();
                    return outCont;
                }
            }
            final int serializedSize = ds.getSerializedSize();
            if (serializedSize == DeferredSerialization.EMPTY_MESSAGE_LENGTH) continue;
            m_bytesQueued.getAndAdd(serializedSize);
            if (outCont.b().remaining() >= serializedSize) {
                final int oldLimit = outCont.b().limit();
                outCont.b().limit(outCont.b().position() + serializedSize);
                final ByteBuffer slice = outCont.b().slice();
                ds.serialize(slice);
                slice.position(0);
                outCont.b().position(outCont.b().limit());
                outCont.b().limit(oldLimit);
            } else {
                assert m_heapBuffer == null;
                m_heapBuffer = ByteBuffer.allocate(serializedSize);
                ds.serialize(m_heapBuffer);
                m_heapBuffer.flip();
                if (outCont.b().position() > 0) {
                    outCont.b().flip();
                    return outCont;
                } else {
                    outCont.discard();
                    return getEncChunkFromHeapBuffer();
                }
            }
        }
    }

    private DBBPool.BBContainer getEncChunkFromHeapBuffer() {
        if (m_heapBuffer.remaining() > m_appBufferSize) {
            int oldLimit = m_heapBuffer.limit();
            m_heapBuffer.limit(m_heapBuffer.position() + m_appBufferSize);
            DBBPool.BBContainer encChunk = DBBPool.wrapBB(m_heapBuffer.slice());
            m_heapBuffer.position(m_heapBuffer.position() + m_appBufferSize);
            m_heapBuffer.limit(oldLimit);
            return encChunk;
        } else {
            DBBPool.BBContainer encChunk = DBBPool.wrapBB(m_heapBuffer.slice());
            m_heapBuffer = null;
            return encChunk;
        }
    }

    @Override
    void unregistered() {
        m_writeGateway.shutdown();
        m_readGateway.shutdown();
        m_encryptionGateway.shutdown();
        m_decryptionGateway.shutdown();
        while (!gatewaysEmpty()) {
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                return;
            }
        }
        m_dstBufferCont.discard();
        m_dstBufferCont = null;
        super.unregistered();
    }

    private class DecryptionGateway {
        private final SSLBufferDecrypter m_sslBufferDecrypter;
        private final SSLMessageParser m_sslMessageParser;
        private final ByteBuffer m_dstBuffer;
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final ReadGateway m_readGateway;
        private final SSLVoltPort m_port;
        protected AtomicBoolean m_isShuttingDown = new AtomicBoolean(false);

        public DecryptionGateway(SSLBufferDecrypter m_sslBufferDecrypter, ReadGateway readGateway, SSLMessageParser sslMessageParser, ByteBuffer dstBuffer, SSLVoltPort port) {
            this.m_sslBufferDecrypter = m_sslBufferDecrypter;
            this.m_sslMessageParser = sslMessageParser;
            this.m_dstBuffer = dstBuffer;
            this.m_readGateway = readGateway;
            this.m_port = port;
        }

        synchronized void start() {
            if (m_hasOutstandingTask.get()) {
                return;
            }
            m_hasOutstandingTask.set(true);
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    if (m_isShuttingDown.get()) {
                        return;
                    }
                    DBBPool.BBContainer srcC = null;
                    List<ByteBuffer> messages = new ArrayList<>();
                    try {
                        srcC = getDecryptionFrame();
                        if (srcC == null) {
                            m_hasOutstandingTask.set(false);
                            // empty list signals that there is no more data coming.
                            m_readGateway.enque(messages);
                            return;
                        }
                        m_hasOutstandingTask.set(true);
                        srcC.b().flip();
                        m_dstBuffer.limit(m_dstBuffer.capacity());
                        m_sslBufferDecrypter.unwrap(srcC.b(), m_dstBuffer);
                        if (m_dstBuffer.hasRemaining()) {
                            ByteBuffer message;
                            while ((message = m_sslMessageParser.message(m_dstBuffer)) != null) {
                                messages.add(message);
                            }
                        }
                        m_dstBuffer.clear();
                        if (!messages.isEmpty()) {
                            m_readGateway.enque(messages);
                        }
                        SSLEncryptionService.instance().submitForEncryption(this);
                    } catch (IOException ioe) {
                        System.err.println("Decryption gateway " + ioe.getMessage());
                        m_hasOutstandingTask.set(false);
                        return;
                    } finally {
                        if (srcC != null) {
                            srcC.discard();
                        }
                    }
                }
            };
            SSLEncryptionService.instance().submitForDecryption(task);
        }
        public boolean isEmpty() {
            return !m_hasOutstandingTask.get();
        }
        public void shutdown() {
            m_isShuttingDown.set(true);
            DBBPool.BBContainer srcCont;
        }
    }

    private static class EncryptionGateway {

        private final SSLBufferEncrypter m_sslBufferEncrypter;
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final WriteGateway m_writeGateway;
        private final SSLVoltPort m_port;
        protected AtomicBoolean m_isShuttingDown = new AtomicBoolean(false);

        public EncryptionGateway(SSLBufferEncrypter m_sslBufferEncrypter, WriteGateway writeGateway, SSLVoltPort port) {
            this.m_sslBufferEncrypter = m_sslBufferEncrypter;
            this.m_writeGateway = writeGateway;
            this.m_port = port;
        }

        synchronized void start() {
            if (m_hasOutstandingTask.get()) {
                // TODO: this should throw
                return;
            }
            m_hasOutstandingTask.set(true);
            Runnable task = new Runnable() {
                @Override
                public void run() {
                    if (m_isShuttingDown.get()) return;
                    DBBPool.BBContainer fragCont = null;
                    try {
                        fragCont = m_port.getChunkToEncrypt();
                        if (fragCont == null) {
                            m_hasOutstandingTask.set(false);
                            m_writeGateway.enque(EncryptionResult.eosResult);
                            return;
                        }
                        m_hasOutstandingTask.set(true);
                        int nBytesClear = fragCont.b().remaining();
                        DBBPool.BBContainer encCont =  m_sslBufferEncrypter.encryptBuffer(fragCont.b().slice());
                        EncryptionResult er = new EncryptionResult(encCont, nBytesClear);
                        m_writeGateway.enque(er);
                        SSLEncryptionService.instance().submitForEncryption(this);
                    } catch (IOException ioe) {
                        System.err.println("Encryption gateway " + ioe.getMessage());
                        m_hasOutstandingTask.set(false);
                        return;
                    } finally {
                        if (fragCont != null) {
                            fragCont.discard();
                        }
                    }
                }
            };
            SSLEncryptionService.instance().submitForEncryption(task);
        }

        public boolean isEmpty() {
            return !m_hasOutstandingTask.get();
        }

        public void shutdown() {
            m_isShuttingDown.set(true);
        }
    }

    private static class ReadGateway {

        private final InputHandler m_handler;
        private final Queue<List<ByteBuffer>> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final VoltNetwork m_network;
        private final AtomicBoolean m_isShuttingDown = new AtomicBoolean(false);
        private final SSLVoltPort m_port;

        public ReadGateway(VoltNetwork network, SSLVoltPort port, InputHandler handler) {
            this.m_port = port;
            this.m_handler = handler;
            this.m_network = network;
        }

        void enque(List<ByteBuffer> messages) {
            synchronized (m_q) {
                m_q.offer(messages);
                if (m_hasOutstandingTask.compareAndSet(false, true)) {
                    Runnable task = new Runnable() {
                        @Override
                        public void run() {
                            if (m_isShuttingDown.get()) return;
                            List<ByteBuffer> ms = null;
                            synchronized (m_q) {
                                ms = m_q.poll();
                                if (ms == null) {
                                    m_hasOutstandingTask.set(false);
                                    return;
                                }
                            }
                            if (ms.isEmpty()) {
                                m_hasOutstandingTask.set(false);
                                m_port.doneProcessingReads();
                                // start the write half of the process
                                m_port.processWrites();
                                return;
                            }
                            if (ms != null) {
                                int mCount = 0;
                                try {
                                    for (ByteBuffer m : ms) {
                                        m_handler.handleMessage(m, m_port);
                                        mCount++;
                                    }
                                } catch (IOException e) {
                                    System.err.println("ReadGateway: " + e.getMessage());
                                    m_hasOutstandingTask.set(false);
                                    return;
                                }
                            }
                            SSLEncryptionService.instance().submitForDecryption(this);
                        }
                    };
                    SSLEncryptionService.instance().submitForDecryption(task);
                }
            }
        }

        public boolean isEmpty() {
            synchronized (m_q) {
                return m_q.isEmpty() && m_hasOutstandingTask.get() == false;
            }
        }

        public void shutdown() {
            synchronized (m_q) {
                m_isShuttingDown.set(true);
                m_q.clear();
            }
        }
    }

    private class WriteGateway {

        private final Queue<EncryptionResult> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final VoltNetwork m_network;
        private final SSLVoltPort m_port;
        private final AtomicBoolean m_isShuttingDown = new AtomicBoolean(false);
        DBBPool.BBContainer m_leftoverWrites = null;
        int m_leftoverBytesClear = 0;

        public WriteGateway(VoltNetwork network, SSLVoltPort connection) {
            m_network = network;
            m_port = connection;
        }

        void enque(EncryptionResult encRes) {
            synchronized (m_q) {
                m_q.offer(encRes);
                if (m_hasOutstandingTask.compareAndSet(false, true)) {
                    Runnable task = new Runnable() {
                        @Override
                        public void run() {
                            if (m_isShuttingDown.get()) return;

                            // if there are leftover writes, process those rather than looking
                            // at the queue.
                            if (m_leftoverWrites != null) {
                                try {
                                    m_channel.write(m_leftoverWrites.b());
                                    if (m_leftoverWrites.b().hasRemaining()) {
                                        SSLEncryptionService.instance().submitForEncryption(this);
                                    } else {
                                        m_bytesQueued.getAndAdd(-m_leftoverBytesClear);
                                        m_leftoverWrites.discard();
                                        m_leftoverWrites = null;

                                    }
                                } catch (IOException e) {
                                    System.err.println("WriteGateway: " + e.getMessage());
                                    if (m_leftoverWrites != null) {
                                        m_leftoverWrites.discard();
                                        m_hasOutstandingTask.set(false);
                                        return;
                                    }
                                }
                            }

                            EncryptionResult er = null;
                            synchronized (m_q) {
                                er = m_q.poll();
                                if (er == null) {
                                    m_hasOutstandingTask.set(false);
                                    return;
                                }
                            }
                            if (er == EncryptionResult.eosResult) {
                                m_hasOutstandingTask.set(false);
                                m_port.disableWriteSelection();
                                m_port.doneProcessingWrites();
                                return;
                            }
                            DBBPool.BBContainer writesCont = er.m_encCont;
                            try {
                                int bytesWritten = m_channel.write(writesCont.b());
                                if (!writesCont.b().hasRemaining()) {
                                    m_bytesQueued.getAndAdd(-er.m_nBytesClear);
                                    m_writeStream.checkBackpressureEnded();
                                } else {
                                    m_leftoverWrites = writesCont;
                                    m_leftoverBytesClear = er.m_nBytesClear;
                                    m_writeStream.checkBackpressureStarted();
                                }
                                SSLEncryptionService.instance().submitForEncryption(this);
                            } catch (IOException e) {
                                System.err.println("WriteGateway: " + e.getMessage());
                                m_hasOutstandingTask.set(false);
                                return;
                            } finally {
                                writesCont.discard();
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

    public static class EncryptionResult {
        public final DBBPool.BBContainer m_encCont;
        public final int m_nBytesClear;
        public static final EncryptionResult eosResult = new EncryptionResult(null, -1);
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
