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
import java.util.concurrent.atomic.AtomicBoolean;

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

    private boolean processingReads = false;
    private boolean processingWrites = false;

    public SSLVoltPort(VoltNetwork network, InputHandler handler, InetSocketAddress remoteAddress, NetworkDBBPool pool, SSLEngine sslEngine) {
        super(network, handler, remoteAddress, pool);
        this.m_sslEngine = sslEngine;
        this.m_sslBufferDecrypter = new SSLBufferDecrypter(sslEngine);
        int appBufferSize = m_sslEngine.getSession().getApplicationBufferSize();
        // the app buffer size will sometimes be greater than 16k, but the ssl engine won't
        // encrypt more than 16k bytes at a time.  So it's simpler to not go over 16k.
        m_appBufferSize = Math.min(appBufferSize, 16 * 1024);
        int packetBufferSize = m_sslEngine.getSession().getPacketBufferSize();
        this.m_sslBufferEncrypter = new SSLBufferEncrypter(sslEngine, appBufferSize, packetBufferSize);
        this.m_writeGateway = new WriteGateway(network, this);
        this.m_encryptionGateway = new EncryptionGateway(m_sslBufferEncrypter, m_writeGateway, this);
        this.m_sslMessageParser = new SSLMessageParser();
        this.m_frameHeader = ByteBuffer.allocate(5);
        this.m_dstBufferCont = DBBPool.allocateDirect(packetBufferSize);
        this.m_dstBuffer = m_dstBufferCont.b();
        m_dstBuffer.clear();
        this.m_readGateway = new ReadGateway(network, this, handler);
        this.m_decryptionGateway = new DecryptionGateway(m_sslBufferDecrypter, m_readGateway, m_sslMessageParser, m_dstBuffer);
        this.m_port = this;
    }

    @Override
    public void run() throws IOException {
        int nRead = 0;
        try {
            if (!processingReads && !processingWrites) {
                disableWriteSelection();
                final int maxRead = m_handler.getMaxRead();
                nRead = fillReadStream(maxRead);
                if (nRead > 0) {
                    queueDecryptionTasks();
                    processingReads = true;
                } else {
                    enableWriteSelection();
                }
            }

            if (processingReads) {
                if (!isReadStreamProcessed()) {
                    m_network.nudgeChannel(this);
                    return;
                } else {
                    processingReads = false;
                    enableWriteSelection();
                }
            }

            if (!processingReads && !processingWrites) {
                boolean responsesReceived = buildEncryptionTasks();
                if (responsesReceived) {
                    processingWrites = true;
                }
            }

            if (processingWrites && !isWriteStreamProcessed()) {
                m_network.nudgeChannel(this);
                return;
            } else {
                processingWrites = false;
            }

            if (m_isShuttingDown) {
                unregistered();
                return;
            }
        } catch (IOException ioe) {
            while (!gatewaysEmpty()) {
                System.out.println("Waiting for ssl task to finish.");
            }
            throw ioe;
        } finally {
            synchronized (m_lock) {
                assert (m_running == true);
                m_running = false;
            }
        }
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

    private void queueDecryptionTasks() {
        int read;
        while (true) {
            if (m_nextFrameLength == 0) {
                read = readStream().getBytes(m_frameHeader);
                if (m_frameHeader.hasRemaining()) {
                    break;
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
                break;
            } else {
                m_decryptionGateway.enque(m_frameCont);
                m_frameCont = null;
                m_nextFrameLength = 0;
            }
        }
    }

    /**
     * Swap the two queues of DeferredSerializations.  Serialize and create callables
     * to consume the resulting write buffers.
     * Similar functionality to NIOWriteStreamBase.swapAndSerializeQueuedWrites().
     * @param pool  The network byte buffer pool.
     * @return
     * @throws IOException
     */
    private boolean buildEncryptionTasks() throws IOException {
        final ArrayDeque<DeferredSerialization> oldlist = m_writeStream.getQueuedWrites();
        if (oldlist.isEmpty()) return false;
        DeferredSerialization ds = null;
        DBBPool.BBContainer outCont = null;
        while ((ds = oldlist.poll()) != null) {
            final int serializedSize = ds.getSerializedSize();
            if (serializedSize == DeferredSerialization.EMPTY_MESSAGE_LENGTH) continue;
            //Fastpath, serialize to direct buffer creating no garbage
            if (outCont == null) {
                outCont = m_pool.acquire();
                outCont.b().clear();
            }
            if (outCont.b().remaining() >= serializedSize) {
                final int oldLimit =  outCont.b().limit();
                outCont.b().limit( outCont.b().position() + serializedSize);
                final ByteBuffer slice =  outCont.b().slice();
                ds.serialize(slice);
                slice.position(0);
                outCont.b().position(outCont.b().limit());
                outCont.b().limit(oldLimit);
            } else {
                // first write out the current allocated container.
                if (outCont.b().position() > 0) {
                    outCont.b().flip();
                    m_encryptionGateway.enque(outCont);
                } else {
                    outCont.discard();
                }
                outCont = null;
                //Slow path serialize to heap, and then put in buffers
                ByteBuffer buf = ByteBuffer.allocate(serializedSize);
                ds.serialize(buf);
                buf.position(0);
                while (buf.hasRemaining()) {
                    if (buf.remaining() > m_appBufferSize) {
                        int oldLimit = buf.limit();
                        buf.limit(buf.position() + m_appBufferSize);
                        m_encryptionGateway.enque(DBBPool.wrapBB(buf.slice()));
                        buf.position(buf.position() + m_appBufferSize);
                        buf.limit(oldLimit);
                    } else {
                        m_encryptionGateway.enque(DBBPool.wrapBB(buf.slice()));
                        buf.position(buf.limit());
                    }
                }
            }
        }
        if (outCont != null) {
            if (outCont.b().position() > 0) {
                outCont.b().flip();
                m_encryptionGateway.enque(outCont);
            } else {
                outCont.discard();
                outCont = null;
            }
        }
        return true;
    }

    @Override
    void unregistered() {
        m_writeGateway.shutdown();
        m_readGateway.shutdown();
        m_encryptionGateway.shutdown();
        m_decryptionGateway.shutdown();
        while (!gatewaysEmpty()) {
            System.out.println("Waiting for decryption task to finish.");
        }
        super.unregistering();
        super.unregistered();
        m_dstBufferCont.discard();
        m_dstBufferCont = null;
    }

    private class DecryptionGateway {
        private final SSLBufferDecrypter m_sslBufferDecrypter;
        private final SSLMessageParser m_sslMessageParser;
        private final ByteBuffer m_dstBuffer;
        private final Queue<DBBPool.BBContainer> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final ReadGateway m_readGateway;
        protected boolean m_isShuttingDown = false;

        public DecryptionGateway(SSLBufferDecrypter m_sslBufferDecrypter, ReadGateway readGateway, SSLMessageParser sslMessageParser, ByteBuffer dstBuffer) {
            this.m_sslBufferDecrypter = m_sslBufferDecrypter;
            this.m_sslMessageParser = sslMessageParser;
            this.m_dstBuffer = dstBuffer;
            m_readGateway = readGateway;
        }

        void enque(final DBBPool.BBContainer srcCont) {
            boolean checkOutstandingTask;
            synchronized (m_q) {
                if (m_isShuttingDown || srcCont.b().position() <= 0) {
                    srcCont.discard();
                    return;
                }
                m_q.add(srcCont);
                checkOutstandingTask = m_hasOutstandingTask.compareAndSet(false, true);
            }
            if (checkOutstandingTask) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        DBBPool.BBContainer srcC;
                        synchronized (m_q) {
                            if (m_isShuttingDown) {
                                m_hasOutstandingTask.set(false);
                                return;
                            }
                            srcC = m_q.peek();
                        }
                        if (srcC != null) {
                            List<ByteBuffer> messages = new ArrayList<>();
                            try {
                                ByteBuffer srcBuffer = srcC.b();
                                srcBuffer.flip();
                                m_dstBuffer.limit(m_dstBuffer.capacity());
                                m_sslBufferDecrypter.unwrap(srcBuffer, m_dstBuffer);
                                if (m_dstBuffer.hasRemaining()) {
                                    ByteBuffer message;
                                    while ((message = m_sslMessageParser.message(m_dstBuffer)) != null) {
                                        messages.add(message);
                                    }
                                }
                                m_dstBuffer.clear();
                                m_readGateway.enque(messages);
                            } catch (IOException e) {
                                System.err.println(e.getMessage());
                            } finally {
                                srcC.discard();
                            }
                            synchronized (m_q) {
                                if (m_isShuttingDown) {
                                    m_hasOutstandingTask.set(false);
                                    return;
                                }
                                m_q.poll();
                                if (!m_q.isEmpty()) {
                                    SSLEncryptionService.instance().submitForDecryption(this);
                                } else {
                                    m_hasOutstandingTask.set(false);
                                }
                            }
                        }
                    }
                };
                SSLEncryptionService.instance().submitForDecryption(task);
            }
        }
        public boolean isEmpty() {
            synchronized (m_q) {
                return m_q.isEmpty() && m_hasOutstandingTask.get() == false;
            }
        }
        public void shutdown() {
            synchronized (m_q) {
                m_isShuttingDown = true;
                DBBPool.BBContainer srcCont;
                while ((srcCont = m_q.poll()) != null) {
                    srcCont.discard();
                }
            }
        }
    }

    private class EncryptionGateway {

        private final SSLBufferEncrypter m_sslBufferEncrypter;
        private final Queue<DBBPool.BBContainer> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final WriteGateway m_writeGateway;
        private final VoltPort m_port;
        protected boolean m_isShuttingDown = false;

        public EncryptionGateway(SSLBufferEncrypter m_sslBufferEncrypter, WriteGateway writeGateway, VoltPort port) {
            this.m_sslBufferEncrypter = m_sslBufferEncrypter;
            m_writeGateway = writeGateway;
            m_port = port;
        }

        void enque(final DBBPool.BBContainer fragmentCont) {
            boolean checkOutstandingTask;
            synchronized (m_q) {
                if (m_isShuttingDown) {
                    fragmentCont.discard();
                    return;
                }
                m_q.add(fragmentCont);
                checkOutstandingTask = m_hasOutstandingTask.compareAndSet(false, true);
            }
            if (checkOutstandingTask) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        DBBPool.BBContainer fragCont;
                        synchronized (m_q) {
                            if (m_isShuttingDown) {
                                m_hasOutstandingTask.set(false);
                                return;
                            }
                            fragCont = m_q.peek();
                        }
                        if (fragCont != null) {
                            DBBPool.BBContainer encCont = null;
                            try {
                                ByteBuffer fragment = fragCont.b();
                                encCont = m_sslBufferEncrypter.encryptBuffer(fragment.slice());
                                EncryptionResult er = new EncryptionResult(encCont, encCont.b().remaining());
                                m_network.updateQueued(er.m_nBytesEncrypted, false, m_port);
                                m_writeGateway.enque(er);
                            } catch (IOException e) {
                                System.err.println("EncryptionGateway: " + e.getMessage());
                                if (encCont != null) {
                                    encCont.discard();
                                }
                            }
                        }
                        synchronized (m_q) {
                            if (m_isShuttingDown) {
                                m_hasOutstandingTask.set(false);
                                return;
                            }
                            fragCont = m_q.poll();
                            fragCont.discard();
                            if (!m_q.isEmpty()) {
                                SSLEncryptionService.instance().submitForEncryption(this);
                            } else {
                                m_hasOutstandingTask.set(false);
                            }
                        }
                    }
                };
                SSLEncryptionService.instance().submitForEncryption(task);
            }
        }

        public boolean isEmpty() {
            synchronized (m_q) {
                return m_q.isEmpty() && m_hasOutstandingTask.get() == false;
            }
        }

        public void shutdown() {
            synchronized (m_q) {
                m_isShuttingDown = true;
                DBBPool.BBContainer fragCont;
                while ((fragCont = m_q.poll()) != null) {
                    fragCont.discard();
                }
            }
        }
    }

    private static class ReadGateway {

        private final Connection m_conn;
        private final InputHandler m_handler;
        private final Queue<List<ByteBuffer>> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final VoltNetwork m_network;
        protected boolean m_isShuttingDown = false;

        public ReadGateway(VoltNetwork network, Connection conn, InputHandler handler) {
            this.m_conn = conn;
            this.m_handler = handler;
            m_network = network;
        }

        void enque(List<ByteBuffer> messages) {
            boolean checkOutstandingTask;
            synchronized (m_q) {
                if (m_isShuttingDown) return;
                m_q.add(messages);
                checkOutstandingTask = m_hasOutstandingTask.compareAndSet(false, true);
            }
            if (checkOutstandingTask) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        List<ByteBuffer> ms;
                        synchronized (m_q) {
                            if (m_isShuttingDown) {
                                m_hasOutstandingTask.set(false);
                                return;
                            }
                            ms = m_q.peek();
                        }
                        if (ms != null) {
                            int mCount = 0;
                            try {
                                for (ByteBuffer m : ms) {
                                    m_handler.handleMessage(m, m_conn);
                                    mCount++;
                                }
                            } catch (IOException e) {
                                System.err.println(e.getMessage());
                            }
                        }
                        synchronized (m_q) {
                            if (m_isShuttingDown) {
                                m_hasOutstandingTask.set(false);
                                return;
                            }
                            m_q.poll();
                            if (!m_q.isEmpty()) {
                                SSLEncryptionService.instance().submitForDecryption(this);
                            } else {
                                m_hasOutstandingTask.set(false);
                            }
                        }
                    }
                };
                SSLEncryptionService.instance().submitForDecryption(task);
            }
        }

        public boolean isEmpty() {
            synchronized (m_q) {
                return m_q.isEmpty() && m_hasOutstandingTask.get() == false;
            }
        }

        public void shutdown() {
            synchronized (m_q) {
                m_isShuttingDown = true;
                m_q.clear();
            }
        }
    }

    private class WriteGateway {

        private final Queue<EncryptionResult> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);
        private final VoltNetwork m_network;
        private final VoltPort m_port;
        protected boolean m_isShuttingDown = false;

        public WriteGateway(VoltNetwork network, VoltPort connection) {
            m_network = network;
            m_port = connection;
        }

        void enque(EncryptionResult encRes) {
            boolean checkOutstandingTask;
            synchronized (m_q) {
                if (m_isShuttingDown) return;
                m_q.add(encRes);
                checkOutstandingTask = m_hasOutstandingTask.compareAndSet(false, true);
            }
            if (checkOutstandingTask) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        EncryptionResult er;
                        synchronized (m_q) {
                            if (m_isShuttingDown) {
                                m_hasOutstandingTask.set(false);
                                return;
                            }
                            er = m_q.peek();
                        }
                        if (er != null) {
                            boolean hasRemaining = false;
                            DBBPool.BBContainer writesCont = er.m_encCont;
                            try {
                                int bytesWritten = m_channel.write(writesCont.b());
                                m_network.updateQueued(bytesWritten, false, m_port);
                                hasRemaining = writesCont.b().hasRemaining();
                                if (writesCont.b().hasRemaining()) {
                                    m_writeStream.checkBackpressureStarted();
                                } else {
                                    synchronized (m_q) {
                                        if (m_isShuttingDown) {
                                            m_hasOutstandingTask.set(false);
                                            return;
                                        }
                                        m_q.poll();
                                        writesCont.discard();
                                    }
                                }
                            } catch (IOException e) {
                                er.m_encCont.discard();
                                System.err.println("WriteGateway: " + e.getMessage());
                            }
                        }
                        synchronized (m_q) {
                            if (m_isShuttingDown) {
                                m_hasOutstandingTask.set(false);
                                return;
                            }
                            if (!m_q.isEmpty()) {
                                SSLEncryptionService.instance().submitForEncryption(this);
                            } else {
                                m_writeStream.checkBackpressureEnded();
                                m_hasOutstandingTask.set(false);
                            }
                        }
                    }
                };
                SSLEncryptionService.instance().submitForEncryption(task);
            }
        }
        public boolean isEmpty() {
            synchronized (m_q) {
                return m_q.isEmpty() && m_hasOutstandingTask.get() == false;
            }
        }

        public void shutdown() {
            synchronized (m_q) {
                m_isShuttingDown = true;
                m_q.clear();
            }
        }
    }

    public class EncryptionResult {
        public final DBBPool.BBContainer m_encCont;
        public final int m_nBytesEncrypted;
        public EncryptionResult(DBBPool.BBContainer encCont, int nBytesEncrypted) {
            this.m_encCont = encCont;
            this.m_nBytesEncrypted = nBytesEncrypted;
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
