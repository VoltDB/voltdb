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

import com.google_voltpatches.common.util.concurrent.ListenableFuture;
import com.google_voltpatches.common.util.concurrent.SettableFuture;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.DeferredSerialization;
import org.voltcore.utils.Pair;
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
    private ByteBuffer m_dstBuffer;
    private final SSLMessageParser m_sslMessageParser;

    private ByteBuffer m_frameHeader;
    private DBBPool.BBContainer m_frameCont;

    private int m_nextFrameLength = 0;

    private final Queue<ListenableFuture<List<ByteBuffer>>> m_readTasks = new ArrayDeque<>();
    private final Queue<ListenableFuture<EncryptionResult>> m_writeTasks = new ArrayDeque<>();
    private final DecryptionGateway m_decryptionGateway;
    private final EncryptionGateway m_encryptionGateway;

    private DBBPool.BBContainer m_remainingBytesToWrite;
    private int m_remainingWritesByteCount;
    private final int m_appBufferSize;

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
        this.m_encryptionGateway = new EncryptionGateway(m_sslBufferEncrypter);
        this.m_sslMessageParser = new SSLMessageParser();
        this.m_frameHeader = ByteBuffer.allocate(5);
        this.m_dstBufferCont = DBBPool.allocateDirect(packetBufferSize);
        this.m_dstBuffer = m_dstBufferCont.b();
        m_dstBuffer.clear();
        this.m_decryptionGateway = new DecryptionGateway(m_sslBufferDecrypter, m_sslMessageParser, m_dstBuffer);
    }

    public void run() throws IOException {
        int nRead = 0;
        try {
            final int maxRead = m_handler.getMaxRead();
            if (maxRead > 0) {
                nRead = fillReadStream(maxRead);
            }

            if (nRead > 0) {
                buildReadTasks();
            }

            // if the write stream didn't fully drain last time, there might be
            // leftover write buffers.
            if (m_remainingBytesToWrite != null) {
                m_remainingBytesToWrite = writeBuffer(m_remainingBytesToWrite);
                if (m_remainingBytesToWrite == null) {
                    m_writeStream.updateQueued(-m_remainingWritesByteCount, false);
                }
            }

            // if the leftover bytes were written, add any new tasks to the the queue.
            if (m_remainingBytesToWrite == null) {
                buildWriteTasks();
            }

            ListenableFuture<List<ByteBuffer>> readFuture = null;
            ListenableFuture<EncryptionResult> writeFuture = null;
            while (readFuture != null || writeFuture != null || !m_readTasks.isEmpty() || (!m_writeTasks.isEmpty() && m_remainingBytesToWrite == null)) {
                if (readFuture == null && !m_readTasks.isEmpty()) {
                    readFuture = m_readTasks.poll();
                }
                if (writeFuture == null && !m_writeTasks.isEmpty() && m_remainingBytesToWrite == null) {
                    writeFuture = m_writeTasks.poll();
                }

                if (readFuture != null && readFuture.isDone()) {
                    handleReadFuture(readFuture);
                    readFuture = null;
                } else if (writeFuture != null && writeFuture.isDone()) {
                    handleWriteFuture(writeFuture);
                    writeFuture = null;
                } else if (readFuture != null) {
                    handleReadFuture(readFuture);
                    readFuture = null;
                } else {
                    handleWriteFuture(writeFuture);
                    writeFuture = null;
                }
            }
        } finally {
            if (m_writeTasks.isEmpty()) {
                m_writeStream.checkBackpressureEnded();
            }

            synchronized (m_lock) {
                assert (m_running == true);
                m_running = false;
            }
        }
    }

    private void handleReadFuture(ListenableFuture<List<ByteBuffer>> readFuture) throws IOException {
        List<ByteBuffer> messages = null;
        try {
            messages = readFuture.get();
            handleRead(messages);
        } catch (Exception e) {
            throw new IOException("decryption task failed in voltport " + m_channel.socket().getRemoteSocketAddress(), e);
        }
    }

    private void handleWriteFuture(ListenableFuture<EncryptionResult> writeFuture) throws IOException {
        EncryptionResult er = null;
        try {
            er = writeFuture.get();
        } catch (Exception e) {
            throw new IOException("encryption task failed in voltport " + m_channel.socket().getRemoteSocketAddress(), e);
        }
        handleWrite(er);
    }

    private void handleRead(List<ByteBuffer> messages) throws IOException {
        for (ByteBuffer message : messages) {
            m_handler.handleMessage(message, this);
            m_messagesRead++;
        }
    }

    private boolean handleWrite(EncryptionResult er) throws IOException {
        DBBPool.BBContainer remainingBytesToWrite = writeBuffer(er.m_encCont);
        if (remainingBytesToWrite != null) {
            m_remainingBytesToWrite = remainingBytesToWrite;
            m_remainingWritesByteCount = er.m_nBytesClear;
            return false;
        } else {
            m_writeStream.updateQueued(-er.m_nBytesClear, false);
            return true;
        }
    }

    private DBBPool.BBContainer writeBuffer(DBBPool.BBContainer bufCont) throws IOException {
        ByteBuffer buf = bufCont.b();
        int bytesWritten = m_channel.write(buf);
        if (buf.hasRemaining()) {
            m_writeStream.checkBackpressureStarted();
            return bufCont;
        } else {
            bufCont.discard();
            bufCont = null;
            return null;
        }
    }

    private void buildReadTasks() {
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
                m_readTasks.add(m_decryptionGateway.enque(m_frameCont));
                m_nextFrameLength = 0;
                m_frameCont = null;
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
    void buildWriteTasks() throws IOException {
        final ArrayDeque<DeferredSerialization> oldlist = m_writeStream.getQueuedWrites();
        if (oldlist.isEmpty()) return;
        DeferredSerialization ds = null;
        int bytesQueued = 0;
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
                bytesQueued += slice.remaining();
                outCont.b().position(outCont.b().limit());
                outCont.b().limit(oldLimit);
            } else {
                // first write out the current allocated container.
                if (outCont.b().position() > 0) {
                    outCont.b().flip();
                    m_writeTasks.add(m_encryptionGateway.enque(outCont));
                } else {
                    outCont.discard();
                }
                outCont = null;
                //Slow path serialize to heap, and then put in buffers
                ByteBuffer buf = ByteBuffer.allocate(serializedSize);
                ds.serialize(buf);
                buf.position(0);
                bytesQueued += buf.remaining();
                while (buf.hasRemaining()) {
                    if (buf.remaining() > m_appBufferSize) {
                        int oldLimit = buf.limit();
                        buf.limit(buf.position() + m_appBufferSize);
                        m_writeTasks.add(m_encryptionGateway.enque(DBBPool.dummyWrapBB(buf.slice())));
                        buf.position(buf.position() + m_appBufferSize);
                        buf.limit(oldLimit);
                    } else {
                        m_writeTasks.add(m_encryptionGateway.enque(DBBPool.dummyWrapBB(buf.slice())));
                        buf.position(buf.limit());
                    }
                }
            }
        }
        if (outCont != null) {
            if (outCont.b().position() > 0) {
                outCont.b().flip();
                m_writeTasks.add(m_encryptionGateway.enque(outCont));
            } else {
                outCont.discard();
                outCont = null;
            }
        }
        m_writeStream.updateQueued(bytesQueued, true);
    }

    public static class EncryptionResult {
        public final DBBPool.BBContainer m_encCont;
        public final int m_nBytesClear;
        public EncryptionResult(DBBPool.BBContainer encCont, int nClearBytes) {
            this.m_encCont = encCont;
            this.m_nBytesClear = nClearBytes;
        }
    }

    @Override
    void unregistered() {
        super.unregistered();
        m_dstBufferCont.discard();
        m_dstBufferCont = null;
    }

    private static class DecryptionGateway {
        private final SSLBufferDecrypter m_sslBufferDecrypter;
        private final SSLMessageParser m_sslMessageParser;
        private final ByteBuffer m_dstBuffer;
        private final Queue<Pair<DBBPool.BBContainer, SettableFuture<List<ByteBuffer>>>> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);

        public DecryptionGateway(SSLBufferDecrypter m_sslBufferDecrypter, SSLMessageParser sslMessageParser, ByteBuffer dstBuffer) {
            this.m_sslBufferDecrypter = m_sslBufferDecrypter;
            this.m_sslMessageParser = sslMessageParser;
            this.m_dstBuffer = dstBuffer;
        }

        ListenableFuture<List<ByteBuffer>> enque(DBBPool.BBContainer srcCont) {

            SettableFuture<List<ByteBuffer>> fut = SettableFuture.create();
            boolean checkOutstandingTask;
            synchronized (m_q) {
                m_q.add(new Pair<DBBPool.BBContainer, SettableFuture<List<ByteBuffer>>>(srcCont, fut));
                checkOutstandingTask = m_hasOutstandingTask.compareAndSet(false, true);
                if (checkOutstandingTask) {
                    Runnable task = new Runnable() {
                        @Override
                        public void run() {
                            Pair<DBBPool.BBContainer, SettableFuture<List<ByteBuffer>>> p;
                            synchronized (m_q) {
                                p = m_q.peek();
                            }
                            if (p != null) {
                                DBBPool.BBContainer srcC = p.getFirst();

                                SettableFuture<List<ByteBuffer>> f = p.getSecond();
                                List<ByteBuffer> messages = new ArrayList<>();
                                try {
                                    ByteBuffer srcBuffer = srcC.b();
                                    if (srcBuffer.position() > 0) {
                                        srcBuffer.flip();
                                    } else {
                                        // won't be able to decrypt is the src buffer is empty.
                                        f.set(messages);
                                        return;
                                    }
                                    m_dstBuffer.limit(m_dstBuffer.capacity());
                                    m_sslBufferDecrypter.unwrap(srcBuffer, m_dstBuffer);
                                } catch (IOException e) {
                                    f.setException(e);
                                    return;
                                } finally {
                                    srcC.discard();
                                    srcC = null;
                                }
                                if (m_dstBuffer.hasRemaining()) {
                                    ByteBuffer message;
                                    while ((message = m_sslMessageParser.message(m_dstBuffer)) != null) {
                                        messages.add(message);
                                    }
                                }
                                m_dstBuffer.clear();
                                f.set(messages);
                            }
                            synchronized (m_q) {
                                m_q.poll();

                                if (!m_q.isEmpty()) {
                                    SSLEncryptionService.instance().submit(this);
                                } else {
                                    m_hasOutstandingTask.set(false);
                                }
                            }
                        }
                    };
                    SSLEncryptionService.instance().submit(task);

                } else {
                }
            }
            return fut;
        }
    }

    private static class EncryptionGateway {

        private final SSLBufferEncrypter m_sslBufferEncrypter;
        private final Queue<Pair<DBBPool.BBContainer, SettableFuture<EncryptionResult>>> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);

        public EncryptionGateway(SSLBufferEncrypter m_sslBufferEncrypter) {
            this.m_sslBufferEncrypter = m_sslBufferEncrypter;
        }

        ListenableFuture<EncryptionResult> enque(DBBPool.BBContainer fragmentCont) {
            SettableFuture<EncryptionResult> fut = SettableFuture.create();

            boolean checkOutstandingTask;
            synchronized (m_q) {
                m_q.add(new Pair<DBBPool.BBContainer, SettableFuture<EncryptionResult>>(fragmentCont, fut));
                checkOutstandingTask = m_hasOutstandingTask.compareAndSet(false, true);
                if (checkOutstandingTask) {
                    Runnable task = new Runnable() {
                        @Override
                        public void run() {
                            DBBPool.BBContainer fragCont = null;
                            Pair<DBBPool.BBContainer, SettableFuture<EncryptionResult>> p;
                            synchronized (m_q) {
                                p = m_q.peek();
                            }
                            if (p != null) {
                                fragCont = p.getFirst();
                                SettableFuture<EncryptionResult> f = p.getSecond();
                                try {
                                    ByteBuffer fragment = fragCont.b();
                                    int nBytes = fragment.remaining();
                                    DBBPool.BBContainer encCont = m_sslBufferEncrypter.encryptBuffer(fragment.slice());
                                    f.set(new EncryptionResult(encCont, nBytes));
                                } catch (IOException e) {
                                    f.setException(e);
                                } finally {
                                    fragCont.discard();
                                    fragCont = null;
                                }
                            }
                            synchronized (m_q) {
                                m_q.poll();
                                if (!m_q.isEmpty()) {
                                    SSLEncryptionService.instance().submit(this);
                                } else {
                                    m_hasOutstandingTask.set(false);
                                }
                            }
                        }
                    };
                    SSLEncryptionService.instance().submit(task);
                }
            }
            return fut;
        }
    }
}
