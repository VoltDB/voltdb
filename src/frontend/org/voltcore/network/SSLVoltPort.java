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
import java.util.concurrent.ExecutionException;
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

    private final Queue<ListenableFuture<List<ByteBuffer>>> m_decryptionTasks = new ArrayDeque<>();
    private final Queue<ListenableFuture<EncryptionResult>> m_encryptionTasks = new ArrayDeque<>();
    private final DecryptionGateway m_decryptionGateway;
    private final EncryptionGateway m_encryptionGateway;
    private final ReadGateway m_readGateway;
    private WriteGateway m_writeGateway;
    private final Queue<ListenableFuture<Integer>> m_readTasks = new ArrayDeque<>();
    private final Queue<ListenableFuture<WriteResult>> m_writeTasks = new ArrayDeque<>();

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
        this.m_readGateway = new ReadGateway(this, handler);
        this.m_writeGateway = new WriteGateway();
    }

    @Override
    public void run() throws IOException {
        int nRead = 0;
        try {
            final int maxRead = m_handler.getMaxRead();
            if (maxRead > 0) {
                nRead = fillReadStream(maxRead);
            }

            if (nRead > 0) {
                buildDecryptionTasks();
            }

            buildEncryptionTasks();

            while (hasTasks()) {
                processDecryptionTasks();
                processDoneReadTasks();
                processDoneEncryptionTasks();
                processDoneWriteTasks();
            }
        } finally {
            if (m_encryptionTasks.isEmpty() && m_writeTasks.isEmpty()) {
                m_writeStream.checkBackpressureEnded();
            }

            synchronized (m_lock) {
                assert (m_running == true);
                m_running = false;
            }
        }
    }

    private boolean hasTasks() {
        return !m_decryptionTasks.isEmpty() || !m_readTasks.isEmpty() || !m_encryptionTasks.isEmpty() || ((!m_writeTasks.isEmpty() && !writeStream().hadBackPressure()));
    }

    private void processDecryptionTasks() throws IOException {
        try {
            if (!m_decryptionTasks.isEmpty()) {
                m_readTasks.add(m_readGateway.enque(m_decryptionTasks.poll().get()));
            }
            while (!m_decryptionTasks.isEmpty() && m_decryptionTasks.peek().isDone()) {
                m_readTasks.add(m_readGateway.enque(m_decryptionTasks.poll().get()));
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("read task failed in voltport " + m_channel.socket().getRemoteSocketAddress(), e);
        }
    }

    private void processDoneReadTasks() throws IOException {
        try {
            if (!m_readTasks.isEmpty()) {
                m_messagesRead += m_readTasks.poll().get();
            }
            while (!m_readTasks.isEmpty() && m_readTasks.peek().isDone()) {
                m_messagesRead += m_readTasks.poll().get();
            }
        } catch (Exception e) {
            throw new IOException("read task failed in voltport " + m_channel.socket().getRemoteSocketAddress(), e);
        }
    }

    private void processDoneEncryptionTasks() throws IOException {
        try {
            if (!m_encryptionTasks.isEmpty()) {
                handleEncryptionTask(m_encryptionTasks.poll().get());
            }
            while (!m_encryptionTasks.isEmpty() && m_encryptionTasks.peek().isDone()) {
                handleEncryptionTask(m_encryptionTasks.poll().get());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new IOException("read task failed in voltport " + m_channel.socket().getRemoteSocketAddress(), e);
        }
    }

    private void handleEncryptionTask(EncryptionResult er) {
        writeStream().updateQueued(er.m_nBytesEncrypted, false);
        m_writeTasks.add(m_writeGateway.enque(er));
    }

    private void processDoneWriteTasks() throws IOException {
        try {
            if (!m_writeTasks.isEmpty()) {
                if (! handleWriteTask(m_writeTasks.poll().get())) {
                    return;
                }
            }
            while (!m_writeTasks.isEmpty() && m_writeTasks.peek().isDone()) {
                if (! handleWriteTask(m_writeTasks.poll().get())) {
                    return;
                }
            }
        } catch (Exception e) {
            throw new IOException("write task failed in voltport " + m_channel.socket().getRemoteSocketAddress(), e);
        }
    }

    private boolean handleWriteTask(WriteResult wr) {
        writeStream().updateQueued(-wr.m_bytesWritten, false);
        if (wr.m_bytesWritten < wr.m_bytesQueued) {
            m_writeStream.checkBackpressureStarted();
            return false;
        }
        return true;
    }

    private void buildDecryptionTasks() {
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
                m_decryptionTasks.add(m_decryptionGateway.enque(m_frameCont));
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
    private void buildEncryptionTasks() throws IOException {
        final ArrayDeque<DeferredSerialization> oldlist = m_writeStream.getQueuedWrites();
        if (oldlist.isEmpty()) return;
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
                    m_encryptionTasks.add(m_encryptionGateway.enque(outCont));
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
                        m_encryptionTasks.add(m_encryptionGateway.enque(DBBPool.wrapBB(buf.slice())));
                        buf.position(buf.position() + m_appBufferSize);
                        buf.limit(oldLimit);
                    } else {
                        m_encryptionTasks.add(m_encryptionGateway.enque(DBBPool.wrapBB(buf.slice())));
                        buf.position(buf.limit());
                    }
                }
            }
        }
        if (outCont != null) {
            if (outCont.b().position() > 0) {
                outCont.b().flip();
                m_encryptionTasks.add(m_encryptionGateway.enque(outCont));
            } else {
                outCont.discard();
                outCont = null;
            }
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

        ListenableFuture<List<ByteBuffer>> enque(final DBBPool.BBContainer srcCont) {
            SettableFuture<List<ByteBuffer>> fut = SettableFuture.create();
            if (srcCont.b().position() <= 0) return fut;

            synchronized (m_q) {
                m_q.add(new Pair<>(srcCont, fut));
            }
            if (m_hasOutstandingTask.compareAndSet(false, true)) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        Pair<DBBPool.BBContainer, SettableFuture<List<ByteBuffer>>> p;
                        p = m_q.poll();
                        if (p != null) {
                            DBBPool.BBContainer srcC = p.getFirst();

                            SettableFuture<List<ByteBuffer>> f = p.getSecond();
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
                                f.set(messages);
                            } catch (IOException e) {
                                f.setException(e);
                            } finally {
                                srcC.discard();
                            }
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

        ListenableFuture<EncryptionResult> enque(final DBBPool.BBContainer fragmentCont) {
            SettableFuture<EncryptionResult> fut = SettableFuture.create();

            synchronized (m_q) {
                m_q.add(new Pair<>(fragmentCont, fut));
            }
            if (m_hasOutstandingTask.compareAndSet(false, true)) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        Pair<DBBPool.BBContainer, SettableFuture<EncryptionResult>> p;
                        p = m_q.poll();
                        if (p != null) {
                            DBBPool.BBContainer fragCont = p.getFirst();
                            SettableFuture<EncryptionResult> f = p.getSecond();
                            try {
                                ByteBuffer fragment = fragCont.b();
                                DBBPool.BBContainer encCont = m_sslBufferEncrypter.encryptBuffer(fragment.slice());
                                f.set(new EncryptionResult(encCont, encCont.b().remaining()));
                            } catch (IOException e) {
                                f.setException(e);
                            } finally {
                                fragCont.discard();
                            }
                        }
                        if (!m_q.isEmpty()) {
                            SSLEncryptionService.instance().submitForEncryption(this);
                        } else {
                            m_hasOutstandingTask.set(false);
                        }
                    }
                };
                SSLEncryptionService.instance().submitForEncryption(task);
            }
            return fut;
        }
    }

    private static class ReadGateway {

        private final Connection m_conn;
        private final InputHandler m_handler;
        private final Queue<Pair<List<ByteBuffer>, SettableFuture<Integer>>> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);

        public ReadGateway(Connection conn, InputHandler handler) {
            this.m_conn = conn;
            this.m_handler = handler;
        }

        ListenableFuture<Integer> enque(List<ByteBuffer> messages) {
            SettableFuture<Integer> fut = SettableFuture.create();
            boolean checkOutstandingTask;
            synchronized (m_q) {
                m_q.add(new Pair<List<ByteBuffer>, SettableFuture<Integer>>(messages, fut));
                checkOutstandingTask = m_hasOutstandingTask.compareAndSet(false, true);
            }
            if (checkOutstandingTask) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        Pair<List<ByteBuffer>, SettableFuture<Integer>> p;
                        synchronized (m_q) {
                            p = m_q.poll();
                        }
                        if (p != null) {
                            int mCount = 0;
                            List<ByteBuffer> ms = p.getFirst();
                            SettableFuture<Integer> f = p.getSecond();
                            try {
                                for (ByteBuffer m : ms) {
                                    m_handler.handleMessage(m, m_conn);
                                    mCount++;
                                }
                                f.set(mCount);
                            } catch (IOException e) {
                                f.setException(e);
                            }
                        }
                        synchronized (m_q) {
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
            return fut;
        }
    }

    private class WriteGateway {

        private final Queue<Pair<EncryptionResult, SettableFuture<WriteResult>>> m_q = new ArrayDeque<>();
        private final AtomicBoolean m_hasOutstandingTask = new AtomicBoolean(false);

        ListenableFuture<WriteResult> enque(EncryptionResult encRes) {
            assert m_channel != null;
            SettableFuture<WriteResult> fut = SettableFuture.create();
            boolean checkOutstandingTask;
            synchronized (m_q) {
                m_q.add(new Pair<EncryptionResult, SettableFuture<WriteResult>>(encRes, fut));
                checkOutstandingTask = m_hasOutstandingTask.compareAndSet(false, true);
            }
            if (checkOutstandingTask) {
                Runnable task = new Runnable() {
                    @Override
                    public void run() {
                        Pair<EncryptionResult, SettableFuture<WriteResult>> p;
                        synchronized (m_q) {
                            p = m_q.peek();
                        }
                        if (p != null) {
                            EncryptionResult er = p.getFirst();
                            SettableFuture<WriteResult> f = p.getSecond();
                            int bytesQueued = er.m_encCont.b().remaining();
                            boolean triedToDiscard = false;
                            try {
                                DBBPool.BBContainer writesCont = er.m_encCont;
                                int bytesWritten = m_channel.write(writesCont.b());
                                if (! writesCont.b().hasRemaining()) {
                                    synchronized (m_q) {
                                        m_q.poll();
                                    }
                                    triedToDiscard = true;
                                    er.m_encCont.discard();
                                    f.set(new WriteResult(bytesQueued, bytesWritten));
                                }
                            } catch (IOException e) {
                                if (!triedToDiscard) {
                                    er.m_encCont.discard();
                                }
                                f.setException(e);
                            }
                        }
                        synchronized (m_q) {
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
            return fut;
        }
    }

    public static class EncryptionResult {
        public final DBBPool.BBContainer m_encCont;
        public final int m_nBytesEncrypted;
        public EncryptionResult(DBBPool.BBContainer encCont, int nBytesEncrypted) {
            this.m_encCont = encCont;
            this.m_nBytesEncrypted = nBytesEncrypted;
        }
    }

    public static class WriteResult {
        public final int m_bytesQueued;
        public final int m_bytesWritten;
        public WriteResult(int bytesQueued, int bytesWritten) {
            this.m_bytesQueued = bytesQueued;
            this.m_bytesWritten = bytesWritten;
        }
    }
}
