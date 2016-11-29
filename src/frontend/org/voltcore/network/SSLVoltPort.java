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
import org.voltdb.common.Constants;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class SSLVoltPort extends VoltPort {

    private final SSLEngine m_sslEngine;
    private final SSLBufferDecrypter m_sslBufferDecrypter;
    private final SSLBufferEncrypter m_sslBufferEncrypter;
    private ByteBuffer m_dstBuffer;
    private final SSLMessageParser m_sslMessageParser;

    private ByteBuffer m_frameHeader;
    private final byte[] m_twoBytes = new byte[2];

    private int m_nextFrameLength = 0;

    private final Queue<Callable<List<ByteBuffer>>> m_readTasks = new ArrayDeque<>();
    private final Queue<Callable<EncryptionCallableResult>> m_writeTasks = new ArrayDeque<>();
    private List<DBBPool.BBContainer> m_remainingWrites;
    private int m_remainingWritesByteCount;

    public SSLVoltPort(VoltNetwork network, InputHandler handler, InetSocketAddress remoteAddress, NetworkDBBPool pool, SSLEngine sslEngine) {
        super(network, handler, remoteAddress, pool);
        this.m_sslEngine = sslEngine;
        m_sslBufferDecrypter = new SSLBufferDecrypter(sslEngine);
        this.m_sslBufferEncrypter = new SSLBufferEncrypter(sslEngine);
        m_sslMessageParser = new SSLMessageParser();
        m_frameHeader = ByteBuffer.allocate(5);

        // should be accessed only by the callable, not thread safe otherwise.
        this.m_dstBuffer = ByteBuffer.allocateDirect(1024 * 1024);
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
            if (m_remainingWrites != null) {
                m_remainingWrites = writeBuffers(m_remainingWrites);
                if (m_remainingWrites == null) {
                    m_writeStream.updateQueued(-m_remainingWritesByteCount, false);
                    // m_writeStream.backpressureEnded();
                }
            }

            // if the leftover bytes were written, add any new tasks to the the queue.
            if (m_remainingWrites == null) {
                    // m_writeStream.backpressureEnded();
                buildWriteTasks();
            }

            Callable<List<ByteBuffer>> readC = m_readTasks.poll();
            Callable<EncryptionCallableResult> writeC = m_writeTasks.poll();
            Future<List<ByteBuffer>> readF = null;
            Future<EncryptionCallableResult> writeF = null;

            while (readC != null || writeC != null) {
                if (readC != null && readF == null) {
                    readF = SSLEncryptionService.instance().submit(readC);
                }
                if (writeC != null && writeF == null) {
                    writeF = SSLEncryptionService.instance().submit(writeC);
                }

                if (writeF != null && writeF.isDone()) {
                    boolean writeCompleted = handleWrite(writeF);
                    writeF = null;
                    if (writeCompleted) {
                        writeC = m_writeTasks.poll();
                    }
                } else if (readF != null && readF.isDone()) {
                    handleRead(readF);
                    readF = null;
                    readC = m_readTasks.poll();
                } else if (readF != null) {
                    handleRead(readF);
                    readF = null;
                    readC = m_readTasks.poll();
                } else if (writeF != null) {
                    boolean writeCompleted = handleWrite(writeF);
                    writeF = null;
                    if (writeCompleted) {
                        writeC = m_writeTasks.poll();
                    }
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

    private void handleRead(Future<List<ByteBuffer>> readF) throws IOException {
        List<ByteBuffer> messages;
        try {
            messages = readF.get();
        } catch (InterruptedException e) {
            return;
        } catch (ExecutionException e) {
            throw new IOException(e);
        }
        for (ByteBuffer message : messages) {
            m_handler.handleMessage(message, this);
            m_messagesRead++;
        }
    }

    private boolean handleWrite(Future<EncryptionCallableResult> writeF) throws IOException {
        EncryptionCallableResult ecr;
        try {
            ecr = writeF.get();
        } catch (InterruptedException e) {
            return false;
        } catch (ExecutionException e) {
            throw new IOException(e);
        }

        List<DBBPool.BBContainer> remainingWrites = writeBuffers(ecr.m_encBuffers);
        if (remainingWrites != null) {
            m_remainingWrites = remainingWrites;
            m_remainingWritesByteCount = ecr.m_nBytesClear;
            return false;
        } else {
            m_writeStream.updateQueued(-ecr.m_nBytesClear, false);
            return true;
        }
    }

    private List<DBBPool.BBContainer> writeBuffers(List<DBBPool.BBContainer> buffersConts) throws IOException {
        int nWrote;
        while (true) {
            if (buffersConts.isEmpty()) {
                return null;
            }
            DBBPool.BBContainer bufCont = buffersConts.get(0);
            ByteBuffer buf = bufCont.b();
            nWrote = m_channel.write(buf);
            if (buf.hasRemaining()) {
                m_writeStream.checkBackpressureStarted();
                return buffersConts;
            } else {
                buffersConts.remove(0);
                bufCont.discard();
            }
        }
    }

    private void buildReadTasks() {
        int read;
        while (true) {
            if (m_nextFrameLength == 0) {
                if (readStream().dataAvailable() >= 5) {
                    read = readStream().getBytes(m_frameHeader);
                    m_frameHeader.flip();
                    m_frameHeader.position(3);
                    m_frameHeader.get(m_twoBytes);
                    m_nextFrameLength = ((m_twoBytes[0] & 0xff) << 8) | (m_twoBytes[1] & 0xff);
                } else {
                    break;
                }
            }

            if (readStream().dataAvailable() >= m_nextFrameLength) {
                DBBPool.BBContainer frameCont = DBBPool.allocateDirectAndPool(m_nextFrameLength + 5);
                ByteBuffer frame = frameCont.b();
                frame.limit(m_nextFrameLength + 5);
                m_frameHeader.flip();
                frame.put(m_frameHeader);
                m_frameHeader.clear();
                readStream().getBytes(frame);
                m_readTasks.add(new DecryptionCallable(frameCont));
                m_nextFrameLength = 0;
            } else {
                break;
            }
        }
    }

    private class DecryptionCallable implements Callable<List<ByteBuffer>> {
        private final DBBPool.BBContainer m_srcCont;

        public DecryptionCallable(DBBPool.BBContainer srcCont) {
            this.m_srcCont = srcCont;
        }

        @Override
        public List<ByteBuffer> call() throws IOException {
            List<ByteBuffer> messages = new ArrayList<>();
            ByteBuffer srcBuffer = m_srcCont.b();
            if (srcBuffer.position() > 0) {
                srcBuffer.flip();
            } else {
                // won't be able to decrypt is the src buffer is empty.
                return messages;
            }
            m_dstBuffer.limit(m_dstBuffer.capacity());
            ByteBuffer dstBuffer = m_sslBufferDecrypter.unwrap(srcBuffer, m_dstBuffer);
            m_srcCont.discard();

            if (m_dstBuffer.hasRemaining()) {
                ByteBuffer message;
                while ((message = m_sslMessageParser.message(m_dstBuffer)) != null) {
                    messages.add(message);
                }
            }

            return messages;
        }
    }

    protected int getMessageLengthFromHeader(ByteBuffer header) {
        header.position(3);
        header.get(m_twoBytes);
        return ((m_twoBytes[0] & 0xff) << 8) | (m_twoBytes[1] & 0xff);
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
        DBBPool.BBContainer outCont = m_pool.acquire();
        ByteBuffer outbuf = outCont.b();
        outbuf.clear();
        while ((ds = oldlist.poll()) != null) {
            final int serializedSize = ds.getSerializedSize();
            if (serializedSize == DeferredSerialization.EMPTY_MESSAGE_LENGTH) continue;
            //Fastpath, serialize to direct buffer creating no garbage
            if (outbuf.remaining() >= serializedSize) {
                final int oldLimit = outbuf.limit();
                outbuf.limit(outbuf.position() + serializedSize);
                final ByteBuffer slice = outbuf.slice();
                ds.serialize(slice);
                m_writeStream.checkSloppySerialization(slice, ds);
                slice.position(0);
                bytesQueued += slice.remaining();
                outbuf.position(outbuf.limit());
                outbuf.limit(oldLimit);
            } else {
                //Slow path serialize to heap, and then put in buffers
                ByteBuffer buf = ByteBuffer.allocate(serializedSize);
                ds.serialize(buf);
                m_writeStream.checkSloppySerialization(buf, ds);
                buf.position(0);
                bytesQueued += buf.remaining();
                while (buf.hasRemaining()) {
                    if (!outbuf.hasRemaining()) {
                        outbuf.flip();
                        m_writeTasks.add(new EncryptionCallable(m_sslBufferEncrypter, outCont));
                        outCont = m_pool.acquire();
                        outbuf = outCont.b();
                        outbuf.clear();
                    }
                    if (outbuf.remaining() >= buf.remaining()) {
                        outbuf.put(buf);
                    } else {
                        final int oldLimit = buf.limit();
                        buf.limit(buf.position() + outbuf.remaining());
                        outbuf.put(buf);
                        buf.limit(oldLimit);
                    }
                }
            }
        }
        if (outbuf.position() > 0) {
            outbuf.flip();
            m_writeTasks.add(new EncryptionCallable(m_sslBufferEncrypter, outCont));
        }
        m_writeStream.updateQueued(bytesQueued, true);
    }

    public static class EncryptionCallable implements Callable<EncryptionCallableResult> {

        private final SSLBufferEncrypter m_sslBufferEncrypter;
        private final DBBPool.BBContainer m_clearCont;
        private final int m_nBytesClear;

        public EncryptionCallable(SSLBufferEncrypter sslBufferEncrypter, DBBPool.BBContainer clearCont) {
            this.m_sslBufferEncrypter = sslBufferEncrypter;
            this.m_clearCont = clearCont;
            this.m_nBytesClear = m_clearCont.b().remaining();
        }

        @Override
        public EncryptionCallableResult call() throws Exception {
            ByteBuffer clearBuf = m_clearCont.b();
            // division with rounding up
            int nChunks = (clearBuf.remaining() + Constants.SSL_CHUNK_SIZE - 1) / Constants.SSL_CHUNK_SIZE;
            List<DBBPool.BBContainer> encChunks = new ArrayList<>(nChunks);
            while (clearBuf.hasRemaining()) {
                if (clearBuf.remaining() <= Constants.SSL_CHUNK_SIZE) {
                    encChunks.add(m_sslBufferEncrypter.encryptBuffer(clearBuf.slice()));
                    clearBuf.position(clearBuf.limit());
                } else {
                    int oldLimit = clearBuf.limit();
                    clearBuf.limit(clearBuf.position() + Constants.SSL_CHUNK_SIZE);
                    encChunks.add(m_sslBufferEncrypter.encryptBuffer(clearBuf.slice()));
                    clearBuf.position(clearBuf.limit());
                    clearBuf.limit(oldLimit);
                }
            }
            m_clearCont.discard();
            return new EncryptionCallableResult(encChunks, m_nBytesClear);
        }

    }

    public static class EncryptionCallableResult {
        public final List<DBBPool.BBContainer> m_encBuffers;
        public final int m_nBytesClear;
        public EncryptionCallableResult(List<DBBPool.BBContainer> encBuffers, int nClearBytes) {
            this.m_encBuffers = encBuffers;
            this.m_nBytesClear = nClearBytes;
        }
    }
}
