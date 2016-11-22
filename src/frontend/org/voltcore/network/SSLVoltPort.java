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
import org.voltcore.utils.ssl.SSLEncryptionService;
import org.voltcore.utils.ssl.SSLMessageParser;

import javax.net.ssl.SSLEngine;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class SSLVoltPort extends VoltPort {

    private final SSLEngine m_sslEngine;
    private final SSLBufferDecrypter m_sslBufferDecrypter;
    private ByteBuffer m_dstBuffer;
    private final SSLMessageParser m_sslMessageParser;

    private ByteBuffer m_frameHeader;
    private final byte[] m_twoBytes = new byte[2];

    private int m_nextFrameLength = 0;

    private final Queue<Callable<List<ByteBuffer>>> m_callables = new ConcurrentLinkedQueue<>();

    public SSLVoltPort(VoltNetwork network, InputHandler handler, InetSocketAddress remoteAddress, NetworkDBBPool pool, SSLEngine sslEngine) {
        super(network, handler, remoteAddress, pool);
        this.m_sslEngine = sslEngine;
        m_sslBufferDecrypter = new SSLBufferDecrypter(sslEngine);
        m_sslMessageParser = new SSLMessageParser();
        m_frameHeader = ByteBuffer.allocate(5);

        // should be accessed only by the callable, not thread safe otherwise.
        this.m_dstBuffer = ByteBuffer.allocateDirect(1024 * 1024);
    }

    protected void setKey (SelectionKey key) {
        m_selectionKey = key;
        m_channel = (SocketChannel)key.channel();
        m_readStream = new NIOReadStream();
        m_writeStream = new SSLNIOWriteStream (
                this,
                m_handler.offBackPressure(),
                m_handler.onBackPressure(),
                m_handler.writestreamMonitor(),
                m_sslEngine);
        m_interestOps = key.interestOps();
    }

    public void run() throws IOException {
        try {
            if (readyForRead()) {
                final int maxRead = m_handler.getMaxRead();
                if (maxRead > 0) {
                    fillReadStream(maxRead);
                }

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
                        m_callables.add(new DecryptionCallable(frameCont));
                        m_nextFrameLength = 0;
                    } else {
                        break;
                    }
                }

                Callable c = m_callables.poll();
                if (c != null) {
                    Future<List<ByteBuffer>> f = SSLEncryptionService.instance().submit(c);
                    List<ByteBuffer> messages;
                    while (f != null) {
                        try {
                            messages = f.get();
                            c = m_callables.poll();
                            f = null;
                            if (c != null) {
                                f = SSLEncryptionService.instance().submit(c);
                            }
                            for (ByteBuffer message : messages) {
                                m_handler.handleMessage(message, this);
                                m_messagesRead++;
                            }
                        } catch (InterruptedException e) {
                            // process is exiting
                            return;
                        } catch (ExecutionException e) {
                            throw new IOException(e);
                        }
                    }
                }
            }

            // On readiness selection, optimistically assume that write will succeed,
            // in the common case it will
            drainWriteStream();
        } finally {
            synchronized (m_lock) {
                assert (m_running == true);
                m_running = false;
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
}
