/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
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
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;

import io.netty_voltpatches.buffer.ByteBuf;
import io.netty_voltpatches.buffer.Unpooled;

public class TLSVoltPort extends VoltPort  {
    public final static int TLS_HEADER_SIZE = 5;

    private final TLSDecryptionAdapter m_tlsDecryptAdapter;

    public TLSVoltPort(VoltNetwork network, InputHandler handler,
            InetSocketAddress remoteAddress, NetworkDBBPool pool,
            SSLEngine sslEngine, CipherExecutor cipherExecutor) {
        super(network, handler, remoteAddress, pool);
        m_tlsDecryptAdapter = new TLSDecryptionAdapter(this, handler, sslEngine, cipherExecutor);
    }

    @Override
    protected void setKey(SelectionKey key) {
        m_selectionKey = key;
        m_channel = (SocketChannel)key.channel();
        m_readStream = new NIOReadStream();
        m_writeStream = m_tlsDecryptAdapter.getWriteStream(key);
        m_interestOps = key.interestOps();
    }

    @Override
    void die() {
        super.die();
        m_tlsDecryptAdapter.die();
    }

    String dumpState() {
        return new StringBuilder(256).append("TLSVoltPort[")
                .append("availableBytes=").append(readStream().dataAvailable())
                .append(", tlsAdapter=").append(m_tlsDecryptAdapter.dumpState())
                .append("]").toString();
    }

    private void waitForPendingEncrypts() throws IOException {
        ((TLSNIOWriteStream)m_writeStream).waitForPendingEncrypts();
    }

    private final static int MAX_READ = CipherExecutor.FRAME_SIZE << 1; //32 KB
    private final static int NOT_AVAILABLE = -1;

    private int m_needed = NOT_AVAILABLE;

    private final int getMaxRead() {
        return m_handler.getMaxRead() == 0 ? 0 // in back pressure
                : m_needed == NOT_AVAILABLE ? MAX_READ :
                    readStream().dataAvailable() > m_needed ? 0 : m_needed - readStream().dataAvailable();
    }

    @Override
    public void run() throws IOException {
        try {
            do {
                m_tlsDecryptAdapter.checkForGatewayExceptions();
                /*
                 * Have the read stream fill from the network
                 */
                if (readyForRead()) {
                    final int maxRead = getMaxRead();
                    if (maxRead > 0) {
                        int read = fillReadStream(maxRead);
                        if (read > 0) {
                            ByteBuf frameHeader = Unpooled.wrappedBuffer(new byte[TLS_HEADER_SIZE]);
                            while (readStream().dataAvailable() >= TLS_HEADER_SIZE) {
                                NIOReadStream rdstrm = readStream();
                                rdstrm.peekBytes(frameHeader.array());
                                m_needed = frameHeader.getShort(3) + TLS_HEADER_SIZE;
                                if (rdstrm.dataAvailable() < m_needed) break;
                                m_tlsDecryptAdapter.offerForDecryption(rdstrm.getSlice(m_needed));
                                m_needed = NOT_AVAILABLE;
                            }
                        }
                    }
                }

                if (m_network.isStopping() || m_isShuttingDown) {
                    m_tlsDecryptAdapter.waitForPendingDecrypts();
                }

                ByteBuffer message = null;
                while ((message = m_tlsDecryptAdapter.pollDecryptedQueue()) != null) {
                    ++m_messagesRead;
                    m_handler.handleMessage(message, this);
                }

                /*
                 * On readiness selection, optimistically assume that write will succeed,
                 * in the common case it will
                 */
                drainEncryptedStream();
                /*
                 * some encrypt or decrypt task may have finished while this port is running
                 * so enabling write interest would have been muted. Signal is there to
                 * reconsider finished decrypt or encrypt tasks.
                 */
            } while (m_signal.compareAndSet(true, false));
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
    /**
     * if this port is running calls to enableWriteSelection are
     * ignored by the super class. This signaling artifacts tells
     * this port run() method to keep polling for finished encrypt
     * and decrypt taks
     */
    private final AtomicBoolean m_signal = new AtomicBoolean(false);

    @Override
    public void enableWriteSelection() {
        m_signal.set(true);
        super.enableWriteSelection();
    }

    @Override
    void unregistered() {
        try {
            m_tlsDecryptAdapter.waitForPendingDecrypts();
        } catch (IOException e) {
            networkLog.warn("unregistered port had an decryption task drain fault", e);
        }
        try {
            waitForPendingEncrypts();
        } catch (IOException e) {
            networkLog.warn("unregistered port had an encryption task drain fault", e);
        }
        m_tlsDecryptAdapter.releaseDecryptedBuffer();
        super.unregistered();
    }
}
