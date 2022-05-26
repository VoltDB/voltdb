/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.io.EOFException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.net.ssl.SSLEngine;

import org.voltcore.utils.Pair;

public class TLSVoltPort extends VoltPort  {
    public final static int TLS_HEADER_SIZE = 5;

    private final TLSDecryptionAdapter m_tlsDecryptAdapter;
    private final SSLEngine m_sslEngine;
    private final CipherExecutor m_cipherExecutor;

    public TLSVoltPort(VoltNetwork network, InputHandler handler,
            InetSocketAddress remoteAddress, NetworkDBBPool pool,
            SSLEngine sslEngine, CipherExecutor cipherExecutor) {
        super(network, handler, remoteAddress, pool);
        m_sslEngine = sslEngine;
        m_cipherExecutor = cipherExecutor;
        m_tlsDecryptAdapter = new TLSDecryptionAdapter(this, handler, sslEngine, cipherExecutor);
    }

    @Override
    protected void setKey(SelectionKey key) {
        m_selectionKey = key;
        m_channel = (SocketChannel)key.channel();
        m_readStream = new NIOReadStream();
        m_writeStream = new VoltTLSNIOWriteStream(
                this,
                m_handler.offBackPressure(),
                m_handler.onBackPressure(),
                m_handler.writestreamMonitor(),
                m_sslEngine, m_cipherExecutor);
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
        ((VoltTLSNIOWriteStream)m_writeStream).waitForPendingEncrypts();
    }

    @Override
    public void run() throws IOException {
        try {
            do {
                try {
                    Pair<Integer, Integer> readInfo = m_tlsDecryptAdapter.handleInputStreamMessages(readyForRead(), readStream(), m_channel, m_pool);
                    m_messagesRead += readInfo.getSecond();
                } catch(EOFException e) {
                    handleReadStreamEOF();
                }

                if (m_network.isStopping() || m_isShuttingDown) {
                    m_tlsDecryptAdapter.waitForPendingDecrypts();
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
        m_writeStream.serializeQueuedWrites(m_pool /* unused and ignored */);
        if (m_network.isStopping()) {
            waitForPendingEncrypts();
        }
        synchronized (m_writeStream) {
            if (!m_writeStream.isEmpty()) {
                m_writeStream.drainTo(m_channel);
            }
            if (m_writeStream.isEmpty()) {
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
