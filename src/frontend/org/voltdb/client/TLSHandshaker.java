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

package org.voltdb.client;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLEngineResult.HandshakeStatus;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;

public class TLSHandshaker {
    // Netty uses 10 seconds most except client uses known number of connection hence not configuring per connection.
    // If somehealthcheck opens a connection and keeps it open beyond 30 seconds we will see a log and that will indicate
    // a DOS attack.
    public static final long DEFAULT_HANDSHAKE_TIMEOUT_MILLIS = Long.getLong("TLS_HANDSHAKE_TIMEOUT", 30000L);

    private final SSLEngine m_eng;
    private final SocketChannel m_sc;

    private ByteBuffer m_remnant;

    public TLSHandshaker(SocketChannel socketChan, SSLEngine engine) {
        m_sc = socketChan;
        m_eng = engine;
    }

    boolean canread(Selector selector) {

        Iterator<SelectionKey> keyIterator = selector.selectedKeys().iterator();
        boolean can = false;
        while(keyIterator.hasNext()) {
            SelectionKey key = keyIterator.next();
            if (key.isReadable()) {
                can = true;
            }
            keyIterator.remove();
        }
        return can;
    }

    public boolean handshake() throws IOException {
        SSLSession session = m_eng.getSession();
        ByteBuffer rxNetData = allocateBuffer(session.getPacketBufferSize());
        ByteBuffer txNetData = allocateBuffer(session.getPacketBufferSize());
        ByteBuffer clearData = allocateBuffer(session.getApplicationBufferSize());

        m_eng.beginHandshake();
        HandshakeStatus status = m_eng.getHandshakeStatus();

        boolean isBlocked;
        synchronized (m_sc.blockingLock()) {
            isBlocked = m_sc.isBlocking();
            if (isBlocked) {
                m_sc.configureBlocking(false);
            }
        }
        Selector selector = Selector.open();
        m_sc.register(selector, SelectionKey.OP_READ);

        SSLEngineResult result;
        try {
            long endTs = System.currentTimeMillis() + DEFAULT_HANDSHAKE_TIMEOUT_MILLIS;
            while (status != HandshakeStatus.FINISHED && status != HandshakeStatus.NOT_HANDSHAKING) {
                if (System.currentTimeMillis() > endTs) {
                    throw new IllegalStateException("TLS handshake timed out after: " + DEFAULT_HANDSHAKE_TIMEOUT_MILLIS + " Milliseconds");
                }
                boolean waitForData = true;
                switch(status) {
                case NEED_UNWRAP:
                    if (waitForData && selector.select(2) == 1 && canread(selector)) {
                        if (m_sc.read(rxNetData) < 0) {
                            if (m_eng.isInboundDone() && m_eng.isOutboundDone()) {
                                return false;
                            }
                            try {
                                m_eng.closeInbound();
                            } catch (SSLException ingnoreIt) {
                            }

                            m_eng.closeOutbound();
                            status = m_eng.getHandshakeStatus();
                            break;
                        }
                    }
                    rxNetData.flip();
                    try {
                        result = m_eng.unwrap(rxNetData, clearData);
                        rxNetData.compact();
                        waitForData = rxNetData.position() == 0;
                        status = result.getHandshakeStatus();
                    } catch (SSLException e) {
                        m_eng.closeOutbound();
                        throw e;
                    }
                    switch (result.getStatus()) {
                    case OK:
                        break;
                    case BUFFER_OVERFLOW:
                        clearData = allocateBuffer(m_eng.getSession().getApplicationBufferSize());
                        break;
                    case BUFFER_UNDERFLOW:
                        // During handshake, this indicates that there's not yet data to read.  We'll stay
                        // in this state until data shows up in m_rxNetData.
                        waitForData = true;
                       break;
                    case CLOSED:
                        if (m_eng.isOutboundDone()) {
                            return false;
                        } else {
                            m_eng.closeOutbound();
                            status = m_eng.getHandshakeStatus();
                        }
                        break;
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                    break;
                case NEED_WRAP:
                    txNetData.clear();
                    try {
                        result = m_eng.wrap(clearData, txNetData);
                        status = result.getHandshakeStatus();
                    } catch (SSLException e) {
                        m_eng.closeOutbound();
                        throw e;
                    }
                    switch(result.getStatus()) {
                    case OK:
                        txNetData.flip();
                        while (txNetData.hasRemaining()) {
                            m_sc.write(txNetData);
                        }
                        break;
                    case BUFFER_OVERFLOW:
                        txNetData = allocateBuffer(m_eng.getSession().getPacketBufferSize());
                        break;
                    case BUFFER_UNDERFLOW:
                        throw new SSLException("Buffer underflow occured after a wrap");
                    case CLOSED:
                        txNetData.flip();
                        while (txNetData.hasRemaining()) {
                            m_sc.write(txNetData);
                        }
                        rxNetData.clear();
                        status = m_eng.getHandshakeStatus();
                        break;
                    default:
                        throw new IllegalStateException("Invalid SSL status: " + result.getStatus());
                    }
                    break;
                case NEED_TASK:
                    Runnable task;
                    while ((task=m_eng.getDelegatedTask()) != null) {
                        task.run();
                    }
                    status = m_eng.getHandshakeStatus();
                    break;
                case FINISHED:
                    break;
                case NOT_HANDSHAKING:
                    break;
                default:
                    throw new IllegalStateException("Invalid SSL handshake status" + status);
                }
            }
        } finally {
            SelectionKey sk = m_sc.keyFor(selector);
            sk.cancel();
            selector.close();
            if (isBlocked) {
                synchronized (m_sc.blockingLock()) {
                    m_sc.configureBlocking(isBlocked);
                }
            }
        }

        // Flip the buffer so it is setup in case there is a remnant
        rxNetData.flip();

        while (rxNetData.hasRemaining()) {
            result = m_eng.unwrap(rxNetData, clearData);
            switch (result.getStatus()) {
                case OK:
                    break;
                case BUFFER_OVERFLOW:
                    clearData = expand(clearData, clearData.limit() << 1);
                    break;
                case BUFFER_UNDERFLOW:
                    // This really is not a bug it just means only part of a TLS frame was read but it is not handled
                    throw new IOException("buffer underflow while decrypting handshake remnant");
                case CLOSED:
                    throw new IOException("ssl engine closed while decrypting handshake remnant");
            }
        }

        clearData.flip();
        m_remnant = clearData.slice();

        return true;
    }

    /**
     * The JDK caches SSL sessions when the participants are the same (i.e.
     * multiple connection requests from the same peer). Once a session is cached
     * the client side ends its handshake session quickly, and is able to send
     * the login Volt message before the server finishes its handshake. This message
     * is caught in the servers last handshake network read. This method returns the
     * login message unencrypted
     *
     * @return potentially a byte buffer containing the client login message
     * @throws IOException if the decryption operation fails
     */
    public ByteBuffer getRemnant() throws IOException {
        return m_remnant.hasRemaining() ? m_remnant.asReadOnlyBuffer() : m_remnant;
    }

    public boolean hasRemnant() {
        return m_remnant.hasRemaining();
    }

    public SSLEngine getSslEngine() {
        return m_eng;
    }

    private static ByteBuffer allocateBuffer(int size) {
        return expand(null, size);
    }

    private static ByteBuffer expand(ByteBuffer bb, int newSize) {
        ByteBuffer expanded = ByteBuffer.allocateDirect(newSize);
        if (bb != null) {
            expanded.put((ByteBuffer) bb.flip());
        }
        return expanded;
    }
}
