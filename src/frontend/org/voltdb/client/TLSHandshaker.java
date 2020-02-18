/* This file is part of VoltDB.
 * Copyright (C) 2008-2020 VoltDB Inc.
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

import org.voltcore.network.CipherExecutor;

public class TLSHandshaker {
    private ByteBuffer m_rxNetData;
    private final SSLEngine m_eng;
    private final SocketChannel m_sc;
    private final int m_appsz;

    public TLSHandshaker(SocketChannel socketChan, SSLEngine engine) {
        m_sc = socketChan;
        m_eng = engine;
        SSLSession dummySession = engine.getSession();

        final int appsz = engine.getSession().getApplicationBufferSize() + 2048;
        if (Integer.highestOneBit(appsz) < appsz) {
            m_appsz = Integer.highestOneBit(appsz) << 1;
        } else {
            m_appsz = appsz;
        }
        m_rxNetData = (ByteBuffer)ByteBuffer.allocate(dummySession.getPacketBufferSize()).clear();
        dummySession.invalidate();
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
        ByteBuffer txNetData = (ByteBuffer)ByteBuffer.allocate(m_appsz).clear();
        ByteBuffer clearData = (ByteBuffer)ByteBuffer.allocate(CipherExecutor.FRAME_SIZE).clear();

        SSLEngineResult result = null;
        m_eng.beginHandshake();
        HandshakeStatus status = m_eng.getHandshakeStatus();
        boolean isBlocked = m_sc.isBlocking();

        synchronized (m_sc.blockingLock()) {
            isBlocked = m_sc.isBlocking();
            if (isBlocked) {
                m_sc.configureBlocking(false);
            }
        }
        Selector selector = Selector.open();
        m_sc.register(selector, SelectionKey.OP_READ);

        try {
            while (status != HandshakeStatus.FINISHED && status != HandshakeStatus.NOT_HANDSHAKING) {
                boolean waitForData = true;
                switch(status) {
                case NEED_UNWRAP:
                    if (waitForData && selector.select(2) == 1 && canread(selector)) {
                        if (m_sc.read(m_rxNetData) < 0) {
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
                    m_rxNetData.flip();
                    try {
                        result = m_eng.unwrap(m_rxNetData, clearData);
                        m_rxNetData.compact();
                        waitForData = m_rxNetData.position() == 0;
                        status = result.getHandshakeStatus();
                    } catch (SSLException e) {
                        m_eng.closeOutbound();
                        throw e;
                    }
                    switch (result.getStatus()) {
                    case OK:
                        break;
                    case BUFFER_OVERFLOW:
                        clearData = expand(clearData, false);
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
                        txNetData = expand(txNetData, false);
                        break;
                    case BUFFER_UNDERFLOW:
                        throw new SSLException("Buffer underflow occured after a wrap");
                    case CLOSED:
                        txNetData.flip();
                        while (txNetData.hasRemaining()) {
                            m_sc.write(txNetData);
                        }
                        m_rxNetData.clear();
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
        m_rxNetData.flip();

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
        if (!m_rxNetData.hasRemaining()) {
            return m_rxNetData.slice();
        }
        ByteBuffer remnant = ByteBuffer.allocate(m_eng.getSession().getApplicationBufferSize());
        SSLEngineResult result = m_eng.unwrap(m_rxNetData, remnant);
        switch (result.getStatus()) {
        case OK:
            assert !m_rxNetData.hasRemaining() : "there are unexpected additional remnants";
            return ((ByteBuffer)remnant.flip()).slice().asReadOnlyBuffer();
        case BUFFER_OVERFLOW:
            throw new IOException("buffer underflow while decrypting handshake remnant");
        case BUFFER_UNDERFLOW:
            throw new IOException("buffer overflow while decrypting handshake remnant");
        case CLOSED:
            throw new IOException("ssl engine closed while decrypting handshake remnant");
        }
        return null; // unreachable
    }

    public boolean hasRemnant() {
        return m_rxNetData.hasRemaining();
    }

    public SSLEngine getSslEngine() {
        return m_eng;
    }

    private static ByteBuffer expand(ByteBuffer bb, boolean copy) {
        ByteBuffer expanded = (ByteBuffer)ByteBuffer.allocate(bb.capacity() <<1).clear();
        if (copy) {
            expanded.put((ByteBuffer)bb.flip());
        }
        return expanded;
    }

}
