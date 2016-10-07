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

package org.voltdb.client;

import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLEngineResult;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

/**
 * Code to perform an SSL handshake.  Will be used by both client and server code.
 */
public class SSLHandshaker {
    private final SocketChannel m_sc;
    private final SSLEngine m_sslEngine;

    private ByteBuffer m_localClearData;
    private ByteBuffer m_localEncData;
    private ByteBuffer m_netClearData;
    private ByteBuffer m_netEncData;

    public SSLHandshaker(SocketChannel sc, SSLEngine engine) {
        m_sc = sc;
        m_sslEngine = engine;
        SSLSession dummySession = engine.getSession();
        m_localClearData = ByteBuffer.allocate(dummySession.getApplicationBufferSize() + 50);
        m_localEncData = ByteBuffer.allocate(dummySession.getPacketBufferSize());
        m_netClearData = ByteBuffer.allocate(dummySession.getApplicationBufferSize() + 50);
        m_netEncData = ByteBuffer.allocate(dummySession.getPacketBufferSize());
        dummySession.invalidate();
    }

    public boolean handshake() throws IOException {

        m_sslEngine.beginHandshake();

        SSLEngineResult.HandshakeStatus hs = m_sslEngine.getHandshakeStatus();
        while (hs != null && hs != SSLEngineResult.HandshakeStatus.FINISHED && hs != SSLEngineResult.HandshakeStatus.NOT_HANDSHAKING) {
            switch (hs) {
                case NEED_UNWRAP:
                    hs = unwrap(m_sc, m_sslEngine);
                    break;
                case NEED_WRAP:
                    hs = wrap();
                    break;
                case NEED_TASK:
                    Runnable runnable;
                    while ((runnable = m_sslEngine.getDelegatedTask()) != null) {
                        runnable.run();
                    }
                    hs = m_sslEngine.getHandshakeStatus();
                    break;
                case FINISHED:
                    break;
                case NOT_HANDSHAKING:
                    break;
            }
        }
        return hs != null;
    }

    private SSLEngineResult.HandshakeStatus wrap() throws IOException {
        SSLEngineResult res;
        SSLEngineResult.HandshakeStatus hs;
        SSLEngineResult.Status status;
        try {
            res = m_sslEngine.wrap(m_localClearData, m_localEncData);
            hs = res.getHandshakeStatus();
        } catch (SSLException e) {
            System.err.println("wrap: see SSLException");
            m_sslEngine.closeOutbound();
            return m_sslEngine.getHandshakeStatus();
        }
        switch (res.getStatus()) {
            case OK:
                m_localEncData.flip();
                while (m_localEncData.hasRemaining()) {
                    m_sc.write(m_localEncData);
                }
                m_localEncData.compact();
                break;
            case BUFFER_OVERFLOW:
                m_localEncData = ByteBuffer.allocate(m_localEncData.capacity() * 2);
                break;
            case BUFFER_UNDERFLOW:
                // There's nothing in m_localClearData.  This is a bug...
                throw new IOException("SSLEngine: nothing in m_localClearData when wrapping");
            case CLOSED:
                // write out any remaining data...
                m_localEncData.flip();
                try {
                    while (m_localEncData.hasRemaining()) {
                        m_sc.write(m_localEncData);
                    }
                } catch (IOException e) {
                    // can safely eat/ignore this.
                }
                hs = m_sslEngine.getHandshakeStatus();
                break;
        }
        return hs;
    }

    private SSLEngineResult.HandshakeStatus unwrap(SocketChannel socketChannel, SSLEngine engine) throws IOException {

        // handle loss of connection
        if (socketChannel.read(m_netEncData) < 0) {

            // if the ssl engine is already closed, just return null, handshaking has failed.
            if (engine.isInboundDone() && engine.isOutboundDone()) {
                return null;
            }
            // otherwise, close the engine.
            try {
                engine.closeInbound();
            } catch (SSLException e) {
                // want to eat this as we're just closing the engine.
            }
            engine.closeOutbound();
            return engine.getHandshakeStatus();
        }

        // the typical unwrap logic.
        m_netEncData.flip();

        SSLEngineResult.HandshakeStatus hs;
        SSLEngineResult res;
        try {
            res = engine.unwrap(m_netEncData, m_netClearData);
            m_netEncData.compact();
            hs = res.getHandshakeStatus();
        } catch (SSLException e) {
            System.err.println("unwrap: see ssl exception " + e.getMessage());
            engine.closeOutbound();
            return engine.getHandshakeStatus();
        }

        switch (res.getStatus()) {
            case OK:
                break;
            case BUFFER_OVERFLOW:
                m_netClearData = ByteBuffer.allocate(m_netClearData.capacity() * 2);;
                break;
            case BUFFER_UNDERFLOW:
                // During handshake, this indicates that there's not yet data to read.  We'll stay
                // in this state until data shows up in m_netEncData.
                break;
            case CLOSED:
                if (engine.isOutboundDone()) {
                    return null;
                } else {
                    engine.closeOutbound();
                    hs = engine.getHandshakeStatus();
                }
                break;
        }
        return hs;
    }
}
