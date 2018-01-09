/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */
package org.voltdb.regressionsuites;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.voltcore.network.CipherExecutor;
import org.voltcore.utils.ssl.SSLBufferDecrypter;
import org.voltcore.utils.ssl.SSLBufferEncrypter;
import org.voltcore.utils.ssl.SSLConfiguration;
import org.voltdb.Inits;
import org.voltdb.client.TLSHandshaker;

public class PortConnector {

    private SocketChannel m_socket;
    private String m_host;
    private int m_port;

    boolean m_topologyChangeAware = false;
    boolean m_enableSSL = false;
    SSLBufferEncrypter m_enc = null;
    SSLBufferDecrypter m_dec = null;
    int m_packetBufferSize;

    //For unit testing.
    private static final boolean ENABLE_SSL_FOR_TEST = Boolean.valueOf(System.getenv("ENABLE_SSL") == null ?
            Boolean.toString(Boolean.getBoolean("ENABLE_SSL")) : System.getenv("ENABLE_SSL"));
    private static final String DEFAULT_SSL_PROPS_FILE = "ssl-config";

    public PortConnector(String host, int port) {
        m_host = host;
        m_port = port;
        //For testing
        if (ENABLE_SSL_FOR_TEST) {
            m_enableSSL = true;
        }
    }

    public void connect() throws IOException {
        InetSocketAddress address = new InetSocketAddress(m_host, m_port);
        m_socket = SocketChannel.open(address);
        if (!m_socket.isConnected()) {
            throw new IOException("Failed to open host " + m_host);
        }
        //configure non blocking.
        m_socket.configureBlocking(false);
        m_socket.socket().setTcpNoDelay(true);
        if (m_enableSSL) {
            SSLConfiguration.SslConfig sslConfig;
            try (InputStream is = Inits.class.getResourceAsStream(DEFAULT_SSL_PROPS_FILE)) {
                Properties sslProperties = new Properties();
                sslProperties.load(is);
                sslConfig = new SSLConfiguration.SslConfig(
                        sslProperties.getProperty(SSLConfiguration.KEYSTORE_CONFIG_PROP),
                        sslProperties.getProperty(SSLConfiguration.KEYSTORE_PASSWORD_CONFIG_PROP),
                        sslProperties.getProperty(SSLConfiguration.TRUSTSTORE_CONFIG_PROP),
                        sslProperties.getProperty(SSLConfiguration.TRUSTSTORE_PASSWORD_CONFIG_PROP));

            } catch (IOException ioe) {
                throw new IllegalArgumentException("Unable to access SSL configuration.", ioe);
            }
            SSLContext sslContext;
            sslContext = SSLConfiguration.createSslContext(sslConfig);

            SSLEngine sslEngine = sslContext.createSSLEngine("client", m_port);
            sslEngine.setUseClientMode(true);
            TLSHandshaker handshaker = new TLSHandshaker(m_socket, sslEngine);
            boolean shookHands = false;
            try {
                shookHands = handshaker.handshake();
            } catch (IOException e) {
                throw new IOException("SSL handshake failed", e);
            }
            if (! shookHands) {
                throw new IOException("SSL handshake failed");
            }
            int appBufferSize = sslEngine.getSession().getApplicationBufferSize();
            m_packetBufferSize = sslEngine.getSession().getPacketBufferSize();
            m_enc = new SSLBufferEncrypter(sslEngine, appBufferSize, m_packetBufferSize);
            m_dec = new SSLBufferDecrypter(sslEngine);
            m_tlsFrame = null;
        }
    }

    public long write(ByteBuffer buf) throws IOException {
        if (m_enc != null) {
            ByteBuffer frame = (ByteBuffer)ByteBuffer.allocate(m_packetBufferSize).clear();
            m_enc.tlswrap(buf, frame);
            buf = frame;
        }
        long wrote = 0;
        while (buf.hasRemaining()) {
            wrote += m_socket.write(buf);
        }
        return wrote;
    }

    private ByteBuffer m_tlsFrame = null;

    ByteBuffer readTlsFrame() throws IOException {
        if (m_tlsFrame != null && m_tlsFrame.hasRemaining()) {
            return m_tlsFrame;
        }
        ByteBuffer header = (ByteBuffer)ByteBuffer.allocate(5).clear();
        while (header.hasRemaining()) {
            if (m_socket.read(header) < 0) {
                throw new IOException("Closed");
            };
        }
        ByteBuffer frame = (ByteBuffer)ByteBuffer.allocate(header.getShort(3) + header.capacity()).clear();
        frame.put((ByteBuffer)header.flip());
        while (frame.hasRemaining()) {
            if (m_socket.read(frame) < 0) {
                throw new IOException("Closed");
            };
        }
        int allocsz = Math.min(CipherExecutor.FRAME_SIZE, Integer.highestOneBit((frame.capacity()<<1) + 128));
        m_tlsFrame = (ByteBuffer)ByteBuffer.allocate(allocsz).clear();
        m_dec.tlsunwrap((ByteBuffer)frame.flip(), m_tlsFrame);
        return m_tlsFrame;
    }

    public void read(ByteBuffer buf, int sz) throws IOException {
        if (m_dec != null) {
            ByteBuffer clear = readTlsFrame();

            if (clear.remaining() < sz) {
                throw new IllegalStateException("failed to match unencrypted frame with expected read size");
            }
            int olim = clear.limit();
            clear.limit(clear.position()+sz);
            buf.put(clear);
            clear.limit(olim);
        } else {
            while (buf.hasRemaining() && sz > 0) {
                int r = m_socket.read(buf);
                if (r == -1) {
                    throw new IOException("Closed");
                }
                sz -= r;
            }
        }
    }

    public int close() {
        if (m_socket != null) {
            try {
                m_socket.close();
            } catch (IOException ex) {
                Logger.getLogger(PortConnector.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        m_enc = null;
        m_dec = null;
        m_tlsFrame = null;
        return 0;
    }
}
