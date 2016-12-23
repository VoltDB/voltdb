/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.voltcore.utils.DBBPool;
import org.voltcore.utils.ssl.SSLBufferDecrypter;
import org.voltcore.utils.ssl.SSLBufferEncrypter;
import org.voltcore.utils.ssl.SSLConfiguration;
import org.voltdb.Inits;
import org.voltdb.client.SSLHandshaker;

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
    private static final boolean ENABLE_SSL_FOR_TEST = Boolean.valueOf(System.getenv("ENABLE_SSL") == null ? Boolean.toString(Boolean.getBoolean("ENABLE_SSL")) : System.getenv("ENABLE_SSL"));
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
        m_socket.configureBlocking(true);
        m_socket.socket().setTcpNoDelay(true);
        if (m_enableSSL) {
            SSLConfiguration.SslConfig sslConfig;
            String sslPropsFile = Inits.class.getResource(DEFAULT_SSL_PROPS_FILE).getFile();

            if ((sslPropsFile == null || sslPropsFile.trim().length() == 0) ) {
                sslConfig = new SSLConfiguration.SslConfig(null, null, null, null);
                SSLConfiguration.applySystemProperties(sslConfig);
            } else {
                File configFile = new File(sslPropsFile);
                Properties sslProperties = new Properties();
                try ( FileInputStream configFis = new FileInputStream(configFile) ) {
                    sslProperties.load(configFis);
                    sslConfig = new SSLConfiguration.SslConfig(
                            sslProperties.getProperty(SSLConfiguration.KEYSTORE_CONFIG_PROP),
                            sslProperties.getProperty(SSLConfiguration.KEYSTORE_PASSWORD_CONFIG_PROP),
                            sslProperties.getProperty(SSLConfiguration.TRUSTSTORE_CONFIG_PROP),
                            sslProperties.getProperty(SSLConfiguration.TRUSTSTORE_PASSWORD_CONFIG_PROP));
                    SSLConfiguration.applySystemProperties(sslConfig);
                } catch (IOException ioe) {
                    throw new IllegalArgumentException("Unable to access SSL configuration.", ioe);
                }
            }
            SSLContext sslContext;
            try {
                sslContext = SSLConfiguration.initializeSslContext(sslConfig);
            } catch (NoSuchAlgorithmException | KeyStoreException | IOException | CertificateException | UnrecoverableKeyException | KeyManagementException ex) {
                throw new IllegalArgumentException(ex.getMessage());
            }
            SSLEngine sslEngine = sslContext.createSSLEngine("client", m_port);
            sslEngine.setUseClientMode(true);
            SSLHandshaker handshaker = new SSLHandshaker(m_socket, sslEngine);
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
        }
    }

    public long write(ByteBuffer buf) throws IOException {
        DBBPool.BBContainer bb = null;
        if (m_enc != null) {
            bb = m_enc.encryptBuffer(buf);
            buf = bb.b();
        }
        long wrote = 0;
        while (buf.hasRemaining()) {
            wrote += m_socket.write(buf);
        }
        if (bb != null) {
            bb.discard();
        }
        return wrote;
    }

    public void read(ByteBuffer buf, long sz) throws IOException {

        while (buf.hasRemaining() && sz > 0) {
            int r = m_socket.read(buf);
            if (r == -1) {
                throw new IOException("Closed");
            }
            sz -= r;
        }
        if (m_dec != null) {
            DBBPool.BBContainer m_dstBufferCont = DBBPool.allocateDirect(m_packetBufferSize);
            ByteBuffer m_dstBuffer = m_dstBufferCont.b();
            m_dec.unwrap(buf, m_dstBuffer);
            buf.limit(m_dstBuffer.remaining());
            buf.put(m_dstBuffer);
            buf.flip();
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
        return 0;
    }
}
