/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class PortConnector {

    private SocketChannel m_socket;
    private String m_host;
    private int m_port;

    public PortConnector(String host, int port) {
        m_host = host;
        m_port = port;
    }

    public void connect() {
        try {
            InetSocketAddress address = new InetSocketAddress(m_host, m_port);
            m_socket = SocketChannel.open(address);
            if (!m_socket.isConnected()) {
                throw new IOException("Failed to open host " + m_host);
            }
            //configure non blocking.
            m_socket.configureBlocking(true);
            m_socket.socket().setTcpNoDelay(true);
        } catch (IOException ex) {
            Logger.getLogger(PortConnector.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    public long write(ByteBuffer buf) throws IOException {
        long wrote = 0;
        while (buf.hasRemaining()) {
            wrote += m_socket.write(buf);
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
    }

    public int close() {
        if (m_socket != null) {
            try {
                m_socket.close();
            } catch (IOException ex) {
                Logger.getLogger(PortConnector.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return 0;
    }
}
