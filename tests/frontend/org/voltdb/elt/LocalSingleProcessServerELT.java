/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
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

package org.voltdb.elt;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;
import java.util.List;

import org.voltdb.BackendTarget;
import org.voltdb.regressionsuites.LocalSingleProcessServer;

/**
 * Adds an ELT sink server to the LocalSingleProcessServer.
 * There are two components: the sink server thread, managed
 * and created by this code; and the TupleVerifier that the
 * sink server uses for verification. The verifier should be
 * configured by each test case using the setTupleVerifier()
 * method on this class.
 */
public class LocalSingleProcessServerELT extends LocalSingleProcessServer {
    ELSinkServer m_sinkServer;
    Class<? extends TupleVerifier> m_verifierClass;
    TupleVerifier m_verifierInstance;

    public LocalSingleProcessServerELT(final String jarFileName, final int siteCount,
        final BackendTarget target, Class<? extends TupleVerifier> verifier) {
        super(jarFileName, siteCount, target);

        // A new verifier instance is constructed for each startUp()
        m_verifierClass = verifier;
        m_verifierInstance = null;

        // The sink server thread is created and started and then destroyed
        // in the suite startUp() and shutDown() methods.
        m_sinkServer = null;
    }


    /**
     * Proxy this TupleVerifier interface. The test function itself doesn't
     * have a reference to m_verifierInstance.
     */
    public boolean allRowsVerified() {
        return m_verifierInstance.allRowsVerified();
    }

    /**
     * Proxy this TupleVerifier interface. The test function itself doesn't
     * have a reference to m_verifierInstance.
     */
    public void addRow(String tableName, Object[] data) {
        m_verifierInstance.addRow(tableName, data);
    }

    // override startup to control the sink server
    @Override
    public void startUp() {
        System.out.println("Starting Sink Server.");
        try {
            m_verifierInstance = m_verifierClass.newInstance();
            m_sinkServer = new ELSinkServer(m_verifierInstance);
        }
        catch (InstantiationException e2) {
            throw new RuntimeException(e2);
        }
        catch (IllegalAccessException e2) {
            throw new RuntimeException(e2);
        }
        m_sinkServer.start();
        // spin on trying to connect.. otherwise the server
        // and the test race against the sink server thread's
        // initialization
        do {
            SocketAddress sockaddr;
            try {
                sockaddr = new InetSocketAddress(InetAddress.getLocalHost(), ELSinkServer.PORTNUMBER);
                final SocketChannel socket = SocketChannel.open(sockaddr);
                socket.close();
                System.out.println("Connection tester connected to SinkServer!");
                break; // get here if a connection was made.
            }
            catch (final UnknownHostException e1) {
                break;
            }
            catch (final IOException e) {
                System.out.println("Waiting for ELSinkServer to start!");
                // spin on failure.. that's the point
            }
        } while (true);
        System.out.println("Starting Everything else.");
        super.startUp();
    }

    // override shutdown to control the el sink server.
    @Override
    public List<String> shutDown() throws InterruptedException {
        final List<String> results = super.shutDown();
        m_sinkServer.shutDown();
        m_sinkServer.join();
        m_sinkServer = null;
        return results;
    }

    public void flushSinkServer() {
        m_sinkServer.finish();
    }
}
