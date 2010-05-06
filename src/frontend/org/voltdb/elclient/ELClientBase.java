/* This file is part of VoltDB.
 * Copyright (C) 2008-2010 VoltDB L.L.C.
 *
 * VoltDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * VoltDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.elclient;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.nio.channels.SocketChannel;

import org.voltdb.elt.processors.RawProcessor;

/**
 * Provides an extensible base class for writing ELT clients
 */

public abstract class ELClientBase implements Runnable {

    private final int m_processorPort;
    private SocketChannel m_socket;
    private ELConnection m_elConnection;

    public ELClientBase() {
        m_processorPort = RawProcessor.LISTENER_PORT;
    }

    /**
     * Allow derived clients to implement their own construction of handlers
     * for the data sources provided by the server on this EL connection
     * @param elConnection
     */
    public abstract void constructELTDataSinks(ELConnection elConnection);

    /**
     * Open the socket to the Volt server's EL port, retrieve the data source
     * info, and create data sinks as appropriate
     */
    public void connectToELServer()
    {
        System.out.println("Starting EL Client socket.");
        try {
            SocketAddress sockaddr =
                new InetSocketAddress(InetAddress.getLocalHost(),
                                      m_processorPort);
            m_socket = SocketChannel.open(sockaddr);
            m_socket.configureBlocking(false);
            m_elConnection = new ELConnection(m_socket);
            m_elConnection.openELTConnection();
            constructELTDataSinks(m_elConnection);
        }
        catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /**
     * Perform one iteration of EL Client work.
     *  Override if the specific client has strange workflow/termination conditions.
     * Largely for EL clients used for test.
     */
    public void work()
    {
        m_elConnection.work();
    }

    @Override
    public void run() {
        connectToELServer();
        work();
    }
}
