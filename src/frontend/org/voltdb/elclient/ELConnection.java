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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

import org.voltdb.elt.ELTProtoMessage;
import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;
import org.voltdb.messaging.FastDeserializer;

/**
 * Manage the connection to the server's EL port
 */

public class ELConnection implements Runnable {

    static final private int CLOSED = 0;
    static final private int CONNECTING = 1;
    static final private int CONNECTED = 2;
    static final private int CLOSING = 3;
    private int m_state = CLOSED;
    private String m_connectionName;

    private InetSocketAddress m_serverAddr;
    private SocketChannel m_socket;

    // cached reference to ELClientBase's collection of ELDataSinks
    private HashMap<Integer, HashMap<Integer, ELDataSink>> m_sinks;

    private ArrayList<AdvertisedDataSource> m_dataSources;

    public ELConnection(InetSocketAddress serverAddr,
                        HashMap<Integer, HashMap<Integer, ELDataSink>> dataSinks)
    {
        m_sinks = dataSinks;
        m_serverAddr = serverAddr;
        m_connectionName = serverAddr.toString();
    }

    /**
     * Open the ELT connection to the processor on m_socket.  This will
     * currently block until it receives the OPEN RESPONSE from the server
     * that contains the information about the available data sources
     */
    public void openELTConnection() throws IOException
    {
        System.out.println("Starting EL Client socket to: " + m_connectionName);
        try {
            m_socket = SocketChannel.open(m_serverAddr);
            m_socket.configureBlocking(false);
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        if (m_state == CLOSED)
        {
            open();
            m_state = CONNECTING;
        }

        while (m_state == CONNECTING)
        {
            ELTProtoMessage m = nextMessage();
            if (m != null && m.isOpenResponse())
            {
                m_dataSources = m.getAdvertisedDataSources();
                m_state = CONNECTED;
            }
        }
    }

    /**
     * Retrieve the list of data sources returned by the
     * open response provided by the server
     * @return
     */
    public ArrayList<AdvertisedDataSource> getDataSources()
    {
        return m_dataSources;
    }

    /**
     * Retrieve the name of this connection.  This is currently
     * equivalent to InetSocketAddress.toString()
     * @return
     */
    public String getConnectionName()
    {
        return m_connectionName;
    }

    /**
     * Retrieve the connected-ness of this EL Connection
     * @return
     */
    public boolean isConnected()
    {
        return (m_state == CONNECTED);
    }

    /**
     * perform a single iteration of work for the EL connection.
     */
    public void work()
    {
        // eltxxx need better error handling code in here
        if (!m_socket.isConnected() || !m_socket.isOpen())
        {
            m_state = CLOSING;
        }
        // loop here to empty RX ?
        // receive data from network and hand to the proper ELProtocolHandler RX queue
        ELTProtoMessage m = null;
        do
        {
            try {
                m = nextMessage();
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            if (m != null &&m.isError())
            {
                // XXX handle error from server, just die for now
                m_state = CLOSING;
            }

            if (m != null && m.isPollResponse())
            {
                ELDataSink rx_sink = m_sinks.get(m.getTableId()).get(m.getPartitionId());
                rx_sink.getRxQueue(m_connectionName).offer(m);
            }
        }
        while (m != null);

        // service all the ELDataSink TX queues
        for (HashMap<Integer, ELDataSink> part_map : m_sinks.values())
        {
            for (ELDataSink tx_sink : part_map.values())
            {
                Queue<ELTProtoMessage> tx_queue =
                    tx_sink.getTxQueue(m_connectionName);
                // this connection might not be connected to every sink
                if (tx_queue != null)
                {
                    // XXX loop to drain the tx queue?
                    ELTProtoMessage tx_m =
                        tx_queue.poll();
                    if (tx_m != null)
                    {
                        try {
                            sendMessage(tx_m);
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                }
            }
        }
    }

    @Override
    public void run()
    {
        // XXX termination?
        while (m_state == CONNECTED)
        {
            work();
        }
    }

    private void open() throws IOException
    {
        System.out.println("Opening new EL stream connection.");
        ELTProtoMessage m = new ELTProtoMessage(-1, -1);
        m.open();
        sendMessage(m);
    }

    public ELTProtoMessage nextMessage() throws IOException
    {
        FastDeserializer fds;
        final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        int bytes_read = 0;
        do
        {
            bytes_read = m_socket.read(lengthBuffer);
        }
        while (lengthBuffer.hasRemaining() && bytes_read != 0);

        if (bytes_read == 0)
        {
            if (lengthBuffer.position() != 0)
            {
                // XXX don't deal with partial reads for now
                System.out.println("Can't handle partial length read: " +
                                   lengthBuffer.position());
                System.exit(1);
            }
            return null;
        }

        lengthBuffer.flip();
        int length = lengthBuffer.getInt();
        ByteBuffer messageBuf = ByteBuffer.allocate(length);
        do
        {
            m_socket.read(messageBuf);
        }
        while (messageBuf.remaining() > 0);
        messageBuf.flip();
        fds = new FastDeserializer(messageBuf);
        ELTProtoMessage m = ELTProtoMessage.readExternal(fds);
        return m;
    }

    public void sendMessage(ELTProtoMessage m) throws IOException
    {
        ByteBuffer buf = m.toBuffer();
        while (buf.remaining() > 0) {
            m_socket.write(buf);
        }
    }
}
