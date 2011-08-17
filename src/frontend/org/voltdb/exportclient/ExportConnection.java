/* This file is part of VoltDB.
 * Copyright (C) 2008-2011 VoltDB Inc.
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

package org.voltdb.exportclient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Queue;

import org.voltdb.client.ConnectionUtil;
import org.voltdb.export.ExportProtoMessage;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.logging.VoltLogger;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.utils.BandwidthMonitor;
import org.voltdb.utils.Pair;

/**
 * Manage the connection to a single server's export port
 */

public class ExportConnection {

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    static final private int CLOSED = 0;
    static final private int CONNECTING = 1;
    static final private int CONNECTED = 2;
    static final private int CLOSING = 3;

    private int m_state = CLOSED;

    public final String name;
    public final String m_ipString;
    private final InetSocketAddress serverAddr;
    private SocketChannel m_socket;

    private final String m_username;
    private final String m_password;

    // cached reference to ELClientBase's collection of ELDataSinks
    private final HashMap<Long, HashMap<String, HashMap<Integer, ExportDataSink>>> m_sinks;

    public final ArrayList<AdvertisedDataSource> dataSources;
    public final ArrayList<String> hosts;

    private long m_lastAckOffset;

    final BandwidthMonitor m_bandwidthMonitor;

    public ExportConnection(
            String username, String password,
            InetSocketAddress serverAddr,
            HashMap<Long, HashMap<String,
            HashMap<Integer, ExportDataSink>>> dataSinks,
            BandwidthMonitor bandwidthMonitor)
    {
        m_username = username != null ? username : "";
        m_password = password != null ? password : "";
        m_sinks = dataSinks;
        this.serverAddr = serverAddr;
        name = serverAddr.toString();
        m_ipString = serverAddr.getAddress().getHostAddress();
        dataSources = new ArrayList<AdvertisedDataSource>();
        hosts = new ArrayList<String>();
        m_bandwidthMonitor = bandwidthMonitor;
    }

    /**
     * Open the Export connection to the processor on m_socket.  This will
     * currently block until it receives the OPEN RESPONSE from the server
     * that contains the information about the available data sources
     */
    public void openExportConnection() throws IOException
    {
        m_logger.info("Starting EL Client socket to: " + serverAddr);
        byte hashedPassword[] = ConnectionUtil.getHashedPassword(m_password);
        Object[] cxndata =
            ConnectionUtil.
            getAuthenticatedExportConnection(serverAddr.getHostName(),
                                             m_username,
                                             hashedPassword,
                                             serverAddr.getPort());

        m_socket = (SocketChannel) cxndata[0];
        m_socket.socket().setTcpNoDelay(true);
        m_logger.info("Opened socket from " + m_socket.socket().getLocalSocketAddress() + " to " + m_socket.socket().getRemoteSocketAddress());
        if (m_state == CLOSED) {
            open();
            m_state = CONNECTING;
        }

        while (m_state == CONNECTING) {
            ExportProtoMessage m = nextMessage();
            if (m != null) {
                if(m.isOpenResponse())
                {
                    Pair<ArrayList<AdvertisedDataSource>,ArrayList<String>> advertisement;
                    advertisement = m.getAdvertisedDataSourcesAndNodes();
                    dataSources.addAll(advertisement.getFirst());
                    hosts.addAll(advertisement.getSecond());
                    m_state = CONNECTED;
                } else if (m.isError()) {
                   throw new IOException("Open response was an error message");
                }
            }
        }
    }

    public void closeExportConnection() {
        if (m_socket != null) {;
        if (m_socket.isConnected()) {
                try {
            m_socket.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        // seems hard to argue with...
        m_state = CLOSED;

        // perhaps a more controversial assertion?
        dataSources.clear();

        // clear this host from the list of tracked hosts
        if (m_bandwidthMonitor != null)
            m_bandwidthMonitor.removeHost(m_ipString);
    }

    /**
     * Retrieve the connected-ness of this EL Connection
     * @return true if in CONNECTED state
     */
    public boolean isConnected()
    {
        return (m_state == CONNECTED);
    }

    /**
     * perform a single iteration of work for the EL connection.
     * @return the number of server messages offered to the rxqueue(s).
     */
    public int work()
    {
        int messagesOffered = 0;

        // exportxxx need better error handling code in here
        if (!m_socket.isConnected() || !m_socket.isOpen()) {
            m_state = CLOSING;
        }

        if (m_state == CLOSING) {
            return messagesOffered;
        }

        // loop here to empty RX ?
        // receive data from network and hand to the proper ELProtocolHandler RX queue
        ExportProtoMessage m = null;
        do {
            try {
                m = nextMessage();
            } catch (IOException e) {
                m_logger.error("Socket error: " + e.getMessage());
                m_state = CLOSING;
            }

            if (m != null && m.isError()) {
                // XXX handle error from server, just die for now
                m_state = CLOSING;
            }

            // exportxxx need better error handling code in here
            if (!m_socket.isConnected() || !m_socket.isOpen()) {
                m_state = CLOSING;
            }

            if (m != null && m.isPollResponse()) {
                m_lastAckOffset = m.getAckOffset();

                HashMap<String, HashMap<Integer, ExportDataSink>> gen_map = m_sinks.get(m.getGeneration());
                if (gen_map == null) {
                    m_logger.error("Could not find sinks for generation " + m.getGeneration());
                    continue;
                }

                HashMap<Integer, ExportDataSink> part_map = gen_map.get(m.getSignature());
                if (part_map == null) {
                    m_logger.error("Could not find datasink for generation "
                            + m.getGeneration() + " table signature " + m.getSignature());
                    continue;
                }

                ExportDataSink rx_sink = part_map.get(m.getPartitionId());
                if (rx_sink == null) {
                    m_logger.error("Could not datasink for generation "
                            + m.getGeneration() + " table signature " + m.getSignature() +
                            " partition " + m.getPartitionId());
                    continue;
                }

                rx_sink.getRxQueue(name).offer(m);
                messagesOffered++;
            }
        }
        while (m_state == CONNECTED && m != null);

        // service all the ELDataSink TX queues
        for (HashMap<String, HashMap<Integer, ExportDataSink>> gen_map : m_sinks.values()) {
            for (HashMap<Integer, ExportDataSink> part_map : gen_map.values()) {
                for (ExportDataSink tx_sink : part_map.values()) {
                    Queue<ExportProtoMessage> tx_queue =
                        tx_sink.getTxQueue(name);
                    // this connection might not be connected to every sink
                    if (tx_queue != null) {
                        // XXX loop to drain the tx queue?
                        ExportProtoMessage tx_m =
                            tx_queue.poll();
                        if (tx_m != null) {
                            try {
                                sendMessage(tx_m);
                            } catch (IOException e) {
                                m_logger.trace("Failed to send message to server", e);
                            }
                        }
                    }
                }
            }
        }

        return messagesOffered;
    }

    private void open() throws IOException
    {
        m_logger.info("Opening new EL stream connection.");
        ExportProtoMessage m = new ExportProtoMessage( -1, -1, null);
        m.open();
        sendMessage(m);
    }

    public ExportProtoMessage nextMessage() throws IOException
    {
        FastDeserializer fds;
        final ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        int bytes_read = 0;
        do {
            bytes_read = m_socket.read(lengthBuffer);
        }
        while (lengthBuffer.hasRemaining() && bytes_read > 0);

        if (bytes_read < 0) {
            // Socket closed, try to bail out
            m_state = CLOSING;
            return null;
        }

        if (bytes_read == 0) {
            if (lengthBuffer.position() != 0 && m_socket.isConnected()) {
                // we're committed now, baby
                do {
                    bytes_read = m_socket.read(lengthBuffer);
                }
                while (lengthBuffer.hasRemaining() && bytes_read >= 0);

                if (bytes_read < 0) {
                    //  Socket closed, try to bail out
                    m_state = CLOSING;
                    return null;
                }
            }
            else {
                // non-blocking case
                return null;
            }
        }

        lengthBuffer.flip();
        int length = lengthBuffer.getInt();
        ByteBuffer messageBuf = ByteBuffer.allocate(length);
        do {
            bytes_read = m_socket.read(messageBuf);
        }
        while (messageBuf.remaining() > 0 && bytes_read >= 0);

        if (bytes_read < 0) {
            //  Socket closed, try to bail out
            m_state = CLOSING;
            return null;
        }
        messageBuf.flip();
        fds = new FastDeserializer(messageBuf);
        ExportProtoMessage m = ExportProtoMessage.readExternal(fds);

        // log bandwidth
        if (m_bandwidthMonitor != null)
            m_bandwidthMonitor.logBytesTransfered(m_ipString, length + 4, 0);

        return m;
    }

    public void sendMessage(ExportProtoMessage m) throws IOException
    {
        ByteBuffer buf = m.toBuffer();
        long length = buf.remaining();
        while (buf.remaining() > 0) {
            m_socket.write(buf);
        }

        // log bandwidth
        if (m_bandwidthMonitor != null)
            m_bandwidthMonitor.logBytesTransfered(m_ipString, 0, length);
    }

    public long getLastAckOffset() {
        return m_lastAckOffset;
    }
}
