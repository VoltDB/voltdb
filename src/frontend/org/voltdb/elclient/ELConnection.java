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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;

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

    private SocketChannel m_socket;

    // First hash by table, second by partition
    private HashMap<Integer, HashMap<Integer, ELDataSink>> m_sinks;

    private ArrayList<AdvertisedDataSource> m_dataSources;

    public ELConnection(SocketChannel socket) {
        m_socket = socket;
        m_sinks = new HashMap<Integer, HashMap<Integer, ELDataSink>>();
    }

    /**
     * Register a data sink to receive and decode an ELT data source.
     * The sink will receive the stream corresponding to the
     * table ID and partition ID with which it was constructed.
     * @param sink
     */
    public void registerDataSink(ELDataSink sink)
    {
        int table_id = sink.getTableId();
        int part_id = sink.getPartitionId();
        HashMap<Integer, ELDataSink> part_map =
            m_sinks.get(table_id);
        if (part_map == null)
        {
            part_map = new HashMap<Integer, ELDataSink>();
            m_sinks.put(table_id, part_map);
        }
        if (part_map.containsKey(part_id))
        {
            System.out.println("ELClient already contains a sink for table: " + table_id +
                               ", partition: " + part_id);
            return;
        }
        part_map.put(part_id, sink);
    }

    /**
     * Open the ELT connection to the processor on m_socket.  This will
     * currently block until it receives the OPEN RESPONSE from the server
     * that contains the information about the available data sources
     */
    public void openELTConnection() throws IOException
    {
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
     * perform a single iteration of work for the EL connection.
     */
    public void work()
    {
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
                rx_sink.getRxQueue().offer(m);
            }
        }
        while (m != null);

        // work all the ELProtocolHandlers
        for (HashMap<Integer, ELDataSink> part_map : m_sinks.values())
        {
            for (ELDataSink work_sink : part_map.values())
            {
                work_sink.work();
            }
        }

        // service all the ELProtocolHandler TX queues
        for (HashMap<Integer, ELDataSink> part_map : m_sinks.values())
        {
            for (ELDataSink tx_sink : part_map.values())
            {
                // XXX loop to drain the tx queue?
                ELTProtoMessage tx_m = tx_sink.getTxQueue().poll();
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

    private ELTProtoMessage nextMessage() throws IOException
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

    private void sendMessage(ELTProtoMessage m) throws IOException
    {
        ByteBuffer buf = m.toBuffer();
        while (buf.remaining() > 0) {
            m_socket.write(buf);
        }
    }
}
