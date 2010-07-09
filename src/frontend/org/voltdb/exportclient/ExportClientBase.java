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

package org.voltdb.exportclient;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.*;

import org.apache.log4j.Logger;

import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;
import org.voltdb.utils.VoltLoggerFactory;

/**
 * Provides an extensible base class for writing ELT clients
 */

public abstract class ExportClientBase implements Runnable {
    private static final Logger m_logger =
        Logger.getLogger("ExportClient",
                         VoltLoggerFactory.instance());

    private List<InetSocketAddress> m_servers = null;
    protected HashMap<String, ExportConnection> m_elConnections;

    // First hash by table, second by partition
    private final HashMap<Long, HashMap<Integer, ExportDataSink>> m_sinks;

    public ExportClientBase()
    {
        m_sinks = new HashMap<Long, HashMap<Integer, ExportDataSink>>();
        m_elConnections = new HashMap<String, ExportConnection>();
        m_servers = null;
    }

    /**
     * Provide the ELClient with a list of servers to which to connect
     * @param servers
     */
    public void setServerInfo(List<InetSocketAddress> servers)
    {
        m_servers = servers;
    }

    /**
     * Allow derived clients to implement their own construction of ELTDecoders
     * for the data sources provided by the server on this EL connection.
     * @param source
     */
    public abstract ExportDecoderBase constructELTDecoder(AdvertisedDataSource source);

    private void constructELTDataSinks(ExportConnection elConnection)
    {
        for (AdvertisedDataSource source : elConnection.getDataSources())
        {
            // Construct the app-specific decoder supplied by subclass
            // and build an ELDataSink for this data source
            m_logger.info("Creating decoder for table: " + source.tableName() +
                          ", part ID: " + source.partitionId());
            // Put the ELDataSink in our hashed collection if it doesn't exist
            ExportDataSink sink = null;
            long table_id = source.tableId();
            int part_id = source.partitionId();
            HashMap<Integer, ExportDataSink> part_map =
                m_sinks.get(table_id);
            if (part_map == null)
            {
                part_map = new HashMap<Integer, ExportDataSink>();
                m_sinks.put(table_id, part_map);
            }
            if (!part_map.containsKey(part_id))
            {
                ExportDecoderBase decoder = constructELTDecoder(source);
                sink = new ExportDataSink(source.partitionId(),
                                      source.tableId(),
                                      source.tableName(),
                                      decoder);
                part_map.put(part_id, sink);
            }
            sink = part_map.get(part_id);
            // and plug the ELConnection into the ELDataSink
            sink.addELConnection(elConnection.getConnectionName());
        }
    }

    /**
     * Connect to all the specified servers.  This will open the sockets,
     * advance the EL protocol to the open state to each server, retrieve
     * each AdvertisedDataSource list, and create data sinks for every
     * table/partition pair.
     * @throws IOException
     */
    public void connectToELServers(String username, String password) throws IOException
    {
        if (m_servers == null || m_servers.size() == 0)
        {
            m_logger.fatal("No servers provided for ELT, exiting...");
            throw new RuntimeException("No servers provided for ELT connection");
        }
        for (InetSocketAddress server_addr : m_servers)
        {
            ExportConnection elConnection =
                new ExportConnection(username, password, server_addr, m_sinks);
            elConnection.openELTConnection();
            constructELTDataSinks(elConnection);
            m_elConnections.put(elConnection.getConnectionName(), elConnection);
        }
    }

    /**
     * Disconnect from any connected servers.
     * @throws IOException
     */
    public void disconnectFromELServers() throws IOException {
        Set<String> keySet = m_elConnections.keySet();
        for (String key : keySet) {
            ExportConnection exportConnection = m_elConnections.get(key);
            exportConnection.closeELTConnection();
        }
    }

    boolean checkConnections()
    {
        boolean retval = true;
        for (String el_connection : m_elConnections.keySet())
        {
            if (!m_elConnections.get(el_connection).isConnected())
            {
                m_logger.error("Lost connection: " + el_connection +
                               ", Closing...");
                retval = false;
            }
        }
        return retval;
    }

    /**
     * Perform one iteration of EL Client work.
     *  Override if the specific client has strange workflow/termination conditions.
     * Largely for EL clients used for test.
     */
    public void work()
    {
        // drain all the received connection messages into the
        // RX queues for the ELDataSinks
        for (ExportConnection el_connection : m_elConnections.values())
        {
            el_connection.work();
        }

        // work all the ELDataSinks to generate outgoing messages
        for (HashMap<Integer, ExportDataSink> part_map : m_sinks.values())
        {
            for (ExportDataSink work_sink : part_map.values())
            {
                work_sink.work();
            }
        }

        // Service all the ELDataSink TX queues
        for (ExportConnection el_connection : m_elConnections.values())
        {
            el_connection.work();
        }
    }

    @Override
    public void run() {
        boolean connected = true;
        while (connected)
        {
            work();
            connected = checkConnections();
        }
    }
}
