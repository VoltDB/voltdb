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
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.logging.VoltLogger;

/**
 * Provides an extensible base class for writing Export clients
 */

public abstract class ExportClientBase implements Runnable {
    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    private List<InetSocketAddress> m_servers = null;
    protected HashMap<String, ExportConnection> m_exportConnections;

    // First hash by table, second by partition
    private final HashMap<Long, HashMap<Integer, ExportDataSink>> m_sinks;

    public ExportClientBase()
    {
        m_sinks = new HashMap<Long, HashMap<Integer, ExportDataSink>>();
        m_exportConnections = new HashMap<String, ExportConnection>();
        m_servers = null;
    }

    /**
     * Provide the ExportClient with a list of servers to which to connect
     * @param servers
     */
    public void setServerInfo(List<InetSocketAddress> servers)
    {
        m_servers = servers;
    }

    /**
     * Allow derived clients to implement their own construction of ExportDecoders
     * for the data sources provided by the server on this Export connection.
     * @param source
     */
    public abstract ExportDecoderBase constructExportDecoder(AdvertisedDataSource source);

    private void constructExportDataSinks(ExportConnection elConnection)
    {
        for (AdvertisedDataSource source : elConnection.getDataSources())
        {
            // Construct the app-specific decoder supplied by subclass
            // and build an ExportDataSink for this data source
            m_logger.info("Creating decoder for table: " + source.tableName() +
                          ", part ID: " + source.partitionId());
            // Put the ExportDataSink in our hashed collection if it doesn't exist
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
                ExportDecoderBase decoder = constructExportDecoder(source);
                sink = new ExportDataSink(source.partitionId(),
                                      source.tableId(),
                                      source.tableName(),
                                      decoder);
                part_map.put(part_id, sink);
            }
            sink = part_map.get(part_id);
            // and plug the ExportConnection into the ExportDataSink
            sink.addExportConnection(elConnection.getConnectionName());
        }
    }

    /**
     * Connect to all the specified servers.  This will open the sockets,
     * advance the Export protocol to the open state to each server, retrieve
     * each AdvertisedDataSource list, and create data sinks for every
     * table/partition pair.
     * @throws IOException
     */
    public void connectToExportServers(String username, String password) throws IOException
    {
        if (m_servers == null || m_servers.size() == 0)
        {
            m_logger.fatal("No servers provided for Export, exiting...");
            throw new RuntimeException("No servers provided for Export connection");
        }
        for (InetSocketAddress server_addr : m_servers)
        {
            ExportConnection exportConnection =
                new ExportConnection(username, password, server_addr, m_sinks);
            exportConnection.openExportConnection();
            constructExportDataSinks(exportConnection);
            m_exportConnections.put(exportConnection.getConnectionName(), exportConnection);
        }
    }

    /**
     * Disconnect from any connected servers.
     * @throws IOException
     */
    public void disconnectFromExportServers() throws IOException {
        Set<String> keySet = m_exportConnections.keySet();
        for (String key : keySet) {
            ExportConnection exportConnection = m_exportConnections.get(key);
            exportConnection.closeExportConnection();
        }
    }

    boolean checkConnections()
    {
        boolean retval = true;
        for (String el_connection : m_exportConnections.keySet())
        {
            if (!m_exportConnections.get(el_connection).isConnected())
            {
                m_logger.error("Lost connection: " + el_connection +
                               ", Closing...");
                retval = false;
            }
        }
        return retval;
    }

    /**
     * Perform one iteration of Export Client work.
     * Override if the specific client has strange workflow/termination conditions.
     * Largely for Export clients used for test.
     */
    public int work()
    {
        int offered_msgs = 0;

        // work all the ExportDataSinks.
        // process incoming data and generate outgoing ack/polls
        for (HashMap<Integer, ExportDataSink> part_map : m_sinks.values())
        {
            for (ExportDataSink work_sink : part_map.values())
            {
                work_sink.work();
            }
        }

        // drain all the received connection messages into the
        // RX queues for the ExportDataSinks and push all acks/polls
        // to the network.
        for (ExportConnection el_connection : m_exportConnections.values())
        {
            offered_msgs += el_connection.work();
        }

        return offered_msgs;
    }

    @Override
    public void run() {
        boolean connected = true;

        // current wait time between polls
        int poll_wait_time = 0;

        // maximum value of poll_wait_time
        final int max_poll_wait_time = 2000;

        // milliseconds to increment wait time when idle
        final int poll_back_off_addend = 100;

        // factor by which poll_wait_time should be reduced when not idle.
        final int poll_accelerate_factor = 2;


        while (connected)
        {
            int offered_msgs = work();
            connected = checkConnections();


            // adjust idle loop wait time.
            if (offered_msgs > 0) {
                poll_wait_time = (int) Math.floor(poll_wait_time / poll_accelerate_factor);
            }
            else if (poll_wait_time < max_poll_wait_time) {
                poll_wait_time += poll_back_off_addend;
            }
            if (poll_wait_time > 0) {
                try {
                    Thread.sleep(poll_wait_time);
                } catch (InterruptedException e) {
                }
            }

        }
    }
}
