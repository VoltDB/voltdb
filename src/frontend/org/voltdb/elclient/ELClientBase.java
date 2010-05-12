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
import java.util.HashMap;
import java.util.List;

import org.voltdb.elt.ELTProtoMessage.AdvertisedDataSource;

/**
 * Provides an extensible base class for writing ELT clients
 */

public abstract class ELClientBase implements Runnable {

    private List<InetSocketAddress> m_servers = null;
    protected HashMap<String, ELConnection> m_elConnections;

    // First hash by table, second by partition
    private HashMap<Integer, HashMap<Integer, ELDataSink>> m_sinks;

    public ELClientBase()
    {
        m_sinks = new HashMap<Integer, HashMap<Integer, ELDataSink>>();
        m_elConnections = new HashMap<String, ELConnection>();
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
     * @return
     */
    public abstract ELTDecoderBase constructELTDecoder(AdvertisedDataSource source);

    private void constructELTDataSinks(ELConnection elConnection)
    {
        for (AdvertisedDataSource source : elConnection.getDataSources())
        {
            // Construct the app-specific decoder supplied by subclass
            // and build an ELDataSink for this data source
            System.out.println("Creating verifier for table: " + source.tableName() +
                               ", part ID: " + source.partitionId());
            // Put the ELDataSink in our hashed collection if it doesn't exist
            ELDataSink sink = null;
            int table_id = source.tableId();
            int part_id = source.partitionId();
            HashMap<Integer, ELDataSink> part_map =
                m_sinks.get(table_id);
            if (part_map == null)
            {
                part_map = new HashMap<Integer, ELDataSink>();
                m_sinks.put(table_id, part_map);
            }
            if (!part_map.containsKey(part_id))
            {
                ELTDecoderBase decoder = constructELTDecoder(source);
                sink = new ELDataSink(source.partitionId(),
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
     */
    public void connectToELServers()
    {
        if (m_servers == null || m_servers.size() == 0)
        {
            System.out.println("No servers provided for ELT, exiting...");
            System.exit(1);
        }
        for (InetSocketAddress server_addr : m_servers)
        {
            ELConnection elConnection =
                new ELConnection(server_addr, m_sinks);
            try {
                elConnection.openELTConnection();
                constructELTDataSinks(elConnection);
            }
            catch (IOException e) {
                // XXX We should be smarter if it fails to connect and open
                e.printStackTrace();
                System.exit(1);
            }
            m_elConnections.put(elConnection.getConnectionName(), elConnection);
        }
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
        for (ELConnection el_connection : m_elConnections.values())
        {
            el_connection.work();
        }

        // work all the ELDataSinks to generate outgoing messages
        for (HashMap<Integer, ELDataSink> part_map : m_sinks.values())
        {
            for (ELDataSink work_sink : part_map.values())
            {
                work_sink.work();
            }
        }

        // Service all the ELDataSink TX queues
        for (ELConnection el_connection : m_elConnections.values())
        {
            el_connection.work();
        }
    }

    @Override
    public void run() {
        connectToELServers();
        // XXX need smarter continue condition
        while (true)
        {
            work();
        }
    }
}
