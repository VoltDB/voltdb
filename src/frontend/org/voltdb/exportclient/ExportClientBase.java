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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.logging.VoltLogger;

/**
 * Provides an extensible base class for writing Export clients
 * Manages a set of connections to servers and a record of all of
 * the partitions and tables that are actively being exported.
 */

public abstract class ExportClientBase {

    protected AtomicBoolean m_connected = new AtomicBoolean(false);

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    private final List<InetSocketAddress> m_servers;
    protected final HashMap<InetSocketAddress, ExportConnection> m_exportConnections;

    // First hash by table signature, second by partition
    private final HashMap<String, HashMap<Integer, ExportDataSink>> m_sinks;

    // credentials
    private String m_username = "", m_password = "";

    public ExportClientBase()
    {
        m_sinks = new HashMap<String, HashMap<Integer, ExportDataSink>>();
        m_exportConnections = new HashMap<InetSocketAddress, ExportConnection>();
        m_servers = new ArrayList<InetSocketAddress>();
    }

    /**
     * Provide the ExportClient with a list of servers to which to connect
     * @param servers
     */
    public void addServerInfo(InetSocketAddress server) {
        m_servers.add(server);
    }

    public void addCredentials(String username, String password) {
        m_username = username;
        m_password = password;
    }

    /**
     * Allow derived clients to implement their own construction of ExportDecoders
     * for the data sources provided by the server on this Export connection.
     * @param source
     */
    public abstract ExportDecoderBase constructExportDecoder(AdvertisedDataSource source);

    private void constructExportDataSinks(ExportConnection elConnection) {
        m_logger.info("Num data sources is " + elConnection.dataSources.size());
        m_logger.info("Processing data sources for connection " + elConnection);
        for (AdvertisedDataSource source : elConnection.dataSources) {
            // Construct the app-specific decoder supplied by subclass
            // and build an ExportDataSink for this data source
            // Put the ExportDataSink in our hashed collection if it doesn't exist
            ExportDataSink sink = null;
            String table_signature = source.signature;
            int part_id = source.partitionId;
            HashMap<Integer, ExportDataSink> part_map =
                m_sinks.get(table_signature);
            if (part_map == null) {
                part_map = new HashMap<Integer, ExportDataSink>();
                m_sinks.put(table_signature, part_map);
            }
            if (!part_map.containsKey(part_id)) {
                m_logger.info("Creating decoder for table: " + source.tableName +
                        ", table ID, " + source.signature + " part ID: " + source.partitionId);
                ExportDecoderBase decoder = constructExportDecoder(source);
                sink = new ExportDataSink(source.partitionId,
                                      source.signature,
                                      source.tableName,
                                      decoder);
                part_map.put(part_id, sink);
            }
            else {
                // verify the export data is the same across partitions
                //sink = part_map.pu
            }
            sink = part_map.get(part_id);
            m_logger.info("Providing connection " + elConnection.name + " for table id " + source.signature + " to sink " + sink);
            // and plug the ExportConnection into the ExportDataSink
            sink.addExportConnection(elConnection.name, source.systemStartTimestamp);
        }
    }

    private ExportConnection connectToServer(InetSocketAddress addr) {
        ExportConnection retval = null;
        try {
            retval = new ExportConnection(m_username, m_password, addr, m_sinks);
            retval.openExportConnection();

            for (String hostname : retval.hosts) {
                assert(hostname.contains(":"));
                /*String[] parts = hostname.split(":");
                InetSocketAddress addr2 = new InetSocketAddress(parts[0], Integer.valueOf(parts[1]));
                assert(m_servers.contains(addr2));*/
        }

            constructExportDataSinks(retval);
        }
        catch (IOException e) {}

        return retval;
    }

    /**
     * Disconnect from any connected servers.
     * @throws IOException
     */
    public void disconnect() {
        for (ExportConnection connection : m_exportConnections.values()) {
            connection.closeExportConnection();
        }
        for (HashMap<Integer, ExportDataSink> part_map : m_sinks.values()) {
            for (ExportDataSink sink : part_map.values()) {
                sink.connectionClosed();
            }
        }
        m_exportConnections.clear();
        m_connected.set(false);
    }

    boolean checkConnections()
    {
        boolean retval = true;
        for (ExportConnection connection : m_exportConnections.values()) {
            if (!connection.isConnected()) {
                m_logger.error("Lost connection: " + connection.name +
                               ", Closing...");
                retval = false;
            }
        }
        return retval;
    }

    public boolean connect() throws IOException {
        if (m_connected.get()) {
            m_logger.error("Export client already connected.");
            throw new IOException("Export client already connected.");
        }
        if (m_servers.size() == 0) {
            m_logger.error("No servers provided for export client.");
            throw new IOException("No servers provided for export client.");
        }

        // Connect to one of the specified servers.  This will open the sockets,
        // advance the Export protocol to the open state to each server, retrieve
        // each AdvertisedDataSource list, and create data sinks for every
        // table/partition pair.
        for (InetSocketAddress serverAddr : m_servers) {
            ExportConnection exportConnection = null;
            try {
                exportConnection = new ExportConnection(m_username, m_password, serverAddr, m_sinks);
                exportConnection.openExportConnection();

                constructExportDataSinks(exportConnection);

                // successfully connected to one server, now rebuild the set of servers in the world
                m_servers.clear();
                // add this server as we know it's valid
                m_servers.add(serverAddr);

                for (String hostname : exportConnection.hosts) {
                    assert(hostname.contains(":"));
                    String[] parts = hostname.split(":");
                    InetSocketAddress addr = new InetSocketAddress(parts[0], Integer.valueOf(parts[1]));
                    m_servers.add(addr);
                }

                // add this one fully formed connection
                m_exportConnections.put(serverAddr, exportConnection);

                // exit out of the loop
                break;
            }
            catch (IOException e) {
                if (exportConnection != null)
                    exportConnection.closeExportConnection();
                m_sinks.clear();
                m_exportConnections.clear();
                if (e.getMessage().contains("Authentication")) {
                    throw e;
                }
            }
        }

        if (m_exportConnections.size() == 0) {
            return false;
        }
        assert (m_exportConnections.size() == 1);

        // connect to the rest of the servers
        // try three rounds with a pause in the middle of each
        for (int tryCount = 0; tryCount < 3; tryCount++) {
            for (InetSocketAddress addr : m_servers) {
                if (m_exportConnections.containsKey(addr)) continue;
                ExportConnection connection = connectToServer(addr);
                if (connection != null)
                    m_exportConnections.put(addr, connection);
            }

            // check for successful connection to all servers
            if (m_servers.size() != m_exportConnections.size())
                break;

            // sleep for 1/4 second
            try { Thread.sleep(250); } catch (InterruptedException e) {}
        }

        // check for non-complete connection and roll back if so
        if (m_servers.size() != m_exportConnections.size()) {
            m_sinks.clear();
            for (ExportConnection ec : m_exportConnections.values())
                ec.closeExportConnection();
            m_exportConnections.clear();
            return false;
        }

        m_connected.set(true);
        return true;
    }

    /**
     * Perform one iteration of Export Client work.
     * Connect if not connected.
     * Override if the specific client has strange workflow/termination conditions.
     * Largely for Export clients used for test.
     */
    protected int work() throws IOException
    {
        int offeredMsgs = 0;

        if (!m_connected.get()) {
            if (!connect())
                return 0;
        }

        // work all the ExportDataSinks.
        // process incoming data and generate outgoing ack/polls
        for (HashMap<Integer, ExportDataSink> partMap : m_sinks.values()) {
            for (ExportDataSink sink : partMap.values()) {
                sink.work();
            }
        }

        // drain all the received connection messages into the
        // RX queues for the ExportDataSinks and push all acks/polls
        // to the network.
        for (ExportConnection connection : m_exportConnections.values()) {
            offeredMsgs += connection.work();
        }

        // make sure still connected
        if (!checkConnections()) {
            disconnect();
            throw new IOException("Disconnected from one or more export servers");
        }

        return offeredMsgs;
    }

    protected long getNextPollDuration(long currentDuration, boolean isIdle) {
        // milliseconds to increment wait time when idle
        final long pollBackOffAddend = 100;
        // factor by which poll_wait_time should be reduced when not idle.
        final long pollAccelerateFactor = 2;
        // maximum value of poll_wait_time
        final long maxPollWaitTime = 2000;

        if (!isIdle) {
            return currentDuration / pollAccelerateFactor;
        }
        else {
            if (currentDuration < maxPollWaitTime) {
                return currentDuration + pollBackOffAddend;
            }
            return maxPollWaitTime;
        }
            }

    public void run() throws IOException {
        run(0);
            }

    public void run(long timeout) throws IOException {

        // current wait time between polls
        long pollTimeMS = 0;

        long now = System.currentTimeMillis();

        while ((System.currentTimeMillis() - now) < timeout) {

            // suck down some export data (if available)
            int offeredMsgs = 0;
                try {
                offeredMsgs = work();
                }
            catch (IOException e) {
                e.printStackTrace();
            }

            // pause for the right amount of time
            pollTimeMS = getNextPollDuration(pollTimeMS, offeredMsgs == 0);
            if (pollTimeMS > 0) {
                try { Thread.sleep(pollTimeMS); }
                catch (InterruptedException e) {}
            }
        }
    }
}
