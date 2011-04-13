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
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.voltdb.VoltDB;
import org.voltdb.export.ExportProtoMessage.AdvertisedDataSource;
import org.voltdb.logging.VoltLogger;
import org.voltdb.utils.Pair;

/**
 * Provides an extensible base class for writing Export clients
 * Manages a set of connections to servers and a record of all of
 * the partitions and tables that are actively being exported.
 */

public abstract class ExportClientBase {

    protected AtomicBoolean m_connected = new AtomicBoolean(false);

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    protected final List<InetSocketAddress> m_servers
        = new ArrayList<InetSocketAddress>();
    protected final HashMap<InetSocketAddress, ExportConnection> m_exportConnections
        = new HashMap<InetSocketAddress, ExportConnection>();

    protected final boolean m_useAdminPorts;

    // First hash by table signature, second by partition
    private final HashMap<Long, HashMap<String, HashMap<Integer, ExportDataSink>>> m_sinks
        = new HashMap<Long, HashMap<String, HashMap<Integer, ExportDataSink>>>();

    private final HashSet<AdvertisedDataSource> m_knownDataSources = new HashSet<AdvertisedDataSource>();

    // credentials
    protected String m_username = "", m_password = "";

    public ExportClientBase() {
        this(false);
    }

    public ExportClientBase(boolean useAdminPorts)
    {
        m_useAdminPorts = useAdminPorts;
    }

    /**
     * Add a server to the list that ExportClient will try to connect to
     * @param server
     */
    public void addServerInfo(InetSocketAddress server) {
        m_servers.add(server);
    }

    /**
     * Add a server to the list that ExportClient will try to connect to
     * @param server
     */
    public void addServerInfo(String server, boolean adminIfNoPort) {
        InetSocketAddress addr = null;
        int defaultPort = adminIfNoPort ? VoltDB.DEFAULT_ADMIN_PORT : VoltDB.DEFAULT_PORT;
        String[] parts = server.trim().split(":");
        if (parts.length == 1) {
            addr = new InetSocketAddress(parts[0], defaultPort);
        }
        else {
            assert(parts.length == 2);
            int port = Integer.parseInt(parts[1]);
            addr = new InetSocketAddress(parts[0], port);
        }
        m_servers.add(addr);
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
        m_logger.debug("Num data sources is " + elConnection.dataSources.size());
        m_logger.debug("Processing data sources for connection " + elConnection);
        for (AdvertisedDataSource source : elConnection.dataSources) {
            // Construct the app-specific decoder supplied by subclass
            // and build an ExportDataSink for this data source
            // Put the ExportDataSink in our hashed collection if it doesn't exist
            ExportDataSink sink = null;
            String table_signature = source.signature;
            int part_id = source.partitionId;
            HashMap<String, HashMap<Integer, ExportDataSink>> gen_map =
                m_sinks.get(source.m_generation);
            if (gen_map == null) {
                gen_map = new HashMap<String, HashMap<Integer, ExportDataSink>>();
                m_sinks.put(source.m_generation, gen_map);
            }

            HashMap<Integer, ExportDataSink> part_map =
                gen_map.get(table_signature);
            if (part_map == null) {
                part_map = new HashMap<Integer, ExportDataSink>();
                gen_map.put(table_signature, part_map);
            }
            if (!part_map.containsKey(part_id)) {
                m_logger.debug("Creating decoder for generation " + source.m_generation + " table: " + source.tableName +
                        ", table ID, " + source.signature + " part ID: " + source.partitionId);
                ExportDecoderBase decoder = constructExportDecoder(source);
                sink = new ExportDataSink( source.m_generation,
                                      source.partitionId,
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
            m_logger.debug("Providing connection " + elConnection.name + " for table id " + source.signature + " to sink " + sink);
            // and plug the ExportConnection into the ExportDataSink
            sink.addExportConnection(elConnection.name, source.systemStartTimestamp);
        }
    }

    private ExportConnection connectToServer(InetSocketAddress addr) {
        ExportConnection retval = null;
        try {
            retval = new ExportConnection(m_username, m_password, addr, m_sinks);
            retval.openExportConnection();

            if (!retval.isConnected()) {
                return null;
            }

            for (String hostname : retval.hosts) {
                assert(hostname.contains(":"));
                /*String[] parts = hostname.split(":");
                InetSocketAddress addr2 = new InetSocketAddress(parts[0], Integer.valueOf(parts[1]));
                assert(m_servers.contains(addr2));*/
            }

            constructExportDataSinks(retval);
        }
        catch (IOException e) {
            m_logger.warn("Error connecting to export server " + addr, e);
        }

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
        for (HashMap<String, HashMap<Integer, ExportDataSink>> gen_map : m_sinks.values()) {
            for (HashMap<Integer, ExportDataSink> part_map : gen_map.values()) {
                for (ExportDataSink sink : part_map.values()) {
                    sink.connectionClosed();
                }
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

    public boolean connect() throws ExportClientException {
        if (m_connected.get()) {
            m_logger.error("Export client already connected.");
            throw new ExportClientException(ExportClientException.Type.USER_ERROR,
                                            "Export client already connected.");
        }
        if (m_servers.size() == 0) {
            m_logger.error("No servers provided for export client.");
            throw new ExportClientException(ExportClientException.Type.USER_ERROR,
                                            "No servers provided for export client.");
        }

        m_exportConnections.clear();

        // Connect to one of the specified servers.  This will open the sockets,
        // advance the Export protocol to the open state to each server, retrieve
        // each AdvertisedDataSource list, and create data sinks for every
        // table/partition pair.
        boolean foundOneActiveServer = false;
        ArrayList<Pair<Exception, InetSocketAddress>> connectErrors =
            new ArrayList<Pair<Exception, InetSocketAddress>>();
        for (InetSocketAddress serverAddr : m_servers) {
            ExportConnection exportConnection = null;
            try {
                exportConnection = new ExportConnection(m_username, m_password, serverAddr, m_sinks);
                exportConnection.openExportConnection();

                // failed to connect
                if (!exportConnection.isConnected())
                    continue;

                // from here down, assume we connected

                // successfully connected to one server, now rebuild the set of servers in the world
                m_servers.clear();
                m_logger.info("Discovered topology " + exportConnection.hosts.toString());
                for (String hostname : exportConnection.hosts) {
                    assert(hostname.contains(":"));
                    String[] parts = hostname.split(":");
                    int port = m_useAdminPorts ? Integer.valueOf(parts[2]) : Integer.valueOf(parts[1]);
                    InetSocketAddress addr = new InetSocketAddress(parts[0], port);
                    m_servers.add(addr);
                }

                // exit out of the loop
                foundOneActiveServer = true;
                break;
            }
            catch (IOException e) {
                if (e.getMessage().contains("Authentication")) {
                    throw new ExportClientException(ExportClientException.Type.AUTH_FAILURE,
                            "Authentication failure", e);
                } else {
                    connectErrors.add(Pair.of((Exception)e, serverAddr));
                }
                // ignore non-auth errors
            }
            finally {
                // disconnect from the "discovery server"
                if (exportConnection != null)
                    exportConnection.closeExportConnection();
            }
        }

        if (!foundOneActiveServer) {
            for (Pair<Exception, InetSocketAddress> p : connectErrors) {
                m_logger.error("Error connecting to server " + p.getSecond() + " while discovering cluster topology",
                        p.getFirst());
            }
            m_logger.error("Unable to connect to a server to discover the cluster topology");
            return false;
        }

        HashSet<AdvertisedDataSource> foundSources = new HashSet<AdvertisedDataSource>();
        // connect to the rest of the servers
        // try three rounds with a pause in the middle of each
        for (int tryCount = 0; tryCount < 3; tryCount++) {
            for (InetSocketAddress addr : m_servers) {
                if (m_exportConnections.containsKey(addr)) continue;
                ExportConnection connection = connectToServer(addr);
                if (connection != null) {
                    m_exportConnections.put(addr, connection);
                    foundSources.addAll(connection.dataSources);
                }
            }

            // check for successful connection to all servers
            if (m_servers.size() == m_exportConnections.size())
                break;

            // sleep for 1/4 second
            try { Thread.sleep(250); } catch (InterruptedException e) {}
        }

        /*
         * All datasinks that are no longer advertised anywhere in the cluster need to be discarded
         */
        HashSet<AdvertisedDataSource> knownSources = new HashSet<AdvertisedDataSource>(m_knownDataSources);
        knownSources.removeAll(foundSources);

        for (AdvertisedDataSource source : knownSources) {
            HashMap<String, HashMap<Integer, ExportDataSink>> gen_map = m_sinks.get(source.m_generation);
            HashMap<Integer, ExportDataSink> part_map = gen_map.get(source.signature);
            ExportDataSink sink = part_map.remove(source.partitionId);
            if (part_map.isEmpty()) {
                gen_map.remove(source.signature);
            }
            if (gen_map.isEmpty()) {
                m_sinks.remove(source.m_generation);
            }
            sink.sourceNoLongerAdvertised(source);
        }
        m_knownDataSources.clear();
        m_knownDataSources.addAll(foundSources);

        // check for non-complete connection and roll back if so
        if (m_servers.size() != m_exportConnections.size()) {
            disconnect();
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
    protected int work() throws ExportClientException
    {
        int offeredMsgs = 0;

        assert(m_connected.get());

        // work all the ExportDataSinks.
        // process incoming data and generate outgoing ack/polls
        for (HashMap<String, HashMap<Integer, ExportDataSink>> gen_map : m_sinks.values()) {
            for (HashMap<Integer, ExportDataSink> part_map : gen_map.values()) {
                for (ExportDataSink sink : part_map.values()) {
                    sink.work();
                }
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
            throw new ExportClientException(ExportClientException.Type.DISCONNECT_UNEXPECTED,
                    "Disconnected from one or more export servers");
        }

        return offeredMsgs;
    }

    protected long getNextPollDuration(long currentDuration,
                                       boolean isIdle,
                                       boolean disconnectedWithError,
                                       boolean disconnectedForUpdate) {

        // milliseconds to increment wait time when idle
        final long pollBackOffAddend = 100;
        // factor by which poll_wait_time should be reduced when not idle.
        final long pollAccelerateFactor = 2;
        // maximum value of poll_wait_time
        final long maxPollWaitTime = 4000;

        if (disconnectedForUpdate || disconnectedWithError)
            return maxPollWaitTime;

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

    protected void run() throws ExportClientException {
        run(0);
    }

    protected void run(long timeout) throws ExportClientException {

        // current wait time between polls
        long pollTimeMS = 0;

        long now = System.currentTimeMillis();

        // run until the timeout
        // runs forever if timeout == 0
        while ((timeout == 0) || ((System.currentTimeMillis() - now) < timeout)) {

            // use the variables to decide how long to wait at the end of the loop
            boolean connected = m_connected.get();
            int offeredMsgs = 0;
            boolean disconnectedWithError = false;
            boolean disconnectedForUpdate = false;

            // if not connected, take a stab at it
            if (!connected) {
                try {
                    connected = connect();
                } catch (ExportClientException e) {
                    m_logger.warn(e.getMessage(), e);
                    e.printStackTrace();

                    // handle the problem and decide whether
                    // to continue or to punt up a stack frame
                    switch (e.type) {
                    case AUTH_FAILURE:
                        disconnectedWithError = true;
                        break;
                    case DISCONNECT_UNEXPECTED:
                        disconnectedWithError = true;
                        break;
                    case DISCONNECT_UPDATE:
                        disconnectedForUpdate = true;
                        break;
                    case USER_ERROR:
                        throw e;
                    }
                }
            }

            // if connected, try to actually do some export work
            if (connected) {
                // suck down some export data (if available)
                try {
                    offeredMsgs = work();
                }
                catch (ExportClientException e) {
                    m_logger.warn(e.getMessage(), e);
                    e.printStackTrace();

                    // handle the problem and decide whether
                    // to continue or to punt up a stack frame
                    switch (e.type) {
                    case AUTH_FAILURE:
                        assert(false);
                        break;
                    case DISCONNECT_UNEXPECTED:
                        disconnectedWithError = true;
                        break;
                    case DISCONNECT_UPDATE:
                        disconnectedForUpdate = true;
                        break;
                    case USER_ERROR:
                        throw e;
                    }
                }
            }

            // pause for the right amount of time
            pollTimeMS = getNextPollDuration(pollTimeMS, offeredMsgs == 0,
                    disconnectedWithError, disconnectedForUpdate);
            if (pollTimeMS > 0) {
                m_logger.trace(String.format("Sleeping for %d ms due to inactivity or no connection.", pollTimeMS));
                try { Thread.sleep(pollTimeMS); }
                catch (InterruptedException e) {
                    throw new ExportClientException(e);
                }
            }
        }
    }
}
