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
import org.voltdb.utils.BandwidthMonitor;
import org.voltdb.utils.Pair;

/**
 * Provides an extensible base class for writing Export clients
 * Manages a set of connections to servers and a record of all of
 * the partitions and tables that are actively being exported.
 */

public abstract class ExportClientBase {

    protected AtomicBoolean m_connected = new AtomicBoolean(false);

    // object used to synchronize on so the shutdown hook can behave
    final java.util.concurrent.locks.ReentrantLock m_atomicWorkLock =
        new java.util.concurrent.locks.ReentrantLock(true);
    final ShutdownHook m_shutdownHook = new ShutdownHook();

    private static final VoltLogger m_logger = new VoltLogger("ExportClient");

    protected final List<InetSocketAddress> m_servers
        = new ArrayList<InetSocketAddress>();
    protected final HashMap<InetSocketAddress, ExportConnection> m_exportConnections
        = new HashMap<InetSocketAddress, ExportConnection>();

    protected final boolean m_useAdminPorts;
    protected final boolean m_autodiscoverTopolgy;
    protected boolean m_hasPrintedAutodiscoveryWarning = false;

    // First hash by table signature, second by partition
    private final HashMap<Long, HashMap<String, HashMap<Integer, ExportDataSink>>> m_sinks
        = new HashMap<Long, HashMap<String, HashMap<Integer, ExportDataSink>>>();

    private final HashSet<AdvertisedDataSource> m_knownDataSources = new HashSet<AdvertisedDataSource>();

    final BandwidthMonitor m_bandwidthMonitor;

    // credentials
    protected String m_username = "", m_password = "";

    /**
     * Shutdown hook that waits until work is done and then kills the JVM
     * BE AFRAID...
     */
    class ShutdownHook extends Thread {
        @Override
        public void run() {
            final VoltLogger log = new VoltLogger("ExportClient");
            log.info("Received request to shutdown.");

            // the ExportClientBase.work() holds this lock during
            // each iteration of its work
            m_atomicWorkLock.lock();
            log.info("Work lock aquired. About to shutdown.");

            // for tests only (noop otherwise)
            extraShutdownHookWork();
        }
    }

    public ExportClientBase() {
        this(false);
    }

    public ExportClientBase(boolean useAdminPorts)
    {
        this(false, 0, true);
    }

    public ExportClientBase(boolean useAdminPorts, int throughputDisplayPeriod, boolean autodiscoverTopolgy)
    {
        m_useAdminPorts = useAdminPorts;
        if (throughputDisplayPeriod > 0)
            m_bandwidthMonitor = new BandwidthMonitor(throughputDisplayPeriod, m_logger);
        else
            m_bandwidthMonitor = null;
        m_autodiscoverTopolgy = autodiscoverTopolgy;
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

    private ExportConnection connectToServer(InetSocketAddress addr) throws InterruptedException {
        try {
            ExportConnection retval = new ExportConnection(m_username, m_password, addr, m_sinks, m_bandwidthMonitor);
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
            return retval;
        }
        catch (IOException e) {
            m_logger.warn("Error connecting to export server " + addr, e);
            if (e instanceof java.nio.channels.ClosedByInterruptException) {
                throw new InterruptedException(e.getMessage());
            }
        }

        return null;
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
                m_logger.warn("Lost connection: " + connection.name + ", Closing...");
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
                exportConnection = new ExportConnection(m_username, m_password, serverAddr, m_sinks, m_bandwidthMonitor);
                exportConnection.openExportConnection();

                // failed to connect
                if (!exportConnection.isConnected())
                    continue;

                // from here down, assume we connected

                if (m_autodiscoverTopolgy) {
                    // successfully connected to one server, now rebuild the set of servers in the world
                    m_servers.clear();
                    m_logger.info("Discovered topology " + exportConnection.hosts.toString());
                    for (String hostname : exportConnection.hosts) {
                        assert(hostname.contains(":"));
                        String[] parts = hostname.split(":");
                        int port = m_useAdminPorts ? Integer.valueOf(parts[2]) : Integer.valueOf(parts[1]);
                        InetSocketAddress addr = new InetSocketAddress(parts[0].split(",")[0], port);
                        m_servers.add(addr);
                    }
                }
                else {
                    // notify the user we're skipping auto-discovery
                    m_logger.warn("Skipping topology auto-discovery per command line configuration.");
                    // warn user once about the perils of their choice
                    if (!m_hasPrintedAutodiscoveryWarning) {
                        m_logger.warn("Running without auto-discovery may produce unexpected results " +
                                "and should only be run if the implications are understood.");
                        m_logger.warn("Please contact VoltDB support if more information is required.");
                        m_hasPrintedAutodiscoveryWarning = true;
                    }
                }

                // exit out of the loop
                foundOneActiveServer = true;
                break;
            }
            catch (IOException e) {
                if (e instanceof java.nio.channels.ClosedByInterruptException) {
                    return false;
                }
                if (e.getMessage().contains("Export")) {
                    throw new ExportClientException(ExportClientException.Type.AUTH_FAILURE,
                            "Export is not enabled on this server.", e);
                }
                else if (e.getMessage().contains("Authentication")) {
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
                m_logger.warn("Failed to connect to server " +
                              p.getSecond() +
                              " with error: " +
                              p.getFirst().getMessage());
            }
            m_logger.warn("Unable to connect to a given server to discover the cluster topology");
            return false;
        }

        HashSet<AdvertisedDataSource> foundSources = new HashSet<AdvertisedDataSource>();
        // connect to the rest of the servers
        // try three rounds with a pause in the middle of each
        for (int tryCount = 0; tryCount < 3; tryCount++) {
            for (InetSocketAddress addr : m_servers) {
                if (m_exportConnections.containsKey(addr)) continue;
                ExportConnection connection;
                try {
                    connection = connectToServer(addr);
                } catch (InterruptedException e) {
                    disconnect();
                    return false;
                }
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

        // check for non-complete connection and roll back if so
        if (m_servers.size() != m_exportConnections.size()) {
            disconnect();
            return false;
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

        m_connected.set(true);
        return true;
    }

    // HOOKS FOR TEST/SUBCLASSES THAT ARE USUALLY NOOPS
    protected void preWorkHook() {}
    protected void startWorkHook() {}
    protected void endWorkHook() {}
    protected void extraShutdownHookWork() {}

    /**
     * Perform one iteration of Export Client work.
     * Connect if not connected.
     * Override if the specific client has strange workflow/termination conditions.
     * Largely for Export clients used for test.
     */
    public int work() throws ExportClientException {
        int offeredMsgs = 0;

        preWorkHook();

        // hold this lock while doing one unit of work
        // the shutdown hook won't let the system die until
        // the lock is released (except via kill -9)
        m_atomicWorkLock.lock();
        try {
            // noop if not running test code
            startWorkHook();

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

            // noop if not running test code
            endWorkHook();
        } finally {
            m_atomicWorkLock.unlock();
        }

        // return the amount of work effectively done
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

    public void run(long timeout) throws ExportClientException {

        try {
            // add the shutdown hook that insulates the process from crtl-c a bit
            Runtime.getRuntime().addShutdownHook(m_shutdownHook);

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

                        //e.printStackTrace();

                        // handle the problem and decide whether
                        // to continue or to punt up a stack frame
                        switch (e.type) {
                        case AUTH_FAILURE:
                            throw e;
                        case DISCONNECT_UNEXPECTED:
                            if (m_logger.isTraceEnabled())
                                m_logger.warn(e.getMessage(), e);
                            else
                                m_logger.warn(e.getMessage());
                            disconnectedWithError = true;
                            break;
                        case DISCONNECT_UPDATE:
                            if (m_logger.isTraceEnabled())
                                m_logger.info(e.getMessage(), e);
                            else
                                m_logger.info(e.getMessage());
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
                        // handle the problem and decide whether
                        // to continue or to punt up a stack frame
                        switch (e.type) {
                        case AUTH_FAILURE:
                            m_logger.fatal(e.getMessage(), e);
                            throw new RuntimeException("Got a unexpect auth error from connected server", e);
                        case DISCONNECT_UNEXPECTED:
                            if (m_logger.isTraceEnabled())
                                m_logger.warn(e.getMessage(), e);
                            else
                                m_logger.warn(e.getMessage());
                            disconnectedWithError = true;
                            break;
                        case DISCONNECT_UPDATE:
                            if (m_logger.isTraceEnabled())
                                m_logger.info(e.getMessage(), e);
                            else
                                m_logger.info(e.getMessage());
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
        finally {
            // ensure the shutdown hook is removed when not doing work
            Runtime.getRuntime().removeShutdownHook(m_shutdownHook);
        }
    }
}
