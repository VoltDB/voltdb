/* This file is part of VoltDB.
 * Copyright (C) 2008-2016 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.voltcore.messaging;

import java.io.EOFException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.ReverseDNSCache;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.VersionChecker;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.net.HostAndPort;

/**
 * SocketJoiner runs all the time listening for new nodes in the cluster. Since it is a dedicated thread
 * it is able to block while a new node joins without disrupting other activities.
 *
 * At startup socket joiner will connect to the rest of the cluster in the start method if it fails
 * to bind to the leader address.
 *
 * If it binds to the leader address and becomes the leader the start method returns immediately and runPrimary
 * is run from a separate thread. runPrimary will wait for the countdown latch for bootstrapping zk to count down
 * before accepting new connections
 */
public class SocketJoiner {
    static final String HOSTS = "hosts";
    static final String REPORTED_ADDRESS = "reportedAddress";
    static final String NEW_HOST_ID = "newHostId";
    static final String REASON = "reason";
    static final String MAY_RETRY = "mayRetry";
    static final String ACCEPTED = "accepted";
    private static final String MAY_EXCHANGE_TS = "mayExchangeTs";
    private static final String TYPE = "type";
    private static final String PUBLISH_HOSTID = "PUBLISH_HOSTID";
    private static final String REQUEST_HOSTID = "REQUEST_HOSTID";
    static final String HOST_ID = "hostId";
    static final String PORT = "port";
    static final String ADDRESS = "address";
    private static final String VERSION_COMPATIBLE = "versionCompatible";
    private static final String BUILD_STRING = "buildString";
    public  static final String VERSION_STRING = "versionString";

    private static final int MAX_CLOCKSKEW = Integer.getInteger("MAX_CLOCKSKEW", 200);
    private static final int RETRY_INTERVAL = Integer.getInteger("MESH_JOIN_RETRY_INTERVAL", 10);
    private static final int RETRY_INTERVAL_SALT = Integer.getInteger("MESH_JOIN_RETRY_INTERVAL_SALT", 30);
    private static final int CRITICAL_CLOCKSKEW = 100;

    /**
     * Supports quick probes for request host id attempts to seed nodes
     */
    enum ConnectStrategy {
        CONNECT, PROBE
    }

    /**
     * Interface into host messenger to notify it of new connections.
     *
     */
    public interface JoinHandler {
        /*
         * Notify that a specific host has joined with the specified host id.
         */
        public void notifyOfJoin(
                int hostId,
                SocketChannel socket,
                InetSocketAddress listeningAddress,
                JSONObject jo);

        /*
         * A node wants to join the socket mesh
         */
        public void requestJoin(
                SocketChannel socket,
                InetSocketAddress listeningAddress,
                JSONObject jo) throws Exception;

        /*
         * A connection has been made to all of the specified hosts. Invoked by
         * nodes connected to the cluster
         */
        public void notifyOfHosts(
                int yourLocalHostId,
                int hosts[],
                SocketChannel sockets[],
                InetSocketAddress listeningAddresses[],
                Map<Integer, JSONObject> jos) throws Exception;
    }

    private static final VoltLogger LOG = new VoltLogger("JOINER");
    private static final VoltLogger consoleLog = new VoltLogger("CONSOLE");
    private static final VoltLogger hostLog = new VoltLogger("HOST");

    private final ExecutorService m_es = CoreUtils.getSingleThreadExecutor("Socket Joiner");

    InetSocketAddress m_coordIp = null;
    int m_localHostId = 0;

    private final List<ServerSocketChannel> m_listenerSockets = new ArrayList<ServerSocketChannel>();
    private Selector m_selector;
    private final JoinHandler m_joinHandler;

    // from configuration data
    int m_internalPort = 3021;
    String m_internalInterface = "";
    /*
     * The interface we connected to the leader on
     */
    String m_reportedInternalInterface;

    public boolean start(final CountDownLatch externalInitBarrier) {
        boolean retval = false;

        /*
         * probe coordinator host list for leader candidates that may are operational
         * (i.e. node state is operational)
         */
        m_coordIp = null;
        for (String coordHost: m_acceptor.getCoordinators()) {
            if (m_coordIp != null) {
                break;
            }
            HostAndPort host = HostAndPort.fromString(coordHost)
                    .withDefaultPort(org.voltcore.common.Constants.DEFAULT_INTERNAL_PORT);

            InetSocketAddress ip = !host.getHostText().isEmpty() ?
                      new InetSocketAddress(host.getHostText(), host.getPort())
                    : new InetSocketAddress(host.getPort());
            /*
             * On an operational leader (i.e. node is up) the request to join the cluster
             * may be rejected, e.g. multiple hosts rejoining at the same time. In this case,
             * the code will retry.
             */
            long retryInterval = RETRY_INTERVAL;
            final Random salt = new Random();
            while (true) {
                try {
                    connectToPrimary(ip, ConnectStrategy.PROBE);
                    break;
                } catch (CoreUtils.RetryException e) {
                    LOG.warn(String.format("Request to join cluster mesh is rejected, retrying in %d seconds. %s",
                                           retryInterval, e.getMessage()));
                    try {
                        Thread.sleep(TimeUnit.SECONDS.toMillis(retryInterval));
                    } catch (InterruptedException ignoreIt) {
                    }
                    // exponential back off with a salt to avoid collision. Max is 5 minutes.
                    retryInterval = (Math.min(retryInterval * 2, TimeUnit.MINUTES.toSeconds(5)) +
                                     salt.nextInt(RETRY_INTERVAL_SALT));
                } catch (Exception e) {
                    hostLog.error("Failed to establish socket mesh.", e);
                    throw new RuntimeException("Failed to establish socket mesh with " + m_coordIp, e);
                }
            }
        }

        boolean haveMeshedLeader = m_coordIp != null;

        /*
         *  if none were found pick the first one in lexicographical order
         */
        if (m_coordIp == null) {
            HostAndPort leader = m_acceptor.getLeader();
            m_coordIp = !leader.getHostText().isEmpty() ?
                      new InetSocketAddress(leader.getHostText(), leader.getPort())
                    : new InetSocketAddress(leader.getPort());
        }

        if (!haveMeshedLeader && m_coordIp.getPort() == m_internalPort) {
            try {
                hostLog.info("Attempting to bind to leader ip " + m_coordIp);
                ServerSocketChannel listenerSocket = ServerSocketChannel.open();
                listenerSocket.socket().bind(m_coordIp);
                listenerSocket.socket().setPerformancePreferences(0, 2, 1);
                listenerSocket.configureBlocking(false);
                m_listenerSockets.add(listenerSocket);
            }
            catch (IOException e) {
                if (!m_listenerSockets.isEmpty()) {
                    try {
                        m_listenerSockets.get(0).close();
                        m_listenerSockets.clear();
                    }
                    catch (IOException ex) {
                        new VoltLogger(SocketJoiner.class.getName()).l7dlog(Level.FATAL, null, ex);
                    }
                }
            }
        }

        if (!m_listenerSockets.isEmpty()) {
            // if an internal interface was specified, see if it matches any
            // of the forms of the leader address we've bound to.
            if (m_internalInterface != null && !m_internalInterface.equals("")) {
                if (!m_internalInterface.equals(ReverseDNSCache.hostnameOrAddress(m_coordIp.getAddress())) &&
                    !m_internalInterface.equals(m_coordIp.getAddress().getCanonicalHostName()) &&
                    !m_internalInterface.equals(m_coordIp.getAddress().getHostAddress()))
                {
                    String msg = "The provided internal interface (" + m_internalInterface +
                    ") does not match the specified leader address (" +
                     ReverseDNSCache.hostnameOrAddress(m_coordIp.getAddress()) +
                    ", " + m_coordIp.getAddress().getHostAddress() +
                    "). This will result in either a cluster which fails to start" +
                    " or an unintended network topology. The leader will now exit;" +
                    " correct your specified leader and interface and try restarting.";
                    org.voltdb.VoltDB.crashLocalVoltDB(msg, false, null);
                }
            }
            retval = true;
            consoleLog.info("Connecting to VoltDB cluster as the leader...");

            /*
             * Need to wait for external initialization to complete before
             * accepting new connections. This is slang for the leader
             * creating an agreement site that agrees with itself
             */
            m_es.submit(new Callable<Object>() {
                @Override
                public Object call() throws Exception {
                    externalInitBarrier.await();
                    return null;
                }
            });
        }
        else if (!haveMeshedLeader) {
            consoleLog.info("Connecting to the VoltDB cluster leader " + m_coordIp);

            try {
                connectToPrimary(m_coordIp, ConnectStrategy.CONNECT);
            } catch (Exception e) {
                hostLog.error("Failed to establish socket mesh.", e);
                throw new RuntimeException("Failed to establish socket mesh with " + m_coordIp, e);
            }
        }

        /*
         * Submit a task to start the main run loop,
         * will wait for agreement to be initialized if this
         * is the leader using the previously queued runnable
         */
        m_es.submit(new Runnable() {
            @Override
            public void run() {
                try {
                    runPrimary();
                } catch (InterruptedException e) {

                } catch (Throwable e) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Error in socket joiner run loop", true, e);
                }
            }
        });

        return retval;
    }

    /** Set to true when the thread exits correctly. */
    private final boolean success = false;
    private final AtomicBoolean m_paused;
    private final JoinAcceptor m_acceptor;
    public boolean getSuccess() {
        return success;
    }

    public SocketJoiner(
            String internalInterface,
            int internalPort,
            AtomicBoolean isPaused,
            JoinAcceptor acceptor,
            JoinHandler jh) {
        if (internalInterface == null || jh == null || acceptor == null) {
            throw new IllegalArgumentException();
        }
        m_joinHandler = jh;
        m_internalInterface = internalInterface;
        m_internalPort = internalPort;
        m_paused = isPaused;
        m_acceptor = acceptor;
    }

    /*
     * Bind to the internal interface if one was specified,
     * otherwise bind on all interfaces. The leader won't invoke this.
     */
    private void doBind() throws Exception {
        LOG.debug("Creating listener socket");
        try {
            m_selector = Selector.open();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ServerSocketChannel listenerSocket = ServerSocketChannel.open();
        InetSocketAddress inetsockaddr;
        if ((m_internalInterface == null) || (m_internalInterface.length() == 0)) {
            inetsockaddr = new InetSocketAddress(m_internalPort);
        }
        else {
            inetsockaddr = new InetSocketAddress(m_internalInterface, m_internalPort);
        }
        try {
            hostLog.info("Attempting to bind to internal ip " + inetsockaddr);
            listenerSocket.socket().bind(inetsockaddr);
            listenerSocket.configureBlocking(false);
            m_listenerSockets.add(listenerSocket);
        } catch (Exception e) {
            /*
             * If we bound to the leader address, the internal interface address  might not
             * bind if it is all interfaces
             */
            if (m_listenerSockets.isEmpty()) {
                LOG.fatal("Failed to bind to " + inetsockaddr);
                CoreUtils.printPortsInUse(hostLog);
                throw e;
            }
        }

        for (ServerSocketChannel ssc : m_listenerSockets) {
            ssc.register(m_selector, SelectionKey.OP_ACCEPT);
        }

        if (LOG.isDebugEnabled()) {
            LOG.debug("Non-Primary Listening on:" + inetsockaddr.toString());
        }
    }

    /**
     * Read a length prefixed JSON message
     */
    private JSONObject readJSONObjFromWire(SocketChannel sc, String remoteAddressForErrorMsg) throws IOException, JSONException {
        // length prefix
        ByteBuffer lengthBuffer = ByteBuffer.allocate(4);
        while (lengthBuffer.remaining() > 0) {
            int read = sc.read(lengthBuffer);
            if (read == -1) {
                throw new EOFException(remoteAddressForErrorMsg);
            }
        }
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();

        // don't allow for a crazy unallocatable json payload
        if (length > 16 * 1024) {
            throw new IOException(
                    "Length prefix on wire for expected JSON string is greater than 16K max.");
        }
        if (length < 2) {
            throw new IOException(
                    "Length prefix on wire for expected JSON string is less than minimum document size of 2.");
        }

        // content
        ByteBuffer messageBytes = ByteBuffer.allocate(length);
        while (messageBytes.hasRemaining()) {
            int read = sc.read(messageBytes);
            if (read == -1) {
                throw new EOFException(remoteAddressForErrorMsg);
            }
        }
        messageBytes.flip();

        JSONObject jsObj = new JSONObject(new String(messageBytes.array(), StandardCharsets.UTF_8));
        return jsObj;
    }

    /*
     * Pull all ready to accept sockets
     */
    private void processSSC(ServerSocketChannel ssc) throws Exception {
        SocketChannel sc = null;
        while ((sc = ssc.accept()) != null) {
            sc.socket().setTcpNoDelay(true);
            sc.socket().setPerformancePreferences(0, 2, 1);
            final String remoteAddress = sc.socket().getRemoteSocketAddress().toString();

            /*
             * Send the current time over the new connection for a clock skew check
             */
            ByteBuffer currentTimeBuf = ByteBuffer.allocate(8);
            currentTimeBuf.putLong(System.currentTimeMillis());
            currentTimeBuf.flip();
            while (currentTimeBuf.hasRemaining()) {
                sc.write(currentTimeBuf);
            }

            /*
             * Read a length prefixed JSON message
             */
            JSONObject jsObj = readJSONObjFromWire(sc, remoteAddress);

            LOG.info(jsObj.toString(2));

            // get the connecting node's version string
            String remoteBuildString = jsObj.getString(VERSION_STRING);

            VersionChecker versionChecker = m_acceptor.getVersionChecker();
            // send a response with version/build data of this node
            JSONObject returnJs = new JSONObject();
            returnJs.put(VERSION_STRING, versionChecker.getVersionString());
            returnJs.put(BUILD_STRING, versionChecker.getBuildString());
            returnJs.put(VERSION_COMPATIBLE,
                    versionChecker.isCompatibleVersionString(remoteBuildString));

            // inject acceptor fields
            m_acceptor.decorate(returnJs, Optional.of(m_paused.get()));

            byte jsBytes[] = returnJs.toString(4).getBytes(StandardCharsets.UTF_8);

            ByteBuffer returnJsBuffer = ByteBuffer.allocate(4 + jsBytes.length);
            returnJsBuffer.putInt(jsBytes.length);
            returnJsBuffer.put(jsBytes).flip();
            while (returnJsBuffer.hasRemaining()) {
                sc.write(returnJsBuffer);
            }

            /*
             * The type of connection, it can be a new request to join the cluster
             * or a node that is connecting to the rest of the cluster and publishing its
             * host id and such
             */
            String type = jsObj.getString(TYPE);

            /*
             * The new connection may specify the address it is listening on,
             * or it can be derived from the connection itself
             */
            InetSocketAddress listeningAddress;
            if (jsObj.has(ADDRESS)) {
                listeningAddress = new InetSocketAddress(
                        InetAddress.getByName(jsObj.getString(ADDRESS)),
                        jsObj.getInt(PORT));
            } else {
                listeningAddress =
                    new InetSocketAddress(
                            ((InetSocketAddress)sc.socket().
                                    getRemoteSocketAddress()).getAddress().getHostAddress(),
                                    jsObj.getInt(PORT));
            }

            hostLog.info("Received request type " + type);
            if (type.equals(REQUEST_HOSTID)) {
                m_joinHandler.requestJoin( sc, listeningAddress, jsObj);
            } else if (type.equals(PUBLISH_HOSTID)){
                m_joinHandler.notifyOfJoin(jsObj.getInt(HOST_ID), sc, listeningAddress, jsObj);
            } else {
                throw new RuntimeException("Unexpected message type " + type + " from " + remoteAddress);
            }
        }
    }

    /*
     * After startup everything is a primary and can accept
     * new nodes into the cluster. This loop accepts the new socket
     * and passes it off the HostMessenger via the JoinHandler interface
     */
    private void runPrimary() throws Exception {
        try {
            // start the server socket on the right interface
            doBind();

            while (true) {
                try {
                    final int selectedKeyCount = m_selector.select();
                    if (selectedKeyCount == 0) continue;
                    Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
                    try {
                        for (SelectionKey key : selectedKeys) {
                            processSSC((ServerSocketChannel)key.channel());
                        }
                    } finally {
                        selectedKeys.clear();
                    }
                } catch (ClosedByInterruptException e) {
                    throw new InterruptedException();
                } catch (ClosedSelectorException e) {
                    throw new InterruptedException();
                } catch (Exception e) {
                    LOG.error("fault occurrent in the connection accept loop", e);
                }
            }
        }
        finally {
            for (ServerSocketChannel ssc : m_listenerSockets) {
                try {
                    ssc.close();
                } catch (IOException e) {
                }
            }
            m_listenerSockets.clear();
            try {
                m_selector.close();
            } catch (IOException e) {
            }
            m_selector = null;
        }
    }

    /**
     * Read version info from a socket and check compatibility.
     * After verifying versions return if "paused" start is indicated. True if paused start otherwise normal start.
     */
    private JSONObject processJSONResponse(SocketChannel sc,
                                            String remoteAddress,
                                            Set<String> activeVersions) throws IOException, JSONException
    {
        // read the json response from socketjoiner with version info
        JSONObject jsonResponse = readJSONObjFromWire(sc, remoteAddress);
        VersionChecker versionChecker = m_acceptor.getVersionChecker();

        String remoteVersionString = jsonResponse.getString(VERSION_STRING);
        String remoteBuildString = jsonResponse.getString(BUILD_STRING);
        boolean remoteAcceptsLocalVersion = jsonResponse.getBoolean(VERSION_COMPATIBLE);
        if (remoteVersionString.equals(versionChecker.getVersionString())) {
            if (!versionChecker.getBuildString().equals(remoteBuildString)) {
                // ignore test/eclipse build string so tests still work
                if (!versionChecker.getBuildString().equals("VoltDB") && !remoteBuildString.equals("VoltDB")) {
                    org.voltdb.VoltDB.crashLocalVoltDB("For VoltDB version " + versionChecker.getVersionString() +
                            " git tag/hash is not identical across the cluster. Node join failed.\n" +
                            "  joining build string:  " + versionChecker.getBuildString() + "\n" +
                            "  existing build string: " + remoteBuildString, false, null);
                    return null;
                }
            }
        }
        else if (!remoteAcceptsLocalVersion) {
            if (!versionChecker.isCompatibleVersionString(remoteVersionString)) {
                org.voltdb.VoltDB.crashLocalVoltDB("Cluster contains nodes running VoltDB version " + remoteVersionString +
                        " which is incompatibile with local version " + versionChecker.getVersionString() +
                        ".\n", false, null);
                return null;
            }
        }
        //Do this only after we think we are compatible.
        activeVersions.add(remoteVersionString);
        return jsonResponse;
    }

    /*
     * If this node failed to bind to the leader address
     * it must connect to the leader which will generate a host id and
     * advertise the rest of the cluster so that connectToPrimary can connect to it
     */
    private void connectToPrimary(InetSocketAddress coordIp, ConnectStrategy mode) throws Exception {
        // collect clock skews from all nodes
        List<Long> skews = new ArrayList<Long>();

        // collect the set of active voltdb version strings in the cluster
        // this is used to limit simulatanious versions to two
        Set<String> activeVersions = new TreeSet<String>();

        SocketChannel socket = null;
        try {
            LOG.debug("Non-Primary Starting & Connecting to Primary");
            int connectAttempts = 0;
            while (socket == null) {
                try {
                    socket = SocketChannel.open(coordIp);
                }
                catch (java.net.ConnectException
                      |java.nio.channels.UnresolvedAddressException
                      |java.net.NoRouteToHostException
                      |java.net.PortUnreachableException e)
                {
                    if (mode == ConnectStrategy.PROBE) {
                        return;
                    }
                    if (connectAttempts >= 8) {
                        LOG.warn("Joining primary failed: " + e.getMessage() + " retrying..");
                    }
                    try {
                        Thread.sleep(250); //  milliseconds
                    }
                    catch (InterruptedException ex) {
                        // don't really care.
                    }
                }
                ++connectAttempts;
            }

            if (!coordIp.equals(m_coordIp)) {
                m_coordIp = coordIp;
            }

            socket.socket().setTcpNoDelay(true);
            socket.socket().setPerformancePreferences(0, 2, 1);

            final String primaryAddress = socket.socket().getRemoteSocketAddress().toString();

            // Read the timestamp off the wire and calculate skew for this connection
            ByteBuffer currentTimeBuf = ByteBuffer.allocate(8);
            while (currentTimeBuf.hasRemaining()) {
                socket.read(currentTimeBuf);
            }
            currentTimeBuf.flip();
            long skew = System.currentTimeMillis() - currentTimeBuf.getLong();
            skews.add(skew);

            VersionChecker versionChecker = m_acceptor.getVersionChecker();
            activeVersions.add(versionChecker.getVersionString());

            JSONObject jsObj = new JSONObject();
            jsObj.put(TYPE, REQUEST_HOSTID);

            // put the version compatibility status in the json
            jsObj.put(VERSION_STRING, versionChecker.getVersionString());

            /*
             * Advertise the port we are going to listen on based on
             * config
             */
            jsObj.put(PORT, m_internalPort);

            /*
             * If config specified an internal interface use that.
             * Otherwise the leader will echo back what we connected on
             */
            if (!m_internalInterface.isEmpty()) {
                jsObj.put(ADDRESS, m_internalInterface);
            }
            /*
             * communicate configuration and node state
             */
            m_acceptor.decorate(jsObj, Optional.empty());
            jsObj.put(MAY_EXCHANGE_TS, true);

            byte jsBytes[] = jsObj.toString(4).getBytes(StandardCharsets.UTF_8);
            ByteBuffer requestHostIdBuffer = ByteBuffer.allocate(4 + jsBytes.length);
            requestHostIdBuffer.putInt(jsBytes.length);
            requestHostIdBuffer.put(jsBytes).flip();
            while (requestHostIdBuffer.hasRemaining()) {
                socket.write(requestHostIdBuffer);
            }

            ImmutableMap.Builder<Integer, JSONObject> cmbld = ImmutableMap.builder();

            // read the json response from socketjoiner with version info and validate it
            JSONObject leaderInfo = processJSONResponse(socket, primaryAddress, activeVersions);

            // read the json response sent by HostMessenger with HostID
            JSONObject jsonObj = readJSONObjFromWire(socket, primaryAddress);

            // check if the membership request is accepted
            if (!jsonObj.optBoolean(ACCEPTED, true)) {
                socket.close();
                if (!jsonObj.optBoolean(MAY_RETRY, false)) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Request to join cluster is rejected: "
                            + jsonObj.optString(REASON, "rejection reason is not available"));
                }
                throw new CoreUtils.RetryException(jsonObj.optString(REASON, "rejection reason is not available"));
            }

            /*
             * Get the generated host id, and the interface we connected on
             * that was echoed back
             */
            m_localHostId = jsonObj.getInt(NEW_HOST_ID);
            m_reportedInternalInterface = jsonObj.getString(REPORTED_ADDRESS);

            cmbld.put(m_localHostId, m_acceptor.decorate(jsonObj, Optional.<Boolean>empty()));

            /*
             * Loop over all the hosts and create a connection (except for the first entry, that is the leader)
             * and publish the host id that was generated. This finishes creating the mesh
             */
            JSONArray otherHosts = jsonObj.getJSONArray(HOSTS);
            int hostIds[] = new int[otherHosts.length()];
            SocketChannel hostSockets[] = new SocketChannel[hostIds.length];
            InetSocketAddress listeningAddresses[] = new InetSocketAddress[hostIds.length];

            for (int ii = 0; ii < otherHosts.length(); ii++) {
                JSONObject host = otherHosts.getJSONObject(ii);
                String address = host.getString(ADDRESS);
                int port = host.getInt(PORT);
                final int hostId = host.getInt(HOST_ID);


                LOG.info("Leader provided address " + address + ":" + port);
                InetSocketAddress hostAddr = new InetSocketAddress(address, port);
                if (ii == 0) {
                    //Leader already has a socket
                    hostIds[ii] = hostId;
                    listeningAddresses[ii] = hostAddr;
                    hostSockets[ii] = socket;
                    cmbld.put(ii,leaderInfo);
                    continue;
                }

                SocketChannel hostSocket = null;
                while (hostSocket == null) {
                    try {
                        hostSocket = SocketChannel.open(hostAddr);
                    }
                    catch (java.net.ConnectException e) {
                        LOG.warn("Joining host failed: " + e.getMessage() + " retrying..");
                        try {
                            Thread.sleep(250); //  milliseconds
                        }
                        catch (InterruptedException ex) {
                            // don't really care.
                        }
                    }
                }

                final String remoteAddress = hostSocket.socket().getRemoteSocketAddress().toString();

                /*
                 * Get the clock skew value
                 */
                currentTimeBuf.clear();
                while (currentTimeBuf.hasRemaining()) {
                    hostSocket.read(currentTimeBuf);
                }
                currentTimeBuf.flip();
                skew = System.currentTimeMillis() - currentTimeBuf.getLong();
                assert(currentTimeBuf.remaining() == 0);
                skews.add(skew);

                jsObj = new JSONObject();
                jsObj.put(TYPE, PUBLISH_HOSTID);
                jsObj.put(HOST_ID, m_localHostId);
                jsObj.put(PORT, m_internalPort);
                jsObj.put(
                        ADDRESS,
                        m_internalInterface.isEmpty() ? m_reportedInternalInterface : m_internalInterface);
                jsObj.put(VERSION_STRING, versionChecker.getVersionString());

                m_acceptor.decorate(jsObj, Optional.empty());
                jsObj.put(MAY_EXCHANGE_TS, true);

                jsBytes = jsObj.toString(4).getBytes(StandardCharsets.UTF_8);
                ByteBuffer pushHostId = ByteBuffer.allocate(4 + jsBytes.length);
                pushHostId.putInt(jsBytes.length);
                pushHostId.put(jsBytes).flip();
                while (pushHostId.hasRemaining()) {
                    hostSocket.write(pushHostId);
                }
                hostIds[ii] = hostId;
                hostSockets[ii] = hostSocket;
                listeningAddresses[ii] = hostAddr;

                // read the json response from socketjoiner with version info and validate it
                JSONObject hostInfo = processJSONResponse(hostSocket, remoteAddress, activeVersions);
                cmbld.put(ii, hostInfo);
            }

            checkClockSkew(skews);

            /*
             * Limit the number of active versions to 2.
             */
            if (activeVersions.size() > 2) {
                String versions = "";
                // get the list of non-local versions
                for (String version : activeVersions) {
                    if (!version.equals(versionChecker.getVersionString())) {
                        versions += version + ", ";
                    }
                }
                // trim the trailing comma + space
                versions = versions.substring(0, versions.length() - 2);

                org.voltdb.VoltDB.crashLocalVoltDB("Cluster already is running mixed voltdb versions (" + versions +").\n" +
                                        "Adding version " + versionChecker.getVersionString() + " would add a third version.\n" +
                                        "VoltDB hotfix support supports only two unique versions simulaniously.", false, null);
            }

            /*
             * Notify the leader that we connected to the entire cluster, it will then go
             * and queue a txn for our agreement site to join the cluster
             */
            ByteBuffer joinCompleteBuffer = ByteBuffer.allocate(1);
            while (joinCompleteBuffer.hasRemaining()) {
                hostSockets[0].write(joinCompleteBuffer);
            }

            /*
             * Let host messenger know about the connections.
             * It will init the agreement site and then we are done.
             */
            m_joinHandler.notifyOfHosts( m_localHostId, hostIds, hostSockets, listeningAddresses, cmbld.build());
        } catch (ClosedByInterruptException e) {
            //This is how shutdown is done
        }
    }

    private static void checkClockSkew(List<Long> skews)
    {
        long maxSkew = Collections.max(skews);
        long minSkew = Collections.min(skews);
        long overallSkew = maxSkew - minSkew;
        if (maxSkew > 0 && minSkew > 0) {
            overallSkew = maxSkew;
        } else if (maxSkew < 0 && minSkew < 0) {
            overallSkew = Math.abs(minSkew);
        }

        if (overallSkew > MAX_CLOCKSKEW) {
            org.voltdb.VoltDB.crashLocalVoltDB("Clock skew is " + overallSkew +
                    " which is > than the " + MAX_CLOCKSKEW + " millisecond limit. Make sure NTP is running.", false, null);
        } else if (overallSkew > CRITICAL_CLOCKSKEW) {
            final String msg = "Clock skew is " + overallSkew +
                    " which is high. Ideally it should be sub-millisecond. Make sure NTP is running.";
            hostLog.warn(msg);
            consoleLog.warn(msg);
        } else {
            hostLog.info("Clock skew to across all nodes in the cluster is " + overallSkew);
        }
    }

    public void shutdown() throws InterruptedException {
        if (m_selector != null) {
            try {
                m_selector.close();
            } catch (IOException e) {
            }
        }
        m_es.shutdownNow();
        m_es.awaitTermination(356, TimeUnit.DAYS);
        for (ServerSocketChannel ssc : m_listenerSockets) {
            try {
                ssc.close();
            } catch (IOException e) {
            }
        }
        m_listenerSockets.clear();
        if (m_selector != null) {
            try {
                m_selector.close();
            } catch (IOException e) {
            }
            m_selector = null;
        }
    }

    int getLocalHostId() {
        return m_localHostId;
    }
}
