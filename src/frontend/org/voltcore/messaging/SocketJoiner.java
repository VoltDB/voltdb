/* This file is part of VoltDB.
 * Copyright (C) 2008-2019 VoltDB Inc.
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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
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

import javax.net.ssl.SSLEngine;

import org.json_voltpatches.JSONArray;
import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.Level;
import org.voltcore.logging.VoltLogger;
import org.voltcore.network.ReverseDNSCache;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.VersionChecker;
import org.voltcore.utils.ssl.MessagingChannel;
import org.voltcore.utils.ssl.SSLConfiguration;
import org.voltdb.client.TLSHandshaker;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ImmutableMap;
import com.google_voltpatches.common.collect.ImmutableSet;
import com.google_voltpatches.common.collect.Sets;
import com.google_voltpatches.common.net.HostAndPort;

import io.netty.buffer.ByteBufAllocator;
import io.netty.handler.ssl.SslContext;

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

    public static final String FAIL_ESTABLISH_MESH_MSG = "Failed to establish socket mesh.";
    /**
     * Supports quick probes for request host id attempts to seed nodes
     */
    enum ConnectStrategy {
        CONNECT, PROBE
    }

    enum ConnectionType {
        REQUEST_HOSTID,
        PUBLISH_HOSTID,
        REQUEST_CONNECTION;
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
                SSLEngine sslEngine,
                InetSocketAddress listeningAddress,
                JSONObject jo);

        /*
         * A node wants to join the socket mesh
         */
        public void requestJoin(
                SocketChannel socket,
                SSLEngine sslEngine,
                MessagingChannel messagingChannel,
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
                SSLEngine[] sslEngines,
                InetSocketAddress listeningAddresses[],
                Map<Integer, JSONObject> jos) throws Exception;

        /*
         * Create new connection between given node and current node
         */
        public void notifyOfConnection(
                int hostId,
                SocketChannel socket,
                SSLEngine sslEngine,
                InetSocketAddress listeningAddress) throws Exception;
    }

    private static class RequestHostIdResponse {
        final private JSONObject m_leaderInfo;
        final private JSONObject m_responseBody;

        public RequestHostIdResponse(JSONObject leaderInfo, JSONObject responseBody) {
            m_leaderInfo = leaderInfo;
            m_responseBody = responseBody;
        }
        JSONObject getLeaderInfo() { return m_leaderInfo; }
        JSONObject getResponseBody() { return m_responseBody; }
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

            InetSocketAddress ip = !host.getHost().isEmpty() ?
                      new InetSocketAddress(host.getHost(), host.getPort())
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

                    //Over waiting may occur in some cases.
                    //For example, there are 4 rejoining nodes. Node 1 may take over 5 min to be completed.
                    //Nodes 2 to 4 continue to wait after they detect that node 1 is still rejoining right before its rejoining is completed
                    //They will wait 5 min + salt before sending another rejoining request. All the following rejoining requests are sent
                    //after 5 min + salt. Reset waiting time to avoid over waiting.
                    if (retryInterval > TimeUnit.MINUTES.toSeconds(5)) {
                        retryInterval = RETRY_INTERVAL;
                    }
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
            m_coordIp = !leader.getHost().isEmpty() ?
                      new InetSocketAddress(leader.getHost(), leader.getPort())
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
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            String.format("The provided internal interface (%s) does not match the "
                                    + "specified leader address (%s, %s). "
                                    + "This will result in either a cluster which fails to start or an unintended network topology. "
                                    + "The leader will now exit; correct your specified leader and interface and try restarting.",
                                    m_internalInterface,
                                    ReverseDNSCache.hostnameOrAddress(m_coordIp.getAddress()),
                                    m_coordIp.getAddress().getHostAddress()),
                            false, null);
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
                hostLog.error(FAIL_ESTABLISH_MESH_MSG, e);
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
    private final SslContext m_sslServerContext;
    private final SslContext m_sslClientContext;
    public boolean getSuccess() {
        return success;
    }

    public SocketJoiner(
            String internalInterface,
            int internalPort,
            AtomicBoolean isPaused,
            JoinAcceptor acceptor,
            JoinHandler jh,
            SslContext sslServerContext, SslContext sslClientContext) {
        if (internalInterface == null || jh == null || acceptor == null) {
            throw new IllegalArgumentException();
        }
        m_joinHandler = jh;
        m_internalInterface = internalInterface;
        m_internalPort = internalPort;
        m_paused = isPaused;
        m_acceptor = acceptor;
        m_sslServerContext = sslServerContext;
        m_sslClientContext = sslClientContext;
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
                MiscUtils.printPortsInUse(hostLog);
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
    private JSONObject readJSONObjFromWire(MessagingChannel messagingChannel) throws IOException, JSONException {
        ByteBuffer messageBytes = messagingChannel.readMessage();

        JSONObject jsObj = new JSONObject(new String(messageBytes.array(), StandardCharsets.UTF_8));
        return jsObj;
    }

    /**
     * Initialize a new {@link SocketChannel} either as a client or server
     *
     * @return {@link SslHandshakeResult} instance. Never {@code null}
     */
    private SslHandshakeResult initializeSocket(SocketChannel sc, boolean clientMode, List<Long> clockSkews)
            throws IOException {
        ByteBuffer timeBuffer = ByteBuffer.allocate(Long.BYTES);
        if (clientMode) {
            synchronized (sc.blockingLock()) {
                boolean isBlocking = sc.isBlocking();
                // Just being lazy and using blocking mode here to get the server's current timestamp
                sc.configureBlocking(true);
                do {
                    sc.read(timeBuffer);
                } while (timeBuffer.hasRemaining());
                sc.configureBlocking(isBlocking);
            }
            if (clockSkews != null) {
                clockSkews.add(System.currentTimeMillis() - ((ByteBuffer) timeBuffer.flip()).getLong());
            }
        } else {
            timeBuffer.putLong(System.currentTimeMillis());
            timeBuffer.flip();
            do {
                sc.write(timeBuffer);
            } while (timeBuffer.hasRemaining());
        }
        return setupSSLIfNeeded(sc, clientMode);
    }

    private SslHandshakeResult setupSSLIfNeeded(SocketChannel sc, boolean clientMode) throws IOException {
        SslContext sslContext = clientMode ? m_sslClientContext : m_sslServerContext;
        if (sslContext == null) {
            return SslHandshakeResult.NO_SSL;
        }
        SSLEngine sslEngine = sslContext.newEngine(ByteBufAllocator.DEFAULT);
        sslEngine.setUseClientMode(clientMode);
        sslEngine.setNeedClientAuth(false);

        Set<String> enabled = ImmutableSet.copyOf(sslEngine.getEnabledCipherSuites());
        Set<String> intersection = Sets.intersection(SSLConfiguration.PREFERRED_CIPHERS, enabled);
        if (intersection.isEmpty()) {
            hostLog.warn("Preferred cipher suites are not available");
            intersection = enabled;
        }
        sslEngine.setEnabledCipherSuites(intersection.toArray(new String[intersection.size()]));
        boolean handshakeStatus;

        sc.socket().setTcpNoDelay(true);
        TLSHandshaker handshaker = new TLSHandshaker(sc, sslEngine);
        handshakeStatus = handshaker.handshake();

        if (!handshakeStatus) {
            throw new IOException("Rejected accepting new internal connection, SSL handshake failed.");
        }
        LOG.info("SSL enabled on internal connection " + sc.socket().getRemoteSocketAddress() +
                " with protocol " + sslEngine.getSession().getProtocol() + " and with cipher " + sslEngine.getSession().getCipherSuite());
        return new SslHandshakeResult(handshaker);
    }

    /*
     * Pull all ready to accept sockets
     */
    private void processSSC(ServerSocketChannel ssc) throws Exception {
        SocketChannel sc = null;
        while ((sc = ssc.accept()) != null) {
            boolean success = false;
            try {
                sc.socket().setTcpNoDelay(true);
                sc.socket().setPerformancePreferences(0, 2, 1);
                SslHandshakeResult result = initializeSocket(sc, false, null);
                SSLEngine sslEngine = result.m_sslEngine;
                final String remoteAddress = sc.socket().getRemoteSocketAddress().toString();

                MessagingChannel messagingChannel = MessagingChannel.get(sc, sslEngine);

                /*
                 * Read a length prefixed JSON message
                 */
                JSONObject jsObj;
                if (result.m_remnant != null) {
                    assert result.m_remnant.getInt() == result.m_remnant.remaining()
                            && result.m_remnant.hasArray() : "Remnant not array or not a single full message. remnant: "
                                    + result.m_remnant + ", expected length: "
                                    + result.m_remnant.getInt(result.m_remnant.position() - Integer.BYTES);

                    jsObj = new JSONObject(new String(result.m_remnant.array(),
                            result.m_remnant.arrayOffset() + result.m_remnant.position(), result.m_remnant.remaining(),
                            StandardCharsets.UTF_8));
                } else {
                    jsObj = readJSONObjFromWire(messagingChannel);
                }

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
                returnJs = m_acceptor.decorate(returnJs, Optional.of(m_paused.get()));
                byte jsBytes[] = returnJs.toString(4).getBytes(StandardCharsets.UTF_8);

                ByteBuffer returnJsBuffer = ByteBuffer.allocate(4 + jsBytes.length);
                returnJsBuffer.putInt(jsBytes.length);
                returnJsBuffer.put(jsBytes).flip();
                messagingChannel.writeMessage(returnJsBuffer);

                /*
                 * The type of connection, it can be a new request to join the cluster
                 * or a node that is connecting to the rest of the cluster and publishing its
                 * host id or a request to add a new connection to the request node.
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
                if (type.equals(ConnectionType.REQUEST_HOSTID.name())) {
                    m_joinHandler.requestJoin(sc, sslEngine, messagingChannel, listeningAddress, jsObj);
                } else if (type.equals(ConnectionType.PUBLISH_HOSTID.name())){
                    m_joinHandler.notifyOfJoin(jsObj.getInt(HOST_ID), sc, sslEngine, listeningAddress, jsObj);
                } else if (type.equals(ConnectionType.REQUEST_CONNECTION.name())) {
                    m_joinHandler.notifyOfConnection(jsObj.getInt(HOST_ID), sc, sslEngine, listeningAddress);
                } else {
                    throw new RuntimeException("Unexpected message type " + type + " from " + remoteAddress);
                }
                success = true;
            } catch (IOException e) {
                LOG.info("IOException occurred while handling new client connection " + sc
                        + ". Client will most likely retry: " + e);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("", e);
                }
            } finally {
                // do not leak sockets when exception happens
                if (!success) {
                    try {
                        sc.close();
                    } catch (IOException ioex) {
                        // ignore the close exception on purpose
                    }
                }
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
                    if (selectedKeyCount == 0) {
                        continue;
                    }
                    Set<SelectionKey> selectedKeys = m_selector.selectedKeys();
                    try {
                        for (SelectionKey key : selectedKeys) {
                            processSSC((ServerSocketChannel)key.channel());
                        }
                    } finally {
                        selectedKeys.clear();
                    }
                } catch (ClosedByInterruptException | ClosedSelectorException e) {
                    throw new InterruptedException();
                } catch (Exception e) {
                    LOG.error("Exception occurred in the connection accept loop", e);
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
    private JSONObject processJSONResponse(MessagingChannel messagingChannel,
                                            Set<String> activeVersions,
                                            boolean checkVersion) throws IOException, JSONException
    {
        // read the json response from socketjoiner with version info
        JSONObject jsonResponse = readJSONObjFromWire(messagingChannel);
        if (!checkVersion) {
            return jsonResponse;
        }

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

    /**
     * Create socket to the leader node
     */
    private SocketChannel createLeaderSocket(
            SocketAddress hostAddr,
            ConnectStrategy mode) throws IOException
    {
        SocketChannel socket;
        int connectAttempts = 0;
        do {
            try {
                socket = SocketChannel.open();
                socket.socket().connect(hostAddr, 5000);
            }
            catch (java.net.ConnectException
                  |java.nio.channels.UnresolvedAddressException
                  |java.net.NoRouteToHostException
                  |java.net.PortUnreachableException e)
            {
                // reset the socket to null for loop purposes
                socket = null;

                if (mode == ConnectStrategy.PROBE) {
                    return null;
                }
                if ((++connectAttempts % 8) == 0) {
                    LOG.warn("Joining primary failed: " + e + " retrying..");
                }
                try {
                    Thread.sleep(250); //  milliseconds
                }
                catch (InterruptedException dontcare) {}
            }
        } while (socket == null);
        return socket;
    }

    /**
     * Create socket to the given host
     */
    private SocketChannel connectToHost(SocketAddress hostAddr)
            throws IOException
    {
        SocketChannel socket = null;
        while (socket == null) {
            try {
                socket = SocketChannel.open(hostAddr);
            }
            catch (java.net.ConnectException e) {
                LOG.warn("Joining host failed: " + e.getMessage() + " retrying..");
                try {
                    Thread.sleep(250); //  milliseconds
                }
                catch (InterruptedException dontcare) {}
            }
        }
        return socket;
    }

    /**
     * Connection handshake to the leader, ask the leader to assign a host Id
     * for current node.
     * @param
     * @return array of two JSON objects, first is leader info, second is
     *         the response to our request
     * @throws Exception
     */
    private RequestHostIdResponse requestHostId (
            MessagingChannel messagingChannel,
            Set<String> activeVersions) throws Exception
    {
        VersionChecker versionChecker = m_acceptor.getVersionChecker();
        activeVersions.add(versionChecker.getVersionString());

        JSONObject jsObj = new JSONObject();
        jsObj.put(TYPE, ConnectionType.REQUEST_HOSTID.name());

        // put the version compatibility status in the json
        jsObj.put(VERSION_STRING, versionChecker.getVersionString());

        // Advertise the port we are going to listen on based on config
        jsObj.put(PORT, m_internalPort);

        // If config specified an internal interface use that.
        // Otherwise the leader will echo back what we connected on
        if (!m_internalInterface.isEmpty()) {
            jsObj.put(ADDRESS, m_internalInterface);
        }

        // communicate configuration and node state
        jsObj = m_acceptor.decorate(jsObj, Optional.empty());
        jsObj.put(MAY_EXCHANGE_TS, true);

        byte jsBytes[] = jsObj.toString(4).getBytes(StandardCharsets.UTF_8);
        ByteBuffer requestHostIdBuffer = ByteBuffer.allocate(4 + jsBytes.length);
        requestHostIdBuffer.putInt(jsBytes.length);
        requestHostIdBuffer.put(jsBytes).flip();
        messagingChannel.writeMessage(requestHostIdBuffer);

        // read the json response from socketjoiner with version info and validate it
        JSONObject leaderInfo = processJSONResponse(messagingChannel, activeVersions, true);
        // read the json response sent by HostMessenger with HostID
        JSONObject jsonObj = readJSONObjFromWire(messagingChannel);

        return new RequestHostIdResponse(leaderInfo, jsonObj);
    }

    /**
     * Connection handshake to non-leader node, broadcast the new hostId to each node of the
     * cluster (except the leader).
     * @param
     * @return JSONObject response message from peer node
     * @throws Exception
     */
    private JSONObject publishHostId(
            InetSocketAddress hostAddr,
            MessagingChannel messagingChannel,
            Set<String> activeVersions) throws Exception
    {
        JSONObject jsObj = new JSONObject();
        jsObj.put(TYPE, ConnectionType.PUBLISH_HOSTID.name());
        jsObj.put(HOST_ID, m_localHostId);
        jsObj.put(PORT, m_internalPort);
        jsObj.put(ADDRESS,
                m_internalInterface.isEmpty() ? m_reportedInternalInterface : m_internalInterface);
        jsObj.put(VERSION_STRING, m_acceptor.getVersionChecker().getVersionString());

        jsObj = m_acceptor.decorate(jsObj, Optional.empty());
        jsObj.put(MAY_EXCHANGE_TS, true);

        byte[] jsBytes = jsObj.toString(4).getBytes(StandardCharsets.UTF_8);
        ByteBuffer pushHostId = ByteBuffer.allocate(4 + jsBytes.length);
        pushHostId.putInt(jsBytes.length);
        pushHostId.put(jsBytes).flip();
        messagingChannel.writeMessage(pushHostId);

        // read the json response from socketjoiner with version info and validate it
        return processJSONResponse(messagingChannel, activeVersions, true);
    }

    static class SocketInfo {
        public final SocketChannel m_socket;
        public final SSLEngine m_sslEngine;

        public SocketInfo(SocketChannel socket, SSLEngine sslEngine) {
            m_socket = socket;
            m_sslEngine = sslEngine;
        }
    }
    public SocketInfo requestForConnection(InetSocketAddress hostAddr, int hostId) throws IOException, JSONException
    {
        SocketChannel socket = connectToHost(hostAddr);
        SSLEngine sslEngine;
        try {
            sslEngine = initializeSocket(socket, true, null).m_sslEngine;
        } catch(IOException e) {
            try {
                socket.close();
            } catch(IOException t) { }
            throw new IOException("SSL setup to " + socket.getRemoteAddress() + " failed", e);
        }
        MessagingChannel messagingChannel = MessagingChannel.get(socket, sslEngine);

        JSONObject jsObj = new JSONObject();
        jsObj.put(TYPE, ConnectionType.REQUEST_CONNECTION.name());
        jsObj.put(VERSION_STRING, m_acceptor.getVersionChecker().getVersionString());
        jsObj.put(HOST_ID, m_localHostId);
        jsObj.put(PORT, m_internalPort);
        jsObj.put(ADDRESS,
                m_internalInterface.isEmpty() ? m_reportedInternalInterface : m_internalInterface);
        byte[] jsBytes = jsObj.toString(4).getBytes(StandardCharsets.UTF_8);
        ByteBuffer addConnection = ByteBuffer.allocate(4 + jsBytes.length);
        addConnection.putInt(jsBytes.length);
        addConnection.put(jsBytes).flip();
        messagingChannel.writeMessage(addConnection);
        // read the json response from socketjoiner with version info and validate it
        processJSONResponse(messagingChannel, null, false);
        return new SocketInfo(socket, sslEngine);
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

        try {
            LOG.debug("Non-Primary Starting & Connecting to Primary: " + coordIp + " in mode: " + mode);
            SocketChannel socket = createLeaderSocket(coordIp, mode);
            if (socket == null)
             {
                return; // in probe mode
            }
            socket.socket().setTcpNoDelay(true);
            socket.socket().setPerformancePreferences(0, 2, 1);
            SSLEngine leaderSSLEngine;
            try {
                leaderSSLEngine = initializeSocket(socket, true, skews).m_sslEngine;
            } catch(IOException e) {
                SocketAddress socketAddress = socket.getRemoteAddress();
                try {
                    socket.close();
                } catch(IOException t) { }
                throw new IOException("SSL setup to " + socketAddress + " failed", e);
            }
            MessagingChannel leaderChannel = MessagingChannel.get(socket, leaderSSLEngine);
            if (!coordIp.equals(m_coordIp)) {
                m_coordIp = coordIp;
            }

            // blocking call, send a request to the leader node and get a host id assigned by the leader
            RequestHostIdResponse response = requestHostId(leaderChannel, activeVersions);
            // check if the membership request is accepted
            JSONObject responseBody = response.getResponseBody();
            if (!responseBody.optBoolean(ACCEPTED, true)) {
                socket.close();
                if (!responseBody.optBoolean(MAY_RETRY, false)) {
                    org.voltdb.VoltDB.crashLocalVoltDB(
                            "Request to join cluster is rejected: "
                            + responseBody.optString(REASON, "rejection reason is not available"));
                }
                throw new CoreUtils.RetryException(responseBody.optString(REASON, "rejection reason is not available"));
            }

            /*
             * Get the generated host id, and the interface we connected on
             * that was echoed back
             */
            m_localHostId = responseBody.getInt(NEW_HOST_ID);
            m_reportedInternalInterface = responseBody.getString(REPORTED_ADDRESS);

            ImmutableMap.Builder<Integer, JSONObject> cmbld = ImmutableMap.builder();
            cmbld.put(m_localHostId, m_acceptor.decorate(responseBody, Optional.<Boolean>empty()));

            /*
             * Loop over all the hosts and create a connection (except for the first entry, that is the leader)
             * and publish the host id that was generated. This finishes creating the mesh
             */
            JSONArray otherHosts = responseBody.getJSONArray(HOSTS);
            int hostIds[] = new int[otherHosts.length()];
            SocketChannel hostSockets[] = new SocketChannel[hostIds.length];
            SSLEngine sslEngines[] = new SSLEngine[hostIds.length];
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
                    sslEngines[ii] = leaderSSLEngine;
                    cmbld.put(ii, response.getLeaderInfo());
                    continue;
                }
                // connect to all the peer hosts (except leader) and advertise our existence
                SocketChannel hostSocket = connectToHost(hostAddr);
                SSLEngine sslEngine = initializeSocket(hostSocket, true, skews).m_sslEngine;
                MessagingChannel messagingChannel = MessagingChannel.get(hostSocket, sslEngine);
                JSONObject hostInfo = publishHostId(hostAddr, messagingChannel, activeVersions);

                hostIds[ii] = hostId;
                hostSockets[ii] = hostSocket;
                sslEngines[ii] = sslEngine;
                listeningAddresses[ii] = hostAddr;

                cmbld.put(ii, hostInfo);
            }

            /*
             * The max difference of clock skew cannot exceed MAX_CLOCKSKEW, and the number of
             * active versions in the cluster cannot be more than 2.
             */
            checkClockSkew(skews);
            checkActiveVersions(activeVersions, m_acceptor.getVersionChecker().getVersionString());

            /*
             * Notify the leader that we connected to the entire cluster, it will then go
             * and queue a txn for our agreement site to join the cluster
             */
            ByteBuffer joinCompleteBuffer = ByteBuffer.allocate(1);
            // No need to encrypt this one byte
            while (joinCompleteBuffer.hasRemaining()) {
                hostSockets[0].write(joinCompleteBuffer);
            }

            /*
             * Let host messenger know about the connections.
             * It will init the agreement site and then we are done.
             */
            m_joinHandler.notifyOfHosts( m_localHostId, hostIds, hostSockets, sslEngines, listeningAddresses, cmbld.build());
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

    private static void checkActiveVersions(Set<String> activeVersions, String localVersion) {
        /*
         * Limit the number of active versions to 2.
         */
        if (activeVersions.size() > 2) {
            String versions = "";
            // get the list of non-local versions
            for (String version : activeVersions) {
                if (!version.equals(localVersion)) {
                    versions += version + ", ";
                }
            }
            // trim the trailing comma + space
            versions = versions.substring(0, versions.length() - 2);

            org.voltdb.VoltDB.crashLocalVoltDB("Cluster already is running mixed voltdb versions (" + versions +").\n" +
                                    "Adding version " + localVersion + " would add a third version.\n" +
                                    "VoltDB hotfix support supports only two unique versions simulaniously.", false, null);
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

    /**
     * Simple class to hold the result of an ssl handshake
     */
    private static final class SslHandshakeResult {
        static final SslHandshakeResult NO_SSL = new SslHandshakeResult(null, null);

        /** {@link SSLEngine} used for the handshake or {@code null} if no handshake was performed */
        final SSLEngine m_sslEngine;
        /** Extra data read and decrypted during the handshake or {@code null} if no extra data was read */
        final ByteBuffer m_remnant;

        private SslHandshakeResult(SSLEngine sslEngine, ByteBuffer remnant) {
            m_sslEngine = sslEngine;
            m_remnant = remnant;
        }

        public SslHandshakeResult(TLSHandshaker handshaker) throws IOException {
            m_sslEngine = handshaker.getSslEngine();
            m_remnant = handshaker.hasRemnant() ? handshaker.getRemnant() : null;
        }
    }
}
