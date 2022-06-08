/* This file is part of VoltDB.
 * Copyright (C) 2008-2022 Volt Active Data Inc.
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

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
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

import org.json_voltpatches.JSONException;
import org.json_voltpatches.JSONObject;
import org.voltcore.logging.VoltLogger;
import org.voltcore.messaging.messages.SocketJoinerMessageBase;
import org.voltcore.messaging.messages.HostInformation;
import org.voltcore.messaging.messages.PublishHostIdRequest;
import org.voltcore.messaging.messages.RequestForConnectionRequest;
import org.voltcore.messaging.messages.RequestHostIdRequest;
import org.voltcore.messaging.messages.RequestJoinResponse;
import org.voltcore.messaging.messages.SocketJoinerMessageParser;
import org.voltcore.messaging.messages.VersionBuildMessage;
import org.voltcore.network.ReverseDNSCache;
import org.voltcore.utils.CoreUtils;
import org.voltcore.utils.VersionChecker;
import org.voltcore.utils.ssl.MessagingChannel;
import org.voltdb.client.TLSHandshaker;
import org.voltdb.common.Constants;
import org.voltdb.utils.MiscUtils;

import com.google_voltpatches.common.collect.ImmutableMap;
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
    static final String MAY_EXCHANGE_TS = "mayExchangeTs";
    public static final String VERSION_STRING = "versionString";

    private static final int MAX_CLOCKSKEW = Integer.getInteger("MAX_CLOCKSKEW", 200);
    private static final int RETRY_INTERVAL = Integer.getInteger("MESH_JOIN_RETRY_INTERVAL", 10);
    private static final int RETRY_INTERVAL_SALT = Integer.getInteger("MESH_JOIN_RETRY_INTERVAL_SALT", 30);
    private static final int CRITICAL_CLOCKSKEW = 100;
    private static final int NAME_LOOKUP_RETRY_MS = 1000;
    private static final int NAME_LOOKUP_RETRY_LIMIT = 10;
    private static final int SOCKET_CONNECT_RETRY_MS = 250;
    private static final int LOG_RATE_LIMIT = 30;

    public static final String FAIL_ESTABLISH_MESH_MSG = "Failed to establish socket mesh.";

    private final String m_hostDisplayName;

    public enum ConnectionType {
        REQUEST_HOSTID,
        PUBLISH_HOSTID,
        REQUEST_CONNECTION;
    }

    /**
     * Exception for wrapping retryable connect failures
     */
    private static final class SocketRetryException extends Exception {
        private static final long serialVersionUID = 1L;
        public SocketRetryException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Interface into host messenger to notify it of new connections.
     *
     */
    public interface JoinHandler {
        /*
         * Notify that a specific host has joined with the specified host id.
         */
        public void notifyOfJoin(SocketChannel socket,
                                 SSLEngine sslEngine,
                                 InetSocketAddress listeningAddress,
                                 PublishHostIdRequest publishHostIdRequest) throws Exception;

        /*
         * A node wants to join the socket mesh
         */
        public void requestJoin(SocketChannel socket,
                                SSLEngine sslEngine,
                                MessagingChannel messagingChannel,
                                InetSocketAddress listeningAddress,
                                RequestHostIdRequest requestHostIdRequest) throws Exception;

        /*
         * A connection has been made to all of the specified hosts. Invoked by
         * nodes connected to the cluster
         */
        public void notifyOfHosts(int yourLocalHostId,
                                  Map<Integer, JSONObject> jos,
                                  List<ConnectedHostInformation> connectedHostInformations) throws Exception;

        /*
         * Create new connection between given node and current node
         */
        public void notifyOfConnection(SocketChannel socket,
                                       SSLEngine sslEngine,
                                       InetSocketAddress listeningAddress,
                                       RequestForConnectionRequest requestForConnectionMessage) throws Exception;
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
         * Probe coordinator host list for leader candidates that are operational
         * (i.e. node state is operational)
         */
        m_coordIp = null;
        for (String coordHost: m_acceptor.getCoordinators()) {
            if (m_coordIp != null) {
                break;
            }
            /*
             * On an operational leader (i.e. node is up) the request to join the cluster
             * may be rejected, e.g. multiple hosts rejoining at the same time. In this case,
             * the code will retry.
             */
            long retryInterval = RETRY_INTERVAL;
            int nameRetries = NAME_LOOKUP_RETRY_LIMIT;
            final Random salt = new Random();
            while (true) {
                InetSocketAddress primary = null;
                try {
                    primary = addressFromHost(coordHost);
                    connectToPrimary(primary);
                    // m_coordIp is now filled in
                    break;
                } catch (UnknownHostException e) {
                    if (--nameRetries <= 0) {
                        LOG.rateLimitedWarn(LOG_RATE_LIMIT, "Unknown host name '%s', no more retries", coordHost);
                        break; // no more retries; move on to next potential coordinator
                    }
                    LOG.rateLimitedWarn(LOG_RATE_LIMIT, "Unknown host name '%s', retrying", coordHost);
                    safeSleep(NAME_LOOKUP_RETRY_MS);
                } catch (SocketRetryException e) {
                    LOG.debug(String.format("Cannot connect to %s. %s", primary, e.getMessage()));
                    m_coordIp = null;  // no retry; move on to next potential coordinator
                    break;
                } catch (CoreUtils.RetryException e) {
                    LOG.warn(String.format("Request to join cluster mesh is rejected, retrying in %d seconds. %s",
                                           retryInterval, e.getMessage()));
                    safeSleep(TimeUnit.SECONDS.toMillis(retryInterval));

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
                    String s = m_coordIp != null ? m_coordIp.toString() : coordHost;
                    hostLog.error("Failed to establish socket mesh.", e);
                    throw new RuntimeException("Failed to establish socket mesh with " + s, e);
                }
            }
        }

        /*
         * m_coordIp will have been set by connectToPrimary if we
         * succeeded in finding a leader. Otherwise we need to
         * appoint one.
         */
        if (m_coordIp == null) {
            String leader = m_acceptor.getLeader().toString(); // may have host name
            InetSocketAddress leaderAddr = null; // resolved to address and port

            /*
             * Attempt to become the leader by binding to the specified address
             * (but only if the port is the correct one for our configuration).
             * Note: no retry for unresolved hostname here; we assume if a
             * name is not known, it cannot be our name, and therefore there
             * is no point in waiting so we can try to bind to that address.
             */
            try {
                leaderAddr = addressFromHost(leader); // may throw for unresolved host name
                if (leaderAddr.getPort() == m_internalPort) {
                    hostLog.info("Attempting to bind to leader ip " + leaderAddr.getAddress().getHostAddress());
                    ServerSocketChannel listenerSocket = ServerSocketChannel.open();
                    listenerSocket.socket().bind(leaderAddr);
                    listenerSocket.socket().setPerformancePreferences(0, 2, 1);
                    listenerSocket.configureBlocking(false);
                    m_listenerSockets.add(listenerSocket);
                }
            }
            catch (IOException e) {
                if (!m_listenerSockets.isEmpty()) {
                    try {
                        m_listenerSockets.get(0).close();
                        m_listenerSockets.clear();
                    }
                    catch (IOException ex) {
                        hostLog.debug("Exception closing listeners after exception", ex);
                    }
                }
            }

            // There's a listener socket if and only if we're the leader
            if (!m_listenerSockets.isEmpty()) {

                // If an internal interface was specified, see if it matches the leader
                // address we have bound to.
                if (m_internalInterface != null && !m_internalInterface.equals("")) {
                    checkLeaderAgainstInterface(leaderAddr);
                }

                // We are the leader
                consoleLog.info("Connecting to VoltDB cluster as the leader...");
                m_coordIp = leaderAddr;
                retval = true;

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

            /*
             * Did not find a meshed leader, and we're not becoming the leader.
             * Retranslate any host name each time in case the name/address
             * mapping has changed.
             */
            while (m_coordIp == null) {
                try {
                    leaderAddr = addressFromHost(leader); // may throw for unresolved host name
                    LOG.rateLimitedInfo(LOG_RATE_LIMIT, "Connecting to the VoltDB cluster leader " + leaderAddr);
                    connectToPrimary(leaderAddr);
                } catch (UnknownHostException e) {
                    LOG.rateLimitedWarn(LOG_RATE_LIMIT, "Unknown host name '%s', retrying", leader);
                    safeSleep(NAME_LOOKUP_RETRY_MS);
                } catch (SocketRetryException e) {
                    LOG.rateLimitedWarn(LOG_RATE_LIMIT, "Cannot connect to %s, retrying. %s", leaderAddr, e.getMessage());
                    safeSleep(SOCKET_CONNECT_RETRY_MS);
                } catch (Exception e) {
                    hostLog.error(FAIL_ESTABLISH_MESH_MSG, e);
                    throw new RuntimeException("Failed to establish socket mesh with " + leader, e);
                }
                leaderAddr = null;
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
                    // ignored
                } catch (Throwable e) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Error in socket joiner run loop", true, e);
                }
            }
        });

        return retval;
    }

    /**
     * Compare internal interface address with leader address for equality.
     * The supplied internal interface may be identified by address, name, or fqdn.
     * We attempt to match any of those against the leader. For addresses, we must
     * compare binary addresses, not string representations, because the same address
     * can be represented in multiple ways.
     *
     * @param leaderAddr
     */
    private void checkLeaderAgainstInterface(InetSocketAddress leaderAddr) {
        final String mismatch = "The provided internal interface (%s%s) does not match the specified leader address (%s%s%s)."
            + " This will result in either a cluster which fails to start or an unintended network topology."
            + " The leader will now exit; correct your specified leader and interface and try restarting.";

        String error = null;
        try {
            InetAddress internalAddr = addressFromHost(m_internalInterface).getAddress();
            InetAddress testAddr = leaderAddr.getAddress();
            if (testAddr.equals(internalAddr)) {
                return;
            }
            String testHost = ReverseDNSCache.hostnameOrAddress(testAddr);
            if (testHost.equals(m_internalInterface)) {
                return;
            }
            String testCanon = testAddr.getCanonicalHostName();
            if (testCanon.equals(m_internalInterface)) {
                return;
            }
            String internalIp = internalAddr.getHostAddress();
            String testIp = testAddr.getHostAddress();
            error = String.format(mismatch,
                                  internalIp,
                                  m_internalInterface.equals(internalIp) ? "" : ", " + m_internalInterface,
                                  testIp,
                                  testHost.equals(testIp) ? "" : ", " + testHost,
                                  testCanon.equals(testHost) ? "" : ", " + testCanon);
        }

        catch (UnknownHostException ex) {
            // addressFromHost failed to map internal interface host string to address.
            // not expected to occur, but if it does, it's a mismatch.
            error = String.format(mismatch, m_internalInterface, "", leaderAddr.getAddress(), "", "");
        }

        if (error != null) {
            org.voltdb.VoltDB.crashLocalVoltDB(error);
        }
    }


    /*
     * Wrapper for sleep(), catching and ignoring interrupts.
     */
    private static void safeSleep(long msec) {
        try {
            Thread.sleep(msec);
        } catch (InterruptedException ex) {
            // ignore
        }
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
            String hostDisplayName,
            AtomicBoolean isPaused,
            JoinAcceptor acceptor,
            JoinHandler jh,
            SslContext sslServerContext,
            SslContext sslClientContext) {
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
        m_hostDisplayName = hostDisplayName;
    }

    /*
     * Form socket address from host string.
     */
    private static InetSocketAddress addressFromHost(String nameOrAddr) throws UnknownHostException  {
        HostAndPort hap = HostAndPort.fromString(nameOrAddr)
                                     .withDefaultPort(org.voltcore.common.Constants.DEFAULT_INTERNAL_PORT);
        if (hap.getHost().isEmpty()) {
            return new InetSocketAddress(hap.getPort());
        } else {
            InetSocketAddress addr = new InetSocketAddress(hap.getHost(), hap.getPort());
            if (addr.isUnresolved()) {
                throw new UnknownHostException(hap.getHost());
            }
            return addr;
        }
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
        return new JSONObject(new String(messageBytes.array(), StandardCharsets.UTF_8));
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
        SocketChannel sc;
        while ((sc = ssc.accept()) != null) {
            boolean success = false;
            boolean active = false;
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
                    int stringLength = result.m_remnant.getInt();
                    if (stringLength != result.m_remnant.remaining()) {
                        throw new IllegalArgumentException( "Remnant not a single full message. remnant: "
                                + result.m_remnant + ", expected length: " + stringLength);
                    }

                    jsObj = new JSONObject(Constants.UTF8ENCODING.decode(result.m_remnant).toString());
                } else {
                    jsObj = readJSONObjFromWire(messagingChannel);
                }

                active = true;  // we've got a live one
                if (LOG.isDebugEnabled()) {
                    LOG.debug(jsObj.toString(2));
                }

                SocketJoinerMessageBase request = SocketJoinerMessageParser.parse(jsObj);
                String remoteBuildString = request.getVersionString();

                // send a response with version/build data of this node
                VersionChecker versionChecker = m_acceptor.getVersionChecker();
                VersionBuildMessage versionBuildMessage = VersionBuildMessage.create(
                        versionChecker.getVersionString(),
                        versionChecker.getBuildString(),
                        versionChecker.isCompatibleVersionString(remoteBuildString)
                );

                // inject acceptor fields
                ByteBuffer messageByteBuffer = decorateAndSerialize(
                        versionBuildMessage.getJsonObject(),
                        Optional.of(m_paused.get()),
                        false
                );
                messagingChannel.writeMessage(messageByteBuffer);

                /*
                 * The new connection may specify the address it is listening on,
                 * or it can be derived from the connection itself
                 */
                InetSocketAddress listeningAddress;
                if (request.hasAddress()) {
                    listeningAddress = new InetSocketAddress(
                            InetAddress.getByName(request.getAddress()),
                            request.getPort()
                    );
                } else {
                    listeningAddress = new InetSocketAddress(
                            ((InetSocketAddress) sc.socket().getRemoteSocketAddress()).getAddress().getHostAddress(),
                            request.getPort()
                    );
                }

                /*
                 * The type of connection, it can be a new request to join the cluster
                 * or a node that is connecting to the rest of the cluster and publishing its
                 * host id or a request to add a new connection to the request node.
                 */
                String type = request.getType();

                hostLog.infoFmt("Received request type %s from %s", type, remoteAddress);
                if (type.equals(ConnectionType.REQUEST_HOSTID.name())) {
                    RequestHostIdRequest requestHostIdRequest = (RequestHostIdRequest) request;
                    m_joinHandler.requestJoin(sc, sslEngine, messagingChannel, listeningAddress, requestHostIdRequest);
                } else if (type.equals(ConnectionType.PUBLISH_HOSTID.name())){
                    PublishHostIdRequest publishHostIdRequest = (PublishHostIdRequest) request;
                    m_joinHandler.notifyOfJoin(sc, sslEngine, listeningAddress, publishHostIdRequest);
                } else if (type.equals(ConnectionType.REQUEST_CONNECTION.name())) {
                    RequestForConnectionRequest requestForConnectionMessage = (RequestForConnectionRequest) request;
                    m_joinHandler.notifyOfConnection(sc, sslEngine, listeningAddress, requestForConnectionMessage);
                } else {
                    throw new RuntimeException("Unexpected message type " + type + " from " + remoteAddress);
                }
                success = true;
            } catch (IOException e) {
                String msg = "IOException occurred while handling new client connection " + sc
                    + ". Client will most likely retry: " + e;
                if (active) {
                    LOG.info(msg);
                } else { // skip info logging if connection never really got started
                    LOG.debug(msg);
                }
            } finally {
                // do not leak sockets when exception happens
                if (!success) {
                    safeClose(sc);
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
                safeClose(ssc);
            }
            m_listenerSockets.clear();
            safeClose(m_selector);
            m_selector = null;
        }
    }

    /**
     * Read version info from a socket and check compatibility.
     * After verifying versions return if "paused" start is indicated. True if paused start otherwise normal start.
     * @return
     */
    private JSONObject readAndValidateResponseVersion(MessagingChannel messagingChannel,
                                                      Set<String> activeVersions) throws IOException, JSONException
    {
        // read the json response from socketjoiner with version info
        JSONObject jsonResponse = readJSONObjFromWire(messagingChannel);
        VersionChecker versionChecker = m_acceptor.getVersionChecker();

        VersionBuildMessage versionBuildMessage = VersionBuildMessage.fromJsonObject(jsonResponse);
        String remoteVersionString = versionBuildMessage.getVersionString();
        String remoteBuildString = versionBuildMessage.getBuildString();
        boolean remoteAcceptsLocalVersion = versionBuildMessage.isVersionCompatible();

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
        return versionBuildMessage.getJsonObject();
    }

    /**
     * Create socket to the leader node. Certain errors are
     * classified as retryable, and will be wrapped in a
     * SocketRetryException. Others are passed on as-is.
     */
    private SocketChannel createLeaderSocket(SocketAddress hostAddr)
        throws IOException, SocketRetryException {

        SocketChannel socket = null;
        try {
            socket = SocketChannel.open();
            socket.socket().connect(hostAddr, 5000);
            return socket;
        }
        catch (java.net.ConnectException
               | java.net.NoRouteToHostException
               | java.net.PortUnreachableException
               | java.net.SocketTimeoutException
               | java.net.UnknownHostException
               | java.nio.channels.UnresolvedAddressException ex) {
            safeClose(socket);
            throw new SocketRetryException(ex);
        }
        catch (Exception ex) {
            safeClose(socket);
            throw ex;
        }
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
                safeSleep(250);
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

        RequestHostIdRequest request;
        if (!m_internalInterface.isEmpty()) {
            request = RequestHostIdRequest.createWithAddress(
                    versionChecker.getVersionString(),
                    m_internalPort,
                    m_internalInterface,
                    m_hostDisplayName
            );
        } else {
            request = RequestHostIdRequest.createWithoutAddress(
                    versionChecker.getVersionString(),
                    m_internalPort,
                    m_hostDisplayName
            );
        }

        // communicate configuration and node state
        messagingChannel.writeMessage(decorateAndSerialize(request.getJsonObject(), Optional.empty(), true));

        // read the json response from socketjoiner with version info and validate it
        JSONObject leaderInfo = readAndValidateResponseVersion(messagingChannel, activeVersions);
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
            MessagingChannel messagingChannel,
            Set<String> activeVersions) throws Exception
    {
        PublishHostIdRequest publishHostIdRequest = PublishHostIdRequest.create(
                m_localHostId,
                m_internalPort,
                m_hostDisplayName,
                m_internalInterface.isEmpty() ? m_reportedInternalInterface : m_internalInterface,
                m_acceptor.getVersionChecker().getVersionString()
        );

        JSONObject jsObj = publishHostIdRequest.getJsonObject();
        ByteBuffer messageByteBuffer = decorateAndSerialize(jsObj, Optional.empty(), true);
        messagingChannel.writeMessage(messageByteBuffer);

        // read the json response from socketjoiner with version info and validate it
        return readAndValidateResponseVersion(messagingChannel, activeVersions);
    }

    private ByteBuffer decorateAndSerialize(JSONObject jsObj, Optional<Boolean> paused, boolean mayExchangeTs) throws JSONException {
        jsObj = m_acceptor.decorate(jsObj, paused);
        if (mayExchangeTs) {
            jsObj.put(MAY_EXCHANGE_TS, mayExchangeTs);
        }

        return serialize(jsObj);
    }

    private ByteBuffer serialize(JSONObject jsObj) throws JSONException {
        byte[] jsBytes = jsObj.toString(4).getBytes(StandardCharsets.UTF_8);
        ByteBuffer pushHostId = ByteBuffer.allocate(4 + jsBytes.length);
        pushHostId.putInt(jsBytes.length);
        pushHostId.put(jsBytes).flip();
        return pushHostId;
    }


    static class SocketInfo {
        public final SocketChannel m_socket;
        public final SSLEngine m_sslEngine;

        public SocketInfo(SocketChannel socket, SSLEngine sslEngine) {
            m_socket = socket;
            m_sslEngine = sslEngine;
        }
    }
    public SocketInfo requestForConnection(InetSocketAddress hostAddr) throws IOException, JSONException
    {
        SocketChannel socket = connectToHost(hostAddr);
        SSLEngine sslEngine;
        try {
            sslEngine = initializeSocket(socket, true, null).m_sslEngine;
        } catch(IOException e) {
            safeClose(socket);
            throw new IOException("SSL setup to " + socket.getRemoteAddress() + " failed", e);
        }
        MessagingChannel messagingChannel = MessagingChannel.get(socket, sslEngine);
        RequestForConnectionRequest requestForConnectionMessage = RequestForConnectionRequest.create(
                m_acceptor.getVersionChecker().getVersionString(),
                m_localHostId,
                m_internalPort,
                m_hostDisplayName,
                m_internalInterface.isEmpty() ? m_reportedInternalInterface : m_internalInterface
        );

        ByteBuffer byteBuffer = serialize(requestForConnectionMessage.getJsonObject());
        messagingChannel.writeMessage(byteBuffer);

        // read the json response from socketjoiner with version info and validate it
        // (Actually version is not checked here, since we pass checkVersion=false)
        readJSONObjFromWire(messagingChannel);
        return new SocketInfo(socket, sslEngine);
    }

    /*
     * If this node failed to bind to the leader address
     * it must connect to the leader which will generate a host id and
     * advertise the rest of the cluster so that connectToPrimary can connect to it
     */
    private void connectToPrimary(InetSocketAddress coordIp) throws Exception {
        // collect clock skews from all nodes
        List<Long> skews = new ArrayList<Long>();

        // collect the set of active voltdb version strings in the cluster
        // this is used to limit simulatanious versions to two
        Set<String> activeVersions = new TreeSet<String>();

        try {
            LOG.debug("Non-Primary Starting & Connecting to Primary: " + coordIp);
            SocketChannel socket = createLeaderSocket(coordIp); // may throw SocketRetryException or other IOException
            socket.socket().setTcpNoDelay(true);
            socket.socket().setPerformancePreferences(0, 2, 1);

            SSLEngine leaderSSLEngine;
            try {
                leaderSSLEngine = initializeSocket(socket, true, skews).m_sslEngine;
            } catch(IOException e) {
                SocketAddress socketAddress = socket.getRemoteAddress();
                safeClose(socket);
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
            RequestJoinResponse requestJoinResponse = RequestJoinResponse.fromJsonObject(responseBody);

            if (!(requestJoinResponse.isAccepted().orElse(true))) {
                safeClose(socket);
                String rejectionReason = requestJoinResponse.getReason().orElse("rejection reason is not available");
                if (!requestJoinResponse.mayRetry().orElse(false)) {
                    org.voltdb.VoltDB.crashLocalVoltDB("Request to join cluster is rejected: " + rejectionReason);
                }
                throw new CoreUtils.RetryException(rejectionReason);
            }

            /*
             * Get the generated host id, and the interface we connected on
             * that was echoed back
             */
            m_localHostId = requestJoinResponse.getNewHostId();
            m_reportedInternalInterface = requestJoinResponse.getReportedAddress();

            ImmutableMap.Builder<Integer, JSONObject> cmbld = ImmutableMap.builder();
            cmbld.put(m_localHostId, m_acceptor.decorate(responseBody, Optional.<Boolean>empty()));

            /*
             * Loop over all the hosts and create a connection (except for the first entry, that is the leader)
             * and publish the host id that was generated. This finishes creating the mesh
             */
            List<HostInformation> hosts = requestJoinResponse.getHosts();
            List<ConnectedHostInformation> connectedHostInformations = new ArrayList<>();

            for (int ii = 0; ii < hosts.size(); ii++) {
                HostInformation hostInformation = hosts.get(ii);
                String address = hostInformation.getAddress();
                int port = hostInformation.getPort();
                final int hostId = hostInformation.getHostId();
                String hostDisplayName = hostInformation.getHostDisplayName();

                LOG.info("Leader provided address " + address + ":" + port);
                InetSocketAddress hostAddr = new InetSocketAddress(address, port);
                if (ii == 0) {
                    //Leader already has a socket
                    cmbld.put(ii, response.getLeaderInfo());
                    connectedHostInformations.add(
                            new ConnectedHostInformation(
                                    hostId,
                                    hostDisplayName,
                                    socket,
                                    leaderSSLEngine,
                                    hostAddr
                            )
                    );
                    continue;
                }
                // connect to all the peer hosts (except leader) and advertise our existence
                SocketChannel hostSocket = connectToHost(hostAddr);
                SSLEngine sslEngine = initializeSocket(hostSocket, true, skews).m_sslEngine;
                MessagingChannel messagingChannel = MessagingChannel.get(hostSocket, sslEngine);
                JSONObject hostInfo = publishHostId(messagingChannel, activeVersions);
                cmbld.put(ii, hostInfo);

                ConnectedHostInformation connectedHostInformation = new ConnectedHostInformation(
                        hostId,
                        hostDisplayName,
                        hostSocket,
                        sslEngine,
                        hostAddr
                );
                connectedHostInformations.add(connectedHostInformation);
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
                connectedHostInformations.get(0).getSocket().write(joinCompleteBuffer);
            }

            /*
             * Let host messenger know about the connections.
             * It will init the agreement site and then we are done.
             */
            m_joinHandler.notifyOfHosts( m_localHostId, cmbld.build(), connectedHostInformations);
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
                    "ms which is > than the " + MAX_CLOCKSKEW + "ms limit. Make sure NTP is running.", false, null);
        } else if (overallSkew > CRITICAL_CLOCKSKEW) {
            final String msg = "Clock skew is " + overallSkew +
                    "ms which is high. Ideally it should be sub-millisecond. Make sure NTP is running.";
            hostLog.warn(msg);
            consoleLog.warn(msg);
        } else {
            hostLog.infoFmt("Clock skew across all nodes in the cluster is %dms", overallSkew);
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
        safeClose(m_selector);
        m_es.shutdownNow();
        m_es.awaitTermination(356, TimeUnit.DAYS);
        for (ServerSocketChannel ssc : m_listenerSockets) {
            safeClose(ssc);
        }
        m_listenerSockets.clear();
        safeClose(m_selector); // duplicate call?
        m_selector = null;
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

    /**
     * Helper for cleaning up resources when we don't care about failure.
     * Usable for socket channels and selectors.
     */
    private static void safeClose(Closeable s) {
        if (s != null) {
            try {
                s.close();
            } catch (Exception ex) {
                // ignore
            }
        }
    }
}
